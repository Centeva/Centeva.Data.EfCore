using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using Centeva.Auditing.Models;
using Microsoft.EntityFrameworkCore;

namespace Centeva.Data.Auditing {
	/// <summary>
	/// Used to revert our Audits, so that after they are run the database goes back to the state it was before the test
	/// </summary>
	public class AuditReverter:IDisposable {
		public enum RunTypes {
			/// <summary>
			/// Run a SQL Statement for each change in the Audit table
			/// </summary>
			Simple
		}

		private enum ChangeTypes {
			Insert,
			Update,
			Delete
		}

		private readonly Func<IAuditContext> _getDb;
		private readonly int _auditCheckpoint;
		private readonly RunTypes _runType;

		public AuditReverter(Func<IAuditContext> getDb, RunTypes runType) {
			_getDb = getDb;
			_runType = runType;
			using(var db = getDb()) {
				var lastAudit = db.Audits.OrderByDescending(a => a.Id).FirstOrDefault();
				_auditCheckpoint = lastAudit?.Id ?? 0;
			}
		}

		public void Dispose() {
			//Let the triggers finish up, by sleeping for just a second
			Thread.Sleep(100);
			//Roll back our changes
			RollbackChanges();
		}

		private void RollbackChanges() {
			using(var db = _getDb()) {
				//using (var trans = new TransactionScope()) {

				//}

				//Get the rows that we need to work on				
				Audit[] submissions = db.Audits.Include(x => x.Details).Where(a => a.Id > _auditCheckpoint).OrderByDescending(g => g.Id).ToArray();

				//Get the collection of database tables
				var databaseTables = GetDatabaseTables(db);
				//Create an empty collection of what we need to rollback
				List<RevertTable> rollbackRecords = new List<RevertTable>();

				//Loop through all of the transactions that occured, and apply them so we have rollback records
				foreach(var submission in submissions) {
					RevertTable.ApplyTransaction(_runType == RunTypes.Simple, rollbackRecords, databaseTables, submission);
				}

				//Rollback our changes
				foreach(RevertTable rollbackRecord in rollbackRecords.OrderBy(x => x.Index)) {
					//Build the statement to revert this entity
					SqlStatement statement = rollbackRecord.BuildSqlStatement();
					System.Diagnostics.Debug.WriteLine(statement.Sql);
					//Execute the statement to revert this entity
					db.Database.ExecuteSqlCommand(statement.Sql, statement.SqlParameters.ToArray());
				}

				//Remove the Audit  Detail Rows that were just updated.
				db.AuditDetails.RemoveRange(db.AuditDetails.Where(a => a.AuditId > _auditCheckpoint));
				//Remove the Audit  Rows that were just updated.
				db.Audits.RemoveRange(db.Audits.Where(a => a.Id > _auditCheckpoint));
				db.SaveChanges();
			}
		}

		private List<DatabaseTable> GetDatabaseTables(IAuditContext db) {
            db.Database.OpenConnection();
			using(var cmd = db.Database.GetDbConnection().CreateCommand()) {
				cmd.CommandText = @"SELECT	DISTINCT S.schema_id AS SchemaId,
									        S.Name as SchemaName,
									        T.object_id AS ObjectId,
									        T.Name AS TableName,
									        C.column_id AS ColumnId,
									        C.Name AS ColumnName,
									        TY.name AS DataTypeName,
									        C.max_length AS MaxLength,
									        C.precision,
									        C.scale,
									        C.is_nullable AS IsNullable,
									        C.is_identity AS IsIdentity,
									        CAST(CASE WHEN PKs.object_id IS NOT NULL THEN 1 ELSE 0 END AS BIT) AS IsPrimaryKey,
									        CAST(CASE WHEN IsNull(C.default_object_id, 0) > 0 THEN 1 ELSE 0 END AS Bit) AS HasDefaultValue
							        FROM sys.schemas S
									        JOIN sys.tables T
										        ON T.schema_id = S.schema_id
									        JOIN sys.columns C
										        ON C.object_id = T.object_id
									        JOIN sys.types TY
										        ON TY.user_type_id = C.user_type_id
									        LEFT OUTER JOIN(
										        SELECT  DISTINCT T.object_id,
												        c.column_id
										        FROM    sys.tables T
												        JOIN sys.columns C
													        ON C.object_id = T.object_id
												        JOIN sys.index_columns IC
													        ON IC.object_id = C.object_id
													        AND IC.column_id = C.column_id
												        JOIN sys.indexes I
													        ON I.object_id = IC.object_id
													        AND I.index_id = IC.index_id
													        AND I.is_primary_key = 1
									        ) as PKs
										        ON PKs.object_id = c.object_id
										        AND PKs.column_id = c.column_id
							        ORDER BY S.Name,
									        T.Name,
									        C.Name";
				using(var reader = cmd.ExecuteReader()) {
					Dictionary<string, DatabaseTable> tables = new Dictionary<string, DatabaseTable>();
					while(reader.Read()) {
						string fullTableName = $"{reader["SchemaName"]}.{reader["TableName"]}";
						if(!tables.TryGetValue(fullTableName, out DatabaseTable table)) {
							table = new DatabaseTable {
								SchemaId = reader.GetInt32(reader.GetOrdinal("SchemaId")),
								SchemaName = reader.GetString(reader.GetOrdinal("SchemaName")),
								ObjectId = reader.GetInt32(reader.GetOrdinal("ObjectId")),
								TableName = reader.GetString(reader.GetOrdinal("TableName")),
								Fields = new List<DatabaseField>()
							};
							tables.Add(fullTableName, table);
						}

						DatabaseField field = new DatabaseField {
							ColumnId = reader.GetInt32(reader.GetOrdinal("ColumnId")),
							ColumnName = reader.GetString(reader.GetOrdinal("ColumnName")),
							DataTypeName = reader.GetString(reader.GetOrdinal("DataTypeName")),
							MaxLength = reader.GetInt16(reader.GetOrdinal("MaxLength")),
							Precision = reader.GetByte(reader.GetOrdinal("Precision")),
							Scale = reader.GetByte(reader.GetOrdinal("Scale")),
							IsNullable = reader.GetBoolean(reader.GetOrdinal("IsNullable")),
							IsIdentity = reader.GetBoolean(reader.GetOrdinal("IsIdentity")),
							IsPrimaryKey = reader.GetBoolean(reader.GetOrdinal("IsPrimaryKey")),
							HasDefaultValue = reader.GetBoolean(reader.GetOrdinal("HasDefaultValue"))
						};
						table.Fields.Add(field);
					}

					return tables.Values.ToList();
				}
			}
		}


		#region Private Classes
		private class RevertTable {
			public string TableName { get; set; }
			public string PrimaryKeyValue { get; set; }
			public DatabaseTable DatabaseTable { get; set; }
			public int Index { get; set; }
			public List<RevertField> Fields { get; set; }
			public ChangeTypes? ChangeType { get; private set; }

			private void ApplyChanges(string changeType, List<AuditDetail> changes, int rollbackOrder) {
				ChangeTypes proposedChangeType;
				switch(changeType) {
					//I is Insert, which means we need to delete.
					case "I":
						proposedChangeType = ChangeTypes.Delete;
						break;
					//D is Delete, which means we need to insert the record, possibly with Identity Insert
					case "D":
						proposedChangeType = ChangeTypes.Insert;
						break;
					//U is Update, which means we need to revert to old change
					default:
						proposedChangeType = ChangeTypes.Update;
						break;
				}

				if(ChangeType == null || ChangeType == proposedChangeType) {
					ChangeType = proposedChangeType;
				}
				else if(proposedChangeType == ChangeTypes.Delete) {
					//Delete trumps everything else
					ChangeType = proposedChangeType;
					//Move where the delete needs to happen to be right here.  Otherwise, we will try to delete it to early in the chain
					Index = rollbackOrder;
				}
				else if(ChangeType == ChangeTypes.Insert && proposedChangeType == ChangeTypes.Update) {
					//Go ahead and allow this to continue
					ChangeType = ChangeTypes.Insert;
				}
				else {
					throw new Exception("What is the maddness?");
				}

				//If we aren't deleting, then 
				if(ChangeType != ChangeTypes.Delete) {
					foreach(AuditDetail change in changes) {
						var field = Fields.FirstOrDefault(x => String.Compare(x.FieldName, change.FieldName, StringComparison.InvariantCultureIgnoreCase) == 0);
						if(field == null) {
							//Add a new field to the table
							var databaseColumn = DatabaseTable.Fields.FirstOrDefault(x => String.Compare(x.ColumnName, change.FieldName, StringComparison.InvariantCultureIgnoreCase) == 0);
							if(databaseColumn == null) {
								throw new NotSupportedException($"Unable to find Field Name {change.FieldName}");
							}
							field = new RevertField {
								FieldName = databaseColumn.ColumnName,
								DatabaseField = databaseColumn,
								FieldValue = String.Empty
							};

							Fields.Add(field);
						}
						field.ApplyOldValue(proposedChangeType, change);
					}
				}
			}

			private static RevertTable Create(string tableName, string primaryKeyValue, DatabaseTable databaseTable, int index) {
				//Used to create a new Revert table record.  Will instanciate the Primary Key change
				RevertTable table = new RevertTable {
					TableName = tableName,
					PrimaryKeyValue = primaryKeyValue,
					DatabaseTable = databaseTable,
					Index = index,
					Fields = new List<RevertField>()
				};
				//Get the Primary Key
				var primaryKeyColumn = databaseTable.Fields.FirstOrDefault(x => x.IsPrimaryKey);
				if(primaryKeyColumn != null) {
					//Set the Primary Key Value
					table.Fields.Add(new RevertField {
						FieldName = primaryKeyColumn.ColumnName,
						DatabaseField = primaryKeyColumn,
						FieldValue = primaryKeyValue
					});
				}
				else {
					throw new NotSupportedException("Cannot find Primary Key for this table");
				}

				return table;
			}

			public static void ApplyTransaction(bool consolidateData, List<RevertTable> rollbackRecords, List<DatabaseTable> databaseTables, Audit submission) {
				var rollbackOrder = rollbackRecords.Count + 1;
				var rollback = rollbackRecords.FirstOrDefault(x => x.TableName == submission.TableName && x.PrimaryKeyValue == submission.PK);
				//If we are not trying to consolidate Updates, then just run a statement for each transaction.
				if(rollback == null || !consolidateData) {
					//Get the database table this applies to
					DatabaseTable databaseTable = databaseTables.FirstOrDefault(x => $"{x.SchemaName}.{x.TableName}" == submission.TableName);
					//Create a new rollback record
					rollback = Create(submission.TableName, submission.PK, databaseTable, rollbackOrder);
					rollbackRecords.Add(rollback);
				}
				rollback.ApplyChanges(submission.Type, submission.Details, rollbackOrder);
			}

			public SqlStatement BuildSqlStatement() {
				string sql = String.Empty;
				List<SqlParameter> parameters = new List<SqlParameter>();
				string formattedTableName = $"[{DatabaseTable.SchemaName}].[{DatabaseTable.TableName}]";
				switch(ChangeType) {
					case ChangeTypes.Delete:
						sql = $"DELETE FROM {formattedTableName} ";
						AppendWhereCondition(ref sql, parameters);
						break;
					case ChangeTypes.Update:
						sql = $" UPDATE {formattedTableName} ";
						AppendUpdateFields(ref sql, parameters);
						AppendWhereCondition(ref sql, parameters);
						break;
					case ChangeTypes.Insert:
						if(DatabaseTable.HasIdentity) {
							sql = $"SET Identity_Insert {formattedTableName} ON; ";
						}

						sql += $"INSERT INTO {formattedTableName} ";
						AppendInsertFields(ref sql, parameters);
						if(DatabaseTable.HasIdentity) {
							sql += $" SET Identity_Insert {formattedTableName} OFF; ";
						}
						break;
					default:
						throw new NotSupportedException("Unknown Change Type");
				}

				return new SqlStatement {
					Sql = sql,
					SqlParameters = parameters
				};
			}

			private void AppendWhereCondition(ref string sql, List<SqlParameter> parameters) {
				var primaryKeys = Fields.Where(f => f.IsPrimaryKey).ToList();
				sql += " WHERE " + String.Join(" and ", primaryKeys.Select(f => $"[{f.FieldName}] = {f.ParameterName}"));
				List<SqlParameter> newParameters = primaryKeys.Select(f => f.BuildSqlParameter()).ToList();
				foreach(var parameter in newParameters) {
					//Don't add duplicate parameters
					if(parameters.All(p => p.ParameterName != parameter.ParameterName)) {
						parameters.Add(parameter);
					}
				}
			}

			private void AppendUpdateFields(ref string sql, List<SqlParameter> parameters) {
				var changedFields = Fields.Where(x => x.IsChanged).ToList();
				if(changedFields.Any(x => x.IsPrimaryKey)) {
					throw new NotSupportedException("Cannot currently support updating a Primary Key");
				}

				sql += " SET " + String.Join(", ", changedFields.Select(f => $" [{f.FieldName}] = {f.ParameterName}"));
				List<SqlParameter> newParameters = changedFields.Select(f => f.BuildSqlParameter()).ToList();

				foreach(var parameter in newParameters) {
					//Don't add duplicate parameters
					if(parameters.All(p => p.ParameterName != parameter.ParameterName)) {
						parameters.Add(parameter);
					}
				}

			}

			private void AppendInsertFields(ref string sql, List<SqlParameter> parameters) {
				var fields = Fields.Where(x => x.IsChanged || x.IsPrimaryKey).ToList();
				//HasDefaultValue
				//Need to add fields that have a default value, but aren't in the list of fields above.
				//This is because if the default value was used, it would be in the audit results.  If it wasn't used, then it was inserted with a null value.
				//And because it was inserted with a null value, we need to insert it with a null value so that the insert doesn't happen
				var databaseFieldsToAdd = DatabaseTable.Fields.FindAll(x => x.HasDefaultValue && fields.All(f => f.DatabaseField != x)).ToList();

				foreach(var databaseColumn in databaseFieldsToAdd) {
					fields.Add(new RevertField {
						FieldName = databaseColumn.ColumnName,
						DatabaseField = databaseColumn,
						FieldValue = String.Empty
					}
					);
				}


				sql += "(" + String.Join(",", fields.Select(f => $"[{f.FieldName}]")) + ") VALUES ";
				sql += "(" + String.Join(",", fields.Select(f => f.ParameterName)) + ");";

				List<SqlParameter> newParameters = fields.Select(f => f.BuildSqlParameter()).ToList();

				foreach(var parameter in newParameters) {
					//Don't add duplicate parameters
					if(parameters.All(p => p.ParameterName != parameter.ParameterName)) {
						parameters.Add(parameter);
					}
				}
			}
		}

		private class SqlStatement {
			public string Sql { get; set; }
			public List<SqlParameter> SqlParameters { get; set; }
		}

		private class RevertField {
			public string FieldName { get; set; }
			public DatabaseField DatabaseField { get; set; }
			public bool IsPrimaryKey => DatabaseField?.IsPrimaryKey ?? false;
			public string FieldValue { get; set; }
			public string ParameterName => $"@{FieldName}";
			public bool IsChanged;
			public SqlParameter BuildSqlParameter() => DatabaseField.BuildSqlParameter(ParameterName, FieldValue);

			public void ApplyOldValue(ChangeTypes changeType, AuditDetail change) {
				if(changeType == ChangeTypes.Delete) {
					FieldValue = change.NewValue;
				}
				else {
					FieldValue = change.OldValue;
				}
				IsChanged = true;
			}
		}

		private class DatabaseTable {
			public int SchemaId { get; set; }
			public string SchemaName { get; set; }
			public int ObjectId { get; set; }
			public string TableName { get; set; }
			public List<DatabaseField> Fields { get; set; }
			public bool HasIdentity => Fields.Any(f => f.IsPrimaryKey && f.IsIdentity);
		}

		private class DatabaseField {
			public int ColumnId { get; set; }
			public string ColumnName { get; set; }
			public string DataTypeName { get; set; }
			public int MaxLength { get; set; }
			public int Precision { get; set; }
			public int Scale { get; set; }
			public bool IsNullable { get; set; }
			public bool IsIdentity { get; set; }
			public bool IsPrimaryKey { get; set; }
			public bool HasDefaultValue { get; set; }

			public SqlParameter BuildSqlParameter(string parameterName, string fieldValue) {
				return new SqlParameter(parameterName, GetSqlDbType()) { Value = GetCastedValue(fieldValue) };
			}

			private object GetCastedValue(string fieldValue) {
				if(String.IsNullOrEmpty(fieldValue)) {
					if(IsNullable)
						return DBNull.Value;
					if(DataTypeName == "bit")
						return false;
					throw new NotSupportedException("This field cannot support a null value");

				}

				//Mappings of object types....
				//https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-data-type-mappings
				//--bigint			==> int
				//--int				==> int
				//--smallint		==> int

				//--char			==> string
				//--nvarchar		==> string
				//--varchar			==> string

				//--bit				==> bool
				//--date			==> DateTime
				//--datetime		==> DateTime
				//--datetime2		==> DateTime
				//--float			==> Double
				//--real			==> single
				//--varbinary		==> byte[]
				switch(DataTypeName.ToLower()) {
					case "bigint":
					case "int":
					case "smallint":
						return Int32.Parse(fieldValue);
					case "char":
					case "nvarchar":
					case "varchar":
						return fieldValue;
					case "date":
					case "datetime":
					case "datetime2":
						return DateTime.Parse(fieldValue);
					case "bit":
						if(fieldValue == "1")
							return true;
						else if(fieldValue == "0")
							return false;
						return Boolean.Parse(fieldValue);
					case "float":
						return Double.Parse(fieldValue);
					case "real":
						return Single.Parse(fieldValue);
					case "varbinary":
						return Byte.Parse(fieldValue);
					default:
						return fieldValue;
				}
			}

			private SqlDbType GetSqlDbType() {
				switch(DataTypeName.ToLower()) {
					case "bigint":
						return SqlDbType.BigInt;
					case "bit":
						return SqlDbType.Bit;
					case "char":
						return SqlDbType.Char;
					case "date":
						return SqlDbType.Date;
					case "datetime":
						return SqlDbType.DateTime;
					case "datetime2":
						return SqlDbType.DateTime2;
					case "float":
						return SqlDbType.Float;
					case "int":
						return SqlDbType.Int;
					case "nvarchar":
						return SqlDbType.NVarChar;
					case "real":
						return SqlDbType.Real;
					case "smallint":
						return SqlDbType.SmallInt;
					case "varbinary":
						return SqlDbType.VarBinary;
					case "varchar":
						return SqlDbType.VarChar;
					default:
						return SqlDbType.VarChar;
				}
			}
		}
		#endregion

	}
}