using Centeva.Auditing.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Centeva.Data.EfCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace Centeva.Data.Auditing {
	public static class AuditScriptCreator {
		public static IEnumerable<string> GetTriggerScripts(DatabaseFacade db, AuditConfig auditConfig = null, params AuditIgnore[] doNotAudit) {
			db = db ?? throw new ArgumentNullException("db");
			auditConfig = auditConfig ?? new AuditConfig();
			var ignoreLogging = doNotAudit.ToList();
			ignoreLogging.Add(AuditIgnore.Create(auditConfig.Schema, auditConfig.AuditTable));// Do not log the table that logs changes
			ignoreLogging.Add(AuditIgnore.Create(auditConfig.Schema, auditConfig.AuditDetailTable));// Do not log the table that logs change details

			var existingTriggers = GetExistingTriggers(db);
			var tables = GetTables(db, auditConfig.AuditTable, auditConfig.AuditDetailTable);
			var tableLookup = tables.ToLookup(TriggerNameForTable);

			// Get a list of triggers that either don't have a matching table or that are older than the table schema and need to be updated
			var triggersToDrop = existingTriggers
				.Where(trig => !tableLookup.Contains(trig.Name))
				.Union(existingTriggers.Where(trig => tableLookup.Contains(trig.Name) && 
					(auditConfig.AlwaysUpdateTriggers || tableLookup[trig.Name].Any(tab => tab.ModDate > trig.ModDate))))
				.ToArray();

			foreach(var trig in triggersToDrop.OrderBy(t => t.Name)) {
				Console.WriteLine("\tDrop " + trig.Name);
			}

			var dropAll = string.Join(Environment.NewLine, triggersToDrop.Select(t => string.Format("DROP TRIGGER [{0}].[{1}]", t.Schema, t.Name)));
			if(!string.IsNullOrWhiteSpace(dropAll)) {
				yield return dropAll;
			}

			var ignoredSchemas = ignoreLogging.OfType<IgnoreSchema>().Select(x => x.Schema).ToList();
			var ignoredTables = ignoreLogging.OfType<IgnoreTable>().Select(x => x.Schema + "." + x.Table).ToList();
			var tablesToCreateTriggersFor = tables.Where(t => !ignoredSchemas.Contains(t.Schema) && !ignoredTables.Contains(t.Schema + "." + t.Name) &&
				(existingTriggers.All(trig => trig.Name != TriggerNameForTable(t)) || triggersToDrop.Any(trig => trig.Name == TriggerNameForTable(t))))
				.ToArray();

			foreach(var t in tablesToCreateTriggersFor.OrderBy(TriggerNameForTable)) {
				Console.WriteLine("\tCreate " + TriggerNameForTable(t));
			}

			foreach(var t in tablesToCreateTriggersFor) {
				var ignoredColumns = ignoreLogging.OfType<IgnoreColumn>().Where(x => x.Schema == t.Schema && x.Table == x.Table).Select(x => x.Column).ToList();
				var cols = GetColumns(db, t.Schema, t.Name).Where(c => !ignoredColumns.Contains(c.ColumnName)).ToList();
				if(!cols.Any(c => c.PK)) {
					continue;
				}

				yield return GetTriggerCreateScript(
					t.Schema,
					t.Name,
					cols.Where(c => c.PK).Select(c => c.ColumnName).First(),
					GetColumnSelectList(cols),
					GetColumnList(cols),
					auditConfig.Schema,
					auditConfig.AuditTable,
					auditConfig.AuditDetailTable);
			}
		}

		private static string TriggerNameForTable(TableInfo t) {
			return string.Format("audit_{0}_{1}", t.Schema, t.Name);
		}

		private static string GetTriggerCreateScript(string schema, string table, string pk, string columnSelects, string columnList, string auditSchemaName, string auditTableName, string auditDetailTableName) {
			return $@"
				CREATE TRIGGER audit_{schema}_{table} ON [{schema}].[{table}] FOR INSERT, UPDATE, DELETE
				AS
				SET NOCOUNT ON

				DECLARE @Audit TABLE (
					[Type] [char](1),
					[PK] [nvarchar](10),
					[FieldName] [varchar](50),
					[OldValue] [nvarchar](max),
					[NewValue] [nvarchar](max)
				);
				DECLARE @InsertedIds TABLE (
					[AuditId] Int,
					[TableName] Varchar(50),
					[PK] [nvarchar](10),
					[Type] [char](1)
				);

				IF (SELECT COUNT(*) FROM deleted) = 0
				BEGIN
					INSERT INTO @Audit
					(
						[Type], 
						[PK], 
						[FieldName], 
						[OldValue], 
						[NewValue]
					)
					SELECT	'I' AS [Type],
							CAST([{pk}] AS nvarchar(10)) AS [PK],
							[FieldName],
							NULL AS [OldValue],
							NULLIF([UnpivotValue], CHAR(0)) AS [NewValue]
					FROM	(
								SELECT	[{pk}],
										{columnSelects}
								FROM	inserted
							) vals
							UNPIVOT ( 
								[UnpivotValue] 
								FOR [FieldName] IN ({columnList})
							) AS pvt
					WHERE [UnpivotValue] <> CHAR(0)
				END
				ELSE IF (SELECT COUNT(*) FROM inserted) = 0
				BEGIN
					INSERT INTO @Audit
					(
						[Type], 
						[PK], 
						[FieldName], 
						[OldValue], 
						[NewValue]
					)
					SELECT	'D' AS [Type],
							CAST([{pk}] AS nvarchar(10)) AS [PK],
							[FieldName],
							NULLIF([UnpivotValue], CHAR(0)) AS [OldValue],
							NULL AS [NewValue]
					FROM	(
								SELECT	[{pk}],
										{columnSelects}
								FROM deleted
							) vals
							UNPIVOT ( 
								[UnpivotValue] 
								FOR [FieldName] IN ({columnList})
							) AS pvt
					WHERE [UnpivotValue] <> CHAR(0)
				END
				ELSE
				BEGIN
					INSERT INTO @Audit
					(
						[Type], 
						[PK], 
						[FieldName], 
						[OldValue], 
						[NewValue]
					)
					SELECT	'U' AS [Type],
							CAST(new.[{pk}] AS nvarchar(10)) AS [PK],
							new.[FieldName],
							NULLIF(old.[UnpivotValue], CHAR(0)) AS [OldValue],
							NULLIF(new.[UnpivotValue], CHAR(0)) AS [NewValue]
					FROM	(
								SELECT	[{pk}],
										{columnSelects}
								FROM deleted
							) t
							UNPIVOT (
								[UnpivotValue] 
								FOR [FieldName] IN ({columnList})
							) AS old
							INNER JOIN
							(
								SELECT	[{pk}], 
										[UnpivotValue], 
										[FieldName]
								FROM	(
											SELECT	[{pk}],
													{columnSelects}
											FROM inserted
										) t
										UNPIVOT (
											[UnpivotValue] 
											FOR [FieldName] IN ({columnList})
										) AS pvt
							) new 
								ON old.[{pk}] = new.[{pk}] 
								AND old.[FieldName] = new.[FieldName] 
								AND old.[UnpivotValue] <> new.[UnpivotValue]
					WHERE	old.[UnpivotValue] <> new.[UnpivotValue]
				END
				IF (@@RowCount>0)
				BEGIN
					INSERT INTO {auditSchemaName}.{auditTableName}
					(
						[TableName],
						[PK],
						[Type],
						[UpdateDate],
						[UserName]
					)
			        OUTPUT	INSERTED.AuditId, 
							INSERTED.TableName,   
							INSERTED.Pk, 
							INSERTED.Type
					INTO	@InsertedIds
					SELECT	DISTINCT '{schema}.{table}' AS [TableName],
							[PK], 
							[Type], 
							Getdate() AS [UpdateDate],
							ISNULL(CONVERT(nvarchar(64), CONTEXT_INFO()), CAST(SUSER_SNAME() AS nvarchar(64))) AS [UserName]
					FROM	@Audit;

					INSERT INTO {auditSchemaName}.{auditDetailTableName}
					(
						[AuditId],
						[FieldName],
						[OldValue],
						[NewValue]
					)
					SELECT	I.[AuditId],
							A.[FieldName],
							A.[OldValue],
							A.[NewValue]
					FROM	@Audit A
							JOIN @InsertedIds I
								ON I.[PK] =  A.[PK]
								AND I.[Type] = A.[Type];
				END
				SET NOCOUNT OFF
				";
		}

		private static string GetColumnSelectList(List<ColumnInfo> columns) {
			return string.Join(",\r\n\t\t\t", columns.Where(c => !c.PK).Select(GetSingleColumnSelect));
		}

		private static string GetColumnList(List<ColumnInfo> columns) {
			return string.Join(", ", columns.Where(c => !c.PK).Select(c => "[" + c.ColumnName + "]"));
		}

		private static readonly HashSet<string> DateAndTimeTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
			"date",
			"datetime",
			"datetime2",
			"datetimeoffset",
			"smalldatetime",
			"time"
		};

		private static readonly HashSet<string> BinaryTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
			"binary",
			"varbinary",
			"image",
		};

        private static string GetSingleColumnSelect(ColumnInfo column) {
            string result;
            if (DateAndTimeTypes.Contains(column.Type)) {
                result = $"ISNULL(CONVERT(nvarchar(MAX), [{column.ColumnName}], 126), CHAR(0)) AS [{column.ColumnName}]";
            }
            else if (BinaryTypes.Contains(column.Type)) {
                result = $"CONVERT(nvarchar(MAX), '<binary: ' + ISNULL('length=' + CONVERT(nvarchar(MAX), LEN([{column.ColumnName}])), 'NULL') + '>') AS [{column.ColumnName}]";
            }
            else {
                result =  $"ISNULL(CONVERT(nvarchar(MAX), [{column.ColumnName}]), CHAR(0)) AS [{column.ColumnName}]";
            }
            return result;
        }

		private static List<ColumnInfo> GetColumns(DatabaseFacade db, string schema, string table) {
			const string sql = @"
				SELECT DISTINCT
					c.column_id AS [Order],
					c.name AS [ColumnName],
					p.name AS [Type],
					CAST(CASE WHEN EXISTS(SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = c.name AND CONSTRAINT_NAME LIKE 'PK_%') THEN 1 ELSE 0 END AS bit) AS [PK],
					--c.is_identity AS [PK],
					CAST(c.max_length AS int) AS [Length],
					CAST(c.precision AS int) AS [Precision],
					CAST(c.scale AS int) AS [Scale]
				FROM sys.columns c
				JOIN sys.tables t on t.object_id = c.object_id
				JOIN sys.types p on p.user_type_id = c.user_type_id
				WHERE SCHEMA_NAME(t.schema_id) = @schema AND t.name = @table AND c.is_computed = 0
				ORDER BY c.column_id
			";

			return db.Query<ColumnInfo>(
				sql,
				new SqlParameter("@schema", SqlDbType.VarChar, 50) { Value = schema },
				new SqlParameter("@table", SqlDbType.VarChar, 50) { Value = table }
			).ToList();
		}

		private static List<TableInfo> GetTables(DatabaseFacade db, string auditTableName, string auditDetailTableName) {
			string sql = $@"
				SELECT SCHEMA_NAME(t.schema_id) AS [Schema], t.name AS [Name], t.modify_date AS [ModDate]
				FROM sys.tables t
				WHERE t.name <> '__MigrationHistory'";

			return db.Query<TableInfo>(sql).ToList();
		}

		private static List<TriggerInfo> GetExistingTriggers(DatabaseFacade db) {
			const string sql = @"
				SELECT SCHEMA_NAME(t.schema_id) AS [Schema], trig.name AS [Name], trig.modify_date AS [ModDate]
				FROM sys.triggers trig
				INNER JOIN sys.tables t on t.object_id = trig.parent_id
				WHERE trig.name LIKE 'audit_%_%'";
			return db.Query<TriggerInfo>(sql).ToList();
		}

		private class TableInfo {
			public string Schema { get; set; }
			public string Name { get; set; }
			public DateTime ModDate { get; set; }
		}

		private class ColumnInfo {
			public int Order { get; set; }
			public string ColumnName { get; set; }
			public string Type { get; set; }
			public bool PK { get; set; }
			public int Length { get; set; }
			public int Precision { get; set; }
			public int Scale { get; set; }
		}

		private class TriggerInfo {
			public string Schema { get; set; }
			public string Name { get; set; }
			public DateTime ModDate { get; set; }
		}
	}
}
