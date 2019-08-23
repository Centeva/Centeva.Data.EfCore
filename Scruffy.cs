using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Centeva.Data.EfCore {
	internal static class Scruffy
	{
		public static IList<T> Query<T>(this DatabaseFacade db, string sql, params SqlParameter[] parameters) where T : new()
        {
            using (var cmd = db.GetDbConnection().CreateCommand())
			{
                db.OpenConnection();

                cmd.CommandText = sql;
				cmd.Parameters.AddRange(parameters);

				using (var reader = cmd.ExecuteReader())
				{
					var map = BuildOrdinalMap(reader);

					List<T> result = new List<T>();
					while (reader.Read())
					{
						result.Add(Map<T>(reader, map));
					}
					return result;
				}
			}
		}

		public static async Task<IList<T>> QueryAsync<T>(this DatabaseFacade db, CancellationToken cancellationToken, string sql,
			params SqlParameter[] parameters) where T : new()
		{
			using (var cmd = db.GetDbConnection().CreateCommand())
			{
                await db.OpenConnectionAsync(cancellationToken: cancellationToken);

                cmd.CommandText = sql;
				cmd.Parameters.AddRange(parameters);

				using (var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
				{
					var map = BuildOrdinalMap(reader);

					List<T> result = new List<T>();
					while (reader.Read())
					{
						result.Add(Map<T>(reader, map));
					}
					return result;
				}
			}
		}

		private static T Map<T>(IDataReader reader, Dictionary<string, OrdinalAndType> ordinalMap) where T : new()
		{
			var result = new T();
			var props = typeof(T).GetProperties()
				.Where(p => p.CanWrite && ordinalMap.ContainsKey(p.Name))
				.ToList();
			props.ForEach(p => p.SetValue(result, MarshalType(reader, ordinalMap[p.Name].Ordinal, ordinalMap[p.Name].Type, p.PropertyType)));
			return result;
		}

		public class OrdinalAndType
		{
			public int Ordinal { get; set; }
			public Type Type { get; set; }
		}

		private static Dictionary<string, OrdinalAndType> BuildOrdinalMap(IDataReader reader)
		{
			var map = new Dictionary<string, OrdinalAndType>(StringComparer.OrdinalIgnoreCase);
			for (int i = 0; i < reader.FieldCount; i++)
			{
				map.Add(reader.GetName(i), new OrdinalAndType { Ordinal = i, Type = reader.GetFieldType(i) });
			}
			return map;
		}

		private static object MarshalType(IDataReader reader, int ordinal, Type dbType, Type propertyType)
		{
			if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>)) {
				if (reader.IsDBNull(ordinal)) {
					return null;
				}
				else {
					// Convert does not deal well with nullable types so you have to cast non-null values to the non-nullable version of the type
					propertyType = propertyType.GetGenericArguments()[0];
				}
				
			}
			return Convert.ChangeType(ValueConverter[dbType](reader, ordinal), propertyType);
		}

		private static readonly Dictionary<Type, Func<IDataReader, int, object>> ValueConverter = new Dictionary<Type, Func<IDataReader, int, object>>
		{
			{typeof(string), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(string) : reader.GetString(ordinal)},
			{typeof(long), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(long) : reader.GetInt64(ordinal)},
			{typeof(long?), (reader, ordinal) => reader[ordinal] == DBNull.Value ? null : (long?)reader.GetInt64(ordinal)},
			{typeof(int), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(int) : reader.GetInt32(ordinal)},
			{typeof(int?), (reader, ordinal) => reader[ordinal] == DBNull.Value ? null : (int?)reader.GetInt32(ordinal)},
			{typeof(short), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(short) : reader.GetInt16(ordinal)},
			{typeof(short?), (reader, ordinal) => reader[ordinal] == DBNull.Value ? null : (short?)reader.GetInt16(ordinal)},
			{typeof(byte), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(byte) : reader.GetByte(ordinal)},
			{typeof(byte?), (reader, ordinal) => reader[ordinal] == DBNull.Value ? null : (byte?)reader.GetByte(ordinal)},
			{typeof(decimal), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(decimal) : reader.GetDecimal(ordinal)},
			{typeof(decimal?), (reader, ordinal) => reader[ordinal] == DBNull.Value ? null : (decimal?)reader.GetDecimal(ordinal)},
			{typeof(float), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(float) : reader.GetFloat(ordinal)},
			{typeof(float?), (reader, ordinal) => reader[ordinal] == DBNull.Value ? null : (float?)reader.GetFloat(ordinal)},
			{typeof(double), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(double) : reader.GetDouble(ordinal)},
			{typeof(double?), (reader, ordinal) => reader[ordinal] == DBNull.Value ? null : (double?)reader.GetDouble(ordinal)},
			{typeof(char), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(char) : reader.GetChar(ordinal)},
			{typeof(char?), (reader, ordinal) => reader[ordinal] == DBNull.Value ? null : (char?)reader.GetChar(ordinal)},
			{typeof(bool), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(bool) : reader.GetBoolean(ordinal)},
			{typeof(bool?), (reader, ordinal) => reader[ordinal] == DBNull.Value ? null : (bool?)reader.GetBoolean(ordinal)},
			{typeof(DateTime), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(DateTime) : reader.GetDateTime(ordinal)},
			{typeof(DateTime?), (reader, ordinal) => reader[ordinal] == DBNull.Value ? null : (DateTime?)(DateTime)reader.GetDateTime(ordinal)},
			{typeof(Guid), (reader, ordinal) => reader[ordinal] == DBNull.Value ? default(Guid) : reader.GetGuid(ordinal)},
			{typeof(Guid?), (reader, ordinal) => reader[ordinal] == DBNull.Value ? null : (Guid?)reader.GetGuid(ordinal)},
		};
	}
}
