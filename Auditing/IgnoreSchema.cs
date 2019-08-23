namespace Centeva.Data.Auditing {
	public class IgnoreSchema:AuditIgnore {
		public string Schema { get; set; }

		public IgnoreSchema(string schema) {
			Schema = schema;
		}
	}
}
