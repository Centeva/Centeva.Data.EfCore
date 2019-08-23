namespace Centeva.Data.Auditing {
	public class AuditConfig {
		public string Schema { get; private set; }
		public string AuditTable { get; private set; }
		public string AuditDetailTable { get; private set; }
		public bool AlwaysUpdateTriggers { get; private set; }

		public AuditConfig(string schema = "dbo", string auditTable = "Audit", string auditDetailTable = "AuditDetail", bool alwaysUpdateTriggers = true) {
			Schema = schema;
			AuditTable = auditTable;
			AuditDetailTable = auditDetailTable;
			AlwaysUpdateTriggers = alwaysUpdateTriggers;
		}
	}
}
