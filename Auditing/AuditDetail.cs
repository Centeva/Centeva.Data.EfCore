using System.ComponentModel.DataAnnotations;

namespace Centeva.Auditing.Models {
	public class AuditDetail {
		public int Id { get; set; }

		public int AuditId { get; set; }
		public Audit Audit { get; set; }

		[StringLength(75), Required]
		public string FieldName { get; set; }

		[MaxLength]
		public string OldValue { get; set; }

		[MaxLength]
		public string NewValue { get; set; }
	}
}
