using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Centeva.Auditing.Models {
	public class Audit {
		public int Id { get; set; }

		[Required, StringLength(1), Column(TypeName = "CHAR")]
		public string Type { get; set; }

		[Required, StringLength(75)]
		public string TableName { get; set; }

		[Required, StringLength(20)]
		public string PK { get; set; }

		[Required, Column(TypeName = "datetime2")]
		public DateTime UpdateDate { get; set; }

		[Required, StringLength(64)]
		public string UserName { get; set; }

		public List<AuditDetail> Details { get; set; }
	}
}
