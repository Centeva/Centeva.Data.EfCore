using System;
using Centeva.Auditing.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Centeva.Data.Auditing {
	public interface IAuditContext:IDisposable {
		DbSet<Audit> Audits { get; set; }
		DbSet<AuditDetail> AuditDetails { get; set; }

		DatabaseFacade Database { get; }
		int SaveChanges();
	}
}
