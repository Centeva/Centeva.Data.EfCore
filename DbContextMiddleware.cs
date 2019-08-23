using Microsoft.EntityFrameworkCore;

namespace Centeva.Data {
	public abstract class DbContextMiddleware {
		public virtual void BeforeModelCreating(DbContext context, ModelBuilder modelBuilder) { }
		public virtual void AfterModelCreating(DbContext context, ModelBuilder modelBuilder) { }
		public virtual void BeforeSaveChanges(DbContext context) { }
		public virtual void AfterSaveChanges(DbContext context) { }
	}
}
