using System.Data;
using System.Data.SqlClient;
using System.Text;
using Microsoft.EntityFrameworkCore;

namespace Centeva.Data.Middleware {
	public class AuditLoggingIdentityMiddleware : DbContextMiddleware {
		private byte[] _currentUser;

		public AuditLoggingIdentityMiddleware() {
			SetCurrentUser(System.Threading.Thread.CurrentPrincipal.Identity.Name);
		}

		public AuditLoggingIdentityMiddleware(string currentUser) {
			SetCurrentUser(currentUser);
		}

		public override void BeforeSaveChanges(DbContext context) {
			if (_currentUser == null)
			{
				SetCurrentUser(System.Threading.Thread.CurrentPrincipal.Identity.Name);
			}
			context.Database.ExecuteSqlCommand("SET CONTEXT_INFO @ctx", new SqlParameter("@ctx", SqlDbType.VarBinary, 128) { Value = _currentUser });
		}

		private void SetCurrentUser(string currentUser)
		{
			if (currentUser.Length > 64)
			{
				currentUser = currentUser.Substring(0, 64);
			}
			_currentUser = Encoding.Unicode.GetBytes(currentUser);
		}
	}
}
