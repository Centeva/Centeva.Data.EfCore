using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Centeva.Data {
	public abstract class DbContextWithMiddleware : DbContext {
		private readonly List<DbContextMiddleware> _middleware;
		private bool _disposed;

        public IReadOnlyList<DbContextMiddleware> Middleware => _middleware.AsReadOnly();
        public T GetMiddleWare<T>() where T : DbContextMiddleware => _middleware.OfType<T>().FirstOrDefault();

        protected DbContextWithMiddleware(params DbContextMiddleware[] middleware) {
			_middleware = middleware.ToList();
		}

		protected DbContextWithMiddleware(DbContextOptions options, params DbContextMiddleware[] middleware) : base(options) {
			_middleware = middleware.ToList();
		}

		protected override void OnModelCreating(ModelBuilder modelBuilder) {
			_middleware.ForEach(m => m.BeforeModelCreating(this, modelBuilder));
			base.OnModelCreating(modelBuilder);
			_middleware.ForEach(m => m.AfterModelCreating(this, modelBuilder));
		}

		public override int SaveChanges() {
            return SaveChanges(true);
        }

        public override int SaveChanges(bool acceptAllChangesOnSuccess) {
            _middleware.ForEach(m => m.BeforeSaveChanges(this));
            var result = base.SaveChanges(acceptAllChangesOnSuccess);
            _middleware.ForEach(m => m.AfterSaveChanges(this));
            return result;
        }

        public override Task<int> SaveChangesAsync(CancellationToken cancellationToken = new CancellationToken()) {
            return SaveChangesAsync(true, CancellationToken.None);
        }

		public override async Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken) {
			_middleware.ForEach(m => m.BeforeSaveChanges(this));
			var result = await base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
			_middleware.ForEach(m => m.AfterSaveChanges(this));
			return result;
		}

        public override void Dispose() {
            if (!_disposed) {
                foreach (var m in _middleware.OfType<IDisposable>()) {
                    m.Dispose();
                }

                base.Dispose();
            }
            _disposed = true;
        }
	}
}
