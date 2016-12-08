using System.Data.Entity;

namespace Shift.Entities
{
    public class BGProcess : DbContext
    {
        public BGProcess(string connectionName) : base(connectionName)
        {
            Database.SetInitializer<BGProcess>(null);
        }

        public DbSet<JobView> JobView { get; set; }

        public DbSet<JobResult> JobResult { get; set; }
    }
}
