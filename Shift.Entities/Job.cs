using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Shift.Entities
{
    [Table("Job")] //Stop EF from using "Jobs" table
    public partial class Job
    {
        [Key]
        public int JobID { get; set; } //PrimaryKey

        public string AppID { get; set; }
        public int? UserID { get; set; } 
        public int? ProcessID { get; set; }
        public string JobType { get; set; }
        public string JobName { get; set; }
        public string InvokeMeta { get; set; }
        public string Parameters { get; set; } //always encrypted
        public string Command { get; set; }
        public JobStatus? Status { get; set; }
        public string Error { get; set; }

        public DateTime? Start { get; set; }
        public DateTime? End { get; set; }

        public DateTime? Created { get; set; }

        [NotMapped]
        public string DecryptedParameters
        {
            get
            {
                return Helpers.Decrypt(Parameters);
            }
        }

        [Editable(false)]
        public string StatusLabel { get { return Status.ToString(); } }
    }


}
