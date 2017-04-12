using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Shift.Entities
{
    [Table("JobView")] 
    public partial class JobView
    {

        [Key]
        public string JobID { get; set; } //PrimaryKey

        public string AppID { get; set; }
        public string UserID { get; set; }
        public string ProcessID { get; set; }
        public string JobType { get; set; }
        public string JobName { get; set; }
        public string InvokeMeta { get; set; }
        public string Parameters { get; set; } 
        public string Command { get; set; }
        public JobStatus? Status { get; set; }
        public string Error { get; set; }

        public DateTime? Start { get; set; }
        public DateTime? End { get; set; }

        public DateTime? Created { get; set; }

        public int? Percent { get; set; }
        public string Note { get; set; }
        public string Data { get; set; }

        public long? Score { get; set; } //for sorting run-now

        [NotMapped]
        [Editable(false)]
        public string StatusLabel { get { return Status.ToString(); } }
    }


}
