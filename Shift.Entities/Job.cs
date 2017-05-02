using Newtonsoft.Json;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Shift.Entities
{
    [Table("Job")] //Stop EF from using "Jobs" table
    public partial class Job
    {
        [Key]
        [JsonProperty(PropertyName = "id")]//PrimaryKey for DocumentDB
        public string JobID { get; set; } //PrimaryKey for SQL, Redis, MongoDB

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

        public long? Score { get; set; } //for sorting run-now

        [Editable(false)]
        public string StatusLabel { get { return Status.ToString(); } }
    }


}
