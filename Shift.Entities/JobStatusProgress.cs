using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Shift.Entities
{
    public class JobStatusProgress
    {
        [Key]
        public string JobID { get; set; } //PrimaryKey

        public int? Percent { get; set; }
        public string Note { get; set; }
        public string Data { get; set; }

        public JobStatus? Status { get; set; }
        public string Error { get; set; }

        public DateTime? Updated { get; set; }
        public bool ExistsInDB { get; set; }

        [NotMapped]
        [Editable(false)]
        public string StatusLabel { get { return Status.ToString(); } }

        public JobStatusProgress ()
        {
            this.ExistsInDB = true;
        }
    }
}
