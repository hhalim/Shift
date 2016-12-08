using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Shift.Entities
{
    [Table("JobResult")]
    public class JobResult
    {
        [Key]
        public int JobResultID {get; set; } //PrimaryKey do not populate directly
        public int JobID { get; set; }
        public string ExternalID { get; set; }
        public string Name { get; set; }
        public byte[] BinaryContent { get; set; }
        public string ContentType { get; set; }

    }
}
