using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;


namespace Shift.Entities
{
    public class JobStatusCount
    {
        public JobStatus? Status { get; set; }
        public int Count { get; set; }
        public int NullCount { get; set; }

        [Editable(false)]
        public string StatusLabel { get { return Status.ToString(); } }
    }
}
