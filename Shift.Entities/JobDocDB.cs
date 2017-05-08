using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift.Entities
{
    public class JobDocDB
    {
        public string id { get; set; } //PrimaryKey

        private Job _job;
        Job Job
        {
            get {
                if (string.IsNullOrWhiteSpace(_job.JobID) && !string.IsNullOrWhiteSpace(id))
                    _job.JobID = id;
                return _job;
            }
            set
            {
                _job = value;
            }
        }
    }
}
