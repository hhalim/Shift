using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift.Entities
{
    public class JobViewList
    {
        public long Total { get; set; }
        public ICollection<JobView> Items { get; set; } 
    }
}
