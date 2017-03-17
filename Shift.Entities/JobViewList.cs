using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift.Entities
{
    public class JobViewList
    {
        public int Total { get; set; }
        public IList<JobView> Items { get; set; } 
    }
}
