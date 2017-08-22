using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shift.Entities
{
    public class TaskInfo
    {
        public Task JobTask { get; set; }
        public CancellationTokenSource CancelSource { get; set; }
        public PauseTokenSource PauseSource { get; set; }
    }

}
