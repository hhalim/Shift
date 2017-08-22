using Shift.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shift.UnitTest
{
    public class TestJob
    {
        public void Start(string value, IProgress<ProgressInfo> progress, CancellationToken cancelToken, PauseToken pauseToken)
        {
            var total = 10;

            var note = "";
            for (var i = 0; i < total; i++)
            {
                if (cancelToken.IsCancellationRequested)
                {
                    cancelToken.ThrowIfCancellationRequested(); //throw OperationCanceledException
                }

                pauseToken.WaitWhilePausedAsync().GetAwaiter().GetResult();

                note += i + " - " + value + "<br/> \n";

                var pInfo = new ProgressInfo();
                pInfo.Percent = (int)Math.Round(((i + 1) / (double)total) * 100.00, MidpointRounding.AwayFromZero); ;
                pInfo.Note = note;
                if (progress != null)
                    progress.Report(pInfo);

                Thread.Sleep(1000);
            }

            return;
        }
    }
}
