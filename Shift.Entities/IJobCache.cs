using Shift.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift.Entities
{
    public interface IJobCache
    {
        JobStatusProgress GetCachedProgress(int jobID);
        void SetCachedProgress(int jobID, int? percent, string note, string data);
        void SetCachedProgressStatus(JobStatusProgress jsProgress, JobStatus status);
        void SetCachedProgressError(JobStatusProgress jsProgress, string error);
        void DeleteCachedProgress(int jobID);

    }
}
