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
        JobStatusProgress GetCachedProgress(string jobID);
        Task<JobStatusProgress> GetCachedProgressAsync(string jobID);
        Task SetCachedProgressAsync(string jobID, int? percent, string note, string data);
        Task SetCachedProgressStatusAsync(JobStatusProgress jsProgress, JobStatus status);
        Task SetCachedProgressErrorAsync(JobStatusProgress jsProgress, string error);
        Task DeleteCachedProgressAsync(string jobID);
    }
}
