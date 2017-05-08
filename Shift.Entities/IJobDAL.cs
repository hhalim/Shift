using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;

namespace Shift.Entities
{
    public interface IJobDAL
    {
        #region insert/update job
        string Add(string appID, string userID, string jobType, string jobName, Expression<Action> methodCall);
        Task<string> AddAsync(string appID, string userID, string jobType, string jobName, Expression<Action> methodCall);
        int Update(string jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall);
        Task<int> UpdateAsync(string jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall);
        #endregion

        #region Set Command field
        int SetCommandStop(ICollection<string> jobIDs);
        Task<int> SetCommandStopAsync(ICollection<string> jobIDs);
        int SetCommandRunNow(ICollection<string> jobIDs);
        Task<int> SetCommandRunNowAsync(ICollection<string> jobIDs);
        #endregion

        #region Direct Action to Jobs
        int Reset(ICollection<string> jobIDs);
        Task<int> ResetAsync(ICollection<string> jobIDs);
        int Delete(ICollection<string> jobIDs);
        Task<int> DeleteAsync(ICollection<string> jobIDs);
        int Delete(int hour, ICollection<JobStatus?> statusList);
        Task<int> DeleteAsync(int hour, ICollection<JobStatus?> statusList);
        int SetToStopped(ICollection<string> jobIDs);
        Task<int> SetToStoppedAsync(ICollection<string> jobIDs);
        IReadOnlyCollection<JobStatusCount> GetJobStatusCount(string appID, string userID);
        Task<IReadOnlyCollection<JobStatusCount>> GetJobStatusCountAsync(string appID, string userID);
        #endregion

        #region Various ways to get Jobs
        Job GetJob(string jobID);
        Task<Job> GetJobAsync(string jobID);
        JobView GetJobView(string jobID);
        Task<JobView> GetJobViewAsync(string jobID);
        IReadOnlyCollection<Job> GetJobs(IEnumerable<string> jobIDs);
        Task<IReadOnlyCollection<Job>> GetJobsAsync(IEnumerable<string> jobIDs);
        IReadOnlyCollection<Job> GetNonRunningJobsByIDs(IEnumerable<string> jobIDs);
        Task<IReadOnlyCollection<Job>> GetNonRunningJobsByIDsAsync(IEnumerable<string> jobIDs);
        IReadOnlyCollection<string> GetJobIdsByProcessAndCommand(string processID, string command);
        Task<IReadOnlyCollection<string>> GetJobIdsByProcessAndCommandAsync(string processID, string command);
        IReadOnlyCollection<Job> GetJobsByProcessAndStatus(string processID, JobStatus status);
        Task<IReadOnlyCollection<Job>> GetJobsByProcessAndStatusAsync(string processID, JobStatus status);
        JobViewList GetJobViews(int? pageIndex, int? pageSize);
        Task<JobViewList> GetJobViewsAsync(int? pageIndex, int? pageSize);
        #endregion

        #region ManageJobs by Server
        int SetToRunning(string processID, string jobID);
        Task<int> SetToRunningAsync(string processID, string jobID);
        int SetError(string processID, string jobID, string error);
        Task<int> SetErrorAsync(string processID, string jobID, string error);
        int SetCompleted(string processID, string jobID);
        Task<int> SetCompletedAsync(string processID, string jobID);
        int CountRunningJobs(string processID);
        Task<int> CountRunningJobsAsync(string processID);
        IReadOnlyCollection<Job> ClaimJobsToRun(string processID, int maxNum);
        Task<IReadOnlyCollection<Job>> ClaimJobsToRunAsync(string processID, int maxNum);
        IReadOnlyCollection<Job> ClaimJobsToRun(string processID, ICollection<Job> jobList);
        Task<IReadOnlyCollection<Job>> ClaimJobsToRunAsync(string processID, ICollection<Job> jobList);
        int SetProgress(string jobID, int? percent, string note, string data);
        Task<int> SetProgressAsync(string jobID, int? percent, string note, string data);
        Task<int> UpdateProgressAsync(string jobID, int? percent, string note, string data);
        #endregion

        #region Cache
        JobStatusProgress GetProgress(string jobID);
        Task<JobStatusProgress> GetProgressAsync(string jobID); //All cached progress are async, except this one that touched DB
        JobStatusProgress GetCachedProgress(string jobID);
        Task<JobStatusProgress> GetCachedProgressAsync(string jobID);
        Task SetCachedProgressAsync(string jobID, int? percent, string note, string data);
        Task SetCachedProgressStatusAsync(string jobID, JobStatus status);
        Task SetCachedProgressStatusAsync(IEnumerable<string> jobIDs, JobStatus status);
        Task SetCachedProgressErrorAsync(string jobID, string error);
        Task DeleteCachedProgressAsync(string jobID);
        Task DeleteCachedProgressAsync(IEnumerable<string> jobIDs);
        #endregion
    }
}
