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
        int Update(string jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall);
        #endregion

        #region Set Command field
        int SetCommandStop(ICollection<string> jobIDs);
        int SetCommandRunNow(ICollection<string> jobIDs);
        #endregion

        #region Direct Action to Jobs
        int Reset(ICollection<string> jobIDs);
        int Delete(ICollection<string> jobIDs);
        int Delete(int hour, ICollection<JobStatus?> statusList);
        Task<int> DeleteAsync(int hour, ICollection<JobStatus?> statusList);
        int SetToStopped(ICollection<string> jobIDs);
        IReadOnlyCollection<JobStatusCount> GetJobStatusCount(string appID, string userID);
        #endregion

        #region Various ways to get Jobs
        Job GetJob(string jobID);
        JobView GetJobView(string jobID);
        IReadOnlyCollection<Job> GetJobs(IEnumerable<string> jobIDs);
        IReadOnlyCollection<Job> GetNonRunningJobsByIDs(IEnumerable<string> jobIDs);
        IReadOnlyCollection<string> GetJobIdsByProcessAndCommand(string processID, string command);
        IReadOnlyCollection<Job> GetJobsByProcessAndStatus(string processID, JobStatus status);
        JobViewList GetJobViews(int? pageIndex, int? pageSize);
        #endregion

        #region ManageJobs by Server
        int SetToRunning(string processID, string jobID);
        int SetError(string processID, string jobID, string error);
        int SetCompleted(string processID, string jobID);
        int CountRunningJobs(string processID);
        IReadOnlyCollection<Job> ClaimJobsToRun(string processID, int maxNum);
        IReadOnlyCollection<Job> ClaimJobsToRun(string processID, ICollection<Job> jobList);
        int SetProgress(string jobID, int? percent, string note, string data);
        Task<int> UpdateProgressAsync(string jobID, int? percent, string note, string data);
        #endregion


        #region Cache
        JobStatusProgress GetProgress(string jobID);
        JobStatusProgress GetCachedProgress(string jobID);
        void SetCachedProgress(string jobID, int? percent, string note, string data);
        void SetCachedProgressStatus(string jobID, JobStatus status);
        void SetCachedProgressStatus(IEnumerable<string> jobIDs, JobStatus status);
        void SetCachedProgressError(string jobID, string error);
        void DeleteCachedProgress(string jobID);
        void DeleteCachedProgress(IEnumerable<string> jobIDs);
        #endregion
    }
}
