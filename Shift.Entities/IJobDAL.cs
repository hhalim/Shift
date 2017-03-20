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
        int? Add(string appID, string userID, string jobType, string jobName, Expression<Action> methodCall);
        int Update(int jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall);
        #endregion

        #region Set Command field
        int SetCommandStop(ICollection<int> jobIDs);
        int SetCommandRunNow(ICollection<int> jobIDs);
        #endregion

        #region Direct Action to Jobs
        int Reset(ICollection<int> jobIDs);
        int Delete(ICollection<int> jobIDs);
        int Delete(int hour, ICollection<JobStatus?> statusList);
        Task<int> DeleteAsync(int hour, ICollection<JobStatus?> statusList);
        int SetToStopped(ICollection<int> jobIDs);
        IReadOnlyCollection<JobStatusCount> GetJobStatusCount(string appID, string userID);
        #endregion

        #region Various ways to get Jobs
        Job GetJob(int jobID);
        JobView GetJobView(int jobID);
        IReadOnlyCollection<Job> GetJobs(IEnumerable<int> jobIDs);
        IReadOnlyCollection<Job> GetNonRunningJobsByIDs(IEnumerable<int> jobIDs);
        IReadOnlyCollection<int> GetJobIdsByProcessAndCommand(string processID, string command);
        IReadOnlyCollection<Job> GetJobsByProcessAndStatus(string processID, JobStatus status);
        JobViewList GetJobViews(int? pageIndex, int? pageSize);
        #endregion

        #region ManageJobs by Server
        int SetToRunning(string processID, int jobID);
        int SetError(string processID, int jobID, string error);
        int SetCompleted(string processID, int jobID);
        int CountRunningJobs(string processID);
        IReadOnlyCollection<Job> ClaimJobsToRun(string processID, int maxNum);
        IReadOnlyCollection<Job> ClaimJobsToRun(string processID, ICollection<Job> jobList);
        int SetProgress(int jobID, int? percent, string note, string data);
        Task<int> UpdateProgressAsync(int jobID, int? percent, string note, string data);
        #endregion


        #region Cache
        JobStatusProgress GetProgress(int jobID);
        JobStatusProgress GetCachedProgress(int jobID);
        void SetCachedProgress(int jobID, int? percent, string note, string data);
        void SetCachedProgressStatus(int jobID, JobStatus status);
        void SetCachedProgressStatus(IEnumerable<int> jobIDs, JobStatus status);
        void SetCachedProgressError(int jobID, string error);
        void DeleteCachedProgress(int jobID);
        void DeleteCachedProgress(IEnumerable<int> jobIDs);
        #endregion
    }
}
