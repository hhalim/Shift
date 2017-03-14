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
        int SetCommandStop(IList<int> jobIDs);
        int SetCommandRunNow(IList<int> jobIDs);
        #endregion

        #region Direct Action to Jobs
        int Reset(IList<int> jobIDs);
        int Delete(IList<int> jobIDs);
        int Delete(int hour, IList<JobStatus?> statusList);
        Task<int> DeleteAsync(int hour, IList<JobStatus?> statusList);
        int SetToStopped(IList<int> jobIDs);
        IList<JobStatusCount> GetJobStatusCount(string appID, string userID);
        #endregion

        #region Various ways to get Jobs
        Job GetJob(int jobID);
        JobView GetJobView(int jobID);
        IList<Job> GetJobs(IEnumerable<int> jobIDs);
        IList<Job> GetNonRunningJobsByIDs(IEnumerable<int> jobIDs);
        IList<int> GetJobIdsByProcessAndCommand(string processID, string command);
        IList<Job> GetJobsByProcessAndStatus(string processID, JobStatus status);
        IList<Job> GetJobsByProcess(string processID, IEnumerable<int> jobIDs);
        JobViewList GetJobViews(int? pageIndex, int? pageSize);
        #endregion

        #region ManageJobs by Server
        int SetToRunning(int jobID);
        int SetError(int jobID, string error);
        int SetCompleted(int jobID);
        int CountRunningJobs(string processID);
        IList<Job> ClaimJobsToRun(string processID, int maxNum);
        IList<Job> ClaimJobsToRun(string processID, IEnumerable<Job> jobList);
        int SetProgress(int jobID, int? percent, string note, string data);
        int UpdateProgress(int jobID, int? percent, string note, string data);
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
