using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Shift.Entities;
using Shift.DataLayer;
using Autofac;
using Autofac.Features.ResolveAnything;
using System.Threading.Tasks;

[assembly: CLSCompliant(true)]
namespace Shift
{
    public class JobClient
    {
        private IJobDAL jobDAL = null;
        private readonly ContainerBuilder builder;
        private readonly IContainer container;

        ///<summary>
        /// Initializes a new instance of JobClient class, injects data layer with connection and configuration strings.
        /// Only three options are used for the client:
        /// * DBConnectionString
        /// * UseCache
        /// * CacheConfigurationString
        /// * EncryptionKey (optional)
        /// 
        /// If UseCache is true, the CacheConfigurationString is required, if false, then it is optional.
        ///</summary>
        ///<param name="config">Setup the database connection string, cache configuration.</param>
        ///
        public JobClient(ClientConfig config)
        {
            if (config == null)
            {
                throw new ArgumentNullException("Unable to start with no configuration.");
            }

            if (string.IsNullOrWhiteSpace(config.StorageMode))
            {
                throw new ArgumentNullException("The storage mode must not be empty.");

            }

            if (string.IsNullOrWhiteSpace(config.DBConnectionString))
            {
                throw new ArgumentNullException("Unable to run without DB storage connection string.");

            }

            if (config.UseCache && string.IsNullOrWhiteSpace(config.CacheConfigurationString))
            {
                throw new ArgumentNullException("Unable to run without Cache configuration string.");
            }

            builder = new ContainerBuilder();
            builder.RegisterSource(new AnyConcreteTypeNotAlreadyRegisteredSource());
            RegisterAssembly.RegisterTypes(builder, config.StorageMode, config.DBConnectionString, config.UseCache, config.CacheConfigurationString, config.EncryptionKey, config.DBAuthKey);
            container = builder.Build();

            jobDAL = container.Resolve<IJobDAL>();
        }

        public JobClient(IJobDAL jobDAL)
        {
            this.jobDAL = jobDAL;
        }

        #region Clients access
        //Provides the clients to submit jobs or commands for jobs

        /// <summary>
        /// Add a method and parameters into the job table.
        /// Ref and out parameters are not supported.
        /// </summary>
        /// <param name="methodCall">Expression body for method call </param>
        /// <returns>JobID of the added job.</returns>
        public string Add(Expression<Action> methodCall)
        {
            return jobDAL.Add(null, null, null, null, methodCall);
        }

        public Task<string> AddAsync(Expression<Action> methodCall)
        {
            return jobDAL.AddAsync(null, null, null, null, methodCall);
        }

        /// <summary>
        /// Add a method and parameters into the job table.
        /// Ref and out parameters are not supported.
        /// </summary>
        /// <param name="appID">Client application ID</param>
        /// <param name="methodCall">Expression body for method call </param>
        /// <returns>JobID of the added job.</returns>
        public string Add(string appID, Expression<Action> methodCall)
        {
            return jobDAL.Add(appID, null, null, null, methodCall);
        }

        public Task<string> AddAsync(string appID, Expression<Action> methodCall)
        {
            return jobDAL.AddAsync(appID, null, null, null, methodCall);
        }

        /// <summary>
        /// Add a method and parameters into the job table.
        /// Job name defaults to class.method name.
        /// Ref and out parameters are not supported.
        /// </summary>
        /// <param name="appID">Client application ID</param>
        /// <param name="userID">User ID</param>
        /// <param name="jobType">Job type category/group</param>
        /// <param name="methodCall">Expression body for method call </param>
        /// <returns>JobID of the added job.</returns>
        public string Add(string appID, string userID, string jobType, Expression<Action> methodCall)
        {
            return jobDAL.Add(appID, userID, jobType, null, methodCall);
        }

        public Task<string> AddAsync(string appID, string userID, string jobType, Expression<Action> methodCall)
        {
            return jobDAL.AddAsync(appID, userID, jobType, null, methodCall);
        }

        /// <summary>
        /// Add a method and parameters into the job table with a custom name.
        /// Ref and out parameters are not supported.
        /// </summary>
        /// <param name="appID">Client application ID</param>
        /// <param name="userID">User ID</param>
        /// <param name="jobType">Job type category/group</param>
        /// <param name="jobName">Name for this job</param>
        /// <param name="methodCall">Expression body for method call </param>
        /// <returns>JobID of the added job.</returns>
        public string Add(string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            return jobDAL.Add(appID, userID, jobType, jobName, methodCall);
        }

        public Task<string> AddAsync(string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            return jobDAL.AddAsync(appID, userID, jobType, jobName, methodCall);
        }

        /// <summary>
        /// Update a job's method and parameters.
        /// Ref and out parameters are not supported.
        /// </summary>
        /// <param name="jobID">Existing job ID</param>
        /// <param name="methodCall">Expression body for method call </param>
        /// <returns>Number of successfully updated job</returns>
        public int Update(string jobID, Expression<Action> methodCall)
        {
            return jobDAL.Update(jobID, null, null, null, null, methodCall);
        }

        public Task<int> UpdateAsync(string jobID, Expression<Action> methodCall)
        {
            return jobDAL.UpdateAsync(jobID, null, null, null, null, methodCall);
        }

        /// <summary>
        /// Update a job's method and parameters.
        /// Ref and out parameters are not supported.
        /// </summary>
        /// <param name="jobID">Existing job ID</param>
        /// <param name="appID">Client application ID</param>
        /// <param name="methodCall">Expression body for method call </param>
        /// <returns>Number of successfully updated job</returns>
        public int Update(string jobID, string appID, Expression<Action> methodCall)
        {
            return jobDAL.Update(jobID, appID, null, null, null, methodCall);
        }

        public Task<int> UpdateAsync(string jobID, string appID, Expression<Action> methodCall)
        {
            return jobDAL.UpdateAsync(jobID, appID, null, null, null, methodCall);
        }

        /// <summary>
        /// Update a job's method and parameters.
        /// Ref and out parameters are not supported.
        /// </summary>
        /// <param name="jobID">Existing job ID</param>
        /// <param name="appID">Client application ID</param>
        /// <param name="userID">User ID</param>
        /// <param name="jobType">Job type category/group</param>
        /// <param name="jobName">Name for this job</param>
        /// <param name="methodCall">Expression body for method call </param>
        /// <returns>Number of successfully updated job</returns>
        public int Update(string jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            return jobDAL.Update(jobID, appID, userID, jobType, jobName, methodCall);
        }

        public Task<int> UpdateAsync(string jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            return jobDAL.UpdateAsync(jobID, appID, userID, jobType, jobName, methodCall);
        }

        ///<summary>
        /// Sets "stop" command to already running or not running jobs.
        ///</summary>
        ///<returns>Number of affected jobs.</returns>
        public int SetCommandStop(IList<string> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return 0;

            return jobDAL.SetCommandStop(jobIDs);
        }

        public Task<int> SetCommandStopAsync(IList<string> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return Task.FromResult(0);

            return jobDAL.SetCommandStopAsync(jobIDs);
        }

        ///<summary>
        /// Sets "run-now" command to not running jobs.
        ///</summary>
        ///<returns>Number of affected jobs.</returns>
        public int SetCommandRunNow(IList<string> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return 0;

            return jobDAL.SetCommandRunNow(jobIDs);
        }

        public Task<int> SetCommandRunNowAsync(IList<string> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return Task.FromResult(0);

            return jobDAL.SetCommandRunNowAsync(jobIDs);
        }

        ///<summary>
        /// Gets the job instance based on a unique job ID.
        ///</summary>
        ///<param name="jobID"></param>
        ///<returns>Job</returns>
        public Job GetJob(string jobID)
        {
            return jobDAL.GetJob(jobID);
        }

        public Task<Job> GetJobAsync(string jobID)
        {
            return jobDAL.GetJobAsync(jobID);
        }

        ///<summary>
        /// Gets the job instance with progress info based on a unique job ID.
        ///</summary>
        ///<param name="jobID"></param>
        ///<returns>JobView</returns>
        public JobView GetJobView(string jobID)
        {
            return jobDAL.GetJobView(jobID);
        }

        public Task<JobView> GetJobViewAsync(string jobID)
        {
            return jobDAL.GetJobViewAsync(jobID);
        }

        /// <summary>
        /// Return job views based on page index and page size.
        /// </summary>
        /// <param name="pageIndex">Page index</param>
        /// <param name="pageSize">Page size</param>
        /// <returns>Total count of job views and list of JobViews</returns>
        public JobViewList GetJobViews(int? pageIndex, int? pageSize)
        {
            return jobDAL.GetJobViews(pageIndex, pageSize);
        }

        public Task<JobViewList> GetJobViewsAsync(int? pageIndex, int? pageSize)
        {
            return jobDAL.GetJobViewsAsync(pageIndex, pageSize);
        }

        ///<summary>
        /// Resets non running jobs. Jobs will be ready for another run after a successful reset.
        ///</summary>
        ///<param name="jobIDs">Job IDs collection.</param>
        ///<returns>Number of affected jobs.</returns>
        public int ResetJobs(IList<string> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return 0;

            jobDAL.DeleteCachedProgressAsync(jobIDs);
            return jobDAL.Reset(jobIDs);
        }

        public Task<int> ResetJobsAsync(IList<string> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return Task.FromResult(0);

            jobDAL.DeleteCachedProgressAsync(jobIDs);
            return jobDAL.ResetAsync(jobIDs);
        }

        ///<summary>
        /// Deletes non running jobs. 
        ///</summary>
        ///<param name="jobIDs">Job IDs collection.</param>
        ///<returns>Number of affected jobs.</returns>
        public int DeleteJobs(IList<string> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return 0;

            jobDAL.DeleteCachedProgressAsync(jobIDs);
            return jobDAL.Delete(jobIDs);
        }

        public Task<int> DeleteJobsAsync(IList<string> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return Task.FromResult(0);

            jobDAL.DeleteCachedProgressAsync(jobIDs);
            return jobDAL.DeleteAsync(jobIDs);
        }

        ///<summary>
        /// Return counts of all job statuses (running, not running, completed, stopped, with errors).
        /// Useful for UI reporting of job statuses.
        ///</summary>
        ///<param name="appID">Client application ID, optional</param>
        ///<param name="userID">User ID, optional.</param>
        ///<returns>Collection of JobStatusCount</returns>
        public IReadOnlyCollection<JobStatusCount> GetJobStatusCount(string appID, string userID)
        {
            return jobDAL.GetJobStatusCount(appID, userID);
        }

        public Task<IReadOnlyCollection<JobStatusCount>> GetJobStatusCountAsync(string appID, string userID)
        {
            return jobDAL.GetJobStatusCountAsync(appID, userID);
        }

        ///<summary>
        /// Gets the current progress of job.
        /// Try to retrieve progress from cache first and then from DB if not available.
        ///</summary>
        ///<param name="jobID"></param>
        ///<returns>JobStatusProgress</returns>
        public JobStatusProgress GetProgress(string jobID)
        {
            return jobDAL.GetProgress(jobID);
        }

        public Task<JobStatusProgress> GetProgressAsync(string jobID)
        {
            return jobDAL.GetProgressAsync(jobID);
        }

        ///<summary>
        /// Gets the current progress of job from cache only.
        ///</summary>
        ///<param name="jobID"></param>
        ///<returns>JobStatusProgress</returns>
        public JobStatusProgress GetCachedProgress(string jobID)
        {
            return jobDAL.GetCachedProgress(jobID);
        }

        public Task<JobStatusProgress> GetCachedProgressAsync(string jobID)
        {
            return jobDAL.GetCachedProgressAsync(jobID);
        }
        #endregion



    }
}
