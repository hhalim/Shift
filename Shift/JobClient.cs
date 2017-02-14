using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Shift.Entities;
using Shift.DataLayer;
using Autofac;
using Autofac.Features.ResolveAnything;

[assembly: CLSCompliant(true)]
namespace Shift
{
    public class JobClient
    {
        private JobDAL jobDAL = null;
        public ClientConfig config = null;
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
                throw new Exception("Unable to start with no configuration.");
            }

            if (string.IsNullOrWhiteSpace(config.DBConnectionString))
            {
                throw new Exception("Error: unable to start without DB connection string.");

            }

            if (config.UseCache && string.IsNullOrWhiteSpace(config.CacheConfigurationString))
            {
                throw new Exception("Error: unable to start without Cache configuration string.");
            }

            this.config = config;

            builder = new ContainerBuilder();
            builder.RegisterSource(new AnyConcreteTypeNotAlreadyRegisteredSource());
            RegisterAssembly.RegisterTypes(builder, config.DBConnectionString, config.UseCache, config.CacheConfigurationString, config.EncryptionKey);
            container = builder.Build();
            jobDAL = container.Resolve<JobDAL>();
        }

        #region Clients access
        //Provides the clients to submit jobs or commands for jobs

        /// <summary>
        /// Add a method and parameters into the job table.
        /// Ref and out parameters are not supported.
        /// </summary>
        /// <param name="methodCall">Expression body for method call </param>
        /// <returns>JobID of the added job.</returns>
        public int? Add(Expression<Action> methodCall)
        {
            return jobDAL.Add(null, null, null, null, methodCall);
        }

        /// <summary>
        /// Add a method and parameters into the job table.
        /// Ref and out parameters are not supported.
        /// </summary>
        /// <param name="appID">Client application ID</param>
        /// <param name="methodCall">Expression body for method call </param>
        /// <returns>JobID of the added job.</returns>
        public int? Add(string appID, Expression<Action> methodCall)
        {
            return jobDAL.Add(appID, null, null, null, methodCall);
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
        public int? Add(string appID, string userID, string jobType, Expression<Action> methodCall)
        {
            return jobDAL.Add(appID, userID, jobType, null, methodCall);
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
        public int? Add(string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            return jobDAL.Add(appID, userID, jobType, jobName, methodCall);
        }

        /// <summary>
        /// Update a job's method and parameters.
        /// Ref and out parameters are not supported.
        /// </summary>
        /// <param name="jobID">Existing job ID</param>
        /// <param name="methodCall">Expression body for method call </param>
        /// <returns>Number of successfully updated job</returns>
        public int Update(int jobID, Expression<Action> methodCall)
        {
            return jobDAL.Update(jobID, null, null, null, null, methodCall);
        }

        /// <summary>
        /// Update a job's method and parameters.
        /// Ref and out parameters are not supported.
        /// </summary>
        /// <param name="jobID">Existing job ID</param>
        /// <param name="appID">Client application ID</param>
        /// <param name="methodCall">Expression body for method call </param>
        /// <returns>Number of successfully updated job</returns>
        public int? Update(int jobID, string appID, Expression<Action> methodCall)
        {
            return jobDAL.Update(jobID, appID, null, null, null, methodCall);
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
        public int Update(int jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            return jobDAL.Update(jobID, appID, userID, jobType, jobName, methodCall);
        }

        ///<summary>
        /// Sets "stop" command to already running or not running jobs.
        ///</summary>
        ///<returns>Number of affected jobs.</returns>
        public int SetCommandStop(IList<int> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return 0;

            return jobDAL.SetCommandStop(jobIDs);
        }

        ///<summary>
        /// Sets "stop-delete" command to already running or not running jobs.
        ///</summary>
        ///<returns>Number of affected jobs.</returns>
        public int SetCommandStopDelete(IList<int> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return 0;

            return jobDAL.SetCommandStopDelete(jobIDs);
        }

        ///<summary>
        /// Sets "run-now" command to not running jobs.
        ///</summary>
        ///<returns>Number of affected jobs.</returns>
        public int SetCommandRunNow(IList<int> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return 0;

            return jobDAL.SetCommandRunNow(jobIDs);
        }

        ///<summary>
        /// Gets the job instance based on a unique job ID.
        ///</summary>
        ///<param name="jobID"></param>
        ///<returns>Job</returns>
        public Job GetJob(int jobID)
        {
            return jobDAL.GetJob(jobID);
        }

        ///<summary>
        /// Gets the job instance with progress info based on a unique job ID.
        ///</summary>
        ///<param name="jobID"></param>
        ///<returns>JobView</returns>
        public JobView GetJobView(int jobID)
        {
            return jobDAL.GetJobView(jobID);
        }

        ///<summary>
        /// Resets non running jobs. Jobs will be ready for another run after a successful reset.
        ///</summary>
        ///<param name="jobIDs">Job IDs collection.</param>
        ///<returns>Number of affected jobs.</returns>
        public int ResetJobs(IList<int> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return 0;

            jobDAL.DeleteCachedProgress(jobIDs);
            return jobDAL.Reset(jobIDs);
        }

        ///<summary>
        /// Deletes non running jobs. 
        ///</summary>
        ///<param name="jobIDs">Job IDs collection.</param>
        ///<returns>Number of affected jobs.</returns>
        public int DeleteJobs(IList<int> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return 0;

            jobDAL.DeleteCachedProgress(jobIDs);
            return jobDAL.Delete(jobIDs);
        }

        ///<summary>
        /// Return counts of all job statuses (running, not running, completed, stopped, with errors).
        /// Useful for UI reporting of job statuses.
        ///</summary>
        ///<param name="appID">Client application ID, optional</param>
        ///<param name="userID">User ID, optional.</param>
        ///<returns>Collection of JobStatusCount</returns>
        public IList<JobStatusCount> GetJobStatusCount(string appID, string userID)
        {
            return jobDAL.GetJobStatusCount(appID, userID);
        }

        ///<summary>
        /// Gets the current progress of job.
        /// Try to retrieve progress from cache first and then from DB if not available.
        ///</summary>
        ///<param name="jobID"></param>
        ///<returns>JobStatusProgress</returns>
        public JobStatusProgress GetProgress(int jobID)
        {
            return jobDAL.GetProgress(jobID);
        }

        ///<summary>
        /// Gets the current progress of job from cache only.
        ///</summary>
        ///<param name="jobID"></param>
        ///<returns>JobStatusProgress</returns>
        public JobStatusProgress GetCachedProgress(int jobID)
        {
            return jobDAL.GetCachedProgress(jobID);
        }
        #endregion



    }
}
