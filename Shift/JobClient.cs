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
        private Options options = null;
        private readonly ContainerBuilder builder;
        private readonly IContainer container;

        ///<summary>
        /// Initializes a new instance of JobClient class, injects data layer with connection and configuration strings.
        /// Only three options are used for the client:
        /// * DBConnectionString
        /// * CacheConfigurationString
        /// * EncryptionKey (optional)
        ///</summary>
        ///<param name="options">Setup the database connection string, cache configuration.</param>
        ///
        public JobClient(Options options)
        {
            if (options == null)
            {
                throw new Exception("Unable to start with no options.");
            }

            if (string.IsNullOrWhiteSpace(options.DBConnectionString))
            {
                throw new Exception("Error: unable to start without DB connection string.");

            }

            if (string.IsNullOrWhiteSpace(options.CacheConfigurationString))
            {
                throw new Exception("Error: unable to start without Cache configuration string.");

            }

            this.options = options;

            builder = new ContainerBuilder();
            builder.RegisterSource(new AnyConcreteTypeNotAlreadyRegisteredSource());
            Register.RegisterTypes(builder, options);
            container = builder.Build();
            jobDAL = container.Resolve<JobDAL>();
        }

        private static class Register
        {
            public static void RegisterTypes(ContainerBuilder builder, Options options)
            {
                builder.RegisterType<DataLayer.Redis.Cache>().As<IJobCache>().WithParameter("configurationString", options.CacheConfigurationString);
                var parameters = Helpers.GenerateNamedParameters(new Dictionary<string, object> { { "connectionString", options.DBConnectionString }, { "encryptionKey", options.EncryptionKey } });
                builder.RegisterType<JobDAL>().As<JobDAL>().WithParameters(parameters);
            }
        }

        #region Clients access
        //Provides the clients to submit jobs or commands for jobs

        /// <summary>
        /// Add a method and parameters into the job table.
        /// Job name defaults to class.method name.
        /// Reference parameters and out (ref and out) are not supported.
        /// </summary>
        /// <paramref name="methodCall"/> expression body 
        /// <returns>The jobID of the added job.</returns>
        public int? Add(string appID, int userID, string jobType, Expression<Action> methodCall)
        {
            return jobDAL.Add(appID, userID, jobType, null, methodCall);
        }

        /// <summary>
        /// Add a method and parameters into the job table with a custom name.
        /// Job name defaults to class.method name.
        /// Reference parameters and out (ref and out) are not supported.
        /// </summary>
        /// <paramref name="methodCall"/> expression body 
        /// <returns>The jobID of the added job.</returns>
        public int? Add(string appID, int userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            return jobDAL.Add(appID, userID, jobType, jobName, methodCall);
        }

        ///<summary>
        /// Sets "STOP" command to already running or not running jobs.
        ///</summary>
        ///<returns>Number of affected jobs.</returns>
        public int SetCommandStop(List<int> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return 0;

            return jobDAL.SetCommandStop(jobIDs);
        }

        ///<summary>
        /// Sets "STOPDELETE" command to already running or not running jobs.
        ///</summary>
        ///<returns>Number of affected jobs.</returns>
        public int SetCommandStopDelete(List<int> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return 0;

            return jobDAL.SetCommandStopDelete(jobIDs);
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
        ///<param name="jobIDs">List of job IDs.</param>
        ///<returns>Number of affected jobs.</returns>
        public int ResetJobs(List<int> jobIDs)
        {
            jobDAL.DeleteCachedProgress(jobIDs);
            return jobDAL.Reset(jobIDs);
        }

        ///<summary>
        /// Deletes non running jobs. 
        ///</summary>
        ///<param name="jobIDs">List of job IDs.</param>
        ///<returns>Number of affected jobs.</returns>
        public int DeleteJobs(List<int> jobIDs)
        {
            jobDAL.DeleteCachedProgress(jobIDs);
            return jobDAL.Delete(jobIDs);
        }

        ///<summary>
        /// Gets the job result instance that contains a blob binary data.
        ///</summary>
        ///<param name="jobResultID"></param>
        ///<returns>JobResult</returns>
        public JobResult GetJobResult(int jobResultID)
        {
            return jobDAL.GetJobResult(jobResultID);
        }

        ///<summary>
        /// Gets the job result instance using unique external GUID.
        ///</summary>
        ///<param name="jobResultID"></param>
        ///<returns>JobResult</returns>
        public JobResult GetJobResult(string externalID)
        {
            return jobDAL.GetJobResult(externalID);
        }

        ///<summary>
        /// Return counts of all job statuses (running, not running, completed, stopped, with errors).
        /// Useful for UI reporting of job statuses.
        ///</summary>
        ///<param name="appID">Application ID for multi-tenant app. Optional.</param>
        ///<param name="userID">User ID. Optional.</param>
        ///<returns>Collection of JobStatusCount</returns>
        public List<JobStatusCount> GetJobStatusCount(string appID, int? userID)
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
