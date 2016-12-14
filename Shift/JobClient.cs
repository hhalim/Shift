using System;
using System.Collections.Generic;
using Annotations;
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

        public static class Register
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
        public int? Add(string appID, int userID, string jobType, [NotNull, InstantHandle]Expression<Action> methodCall)
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
        public int? Add(string appID, int userID, string jobType, string jobName, [NotNull, InstantHandle]Expression<Action> methodCall)
        {
            return jobDAL.Add(appID, userID, jobType, jobName, methodCall);
        }

        // Cancel only for running or not started jobs.
        public int SetCommandStop(List<int> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return -1;

            return jobDAL.SetCommandStop(jobIDs);
        }

        public int SetCommandStopDelete(List<int> jobIDs)
        {
            if (jobIDs == null || jobIDs.Count == 0)
                return -1;

            return jobDAL.SetCommandStopDelete(jobIDs);
        }

        public Job GetJob(int jobID)
        {
            return jobDAL.GetJob(jobID);
        }

        public JobView GetJobView(int jobID)
        {
            return jobDAL.GetJobView(jobID);
        }
        
        //Reset Jobs
        public int ResetJobs(List<int> jobIDs)
        {
            jobDAL.DeleteCachedProgress(jobIDs);
            return jobDAL.Reset(jobIDs);
        }

        //Delete Jobs
        public int DeleteJobs(List<int> jobIDs)
        {
            jobDAL.DeleteCachedProgress(jobIDs);
            return jobDAL.Delete(jobIDs);
        }

        public JobResult GetJobResult(int jobResultID)
        {
            return jobDAL.GetJobResult(jobResultID);
        }

        public JobResult GetJobResult(string externalID)
        {
            return jobDAL.GetJobResult(externalID);
        }

        public List<JobStatusCount> GetJobStatusCount(string appID, int? userID)
        {
            return jobDAL.GetJobStatusCount(appID, userID);
        }

        //Use Redis and DB to get the job's progress
        public JobStatusProgress GetProgress(int jobID)
        {
            return jobDAL.GetProgress(jobID);
        }

        public JobStatusProgress GetCachedProgress(int jobID)
        {
            return jobDAL.GetCachedProgress(jobID);
        }
        #endregion



    }
}
