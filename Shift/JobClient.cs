using System;
using System.Collections.Generic;
using Annotations;
using System.Linq.Expressions;

using Shift.Entities;
using Shift.DataLayer;

[assembly: CLSCompliant(true)]
namespace Shift
{
    public class JobClient
    {
        private JobDAL jobDAL = null;
        private Options options = null;

        public JobClient(Options options)
        {
            if (options == null)
            {
                throw new Exception("Unable to start with no options.");
            }

            if (string.IsNullOrWhiteSpace(options.DBConnectionString))
            {
                throw new Exception("Error: unable to start without Process Jobs DB connection string.");

            }

            if (string.IsNullOrWhiteSpace(options.RedisConnectionString))
            {
                throw new Exception("Error: unable to start without Process Jobs Redis connection string.");

            }

            this.options = options;
            jobDAL = new JobDAL(options.DBConnectionString);
            jobDAL.RedisConnect(options.RedisConnectionString);
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
