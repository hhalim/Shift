using StackExchange.Redis;
using Newtonsoft.Json;
using Shift.Entities;
using System;

namespace Shift.Cache.Redis
{
    public class JobCache : IJobCache
    {
        const string KeyPrefix = "job-progress:";
        private readonly Lazy<ConnectionMultiplexer> lazyConnection;

        public ConnectionMultiplexer Connection
        {
            get
            {
                return lazyConnection.Value;
            }
        }

        public IDatabase RedisDatabase
        {
            get
            {
                return Connection.GetDatabase();
            }
        }
 
        public JobCache(string configurationString)
        {
            if (string.IsNullOrWhiteSpace(configurationString))
                throw new ArgumentNullException("configurationString");

            lazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(configurationString));
        }

        public JobStatusProgress GetCachedProgress(int jobID)
        {
            var jsProgress = new JobStatusProgress();

            var jobStatusProgressString = RedisDatabase.StringGet(KeyPrefix + jobID.ToString());
            if (!string.IsNullOrWhiteSpace(jobStatusProgressString))
            {
                jsProgress = JsonConvert.DeserializeObject<JobStatusProgress>(jobStatusProgressString);
                return jsProgress;
            }

            return null;
        }

        //Set Cached progress
        public void SetCachedProgress(int jobID, int? percent, string note, string data)
        {
            var jobStatusProgressString = RedisDatabase.StringGet(KeyPrefix + jobID.ToString());

            var jsProgress = new JobStatusProgress();
            if (!string.IsNullOrWhiteSpace(jobStatusProgressString))
            {
                jsProgress = JsonConvert.DeserializeObject<JobStatusProgress>(jobStatusProgressString);
            }
            else
            {
                //missing, then setup a new one, always status = running
                jsProgress.JobID = jobID;
                jsProgress.Status = JobStatus.Running;
            }
            jsProgress.Percent = percent;
            jsProgress.Note = note;
            jsProgress.Data = data;
            jsProgress.Updated = DateTime.Now;

            RedisDatabase.StringSetAsync(KeyPrefix + jobID.ToString(), JsonConvert.SerializeObject(jsProgress), flags: CommandFlags.FireAndForget);
        }

        public void SetCachedProgressStatus(JobStatusProgress jsProgress, JobStatus status)
        {
            //Update running/stop status only if it exists in DB
            jsProgress.Status = status;
            jsProgress.Updated = DateTime.Now;
            RedisDatabase.StringSetAsync(KeyPrefix + jsProgress.JobID.ToString(), JsonConvert.SerializeObject(jsProgress), flags: CommandFlags.FireAndForget);
        }

        //Set cached progress error
        public void SetCachedProgressError(JobStatusProgress jsProgress, string error)
        {
            jsProgress.Status = JobStatus.Error;
            jsProgress.Error = error;
            jsProgress.Updated = DateTime.Now;
            RedisDatabase.StringSetAsync(KeyPrefix + jsProgress.JobID.ToString(), JsonConvert.SerializeObject(jsProgress), flags: CommandFlags.FireAndForget);
        }

        public void DeleteCachedProgress(int jobID)
        {
            RedisDatabase.KeyDeleteAsync(KeyPrefix + jobID.ToString(), flags: CommandFlags.FireAndForget);
        }


    }
}
