using StackExchange.Redis;
using Newtonsoft.Json;
using Shift.Entities;
using System;
using System.Threading.Tasks;

namespace Shift.Cache.Redis
{
    public class JobCache : IJobCache
    {
        const string KeyPrefix = "job-progress:";
        private static Lazy<ConnectionMultiplexer> lazyConnection;
        private static Lazy<ConfigurationOptions> lazyConfigOptions;
        private IDatabase _IDatabase;

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
                if (_IDatabase == null)
                {
                    Connection.PreserveAsyncOrder = false;
                    _IDatabase = Connection.GetDatabase();
                }
                return _IDatabase;
            }
        }
 
        public JobCache(string configurationString)
        {
            if (string.IsNullOrWhiteSpace(configurationString))
                throw new ArgumentNullException("configurationString");

            lazyConfigOptions = new Lazy<ConfigurationOptions>(() =>
            {
                var configOptions = new ConfigurationOptions();
                configOptions.EndPoints.Add(configurationString);
                configOptions.ClientName = this.ToString();
                configOptions.ConnectTimeout = 30000;
                configOptions.SyncTimeout = 30000;
                configOptions.AbortOnConnectFail = false;
                return configOptions;
            });
            lazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(lazyConfigOptions.Value));
        }

        public JobStatusProgress GetCachedProgress(string jobID)
        {
            return GetCachedProgressAsync(jobID, true).GetAwaiter().GetResult();
        }

        public Task<JobStatusProgress> GetCachedProgressAsync(string jobID)
        {
            return GetCachedProgressAsync(jobID, false);
        }

        private async Task<JobStatusProgress> GetCachedProgressAsync(string jobID, bool isSync)
        {
            var jsProgress = new JobStatusProgress();

            var jobStatusProgressString = "";
            if (isSync)
            {
                jobStatusProgressString = RedisDatabase.StringGet(KeyPrefix + jobID.ToString());
            }
            else
            {
                jobStatusProgressString = await RedisDatabase.StringGetAsync(KeyPrefix + jobID.ToString());
            }

            if (!string.IsNullOrWhiteSpace(jobStatusProgressString))
            {
                jsProgress = JsonConvert.DeserializeObject<JobStatusProgress>(jobStatusProgressString);
                return jsProgress;
            }

            return null;
        }

        //Set Cached progress
        public Task SetCachedProgressAsync(string jobID, int? percent, string note, string data)
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

            return RedisDatabase.StringSetAsync(KeyPrefix + jobID.ToString(), JsonConvert.SerializeObject(jsProgress), flags: CommandFlags.FireAndForget);
        }

        public Task SetCachedProgressStatusAsync(JobStatusProgress jsProgress, JobStatus status)
        {
            //Update running/stop status only if it exists in DB
            jsProgress.Status = status;
            jsProgress.Updated = DateTime.Now;
            return RedisDatabase.StringSetAsync(KeyPrefix + jsProgress.JobID.ToString(), JsonConvert.SerializeObject(jsProgress), flags: CommandFlags.FireAndForget);
        }

        //Set cached progress error
        public Task SetCachedProgressErrorAsync(JobStatusProgress jsProgress, string error)
        {
            jsProgress.Status = JobStatus.Error;
            jsProgress.Error = error;
            jsProgress.Updated = DateTime.Now;
            return RedisDatabase.StringSetAsync(KeyPrefix + jsProgress.JobID.ToString(), JsonConvert.SerializeObject(jsProgress), flags: CommandFlags.FireAndForget);
        }

        public Task DeleteCachedProgressAsync(string jobID)
        {
            return RedisDatabase.KeyDeleteAsync(KeyPrefix + jobID.ToString(), flags: CommandFlags.FireAndForget);
        }


    }
}
