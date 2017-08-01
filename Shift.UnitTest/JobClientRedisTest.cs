using System;
using System.Text;
using System.Collections.Generic;
using System.Configuration;
using Xunit;
using Shift.Entities;
using Moq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Shift.UnitTest
{
    public class JobClientRedisTest
    {
        JobClient jobClient;
        private const string AppID = "TestAppID";

        public JobClientRedisTest()
        {
            var appSettingsReader = new AppSettingsReader();

            //Configure storage connection
            var clientConfig = new ClientConfig();
            clientConfig.DBConnectionString = appSettingsReader.GetValue("RedisConnectionString", typeof(string)) as string;
            clientConfig.StorageMode = "redis";
            jobClient = new JobClient(clientConfig);
        }

        [Fact]
        public void GetJob_Valid()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            var job = jobClient.GetJob(jobID);

            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
        }
    }
}
