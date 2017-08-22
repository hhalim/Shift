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
    public class JobClientRedisAsyncTest
    {
        JobClient jobClient;
        private const string AppID = "TestAppID";

        public JobClientRedisAsyncTest()
        {
            var appSettingsReader = new AppSettingsReader();

            //Configure storage connection
            var clientConfig = new ClientConfig();
            clientConfig.DBConnectionString = appSettingsReader.GetValue("RedisConnectionString", typeof(string)) as string;
            clientConfig.StorageMode = "redis";
            jobClient = new JobClient(clientConfig);
        }

        //Integration Test of Redis DB from JobClient.
        //The JobClientTest uses Moq to mock the DB layer, this one actually uses the DB layer.
        [Fact]
        public async Task GetJob_Valid()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);

            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
        }
    }
}
