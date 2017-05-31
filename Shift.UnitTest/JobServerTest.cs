using System;
using System.Configuration;
using Xunit;
using System.Threading.Tasks;

namespace Shift.UnitTest
{
     
    public class JobServerTest
    {
        private string connectionString;
        private string processID;
        public JobServerTest()
        {
            var appSettingsReader = new AppSettingsReader();
            connectionString = appSettingsReader.GetValue("RedisConnectionString", typeof(string)) as string;
            processID = this.ToString();
        }

        [Fact]
        public void JobServerConfigNullTest()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => { var jobServer = new JobServer(null); });
        }

        [Fact]
        public void JobServerStorageModeNullTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;

            var ex = Assert.Throws<ArgumentNullException>(() => { var jobServer = new JobServer(config); });
        }

        [Fact]
        public void JobServerProcessIDTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = "";

            var jobServer = new JobServer(config);

            Assert.NotNull(config.ProcessID);
        }

        [Fact]
        public void JobServerDBConnectionStringNullTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = "";
            config.ProcessID = processID;

            var ex = Assert.Throws<ArgumentNullException>(() => { var jobServer = new JobServer(config); });
        }

        [Fact]
        public void JobServerCacheConfigurationStringNullTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.UseCache = true;
            config.CacheConfigurationString = "";

            var ex = Assert.Throws<ArgumentNullException>(() => { var jobServer = new JobServer(config); });
        }

        //Should get no Exception
        [Fact]
        public void JobServerMaxRunnableJobsZeroTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.MaxRunnableJobs = 0;

            var jobServer = new JobServer(config);
            Assert.True(true);
        }

        [Fact]
        public void RunServerTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.MaxRunnableJobs = 1;

            var jobServer = new JobServer(config);
            jobServer.RunServer();
            Assert.True(true);
        }

        [Fact]
        public async Task RunServerAsyncTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.MaxRunnableJobs = 1;

            var jobServer = new JobServer(config);
            await jobServer.RunServerAsync();
            Assert.True(true);
        }

        [Fact]
        public void StopServerTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.MaxRunnableJobs = 1;
            config.ForceStopServer = true;
            config.StopServerDelay = 1000;

            var jobServer = new JobServer(config);
            jobServer.StopServer();
            Assert.True(true);
        }

        [Fact]
        public async Task StopServerAsyncTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.MaxRunnableJobs = 1;
            config.ForceStopServer = true;
            config.StopServerDelay = 1000;

            var jobServer = new JobServer(config);
            await jobServer.StopServerAsync();
            Assert.True(true);
        }

        [Fact]
        public void StopServerWaitForAllRunningJobsTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.MaxRunnableJobs = 1;
            config.ForceStopServer = false;
            config.StopServerDelay = 1000;

            var jobServer = new JobServer(config);
            jobServer.StopServer();
            Assert.True(true);
        }


        [Fact]
        public async Task StopServerWaitForAllRunningJobsAsyncTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.MaxRunnableJobs = 1;
            config.ForceStopServer = false;
            config.StopServerDelay = 1000;

            var jobServer = new JobServer(config);
            await jobServer.StopServerAsync();
            Assert.True(true);
        }
    }
}
