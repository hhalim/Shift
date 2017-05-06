using System;
using System.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace Shift.UnitTest
{
    [TestClass]
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

        [TestMethod]
        [ExpectedException(typeof(System.ArgumentNullException))]
        public void JobServerConfigNullTest()
        {
            var jobServer = new JobServer(null);
        }

        [TestMethod]
        [ExpectedException(typeof(System.ArgumentNullException))]
        public void JobServerStorageModeNullTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;

            var jobServer = new JobServer(config);
        }

        [TestMethod]
        [ExpectedException(typeof(System.ArgumentNullException))]
        public void JobServerProcessIDNullTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = "";

            var jobServer = new JobServer(config);
        }

        [TestMethod]
        [ExpectedException(typeof(System.ArgumentNullException))]
        public void JobServerDBConnectionStringNullTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = "";
            config.ProcessID = processID;

            var jobServer = new JobServer(config);
        }

        [TestMethod]
        [ExpectedException(typeof(System.ArgumentNullException))]
        public void JobServerCacheConfigurationStringNullTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.UseCache = true;
            config.CacheConfigurationString = "";

            var jobServer = new JobServer(config);
        }

        //Should get no Exception
        [TestMethod]
        public void JobServerMaxRunnableJobsZeroTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.MaxRunnableJobs = 0;

            var jobServer = new JobServer(config);
            Assert.IsTrue(true);
        }

        [TestMethod]
        public void RunServerTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.MaxRunnableJobs = 1;

            var jobServer = new JobServer(config);
            jobServer.RunServer();
            Assert.IsTrue(true);
        }

        [TestMethod]
        public async Task RunServerAsyncTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";
            config.DBConnectionString = connectionString;
            config.ProcessID = processID;
            config.MaxRunnableJobs = 1;

            var jobServer = new JobServer(config);
            await jobServer.RunServerAsync();
            Assert.IsTrue(true);
        }

        [TestMethod]
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
            Assert.IsTrue(true);
        }

        [TestMethod]
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
            Assert.IsTrue(true);
        }

        [TestMethod]
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
            Assert.IsTrue(true);
        }


        [TestMethod]
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
            Assert.IsTrue(true);
        }
    }
}
