using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shift;
using Shift.Entities;

namespace Shift.UnitTest
{
    [TestClass]
    public class ServerConfigTest
    {
        [TestMethod]
        public void ProcessIDTest()
        {
            var config = new ServerConfig();
            config.ProcessID = "Process-1234";

            Assert.AreEqual("Process-1234", config.ProcessID);
        }

        [TestMethod]
        public void DBConnectionStringTest()
        {
            var config = new ServerConfig();
            config.DBConnectionString = "localhost:6400";

            Assert.AreEqual("localhost:6400", config.DBConnectionString);
        }

        [TestMethod]
        public void StorageModeRedisTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";

            Assert.AreEqual(StorageMode.Redis, config.StorageMode);
        }

        [TestMethod]
        public void StorageModeMssqlTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "mssql";

            Assert.AreEqual(StorageMode.MSSql, config.StorageMode);
        }

        [TestMethod]
        public void StorageModeMongoDBTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "mongo";

            Assert.AreEqual(StorageMode.MongoDB, config.StorageMode);
        }

        [TestMethod]
        public void MaxRunnableJobsTest()
        {
            var config = new ServerConfig();
            config.MaxRunnableJobs = 10;

            Assert.AreEqual(10, config.MaxRunnableJobs);
        }

        [TestMethod]
        public void MaxRunnableJobsDefaultTest()
        {
            var config = new ServerConfig();

            Assert.AreEqual(100, config.MaxRunnableJobs);
        }

        [TestMethod]
        public void AssemblyListPathTest()
        {
            var config = new ServerConfig();
            config.AssemblyListPath = "\\job-assemblies\\assemblyList.txt";

            Assert.AreEqual("\\job-assemblies\\assemblyList.txt", config.AssemblyListPath);
        }

        [TestMethod]
        public void AssemblyFolderTest()
        {
            var config = new ServerConfig();
            config.AssemblyFolder = "\\job-assemblies\\";

            Assert.AreEqual("\\job-assemblies\\", config.AssemblyFolder);
        }

        [TestMethod]
        public void UseCacheTest()
        {
            var config = new ServerConfig();
            config.UseCache = true;

            Assert.AreEqual(true, config.UseCache);
        }

        [TestMethod]
        public void UseCacheDefaultTest()
        {
            var config = new ServerConfig();

            Assert.AreEqual(false, config.UseCache);
        }

        [TestMethod]
        public void CacheConfigurationStringTest()
        {
            var config = new ServerConfig();
            config.CacheConfigurationString = "localhost:6370";

            Assert.AreEqual("localhost:6370", config.CacheConfigurationString);
        }

        [TestMethod]
        public void EncryptionKeyTest()
        {
            var config = new ServerConfig();
            config.EncryptionKey = "123$asdflouwrtasrthj";

            Assert.AreEqual("123$asdflouwrtasrthj", config.EncryptionKey);
        }

        [TestMethod]
        public void ProgressDBIntervalTest()
        {
            var config = new ServerConfig();
            config.ProgressDBInterval = new TimeSpan(0, 1, 0);
            var expectedTS = new TimeSpan(0, 1, 0);

            Assert.AreEqual(expectedTS, config.ProgressDBInterval);
        }

        [TestMethod]
        public void ProgressDBIntervalDefaultTest()
        {
            var config = new ServerConfig();
            var expectedTS = new TimeSpan(0, 0, 10);

            Assert.AreEqual(expectedTS, config.ProgressDBInterval);
        }

        [TestMethod]
        public void ServerTimerIntervalTest()
        {
            var config = new ServerConfig();
            config.ServerTimerInterval = 2500;

            Assert.AreEqual(2500, config.ServerTimerInterval);
        }

        [TestMethod]
        public void ServerTimerIntervalDefaultTest()
        {
            var config = new ServerConfig();

            Assert.AreEqual(5000, config.ServerTimerInterval);
        }

        [TestMethod]
        public void ServerTimerInterval2Test()
        {
            var config = new ServerConfig();
            config.ServerTimerInterval2 = 12000;

            Assert.AreEqual(12000, config.ServerTimerInterval2);
        }

        [TestMethod]
        public void ServerTimerInterval2DefaultTest()
        {
            var config = new ServerConfig();

            Assert.AreEqual(10000, config.ServerTimerInterval2);
        }

        [TestMethod]
        public void AutoDeletePeriodTest()
        {
            var config = new ServerConfig();
            config.AutoDeletePeriod = 24;

            Assert.AreEqual(24, config.AutoDeletePeriod);
        }

        [TestMethod]
        public void AutoDeletePeriodDefaultTest()
        {
            var config = new ServerConfig();

            Assert.IsNull(config.AutoDeletePeriod);
        }

        [TestMethod]
        public void AutoDeleteStatusTest()
        {
            var config = new ServerConfig();
            config.AutoDeleteStatus = new List<JobStatus?>() { JobStatus.Completed, JobStatus.Stopped, null };
            var expectedList = new List<JobStatus?>() { JobStatus.Completed, JobStatus.Stopped, null };

            Assert.AreEqual(expectedList.Count, config.AutoDeleteStatus.Count);
            CollectionAssert.AreEqual(expectedList, (List<JobStatus?>)config.AutoDeleteStatus);
        }

        [TestMethod]
        public void AutoDeleteStatusDefaultTest()
        {
            var config = new ServerConfig();
            var expectedList = new List<JobStatus?>() { JobStatus.Completed };

            Assert.AreEqual(expectedList.Count, config.AutoDeleteStatus.Count);
            CollectionAssert.AreEqual(expectedList, (List<JobStatus?>)config.AutoDeleteStatus);
        }

        [TestMethod]
        public void PollingOnceTest()
        {
            var config = new ServerConfig();
            config.PollingOnce = true;

            Assert.AreEqual(true, config.PollingOnce);
        }

        [TestMethod]
        public void PollingOnceDefaultTest()
        {
            var config = new ServerConfig();

            Assert.AreEqual(false, config.PollingOnce);
        }

        [TestMethod]
        public void WorkersTest()
        {
            var config = new ServerConfig();
            config.Workers = 5;

            Assert.AreEqual(5, config.Workers);
        }

        [TestMethod]
        public void WorkersDefaultTest()
        {
            var config = new ServerConfig();

            Assert.AreEqual(1, config.Workers);
        }

        [TestMethod]
        public void ForceStopServerTest()
        {
            var config = new ServerConfig();
            config.ForceStopServer = true;

            Assert.AreEqual(true, config.ForceStopServer);
        }

        [TestMethod]
        public void ForceStopServerDefaultTest()
        {
            var config = new ServerConfig();

            Assert.AreEqual(false, config.ForceStopServer);
        }

        [TestMethod]
        public void StopServerDelayTest()
        {
            var config = new ServerConfig();
            config.StopServerDelay = 5000;

            Assert.AreEqual(5000, config.StopServerDelay);
        }

        [TestMethod]
        public void StopServerDelayDefaultTest()
        {
            var config = new ServerConfig();

            Assert.AreEqual(30000, config.StopServerDelay);
        }

    }
}
