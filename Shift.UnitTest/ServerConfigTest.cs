using System;
using System.Collections.Generic;
using Xunit;
using Shift;
using Shift.Entities;

namespace Shift.UnitTest
{
     
    public class ServerConfigTest
    {
        [Fact]
        public void ProcessIDTest()
        {
            var config = new ServerConfig();
            config.ProcessID = "Process-1234";

            Assert.Equal("Process-1234", config.ProcessID);
        }

        [Fact]
        public void DBConnectionStringTest()
        {
            var config = new ServerConfig();
            config.DBConnectionString = "localhost:6400";

            Assert.Equal("localhost:6400", config.DBConnectionString);
        }

        [Fact]
        public void StorageModeRedisTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "redis";

            Assert.Equal(StorageMode.Redis, config.StorageMode);
        }

        [Fact]
        public void StorageModeMssqlTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "mssql";

            Assert.Equal(StorageMode.MSSql, config.StorageMode);
        }

        [Fact]
        public void StorageModeMongoDBTest()
        {
            var config = new ServerConfig();
            config.StorageMode = "mongo";

            Assert.Equal(StorageMode.MongoDB, config.StorageMode);
        }

        [Fact]
        public void MaxRunnableJobsTest()
        {
            var config = new ServerConfig();
            config.MaxRunnableJobs = 10;

            Assert.Equal(10, config.MaxRunnableJobs);
        }

        [Fact]
        public void MaxRunnableJobsDefaultTest()
        {
            var config = new ServerConfig();

            Assert.Equal(100, config.MaxRunnableJobs);
        }

        [Fact]
        public void AssemblyListPathTest()
        {
            var config = new ServerConfig();
            config.AssemblyListPath = "\\job-assemblies\\assemblyList.txt";

            Assert.Equal("\\job-assemblies\\assemblyList.txt", config.AssemblyListPath);
        }

        [Fact]
        public void AssemblyFolderTest()
        {
            var config = new ServerConfig();
            config.AssemblyFolder = "\\job-assemblies\\";

            Assert.Equal("\\job-assemblies\\", config.AssemblyFolder);
        }

        [Fact]
        public void UseCacheTest()
        {
            var config = new ServerConfig();
            config.UseCache = true;

            Assert.Equal(true, config.UseCache);
        }

        [Fact]
        public void UseCacheDefaultTest()
        {
            var config = new ServerConfig();

            Assert.Equal(false, config.UseCache);
        }

        [Fact]
        public void CacheConfigurationStringTest()
        {
            var config = new ServerConfig();
            config.CacheConfigurationString = "localhost:6370";

            Assert.Equal("localhost:6370", config.CacheConfigurationString);
        }

        [Fact]
        public void EncryptionKeyTest()
        {
            var config = new ServerConfig();
            config.EncryptionKey = "123$asdflouwrtasrthj";

            Assert.Equal("123$asdflouwrtasrthj", config.EncryptionKey);
        }

        [Fact]
        public void ProgressDBIntervalTest()
        {
            var config = new ServerConfig();
            config.ProgressDBInterval = new TimeSpan(0, 1, 0);
            var expectedTS = new TimeSpan(0, 1, 0);

            Assert.Equal(expectedTS, config.ProgressDBInterval);
        }

        [Fact]
        public void ProgressDBIntervalDefaultTest()
        {
            var config = new ServerConfig();
            var expectedTS = new TimeSpan(0, 0, 10);

            Assert.Equal(expectedTS, config.ProgressDBInterval);
        }

        [Fact]
        public void ServerTimerIntervalTest()
        {
            var config = new ServerConfig();
            config.ServerTimerInterval = 2500;

            Assert.Equal(2500, config.ServerTimerInterval);
        }

        [Fact]
        public void ServerTimerIntervalDefaultTest()
        {
            var config = new ServerConfig();

            Assert.Equal(5000, config.ServerTimerInterval);
        }

        [Fact]
        public void ServerTimerInterval2Test()
        {
            var config = new ServerConfig();
            config.ServerTimerInterval2 = 12000;

            Assert.Equal(12000, config.ServerTimerInterval2);
        }

        [Fact]
        public void ServerTimerInterval2DefaultTest()
        {
            var config = new ServerConfig();

            Assert.Equal(10000, config.ServerTimerInterval2);
        }

        [Fact]
        public void AutoDeletePeriodTest()
        {
            var config = new ServerConfig();
            config.AutoDeletePeriod = 24;

            Assert.Equal(24, config.AutoDeletePeriod);
        }

        [Fact]
        public void AutoDeletePeriodDefaultTest()
        {
            var config = new ServerConfig();

            Assert.Null(config.AutoDeletePeriod);
        }

        [Fact]
        public void AutoDeleteStatusTest()
        {
            var config = new ServerConfig();
            config.AutoDeleteStatus = new List<JobStatus?>() { JobStatus.Completed, JobStatus.Stopped, null };
            var expectedList = new List<JobStatus?>() { JobStatus.Completed, JobStatus.Stopped, null };

            Assert.Equal(expectedList.Count, config.AutoDeleteStatus.Count);
            Assert.Equal(expectedList, config.AutoDeleteStatus);
        }

        [Fact]
        public void AutoDeleteStatusDefaultTest()
        {
            var config = new ServerConfig();
            var expectedList = new List<JobStatus?>() { JobStatus.Completed };

            Assert.Equal(expectedList.Count, config.AutoDeleteStatus.Count);
            Assert.Equal(expectedList, config.AutoDeleteStatus);
        }

        [Fact]
        public void PollingOnceTest()
        {
            var config = new ServerConfig();
            config.PollingOnce = true;

            Assert.Equal(true, config.PollingOnce);
        }

        [Fact]
        public void PollingOnceDefaultTest()
        {
            var config = new ServerConfig();

            Assert.Equal(false, config.PollingOnce);
        }

        [Fact]
        public void WorkersTest()
        {
            var config = new ServerConfig();
            config.Workers = 5;

            Assert.Equal(5, config.Workers);
        }

        [Fact]
        public void WorkersDefaultTest()
        {
            var config = new ServerConfig();

            Assert.Equal(1, config.Workers);
        }

        [Fact]
        public void ForceStopServerTest()
        {
            var config = new ServerConfig();
            config.ForceStopServer = true;

            Assert.Equal(true, config.ForceStopServer);
        }

        [Fact]
        public void ForceStopServerDefaultTest()
        {
            var config = new ServerConfig();

            Assert.Equal(false, config.ForceStopServer);
        }

        [Fact]
        public void StopServerDelayTest()
        {
            var config = new ServerConfig();
            config.StopServerDelay = 5000;

            Assert.Equal(5000, config.StopServerDelay);
        }

        [Fact]
        public void StopServerDelayDefaultTest()
        {
            var config = new ServerConfig();

            Assert.Equal(30000, config.StopServerDelay);
        }

    }
}
