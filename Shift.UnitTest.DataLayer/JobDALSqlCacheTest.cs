using System;
using Xunit;
using Shift.Entities;
using System.Collections.Generic;
using Shift.DataLayer;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Moq;
using System.Collections;

namespace Shift.UnitTest.DataLayer
{
     
    public class JobDALSqlCacheTest
    {
        private static AppSettingsReader appSettingsReader = new AppSettingsReader();
        private const string AppID = "TestAppID";
        private readonly string processID;
        private static string connectionString = appSettingsReader.GetValue("MSSqlConnectionString", typeof(string)) as string;
        private const string encryptionKey = "";

        //Tests various calls from JobDALSql to JobCache
        //Use mock to mock JobCache, so this doesn't actually touch the underlying Cache, only the JobDAL methods
        public JobDALSqlCacheTest()
        {
            processID = this.ToString();
        }

        [Fact]
        public void CacheNoConnectionTest()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => { var cache = new Shift.Cache.Redis.JobCache(null); });
        }

        [Fact]
        public void GetProgressTest()
        {
            var jobID = Guid.NewGuid().ToString("N");

            var mockJobCache = new Mock<IJobCache>();
            mockJobCache
                .Setup(ss => ss.GetCachedProgress(jobID) )
                .Returns(new JobStatusProgress() { JobID= jobID, Percent = 50 });

            var jobDAL = new JobDALSql(connectionString, mockJobCache.Object, encryptionKey);
            var actual = jobDAL.GetProgress(jobID);

            Assert.Equal(jobID, actual.JobID);
            Assert.True(actual.Percent == 50);
        }

        [Fact]
        public async Task GetProgressAsyncTest()
        {
            var jobID = Guid.NewGuid().ToString("N");

            var mockJobCache = new Mock<IJobCache>();
            mockJobCache
                .Setup(ss => ss.GetCachedProgressAsync(jobID))
                .ReturnsAsync(new JobStatusProgress() { JobID = jobID, Percent = 50 });

            var jobDAL = new JobDALSql(connectionString, mockJobCache.Object, encryptionKey);
            var actual = await jobDAL.GetProgressAsync(jobID);

            Assert.Equal(jobID, actual.JobID);
            Assert.True(actual.Percent == 50);
        }

        [Fact]
        public void GetCachedProgressTest()
        {
            var jobID = Guid.NewGuid().ToString("N");

            var mockJobCache = new Mock<IJobCache>();
            mockJobCache
                .Setup(ss => ss.GetCachedProgress(jobID))
                .Returns(new JobStatusProgress() { JobID = jobID, Percent = 50 });

            var jobDAL = new JobDALSql(connectionString, mockJobCache.Object, encryptionKey);
            var actual = jobDAL.GetCachedProgress(jobID);

            Assert.Equal(jobID, actual.JobID);
            Assert.True(actual.Percent == 50);
        }

        [Fact]
        public async Task GetCachedProgressAsyncTest()
        {
            var jobID = Guid.NewGuid().ToString("N");

            var mockJobCache = new Mock<IJobCache>();
            mockJobCache
                .Setup(ss => ss.GetCachedProgressAsync(jobID))
                .ReturnsAsync(new JobStatusProgress() { JobID = jobID, Percent = 50 });

            var jobDAL = new JobDALSql(connectionString, mockJobCache.Object, encryptionKey);
            var actual = await jobDAL.GetCachedProgressAsync(jobID);

            Assert.Equal(jobID, actual.JobID);
            Assert.True(actual.Percent == 50);
        }

        [Fact]
        public async Task SetCachedProgressAsyncTest()
        {
            var mockJobCache = new Mock<IJobCache>();
            mockJobCache
                .Setup(ss => ss.SetCachedProgressAsync(It.IsAny<string>(), It.IsAny<int?>(),
               It.IsAny<string>(), It.IsAny<string>()))
               .Returns(Task.CompletedTask);

            var jobDAL = new JobDALSql(connectionString, mockJobCache.Object, encryptionKey);
            var task = jobDAL.SetCachedProgressAsync(Guid.NewGuid().ToString("N"), 50, "Note", "Data");
            await task;

            Assert.Null(task.Exception); //no exception
            Assert.True(task.IsCompleted);
        }

        [Fact]
        public async Task SetCachedProgressErrorAsyncTest()
        {
            var jobID = Guid.NewGuid().ToString("N");

            var mockJobCache = new Mock<IJobCache>();
            mockJobCache
                .Setup(ss => ss.GetCachedProgressAsync(jobID))
                .ReturnsAsync(new JobStatusProgress() { JobID = jobID, Percent = 50 });
            mockJobCache
                .Setup(ss => ss.SetCachedProgressErrorAsync(It.IsAny<JobStatusProgress>(), It.IsAny<string>()))
                .Returns(Task.CompletedTask);

            var jobDAL = new JobDALSql(connectionString, mockJobCache.Object, encryptionKey);
            var task = jobDAL.SetCachedProgressErrorAsync(jobID, "Test Error");
            await task;

            Assert.Null(task.Exception);
            Assert.True(task.IsCompleted);
        }

        [Fact]
        public async Task DeleteCachedProgressAsync_ForOneJob()
        {
            var mockJobCache = new Mock<IJobCache>();
            mockJobCache
                .Setup(ss => ss.DeleteCachedProgressAsync(It.IsAny<string>()))
                .Returns(Task.CompletedTask);

            var jobDAL = new JobDALSql(connectionString, mockJobCache.Object, encryptionKey);
            var task = jobDAL.DeleteCachedProgressAsync(Guid.NewGuid().ToString("N"));
            await task;

            Assert.Null(task.Exception);
            Assert.True(task.IsCompleted);
        }

        [Fact]
        public async Task DeleteCachedProgressAsync_ForMultipleJobs()
        {
            var mockJobCache = new Mock<IJobCache>();
            mockJobCache
                .Setup(ss => ss.DeleteCachedProgressAsync(It.IsAny<string>()))
                .Returns(Task.CompletedTask);

            var jobDAL = new JobDALSql(connectionString, mockJobCache.Object, encryptionKey);
            var task = jobDAL.DeleteCachedProgressAsync(
                new List<string> { Guid.NewGuid().ToString("N"), Guid.NewGuid().ToString("N") } 
                );
            await task;

            Assert.Null(task.Exception); 
            Assert.True(task.IsCompleted);
        }

        [Fact]
        public async Task SetCachedProgressStatusAsync_ForOneJob()
        {
            var jobID = Guid.NewGuid().ToString("N");

            var mockJobCache = new Mock<IJobCache>();
            mockJobCache
                .Setup(ss => ss.GetCachedProgressAsync(jobID))
                .ReturnsAsync(new JobStatusProgress() { JobID = jobID, Percent = 50, ExistsInDB = true });
            mockJobCache
                .Setup(ss => ss.SetCachedProgressStatusAsync(It.IsAny<JobStatusProgress>(), It.IsAny<JobStatus>()))
                .Returns(Task.CompletedTask);

            var jobDAL = new JobDALSql(connectionString, mockJobCache.Object, encryptionKey);
            var task = jobDAL.SetCachedProgressStatusAsync(jobID, JobStatus.Stopped);
            await task;

            Assert.Null(task.Exception);
            Assert.True(task.IsCompleted);
        }

        [Fact]
        public async Task SetCachedProgressStatusAsync_ForMultipleJobs()
        {
            var jobID1 = Guid.NewGuid().ToString("N");
            var jobID2 = Guid.NewGuid().ToString("N");

            var mockJobCache = new Mock<IJobCache>();
            mockJobCache
                .Setup(ss => ss.GetCachedProgressAsync(jobID1))
                .ReturnsAsync(new JobStatusProgress() { JobID = jobID1, Percent = 50, ExistsInDB = true });
            mockJobCache
                .Setup(ss => ss.GetCachedProgressAsync(jobID2))
                .ReturnsAsync(new JobStatusProgress() { JobID = jobID2, Percent = 25, ExistsInDB = true });
            mockJobCache
                .Setup(ss => ss.SetCachedProgressStatusAsync(It.IsAny<JobStatusProgress>(), It.IsAny<JobStatus>()))
                .Returns(Task.CompletedTask);

            var jobDAL = new JobDALSql(connectionString, mockJobCache.Object, encryptionKey);
            var task = jobDAL.SetCachedProgressStatusAsync(
                new List<string> { jobID1, jobID2 }, JobStatus.Stopped
                );
            await task;

            Assert.Null(task.Exception);
            Assert.True(task.IsCompleted);
        }
    }
}
