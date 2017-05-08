using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shift.Entities;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Shift.Cache.Redis;
using System.Threading.Tasks;

namespace Shift.UnitTest.DataLayer
{
    [TestClass]
    public class JobCacheRedisTest
    {
        private static AppSettingsReader appSettingsReader = new AppSettingsReader();
        private const string AppID = "TestAppID";
        private readonly string processID;
        private static string configurationString = appSettingsReader.GetValue("RedisConnectionString", typeof(string)) as string;
        private const string encryptionKey = "";
        JobCache jobCache;

        public JobCacheRedisTest()
        {
            processID = this.ToString();
            this.jobCache = new JobCache(configurationString);
        }

        [TestMethod]
        public void GetCachedProgressTest()
        {
            var jobID = "1Test";
            var note = "progress note";
            var data = "progress data";
            var percent = 100;
            jobCache.SetCachedProgressAsync(jobID, percent, note, data).GetAwaiter().GetResult();

            var jobProgress = jobCache.GetCachedProgress(jobID);
            jobCache.DeleteCachedProgressAsync(jobID).GetAwaiter().GetResult();

            Assert.IsNotNull(jobProgress);
            Assert.AreEqual(jobID, jobProgress.JobID);
            Assert.AreEqual(percent, jobProgress.Percent);
            Assert.AreEqual(note, jobProgress.Note);
            Assert.AreEqual(data, jobProgress.Data);
        }

        [TestMethod]
        public async Task GetCachedProgressAsyncTest()
        {
            var jobID = "1Test";
            var note = "progress note";
            var data = "progress data";
            var percent = 100;
            await jobCache.SetCachedProgressAsync(jobID, percent, note, data);

            var jobProgress = await jobCache.GetCachedProgressAsync(jobID);
            await jobCache.DeleteCachedProgressAsync(jobID);

            Assert.IsNotNull(jobProgress);
            Assert.AreEqual(jobID, jobProgress.JobID);
            Assert.AreEqual(percent, jobProgress.Percent);
            Assert.AreEqual(note, jobProgress.Note);
            Assert.AreEqual(data, jobProgress.Data);
        }

        [TestMethod]
        public async Task SetCachedProgressAsyncTest()
        {
            var jobID = "1Test";
            var note = "progress note";
            var data = "progress data";
            var percent = 100;
            await jobCache.SetCachedProgressAsync(jobID, percent, note, data);

            var jobProgress = await jobCache.GetCachedProgressAsync(jobID);
            await jobCache.DeleteCachedProgressAsync(jobID);

            Assert.IsNotNull(jobProgress);
            Assert.AreEqual(jobID, jobProgress.JobID);
            Assert.AreEqual(percent, jobProgress.Percent);
            Assert.AreEqual(note, jobProgress.Note);
            Assert.AreEqual(data, jobProgress.Data);
        }

        [TestMethod]
        public async Task SetCachedProgressStatusAsyncTest()
        {
            var jobID = "1Test";
            var note = "progress note";
            var data = "progress data";
            var percent = 100;
            var jsProgress = new JobStatusProgress()
            {
                JobID = jobID,
                Note = note,
                Data = data,
                Percent = percent,
                Status = null
            };
            await jobCache.SetCachedProgressStatusAsync(jsProgress, JobStatus.Stopped);

            var jobProgress = await jobCache.GetCachedProgressAsync(jobID);
            await jobCache.DeleteCachedProgressAsync(jobID);

            Assert.IsNotNull(jobProgress);
            Assert.AreEqual(jobID, jobProgress.JobID);
            Assert.AreEqual(percent, jobProgress.Percent);
            Assert.AreEqual(note, jobProgress.Note);
            Assert.AreEqual(data, jobProgress.Data);
            Assert.AreEqual(JobStatus.Stopped, jobProgress.Status);
        }

        [TestMethod]
        public async Task SetCachedProgressErrorAsyncTest()
        {
            var jobID = "1Test";
            var note = "progress note";
            var data = "progress data";
            var percent = 100;
            var error = "progress error";
            var jsProgress = new JobStatusProgress()
            {
                JobID = jobID,
                Note = note,
                Data = data,
                Percent = percent,
                Status = null
            };
            await jobCache.SetCachedProgressErrorAsync(jsProgress, error);

            var jobProgress = await jobCache.GetCachedProgressAsync(jobID);
            await jobCache.DeleteCachedProgressAsync(jobID);

            Assert.IsNotNull(jobProgress);
            Assert.AreEqual(jobID, jobProgress.JobID);
            Assert.AreEqual(percent, jobProgress.Percent);
            Assert.AreEqual(note, jobProgress.Note);
            Assert.AreEqual(data, jobProgress.Data);
            Assert.AreEqual(JobStatus.Error, jobProgress.Status);
            Assert.AreEqual(error, jobProgress.Error);
        }

        [TestMethod]
        public async Task DeleteCachedProgressAsync()
        {
            var jobID = "1Test";
            var note = "progress note";
            var data = "progress data";
            var percent = 100;
            var jsProgress = new JobStatusProgress()
            {
                JobID = jobID,
                Note = note,
                Data = data,
                Percent = percent,
                Status = null
            };
            await jobCache.SetCachedProgressAsync(jobID, percent, note, data);

            await jobCache.DeleteCachedProgressAsync(jobID);

            Task.Delay(3000).GetAwaiter().GetResult();
            var jobProgress =await jobCache.GetCachedProgressAsync(jobID);

            Assert.IsNull(jobProgress);
        }
    }
}
