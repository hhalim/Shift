using System;
using Xunit;
using Shift.Entities;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Shift.Cache.Redis;
using System.Threading.Tasks;

namespace Shift.UnitTest.DataLayer
{

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

        [Fact]
        public void GetCachedProgressTest()
        {
            var jobID = "1Test";
            var note = "progress note";
            var data = "progress data";
            var percent = 100;
            jobCache.SetCachedProgressAsync(jobID, percent, note, data).GetAwaiter().GetResult();

            var jobProgress = jobCache.GetCachedProgress(jobID);
            jobCache.DeleteCachedProgressAsync(jobID).GetAwaiter().GetResult();

            Assert.NotNull(jobProgress);
            Assert.Equal(jobID, jobProgress.JobID);
            Assert.Equal(percent, jobProgress.Percent);
            Assert.Equal(note, jobProgress.Note);
            Assert.Equal(data, jobProgress.Data);
        }

        [Fact]
        public async Task GetCachedProgressAsyncTest()
        {
            var jobID = "1Test";
            var note = "progress note";
            var data = "progress data";
            var percent = 100;
            await jobCache.SetCachedProgressAsync(jobID, percent, note, data);

            var jobProgress = await jobCache.GetCachedProgressAsync(jobID);
            await jobCache.DeleteCachedProgressAsync(jobID);

            Assert.NotNull(jobProgress);
            Assert.Equal(jobID, jobProgress.JobID);
            Assert.Equal(percent, jobProgress.Percent);
            Assert.Equal(note, jobProgress.Note);
            Assert.Equal(data, jobProgress.Data);
        }

        [Fact]
        public async Task SetCachedProgressAsyncTest()
        {
            var jobID = "1Test";
            var note = "progress note";
            var data = "progress data";
            var percent = 100;
            await jobCache.SetCachedProgressAsync(jobID, percent, note, data);

            var jobProgress = await jobCache.GetCachedProgressAsync(jobID);
            await jobCache.DeleteCachedProgressAsync(jobID);

            Assert.NotNull(jobProgress);
            Assert.Equal(jobID, jobProgress.JobID);
            Assert.Equal(percent, jobProgress.Percent);
            Assert.Equal(note, jobProgress.Note);
            Assert.Equal(data, jobProgress.Data);
        }

        [Fact]
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

            Assert.NotNull(jobProgress);
            Assert.Equal(jobID, jobProgress.JobID);
            Assert.Equal(percent, jobProgress.Percent);
            Assert.Equal(note, jobProgress.Note);
            Assert.Equal(data, jobProgress.Data);
            Assert.Equal(JobStatus.Stopped, jobProgress.Status);
        }

        [Fact]
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

            Assert.NotNull(jobProgress);
            Assert.Equal(jobID, jobProgress.JobID);
            Assert.Equal(percent, jobProgress.Percent);
            Assert.Equal(note, jobProgress.Note);
            Assert.Equal(data, jobProgress.Data);
            Assert.Equal(JobStatus.Error, jobProgress.Status);
            Assert.Equal(error, jobProgress.Error);
        }

        [Fact]
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

            Assert.Null(jobProgress);
        }
    }
}
