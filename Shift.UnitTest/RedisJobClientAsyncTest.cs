using System;
using System.Text;
using System.Collections.Generic;
using System.Configuration;
using Xunit;
using Shift.Entities;
using System.Threading.Tasks;

namespace Shift.UnitTest
{
     
    public class RedisJobClientAsyncTest
    {
        JobClient jobClient;
        private const string AppID = "TestAppID";

        public RedisJobClientAsyncTest()
        {
            var appSettingsReader = new AppSettingsReader();

            //Configure storage connection
            var config = new ClientConfig();
            config.DBConnectionString = appSettingsReader.GetValue("RedisConnectionString", typeof(string)) as string;
            config.StorageMode = "redis";
            jobClient = new JobClient(config);
        }

        [Fact]
        public async Task GetJobAsyncInvalidTest()
        {
            var job = await jobClient.GetJobAsync("-ZZTOP");

            Assert.Null(job);
        }

        [Fact]
        public async Task GetJobAsyncValidTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
        }

        [Fact]
        public async Task AddAsyncJobTest1()
        {
            var jobID = await jobClient.AddAsync(() => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
        }

        [Fact]
        public async Task AddAsyncJobTest2()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
            Assert.Equal(AppID, job.AppID);
        }

        [Fact]
        public async Task AddAsyncJobTest3()
        {
            var jobID = await jobClient.AddAsync(AppID, "-123", "TestJobType", () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
            Assert.Equal(AppID, job.AppID);
            Assert.Equal("-123", job.UserID);
            Assert.Equal("TestJobType", job.JobType);
        }

        [Fact]
        public async Task AddAsyncJobTest4()
        {
            var jobID = await jobClient.AddAsync(AppID, "-123", "TestJobType", "Test.JobName", () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
            Assert.Equal(AppID, job.AppID);
            Assert.Equal("-123", job.UserID);
            Assert.Equal("TestJobType", job.JobType);
            Assert.Equal("Test.JobName", job.JobName);
        }

        [Fact]
        public async Task UpdateAsyncJobTest1()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            await jobClient.UpdateAsync(jobID, () => Console.WriteLine("Hello Test Updated"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal("[\"\\\"Hello Test Updated\\\"\"]", job.Parameters);
        }

        [Fact]
        public async Task UpdateAsyncJobTest2()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            await jobClient.UpdateAsync(jobID, "TestAppIDUpdated", () => Console.WriteLine("Hello Test Updated"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal("TestAppIDUpdated", job.AppID);
            Assert.Equal("[\"\\\"Hello Test Updated\\\"\"]", job.Parameters);
        }

        [Fact]
        public async Task UpdateAsyncJobTest3()
        {
            var jobID = await jobClient.AddAsync(AppID, "-123", "TestJobType", "Test.JobName", () => Console.WriteLine("Hello Test"));
            await jobClient.UpdateAsync(jobID, "TestAppIDUpdated", "-222", "TestJobTypeUpdated", "Test.JobNameUpdated", () => Console.WriteLine("Hello Test Updated"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal("TestAppIDUpdated", job.AppID);
            Assert.Equal("-222", job.UserID);
            Assert.Equal("TestJobTypeUpdated", job.JobType);
            Assert.Equal("Test.JobNameUpdated", job.JobName);
            Assert.Equal("[\"\\\"Hello Test Updated\\\"\"]", job.Parameters);
        }

        [Fact]
        public async Task SetCommandStopAsyncTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(JobCommand.Stop, job.Command);
        }

        [Fact]
        public async Task SetCommandRunNowAsyncTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            await jobClient.SetCommandRunNowAsync(new List<string> { jobID });
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(JobCommand.RunNow, job.Command);
        }

        [Fact]
        public async Task GetJobViewAsyncTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            var jobView = await jobClient.GetJobViewAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(jobView);
            Assert.Equal(jobID, jobView.JobID);
            Assert.IsType<JobView>(jobView);
        }

        [Fact]
        public async Task GetJobViewsAsyncTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            var jobID2 = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test2"));
            var jobViews = await jobClient.GetJobViewsAsync(0, 10);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID, jobID2 });

            Assert.IsType<JobViewList>(jobViews);
            Assert.NotNull(jobViews);
            Assert.NotNull(jobViews.Items);
            Assert.True(jobViews.Items.Count > 0);
            Assert.True(jobViews.Total > 0);
        }

        [Fact]
        public async Task ResetJobsAsyncTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            var job = await jobClient.GetJobAsync(jobID);

            Assert.NotNull(job);
            Assert.Equal(JobCommand.Stop, job.Command); //ensure it is set to 'stop' command

            //try to reset
            await jobClient.ResetJobsAsync(new List<string> { jobID });
            job = await jobClient.GetJobAsync(jobID);

            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.True(string.IsNullOrWhiteSpace(job.Command));
        }

        [Fact]
        public async Task DeleteJobsAsyncTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            Assert.NotNull(job); //ensure it exists

            //try to delete
            await jobClient.DeleteJobsAsync(new List<string> { jobID });
            job = await jobClient.GetJobAsync(jobID);

            Assert.Null(job);
        }

        [Fact]
        public async Task GetJobStatusCountAsyncTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            var statusCount = await jobClient.GetJobStatusCountAsync(AppID, null);
            await jobClient.DeleteJobsAsync(new List<string> { jobID });

            Assert.NotNull(statusCount); 
            Assert.True(statusCount.Count > 0);
        }

        [Fact]
        public async Task GetProgressAsyncTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            var progress = await jobClient.GetProgressAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string> { jobID });

            Assert.NotNull(progress);
            Assert.IsType<JobStatusProgress>(progress);
            Assert.Equal(jobID, progress.JobID);
        }

        [Fact]
        public async Task GetCachedProgressAsyncTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            var progress = await jobClient.GetCachedProgressAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string> { jobID });

            Assert.NotNull(progress);
            Assert.IsType<JobStatusProgress>(progress);
            Assert.Equal(jobID, progress.JobID);
        }
    }
}
