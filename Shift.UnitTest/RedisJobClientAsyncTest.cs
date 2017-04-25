using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shift.Entities;
using System.Threading.Tasks;

namespace Shift.UnitTest
{
    [TestClass]
    public class RedisJobClientAsyncTest
    {
        JobClient jobClient;
        const string appID = "TestAppID";

        public RedisJobClientAsyncTest()
        {
            //Configure storage connection
            var config = new ClientConfig();
            config.DBConnectionString = "localhost:6379";
            config.StorageMode = "redis";
            jobClient = new JobClient(config);
        }

        [TestMethod]
        public async Task GetJobAsyncInvalidTest()
        {
            var job = await jobClient.GetJobAsync("-ZZTOP");

            Assert.IsNull(job);
        }

        [TestMethod]
        public async Task GetJobAsyncValidTest()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(job);
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public async Task AddAsyncJobTest1()
        {
            var jobID = await jobClient.AddAsync(() => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(job);
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public async Task AddAsyncJobTest2()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(job);
            Assert.AreEqual(jobID, job.JobID);
            Assert.AreEqual(appID, job.AppID);
        }

        [TestMethod]
        public async Task AddAsyncJobTest3()
        {
            var jobID = await jobClient.AddAsync(appID, "-123", "TestJobType", () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(job);
            Assert.AreEqual(jobID, job.JobID);
            Assert.AreEqual(appID, job.AppID);
            Assert.AreEqual("-123", job.UserID);
            Assert.AreEqual("TestJobType", job.JobType);
        }

        [TestMethod]
        public async Task AddAsyncJobTest4()
        {
            var jobID = await jobClient.AddAsync(appID, "-123", "TestJobType", "Test.JobName", () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(job);
            Assert.AreEqual(jobID, job.JobID);
            Assert.AreEqual(appID, job.AppID);
            Assert.AreEqual("-123", job.UserID);
            Assert.AreEqual("TestJobType", job.JobType);
            Assert.AreEqual("Test.JobName", job.JobName);
        }

        [TestMethod]
        public async Task UpdateAsyncJobTest1()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            await jobClient.UpdateAsync(jobID, () => Console.WriteLine("Hello Test Updated"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(job);
            Assert.AreEqual("[\"\\\"Hello Test Updated\\\"\"]", job.Parameters);
        }

        [TestMethod]
        public async Task UpdateAsyncJobTest2()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            await jobClient.UpdateAsync(jobID, "TestAppIDUpdated", () => Console.WriteLine("Hello Test Updated"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(job);
            Assert.AreEqual("TestAppIDUpdated", job.AppID);
            Assert.AreEqual("[\"\\\"Hello Test Updated\\\"\"]", job.Parameters);
        }

        [TestMethod]
        public async Task UpdateAsyncJobTest3()
        {
            var jobID = await jobClient.AddAsync(appID, "-123", "TestJobType", "Test.JobName", () => Console.WriteLine("Hello Test"));
            await jobClient.UpdateAsync(jobID, "TestAppIDUpdated", "-222", "TestJobTypeUpdated", "Test.JobNameUpdated", () => Console.WriteLine("Hello Test Updated"));
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(job);
            Assert.AreEqual("TestAppIDUpdated", job.AppID);
            Assert.AreEqual("-222", job.UserID);
            Assert.AreEqual("TestJobTypeUpdated", job.JobType);
            Assert.AreEqual("Test.JobNameUpdated", job.JobName);
            Assert.AreEqual("[\"\\\"Hello Test Updated\\\"\"]", job.Parameters);
        }

        [TestMethod]
        public async Task SetCommandStopAsyncTest()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(job);
            Assert.AreEqual(JobCommand.Stop, job.Command);
        }

        [TestMethod]
        public async Task SetCommandRunNowAsyncTest()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            await jobClient.SetCommandRunNowAsync(new List<string> { jobID });
            var job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(job);
            Assert.AreEqual(JobCommand.RunNow, job.Command);
        }

        [TestMethod]
        public async Task GetJobViewAsyncTest()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            var jobView = await jobClient.GetJobViewAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(jobView);
            Assert.AreEqual(jobID, jobView.JobID);
            Assert.IsInstanceOfType(jobView, typeof(JobView));
        }

        [TestMethod]
        public async Task GetJobViewsAsyncTest()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            var jobID2 = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test2"));
            var jobViews = await jobClient.GetJobViewsAsync(0, 10);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID, jobID2 });

            Assert.IsInstanceOfType(jobViews, typeof(JobViewList));
            Assert.IsNotNull(jobViews);
            Assert.IsNotNull(jobViews.Items);
            Assert.IsTrue(jobViews.Items.Count > 0);
            Assert.IsTrue(jobViews.Total > 0);
        }

        [TestMethod]
        public async Task ResetJobsAsyncTest()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            var job = await jobClient.GetJobAsync(jobID);

            Assert.IsNotNull(job);
            Assert.AreEqual(JobCommand.Stop, job.Command); //ensure it is set to 'stop' command

            //try to reset
            await jobClient.ResetJobsAsync(new List<string> { jobID });
            job = await jobClient.GetJobAsync(jobID);

            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.IsNotNull(job);
            Assert.IsTrue(string.IsNullOrWhiteSpace(job.Command));
        }

        [TestMethod]
        public async Task DeleteJobsAsyncTest()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);
            Assert.IsNotNull(job); //ensure it exists

            //try to delete
            await jobClient.DeleteJobsAsync(new List<string> { jobID });
            job = await jobClient.GetJobAsync(jobID);

            Assert.IsNull(job);
        }

        [TestMethod]
        public async Task GetJobStatusCountAsyncTest()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            var statusCount = await jobClient.GetJobStatusCountAsync(appID, null);
            await jobClient.DeleteJobsAsync(new List<string> { jobID });

            Assert.IsNotNull(statusCount); 
            Assert.IsTrue(statusCount.Count > 0);
        }

        [TestMethod]
        public async Task GetProgressAsyncTest()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            var progress = await jobClient.GetProgressAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string> { jobID });

            Assert.IsNotNull(progress);
            Assert.IsInstanceOfType(progress, typeof(JobStatusProgress));
            Assert.AreEqual(jobID, progress.JobID);
        }

        [TestMethod]
        public async Task GetCachedProgressAsyncTest()
        {
            var jobID = await jobClient.AddAsync(appID, () => Console.WriteLine("Hello Test"));
            var progress = await jobClient.GetCachedProgressAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string> { jobID });

            Assert.IsNotNull(progress);
            Assert.IsInstanceOfType(progress, typeof(JobStatusProgress));
            Assert.AreEqual(jobID, progress.JobID);
        }
    }
}
