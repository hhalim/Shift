using System;
using System.Text;
using System.Collections.Generic;
using System.Configuration;
using Xunit;
using Shift.Entities;

namespace Shift.UnitTest
{
     
    public class SqlJobClientTest
    {
        JobClient jobClient;
        private const string AppID = "TestAppID";

        public SqlJobClientTest()
        {
            var appSettingsReader = new AppSettingsReader();

            //Configure storage connection
            var config = new ClientConfig();
            config.DBConnectionString = appSettingsReader.GetValue("MSSqlConnectionString", typeof(string)) as string;
            config.StorageMode = "mssql";
            jobClient = new JobClient(config);
        }

        [Fact]
        public void GetJobInvalidTest()
        {
            var job = jobClient.GetJob("-123");

            Assert.Null(job);
        }

        [Fact]
        public void GetJobValidTest()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            var job = jobClient.GetJob(jobID);
            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
        }

        [Fact]
        public void AddJobTest1()
        {
            var jobID = jobClient.Add(() => Console.WriteLine("Hello Test"));
            var job = jobClient.GetJob(jobID);
            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
        }

        [Fact]
        public void AddJobTest2()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            var job = jobClient.GetJob(jobID);
            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
            Assert.Equal(AppID, job.AppID);
        }

        [Fact]
        public void AddJobTest3()
        {
            var jobID = jobClient.Add(AppID, "-123", "TestJobType", () => Console.WriteLine("Hello Test"));
            var job = jobClient.GetJob(jobID);
            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
            Assert.Equal(AppID, job.AppID);
            Assert.Equal("-123", job.UserID);
            Assert.Equal("TestJobType", job.JobType);
        }

        [Fact]
        public void AddJobTest4()
        {
            var jobID = jobClient.Add(AppID, "-123", "TestJobType", "Test.JobName", () => Console.WriteLine("Hello Test"));
            var job = jobClient.GetJob(jobID);
            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);
            Assert.Equal(AppID, job.AppID);
            Assert.Equal("-123", job.UserID);
            Assert.Equal("TestJobType", job.JobType);
            Assert.Equal("Test.JobName", job.JobName);
        }

        [Fact]
        public void UpdateJobTest1()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            jobClient.Update(jobID, () => Console.WriteLine("Hello Test Updated"));
            var job = jobClient.GetJob(jobID);
            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal("[\"\\\"Hello Test Updated\\\"\"]", job.Parameters);
        }

        [Fact]
        public void UpdateJobTest2()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            jobClient.Update(jobID, "TestAppIDUpdated", () => Console.WriteLine("Hello Test Updated"));
            var job = jobClient.GetJob(jobID);
            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal("TestAppIDUpdated", job.AppID);
            Assert.Equal("[\"\\\"Hello Test Updated\\\"\"]", job.Parameters);
        }

        [Fact]
        public void UpdateJobTest3()
        {
            var jobID = jobClient.Add(AppID, "-123", "TestJobType", "Test.JobName", () => Console.WriteLine("Hello Test"));
            jobClient.Update(jobID, "TestAppIDUpdated", "-222", "TestJobTypeUpdated", "Test.JobNameUpdated", () => Console.WriteLine("Hello Test Updated"));
            var job = jobClient.GetJob(jobID);
            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal("TestAppIDUpdated", job.AppID);
            Assert.Equal("-222", job.UserID);
            Assert.Equal("TestJobTypeUpdated", job.JobType);
            Assert.Equal("Test.JobNameUpdated", job.JobName);
            Assert.Equal("[\"\\\"Hello Test Updated\\\"\"]", job.Parameters);
        }

        [Fact]
        public void SetCommandStopTest()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            jobClient.SetCommandStop(new List<string> { jobID });
            var job = jobClient.GetJob(jobID);
            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(JobCommand.Stop, job.Command);
        }

        [Fact]
        public void SetCommandRunNowTest()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            jobClient.SetCommandRunNow(new List<string> { jobID });
            var job = jobClient.GetJob(jobID);
            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.Equal(JobCommand.RunNow, job.Command);
        }

        [Fact]
        public void GetJobViewTest()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            var jobView = jobClient.GetJobView(jobID);
            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(jobView);
            Assert.Equal(jobID, jobView.JobID);
            Assert.IsType<JobView>(jobView);
        }

        [Fact]
        public void GetJobViewsTest()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            var jobID2 = jobClient.Add(AppID, () => Console.WriteLine("Hello Test2"));
            var jobViews = jobClient.GetJobViews(0, 10);
            jobClient.DeleteJobs(new List<string>() { jobID, jobID2 });

            Assert.IsType<JobViewList>(jobViews);
            Assert.NotNull(jobViews);
            Assert.NotNull(jobViews.Items);
            Assert.True(jobViews.Items.Count > 0);
            Assert.True(jobViews.Total > 0);
        }

        [Fact]
        public void ResetJobsTest()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            jobClient.SetCommandStop(new List<string> { jobID });
            var job = jobClient.GetJob(jobID);
            Assert.NotNull(job);
            Assert.Equal(JobCommand.Stop, job.Command); //ensure it is set to 'stop' command

            //try to reset
            jobClient.ResetJobs(new List<string> { jobID });
            job = jobClient.GetJob(jobID);

            jobClient.DeleteJobs(new List<string>() { jobID });

            Assert.NotNull(job);
            Assert.True(string.IsNullOrWhiteSpace(job.Command));
        }

        [Fact]
        public void DeleteJobsTest()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            var job = jobClient.GetJob(jobID);
            Assert.NotNull(job); //ensure it exists

            //try to delete
            jobClient.DeleteJobs(new List<string> { jobID });
            job = jobClient.GetJob(jobID);

            Assert.Null(job);
        }

        [Fact]
        public void GetJobStatusCountTest()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            var statusCount = jobClient.GetJobStatusCount(AppID, null);
            jobClient.DeleteJobs(new List<string> { jobID });

            Assert.NotNull(statusCount); 
            Assert.True(statusCount.Count > 0);
        }

        [Fact]
        public void GetProgressTest()
        {
            var jobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));
            var progress = jobClient.GetProgress(jobID);
            jobClient.DeleteJobs(new List<string> { jobID });

            Assert.NotNull(progress);
            Assert.IsType<JobStatusProgress>(progress);
            Assert.Equal(jobID, progress.JobID);
        }

        public void GetCachedProgressTest()
        {
            //Note: Unable to test, no progress data in Redis, since no running job.
        }
    }
}
