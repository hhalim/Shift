using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shift.Entities;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Threading;
using Shift.DataLayer;
using System.Linq;

namespace Shift.UnitTest.DataLayer
{
    [TestClass]
    public class JobDALDocumentDBAsyncTest : JobDALDocumentDB
    {
        const string appID = "TestAppID";
        private string processID;
        private static string connectionString = "https://shiftdb.documents.azure.com:443/";
        private static string encryptionKey = "";
        private static string authKey = "fwkADWkatMitV3jg4kcNCVGdDRK9GRgfBG6jcS374yz2SQt0d9DXML6LI3v42HKiBr4dU4vXgayhPVl4U0PwSw==";

        public JobDALDocumentDBAsyncTest() : base(connectionString, encryptionKey, authKey)
        {
            processID = this.ToString();
        }

        [TestMethod]
        public async Task DeleteAsyncTest()
        {
            var jobID = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            await DeleteAsync(new List<string> { jobID });
            var job = await GetJobAsync(jobID);

            Assert.IsNull(job);
        }

        [TestMethod]
        public async Task GetJobAsyncTest()
        {
            var jobID = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = await GetJobAsync(jobID);
            await DeleteAsync(new List<string> { jobID });

            Assert.IsInstanceOfType(job, typeof(Job));
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public async Task GetJobViewAsyncTest()
        {
            var jobID = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = await GetJobViewAsync(jobID);
            await DeleteAsync(new List<string> { jobID });

            Assert.IsInstanceOfType(job, typeof(JobView));
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public async Task AddAsyncTest()
        {
            var jobID = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            await DeleteAsync(new List<string> { jobID });
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));
        }

        [TestMethod]
        public async Task UpdateAsyncTest()
        {
            var jobID = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var count = await UpdateAsync(jobID, appID, "", "", "JobNameUpdated", () => Console.WriteLine("Hello World Test!"));

            var job = await GetJobAsync(jobID);
            await DeleteAsync(new List<string> { jobID });
            Assert.IsTrue(count > 0);
            Assert.AreEqual("JobNameUpdated", job.JobName);
        }

        //Test auto delete older than 24 hours and Null(not started) status
        [TestMethod]
        public async Task DeleteAsyncOldJobsNotStarted()
        {
            var job = new Job();
            job.AppID = appID;
            job.Created = DateTime.Now.AddHours(-48);
            job = await SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = await DeleteAsync(24, new List<JobStatus?> { null });
            var outJob = await GetJobAsync(job.JobID);

            Assert.IsTrue(count > 0);
            Assert.IsNull(outJob);
        }

        //Test auto delete older than 24 hours and with Error or Completed status
        [TestMethod]
        public async Task DeleteAsyncOldJobsErrorAndCompletedTest()
        {
            var job = new Job();
            job.AppID = appID;
            job.Created = DateTime.Now.AddHours(-48);
            job.Status = JobStatus.Error;
            job.Error = "Test delete old job with status: Error";
            job = await SetJobAsync(job);
            var job2 = new Job();
            job2.AppID = appID;
            job2.Created = DateTime.Now.AddHours(-48);
            job2.Status = JobStatus.Completed;
            job2.Error = "Test delete old job with status: Completed";
            job2 = await SetJobAsync(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var count = await DeleteAsync(24, new List<JobStatus?> { JobStatus.Error, JobStatus.Completed });
            Assert.IsTrue(count > 0);

            var outJob = await GetJobAsync(job.JobID);
            Assert.IsNull(outJob);

            var outJob2 = await GetJobAsync(job2.JobID);
            Assert.IsNull(outJob2);
        }

        [TestMethod]
        public async Task SetCommandStopAsyncTest()
        {
            var jobID = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            await SetCommandStopAsync(new List<string> { jobID });
            var job = await GetJobAsync(jobID);
            await DeleteAsync(new List<string> { jobID });

            Assert.AreEqual(JobCommand.Stop, job.Command);
        }

        [TestMethod]
        public async Task SetCommandRunNowAsyncTest()
        {
            var jobID = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            await SetCommandRunNowAsync(new List<string> { jobID });
            var job = await GetJobAsync(jobID);
            await DeleteAsync(new List<string> { jobID });

            Assert.AreEqual(JobCommand.RunNow, job.Command);
        }

        [TestMethod]
        public async Task ResetAsyncTest()
        {
            var job = new Job();
            job.AppID = appID;
            job.Created = DateTime.Now;
            job.Command = JobCommand.Stop;
            job = await SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            await ResetAsync(new List<string> { job.JobID });
            var outJob = await GetJobAsync(job.JobID);
            await DeleteAsync(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(string.IsNullOrWhiteSpace(outJob.Command));
        }

        [TestMethod]
        public async Task SetToStoppedAsyncTest()
        {
            var job = new Job();
            job.AppID = appID;
            job.Created = DateTime.Now;
            job.Command = JobCommand.Stop;
            job = await SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = await SetToStoppedAsync(new List<string> { job.JobID });
            var outJob = await GetJobAsync(job.JobID);
            await DeleteAsync(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(count == 1);
            Assert.IsTrue(string.IsNullOrWhiteSpace(outJob.Command));
            Assert.AreEqual(JobStatus.Stopped, outJob.Status);
        }

        //Get Multiple jobs
        [TestMethod]
        public async Task GetJobsAsyncTest()
        {
            var jobID = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = await GetJobsAsync(new List<string> { jobID, jobID2 });
            await DeleteAsync(new List<string> { jobID, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobs.Count == 2);
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobIDs.Contains(jobID2));
        }

        [TestMethod]
        public async Task GetNonRunningJobsByIDsAsyncTest()
        {
            var jobID = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = await GetNonRunningJobsByIDsAsync(new List<string> { jobID, jobID2 });
            await DeleteAsync(new List<string> { jobID, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobs.Count == 2);
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobIDs.Contains(jobID2));
        }

        [TestMethod]
        public async Task GetJobIdsByProcessAndCommandAsyncTest()
        {
            var job = new Job();
            job.AppID = appID;
            job.ProcessID = processID;
            job.Created = DateTime.Now;
            job.Command = JobCommand.Stop;
            job = await SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var outJobIDs = await GetJobIdsByProcessAndCommandAsync(processID, JobCommand.Stop);
            await DeleteAsync(new List<string> { job.JobID });

            Assert.IsTrue(outJobIDs.Contains(job.JobID));
            Assert.IsTrue(outJobIDs.Count >= 1);
        }

        [TestMethod]
        public async Task GetJobsByProcessAndStatusAsyncTest()
        {
            var job = new Job();
            job.AppID = appID;
            job.ProcessID = processID;
            job.Created = DateTime.Now;
            job.Status = JobStatus.Stopped;
            job = await SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var outJobs = await GetJobsByProcessAndStatusAsync(processID, JobStatus.Stopped);
            await DeleteAsync(new List<string> { job.JobID });

            var jobIDs = outJobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(outJobs.Count >= 1);
            Assert.IsTrue(jobIDs.Contains(job.JobID));
        }

        [TestMethod]
        public async Task GetJobViewsAsyncTest()
        {
            var jobID = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = await GetJobViewsAsync(1, 10);
            await DeleteAsync(new List<string> { jobID, jobID2 });

            Assert.IsTrue(jobs.Total >= 2);
            var jobIDs = jobs.Items.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobIDs.Contains(jobID2));
        }


        [TestMethod]
        public async Task GetJobViewsAsyncTest2()
        {
            var jobID1 = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await AddAsync(appID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs1 = await GetJobViewsAsync(1, 1);
            var jobs2 = await GetJobViewsAsync(2, 1);
            await DeleteAsync(new List<string> { jobID1, jobID2 });

            Assert.IsTrue(jobs1.Total >= 2);
            var jobIDs1 = jobs1.Items.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs1.Contains(jobID1));

            Assert.IsTrue(jobs2.Total >= 2);
            var jobIDs2 = jobs2.Items.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs2.Contains(jobID2));
        }

    }
}
