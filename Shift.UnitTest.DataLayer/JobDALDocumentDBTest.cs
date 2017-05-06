using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shift.Entities;
using System.Collections.Generic;
using Shift.DataLayer;
using System.Configuration;
using System.Linq;

namespace Shift.UnitTest.DataLayer
{
    [TestClass]
    public class JobDALDocumentDBTest : JobDALDocumentDB
    {

        private static AppSettingsReader appSettingsReader = new AppSettingsReader();
        private const string AppID = "TestAppID";
        private readonly string processID;
        private static string connectionString = appSettingsReader.GetValue("DocumentDBUrl", typeof(string)) as string;
        private static string authKey = appSettingsReader.GetValue("DocumentDBAuthKey", typeof(string)) as string;
        private static string encryptionKey = "";

        public JobDALDocumentDBTest() :  base(connectionString, encryptionKey, authKey)
        {
            processID = this.ToString();
        }

        [TestMethod]
        public void DeleteTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            Delete(new List<string> { jobID });
            var job = GetJob(jobID);

            Assert.IsNull(job);
        }

        [TestMethod]
        public void GetJobTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = GetJob(jobID);
            Delete(new List<string> { jobID });

            Assert.IsInstanceOfType(job, typeof(Job));
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public void GetJobViewTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = GetJobView(jobID);
            Delete(new List<string> { jobID });

            Assert.IsInstanceOfType(job, typeof(JobView));
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public void AddTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            Delete(new List<string> { jobID });
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));
        }

        [TestMethod]
        public void UpdateTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var count = Update(jobID, AppID, "", "", "JobNameUpdated", () => Console.WriteLine("Hello World Test!"));

            var job = GetJob(jobID);
            Delete(new List<string> { jobID });
            Assert.IsTrue(count > 0);
            Assert.AreEqual("JobNameUpdated", job.JobName);
        }

        //Test auto delete older than 24 hours and Null(not started) status
        [TestMethod]
        public void DeleteAsyncOldJobsNotStarted()
        {
            var job = new Job();
            job.AppID = AppID;
            job.Created = DateTime.Now.AddHours(-48);
            job = SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = Delete(24, new List<JobStatus?> { null });
            var outJob = GetJob(job.JobID);

            Assert.IsTrue(count > 0);
            Assert.IsNull(outJob);
        }

        //Test auto delete older than 24 hours and with Error or Completed status
        [TestMethod]
        public void DeleteAsyncOldJobsErrorAndCompletedTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now.AddHours(-48),
                Status = JobStatus.Error,
                Error = "Test delete old job with status: Error"
            };
            job = SetJob(job);
            var job2 = new Job();
            job2.AppID = AppID;
            job2.Created = DateTime.Now.AddHours(-48);
            job2.Status = JobStatus.Completed;
            job2.Error = "Test delete old job with status: Completed";
            job2 = SetJob(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var count = Delete(24, new List<JobStatus?> { JobStatus.Error, JobStatus.Completed });
            Assert.IsTrue(count > 0);

            var outJob = GetJob(job.JobID);
            Assert.IsNull(outJob);

            var outJob2 = GetJob(job2.JobID);
            Assert.IsNull(outJob2);
        }

        [TestMethod]
        public void SetCommandStopTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            SetCommandStop(new List<string> { jobID });
            var job = GetJob(jobID);
            Delete(new List<string> { jobID });

            Assert.AreEqual(JobCommand.Stop, job.Command);
        }

        [TestMethod]
        public void SetCommandRunNowTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            SetCommandRunNow(new List<string> { jobID });
            var job = GetJob(jobID);
            Delete(new List<string> { jobID });

            Assert.AreEqual(JobCommand.RunNow, job.Command);
        }

        [TestMethod]
        public void ResetTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Command = JobCommand.Stop
            };
            job = SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            Reset(new List<string> { job.JobID });
            var outJob = GetJob(job.JobID);
            Delete(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(string.IsNullOrWhiteSpace(outJob.Command));
        }

        [TestMethod]
        public void SetToStoppedTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Command = JobCommand.Stop
            };
            job = SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = SetToStopped(new List<string> { job.JobID });
            var outJob = GetJob(job.JobID);
            Delete(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(count == 1);
            Assert.IsTrue(string.IsNullOrWhiteSpace(outJob.Command));
            Assert.AreEqual(JobStatus.Stopped, outJob.Status);
        }

        //Get Multiple jobs
        [TestMethod]
        public void GetJobsTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = GetJobs(new List<string> { jobID, jobID2 });
            Delete(new List<string> { jobID, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobs.Count == 2);
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobIDs.Contains(jobID2));
        }

        [TestMethod]
        public void GetNonRunningJobsByIDsTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = GetNonRunningJobsByIDs(new List<string> { jobID, jobID2 });
            Delete(new List<string> { jobID, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobs.Count == 2);
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobIDs.Contains(jobID2));
        }

        [TestMethod]
        public void GetJobIdsByProcessAndCommandTest()
        {
            var job = new Job
            {
                AppID = AppID,
                ProcessID = processID,
                Created = DateTime.Now,
                Command = JobCommand.Stop
            };
            job = SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var outJobIDs = GetJobIdsByProcessAndCommand(processID, JobCommand.Stop);
            Delete(new List<string> { job.JobID });

            Assert.IsTrue(outJobIDs.Contains(job.JobID));
            Assert.IsTrue(outJobIDs.Count >= 1);
        }

        [TestMethod]
        public void GetJobsByProcessAndStatusTest()
        {
            var job = new Job
            {
                AppID = AppID,
                ProcessID = processID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job = SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var outJobs = GetJobsByProcessAndStatus(processID, JobStatus.Stopped);
            Delete(new List<string> { job.JobID });

            var jobIDs = outJobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(outJobs.Count >= 1);
            Assert.IsTrue(jobIDs.Contains(job.JobID));
        }

        [TestMethod]
        public void GetJobViewsTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = GetJobViews(1, 10);
            Delete(new List<string> { jobID, jobID2 });

            Assert.IsTrue(jobs.Total >= 2);
            var jobIDs = jobs.Items.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobIDs.Contains(jobID2));
        }


        [TestMethod]
        public void GetJobViewsAsyncTest2()
        {
            var jobID1 = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs1 = GetJobViews(1, 1);
            var jobs2 = GetJobViews(2, 1);
            Delete(new List<string> { jobID1, jobID2 });

            Assert.IsTrue(jobs1.Total >= 2);
            var jobIDs1 = jobs1.Items.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs1.Contains(jobID1));

            Assert.IsTrue(jobs2.Total >= 2);
            var jobIDs2 = jobs2.Items.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs2.Contains(jobID2));
        }

        [TestMethod]
        public void SetToRunningTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID
            };
            job = SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = SetToRunning(job.ProcessID, job.JobID);
            var outJob = GetJob(job.JobID);

            //set to stop before delete
            job.Status = JobStatus.Stopped;
            job = SetJob(job);
            Delete(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(count == 1);
            Assert.AreEqual(JobStatus.Running, outJob.Status);
        }

        [TestMethod]
        public void SetErrorTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID
            };
            job = SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var error = "Test Error";
            var count = SetError(job.ProcessID, job.JobID, error);
            var outJob = GetJob(job.JobID);

            Delete(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(count == 1);
            Assert.AreEqual(JobStatus.Error, outJob.Status);
            Assert.AreEqual(error, outJob.Error);
        }

        [TestMethod]
        public void SetCompletedTest()
        {
            var job = new Job();
            job.AppID = AppID;
            job.Created = DateTime.Now;
            job.Status = null;
            job.ProcessID = processID;
            job = SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = SetCompleted(job.ProcessID, job.JobID);
            var outJob = GetJob(job.JobID);

            Delete(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(count == 1);
            Assert.AreEqual(JobStatus.Completed, outJob.Status);
        }

        [TestMethod]
        public void CountRunningJobsTest()
        {
            var job1 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = JobStatus.Running,
                ProcessID = processID
            };
            job1 = SetJob(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));
            var job2 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = JobStatus.Running,
                ProcessID = processID
            };
            job2 = SetJob(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var count = CountRunningJobs(processID);

            //set to stop before delete
            job1.Status = JobStatus.Stopped;
            job1 = SetJob(job1);
            job2.Status = JobStatus.Stopped;
            job2 = SetJob(job2);
            Delete(new List<string> { job1.JobID, job2.JobID });

            Assert.IsTrue(count >= 2);
        }

        [TestMethod]
        public void ClaimJobsToRunTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = null
            };
            job = SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var jobs = ClaimJobsToRun(processID, new List<Job> { job });
            var outJob = GetJob(job.JobID);

            Delete(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.AreEqual(processID, outJob.ProcessID);
            Assert.IsTrue(jobIDs.Contains(job.JobID));
            Assert.IsTrue(jobs.Count == 1);
        }

        //Don't claim running jobs
        [TestMethod]
        public void ClaimJobsToRunAsyncTest2()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = JobStatus.Running,
                ProcessID = null
            };
            job = SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var jobs = ClaimJobsToRun(processID, new List<Job> { job });
            var outJob = GetJob(job.JobID);

            //set to stop before delete
            job.Status = JobStatus.Stopped;
            job = SetJob(job);
            Delete(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.AreNotEqual(processID, outJob.ProcessID);
            Assert.IsTrue(!jobIDs.Contains(job.JobID));
            Assert.IsTrue(jobs.Count == 0);
        }

        //Don't claim jobs already claimed by someone else
        [TestMethod]
        public void ClaimJobsToRunAsyncTest3()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID + "-someoneElseTest"
            };
            job = SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var jobs = ClaimJobsToRun(processID, new List<Job> { job });
            var outJob = GetJob(job.JobID);

            Delete(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.AreNotEqual(processID, outJob.ProcessID);
            Assert.IsTrue(!jobIDs.Contains(job.JobID));
            Assert.IsTrue(jobs.Count == 0);
        }

        [TestMethod]
        public void GetJobsToRunTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            var jobs = GetJobsToRun(1);

            Delete(new List<string> { jobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobs.Count == 1);
        }

        //Get run-now job first
        [TestMethod]
        public void GetJobsToRunAsyncTest2()
        {
            var jobID1 = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID1));
            var jobID2 = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID2));

            SetCommandRunNow(new List<string> { jobID2 });
            var jobs = GetJobsToRun(1);

            Delete(new List<string> { jobID1, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(jobID2));
            Assert.IsTrue(jobs.Count == 1);
        }

        //Should return no jobs that was added
        [TestMethod]
        public void GetJobsToRunAsyncTest3()
        {
            //procesID != null
            var job1 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID
            };
            job1 = SetJob(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped,
                ProcessID = null
            };
            job2 = SetJob(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            //command != null
            var job3 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = null,
                Command = JobCommand.Stop
            };
            job3 = SetJob(job3);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job3.JobID));

            var jobs = GetJobsToRun(3);

            Delete(new List<string> { job1.JobID, job2.JobID, job3.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(!jobIDs.Contains(job1.JobID));
            Assert.IsTrue(!jobIDs.Contains(job2.JobID));
            Assert.IsTrue(!jobIDs.Contains(job3.JobID));
        }

        [TestMethod]
        public void SetProgressTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            var note = "progress note";
            var data = "progress data";
            var percent = 50;
            var count = SetProgress(jobID, percent, note, data);
            var job = GetJobView(jobID);

            Delete(new List<string> { jobID });

            Assert.IsTrue(count == 1);
            Assert.AreEqual(percent, job.Percent);
            Assert.AreEqual(note, job.Note);
            Assert.AreEqual(data, job.Data);
        }

        [TestMethod]
        public void GetProgressTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            var note = "progress note";
            var data = "progress data";
            var percent = 50;
            SetProgress(jobID, percent, note, data);

            var progress = GetProgress(jobID);

            Delete(new List<string> { jobID });

            Assert.AreEqual(percent, progress.Percent);
            Assert.AreEqual(note, progress.Note);
            Assert.AreEqual(data, progress.Data);
        }

        [TestMethod]
        public void GetCachedProgressTest()
        {
            var jobID = Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            var note = "progress note";
            var data = "progress data";
            var percent = 50;
            SetProgress(jobID, percent, note, data);

            var progress = GetCachedProgress(jobID);

            Delete(new List<string> { jobID });

            Assert.AreEqual(percent, progress.Percent);
            Assert.AreEqual(note, progress.Note);
            Assert.AreEqual(data, progress.Data);
        }

    }
}
