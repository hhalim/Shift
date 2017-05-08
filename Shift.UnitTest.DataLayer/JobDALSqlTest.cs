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
    public class JobDALSqlTest
    {
        private static AppSettingsReader appSettingsReader = new AppSettingsReader();
        private const string AppID = "TestAppID";
        private readonly string processID;
        private static string connectionString = appSettingsReader.GetValue("MSSqlConnectionString", typeof(string)) as string;
        private const string encryptionKey = "";
        JobDALSql jobDAL;

        public JobDALSqlTest()
        {
            processID = this.ToString();
            this.jobDAL = new JobDALSql(connectionString, encryptionKey);
        }

        [TestMethod]
        public void DeleteTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            jobDAL.Delete(new List<string> { jobID });
            var job = jobDAL.GetJob(jobID);

            Assert.IsNull(job);
        }

        [TestMethod]
        public void GetJobTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = jobDAL.GetJob(jobID);
            jobDAL.Delete(new List<string> { jobID });

            Assert.IsInstanceOfType(job, typeof(Job));
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public void GetJobViewTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = jobDAL.GetJobView(jobID);
            jobDAL.Delete(new List<string> { jobID });

            Assert.IsInstanceOfType(job, typeof(JobView));
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public void AddTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            jobDAL.Delete(new List<string> { jobID });
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));
        }

        [TestMethod]
        public void UpdateTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var count = jobDAL.Update(jobID, AppID, "", "", "JobNameUpdated", () => Console.WriteLine("Hello World Test!"));

            var job = jobDAL.GetJob(jobID);
            jobDAL.Delete(new List<string> { jobID });
            Assert.IsTrue(count > 0);
            Assert.AreEqual("JobNameUpdated", job.JobName);
        }

        //Test auto delete older than 24 hours and Null(not started) status
        [TestMethod]
        public void DeleteOldJobsNotStarted()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now.AddHours(-48)
            };
            job = jobDAL.SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = jobDAL.Delete(24, new List<JobStatus?> { null });
            var outJob = jobDAL.GetJob(job.JobID);

            Assert.IsTrue(count > 0);
            Assert.IsNull(outJob);
        }

        //Test auto delete older than 24 hours and with Error or Completed status
        [TestMethod]
        public void DeleteOldJobsErrorAndCompletedTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now.AddHours(-48),
                Status = JobStatus.Error,
                Error = "Test delete old job with status: Error"
            };
            job = jobDAL.SetJob(job);
            var job2 = new Job();
            job2.AppID = AppID;
            job2.Created = DateTime.Now.AddHours(-48);
            job2.Status = JobStatus.Completed;
            job2.Error = "Test delete old job with status: Completed";
            job2 = jobDAL.SetJob(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var count = jobDAL.Delete(24, new List<JobStatus?> { JobStatus.Error, JobStatus.Completed });
            Assert.IsTrue(count > 0);

            var outJob = jobDAL.GetJob(job.JobID);
            Assert.IsNull(outJob);

            var outJob2 = jobDAL.GetJob(job2.JobID);
            Assert.IsNull(outJob2);
        }

        [TestMethod]
        public void SetCommandStopTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            jobDAL.SetCommandStop(new List<string> { jobID });
            var job = jobDAL.GetJob(jobID);
            jobDAL.Delete(new List<string> { jobID });

            Assert.AreEqual(JobCommand.Stop, job.Command);
        }

        [TestMethod]
        public void SetCommandRunNowTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            jobDAL.SetCommandRunNow(new List<string> { jobID });
            var job = jobDAL.GetJob(jobID);
            jobDAL.Delete(new List<string> { jobID });

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
            job = jobDAL.SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            jobDAL.Reset(new List<string> { job.JobID });
            var outJob = jobDAL.GetJob(job.JobID);
            jobDAL.Delete(new List<string> { job.JobID });

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
            job = jobDAL.SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = jobDAL.SetToStopped(new List<string> { job.JobID });
            var outJob = jobDAL.GetJob(job.JobID);
            jobDAL.Delete(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(count == 1);
            Assert.IsTrue(string.IsNullOrWhiteSpace(outJob.Command));
            Assert.AreEqual(JobStatus.Stopped, outJob.Status);
        }

        //Get Multiple jobs
        [TestMethod]
        public void GetJobsTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = jobDAL.GetJobs(new List<string> { jobID, jobID2 });
            jobDAL.Delete(new List<string> { jobID, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobs.Count == 2);
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobIDs.Contains(jobID2));
        }

        [TestMethod]
        public void GetNonRunningJobsByIDsTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = jobDAL.GetNonRunningJobsByIDs(new List<string> { jobID, jobID2 });
            jobDAL.Delete(new List<string> { jobID, jobID2 });

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
                Command = null
            };
            job = jobDAL.SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            jobDAL.SetCommandStop(new List<string> { job.JobID });

            var outJobIDs = jobDAL.GetJobIdsByProcessAndCommand(processID, JobCommand.Stop);
            jobDAL.Delete(new List<string> { job.JobID });

            Assert.IsTrue(outJobIDs.Contains(job.JobID));
        }

        [TestMethod]
        public void GetJobsByProcessAndStatusTest()
        {
            var job = new Job
            {
                AppID = AppID,
                ProcessID = processID,
                Created = DateTime.Now,
                Status = null
            };
            job = jobDAL.SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            jobDAL.SetToRunning(processID, job.JobID);

            var outJobs = jobDAL.GetJobsByProcessAndStatus(processID, JobStatus.Running);

            jobDAL.SetToStopped(new List<string> { job.JobID });
            jobDAL.Delete(new List<string> { job.JobID });

            var jobIDs = outJobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(job.JobID));
        }

        [TestMethod]
        public void GetJobViewsTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = jobDAL.GetJobViews(1, 10);
            jobDAL.Delete(new List<string> { jobID, jobID2 });

            Assert.IsTrue(jobs.Total >= 2);
            var jobIDs = jobs.Items.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobIDs.Contains(jobID2));
        }


        [TestMethod]
        public void GetJobViewsTest2()
        {
            var jobID1 = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs1 = jobDAL.GetJobViews(1, 1);
            var jobs2 = jobDAL.GetJobViews(2, 1);
            jobDAL.Delete(new List<string> { jobID1, jobID2 });

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
            job = jobDAL.SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = jobDAL.SetToRunning(job.ProcessID, job.JobID);
            var outJob = jobDAL.GetJob(job.JobID);

            //set to stop before delete
            jobDAL.SetToStopped(new List<string> { job.JobID });
            jobDAL.Delete(new List<string> { job.JobID });

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
            job = jobDAL.SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var error = "Test Error";
            var count = jobDAL.SetError(job.ProcessID, job.JobID, error);
            var outJob = jobDAL.GetJob(job.JobID);

            jobDAL.Delete(new List<string> { job.JobID });

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
            job = jobDAL.SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = jobDAL.SetCompleted(job.ProcessID, job.JobID);
            var outJob = jobDAL.GetJob(job.JobID);

            jobDAL.Delete(new List<string> { job.JobID });

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
                Status = null,
                ProcessID = processID
            };
            job1 = jobDAL.SetJob(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));
            jobDAL.SetToRunning(processID, job1.JobID);

            var job2 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID
            };
            job2 = jobDAL.SetJob(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));
            jobDAL.SetToRunning(processID, job2.JobID);

            var count = jobDAL.CountRunningJobs(processID);

            //set to stop before delete
            jobDAL.SetToStopped(new List<string> { job1.JobID, job2.JobID });
            jobDAL.Delete(new List<string> { job1.JobID, job2.JobID });

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
            job = jobDAL.SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var jobs = jobDAL.ClaimJobsToRun(processID, new List<Job> { job });
            var outJob = jobDAL.GetJob(job.JobID);

            jobDAL.Delete(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.AreEqual(processID, outJob.ProcessID);
            Assert.IsTrue(jobIDs.Contains(job.JobID));
        }

        //Don't claim running jobs
        [TestMethod]
        public void ClaimJobsToRunTest2()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID + "-someoneElseTest"
            };
            job = jobDAL.SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            jobDAL.SetToRunning(processID + "-someoneElseTest", job.JobID);

            var jobs = jobDAL.ClaimJobsToRun(processID, new List<Job> { job });

            jobDAL.SetToStopped(new List<string> {job.JobID});
            jobDAL.Delete(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(!jobIDs.Contains(job.JobID));
        }

        //Don't claim jobs already claimed by someone else
        [TestMethod]
        public void ClaimJobsToRunTest3()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID + "-someoneElseTest"
            };
            job = jobDAL.SetJob(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var jobs = jobDAL.ClaimJobsToRun(processID, new List<Job> { job });

            jobDAL.Delete(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(!jobIDs.Contains(job.JobID));
        }

        [TestMethod]
        public void GetJobsToRunTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            var jobs = jobDAL.GetJobsToRun(1);

            jobDAL.Delete(new List<string> { jobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobs.Count == 1);
        }

        //Get run-now job first
        [TestMethod]
        public void GetJobsToRunTest2()
        {
            var jobID1 = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID1));
            var jobID2 = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID2));

            jobDAL.SetCommandRunNow(new List<string> { jobID2 });
            var jobs = jobDAL.GetJobsToRun(1);

            jobDAL.Delete(new List<string> { jobID1, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(jobID2));
            Assert.IsTrue(jobs.Count == 1);
        }

        //Should return no jobs that was added
        [TestMethod]
        public void GetJobsToRunTest3()
        {
            //procesID != null
            var job1 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID
            };
            job1 = jobDAL.SetJob(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped,
                ProcessID = null
            };
            job2 = jobDAL.SetJob(job2);
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
            job3 = jobDAL.SetJob(job3);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job3.JobID));

            var jobs = jobDAL.GetJobsToRun(3);

            jobDAL.Delete(new List<string> { job1.JobID, job2.JobID, job3.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(!jobIDs.Contains(job1.JobID));
            Assert.IsTrue(!jobIDs.Contains(job2.JobID));
            Assert.IsTrue(!jobIDs.Contains(job3.JobID));
        }

        [TestMethod]
        public void SetProgressTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            var note = "progress note";
            var data = "progress data";
            var percent = 50;
            var count = jobDAL.SetProgress(jobID, percent, note, data);
            var job = jobDAL.GetJobView(jobID);

            jobDAL.Delete(new List<string> { jobID });

            Assert.IsTrue(count == 1);
            Assert.AreEqual(percent, job.Percent);
            Assert.AreEqual(note, job.Note);
            Assert.AreEqual(data, job.Data);
        }

        [TestMethod]
        public void GetProgressTest()
        {
            var jobID = jobDAL.Add(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            var note = "progress note";
            var data = "progress data";
            var percent = 50;
            jobDAL.SetProgress(jobID, percent, note, data);

            var progress = jobDAL.GetProgress(jobID);

            jobDAL.Delete(new List<string> { jobID });

            Assert.AreEqual(percent, progress.Percent);
            Assert.AreEqual(note, progress.Note);
            Assert.AreEqual(data, progress.Data);
        }

        [TestMethod]
        public void GetJobStatusCountTest()
        {
            var userID = "UserIDTest";
            var job1 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = null
            };
            job1 = jobDAL.SetJob(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = jobDAL.SetJob(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = jobDAL.GetJobStatusCount(null, null);

            jobDAL.Delete(new List<string> { job1.JobID, job2.JobID });

            var jobStatuses = statusCounts.Select(s => s.Status).ToList();
            Assert.IsTrue(jobStatuses.Contains(null));
            Assert.IsTrue(jobStatuses.Contains(JobStatus.Stopped));
            Assert.IsTrue(statusCounts.Count >= 2);
            foreach (var jobStatusCount in statusCounts)
            {
                if (jobStatusCount.Status == null)
                {
                    Assert.IsTrue(jobStatusCount.NullCount >= 1);
                }
                if (jobStatusCount.Status == JobStatus.Stopped)
                {
                    Assert.IsTrue(jobStatusCount.Count >= 1);
                }
            }
        }

        //Count by AppID and UserID
        [TestMethod]
        public void GetJobStatusCountTest2()
        {
            var userID = "UserIDTest";
            var job1 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = null
            };
            job1 = jobDAL.SetJob(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = jobDAL.SetJob(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = jobDAL.GetJobStatusCount(AppID, userID);

            jobDAL.Delete(new List<string> { job1.JobID, job2.JobID });

            var jobStatuses = statusCounts.Select(s => s.Status).ToList();
            Assert.IsTrue(jobStatuses.Contains(null));
            Assert.IsTrue(jobStatuses.Contains(JobStatus.Stopped));
            Assert.IsTrue(statusCounts.Count >= 2);
            foreach (var jobStatusCount in statusCounts)
            {
                if (jobStatusCount.Status == null)
                {
                    Assert.IsTrue(jobStatusCount.NullCount >= 1);
                }
                if (jobStatusCount.Status == JobStatus.Stopped)
                {
                    Assert.IsTrue(jobStatusCount.Count >= 1);
                }
            }
        }

        //Count by AppID 
        [TestMethod]
        public void GetJobStatusCountTest3()
        {
            var userID = "UserIDTest";
            var job1 = new Job
            {
                AppID = AppID + "-otherAppID",
                UserID = userID,
                Created = DateTime.Now,
                Status = null
            };
            job1 = jobDAL.SetJob(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = jobDAL.SetJob(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = jobDAL.GetJobStatusCount(AppID, null);

            jobDAL.Delete(new List<string> { job1.JobID, job2.JobID });

            var jobStatuses = statusCounts.Select(s => s.Status).ToList();
            Assert.IsTrue(jobStatuses.Contains(JobStatus.Stopped));
            Assert.IsTrue(statusCounts.Count >= 1);
            foreach (var jobStatusCount in statusCounts)
            {
                if (jobStatusCount.Status == JobStatus.Stopped)
                {
                    Assert.IsTrue(jobStatusCount.Count >= 1);
                }
            }
        }

        //Count by UserID 
        [TestMethod]
        public void GetJobStatusCountTest4()
        {
            var userID = "UserIDTest";
            var job1 = new Job
            {
                AppID = AppID,
                UserID = userID + "-otherUserID",
                Created = DateTime.Now,
                Status = null
            };
            job1 = jobDAL.SetJob(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = jobDAL.SetJob(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = jobDAL.GetJobStatusCount(null, userID);

            jobDAL.Delete(new List<string> { job1.JobID, job2.JobID });

            var jobStatuses = statusCounts.Select(s => s.Status).ToList();
            Assert.IsTrue(jobStatuses.Contains(JobStatus.Stopped));
            Assert.IsTrue(statusCounts.Count >= 1);
            foreach (var jobStatusCount in statusCounts)
            {
                if (jobStatusCount.Status == JobStatus.Stopped)
                {
                    Assert.IsTrue(jobStatusCount.Count >= 1);
                }
            }
        }
    }
}
