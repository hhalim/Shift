using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shift.Entities;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using Shift.DataLayer;
using System.Linq;

namespace Shift.UnitTest.DataLayer
{
    [TestClass]
    public class JobDALSqlAsyncTest
    {
        private static AppSettingsReader appSettingsReader = new AppSettingsReader();
        private const string AppID = "TestAppID";
        private readonly string processID;
        private static string connectionString = appSettingsReader.GetValue("MSSqlConnectionString", typeof(string)) as string;
        private const string encryptionKey = "";
        JobDALSql jobDAL;

        public JobDALSqlAsyncTest()
        {
            processID = this.ToString();
            this.jobDAL = new JobDALSql(connectionString, encryptionKey);
        }

        [TestMethod]
        public async Task DeleteAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            await jobDAL.DeleteAsync(new List<string> { jobID });
            var job = await jobDAL.GetJobAsync(jobID);

            Assert.IsNull(job);
        }

        [TestMethod]
        public async Task GetJobAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = await jobDAL.GetJobAsync(jobID);
            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.IsInstanceOfType(job, typeof(Job));
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public async Task GetJobViewAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = await jobDAL.GetJobViewAsync(jobID);
            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.IsInstanceOfType(job, typeof(JobView));
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public async Task AddAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            await jobDAL.DeleteAsync(new List<string> { jobID });
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));
        }

        [TestMethod]
        public async Task UpdateAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var count = await jobDAL.UpdateAsync(jobID, AppID, "", "", "JobNameUpdated", () => Console.WriteLine("Hello World Test!"));

            var job = await jobDAL.GetJobAsync(jobID);
            await jobDAL.DeleteAsync(new List<string> { jobID });
            Assert.IsTrue(count > 0);
            Assert.AreEqual("JobNameUpdated", job.JobName);
        }

        //Test auto delete older than 24 hours and Null(not started) status
        [TestMethod]
        public async Task DeleteAsyncOldJobsNotStarted()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now.AddHours(-48)
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = await jobDAL.DeleteAsync(24, new List<JobStatus?> { null });
            var outJob = await jobDAL.GetJobAsync(job.JobID);

            Assert.IsTrue(count > 0);
            Assert.IsNull(outJob);
        }

        //Test auto delete older than 24 hours and with Error or Completed status
        [TestMethod]
        public async Task DeleteAsyncOldJobsErrorAndCompletedTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now.AddHours(-48),
                Status = JobStatus.Error,
                Error = "Test delete old job with status: Error"
            };
            job = await jobDAL.SetJobAsync(job);
            var job2 = new Job();
            job2.AppID = AppID;
            job2.Created = DateTime.Now.AddHours(-48);
            job2.Status = JobStatus.Completed;
            job2.Error = "Test delete old job with status: Completed";
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var count = await jobDAL.DeleteAsync(24, new List<JobStatus?> { JobStatus.Error, JobStatus.Completed });
            Assert.IsTrue(count > 0);

            var outJob = await jobDAL.GetJobAsync(job.JobID);
            Assert.IsNull(outJob);

            var outJob2 = await jobDAL.GetJobAsync(job2.JobID);
            Assert.IsNull(outJob2);
        }

        [TestMethod]
        public async Task SetCommandStopAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            await jobDAL.SetCommandStopAsync(new List<string> { jobID });
            var job = await jobDAL.GetJobAsync(jobID);
            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.AreEqual(JobCommand.Stop, job.Command);
        }

        [TestMethod]
        public async Task SetCommandRunNowAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            await jobDAL.SetCommandRunNowAsync(new List<string> { jobID });
            var job = await jobDAL.GetJobAsync(jobID);
            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.AreEqual(JobCommand.RunNow, job.Command);
        }

        [TestMethod]
        public async Task ResetAsyncTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Command = JobCommand.Stop
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            await jobDAL.ResetAsync(new List<string> { job.JobID });
            var outJob = await jobDAL.GetJobAsync(job.JobID);
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(string.IsNullOrWhiteSpace(outJob.Command));
        }

        [TestMethod]
        public async Task SetToStoppedAsyncTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Command = JobCommand.Stop
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = await jobDAL.SetToStoppedAsync(new List<string> { job.JobID });
            var outJob = await jobDAL.GetJobAsync(job.JobID);
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(count == 1);
            Assert.IsTrue(string.IsNullOrWhiteSpace(outJob.Command));
            Assert.AreEqual(JobStatus.Stopped, outJob.Status);
        }

        //Get Multiple jobs
        [TestMethod]
        public async Task GetJobsAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = await jobDAL.GetJobsAsync(new List<string> { jobID, jobID2 });
            await jobDAL.DeleteAsync(new List<string> { jobID, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobs.Count == 2);
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobIDs.Contains(jobID2));
        }

        [TestMethod]
        public async Task GetNonRunningJobsByIDsAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = await jobDAL.GetNonRunningJobsByIDsAsync(new List<string> { jobID, jobID2 });
            await jobDAL.DeleteAsync(new List<string> { jobID, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobs.Count == 2);
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobIDs.Contains(jobID2));
        }

        [TestMethod]
        public async Task GetJobIdsByProcessAndCommandAsyncTest()
        {
            var job = new Job
            {
                AppID = AppID,
                ProcessID = processID,
                Created = DateTime.Now,
                Command = null
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            await jobDAL.SetCommandStopAsync(new List<string> {job.JobID});

            var outJobIDs = await jobDAL.GetJobIdsByProcessAndCommandAsync(processID, JobCommand.Stop);
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.IsTrue(outJobIDs.Contains(job.JobID));
        }

        [TestMethod]
        public async Task GetJobsByProcessAndStatusAsyncTest()
        {
            var job = new Job
            {
                AppID = AppID,
                ProcessID = processID,
                Created = DateTime.Now,
                Status = null
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            await jobDAL.SetToRunningAsync(processID, job.JobID);

            var outJobs = await jobDAL.GetJobsByProcessAndStatusAsync(processID, JobStatus.Running);

            await jobDAL.SetToStoppedAsync(new List<string> {job.JobID});
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            var jobIDs = outJobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(job.JobID));
        }

        [TestMethod]
        public async Task GetJobViewsAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = await jobDAL.GetJobViewsAsync(1, 10);
            await jobDAL.DeleteAsync(new List<string> { jobID, jobID2 });

            Assert.IsTrue(jobs.Total >= 2);
            var jobIDs = jobs.Items.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobIDs.Contains(jobID2));
        }


        [TestMethod]
        public async Task GetJobViewsAsyncTest2()
        {
            var jobID1 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs1 = await jobDAL.GetJobViewsAsync(1, 1);
            var jobs2 = await jobDAL.GetJobViewsAsync(2, 1);
            await jobDAL.DeleteAsync(new List<string> { jobID1, jobID2 });

            Assert.IsTrue(jobs1.Total >= 2);
            var jobIDs1 = jobs1.Items.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs1.Contains(jobID1));

            Assert.IsTrue(jobs2.Total >= 2);
            var jobIDs2 = jobs2.Items.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs2.Contains(jobID2));
        }

        [TestMethod]
        public async Task SetToRunningAsyncTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = await jobDAL.SetToRunningAsync(job.ProcessID, job.JobID);
            var outJob = await jobDAL.GetJobAsync(job.JobID);

            //set to stop before delete
            await jobDAL.SetToStoppedAsync(new List<string> { job.JobID });
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(count == 1);
            Assert.AreEqual(JobStatus.Running, outJob.Status);
        }

        [TestMethod]
        public async Task SetErrorAsyncTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var error = "Test Error";
            var count = await jobDAL.SetErrorAsync(job.ProcessID, job.JobID, error);
            var outJob = await jobDAL.GetJobAsync(job.JobID);

            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(count == 1);
            Assert.AreEqual(JobStatus.Error, outJob.Status);
            Assert.AreEqual(error, outJob.Error);
        }

        [TestMethod]
        public async Task SetCompletedAsyncTest()
        {
            var job = new Job();
            job.AppID = AppID;
            job.Created = DateTime.Now;
            job.Status = null;
            job.ProcessID = processID;
            job = await jobDAL.SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var count = await jobDAL.SetCompletedAsync(job.ProcessID, job.JobID);
            var outJob = await jobDAL.GetJobAsync(job.JobID);

            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.IsNotNull(outJob);
            Assert.IsTrue(count == 1);
            Assert.AreEqual(JobStatus.Completed, outJob.Status);
        }

        [TestMethod]
        public async Task CountRunningJobsAsyncTest()
        {
            var job1 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID
            };
            job1 = await jobDAL.SetJobAsync(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));
            await jobDAL.SetToRunningAsync(processID, job1.JobID);

            var job2 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID
            };
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));
            await jobDAL.SetToRunningAsync(processID, job2.JobID);

            var count = await jobDAL.CountRunningJobsAsync(processID);

            //set to stop before delete
            await jobDAL.SetToStoppedAsync(new List<string> {job1.JobID, job2.JobID});
            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID });

            Assert.IsTrue(count >= 2);
        }

        [TestMethod]
        public async Task ClaimJobsToRunAsyncTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = null
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var jobs = await jobDAL.ClaimJobsToRunAsync(processID, new List<Job> { job });
            var outJob = await jobDAL.GetJobAsync(job.JobID);

            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.AreEqual(processID, outJob.ProcessID);
            Assert.IsTrue(jobIDs.Contains(job.JobID));
        }

        //Don't claim running jobs
        [TestMethod]
        public async Task ClaimJobsToRunAsyncTest2()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID + "-someoneElseTest"
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            await jobDAL.SetToRunningAsync(processID + "-someoneElseTest", job.JobID);

            var jobs = await jobDAL.ClaimJobsToRunAsync(processID, new List<Job> { job });

            await jobDAL.SetToStoppedAsync(new List<string> { job.JobID });
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(!jobIDs.Contains(job.JobID));
        }

        //Don't claim jobs already claimed by someone else
        [TestMethod]
        public async Task ClaimJobsToRunAsyncTest3()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID + "-someoneElseTest"
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job.JobID));

            var jobs = await jobDAL.ClaimJobsToRunAsync(processID, new List<Job> { job });

            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(!jobIDs.Contains(job.JobID));
        }

        [TestMethod]
        public async Task GetJobsToRunAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            var jobs = await jobDAL.GetJobsToRunAsync(1);

            await jobDAL.DeleteAsync(new List<string> { jobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(jobID));
            Assert.IsTrue(jobs.Count == 1);
        }

        //Get run-now job first
        [TestMethod]
        public async Task GetJobsToRunAsyncTest2()
        {
            var jobID1 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID1));
            var jobID2 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID2));

            await jobDAL.SetCommandRunNowAsync(new List<string> { jobID2 });
            var jobs = await jobDAL.GetJobsToRunAsync(1);

            await jobDAL.DeleteAsync(new List<string> { jobID1, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(jobIDs.Contains(jobID2));
            Assert.IsTrue(jobs.Count == 1);
        }

        //Should return no jobs that was added
        [TestMethod]
        public async Task GetJobsToRunAsyncTest3()
        {
            //procesID != null
            var job1 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID
            };
            job1 = await jobDAL.SetJobAsync(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped,
                ProcessID = null
            };
            job2 = await jobDAL.SetJobAsync(job2);
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
            job3 = await jobDAL.SetJobAsync(job3);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job3.JobID));

            var jobs = await jobDAL.GetJobsToRunAsync(3);

            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID, job3.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.IsTrue(!jobIDs.Contains(job1.JobID));
            Assert.IsTrue(!jobIDs.Contains(job2.JobID));
            Assert.IsTrue(!jobIDs.Contains(job3.JobID));
        }

        [TestMethod]
        public async Task SetProgressAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            var note = "progress note";
            var data = "progress data";
            var percent = 50;
            var count = await jobDAL.SetProgressAsync(jobID, percent, note, data);
            var job = await jobDAL.GetJobViewAsync(jobID);

            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.IsTrue(count == 1);
            Assert.AreEqual(percent, job.Percent);
            Assert.AreEqual(note, job.Note);
            Assert.AreEqual(data, job.Data);
        }

        [TestMethod]
        public async Task UpdateProgressAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            await jobDAL.SetProgressAsync(jobID, null, null, null);

            var note = "progress note";
            var data = "progress data";
            var percent = 50;
            var count = await jobDAL.UpdateProgressAsync(jobID, percent, note, data);
            var job = await jobDAL.GetJobViewAsync(jobID);

            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.IsTrue(count == 1);
            Assert.AreEqual(percent, job.Percent);
            Assert.AreEqual(note, job.Note);
            Assert.AreEqual(data, job.Data);
        }

        [TestMethod]
        public async Task GetProgressAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            var note = "progress note";
            var data = "progress data";
            var percent = 50;
            await jobDAL.SetProgressAsync(jobID, percent, note, data);

            var progress = await jobDAL.GetProgressAsync(jobID);

            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.AreEqual(percent, progress.Percent);
            Assert.AreEqual(note, progress.Note);
            Assert.AreEqual(data, progress.Data);
        }

        [TestMethod]
        public async Task GetJobStatusCountAsyncTest()
        {
            var userID = "UserIDTest";
            var job1 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = null
            };
            job1 = await jobDAL.SetJobAsync(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = await jobDAL.GetJobStatusCountAsync(null, null);

            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID });

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
        public async Task GetJobStatusCountAsyncTest2()
        {
            var userID = "UserIDTest";
            var job1 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = null
            };
            job1 = await jobDAL.SetJobAsync(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = await jobDAL.GetJobStatusCountAsync(AppID, userID);

            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID });

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
        public async Task GetJobStatusCountAsyncTest3()
        {
            var userID = "UserIDTest";
            var job1 = new Job
            {
                AppID = AppID + "-otherAppID",
                UserID = userID,
                Created = DateTime.Now,
                Status = null
            };
            job1 = await jobDAL.SetJobAsync(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = await jobDAL.GetJobStatusCountAsync(AppID, null);

            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID });

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
        public async Task GetJobStatusCountAsyncTest4()
        {
            var userID = "UserIDTest";
            var job1 = new Job
            {
                AppID = AppID,
                UserID = userID + "-otherUserID",
                Created = DateTime.Now,
                Status = null
            };
            job1 = await jobDAL.SetJobAsync(job1);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = await jobDAL.GetJobStatusCountAsync(null, userID);

            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID });

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
