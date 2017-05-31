using System;
using Xunit;
using Shift.Entities;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using Shift.DataLayer;
using System.Linq;

namespace Shift.UnitTest.DataLayer
{
     
    public class JobDALDocumentDBAsyncTest 
    {
        private static AppSettingsReader appSettingsReader = new AppSettingsReader();
        private const string AppID = "TestAppID";
        private readonly string processID;
        private static string connectionString = appSettingsReader.GetValue("DocumentDBUrl", typeof(string)) as string;
        private static string authKey = appSettingsReader.GetValue("DocumentDBAuthKey", typeof(string)) as string;
        private const string encryptionKey = "";
        JobDALDocumentDB jobDAL;

        public JobDALDocumentDBAsyncTest()
        {
            processID = this.ToString();
            jobDAL = new JobDALDocumentDB(connectionString, encryptionKey, authKey);
        }

        [Fact]
        public async Task DeleteAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            Assert.True(!string.IsNullOrWhiteSpace(jobID));

            await jobDAL.DeleteAsync(new List<string> { jobID });
            var job = await jobDAL.GetJobAsync(jobID);

            Assert.Null(job);
        }

        [Fact]
        public async Task GetJobAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = await jobDAL.GetJobAsync(jobID);
            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.IsType<Job>(job);
            Assert.Equal(jobID, job.JobID);
        }

        [Fact]
        public async Task GetJobViewAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = await jobDAL.GetJobViewAsync(jobID);
            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.IsType<JobView>(job);
            Assert.Equal(jobID, job.JobID);
        }

        [Fact]
        public async Task AddAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            await jobDAL.DeleteAsync(new List<string> { jobID });
            Assert.True(!string.IsNullOrWhiteSpace(jobID));
        }

        [Fact]
        public async Task UpdateAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var count = await jobDAL.UpdateAsync(jobID, AppID, "", "", "JobNameUpdated", () => Console.WriteLine("Hello World Test!"));

            var job = await jobDAL.GetJobAsync(jobID);
            await jobDAL.DeleteAsync(new List<string> { jobID });
            Assert.True(count > 0);
            Assert.Equal("JobNameUpdated", job.JobName);
        }

        //Test auto delete older than 24 hours and Null(not started) status
        [Fact]
        public async Task DeleteAsyncOldJobsNotStarted()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now.AddHours(-48)
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));

            var count = await jobDAL.DeleteAsync(24, new List<JobStatus?> { null });
            var outJob = await jobDAL.GetJobAsync(job.JobID);

            Assert.True(count > 0);
            Assert.Null(outJob);
        }

        //Test auto delete older than 24 hours and with Error or Completed status
        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));
            Assert.True(!string.IsNullOrWhiteSpace(job2.JobID));

            var count = await jobDAL.DeleteAsync(24, new List<JobStatus?> { JobStatus.Error, JobStatus.Completed });
            Assert.True(count > 0);

            var outJob = await jobDAL.GetJobAsync(job.JobID);
            Assert.Null(outJob);

            var outJob2 = await jobDAL.GetJobAsync(job2.JobID);
            Assert.Null(outJob2);
        }

        [Fact]
        public async Task SetCommandStopAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            await jobDAL.SetCommandStopAsync(new List<string> { jobID });
            var job = await jobDAL.GetJobAsync(jobID);
            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.Equal(JobCommand.Stop, job.Command);
        }

        [Fact]
        public async Task SetCommandRunNowAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            await jobDAL.SetCommandRunNowAsync(new List<string> { jobID });
            var job = await jobDAL.GetJobAsync(jobID);
            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.Equal(JobCommand.RunNow, job.Command);
        }

        [Fact]
        public async Task ResetAsyncTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Command = JobCommand.Stop
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));

            await jobDAL.ResetAsync(new List<string> { job.JobID });
            var outJob = await jobDAL.GetJobAsync(job.JobID);
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.NotNull(outJob);
            Assert.True(string.IsNullOrWhiteSpace(outJob.Command));
        }

        [Fact]
        public async Task SetToStoppedAsyncTest()
        {
            var job = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Command = JobCommand.Stop
            };
            job = await jobDAL.SetJobAsync(job);
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));

            var count = await jobDAL.SetToStoppedAsync(new List<string> { job.JobID });
            var outJob = await jobDAL.GetJobAsync(job.JobID);
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.NotNull(outJob);
            Assert.True(count == 1);
            Assert.True(string.IsNullOrWhiteSpace(outJob.Command));
            Assert.Equal(JobStatus.Stopped, outJob.Status);
        }

        //Get Multiple jobs
        [Fact]
        public async Task GetJobsAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = await jobDAL.GetJobsAsync(new List<string> { jobID, jobID2 });
            await jobDAL.DeleteAsync(new List<string> { jobID, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.True(jobs.Count == 2);
            Assert.True(jobIDs.Contains(jobID));
            Assert.True(jobIDs.Contains(jobID2));
        }

        [Fact]
        public async Task GetNonRunningJobsByIDsAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = await jobDAL.GetNonRunningJobsByIDsAsync(new List<string> { jobID, jobID2 });
            await jobDAL.DeleteAsync(new List<string> { jobID, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.True(jobs.Count == 2);
            Assert.True(jobIDs.Contains(jobID));
            Assert.True(jobIDs.Contains(jobID2));
        }

        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));

            await jobDAL.SetCommandStopAsync(new List<string> {job.JobID});

            var outJobIDs = await jobDAL.GetJobIdsByProcessAndCommandAsync(processID, JobCommand.Stop);
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.True(outJobIDs.Contains(job.JobID));
        }

        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));

            await jobDAL.SetToRunningAsync(processID, job.JobID);

            var outJobs = await jobDAL.GetJobsByProcessAndStatusAsync(processID, JobStatus.Running);

            await jobDAL.SetToStoppedAsync(new List<string> {job.JobID});
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            var jobIDs = outJobs.Select(j => j.JobID).ToList();
            Assert.True(jobIDs.Contains(job.JobID));
        }

        [Fact]
        public async Task GetJobViewsAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs = await jobDAL.GetJobViewsAsync(1, 10);
            await jobDAL.DeleteAsync(new List<string> { jobID, jobID2 });

            Assert.True(jobs.Total >= 2);
            var jobIDs = jobs.Items.Select(j => j.JobID).ToList();
            Assert.True(jobIDs.Contains(jobID));
            Assert.True(jobIDs.Contains(jobID2));
        }


        [Fact]
        public async Task GetJobViewsAsyncTest2()
        {
            var jobID1 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            var jobID2 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));

            var jobs1 = await jobDAL.GetJobViewsAsync(1, 1);
            var jobs2 = await jobDAL.GetJobViewsAsync(2, 1);
            await jobDAL.DeleteAsync(new List<string> { jobID1, jobID2 });

            Assert.True(jobs1.Total >= 2);
            var jobIDs1 = jobs1.Items.Select(j => j.JobID).ToList();
            Assert.True(jobIDs1.Contains(jobID1));

            Assert.True(jobs2.Total >= 2);
            var jobIDs2 = jobs2.Items.Select(j => j.JobID).ToList();
            Assert.True(jobIDs2.Contains(jobID2));
        }

        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));

            var count = await jobDAL.SetToRunningAsync(job.ProcessID, job.JobID);
            var outJob = await jobDAL.GetJobAsync(job.JobID);

            //set to stop before delete
            await jobDAL.SetToStoppedAsync(new List<string> { job.JobID });
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.NotNull(outJob);
            Assert.True(count == 1);
            Assert.Equal(JobStatus.Running, outJob.Status);
        }

        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));

            var error = "Test Error";
            var count = await jobDAL.SetErrorAsync(job.ProcessID, job.JobID, error);
            var outJob = await jobDAL.GetJobAsync(job.JobID);

            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.NotNull(outJob);
            Assert.True(count == 1);
            Assert.Equal(JobStatus.Error, outJob.Status);
            Assert.Equal(error, outJob.Error);
        }

        [Fact]
        public async Task SetCompletedAsyncTest()
        {
            var job = new Job();
            job.AppID = AppID;
            job.Created = DateTime.Now;
            job.Status = null;
            job.ProcessID = processID;
            job = await jobDAL.SetJobAsync(job);
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));

            var count = await jobDAL.SetCompletedAsync(job.ProcessID, job.JobID);
            var outJob = await jobDAL.GetJobAsync(job.JobID);

            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            Assert.NotNull(outJob);
            Assert.True(count == 1);
            Assert.Equal(JobStatus.Completed, outJob.Status);
        }

        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job1.JobID));
            await jobDAL.SetToRunningAsync(processID, job1.JobID);

            var job2 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = null,
                ProcessID = processID
            };
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.True(!string.IsNullOrWhiteSpace(job2.JobID));
            await jobDAL.SetToRunningAsync(processID, job2.JobID);

            var count = await jobDAL.CountRunningJobsAsync(processID);

            //set to stop before delete
            await jobDAL.SetToStoppedAsync(new List<string> {job1.JobID, job2.JobID});
            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID });

            Assert.True(count >= 2);
        }

        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));

            var jobs = await jobDAL.ClaimJobsToRunAsync(processID, new List<Job> { job });
            var outJob = await jobDAL.GetJobAsync(job.JobID);

            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.Equal(processID, outJob.ProcessID);
            Assert.True(jobIDs.Contains(job.JobID));
        }

        //Don't claim running jobs
        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));

            await jobDAL.SetToRunningAsync(processID + "-someoneElseTest", job.JobID);

            var jobs = await jobDAL.ClaimJobsToRunAsync(processID, new List<Job> { job });

            await jobDAL.SetToStoppedAsync(new List<string> { job.JobID });
            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.True(!jobIDs.Contains(job.JobID));
        }

        //Don't claim jobs already claimed by someone else
        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job.JobID));

            var jobs = await jobDAL.ClaimJobsToRunAsync(processID, new List<Job> { job });

            await jobDAL.DeleteAsync(new List<string> { job.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.True(!jobIDs.Contains(job.JobID));
        }

        [Fact]
        public async Task GetJobsToRunAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.True(!string.IsNullOrWhiteSpace(jobID));

            var jobs = await jobDAL.GetJobsToRunAsync(1);

            await jobDAL.DeleteAsync(new List<string> { jobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.True(jobIDs.Contains(jobID));
            Assert.True(jobs.Count == 1);
        }

        //Get run-now job first
        [Fact]
        public async Task GetJobsToRunAsyncTest2()
        {
            var jobID1 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.True(!string.IsNullOrWhiteSpace(jobID1));
            var jobID2 = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test2!"));
            Assert.True(!string.IsNullOrWhiteSpace(jobID2));

            await jobDAL.SetCommandRunNowAsync(new List<string> { jobID2 });
            var jobs = await jobDAL.GetJobsToRunAsync(1);

            await jobDAL.DeleteAsync(new List<string> { jobID1, jobID2 });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.True(jobIDs.Contains(jobID2));
            Assert.True(jobs.Count == 1);
        }

        //Should return no jobs that was added
        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped,
                ProcessID = null
            };
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.True(!string.IsNullOrWhiteSpace(job2.JobID));

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
            Assert.True(!string.IsNullOrWhiteSpace(job3.JobID));

            var jobs = await jobDAL.GetJobsToRunAsync(3);

            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID, job3.JobID });

            var jobIDs = jobs.Select(j => j.JobID).ToList();
            Assert.True(!jobIDs.Contains(job1.JobID));
            Assert.True(!jobIDs.Contains(job2.JobID));
            Assert.True(!jobIDs.Contains(job3.JobID));
        }

        [Fact]
        public async Task SetProgressAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.True(!string.IsNullOrWhiteSpace(jobID));

            var note = "progress note";
            var data = "progress data";
            var percent = 50;
            var count = await jobDAL.SetProgressAsync(jobID, percent, note, data);
            var job = await jobDAL.GetJobViewAsync(jobID);

            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.True(count == 1);
            Assert.Equal(percent, job.Percent);
            Assert.Equal(note, job.Note);
            Assert.Equal(data, job.Data);
        }

        [Fact]
        public async Task UpdateProgressAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.True(!string.IsNullOrWhiteSpace(jobID));

            await jobDAL.SetProgressAsync(jobID, null, null, null);

            var note = "progress note";
            var data = "progress data";
            var percent = 50;
            var count = await jobDAL.UpdateProgressAsync(jobID, percent, note, data);
            var job = await jobDAL.GetJobViewAsync(jobID);

            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.True(count == 1);
            Assert.Equal(percent, job.Percent);
            Assert.Equal(note, job.Note);
            Assert.Equal(data, job.Data);
        }

        [Fact]
        public async Task GetProgressAsyncTest()
        {
            var jobID = await jobDAL.AddAsync(AppID, "", "", "", () => Console.WriteLine("Hello World Test1!"));
            Assert.True(!string.IsNullOrWhiteSpace(jobID));

            var note = "progress note";
            var data = "progress data";
            var percent = 50;
            await jobDAL.SetProgressAsync(jobID, percent, note, data);

            var progress = await jobDAL.GetProgressAsync(jobID);

            await jobDAL.DeleteAsync(new List<string> { jobID });

            Assert.Equal(percent, progress.Percent);
            Assert.Equal(note, progress.Note);
            Assert.Equal(data, progress.Data);
        }

        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.True(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = await jobDAL.GetJobStatusCountAsync(null, null);

            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID });

            var jobStatuses = statusCounts.Select(s => s.Status).ToList();
            Assert.True(jobStatuses.Contains(null));
            Assert.True(jobStatuses.Contains(JobStatus.Stopped));
            Assert.True(statusCounts.Count >= 2);
            foreach (var jobStatusCount in statusCounts)
            {
                if (jobStatusCount.Status == null)
                {
                    Assert.True(jobStatusCount.NullCount >= 1);
                }
                if (jobStatusCount.Status == JobStatus.Stopped)
                {
                    Assert.True(jobStatusCount.Count >= 1);
                }
            }
        }

        //Count by AppID and UserID
        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.True(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = await jobDAL.GetJobStatusCountAsync(AppID, userID);

            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID });

            var jobStatuses = statusCounts.Select(s => s.Status).ToList();
            Assert.True(jobStatuses.Contains(null));
            Assert.True(jobStatuses.Contains(JobStatus.Stopped));
            Assert.True(statusCounts.Count >= 2);
            foreach (var jobStatusCount in statusCounts)
            {
                if (jobStatusCount.Status == null)
                {
                    Assert.True(jobStatusCount.NullCount >= 1);
                }
                if (jobStatusCount.Status == JobStatus.Stopped)
                {
                    Assert.True(jobStatusCount.Count >= 1);
                }
            }
        }

        //Count by AppID 
        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.True(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = await jobDAL.GetJobStatusCountAsync(AppID, null);

            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID });

            var jobStatuses = statusCounts.Select(s => s.Status).ToList();
            Assert.True(jobStatuses.Contains(JobStatus.Stopped));
            Assert.True(statusCounts.Count >= 1);
            foreach (var jobStatusCount in statusCounts)
            {
                if (jobStatusCount.Status == JobStatus.Stopped)
                {
                    Assert.True(jobStatusCount.Count >= 1);
                }
            }
        }

        //Count by UserID 
        [Fact]
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
            Assert.True(!string.IsNullOrWhiteSpace(job1.JobID));

            //status != null
            var job2 = new Job
            {
                AppID = AppID,
                UserID = userID,
                Created = DateTime.Now,
                Status = JobStatus.Stopped
            };
            job2 = await jobDAL.SetJobAsync(job2);
            Assert.True(!string.IsNullOrWhiteSpace(job2.JobID));

            var statusCounts = await jobDAL.GetJobStatusCountAsync(null, userID);

            await jobDAL.DeleteAsync(new List<string> { job1.JobID, job2.JobID });

            var jobStatuses = statusCounts.Select(s => s.Status).ToList();
            Assert.True(jobStatuses.Contains(JobStatus.Stopped));
            Assert.True(statusCounts.Count >= 1);
            foreach (var jobStatusCount in statusCounts)
            {
                if (jobStatusCount.Status == JobStatus.Stopped)
                {
                    Assert.True(jobStatusCount.Count >= 1);
                }
            }
        }
    }
}
