using System;
using Xunit;
using System.Threading;
using System.Collections.Generic;
using System.Configuration;
using Autofac;
using Autofac.Features.ResolveAnything;

using Shift.Entities;
using System.Threading.Tasks;

namespace Shift.UnitTest
{
     
    public class JobServerRedisAsyncTest
    {
        JobClient jobClient;
        JobServer jobServer;
        private const string AppID = "TestAppID";

        public JobServerRedisAsyncTest()
        {
            var appSettingsReader = new AppSettingsReader();

            //Configure storage connection
            var clientConfig = new ClientConfig();
            clientConfig.DBConnectionString = appSettingsReader.GetValue("RedisConnectionString", typeof(string)) as string;
            clientConfig.StorageMode = "redis";
            jobClient = new JobClient(clientConfig);

            var serverConfig = new ServerConfig();
            serverConfig.DBConnectionString = appSettingsReader.GetValue("RedisConnectionString", typeof(string)) as string;
            serverConfig.StorageMode = "redis";
            serverConfig.ProcessID = this.ToString();
            serverConfig.Workers = 1;
            serverConfig.MaxRunnableJobs = 1;

            serverConfig.ProgressDBInterval = new TimeSpan(0);
            serverConfig.AutoDeletePeriod = null;
            serverConfig.ForceStopServer = true;
            serverConfig.StopServerDelay = 3000;
            jobServer = new JobServer(serverConfig);
        }

        [Fact]
        public async Task RunJobsSelectedAsyncTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);

            Assert.NotNull(job);
            Assert.Equal(jobID, job.JobID);

            //run job
            await jobServer.RunJobsAsync(new List<string> { jobID });
            Thread.Sleep(5000);

            job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            Assert.Equal(JobStatus.Completed, job.Status);
        }


        [Fact]
        public async Task StopJobsNonRunningAsyncTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            var job = await jobClient.GetJobAsync(jobID);

            Assert.NotNull(job);
            Assert.Equal(JobCommand.Stop, job.Command);

            await jobServer.StopJobsAsync(); //stop non-running job
            Thread.Sleep(5000);

            job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            Assert.Equal(JobStatus.Stopped, job.Status);
        }

        [Fact]
        public async Task StopJobsRunningAsyncTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var cancelToken = (new CancellationTokenSource()).Token;
            var pauseToken = (new PauseTokenSource()).Token;
            var jobID = await jobClient.AddAsync(AppID, () => jobTest.Start("Hello World", progress, cancelToken, pauseToken));

            //run job
            await jobServer.RunJobsAsync(new List<string> { jobID });
            Thread.Sleep(1000);

            var job = await jobClient.GetJobAsync(jobID);
            Assert.NotNull(job);
            Assert.Equal(JobStatus.Running, job.Status);

            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            await jobServer.StopJobsAsync(); //stop running job
            Thread.Sleep(3000);

            job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            Assert.Equal(JobStatus.Stopped, job.Status);
        }

        [Fact]
        public async Task CleanUpAsyncTest()
        {
            //Test StopJobs with CleanUp() calls

            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var cancelToken = (new CancellationTokenSource()).Token;
            var pauseToken = (new PauseTokenSource()).Token;
            var jobID = await jobClient.AddAsync(AppID, () => jobTest.Start("Hello World", progress, cancelToken, pauseToken));

            //run job
            await jobServer.RunJobsAsync(new List<string> { jobID });
            Thread.Sleep(1000);

            var job = await jobClient.GetJobAsync(jobID);
            Assert.NotNull(job);
            Assert.Equal(JobStatus.Running, job.Status);

            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            await jobServer.CleanUpAsync(); 
            Thread.Sleep(3000);

            job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            Assert.Equal(JobStatus.Stopped, job.Status);
        }

        [Fact]
        public async Task PauseJobsRunningAsyncTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var cancelToken = (new CancellationTokenSource()).Token;
            var pauseToken = (new PauseTokenSource()).Token;
            var jobID = await jobClient.AddAsync(AppID, () => jobTest.Start("Hello World", progress, cancelToken, pauseToken));

            //run job
            await jobServer.RunJobsAsync(new List<string> { jobID });
            Thread.Sleep(1000);

            var job = await jobClient.GetJobAsync(jobID);
            Assert.NotNull(job);
            Assert.Equal(JobStatus.Running, job.Status);

            await jobClient.SetCommandPauseAsync(new List<string> { jobID });
            await jobServer.PauseJobsAsync(); //pause running job
            Thread.Sleep(3000);

            job = await jobClient.GetJobAsync(jobID);
            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            await jobServer.StopJobsAsync();
            Thread.Sleep(3000);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.Equal(JobStatus.Paused, job.Status);
        }

        [Fact]
        public async Task ContinueJobsPausedAsyncTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var cancelToken = (new CancellationTokenSource()).Token;
            var pauseToken = (new PauseTokenSource()).Token;
            var jobID = await jobClient.AddAsync(AppID, () => jobTest.Start("Hello World", progress, cancelToken, pauseToken));

            //run job
            await jobServer.RunJobsAsync(new List<string> { jobID });
            Thread.Sleep(1000);

            await jobClient.SetCommandPauseAsync(new List<string> { jobID });
            await jobServer.PauseJobsAsync(); //pause running job
            Thread.Sleep(3000);

            var job = await jobClient.GetJobAsync(jobID);
            Assert.NotNull(job);
            Assert.Equal(JobStatus.Paused, job.Status);

            await jobClient.SetCommandContinueAsync(new List<string> { jobID });
            await jobServer.ContinueJobsAsync(); //continue paused job
            Thread.Sleep(3000);

            job = await jobClient.GetJobAsync(jobID);
            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            await jobServer.StopJobsAsync();
            Thread.Sleep(3000);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });

            Assert.Equal(JobStatus.Running, job.Status);
        }
    }
}
