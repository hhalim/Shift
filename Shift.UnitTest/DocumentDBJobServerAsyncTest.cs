﻿using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading;
using System.Collections.Generic;
using System.Configuration;

using Shift.Entities;
using System.Threading.Tasks;

namespace Shift.UnitTest
{
    [TestClass]
    public class DocumentDBJobServerAsyncTest
    {
        JobClient jobClient;
        JobServer jobServer;
        private const string AppID = "TestAppID";

        public DocumentDBJobServerAsyncTest()
        {
            var appSettingsReader = new AppSettingsReader();

            //Configure storage connection
            var clientConfig = new ClientConfig();
            clientConfig.DBConnectionString = appSettingsReader.GetValue("DocumentDBUrl", typeof(string)) as string;
            clientConfig.DBAuthKey = appSettingsReader.GetValue("DocumentDBAuthKey", typeof(string)) as string;
            clientConfig.StorageMode = "documentdb";
            jobClient = new JobClient(clientConfig);

            var serverConfig = new ServerConfig();
            serverConfig.DBConnectionString = appSettingsReader.GetValue("DocumentDBUrl", typeof(string)) as string;
            serverConfig.DBAuthKey = appSettingsReader.GetValue("DocumentDBAuthKey", typeof(string)) as string;
            serverConfig.StorageMode = "documentdb";
            serverConfig.ProcessID = this.ToString();
            serverConfig.Workers = 1;
            serverConfig.MaxRunnableJobs = 1;

            serverConfig.ProgressDBInterval = new TimeSpan(0);
            serverConfig.AutoDeletePeriod = null;
            serverConfig.ForceStopServer = true;
            serverConfig.StopServerDelay = 3000;
            jobServer = new JobServer(serverConfig);
        }

        [TestMethod]
        public async Task RunJobsSelectedTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            var job = await jobClient.GetJobAsync(jobID);

            Assert.IsNotNull(job);
            Assert.AreEqual(jobID, job.JobID);

            //run job
            await jobServer.RunJobsAsync(new List<string> { jobID });
            Thread.Sleep(5000);

            job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            Assert.AreEqual(JobStatus.Completed, job.Status);
        }


        [TestMethod]
        public async Task StopJobsNonRunningTest()
        {
            var jobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));
            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            var job = await jobClient.GetJobAsync(jobID);

            Assert.IsNotNull(job);
            Assert.AreEqual(JobCommand.Stop, job.Command);

            await jobServer.StopJobsAsync(); //stop non-running job
            Thread.Sleep(5000);

            job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            Assert.AreEqual(JobStatus.Stopped, job.Status);
        }

        [TestMethod]
        public async Task StopJobsRunningTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>(); 
            var token = (new CancellationTokenSource()).Token; 
            var jobID = await jobClient.AddAsync(AppID, () => jobTest.Start("Hello World", progress, token));

            //run job
            await jobServer.RunJobsAsync(new List<string> { jobID });
            Thread.Sleep(1000);

            var job = await jobClient.GetJobAsync(jobID);
            Assert.IsNotNull(job);
            Assert.AreEqual(JobStatus.Running, job.Status);

            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            await jobServer.StopJobsAsync(); //stop running job
            Thread.Sleep(3000);

            job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            Assert.AreEqual(JobStatus.Stopped, job.Status);
        }

        [TestMethod]
        public async Task CleanUpTest()
        {
            //Test StopJobs with CleanUp() calls

            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>(); 
            var token = (new CancellationTokenSource()).Token; 
            var jobID = await jobClient.AddAsync(AppID, () => jobTest.Start("Hello World", progress, token));

            //run job
            await jobServer.RunJobsAsync(new List<string> { jobID });
            Thread.Sleep(1000);

            var job = await jobClient.GetJobAsync(jobID);
            Assert.IsNotNull(job);
            Assert.AreEqual(JobStatus.Running, job.Status);

            await jobClient.SetCommandStopAsync(new List<string> { jobID });
            await jobServer.CleanUpAsync(); 
            Thread.Sleep(3000);

            job = await jobClient.GetJobAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            Assert.AreEqual(JobStatus.Stopped, job.Status);
        }

    }
}