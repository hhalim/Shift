using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading;
using System.Collections.Generic;

using Autofac;
using Autofac.Features.ResolveAnything;

using Shift.Entities;

namespace Shift.UnitTest
{
    [TestClass]
    public class SqlJobServerTest
    {
        JobClient jobClient;
        JobServer jobServer;
        const string appID = "TestAppID";

        public SqlJobServerTest()
        {
            //Configure storage connection
            var clientConfig = new ClientConfig();
            clientConfig.DBConnectionString = "Data Source=localhost\\SQL2014;Initial Catalog=ShiftJobsDB;Integrated Security=SSPI;";
            clientConfig.StorageMode = "mssql";
            jobClient = new JobClient(clientConfig);

            var serverConfig = new ServerConfig();
            serverConfig.DBConnectionString = "Data Source=localhost\\SQL2014;Initial Catalog=ShiftJobsDB;Integrated Security=SSPI;";
            serverConfig.StorageMode = "mssql";
            serverConfig.ProcessID = "JobServerTest";
            serverConfig.Workers = 1;
            serverConfig.MaxRunnableJobs = 1;

            serverConfig.ProgressDBInterval = new TimeSpan(0);
            serverConfig.AutoDeletePeriod = null;
            serverConfig.ForceStopServer = true;
            serverConfig.StopServerDelay = 3000;
            jobServer = new JobServer(serverConfig);
        }

        [TestMethod]
        public void RunJobsTest()
        {
            var jobID = jobClient.Add(appID, () => Console.WriteLine("Hello Test"));
            var job = jobClient.GetJob(jobID);

            Assert.IsNotNull(job);
            Assert.AreEqual(jobID, job.JobID);

            //run job
            jobServer.RunJobs();
            Thread.Sleep(5000);

            job = jobClient.GetJob(jobID);
            Assert.AreEqual(JobStatus.Completed, job.Status);

            jobClient.DeleteJobs(new List<string>() { jobID });
        }

        [TestMethod]
        public void RunJobsSelectedTest()
        {
            var jobID = jobClient.Add(appID, () => Console.WriteLine("Hello Test"));
            var job = jobClient.GetJob(jobID);

            Assert.IsNotNull(job);
            Assert.AreEqual(jobID, job.JobID);

            //run job
            jobServer.RunJobs(new List<string> { jobID });
            Thread.Sleep(5000);

            job = jobClient.GetJob(jobID);
            Assert.AreEqual(JobStatus.Completed, job.Status);

            jobClient.DeleteJobs(new List<string>() { jobID });
        }


        [TestMethod]
        public void StopJobsNonRunningTest()
        {
            var jobID = jobClient.Add(appID, () => Console.WriteLine("Hello Test"));
            jobClient.SetCommandStop(new List<string> { jobID });
            var job = jobClient.GetJob(jobID);

            Assert.IsNotNull(job);
            Assert.AreEqual(JobCommand.Stop, job.Command);

            jobServer.StopJobs(); //stop non-running job
            Thread.Sleep(5000);

            job = jobClient.GetJob(jobID);
            Assert.AreEqual(JobStatus.Stopped, job.Status);

            jobClient.DeleteJobs(new List<string>() { jobID });
        }

        [TestMethod]
        public void StopJobsRunningTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>(); 
            var token = (new CancellationTokenSource()).Token; 
            var jobID = jobClient.Add(appID, () => jobTest.Start("Hello World", progress, token));

            //run job
            jobServer.RunJobs(new List<string> { jobID });
            Thread.Sleep(1000);

            var job = jobClient.GetJob(jobID);
            Assert.IsNotNull(job);
            Assert.AreEqual(JobStatus.Running, job.Status);

            jobClient.SetCommandStop(new List<string> { jobID });
            jobServer.StopJobs(); //stop running job
            Thread.Sleep(3000);

            job = jobClient.GetJob(jobID);
            Assert.AreEqual(JobStatus.Stopped, job.Status);

            jobClient.DeleteJobs(new List<string>() { jobID });
        }

        [TestMethod]
        public void CleanUpTest()
        {
            //Test StopJobs with CleanUp() calls

            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>(); 
            var token = (new CancellationTokenSource()).Token; 
            var jobID = jobClient.Add(appID, () => jobTest.Start("Hello World", progress, token));

            //run job
            jobServer.RunJobs(new List<string> { jobID });
            Thread.Sleep(1000);

            var job = jobClient.GetJob(jobID);
            Assert.IsNotNull(job);
            Assert.AreEqual(JobStatus.Running, job.Status);

            jobClient.SetCommandStop(new List<string> { jobID });
            jobServer.CleanUp(); 
            Thread.Sleep(3000);

            job = jobClient.GetJob(jobID);
            Assert.AreEqual(JobStatus.Stopped, job.Status);

            jobClient.DeleteJobs(new List<string>() { jobID });
        }

    }
}
