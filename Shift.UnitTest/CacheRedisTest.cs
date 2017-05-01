using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shift.Entities;
using Autofac;
using Autofac.Features.ResolveAnything;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Threading;

namespace Shift.UnitTest
{
    [TestClass]
    public class CacheRedisTest
    {
        JobClient jobClient;
        JobServer jobServer;
        IJobDAL jobDAL;
        const string appID = "TestAppID";

        public CacheRedisTest()
        {
            //Configure storage connection
            var clientConfig = new ClientConfig();
            clientConfig.DBConnectionString = "Data Source=localhost\\SQL2014;Initial Catalog=ShiftJobsDB;Integrated Security=SSPI;";
            clientConfig.StorageMode = "mssql";
            jobClient = new JobClient(clientConfig);

            var serverConfig = new ServerConfig();
            serverConfig.DBConnectionString = "Data Source=localhost\\SQL2014;Initial Catalog=ShiftJobsDB;Integrated Security=SSPI;";
            serverConfig.StorageMode = "mssql";
            serverConfig.UseCache = true;
            serverConfig.CacheConfigurationString = "localhost:6379";
            serverConfig.ProcessID = "JobServerAsyncTest";
            serverConfig.Workers = 1;
            serverConfig.MaxRunnableJobs = 1;

            serverConfig.ProgressDBInterval = new TimeSpan(0);
            serverConfig.AutoDeletePeriod = null;
            serverConfig.ForceStopServer = true;
            serverConfig.StopServerDelay = 3000;
            jobServer = new JobServer(serverConfig);

            InitializeJobDAL(serverConfig);
        }

        private void InitializeJobDAL(ServerConfig config)
        {
            var builder = new ContainerBuilder();
            builder.RegisterSource(new AnyConcreteTypeNotAlreadyRegisteredSource());
            RegisterAssembly.RegisterTypes(builder, config.StorageMode, config.DBConnectionString, config.UseCache, config.CacheConfigurationString, config.EncryptionKey);
            var container = builder.Build();
            this.jobDAL = container.Resolve<IJobDAL>();
        }

        [TestMethod]
        [ExpectedException(typeof(System.ArgumentNullException))]
        public void CacheNoConnectionTest()
        {
            var jobTest = new Shift.Cache.Redis.JobCache(null);
        }

        [TestMethod]
        public void GetProgressTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var token = (new CancellationTokenSource()).Token;
            var jobID = jobClient.Add(appID, () => jobTest.Start("Hello World", progress, token));

            //run job
            jobServer.RunJobs(new List<string> { jobID });
            Thread.Sleep(3000);

            var jsProgress = jobDAL.GetProgress(jobID);
            jobClient.SetCommandStop(new List<string>() { jobID });
            jobServer.StopJobs();
            Thread.Sleep(3000);
            jobClient.DeleteJobs(new List<string>() { jobID });
            jobDAL.DeleteCachedProgressAsync(new List<string>() { jobID });

            Assert.AreEqual(jobID, jsProgress.JobID);
            Assert.IsTrue(jsProgress.Percent > 0);
        }

        [TestMethod]
        public async Task GetProgressAsyncTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var token = (new CancellationTokenSource()).Token;
            var jobID = await jobClient.AddAsync(appID, () => jobTest.Start("Hello World", progress, token));

            //run job
            await jobServer.RunJobsAsync(new List<string> { jobID });
            Thread.Sleep(3000);

            var jsProgress = await jobDAL.GetProgressAsync(jobID);
            await jobClient.SetCommandStopAsync(new List<string>() { jobID });
            await jobServer.StopJobsAsync();
            Thread.Sleep(3000);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            await jobDAL.DeleteCachedProgressAsync(new List<string>() { jobID });

            Assert.AreEqual(jobID, jsProgress.JobID);
            Assert.IsTrue(jsProgress.Percent > 0);
        }

        [TestMethod]
        public void GetCachedProgressTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var token = (new CancellationTokenSource()).Token;
            var jobID = jobClient.Add(appID, () => jobTest.Start("Hello World", progress, token));

            //run job
            jobServer.RunJobs(new List<string> { jobID });
            Thread.Sleep(3000);

            var jsProgress = jobDAL.GetCachedProgress(jobID);
            jobClient.SetCommandStop(new List<string>() { jobID });
            jobServer.StopJobs();
            Thread.Sleep(3000);
            jobClient.DeleteJobs(new List<string>() { jobID });
            jobDAL.DeleteCachedProgressAsync(new List<string>() { jobID });

            Assert.AreEqual(jobID, jsProgress.JobID);
            Assert.IsTrue(jsProgress.Percent > 0);
        }

        [TestMethod]
        public async Task GetCachedProgressAsyncTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var token = (new CancellationTokenSource()).Token;
            var jobID = await jobClient.AddAsync(appID, () => jobTest.Start("Hello World", progress, token));

            //run job
            await jobServer.RunJobsAsync(new List<string> { jobID });
            Thread.Sleep(3000);

            var jsProgress = await jobDAL.GetCachedProgressAsync(jobID);
            await jobClient.SetCommandStopAsync(new List<string>() { jobID });
            await jobServer.StopJobsAsync();
            Thread.Sleep(3000);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            await jobDAL.DeleteCachedProgressAsync(new List<string>() { jobID });

            Assert.AreEqual(jobID, jsProgress.JobID);
            Assert.IsTrue(jsProgress.Percent > 0);
        }

        [TestMethod]
        public async Task SetCachedProgressAsyncTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var token = (new CancellationTokenSource()).Token;
            var jobID = await jobClient.AddAsync(appID, () => jobTest.Start("Hello World", progress, token));

            await jobDAL.SetCachedProgressAsync(jobID, 10, "Test Note", "Test Data");
            var jsProgress = await jobDAL.GetCachedProgressAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            await jobDAL.DeleteCachedProgressAsync(jobID);

            Assert.AreEqual(jobID, jsProgress.JobID);
            Assert.AreEqual(10, jsProgress.Percent);
            Assert.AreEqual("Test Note", jsProgress.Note);
            Assert.AreEqual("Test Data", jsProgress.Data);
        }

        [TestMethod]
        public async Task SetCachedProgressErrorAsyncTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var token = (new CancellationTokenSource()).Token;
            var jobID = await jobClient.AddAsync(appID, () => jobTest.Start("Hello World", progress, token));

            await jobDAL.SetCachedProgressErrorAsync(jobID, "Test Error");
            var jsProgress = await jobDAL.GetCachedProgressAsync(jobID);
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            await jobDAL.DeleteCachedProgressAsync(jobID);

            Assert.AreEqual(jobID, jsProgress.JobID);
            Assert.AreEqual(JobStatus.Error, jsProgress.Status);
            Assert.AreEqual("Test Error", jsProgress.Error);
        }

        [TestMethod]
        public async Task DeleteCachedProgressAsyncTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var token = (new CancellationTokenSource()).Token;
            var jobID = await jobClient.AddAsync(appID, () => jobTest.Start("Hello World", progress, token));

            await jobDAL.SetCachedProgressAsync(jobID, 10, "Test Note", "Test Data");
            await jobClient.DeleteJobsAsync(new List<string>() { jobID });
            await jobDAL.DeleteCachedProgressAsync(jobID);
            var jsProgress = await jobDAL.GetCachedProgressAsync(jobID);

            Assert.IsNull(jsProgress);
        }

        [TestMethod]
        public async Task DeleteCachedProgressAsyncMultipleJobsTest()
        {
            var jobTest = new TestJob();
            var progress = new SynchronousProgress<ProgressInfo>();
            var token = (new CancellationTokenSource()).Token;
            var jobID = await jobClient.AddAsync(appID, () => jobTest.Start("Hello World", progress, token));
            var jobID2 = await jobClient.AddAsync(appID, () => jobTest.Start("Hello World", progress, token));

            await jobDAL.SetCachedProgressAsync(jobID, 10, "Test1 Note", "Test1 Data");
            await jobDAL.SetCachedProgressAsync(jobID2, 15, "Test2 Note", "Test2 Data");
            await jobClient.DeleteJobsAsync(new List<string>() { jobID, jobID2 });
            await jobDAL.DeleteCachedProgressAsync(new List<string>() { jobID, jobID2 });
            var jsProgress = await jobDAL.GetCachedProgressAsync(jobID);
            var jsProgress2 = await jobDAL.GetCachedProgressAsync(jobID2);

            Assert.IsNull(jsProgress);
            Assert.IsNull(jsProgress2);
        }
    }
}
