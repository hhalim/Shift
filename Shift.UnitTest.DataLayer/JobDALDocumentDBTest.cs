using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shift.Entities;
using System.Collections.Generic;
using Shift.DataLayer;

namespace Shift.UnitTest.DataLayer
{
    [TestClass]
    public class JobDALDocumentDBTest : JobDALDocumentDB
    {
        const string appID = "TestAppID";
        private static string connectionString = "https://shiftdb.documents.azure.com:443/";
        private static string encryptionKey = "";
        private static string authKey = "fwkADWkatMitV3jg4kcNCVGdDRK9GRgfBG6jcS374yz2SQt0d9DXML6LI3v42HKiBr4dU4vXgayhPVl4U0PwSw==";

        public JobDALDocumentDBTest() : base(connectionString, encryptionKey, authKey)
        {
        }

        [TestMethod]
        public void DeleteTest()
        {
            var jobID = Add(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));

            Delete(new List<string> { jobID });
            var job = GetJob(jobID);

            Assert.IsNull(job);
        }

        [TestMethod]
        public void GetJobTest()
        {
            var jobID = Add(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = GetJob(jobID);
            Delete(new List<string> { jobID });

            Assert.IsInstanceOfType(job, typeof(Job));
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public void GetJobViewTest()
        {
            var jobID = Add(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var job = GetJobView(jobID);
            Delete(new List<string> { jobID });

            Assert.IsInstanceOfType(job, typeof(JobView));
            Assert.AreEqual(jobID, job.JobID);
        }

        [TestMethod]
        public void AddTest()
        {
            var jobID = Add(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            Delete(new List<string> { jobID });
            Assert.IsTrue(!string.IsNullOrWhiteSpace(jobID));
        }

        [TestMethod]
        public void UpdateTest()
        {
            var jobID = Add(appID, "", "", "", () => Console.WriteLine("Hello World Test!"));
            var count = Update(jobID, appID, "", "", "JobNameUpdated", () => Console.WriteLine("Hello World Test!"));

            var job = GetJob(jobID);
            Delete(new List<string> { jobID });
            Assert.IsTrue(count > 0);
            Assert.AreEqual("JobNameUpdated", job.JobName);
        }


    }
}
