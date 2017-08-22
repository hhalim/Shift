using System;
using System.Text;
using System.Collections.Generic;
using System.Configuration;
using Xunit;
using Shift.Entities;
using Moq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Shift.UnitTest
{

    public class JobClientTest
    {
        private const string AppID = "TestAppID";
        private readonly string JobID = Guid.NewGuid().ToString("N");

        [Fact]
        public void GetJob_NotValid()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetJob(It.IsAny<string>()))
                .Returns((Job)null);

            var jobClient = new JobClient(mockJobDAL.Object);
            var job = jobClient.GetJob("-123");

            Assert.Null(job);
        }

        [Fact]
        public void GetJob_Valid()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetJob(It.Is<string>(id => id== JobID)))
                .Returns(new Job() { JobID = JobID });

            var jobClient = new JobClient(mockJobDAL.Object);
            var job = jobClient.GetJob(JobID);

            Assert.NotNull(job);
            Assert.Equal(JobID, job.JobID);
        }

        [Fact]
        public void AddJobTest1()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.Add(null, null, null, null, It.IsAny<Expression<Action>>()))
                .Returns(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = jobClient.Add(() => Console.WriteLine("Hello Test"));

            Assert.Equal(JobID, actualJobID);
        }

        [Fact]
        public void AddJobTest2()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.Add(AppID, null, null, null, It.IsAny<Expression<Action>>()))
                .Returns(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = jobClient.Add(AppID, () => Console.WriteLine("Hello Test"));

            Assert.Equal(JobID, actualJobID);
        }

        [Fact]
        public void AddJobTest3()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.Add(AppID, "-123", "TestJobType", null, It.IsAny<Expression<Action>>()))
                .Returns(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = jobClient.Add(AppID, "-123", "TestJobType", () => Console.WriteLine("Hello Test"));

            Assert.Equal(JobID, actualJobID);
        }

        [Fact]
        public void AddJobTest4()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.Add(AppID, "-123", "TestJobType", "Test.JobName", It.IsAny<Expression<Action>>()))
                .Returns(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = jobClient.Add(AppID, "-123", "TestJobType", "Test.JobName", () => Console.WriteLine("Hello Test"));

            Assert.Equal(JobID, actualJobID);
        }

        [Fact]
        public void UpdateJobTest1()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.Update(JobID, null, null, null, null, It.IsAny<Expression<Action>>()))
                .Returns(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = jobClient.Update(JobID, () => Console.WriteLine("Hello Test Updated"));

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void UpdateJobTest2()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.Update(JobID, "TestAppIDUpdated", null, null, null, It.IsAny<Expression<Action>>()))
                .Returns(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = jobClient.Update(JobID, "TestAppIDUpdated", () => Console.WriteLine("Hello Test Updated"));
            
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void UpdateJobTest3()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.Update(JobID, "TestAppIDUpdated", "-222", "TestJobTypeUpdated", "Test.JobNameUpdated", It.IsAny<Expression<Action>>()))
                .Returns(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = jobClient.Update(JobID, "TestAppIDUpdated", "-222", "TestJobTypeUpdated", "Test.JobNameUpdated", () => Console.WriteLine("Hello Test Updated"));

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void SetCommandStopTest()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.SetCommandStop(It.IsAny<IList<string>>()))
                .Returns(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = jobClient.SetCommandStop(new List<string> { JobID });

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void SetCommandRunNowTest()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.SetCommandRunNow(It.IsAny<IList<string>>()))
                .Returns(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = jobClient.SetCommandRunNow(new List<string> { JobID });

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetJobViewTest()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetJobView(JobID))
                .Returns(new JobView() { JobID = JobID });

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = jobClient.GetJobView(JobID);

            Assert.NotNull(actual);
            Assert.IsType<JobView>(actual);
            Assert.Equal(JobID, actual.JobID);
        }

        [Fact]
        public void GetJobViewsTest()
        {
            var pageIndex = 0;
            var pageSize = 10;
            var expected = 2;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetJobViews(pageIndex, pageSize))
                .Returns(new JobViewList() { Total = expected, Items = new List<JobView>() { new JobView(), new JobView() } });

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = jobClient.GetJobViews(pageIndex, pageSize);

            Assert.NotNull(actual);
            Assert.IsType<JobViewList>(actual);
            Assert.Equal(expected, actual.Total);
            Assert.True(actual.Items.Count == expected);
        }

        [Fact]
        public void ResetJobsTest()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.Reset(new List<string>() { JobID } ))
                .Returns(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = jobClient.ResetJobs(new List<string>() { JobID });

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void DeleteJobsTest()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.Delete(new List<string>() { JobID }))
                .Returns(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = jobClient.DeleteJobs(new List<string>() { JobID });

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetJobStatusCountTest()
        {
            var expected = new List<JobStatusCount>() { new JobStatusCount() };

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetJobStatusCount(AppID, JobID))
                .Returns(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = jobClient.GetJobStatusCount(AppID, JobID);

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetProgressTest()
        {
            var expected = new JobStatusProgress();

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetProgress(JobID))
                .Returns(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = jobClient.GetProgress(JobID);

            Assert.IsType<JobStatusProgress>(actual);
            Assert.Equal(expected, actual);
        }


    }
}
