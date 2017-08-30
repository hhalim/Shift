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

    public class JobClientAsyncTest
    {
        private const string AppID = "AsyncTestAppID";
        private readonly string JobID = Guid.NewGuid().ToString("N");

        [Fact]
        public async Task GetJobAsync_NotValid()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetJobAsync(It.IsAny<string>()))
                .ReturnsAsync((Job)null);

            var jobClient = new JobClient(mockJobDAL.Object);
            var job = await jobClient.GetJobAsync("-123");

            Assert.Null(job);
        }

        [Fact]
        public async Task GetJobAsync_Valid()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetJobAsync(It.Is<string>(id => id== JobID)))
                .ReturnsAsync(new Job() { JobID = JobID });

            var jobClient = new JobClient(mockJobDAL.Object);
            var job = await jobClient.GetJobAsync(JobID);

            Assert.NotNull(job);
            Assert.Equal(JobID, job.JobID);
        }

        [Fact]
        public async Task AddJobAsyncTest1()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.AddAsync(null, null, null, null, It.IsAny<Expression<Action>>()))
                .ReturnsAsync(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = await jobClient.AddAsync(() => Console.WriteLine("Hello Test"));

            Assert.Equal(JobID, actualJobID);
        }

        [Fact]
        public async Task AddJobAsyncTest2()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.AddAsync(AppID, null, null, null, It.IsAny<Expression<Action>>()))
                .ReturnsAsync(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = await jobClient.AddAsync(AppID, () => Console.WriteLine("Hello Test"));

            Assert.Equal(JobID, actualJobID);
        }

        [Fact]
        public async Task AddJobAsyncTest3()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.AddAsync(AppID, "-123", "TestJobType", null, It.IsAny<Expression<Action>>()))
                .ReturnsAsync(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = await jobClient.AddAsync(AppID, "-123", "TestJobType", () => Console.WriteLine("Hello Test"));

            Assert.Equal(JobID, actualJobID);
        }

        [Fact]
        public async Task AddJobAsyncTest4()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.AddAsync(AppID, "-123", "TestJobType", "Test.JobName", It.IsAny<Expression<Action>>()))
                .ReturnsAsync(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = await jobClient.AddAsync(AppID, "-123", "TestJobType", "Test.JobName", () => Console.WriteLine("Hello Test"));

            Assert.Equal(JobID, actualJobID);
        }

        #region Add AsyncJob
        public async Task StartAsyncJob(string message)
        {
            Console.WriteLine(message);
            await Task.Delay(1000);
        }

        [Fact]
        public async Task AddAsyncJobAsyncTest1()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.AddAsync(null, null, null, null, It.IsAny<Expression<Func<Task>>>()))
                .ReturnsAsync(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = await jobClient.AddAsync(() => StartAsyncJob("Hello World Test!"));

            Assert.Equal(JobID, actualJobID);
        }

        [Fact]
        public async Task AddAsyncJobAsyncTest2()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.AddAsync(AppID, null, null, null, It.IsAny<Expression<Func<Task>>>()))
                .ReturnsAsync(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = await jobClient.AddAsync(AppID, () => StartAsyncJob("Hello World Test!"));

            Assert.Equal(JobID, actualJobID);
        }

        [Fact]
        public async Task  AddAsyncJobAsyncTest3()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.AddAsync(AppID, "-123", "TestJobType", null, It.IsAny<Expression<Func<Task>>>()))
                .ReturnsAsync(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = await jobClient.AddAsync(AppID, "-123", "TestJobType", () => StartAsyncJob("Hello World Test!"));

            Assert.Equal(JobID, actualJobID);
        }

        [Fact]
        public async Task  AddAsyncJobAsyncTest4()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.AddAsync(AppID, "-123", "TestJobType", "Test.JobName", It.IsAny<Expression<Func<Task>>>()))
                .ReturnsAsync(JobID);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actualJobID = await jobClient.AddAsync(AppID, "-123", "TestJobType", "Test.JobName", () => StartAsyncJob("Hello World Test!"));

            Assert.Equal(JobID, actualJobID);
        }
        #endregion

        [Fact]
        public async Task UpdateJobAsyncTest1()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.UpdateAsync(JobID, null, null, null, null, It.IsAny<Expression<Action>>()))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.UpdateAsync(JobID, () => Console.WriteLine("Hello AsyncTest Updated"));

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task UpdateJobAsyncTest2()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.UpdateAsync(JobID, "AsyncTestAppIDUpdated", null, null, null, It.IsAny<Expression<Action>>()))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.UpdateAsync(JobID, "AsyncTestAppIDUpdated", () => Console.WriteLine("Hello AsyncTest Updated"));
            
            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task UpdateJobAsyncTest3()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.UpdateAsync(JobID, "AsyncTestAppIDUpdated", "-222", "TestJobTypeUpdated", "Test.JobNameUpdated", It.IsAny<Expression<Action>>()))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.UpdateAsync(JobID, "AsyncTestAppIDUpdated", "-222", "TestJobTypeUpdated", "Test.JobNameUpdated", () => Console.WriteLine("Hello Test Updated"));

            Assert.Equal(expected, actual);
        }

        #region Update AsyncJob
        [Fact]
        public async Task UpdateAsyncJobTest1()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.UpdateAsync(JobID, null, null, null, null, It.IsAny<Expression<Func<Task>>>()))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.UpdateAsync(JobID, () => StartAsyncJob("Hello Test Updated"));

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task UpdateAsyncJobTest2()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.UpdateAsync(JobID, "TestAppIDUpdated", null, null, null, It.IsAny<Expression<Func<Task>>>()))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.UpdateAsync(JobID, "TestAppIDUpdated", () => StartAsyncJob("Hello Test Updated"));

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task UpdateAsyncJobTest3()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.UpdateAsync(JobID, "TestAppIDUpdated", "-222", "TestJobTypeUpdated", "Test.JobNameUpdated", It.IsAny<Expression<Func<Task>>>()))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.UpdateAsync(JobID, "TestAppIDUpdated", "-222", "TestJobTypeUpdated", "Test.JobNameUpdated", () => StartAsyncJob("Hello Test Updated"));

            Assert.Equal(expected, actual);
        }
        #endregion

        [Fact]
        public async Task SetCommandStopAsyncTest()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.SetCommandStopAsync(It.IsAny<IList<string>>()))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.SetCommandStopAsync(new List<string> { JobID });

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task SetCommandRunNowAsyncTest()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.SetCommandRunNowAsync(It.IsAny<IList<string>>()))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.SetCommandRunNowAsync(new List<string> { JobID });

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task GetJobViewAsyncTest()
        {
            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetJobViewAsync(JobID))
                .ReturnsAsync(new JobView() { JobID = JobID });

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.GetJobViewAsync(JobID);

            Assert.NotNull(actual);
            Assert.IsType<JobView>(actual);
            Assert.Equal(JobID, actual.JobID);
        }

        [Fact]
        public async Task GetJobViewsAsyncTest()
        {
            var pageIndex = 0;
            var pageSize = 10;
            var expected = 2;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetJobViewsAsync(pageIndex, pageSize))
                .ReturnsAsync(new JobViewList() { Total = expected, Items = new List<JobView>() { new JobView(), new JobView() } });

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.GetJobViewsAsync(pageIndex, pageSize);

            Assert.NotNull(actual);
            Assert.IsType<JobViewList>(actual);
            Assert.Equal(expected, actual.Total);
            Assert.True(actual.Items.Count == expected);
        }

        [Fact]
        public async Task ResetJobsAsyncTest()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.ResetAsync(new List<string>() { JobID } ))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.ResetJobsAsync(new List<string>() { JobID });

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task DeleteJobsAsyncTest()
        {
            var expected = 1;

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.DeleteAsync(new List<string>() { JobID }))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.DeleteJobsAsync(new List<string>() { JobID });

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task GetJobStatusCountAsyncTest()
        {
            var expected = new List<JobStatusCount>() { new JobStatusCount() };

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetJobStatusCountAsync(AppID, JobID))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.GetJobStatusCountAsync(AppID, JobID);

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task GetProgressAsyncTest()
        {
            var expected = new JobStatusProgress();

            var mockJobDAL = new Mock<IJobDAL>();
            mockJobDAL
                .Setup(ss => ss.GetProgressAsync(JobID))
                .ReturnsAsync(expected);

            var jobClient = new JobClient(mockJobDAL.Object);
            var actual = await jobClient.GetProgressAsync(JobID);

            Assert.IsType<JobStatusProgress>(actual);
            Assert.Equal(expected, actual);
        }

    }
}
