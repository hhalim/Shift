using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using System.Data.SqlClient;
using System.Linq.Expressions;

using Newtonsoft.Json;
using Shift.Entities;
using Dapper;

using MongoDB.Driver;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Bson;

namespace Shift.DataLayer
{
    public class JobDALMongo : IJobDAL
    {
        private const string JobCollectionName = "Jobs";

        private string connectionString;
        private string encryptionKey;

        private MongoClient client;
        private IMongoDatabase database;

        #region Constructor
        public JobDALMongo(string connectionString, string encryptionKey)
        {
            this.connectionString = connectionString;
            this.encryptionKey = encryptionKey;

            InitMongoDB();
        }

        public JobDALMongo(string connectionString, IJobCache jobCache, string encryptionKey)
        {
            this.connectionString = connectionString;
            this.encryptionKey = encryptionKey;

            InitMongoDB();
        }

        protected void InitMongoDB()
        {
            if (!BsonClassMap.IsClassMapRegistered(typeof(Job)))
            {
                BsonClassMap.RegisterClassMap<Job>(j =>
                {
                    j.AutoMap();
                    j.SetIgnoreExtraElements(true);
                    j.MapIdMember(p => p.JobID)
                    .SetIdGenerator(StringObjectIdGenerator.Instance)
                    .SetSerializer(new StringSerializer(BsonType.ObjectId));
                });

                BsonClassMap.RegisterClassMap<JobView>(j =>
                {
                    j.AutoMap();
                    j.SetIgnoreExtraElements(true);
                    j.MapIdMember(p => p.JobID)
                    .SetIdGenerator(StringObjectIdGenerator.Instance)
                    .SetSerializer(new StringSerializer(BsonType.ObjectId));
                });
            }

            var settings = MongoClientSettings.FromUrl(MongoUrl.Create(connectionString));
            client = new MongoClient(settings);

            database = client.GetDatabase("ShiftDB");
            var collection = database.GetCollection<Job>(JobCollectionName);
            collection.Indexes.CreateOne(Builders<Job>.IndexKeys.Ascending(j => j.Score));
            collection.Indexes.CreateOne(Builders<Job>.IndexKeys.Ascending(j => j.Created));
            collection.Indexes.CreateOne(Builders<Job>.IndexKeys.Ascending(j => j.Status));
            collection.Indexes.CreateOne(Builders<Job>.IndexKeys.Ascending(j => j.ProcessID));
        }

        #endregion

        #region insert/update job
        /// <summary>
        /// Add a new job in to queue.
        /// </summary>
        public string Add(string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            if (methodCall == null)
                throw new ArgumentNullException("methodCall");

            var callExpression = methodCall.Body as MethodCallExpression;
            if (callExpression == null)
            {
                throw new ArgumentException("Expression body must be 'System.Linq.Expressions.MethodCallExpression' type.", "methodCall");
            }

            Type type;
            if (callExpression.Object != null)
            {
                var value = DALHelpers.GetExpressionValue(callExpression.Object);
                if (value == null)
                    throw new InvalidOperationException("Expression object can not be null.");

                type = value.GetType();
            }
            else
            {
                type = callExpression.Method.DeclaringType;
            }

            var methodInfo = callExpression.Method;
            var args = callExpression.Arguments.Select(DALHelpers.GetExpressionValue).ToArray();

            if (type == null) throw new ArgumentNullException("type");
            if (methodInfo == null) throw new ArgumentNullException("method");
            if (args == null) throw new ArgumentNullException("args");

            DALHelpers.Validate(type, "type", methodInfo, "method", args.Length, "args");

            var invokeMeta = new InvokeMeta(type, methodInfo);

            //Save InvokeMeta and args
            var now = DateTime.Now;
            var job = new Job();
            job.AppID = appID;
            job.UserID = userID;
            job.JobType = jobType;
            job.JobName = string.IsNullOrWhiteSpace(jobName) ? type.Name + "." + methodInfo.Name : jobName;
            job.InvokeMeta = JsonConvert.SerializeObject(invokeMeta, SerializerSettings.Settings);
            job.Parameters = Helpers.Encrypt(JsonConvert.SerializeObject(DALHelpers.SerializeArguments(args), SerializerSettings.Settings), encryptionKey); //ENCRYPT it!!!
            job.Created = now;
            job.Score = (new DateTimeOffset(now)).ToUnixTimeSeconds();

            var collection = database.GetCollection<Job>(JobCollectionName);
            collection.InsertOneAsync(job);

            return job.JobID;
        }

        /// <summary>
        /// Update existing job, reset fields, return updated record count.
        /// </summary>
        public int Update(string jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            if (methodCall == null)
                throw new ArgumentNullException("methodCall");

            var callExpression = methodCall.Body as MethodCallExpression;
            if (callExpression == null)
            {
                throw new ArgumentException("Expression body must be 'System.Linq.Expressions.MethodCallExpression' type.", "methodCall");
            }

            Type type;
            if (callExpression.Object != null)
            {
                var value = DALHelpers.GetExpressionValue(callExpression.Object);
                if (value == null)
                    throw new InvalidOperationException("Expression object can not be null.");

                type = value.GetType();
            }
            else
            {
                type = callExpression.Method.DeclaringType;
            }

            var methodInfo = callExpression.Method;
            var args = callExpression.Arguments.Select(DALHelpers.GetExpressionValue).ToArray();

            if (type == null) throw new ArgumentNullException("type");
            if (methodInfo == null) throw new ArgumentNullException("method");
            if (args == null) throw new ArgumentNullException("args");

            DALHelpers.Validate(type, "type", methodInfo, "method", args.Length, "args");

            var invokeMeta = new InvokeMeta(type, methodInfo);

            //Save InvokeMeta and args
            var now = DateTime.Now;
            var job = new Job();
            job.JobID = jobID;
            job.AppID = appID;
            job.UserID = userID;
            job.JobType = jobType;
            job.JobName = string.IsNullOrWhiteSpace(jobName) ? type.Name + "." + methodInfo.Name : jobName;
            job.InvokeMeta = JsonConvert.SerializeObject(invokeMeta, SerializerSettings.Settings);
            job.Parameters = Helpers.Encrypt(JsonConvert.SerializeObject(DALHelpers.SerializeArguments(args), SerializerSettings.Settings), encryptionKey); //ENCRYPT it!!!
            job.Created = now;
            job.Score = (new DateTimeOffset(now)).ToUnixTimeSeconds();

            var count = 0;
            var collection = database.GetCollection<Job>(JobCollectionName);
            var filter = Builders<Job>.Filter.Eq(j => j.JobID, jobID);

            var result = collection.ReplaceOne(filter, job);
            if (result.IsAcknowledged)
                count = (int)result.ModifiedCount;

            return count;
        }
        #endregion

        #region Set Command field
        /// <summary>
        /// Flag jobs with 'stop' command. 
        /// </summary>
        /// <remarks>
        /// This works only for running jobs and jobs with no status. The server will attempt to 'stop' jobs marked as 'stop'.
        /// </remarks>
        public int SetCommandStop(ICollection<string> jobIDs)
        {
            var count = 0;

            if (jobIDs.Count == 0)
                return count;

            var collection = database.GetCollection<Job>(JobCollectionName);
            var blFilter = Builders<Job>.Filter;
            var filter = blFilter.In(j => j.JobID, jobIDs) & (blFilter.Eq(j => j.Status, null) | blFilter.Eq(j => j.Status, JobStatus.Running));
            var update = Builders<Job>.Update.Set("Command", JobCommand.Stop);

            var result = collection.UpdateMany(filter, update);
            if (result.IsAcknowledged)
                count = (int)result.ModifiedCount;

            return count;
        }

        /// <summary>
        /// Set the Command field to run-now, only works for jobs with no status (ready to run).
        /// </summary>
        /// <remarks>
        /// </remarks>
        public int SetCommandRunNow(ICollection<string> jobIDs)
        {
            var count = 0;

            if (jobIDs.Count == 0)
                return count;

            var collection = database.GetCollection<Job>(JobCollectionName);
            var blFilter = Builders<Job>.Filter;
            var filter = blFilter.In(j => j.JobID, jobIDs) & blFilter.Eq(j => j.Status, null) & blFilter.Eq(j => j.ProcessID, null);
            var blUpdate = Builders<Job>.Update;
            var listUpdate = new List<UpdateDefinition<Job>>();
            listUpdate.Add(blUpdate.Set("Command", JobCommand.RunNow));
            listUpdate.Add(blUpdate.Set<long>("Score", 0));
            var update = blUpdate.Combine(listUpdate.ToArray());

            var result = collection.UpdateMany(filter, update);
            if (result.IsAcknowledged)
                count = (int)result.ModifiedCount;

            return count;
        }
        #endregion

        #region Direct Action to Jobs
        /// <summary>
        /// Reset jobs, only affect non-running jobs.
        /// </summary>
        public int Reset(ICollection<string> jobIDs)
        {
            var count = 0;
            if (jobIDs.Count == 0)
                return count;

            var collection = database.GetCollection<JobView>(JobCollectionName);
            foreach (var jobID in jobIDs)
            {
                var job = collection.Find(j => j.JobID == jobID).FirstOrDefault();
                if(job != null)
                {
                    var score = (new DateTimeOffset(job.Created.GetValueOrDefault())).ToUnixTimeSeconds();
                    var blFilter = Builders<JobView>.Filter;
                    var filter = blFilter.Eq(j => j.JobID, jobID) & (blFilter.Eq(j => j.Status, null) | !blFilter.Eq(j => j.Status, JobStatus.Running));
                    var blUpdate = Builders<JobView>.Update;
                    var listUpdate = new List<UpdateDefinition<JobView>>();
                    listUpdate.Add(blUpdate.Set<string>("Data", null));
                    listUpdate.Add(blUpdate.Set<int?>("Percent", null));
                    listUpdate.Add(blUpdate.Set<string>("Note", null));
                    listUpdate.Add(blUpdate.Set<string>("ProcessID", null));
                    listUpdate.Add(blUpdate.Set<string>("Command", null));
                    listUpdate.Add(blUpdate.Set<int?>("Status", null));
                    listUpdate.Add(blUpdate.Set<string>("Error", null));
                    listUpdate.Add(blUpdate.Set<DateTime?>("Start", null));
                    listUpdate.Add(blUpdate.Set<DateTime?>("End", null));
                    listUpdate.Add(blUpdate.Set<long>("Score", score));
                    var update = blUpdate.Combine(listUpdate.ToArray());

                    var result = collection.UpdateOne(filter, update);
                    if (result.IsAcknowledged)
                        count += (int)result.ModifiedCount;
                }
            }

            return count;
        }

        /// <summary>
        /// Delete jobs, only affect non-running jobs.
        /// </summary>
        public int Delete(ICollection<string> jobIDs)
        {
            var count = 0;

            if (jobIDs.Count == 0)
                return count;

            var collection = database.GetCollection<Job>(JobCollectionName);
            var blFilter = Builders<Job>.Filter;
            var filter = blFilter.In(j => j.JobID, jobIDs) & (blFilter.Eq(j => j.Status, null) | !blFilter.Eq(j => j.Status, JobStatus.Running)); //Only NOT Running jobs

            var result = collection.DeleteMany(filter);
            if (result.IsAcknowledged)
                count = (int)result.DeletedCount;

            return count;
        }

        /// <summary>
        /// Delete past jobs with specified status(es). 
        /// </summary>
        /// <param name="hour">Job create hour in the past</param>
        /// <param name="statusList">A list of job's status to delete. Null job status is valid. Default is JobStatus.Completed.</param>
        public int Delete(int hour, ICollection<JobStatus?> statusList)
        {
            var count = 0;
            var collection = database.GetCollection<Job>(JobCollectionName);
            var blFilter = Builders<Job>.Filter;

            var pastDate = DateTime.Now.AddHours(-hour);
            //WARNING: The nullable Created field filter must use j.Created != null or it will CRASH the ToList action
            var dateFilter = (!blFilter.Eq(j => j.Created, null) & blFilter.Lt(j => j.Created, pastDate.ToUniversalTime())); //use UTC for datetime filtering!

            //build where status
            FilterDefinition<Job> statusFilter = null;
            if (statusList != null)
            {
                var listFilter = new List<FilterDefinition<Job>>();
                foreach (var status in statusList)
                {
                    if (status == null)
                    {
                        listFilter.Add(blFilter.Eq(j => j.Status, null));
                    }
                    else
                    {
                        listFilter.Add(blFilter.Eq(j => j.Status, status));
                    }
                }

                if (listFilter.Count > 0)
                {
                    statusFilter = blFilter.Or(listFilter.ToArray());
                }
            }

            FilterDefinition<Job> filter = null;
            if (statusFilter != null)
            {
                filter = dateFilter & statusFilter;
            }
            else
            {
                filter = dateFilter;
            }

            var result = collection.DeleteMany(filter);
            if (result.IsAcknowledged)
                count = (int)result.DeletedCount;

            return count;
        }

        /// <summary>
        /// Asynchronous delete past jobs with specified status(es). 
        /// </summary>
        /// <param name="hour">Job create hour in the past</param>
        /// <param name="statusList">A list of job's status to delete. Null job status is valid. Default is JobStatus.Completed.</param>
        public async Task<int> DeleteAsync(int hour, ICollection<JobStatus?> statusList)
        {
            var count = await Task.Run(() => Delete(hour, statusList));
            return count;
        }

        /// <summary>
        ///  Mark job status to JobStatus.Stopped. 
        /// </summary>
        public int SetToStopped(ICollection<string> jobIDs)
        {
            var count = 0;

            if (jobIDs.Count == 0)
                return count;

            var collection = database.GetCollection<Job>(JobCollectionName);
            var blFilter = Builders<Job>.Filter;
            var filter = blFilter.In(j => j.JobID, jobIDs);
            var blUpdate = Builders<Job>.Update;
            var listUpdate = new List<UpdateDefinition<Job>>();
            listUpdate.Add(blUpdate.Set<string>("Command", null));
            listUpdate.Add(blUpdate.Set("Status", JobStatus.Stopped));
            var update = blUpdate.Combine(listUpdate.ToArray());

            var result = collection.UpdateMany(filter, update);
            if (result.IsAcknowledged)
                count = (int)result.ModifiedCount;

            return count;
        }

        /// <summary>
        /// Return Job Status Count based on appID and/or userID.
        /// Must use unique appID for multi tenant client apps.
        /// Can return count based on only userID for single tenant client apps.
        /// </summary>
        /// <param name="appID"></param>
        /// <param name="userID"></param>
        /// <returns>JobStatusCount</returns>
        public IReadOnlyCollection<JobStatusCount> GetJobStatusCount(string appID, string userID)
        {
            return GetJobStatusCountAsync(appID, userID).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public async Task<IReadOnlyCollection<JobStatusCount>> GetJobStatusCountAsync(string appID, string userID)
        {
            var groupStatus = new Dictionary<string, JobStatusCount>();

            var collection = database.GetCollection<Job>(JobCollectionName);

            var blFilter = Builders<Job>.Filter;
            FilterDefinition<Job> filter = null;
            if (!string.IsNullOrWhiteSpace(appID) && !string.IsNullOrWhiteSpace(userID))
            {
                filter = blFilter.Eq(j => j.AppID, appID) & blFilter.Eq(j => j.UserID, userID);
            }
            else if (!string.IsNullOrWhiteSpace(appID) && string.IsNullOrWhiteSpace(userID)) //appID not null, userID is null
            {
                filter = blFilter.Eq(j => j.AppID, appID);
            }
            else if (string.IsNullOrWhiteSpace(appID) && !string.IsNullOrWhiteSpace(userID)) //appID is null, userID not null
            {
                filter = blFilter.Eq(j => j.UserID, userID);
            }

            IAsyncCursor<Job> cursor = null;
            if(filter == null)
            {
                cursor = await collection.FindAsync(p => true);
            }
            else
            {
                cursor = await collection.FindAsync(filter);
            }

            using (cursor)
            {
                while (await cursor.MoveNextAsync())
                {
                    var batch = cursor.Current;
                    foreach (var job in batch)
                    {
                        GroupStatusCount(groupStatus, job);
                    }

                }

            }

            return groupStatus.Values.ToList();
        }

        private static void GroupStatusCount(IDictionary<string, JobStatusCount> groupStatus, Job job)
        {
            var jsCount = new JobStatusCount();
            if (job.Status == null)
            {
                if (groupStatus.ContainsKey("NullStatus"))
                    jsCount = groupStatus["NullStatus"];
                jsCount.Status = null;
                jsCount.NullCount++;
                groupStatus["NullStatus"] = jsCount;
            }
            else
            {
                if (groupStatus.ContainsKey(job.StatusLabel))
                    jsCount = groupStatus[job.StatusLabel];
                jsCount.Status = job.Status;
                jsCount.Count++;
                groupStatus[job.StatusLabel] = jsCount;
            }
        }
        #endregion

        #region Various ways to get Jobs
        /// <summary>
        ///  Get Job object by specific jobID.
        /// </summary>
        /// <param name="jobID">The existing unique jobID</param>
        /// <returns>Job</returns>
        public Job GetJob(string jobID)
        {
            var collection = database.GetCollection<Job>(JobCollectionName);
            return collection.Find(j => j.JobID == jobID).FirstOrDefault();
        }

        /// <summary>
        ///  Get Jobs object by a group of jobIDs.
        /// </summary>
        /// <param name="jobIDs">group of jobIDs</param>
        /// <returns>List of Jobs</returns>
        public IReadOnlyCollection<Job> GetJobs(IEnumerable<string> jobIDs)
        {
            var collection = database.GetCollection<Job>(JobCollectionName);
            return collection.Find(j => jobIDs.Contains(j.JobID)).ToList();
        }

        /// <summary>
        ///  Get JobView by specific jobID.
        /// </summary>
        /// <param name="jobID">The existing unique jobID</param>
        /// <returns>JobView</returns>
        public JobView GetJobView(string jobID)
        {
            var collection = database.GetCollection<JobView>(JobCollectionName);
            return collection.Find(j => j.JobID == jobID).FirstOrDefault();
        }

        /// <summary>
        /// Get ready to run jobs by specified job IDs.
        /// </summary>
        /// <param name="jobIDs"></param>
        /// <returns></returns>
        public IReadOnlyCollection<Job> GetNonRunningJobsByIDs(IEnumerable<string> jobIDs)
        {
            var collection = database.GetCollection<Job>(JobCollectionName);
            return collection.Find(j => jobIDs.Contains(j.JobID) && j.Status == null).ToList();
        }

        /// <summary>
        ///  Return all job IDs by specified command and owned by processID. And all jobs with specified command, but no owner.
        /// </summary>
        /// <param name="processID">The processID owning the jobs</param>
        /// <param name="command">The command specified in JobCommand</param>
        /// <returns>List of JobIDs</returns>
        public IReadOnlyCollection<string> GetJobIdsByProcessAndCommand(string processID, string command)
        {
            var collection = database.GetCollection<Job>(JobCollectionName);
            var fields = Builders<Job>.Projection.Include(j => j.JobID);

            var queryResult = collection.Find(j => (j.ProcessID == processID || j.ProcessID == null) && j.Command == command)
                .Project<Job>(fields).ToList();
            var result = queryResult.Select(p => p.JobID).ToList();

            return result;
        }

        /// <summary>
        /// Return jobs based on owner processID and by job's status.
        /// </summary>
        /// <param name="processID">Owner processID</param>
        /// <param name="status">JobStatus</param>
        /// <returns>List of Jobs</returns>
        public IReadOnlyCollection<Job> GetJobsByProcessAndStatus(string processID, JobStatus status)
        {
            var collection = database.GetCollection<Job>(JobCollectionName);
            var jobList = collection.Find(j => j.ProcessID == processID && j.Status == status).ToList();
            return jobList;
        }

        /// <summary>
        /// Return job views based on page index and page size.
        /// </summary>
        /// <param name="pageIndex">Page index</param>
        /// <param name="pageSize">Page size</param>
        /// <returns>Total count of job views and list of JobViews</returns>
        public JobViewList GetJobViews(int? pageIndex, int? pageSize)
        {
            var result = new List<JobView>();

            pageIndex = pageIndex == null || pageIndex == 0 ? 1 : pageIndex; //default to 1
            pageSize = pageSize == null || pageSize == 0 ? 10 : pageSize; //default to 10
            var offset = (pageIndex.Value - 1) * pageSize.Value;

            var collection = database.GetCollection<JobView>(JobCollectionName);
            var query = collection.Find(j => true).SortBy(j => j.Created).SortBy(j => j.JobID);

            var totalTask = query.Count();
            var itemsTask = query.Skip(offset).Limit(pageSize).ToList();

            var jobViewList = new JobViewList();
            jobViewList.Total = totalTask;
            jobViewList.Items = itemsTask;

            return jobViewList;
        }
        #endregion

        #region ManageJobs by Server
        /// <summary>
        /// Set job status to running and set start date and time to now.
        /// </summary>
        /// <param name="processID">process ID</param>
        /// <param name="jobID">job ID</param>
        /// <returns>Updated record count, 0 or 1 record updated</returns>
        public int SetToRunning(string processID, string jobID)
        {
            var count = 0;

            var collection = database.GetCollection<Job>(JobCollectionName);
            var blFilter = Builders<Job>.Filter;
            var filter = blFilter.Eq(j => j.JobID, jobID) & blFilter.Eq(j => j.ProcessID, processID);
            var blUpdate = Builders<Job>.Update;
            var listUpdate = new List<UpdateDefinition<Job>>();
            listUpdate.Add(blUpdate.Set("Status", JobStatus.Running));
            listUpdate.Add(blUpdate.Set("Start", DateTime.Now));
            var update = blUpdate.Combine(listUpdate.ToArray());

            var result = collection.UpdateOne(filter, update);
            if (result.IsAcknowledged)
                count = (int)result.ModifiedCount;

            return count;
        }

        /// <summary>
        /// Set job status to error and fill in the error message.
        /// </summary>
        /// <param name="processID">process ID</param>
        /// <param name="jobID">job ID</param>
        /// <param name="error">Error message</param>
        /// <returns>Updated record count, 0 or 1 record updated</returns>
        public int SetError(string processID, string jobID, string error)
        {
            var count = 0;

            var collection = database.GetCollection<Job>(JobCollectionName);
            var blFilter = Builders<Job>.Filter;
            var filter = blFilter.Eq(j => j.JobID, jobID) & blFilter.Eq(j => j.ProcessID, processID);
            var blUpdate = Builders<Job>.Update;
            var listUpdate = new List<UpdateDefinition<Job>>();
            listUpdate.Add(blUpdate.Set("Status", JobStatus.Error));
            listUpdate.Add(blUpdate.Set("Error", error));
            var update = blUpdate.Combine(listUpdate.ToArray());

            var result = collection.UpdateOne(filter, update);
            if (result.IsAcknowledged)
                count = (int)result.ModifiedCount;

            return count;
        }

        /// <summary>
        /// Set job as completed.
        /// </summary>
        /// <param name="processID">process ID</param>
        /// <param name="jobID">job ID</param>
        /// <returns>Updated record count, 0 or 1 record updated</returns>
        public int SetCompleted(string processID, string jobID)
        {
            var count = 0;
            var collection = database.GetCollection<Job>(JobCollectionName);
            var blFilter = Builders<Job>.Filter;
            var filter = blFilter.Eq(j => j.JobID, jobID) & blFilter.Eq(j => j.ProcessID, processID);
            var blUpdate = Builders<Job>.Update;
            var listUpdate = new List<UpdateDefinition<Job>>();
            listUpdate.Add(blUpdate.Set("Command", ""));
            listUpdate.Add(blUpdate.Set("Status", JobStatus.Completed));
            listUpdate.Add(blUpdate.Set("End", DateTime.Now));
            var update = blUpdate.Combine(listUpdate.ToArray());

            var result = collection.UpdateOne(filter, update);
            if (result.IsAcknowledged)
                count = (int)result.ModifiedCount;

            return count;
        }

        /// <summary>
        /// Count how many running jobs owned by processID.
        /// </summary>
        /// <param name="processID">Owner processID</param>
        /// <returns>Total count of running jobs.</returns>
        public int CountRunningJobs(string processID)
        {
            var runningCount = 0;
 
            var collection = database.GetCollection<Job>(JobCollectionName);
            var builder = Builders<Job>.Filter;
            var filter = builder.Eq(j => j.ProcessID, processID) & builder.Eq(j => j.Status, JobStatus.Running);
            runningCount = (int)collection.Count(filter);

            return runningCount;
        }

        /// <summary>
        /// Claim specific number of ready to run jobs to be owned by processID.
        /// Use Optimistic Concurrency, don't claim job if it's already running.
        /// </summary>
        /// <param name="processID">Owner processID</param>
        /// <param name="maxNum">Number of jobs to claim</param>
        /// <returns>List of jobs claimed by processID</returns>
        public IReadOnlyCollection<Job> ClaimJobsToRun(string processID, int maxNum)
        {
            var jobList = GetJobsToRun(maxNum);
            return ClaimJobsToRun(processID, jobList.ToList());
        }

        /// <summary>
        /// Attempt to claim specific jobs to be owned by processID.
        /// Use Optimistic Concurrency, don't claim job if it's already running or claimed by someone else.
        /// </summary>
        /// <param name="processID">Owner processID</param>
        /// <param name="jobList">List of jobs to claim</param>
        /// <returns>List of actual jobs claimed by processID</returns>
        public IReadOnlyCollection<Job> ClaimJobsToRun(string processID, ICollection<Job> jobList)
        {
            var claimedJobs = new List<Job>();

            foreach(var job in jobList)
            {
                var count = 0;
                try
                {
                    var collection = database.GetCollection<Job>(JobCollectionName);
                    var blFilter = Builders<Job>.Filter;
                    var filter = blFilter.Eq(j => j.JobID, job.JobID) & blFilter.Eq(j => j.Status, null) & blFilter.Eq(j=> j.ProcessID, null);
                    var update = Builders<Job>.Update.Set("ProcessID", processID);

                    var result = collection.UpdateOne(filter, update);
                    if (result.IsAcknowledged)
                    {
                        count = (int)result.ModifiedCount;
                        job.ProcessID = processID; //set it similar to DB record!
                    }
                }
                catch (Exception exc)
                {
                    //just mark error, don't stop
                    var error = job.Error + " ClaimJobsToRun error: " + exc.ToString();
                    SetError(processID, job.JobID, error); //set error in storage
                    job.Status = JobStatus.Error;
                    job.Error = error;
                    continue;
                }

                if (count > 0) //successful update
                    claimedJobs.Add(job);
            }

            return claimedJobs; //it's possible to return less than passed jobIDs, since multiple Shift server might run and already claimed the job(s)
        }

        /// <summary>
        /// Return ready to run or 'run-now' jobs based on a set number, don't return if it's already claimed by other processes.
        /// Sort by inserted date and by 'run-now' command. The jobs with 'run-now' command are given highest priority.
        /// </summary>
        /// <param name="maxNum">Maximum number to return</param>
        /// <returns>List of jobs</returns>
        private IReadOnlyCollection<Job> GetJobsToRun(int maxNum)
        {
            var jobList = new List<Job>();
 
            var collection = database.GetCollection<Job>(JobCollectionName);
            jobList = collection
                .Find(j => j.Status == null && j.ProcessID == null && (j.Command == JobCommand.RunNow || j.Command == null))
                .SortBy(j => j.Score)
                .Limit(maxNum).ToList();

            return jobList;
        }

        /// <summary>
        /// Set progress for specific job.
        /// </summary>
        /// <param name="jobID">jobID</param>
        /// <param name="percent">% of progress</param>
        /// <param name="note">Any type of note for the progress</param>
        /// <param name="data">Any data for the progress</param>
        /// <returns>0 for no insert/update, 1 for successful insert/update</returns>
        public int SetProgress(string jobID, int? percent, string note, string data)
        {
            return SetProgressAsync(jobID, percent, note,data).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public async Task<int> SetProgressAsync(string jobID, int? percent, string note, string data)
        {
            return await UpdateProgressAsync(jobID, percent, note, data);
        }

        /// <summary>
        /// Update progress, similar to SetProgress() method. 
        /// Higher performance than SetProgress(). This method only uses 1 call to the database storage.
        /// </summary>
        /// <param name="jobID">jobID</param>
        /// <param name="percent">% of progress</param>
        /// <param name="note">Any type of note for the progress</param>
        /// <param name="data">Any data for the progress</param>
        /// <returns>0 for no update, 1 for successful update</returns>
        public async Task<int> UpdateProgressAsync(string jobID, int? percent, string note, string data)
        {
            var count = 0;
            var collection = database.GetCollection<Job>(JobCollectionName);
            var blFilter = Builders<Job>.Filter;
            var filter = blFilter.Eq(j => j.JobID, jobID);
            var blUpdate = Builders<Job>.Update;
            var listUpdate = new List<UpdateDefinition<Job>>();
            listUpdate.Add(blUpdate.Set("Percent", percent));
            listUpdate.Add(blUpdate.Set("Note", note));
            listUpdate.Add(blUpdate.Set("Data", data));
            var update = blUpdate.Combine(listUpdate.ToArray());
            var result = await collection.UpdateOneAsync(filter, update);

            if (result.IsAcknowledged)
                count = (int)result.ModifiedCount;

            return count;
        }

        #endregion


        #region Cache
        /* Use Cache and DB to return progress */
        public JobStatusProgress GetProgress(string jobID)
        {
            //No cache, so always get direct
            var jsProgress = new JobStatusProgress();
            //try to get from DB
            var jobView = GetJobView(jobID);
            if (jobView != null)
            {
                jsProgress.JobID = jobView.JobID;
                jsProgress.Status = jobView.Status;
                jsProgress.Error = jobView.Error;
                jsProgress.Percent = jobView.Percent;
                jsProgress.Note = jobView.Note;
                jsProgress.Data = jobView.Data;
            }
            else
            {
                jsProgress.JobID = jobID;
                jsProgress.ExistsInDB = false;
                jsProgress.Error = "Job progress id: " + jobID + " not found!";
            }

            return jsProgress;
        }

        public JobStatusProgress GetCachedProgress(string jobID)
        {
            return GetProgress(jobID); //no cache in pure Redis
        }

        //Set Cached progress similar to the DB SetProgress()
        //Not needed in Redis
        public void SetCachedProgress(string jobID, int? percent, string note, string data)
        {
            return;
        }

        //Set cached progress status
        //Not needed in Redis
        public void SetCachedProgressStatus(string jobID, JobStatus status)
        {
            return;
        }

        //Not needed in Redis
        public void SetCachedProgressStatus(IEnumerable<string> jobIDs, JobStatus status)
        {
            return;
        }

        //Set cached progress error
        //Not needed in Redis
        public void SetCachedProgressError(string jobID, string error)
        {
            return;
        }

        //Not needed in Redis
        public void DeleteCachedProgress(string jobID)
        {
            return;
        }

        //Not needed in Redis
        public void DeleteCachedProgress(IEnumerable<string> jobIDs)
        {
            return;
        }

        #endregion 
    }
}
