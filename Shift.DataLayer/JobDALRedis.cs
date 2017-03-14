using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using System.Data.SqlClient;
using System.Linq.Expressions;

using Newtonsoft.Json;
using Shift.Entities;
using StackExchange.Redis;

using Dapper;

namespace Shift.DataLayer
{
    public class JobDALRedis : IJobDAL
    {
        private string connectionString; //obsolete

        private string encryptionKey;
        const string JobKeyPrefix = "job:";
        const string JobStopKeyPrefix = "job-stop:"; //job-stop:[processID]

        const string JobIDMax = "jobid-max";
        const string JobQueue = "job-queue";
        const string JobSorted = "job-sorted"; //Hash set for sorted jobs by Created/Score

        //index
        const string JobStopIndex = "job-stop-index";

        const string JobCommandIndexTemplate = "job-[command]-index";
        const string JobCommandProcessTemplate = "job-[command]:[processid]";

        private readonly Lazy<ConnectionMultiplexer> lazyConnection;

        public ConnectionMultiplexer Connection
        {
            get
            {
                return lazyConnection.Value;
            }
        }

        public IDatabase RedisDatabase
        {
            get
            {
                return Connection.GetDatabase();
            }
        }

        #region Constructor
        public JobDALRedis(string connectionString, string encryptionKey)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException("connectionString");

            lazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(connectionString));
            this.encryptionKey = encryptionKey;
        }
        #endregion

        private int IncrementJobID()
        {
            var id = RedisDatabase.StringIncrement(JobIDMax);
            return (int)id;
        }

        #region insert/update job
        /// <summary>
        /// Add a new job in to queue.
        /// </summary>
        public int? Add(string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
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
            var job = new JobView();
            job.AppID = appID;
            job.UserID = userID;
            job.JobType = jobType;
            job.JobName = string.IsNullOrWhiteSpace(jobName) ? type.Name + "." + methodInfo.Name : jobName;
            job.InvokeMeta = JsonConvert.SerializeObject(invokeMeta, SerializerSettings.Settings);
            job.Parameters = Helpers.Encrypt(JsonConvert.SerializeObject(DALHelpers.SerializeArguments(args), SerializerSettings.Settings), encryptionKey); //ENCRYPT it!!!
            job.Created = now;
            job.Score = ((DateTimeOffset)now).ToUnixTimeSeconds();

            int? jobID = IncrementJobID();
            job.JobID = jobID.GetValueOrDefault();
            var key = JobKeyPrefix + jobID.GetValueOrDefault();
            //add into HashSet
            var tran = RedisDatabase.CreateTransaction();
            var hashEntries = RedisHelpers.ToHashEntries(job);
            tran.HashSetAsync(key, hashEntries);

            //Add to sorted set
            //The JobsSorted acts as the only way to sort the Jobs data, it is similar to the SQL version using Created field as a sort field.
            //The JobsSorted is not the same as JobQueue, hence the score doesn't change, it is always the same as Created field. 
            var index = tran.SortedSetAddAsync(JobSorted, new SortedSetEntry[] { new SortedSetEntry(key, job.Score) });

            //Add to queue
            //job.Score can change if set to run-now
            var index2 = tran.SortedSetAddAsync(JobQueue, new SortedSetEntry[] { new SortedSetEntry(key, job.Score) });

            tran.Execute();

            return jobID;
        }

        /// <summary>
        /// Update existing job, reset fields, return updated record count.
        /// </summary>
        public int Update(int jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
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
            var job = new JobView();
            job.JobID = jobID;
            job.AppID= appID;
            job.UserID = userID;
            job.JobType = jobType;
            job.JobName = string.IsNullOrWhiteSpace(jobName) ? type.Name + "." + methodInfo.Name : jobName;
            job.InvokeMeta = JsonConvert.SerializeObject(invokeMeta, SerializerSettings.Settings);
            job.Parameters = Helpers.Encrypt(JsonConvert.SerializeObject(DALHelpers.SerializeArguments(args), SerializerSettings.Settings), encryptionKey); //ENCRYPT it!!!
            job.Created = now;
            job.Status = null;
            job.Score = ((DateTimeOffset)now).ToUnixTimeSeconds();

            var count = 0;

            //Delete/reset job Progress => already reset with new jobView object

            //Update it
            var key = JobKeyPrefix + job.JobID;
            var tran = RedisDatabase.CreateTransaction();
            var hashEntries = RedisHelpers.ToHashEntries(job);
            tran.HashSetAsync(key, hashEntries);

            //Update jobs-sorted
            //The JobsSorted acts as the only way to sort the Jobs data, it is similar to the SQL version using Created field as a sort field.
            //The JobsSorted is not the same as JobQueue, hence the score doesn't change, it is always the same as Created field. 
            var index = tran.SortedSetAddAsync(JobSorted, new SortedSetEntry[] { new SortedSetEntry(key, job.Score) });

            //Update job-queue
            //job.Score can change if set to run-now
            var index2 = tran.SortedSetAddAsync(JobQueue, new SortedSetEntry[] { new SortedSetEntry(key, job.Score) });

            if(tran.Execute())
                count++;

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
        public int SetCommandStop(IList<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;
            foreach (var jobID in jobIDs) {
                var key = JobKeyPrefix + jobID.ToString();

                //Check status is null or status = running 
                var job = GetJob(jobID);
                if (job.Status == null || job.Status == JobStatus.Running)
                {
                    var tran = RedisDatabase.CreateTransaction();
                    if (string.IsNullOrWhiteSpace(job.ProcessID))
                    {
                        tran.HashSetAsync(JobStopIndex, new HashEntry[] { new HashEntry(key, "") }); //set hash job-stop-index job:123 ""
                    }
                    else
                    {
                        tran.HashSetAsync(JobStopKeyPrefix + job.ProcessID, new HashEntry[] { new HashEntry(key, "") }); //set job-stop:[processID] job:123 ""
                    }
                    tran.HashSetAsync(key, "Command", JobCommand.Stop);
                    if (tran.Execute())
                        count++;
                }
            }

            return count;
        }

        /// <summary>
        /// Set the Command field to run-now, only works for jobs with no status (ready to run).
        /// </summary>
        /// <remarks>
        /// </remarks>
        public int SetCommandRunNow(IList<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;
            foreach (var jobID in jobIDs)
            {
                var key = JobKeyPrefix + jobID.ToString();

                //Check status null and processID = empty
                var job = GetJob(jobID);
                if (job.Status == null && string.IsNullOrWhiteSpace(job.ProcessID))
                {
                    var tran = RedisDatabase.CreateTransaction();
                    tran.HashSetAsync(key, "Command", JobCommand.RunNow);
                    tran.HashSetAsync(key, "Score", 0);
                    //Set queue sort to 0
                    tran.SortedSetAddAsync(JobQueue, new SortedSetEntry[] { new SortedSetEntry(key, 0) });
                    if (tran.Execute())
                        count++;
                }
            }

            return count;
        }
        #endregion

        #region Direct Action to Jobs
        /// <summary>
        /// Reset jobs, only affect non-running jobs.
        /// </summary>
        public int Reset(IList<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;

            foreach (var jobID in jobIDs)
            {
                var key = JobKeyPrefix + jobID.ToString();

                //Check status null and status != running
                var jobHash = RedisDatabase.HashGetAll(key);
                var job = RedisHelpers.ConvertFromRedis<JobView>(jobHash);
                if (job.Status == null || job.Status != JobStatus.Running)
                {
                    var processID = job.ProcessID; //used to delete job-stop:[processid] keys

                    //reset progress
                    job.Data = null;
                    job.Percent = null;
                    job.Note = null;

                    //reset job
                    var score = ((DateTimeOffset)job.Created).ToUnixTimeSeconds(); //reset score to created
                    job.ProcessID = null;
                    job.Command = null;
                    job.Status = null;
                    job.Error = null;
                    job.Start = null;
                    job.End = null;
                    job.Score = score;
                    var hashEntries = RedisHelpers.ToHashEntries(job);

                    var tran = RedisDatabase.CreateTransaction();
                    //delete from stop index
                    tran = DeleteFromCommand(tran, JobCommand.Stop, processID, key); //Don't use job.ProcessID, already reset and always empty

                    tran.KeyDeleteAsync(key); //delete entire job object
                    tran.HashSetAsync(key, hashEntries); //add it back
                    tran.SortedSetAddAsync(JobQueue, new SortedSetEntry[] { new SortedSetEntry(key, score) }); //reset queue
                    tran.SortedSetAddAsync(JobSorted, new SortedSetEntry[] { new SortedSetEntry(key, score) }); //reset job sorted

                    if (tran.Execute())
                        count++;
                }
            }

            return count;
        }

        /// <summary>
        /// Delete jobs, only affect non-running jobs.
        /// </summary>
        public int Delete(IList<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;

            foreach (var jobID in jobIDs)
            {
                var key = JobKeyPrefix + jobID.ToString();

                //Check status is null or status != running 
                var job = GetJob(jobID);
                if (job.Status == null || job.Status != JobStatus.Running)
                {
                    var tran = RedisDatabase.CreateTransaction();
                    //delete from stop index
                    tran = DeleteFromCommand(tran, JobCommand.Stop, job.ProcessID, key);

                    tran.SortedSetRemoveAsync(JobQueue, key);
                    tran.SortedSetRemoveAsync(JobSorted, key);
                    tran.KeyDeleteAsync(key);
                    if (tran.Execute())
                        count++;
                }
            }

            return count;
        }

        private ITransaction DeleteFromCommand(ITransaction tran, string command, string processID, string hashField)
        {
            var key = "";
            if (string.IsNullOrWhiteSpace(processID))
            {
                //job-[command]-index
                key = JobCommandIndexTemplate.Replace("[command]", command.ToLower());
            }
            else
            {
                //job-[command]:[processid]
                key = JobCommandProcessTemplate
                    .Replace("[command]", command.ToLower())
                    .Replace("[processid]", processID);
            }
            tran.HashDeleteAsync(key, hashField); //delete job-stop-index job:123 

            return tran;
        }

        /// <summary>
        /// Delete past jobs with specified status(es). 
        /// </summary>
        /// <param name="hour">Job create hour in the past</param>
        /// <param name="statusList">A list of job's status to delete. Null job status is valid. Default is JobStatus.Completed.</param>
        public int Delete(int hour, IList<JobStatus?> statusList)
        {
            //TODO: DELETE based on JobsSorted scores

            var whereQuery = "j.Created < DATEADD(hour, -@hour, GETDATE())";

            //build where status
            if (statusList != null)
            {
                var whereStatus = "";
                foreach (var status in statusList)
                {
                    whereStatus += string.IsNullOrWhiteSpace(whereStatus) ? "" : " OR ";
                    if (status == null)
                    {
                        whereStatus += "j.Status IS NULL";
                    }
                    else
                    {
                        whereStatus += "j.Status = " + (int)status;
                    }
                }

                if (!string.IsNullOrWhiteSpace(whereStatus))
                {
                    whereQuery += " AND " + "(" + whereStatus + ")";
                }
            }

            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                //Get only the NON running jobs
                var sql = @"SELECT j.JobID 
                            FROM Job j
                            WHERE  " + whereQuery 
                            + " ORDER BY j.Created, j.JobID; "; // FIFO deletion
                var deleteIDs = connection.Query<int>(sql, new { hour }).ToList<int>();

                if (deleteIDs.Count > 0)
                {
                    //Delete JobProgress
                    sql = @"DELETE  
                            FROM JobProgress
                            WHERE JobID IN @ids; ";
                    connection.Execute(sql, new { ids = deleteIDs.ToArray() });

                    //Delete Job
                    sql = @"DELETE  
                            FROM Job
                            WHERE JobID IN @ids; ";
                    count = connection.Execute(sql, new { ids = deleteIDs.ToArray() });
                }
            }

            return count;
        }

        /// <summary>
        /// Asynchronous delete past jobs with specified status(es). 
        /// </summary>
        /// <param name="hour">Job create hour in the past</param>
        /// <param name="statusList">A list of job's status to delete. Null job status is valid. Default is JobStatus.Completed.</param>
        public async Task<int> DeleteAsync(int hour, IList<JobStatus?> statusList)
        {
            var count = await Task.Run(() => Delete(hour, statusList));
            return count;
        }

        /// <summary>
        ///  Mark job status to JobStatus.Stopped. 
        /// </summary>
        public int SetToStopped(IList<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;
            foreach( var jobID in jobIDs)
            {
                var fieldKey = JobKeyPrefix + jobID;

                var tran = RedisDatabase.CreateTransaction();
                //set job command to empty, status to Stopped
                tran.HashSetAsync(fieldKey, new HashEntry[] { new HashEntry ("Command", ""), new HashEntry ("Status", Convert.ToInt32(JobStatus.Stopped)) });

                //delete from job-stop-index and job-stop:processid
                var job = GetJob(jobID);
                tran = DeleteFromCommand(tran, JobCommand.Stop, job.ProcessID, fieldKey);
                if (tran.Execute())
                    count++;
            }

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
        public IList<JobStatusCount> GetJobStatusCount(string appID, string userID)
        {
            //TODO: REDIS, use ZScan per 10000 records and then count per status

            var countList = new List<JobStatusCount>();
            using (var connection = new SqlConnection(connectionString))
            {
                var sql = "";
                if (!string.IsNullOrWhiteSpace(appID) && !string.IsNullOrWhiteSpace(userID))
                {
                    sql = @"SELECT [Status], Count([Status]) [Count], sum(case when [Status] IS null then 1 else 0 end) NullCount
                          FROM [Job]
                          WHERE [AppID] = @appID AND [UserID] = @userID
                          GROUP BY [Status]; ";
                }
                else if (!string.IsNullOrWhiteSpace(appID) && string.IsNullOrWhiteSpace(userID)) //appID not null, userID is null
                {
                    sql = @"SELECT [Status], Count([Status]) [Count], sum(case when [Status] IS null then 1 else 0 end) NullCount
                          FROM [Job]
                          WHERE [AppID] = @appID
                          GROUP BY [Status]; ";
                }
                else if (string.IsNullOrWhiteSpace(appID) && !string.IsNullOrWhiteSpace(userID)) //appID is null, userID not null
                {
                    //This works okay for single tenant/app, but for multi-tenant, there can be multiple UserID for the different apps
                    //Works okay for multi tenant apps with GUID for UserID
                    sql = @"SELECT [Status], Count([Status]) [Count], sum(case when [Status] IS null then 1 else 0 end) NullCount
                          FROM [Job]
                          WHERE [UserID] = @userID
                          GROUP BY [Status]; ";
                }
                else
                {
                    sql = @"SELECT [Status], Count([Status]) [Count], sum(case when [Status] IS null then 1 else 0 end) NullCount
                          FROM [Job]
                          GROUP BY [Status]; ";
                }

                countList = connection.Query<JobStatusCount>(sql, new { appID, userID }).ToList();
            }

            return countList;
        }

        #endregion

        #region Various ways to get Jobs
        /// <summary>
        ///  Get Job object by specific jobID.
        /// </summary>
        /// <param name="jobID">The existing unique jobID</param>
        /// <returns>Job</returns>
        public Job GetJob(int jobID)
        {
            var hashEntry = RedisDatabase.HashGetAll(JobKeyPrefix + jobID);
            var job = RedisHelpers.ConvertFromRedis<Job>(hashEntry);
            return job;
        }

        /// <summary>
        ///  Get Jobs object by a group of jobIDs.
        /// </summary>
        /// <param name="jobIDs">group of jobIDs</param>
        /// <returns>List of Jobs</returns>
        public IList<Job> GetJobs(IEnumerable<int> jobIDs)
        {
            var jobList = new List<Job>();
            foreach (var jobID in jobIDs)
            {
                var hashEntry = RedisDatabase.HashGetAll(JobKeyPrefix + jobID);
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntry);
                jobList.Add(job);
            }

            return jobList;
        }

        /// <summary>
        ///  Get JobView by specific jobID.
        /// </summary>
        /// <param name="jobID">The existing unique jobID</param>
        /// <returns>JobView</returns>
        public JobView GetJobView(int jobID)
        {
            var hashEntry = RedisDatabase.HashGetAll(JobKeyPrefix + jobID);
            var jobView = RedisHelpers.ConvertFromRedis<JobView>(hashEntry);
            return jobView;
        }

        /// <summary>
        /// Get ready to run jobs by specified job IDs.
        /// </summary>
        /// <param name="jobIDs"></param>
        /// <returns></returns>
        public IList<Job> GetNonRunningJobsByIDs(IEnumerable<int> jobIDs)
        {
            var jobList = new List<Job>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT * 
                            FROM Job j
                            WHERE j.JobID IN @ids 
                            AND j.Status IS NULL; ";
                jobList = connection.Query<Job>(sql, new { ids = jobIDs.ToArray() }).ToList();

            }
            return jobList;
        }

        /// <summary>
        ///  Return all job IDs by specified command and owned by processID. And all jobs with specified command, but no owner.
        /// </summary>
        /// <param name="processID">The processID owning the jobs</param>
        /// <param name="command">The command specified in JobCommand</param>
        /// <returns>List of JobIDs</returns>
        public IList<int> GetJobIdsByProcessAndCommand(string processID, string command)
        {

            var key = "";
            //get from index: job-[command]-index
            key = JobCommandIndexTemplate.Replace("[command]", command.ToLower());
            var hashEntries = RedisDatabase.HashGetAll(key);
            var jobIDs = GetJobIDs(hashEntries);

            //get from process: job-[command]:[processid]
            key = JobCommandProcessTemplate
                .Replace("[command]", command.ToLower())
                .Replace("[processid]", processID);
            hashEntries = RedisDatabase.HashGetAll(key);
            jobIDs = jobIDs.Concat(GetJobIDs(hashEntries)).ToList();
            
            return jobIDs;
        }

        private IList<int> GetJobIDs(HashEntry[] hashEntries)
        {
            var jobIDs = new List<int>();
            foreach (var item in hashEntries)
            {
                var arr = item.Name.ToString().Split(':');
                if (arr.Count() == 2)
                    jobIDs.Add(Convert.ToInt32(arr[1])); //arr[0] = "job" ; arr[1] = ####
            }
            return jobIDs;
        }

        /// <summary>
        /// Return jobs based on owner processID and by job's status.
        /// </summary>
        /// <param name="processID">Owner processID</param>
        /// <param name="status">JobStatus</param>
        /// <returns>List of Jobs</returns>
        public IList<Job> GetJobsByProcessAndStatus(string processID, JobStatus status)
        {
            var jobList = new List<Job>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT * 
                            FROM Job j
                            WHERE j.ProcessID = @processID
                            AND j.Status = @status; ";
                jobList = connection.Query<Job>(sql, new { processID, status }).ToList();
            }

            return jobList;
        }

        /// <summary>
        /// Return jobs based on owner processID and specified jobIDs.
        /// </summary>
        /// <param name="processID">Owner processID</param>
        /// <param name="jobIDs">List of jobIDs</param>
        /// <returns>List of Jobs</returns>
        public IList<Job> GetJobsByProcess(string processID, IEnumerable<int> jobIDs)
        {
            var jobList = new List<Job>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"Select j.*
                            FROM Job j
                            WHERE j.ProcessID = @processID 
                            AND j.JobID IN @ids ;
                            ";
                jobList = connection.Query<Job>(sql, new { processID, ids = jobIDs.ToArray() }).ToList();
            }

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
            var totalCount = 0;

            pageIndex = pageIndex == null || pageIndex == 0 ? 1 : pageIndex; //default to 1
            pageSize = pageSize == null || pageSize == 0 ? 10 : pageSize; //default to 10

            var start = (pageIndex.Value - 1) * pageSize.Value;
            var stop = (pageIndex.Value * pageSize.Value) - 1;
            var resultSortedSet = RedisDatabase.SortedSetRangeByRankWithScores(JobSorted, start, stop, Order.Ascending);
            foreach(var sortedSet in resultSortedSet)
            {
                var hashEntry = RedisDatabase.HashGetAll(sortedSet.Element.ToString());
                var jobView = RedisHelpers.ConvertFromRedis<JobView>(hashEntry);
                result.Add(jobView);
                totalCount++;
            }

            var jobViewList = new JobViewList();
            jobViewList.Total= totalCount;
            jobViewList.Items = result;

            return jobViewList;
        }
        #endregion

        #region ManageJobs by Server
        /// <summary>
        /// Set job status to running and set start date and time to now.
        /// </summary>
        /// <param name="jobID">jobID</param>
        /// <returns>Updated record count, 0 or 1 record updated</returns>
        public int SetToRunning(int jobID)
        {
            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = "UPDATE [Job] SET Status = @status, [Start] = @start WHERE JobID = @jobID;";
                count = connection.Execute(sql, new { status = JobStatus.Running, start = DateTime.Now, jobID });
            }

            return count;
        }

        /// <summary>
        /// Set job status to error and fill in the error message.
        /// </summary>
        /// <param name="jobID">jobID</param>
        /// <param name="error">Error message</param>
        /// <returns>Updated record count, 0 or 1 record updated</returns>
        public int SetError(int jobID, string error)
        {
            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var sql = "UPDATE [Job] SET [Error] = @error, [Status] = @status WHERE JobID = @jobID;";
                count = connection.Execute(sql, new { error, status=JobStatus.Error, jobID });
            }

            return count;
        }

        /// <summary>
        /// Set job as completed.
        /// </summary>
        /// <param name="jobID">jobID</param>
        /// <returns>Updated record count, 0 or 1 record updated</returns>
        public int SetCompleted(int jobID)
        {
            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = "UPDATE [Job] SET Status = @status, [End] = @end WHERE JobID = @jobID;";
                count = connection.Execute(sql, new { status = JobStatus.Completed, end = DateTime.Now, jobID });
            }

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
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT COUNT(j.JobID) 
                            FROM Job j
                            WHERE j.Status = @status 
                            AND j.ProcessID = @processID; ";
                runningCount = connection.Query<int>(sql, new { status = JobStatus.Running, processID }).FirstOrDefault();
            }

            return runningCount;
        }

        /// <summary>
        /// Claim specific number of ready to run jobs to be owned by processID.
        /// Use Optimistic Concurrency, don't claim job if it's already running.
        /// </summary>
        /// <param name="processID">Owner processID</param>
        /// <param name="maxNum">Number of jobs to claim</param>
        /// <returns>List of jobs claimed by processID</returns>
        public IList<Job> ClaimJobsToRun(string processID, int maxNum)
        {
            var jobList = GetJobsToRun(maxNum);
            return ClaimJobsToRun(processID, jobList);
        }

        /// <summary>
        /// Attempt to claim specific jobs to be owned by processID.
        /// Use Optimistic Concurrency, don't claim job if it's already running or claimed by someone else.
        /// </summary>
        /// <param name="processID">Owner processID</param>
        /// <param name="jobList">List of jobs to claim</param>
        /// <returns>List of actual jobs claimed by processID</returns>
        public IList<Job> ClaimJobsToRun(string processID, IEnumerable<Job> jobList)
        {
            var claimedJobs = new List<Job>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                foreach(var job in jobList)
                {
                    var sql = @"UPDATE [Job]
                                SET ProcessID = @processID 
                                WHERE Status IS NULL 
                                AND ProcessID IS NULL
                                AND [jobID] = @jobID; ";
                    var count = connection.Execute(sql, new { processID, job.JobID });

                    if (count > 0) //successful update
                        claimedJobs.Add(job);
                }
            }

            return claimedJobs; //it's possible to return less than passed jobIDs, since multiple Shift server might run and already claimed the job(s)
        }

        /// <summary>
        /// Return ready to run or 'run-now' jobs based on a set number, don't return if it's already claimed by other processes.
        /// Sort by inserted date and by 'run-now' command. The jobs with 'run-now' command are given highest priority.
        /// </summary>
        /// <param name="maxNum">Maximum number to return</param>
        /// <returns>List of jobs</returns>
        private IList<Job> GetJobsToRun(int maxNum)
        {
            var jobList = new List<Job>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT * 
                            FROM Job j
                            WHERE j.Status IS NULL 
                            AND j.ProcessID IS NULL
                            AND (j.Command = @runNow OR j.Command IS NULL)
                            ORDER BY j.Score ASC
                            OFFSET 0 ROWS FETCH NEXT @maxNum ROWS ONLY; ";
                jobList = connection.Query<Job>(sql, new { runNow = JobCommand.RunNow, maxNum }).ToList();

            }

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
        public int SetProgress(int jobID, int? percent, string note, string data)
        {
            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var found = connection.Query<int>("SELECT COUNT(JobID) FROM JobProgress WHERE JobID = @jobID;", new { jobID }).FirstOrDefault(); ;
                if (found == 0)
                {
                    //INSERT
                    var sql = @"INSERT INTO JobProgress ([JobID], [Percent], [Note], [Data]) 
                                VALUES ( @jobID, @percent, @note, @data ); ";
                    count = connection.Execute(sql, new { jobID, percent, note, data });
                }
                else
                {
                    //UPDATE
                    var sql = @"UPDATE [JobProgress] SET [Percent] = @percent, Note = @note, Data = @data 
                                WHERE JobID = @jobID; ";
                    count = connection.Execute(sql, new { percent, note, data, jobID });
                }
            }

            return count;
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
        public int UpdateProgress(int jobID, int? percent, string note, string data)
        {
            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"UPDATE [JobProgress] SET [Percent] = @percent, Note = @note, Data = @data 
                            WHERE JobID = @jobID; ";
                count = connection.Execute(sql, new { percent, note, data, jobID });
            }

            return count;
        }

        /// <summary>
        /// Asynchronous update progress. 
        /// </summary>
        /// <param name="jobID">jobID</param>
        /// <param name="percent">% of progress</param>
        /// <param name="note">Any type of note for the progress</param>
        /// <param name="data">Any data for the progress</param>
        /// <returns>0 for no update, 1 for successful update</returns>
        public async Task<int> UpdateProgressAsync(int jobID, int? percent, string note, string data)
        {
            var count = await Task.Run(() => UpdateProgress(jobID, percent, note, data));
            return count;
        }

        #endregion


        #region Cache
        /* Use Cache and DB to return progress */
        public JobStatusProgress GetProgress(int jobID)
        {
            //No cache, so always get direct from Redis
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
                jsProgress.Error = "Job progress id: " +jobID + " not found!";
            }

            return jsProgress;
        }

        public JobStatusProgress GetCachedProgress(int jobID)
        {
            return GetProgress(jobID); //no cache in pure Redis
        }

        //Set Cached progress similar to the DB SetProgress()
        //Not needed in Redis
        public void SetCachedProgress(int jobID, int? percent, string note, string data)
        {
                return;
        }

        //Set cached progress status
        //Not needed in Redis
        public void SetCachedProgressStatus(int jobID, JobStatus status)
        {
                return;
        }

        //Not needed in Redis
        public void SetCachedProgressStatus(IEnumerable<int> jobIDs, JobStatus status)
        {
                return;
        }

        //Set cached progress error
        //Not needed in Redis
        public void SetCachedProgressError(int jobID, string error)
        {
                return;
        }

        //Not needed in Redis
        public void DeleteCachedProgress(int jobID)
        {
                return;
        }

        //Not needed in Redis
        public void DeleteCachedProgress(IEnumerable<int> jobIDs)
        {
                return;
        }

        #endregion 
    }
}
