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
    public static class JobFields
    {
        public const string Command = "Command";
        public const string Status = "Status";
        public const string Error = "Error";
        public const string Start = "Start";
        public const string End = "End";
        public const string ProcessID = "ProcessID";

        public const string Percent = "Percent";
        public const string Note = "Note";
        public const string Data = "Data";
    }

    public class JobDALRedis : IJobDAL
    {
        private string encryptionKey;
        const string JobKeyPrefix = "job:";

        const string JobIDMax = "jobid-max";
        const string JobQueue = "job-queue";
        const string JobSorted = "job-sorted"; //Hash set for sorted jobs by jobID
        const string JobCreated = "job-created";

        //index template
        const string JobCommandIndexTemplate = "job-[command]-index";
        const string JobCommandProcessTemplate = "job-[command]:[processid]";
        const string JobStatusProcessTemplate = "job-[status]:[processid]";

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

            int? jobID = IncrementJobID();
            job.JobID = jobID.GetValueOrDefault();
            var key = JobKeyPrefix + jobID.GetValueOrDefault();
            //add into HashSet
            var trn = RedisDatabase.CreateTransaction();
            var hashEntries = RedisHelpers.ToHashEntries(job);
            trn.HashSetAsync(key, hashEntries);

            //Add to sorted set
            //The JobsSorted acts as the only way to sort the Jobs data, it is similar to the SQL version using jobID field as a sort field.
            var index = trn.SortedSetAddAsync(JobSorted, key, job.JobID);

            //Add to queue
            var index2 = trn.SortedSetAddAsync(JobQueue, key, job.JobID);

            //Add to created
            var createdTS = ((DateTimeOffset)now).ToUnixTimeSeconds();
            var index3 = trn.SortedSetAddAsync(JobCreated, key, createdTS);

            trn.Execute();

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

            var key = JobKeyPrefix + jobID;

            //Running check
            var status = RedisDatabase.HashGet(key, JobFields.Status);
            if ((int)status == (int)JobStatus.Running)
                return 0; //Unable to update if still running.

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

            var count = 0;

            //Delete/reset job Progress => already reset with new jobView object

            //Update it
            var trn = RedisDatabase.CreateTransaction();
            var hashEntries = RedisHelpers.ToHashEntries(job);
            trn.HashSetAsync(key, hashEntries);

            //Update jobs-sorted
            var index = trn.SortedSetAddAsync(JobSorted, key, job.JobID);

            //Update job-queue
            var index2 = trn.SortedSetAddAsync(JobQueue, key, job.JobID);

            //Update job-created
            var createdTS = ((DateTimeOffset)now).ToUnixTimeSeconds();
            var index3 = trn.SortedSetAddAsync(JobCreated, key, createdTS);

            if (trn.Execute())
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
        public int SetCommandStop(ICollection<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;
            var command = JobCommand.Stop.ToString().ToLower();
            var jobStopIndex = JobCommandIndexTemplate.Replace("[command]", command);
            var jobStopProcess = JobCommandProcessTemplate.Replace("[command]", command);
            foreach (var jobID in jobIDs) {
                var jobKey = JobKeyPrefix + jobID;

                //Check status is null or status = running 
                var job = GetJob(jobID);
                if (job.Status == null || job.Status == JobStatus.Running)
                {
                    var trn = RedisDatabase.CreateTransaction();
                    if (string.IsNullOrWhiteSpace(job.ProcessID))
                    {
                        trn.SetAddAsync(jobStopIndex, jobKey); //set hash job-stop-index job:123 ""
                    }
                    else
                    {
                        jobStopProcess = jobStopProcess.Replace("[processid]", job.ProcessID);
                        trn.SetAddAsync(jobStopProcess, jobKey); //set job-[command]:[processID] job:123 ""
                    }
                    trn.HashSetAsync(jobKey, JobFields.Command, JobCommand.Stop);
                    if (trn.Execute())
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
        public int SetCommandRunNow(ICollection<int> jobIDs)
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
                    var trn = RedisDatabase.CreateTransaction();
                    trn.HashSetAsync(key, JobFields.Command, JobCommand.RunNow);
                    //Set queue sort to 0
                    trn.SortedSetAddAsync(JobQueue, key, 0);
                    if (trn.Execute())
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
        public int Reset(ICollection<int> jobIDs)
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
                    var hashEntries = RedisHelpers.ToHashEntries(job);

                    var trn = RedisDatabase.CreateTransaction();
                    trn = CleanUpCommandAndStatusIndex(trn, processID, jobID);

                    trn.KeyDeleteAsync(key); //delete entire job object
                    trn.HashSetAsync(key, hashEntries); //add it back
                    trn.SortedSetAddAsync(JobQueue, key, jobID); //reset queue

                    if (trn.Execute())
                        count++;
                }
            }

            return count;
        }

        /// <summary>
        /// Delete jobs, only affect non-running jobs.
        /// </summary>
        public int Delete(ICollection<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;

            foreach (var jobID in jobIDs)
            {
                var jobKey = JobKeyPrefix + jobID.ToString();

                //Check status is null or status != running 
                var job = GetJob(jobID);
                if (job.Status == null || job.Status != JobStatus.Running)
                {
                    var trn = RedisDatabase.CreateTransaction();
                    trn = CleanUpCommandAndStatusIndex(trn, job.ProcessID, jobID);
                    trn.SortedSetRemoveAsync(JobQueue, jobKey);
                    trn.SortedSetRemoveAsync(JobSorted, jobKey);
                    trn.SortedSetRemoveAsync(JobCreated, jobKey);
                    trn.KeyDeleteAsync(jobKey);
                    if (trn.Execute())
                        count++;
                }
            }

            return count;
        }

        private ITransaction DeleteFromCommand(ITransaction trn, string command, string processID, string jobKey)
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
            trn.SetRemoveAsync(key, jobKey); 

            return trn;
        }

        private ITransaction DeleteFromStatus(ITransaction trn, string status, string processID, string jobKey)
        {
            //job-[status]:[processid]
            var key = JobStatusProcessTemplate
                .Replace("[status]", status.ToLower())
                .Replace("[processid]", processID);
            trn.SetRemoveAsync(key, jobKey); //delete job-[status]:[processingID] job:123 

            return trn;
        }

        private ITransaction CleanUpCommandAndStatusIndex(ITransaction trn, string processID, int jobID)
        {
            var jobKey = JobKeyPrefix + jobID;
            trn = DeleteFromCommand(trn, JobCommand.Stop, processID, jobKey); //stop index
            trn = DeleteFromStatus(trn, JobStatus.Running.ToString(), processID, jobKey); //running index

            return trn;
        }

        /// <summary>
        /// Delete past jobs with specified status(es). 
        /// Delete all jobs created older than hours specified. Only deletes older jobs with specified statuses.
        /// </summary>
        /// <param name="hours">Hours in the past</param>
        /// <param name="statusList">A list of job's status to delete. Null job status is valid. Default is JobStatus.Completed.</param>
        public int Delete(int hours, ICollection<JobStatus?> statusList)
        {
            var maxDate = DateTime.Now.AddHours(-hours);
            var maxTS = ((DateTimeOffset)maxDate).ToUnixTimeSeconds();

            var jobIDs = new List<int>();
            var sortedSetArray = RedisDatabase.SortedSetRangeByScoreWithScores(JobCreated, 0, maxTS, Exclude.Stop);
            foreach (var sortedSet in sortedSetArray)
            {
                var hashEntry = RedisDatabase.HashGetAll(sortedSet.Element.ToString());
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntry);
                if(statusList.Contains(job.Status))
                {
                    jobIDs.Add(job.JobID);
                }
            }
            
            var count = Delete(jobIDs); //Delete jobs, except the running jobs, can't delete running ones unless they're stopped first
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
        ///  Set job status to JobStatus.Stopped. 
        /// </summary>
        public int SetToStopped(ICollection<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;
            foreach( var jobID in jobIDs)
            {
                var jobKey= JobKeyPrefix + jobID;

                var trn = RedisDatabase.CreateTransaction();
                //set job command to empty, status to Stopped
                trn.HashSetAsync(jobKey, new HashEntry[] { new HashEntry (JobFields.Command, ""), new HashEntry (JobFields.Status, (int)JobStatus.Stopped) });
                trn.SortedSetRemoveAsync(JobQueue, jobKey); //remove from JobQueue

                //delete from job-stop-index and job-stop:processid
                var job = GetJob(jobID);
                trn = CleanUpCommandAndStatusIndex(trn, job.ProcessID, jobID);

                if (trn.Execute())
                    count++;
            }

            return count;
        }
        #endregion

        #region Count Status
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
            if (!string.IsNullOrWhiteSpace(appID) && !string.IsNullOrWhiteSpace(userID))
            {
                return GroupStatusCount(appID, userID);
            }
            else if (!string.IsNullOrWhiteSpace(appID) && string.IsNullOrWhiteSpace(userID)) //appID not null, userID is null
            {
                return GroupStatusCountByAppID(appID);
            }
            else if (string.IsNullOrWhiteSpace(appID) && !string.IsNullOrWhiteSpace(userID)) //appID is null, userID not null
            {
                return GroupStatusCountByUserID(userID);
            }

            return GroupStatusCount();
        }

        private IReadOnlyCollection<JobStatusCount> GroupStatusCount(string appID, string userID)
        {
            var groupStatus = new Dictionary<string, JobStatusCount>();
            var result = RedisDatabase.SortedSetScan(JobSorted, JobKeyPrefix + "*");

            foreach (var item in result)
            {
                var key = item.Element;
                var score = item.Score;

                var hashEntries = RedisDatabase.HashGetAll(key.ToString());
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntries);

                if (job.AppID == appID && job.UserID == userID)
                {
                    GroupStatusCount(groupStatus, job);
                }
            }

            return groupStatus.Values.ToList();
        }

        private IReadOnlyCollection<JobStatusCount> GroupStatusCountByAppID(string appID)
        {
            var groupStatus = new Dictionary<string, JobStatusCount>();
            var result = RedisDatabase.SortedSetScan(JobSorted, JobKeyPrefix + "*");

            foreach (var item in result)
            {
                var key = item.Element;
                var score = item.Score;

                var hashEntries = RedisDatabase.HashGetAll(key.ToString());
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntries);

                if (job.AppID == appID)
                {
                    GroupStatusCount(groupStatus, job);
                }
            }

            return groupStatus.Values.ToList();
        }

        private IReadOnlyCollection<JobStatusCount> GroupStatusCountByUserID(string userID)
        {
            var groupStatus = new Dictionary<string, JobStatusCount>();
            var result = RedisDatabase.SortedSetScan(JobSorted, JobKeyPrefix + "*");

            foreach (var item in result)
            {
                var key = item.Element;
                var score = item.Score;

                var hashEntries = RedisDatabase.HashGetAll(key.ToString());
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntries);

                if (job.UserID == userID)
                {
                    GroupStatusCount(groupStatus, job);
                }
            }

            return groupStatus.Values.ToList();
        }

        private IReadOnlyCollection<JobStatusCount> GroupStatusCount()
        {
            var groupStatus = new Dictionary<string, JobStatusCount>();
            var result = RedisDatabase.SortedSetScan(JobSorted, JobKeyPrefix + "*");

            foreach (var item in result)
            {
                var key = item.Element;
                var score = item.Score;

                var hashEntries = RedisDatabase.HashGetAll(key.ToString());
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntries);

                GroupStatusCount(groupStatus, job);
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
        public IReadOnlyCollection<Job> GetJobs(IEnumerable<int> jobIDs)
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
        public IReadOnlyCollection<Job> GetNonRunningJobsByIDs(IEnumerable<int> jobIDs)
        {
            var jobList = new List<Job>();
            foreach(var jobID in jobIDs)
            {
                var jobKey = JobKeyPrefix + jobID;
                var score = RedisDatabase.SortedSetScore(JobQueue, jobKey);
                if (score != null) {
                    RedisDatabase.SortedSetRemove(JobQueue, jobKey);
                    var job = GetJob(jobID);
                    jobList.Add(job);
                }
            }

            return jobList;
        }

        /// <summary>
        ///  Return all job IDs by specified command and owned by processID. And all jobs with specified command, but no owner.
        /// </summary>
        /// <param name="processID">The processID owning the jobs</param>
        /// <param name="command">The command specified in JobCommand</param>
        /// <returns>List of JobIDs</returns>
        public IReadOnlyCollection<int> GetJobIdsByProcessAndCommand(string processID, string command)
        {
            var jobIDs = new List<int>();

            var key = "";
            //get from index: job-[command]-index
            key = JobCommandIndexTemplate.Replace("[command]", command.ToLower());
            var elements = RedisDatabase.SetMembers(key);
            if (elements != null && elements.Count() > 0)
            {
                jobIDs = GetJobIDs(RedisHelpers.ToStringArray(elements)).ToList();
            }
            //get from process: job-[command]:[processid]
            key = JobCommandProcessTemplate
                .Replace("[command]", command.ToLower())
                .Replace("[processid]", processID);
            elements = RedisDatabase.SetMembers(key);
            if (elements != null && elements.Count() > 0)
            {
                var jobIDs2 = GetJobIDs(RedisHelpers.ToStringArray(elements)).ToList();
                jobIDs = jobIDs.Concat(jobIDs2).ToList();
            }

            return jobIDs;
        }

        private IReadOnlyCollection<int> GetJobIDs(string[] values)
        {
            var jobIDs = new List<int>();
            foreach (var item in values)
            {
                var arr = item.Split(':');
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
        public IReadOnlyCollection<Job> GetJobsByProcessAndStatus(string processID, JobStatus status)
        {
            var key = JobStatusProcessTemplate
                .Replace("[status]", status.ToString().ToLower())
                .Replace("[processid]", processID);
            var elements = RedisHelpers.ToStringArray(RedisDatabase.SetMembers(key));
            var jobList = new List<Job>();
            if (elements != null && elements.Count() > 0)
            {
                var jobIDs = GetJobIDs(elements);
                jobList = GetJobs(jobIDs).ToList();
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

            pageIndex = pageIndex == null || pageIndex == 0 ? 1 : pageIndex; //default to 1
            pageSize = pageSize == null || pageSize == 0 ? 10 : pageSize; //default to 10

            var start = (pageIndex.Value - 1) * pageSize.Value;
            var stop = (pageIndex.Value * pageSize.Value) - 1;
            var sortedSetArray = RedisDatabase.SortedSetRangeByRankWithScores(JobSorted, start, stop, Order.Ascending);
            foreach(var sortedSet in sortedSetArray)
            {
                var hashEntry = RedisDatabase.HashGetAll(sortedSet.Element.ToString());
                var jobView = RedisHelpers.ConvertFromRedis<JobView>(hashEntry);
                result.Add(jobView);
            }
            var totalCount = RedisDatabase.SortedSetLength(JobSorted);

            var jobViewList = new JobViewList();
            jobViewList.Total= (int)totalCount;
            jobViewList.Items = result;

            return jobViewList;
        }
        #endregion

        #region ManageJobs by Server
        /// <summary>
        /// Set job status to running and set start date and time to now.
        /// </summary>
        /// <param name="jobID">job ID</param>
        /// <param name="processID">process ID</param>
        /// <returns>Updated record count, 0 or 1 record updated</returns>
        public int SetToRunning(string processID, int jobID)
        {
            var count = 0;
            var jspKey = JobStatusProcessTemplate
                .Replace("[status]", JobStatus.Running.ToString().ToLower())
                .Replace("[processid]", processID);

            var jobKey = JobKeyPrefix + jobID;
            var trn = RedisDatabase.CreateTransaction();
            trn.HashSetAsync(jobKey, new HashEntry[] { new HashEntry(JobFields.Status, (int)JobStatus.Running), new HashEntry(JobFields.Start, DateTime.Now.ToString()) });
            trn.SetAddAsync(jspKey, jobKey); //set index
            if (trn.Execute())
                count++;

            return count;
        }

        /// <summary>
        /// Set job status to error and fill in the error message.
        /// </summary>
        /// <param name="processID">process ID</param>
        /// <param name="jobID">job ID</param>
        /// <param name="error">Error message</param>
        /// <returns>Updated record count, 0 or 1 record updated</returns>
        public int SetError(string processID, int jobID, string error)
        {
            var count = 0;

            var jobKey = JobKeyPrefix + jobID;

            var trn = RedisDatabase.CreateTransaction();
            trn.HashSetAsync(jobKey, new HashEntry[] { new HashEntry(JobFields.Status, (int)JobStatus.Error), new HashEntry(JobFields.Error, error) });
            trn.SortedSetRemoveAsync(JobQueue, jobKey); //Remove from queue
            trn = CleanUpCommandAndStatusIndex(trn, processID, jobID); //Remove from all stop/running indexes
            if(trn.Execute())
                count++;

            return count;
        }

        /// <summary>
        /// Set job as completed.
        /// </summary>
        /// <param name="processID">process ID</param>
        /// <param name="jobID">job ID</param>
        /// <returns>Updated record count, 0 or 1 record updated</returns>
        public int SetCompleted(string processID, int jobID)
        {
            var count = 0;

            var jspKey = JobStatusProcessTemplate
            .Replace("[status]", JobStatus.Running.ToString().ToLower())
            .Replace("[processid]", processID);
            var jobKey = JobKeyPrefix + jobID;

            var trn = RedisDatabase.CreateTransaction();
            trn.HashSetAsync(jobKey, new HashEntry[] { new HashEntry(JobFields.Status, (int)JobStatus.Completed), new HashEntry(JobFields.End, DateTime.Now.ToString()) });
            trn.SetRemoveAsync(jspKey, jobKey);
            if (trn.Execute())
                count++;

            return count;
        }

        /// <summary>
        /// Count how many running jobs owned by processID.
        /// </summary>
        /// <param name="processID">Owner processID</param>
        /// <returns>Total count of running jobs.</returns>
        public int CountRunningJobs(string processID)
        {
            var count = 0;
            var jspKey = JobStatusProcessTemplate
            .Replace("[status]", JobStatus.Running.ToString().ToLower())
            .Replace("[processid]", processID);
            count = Convert.ToInt32(RedisDatabase.SetLength(jspKey));

            return count;
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
            foreach (var job in jobList)
            {
                try
                {
                    var key = JobKeyPrefix + job.JobID;
                    if (RedisDatabase.HashSet(key, JobFields.ProcessID, processID, When.NotExists))
                    {
                        job.ProcessID = processID; //the job object is old, so set with the new processID
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

            var start = 0;
            var stop = maxNum - 1; //0 based index

            var trn = RedisDatabase.CreateTransaction();
            var resultSortedSet = trn.SortedSetRangeByRankWithScoresAsync(JobQueue, start, stop, Order.Ascending);
            trn.SortedSetRemoveRangeByRankAsync(JobQueue, start, stop); //remove from queue
            if (trn.Execute())
            {
                trn.WaitAll();
                foreach (var sortedSet in resultSortedSet.Result)
                {
                    var jobKey = sortedSet.Element.ToString();
                    var hashEntry = RedisDatabase.HashGetAll(jobKey);
                    var job = RedisHelpers.ConvertFromRedis<Job>(hashEntry);

                    //conditions
                    if (job.Status == null && job.ProcessID == null &&
                        (job.Command == null || job.Command == JobCommand.RunNow))
                    {
                        jobList.Add(job);
                    }
                }
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
            var task = UpdateProgressAsync(jobID, percent, note, data);
            return task.Result; //Possible Deadlock unless the ConfigureAwait(continueOnCapturedContext:false)
        }

        /// <summary>
        /// Update progress, similar to SetProgress() method. No difference in Redis.
        /// </summary>
        /// <param name="jobID">jobID</param>
        /// <param name="percent">% of progress</param>
        /// <param name="note">Any type of note for the progress</param>
        /// <param name="data">Any data for the progress</param>
        /// <returns>0 for no update, 1 for successful update</returns>
        public async Task<int> UpdateProgressAsync(int jobID, int? percent, string note, string data)
        {
            var count = 0;
            var jobKey = JobKeyPrefix + jobID;
            var trn = RedisDatabase.CreateTransaction();
            trn.HashSetAsync(jobKey, JobFields.Percent, percent);
            trn.HashSetAsync(jobKey, JobFields.Note, note);
            trn.HashSetAsync(jobKey, JobFields.Data, data);
            if (await trn.ExecuteAsync().ConfigureAwait(continueOnCapturedContext:false))
                count++;
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
