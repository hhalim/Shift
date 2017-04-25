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

        private IDatabase _IDatabase;
        private readonly Lazy<ConnectionMultiplexer> lazyConnection;

        public IDatabase RedisDatabase
        {
            get
            {
                if (_IDatabase == null)
                {
                    var connection = lazyConnection.Value;
                    connection.PreserveAsyncOrder = false;
                    _IDatabase = connection.GetDatabase();
                }
                return _IDatabase;
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

        private string IncrementJobID()
        {
            var id = RedisDatabase.StringIncrement(JobIDMax);
            return id.ToString();
        }

        #region insert/update job
        /// <summary>
        /// Add a new job in to queue.
        /// </summary>
        public string Add(string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            return AddAsync(appID, userID, jobType, jobName, methodCall, true).GetAwaiter().GetResult();
        }

        public Task<string> AddAsync(string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            return AddAsync(appID, userID, jobType, jobName, methodCall, false);
        }

        private async Task<string> AddAsync(string appID, string userID, string jobType, string jobName, Expression<Action> methodCall, bool isSync)
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

            var jobID = IncrementJobID();
            job.JobID = jobID;
            var key = JobKeyPrefix + jobID;
            //add into HashSet
            var trn = RedisDatabase.CreateTransaction();
            var hashEntries = RedisHelpers.ToHashEntries(job);
            trn.HashSetAsync(key, hashEntries);

            //Add to sorted set
            //The JobsSorted acts as the only way to sort the Jobs data, it is similar to the SQL version using jobID field as a sort field.
            var index = trn.SortedSetAddAsync(JobSorted, key, Convert.ToDouble(job.JobID));

            //Add to queue
            var index2 = trn.SortedSetAddAsync(JobQueue, key, Convert.ToDouble(job.JobID));

            //Add to created
            var createdTS = ((DateTimeOffset)now).ToUnixTimeSeconds();
            var index3 = trn.SortedSetAddAsync(JobCreated, key, createdTS);

            if (isSync)
                trn.Execute();
            else
                await trn.ExecuteAsync();

            return jobID;
        }

        /// <summary>
        /// Update existing job, reset fields, return updated record count.
        /// </summary>
        public int Update(string jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            return UpdateAsync(jobID, appID, userID, jobType, jobName, methodCall, true).GetAwaiter().GetResult();
        }

        public Task<int> UpdateAsync(string jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall)
        {
            return UpdateAsync(jobID, appID, userID, jobType, jobName, methodCall, false);
        }

        private async Task<int> UpdateAsync(string jobID, string appID, string userID, string jobType, string jobName, Expression<Action> methodCall, bool isSync)
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
            var status = isSync ? RedisDatabase.HashGet(key, JobFields.Status) : await RedisDatabase.HashGetAsync(key, JobFields.Status);
            if (!status.IsNullOrEmpty && (int)status == (int)JobStatus.Running)
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
            var index = trn.SortedSetAddAsync(JobSorted, key, Convert.ToDouble(job.JobID));

            //Update job-queue
            var index2 = trn.SortedSetAddAsync(JobQueue, key, Convert.ToDouble(job.JobID));

            //Update job-created
            var createdTS = ((DateTimeOffset)now).ToUnixTimeSeconds();
            var index3 = trn.SortedSetAddAsync(JobCreated, key, createdTS);

            if (isSync)
            {
                if (trn.Execute())
                    count++;
            } 
            else
            {
                if (await trn.ExecuteAsync())
                    count++;
            }
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
            return SetCommandStopAsync(jobIDs, true).GetAwaiter().GetResult();
        }

        public Task<int> SetCommandStopAsync(ICollection<string> jobIDs)
        {
            return SetCommandStopAsync(jobIDs, false);
        }

        private async Task<int> SetCommandStopAsync(ICollection<string> jobIDs, bool isSync)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;
            var command = JobCommand.Stop.ToString().ToLower();
            var jobStopIndex = JobCommandIndexTemplate.Replace("[command]", command);
            var jobStopProcessCommand = JobCommandProcessTemplate.Replace("[command]", command);
            foreach (var jobID in jobIDs) {
                var jobKey = JobKeyPrefix + jobID;

                //Check status is null or status = running 
                var job =  isSync ? GetJob(jobID) : await GetJobAsync(jobID);
                if (job != null && (job.Status == null || job.Status == JobStatus.Running))
                {
                    var trn = RedisDatabase.CreateTransaction();
                    if (string.IsNullOrWhiteSpace(job.ProcessID))
                    {
                        trn.SetAddAsync(jobStopIndex, jobKey); //set hash job-stop-index job:123 ""
                    }
                    else
                    {
                        var jobStopProcess = jobStopProcessCommand.Replace("[processid]", job.ProcessID);
                        trn.SetAddAsync(jobStopProcess, jobKey); //set job-[command]:[processID] job:123 ""
                    }
                    trn.HashSetAsync(jobKey, JobFields.Command, JobCommand.Stop);

                    if (isSync)
                    {
                        if (trn.Execute())
                            count++;
                    }
                    else
                    {
                        if (await trn.ExecuteAsync())
                            count++;
                    }
                }
            }

            return count;
        }

        /// <summary>
        /// Set the Command field to run-now, only works for jobs with no status (ready to run).
        /// </summary>
        /// <remarks>
        /// </remarks>
        public int SetCommandRunNow(ICollection<string> jobIDs)
        {
            return SetCommandRunNowAsync(jobIDs, true).GetAwaiter().GetResult();
        }

        public Task<int> SetCommandRunNowAsync(ICollection<string> jobIDs)
        {
            return SetCommandRunNowAsync(jobIDs, false);
        }

        private async Task<int> SetCommandRunNowAsync(ICollection<string> jobIDs, bool isSync)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;
            foreach (var jobID in jobIDs)
            {
                var key = JobKeyPrefix + jobID.ToString();

                //Check status null and processID = empty
                var job = isSync ? GetJob(jobID) : await GetJobAsync(jobID);
                if (job != null && job.Status == null && string.IsNullOrWhiteSpace(job.ProcessID))
                {
                    var trn = RedisDatabase.CreateTransaction();
                    trn.HashSetAsync(key, JobFields.Command, JobCommand.RunNow);
                    //Set queue sort to 0
                    trn.SortedSetAddAsync(JobQueue, key, 0);

                    if (isSync)
                    {
                        if (trn.Execute())
                            count++;
                    }
                    else
                    {
                        if (await trn.ExecuteAsync())
                            count++;
                    }
                }
            }

            return count;
        }
        #endregion

        #region Direct Action to Jobs
        /// <summary>
        /// Reset jobs, only affect non-running jobs.
        /// </summary>
        public int Reset(ICollection<string> jobIDs)
        {
            return ResetAsync(jobIDs, true).GetAwaiter().GetResult();
        }

        public Task<int> ResetAsync(ICollection<string> jobIDs)
        {
            return ResetAsync(jobIDs, false);
        }

        private async Task<int> ResetAsync(ICollection<string> jobIDs, bool isSync)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;

            foreach (var jobID in jobIDs)
            {
                var key = JobKeyPrefix + jobID.ToString();

                //Check status null and status != running
                var jobHash = isSync ? RedisDatabase.HashGetAll(key) : await RedisDatabase.HashGetAllAsync(key);

                var job = RedisHelpers.ConvertFromRedis<JobView>(jobHash);
                if (job != null && (job.Status == null || job.Status != JobStatus.Running))
                {
                    var processID = job.ProcessID; //used to delete job-stop:[processid] keys

                    //reset progress
                    job.Data = "";
                    job.Percent = null;
                    job.Note = "";

                    //reset job
                    var score = ((DateTimeOffset)job.Created).ToUnixTimeSeconds(); //reset score to created
                    job.ProcessID = "";
                    job.Command = "";
                    job.Status = null;
                    job.Error = "";
                    job.Start = null;
                    job.End = null;
                    var hashEntries = RedisHelpers.ToHashEntries(job);

                    var trn = RedisDatabase.CreateTransaction();
                    trn = CleanUpCommandAndStatusIndex(trn, processID, jobID);

                    trn.HashSetAsync(key, hashEntries); //add it back
                    trn.SortedSetAddAsync(JobQueue, key, Convert.ToDouble(jobID)); //reset queue

                    if (isSync)
                    {
                        if (trn.Execute())
                            count++;
                    }
                    else
                    {
                        if (await trn.ExecuteAsync())
                            count++;
                    }
                }
            }

            return count;
        }

        /// <summary>
        /// Delete jobs, only affect non-running jobs.
        /// </summary>
        public int Delete(ICollection<string> jobIDs)
        {
            return DeleteAsync(jobIDs, true).GetAwaiter().GetResult();
        }

        public Task<int> DeleteAsync(ICollection<string> jobIDs)
        {
            return DeleteAsync(jobIDs, false);
        }

        private async Task<int> DeleteAsync(ICollection<string> jobIDs, bool isSync)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;

            foreach (var jobID in jobIDs)
            {
                var jobKey = JobKeyPrefix + jobID.ToString();

                //Check status is null or status != running 
                var job = isSync ? GetJob(jobID) : await GetJobAsync(jobID);
                if (job != null && (job.Status == null || job.Status != JobStatus.Running))
                {
                    var trn = RedisDatabase.CreateTransaction();
                    trn = CleanUpCommandAndStatusIndex(trn, job.ProcessID, jobID);
                    trn.SortedSetRemoveAsync(JobQueue, jobKey);
                    trn.SortedSetRemoveAsync(JobSorted, jobKey);
                    trn.SortedSetRemoveAsync(JobCreated, jobKey);
                    trn.KeyDeleteAsync(jobKey);
                    if (isSync)
                    {
                        if (trn.Execute())
                            count++;
                    }
                    else
                    {
                        if (await trn.ExecuteAsync())
                            count++;
                    }
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

        private ITransaction CleanUpCommandAndStatusIndex(ITransaction trn, string processID, string jobID)
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
            return DeleteAsync(hours, statusList, true).GetAwaiter().GetResult();
        }

        public Task<int> DeleteAsync(int hours, ICollection<JobStatus?> statusList)
        {
            return DeleteAsync(hours, statusList, false);
        }

        private async Task<int> DeleteAsync(int hours, ICollection<JobStatus?> statusList, bool isSync)
        {
            var maxDate = DateTime.Now.AddHours(-hours);
            var maxTS = ((DateTimeOffset)maxDate).ToUnixTimeSeconds();

            var jobIDs = new List<string>();
            var sortedSetArray = isSync ? RedisDatabase.SortedSetRangeByScoreWithScores(JobCreated, 0, maxTS, Exclude.Stop)
                : await RedisDatabase.SortedSetRangeByScoreWithScoresAsync(JobCreated, 0, maxTS, Exclude.Stop);
            foreach (var sortedSet in sortedSetArray)
            {
                var hashEntry = isSync ? RedisDatabase.HashGetAll(sortedSet.Element.ToString())
                    : await RedisDatabase.HashGetAllAsync(sortedSet.Element.ToString());
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntry);
                if(job != null && statusList.Contains(job.Status))
                {
                    jobIDs.Add(job.JobID);
                }
            }
            
            var count = isSync ? Delete(jobIDs): await DeleteAsync(jobIDs); //Delete jobs, except the running jobs, can't delete running ones unless they're stopped first

            return count;
        }

        /// <summary>
        ///  Set job status to JobStatus.Stopped. 
        /// </summary>
        public int SetToStopped(ICollection<string> jobIDs)
        {
            return SetToStoppedAsync(jobIDs, true).GetAwaiter().GetResult();
        }

        public Task<int> SetToStoppedAsync(ICollection<string> jobIDs)
        {
            return SetToStoppedAsync(jobIDs, false);
        }

        private async Task<int> SetToStoppedAsync(ICollection<string> jobIDs, bool isSync)
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
                var job = isSync ? GetJob(jobID) : await GetJobAsync(jobID);
                if (job != null)
                {
                    trn = CleanUpCommandAndStatusIndex(trn, job.ProcessID, jobID);

                    if (isSync)
                    {
                        if (trn.Execute())
                            count++;
                    }
                    else
                    {
                        if (await trn.ExecuteAsync())
                            count++;
                    }
                }
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
            return GetJobStatusCountAsync(appID, userID, true).GetAwaiter().GetResult();
        }

        public Task<IReadOnlyCollection<JobStatusCount>> GetJobStatusCountAsync(string appID, string userID)
        {
            return GetJobStatusCountAsync(appID, userID, false);
        }

        private async Task<IReadOnlyCollection<JobStatusCount>> GetJobStatusCountAsync(string appID, string userID, bool isSync)
        {
            if (!string.IsNullOrWhiteSpace(appID) && !string.IsNullOrWhiteSpace(userID))
            {
                return await GroupStatusCountAsync(appID, userID, isSync);
            }
            else if (!string.IsNullOrWhiteSpace(appID) && string.IsNullOrWhiteSpace(userID)) //appID not null, userID is null
            {
                return await GroupStatusCountByAppIDAsync(appID, isSync);
            }
            else if (string.IsNullOrWhiteSpace(appID) && !string.IsNullOrWhiteSpace(userID)) //appID is null, userID not null
            {
                return await GroupStatusCountByUserIDAsync(userID, isSync);
            }

            return await GroupStatusCountAsync(isSync);
        }

        private async Task<IReadOnlyCollection<JobStatusCount>> GroupStatusCountAsync(string appID, string userID, bool isSync)
        {
            var groupStatus = new Dictionary<string, JobStatusCount>();
            var result = RedisDatabase.SortedSetScan(JobSorted, JobKeyPrefix + "*");

            foreach (var item in result)
            {
                var key = item.Element;
                var score = item.Score;

                var hashEntries = isSync ? RedisDatabase.HashGetAll(key.ToString()) : await RedisDatabase.HashGetAllAsync(key.ToString());
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntries);

                if (job != null && job.AppID == appID && job.UserID == userID)
                {
                    GatherGroupStatusCount(groupStatus, job);
                }
            }

            return groupStatus.Values.ToList();
        }

        private async Task<IReadOnlyCollection<JobStatusCount>> GroupStatusCountByAppIDAsync(string appID, bool isSync)
        {
            var groupStatus = new Dictionary<string, JobStatusCount>();
            var result = RedisDatabase.SortedSetScan(JobSorted, JobKeyPrefix + "*");

            foreach (var item in result)
            {
                var key = item.Element;
                var score = item.Score;

                var hashEntries = isSync ? RedisDatabase.HashGetAll(key.ToString()) : await RedisDatabase.HashGetAllAsync(key.ToString());
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntries);

                if (job != null && job.AppID == appID)
                {
                    GatherGroupStatusCount(groupStatus, job);
                }
            }

            return groupStatus.Values.ToList();
        }

        private async Task<IReadOnlyCollection<JobStatusCount>> GroupStatusCountByUserIDAsync(string userID, bool isSync)
        {
            var groupStatus = new Dictionary<string, JobStatusCount>();
            var result = RedisDatabase.SortedSetScan(JobSorted, JobKeyPrefix + "*");

            foreach (var item in result)
            {
                var key = item.Element;
                var score = item.Score;

                var hashEntries = isSync ? RedisDatabase.HashGetAll(key.ToString()) : await RedisDatabase.HashGetAllAsync(key.ToString());
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntries);

                if (job != null && job.UserID == userID)
                {
                    GatherGroupStatusCount(groupStatus, job);
                }
            }

            return groupStatus.Values.ToList();
        }

        private async Task<IReadOnlyCollection<JobStatusCount>> GroupStatusCountAsync(bool isSync)
        {
            var groupStatus = new Dictionary<string, JobStatusCount>();
            var result = RedisDatabase.SortedSetScan(JobSorted, JobKeyPrefix + "*");

            foreach (var item in result)
            {
                var key = item.Element;
                var score = item.Score;

                var hashEntries = isSync ? RedisDatabase.HashGetAll(key.ToString()) : await RedisDatabase.HashGetAllAsync(key.ToString());
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntries);

                if (job != null)
                    GatherGroupStatusCount(groupStatus, job);
            }

            return groupStatus.Values.ToList();
        }

        private static void GatherGroupStatusCount(IDictionary<string, JobStatusCount> groupStatus, Job job)
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
            return GetJobAsync(jobID, true).GetAwaiter().GetResult();
        }

        public Task<Job> GetJobAsync(string jobID)
        {
            return GetJobAsync(jobID, false);
        }

        private async Task<Job> GetJobAsync(string jobID, bool isSync)
        {
            var hashEntry = isSync ? RedisDatabase.HashGetAll(JobKeyPrefix + jobID) : await RedisDatabase.HashGetAllAsync(JobKeyPrefix + jobID);
            var job = RedisHelpers.ConvertFromRedis<Job>(hashEntry);
            return job;
        }

        /// <summary>
        ///  Get Jobs object by a group of jobIDs.
        /// </summary>
        /// <param name="jobIDs">group of jobIDs</param>
        /// <returns>List of Jobs</returns>
        public IReadOnlyCollection<Job> GetJobs(IEnumerable<string> jobIDs)
        {
            return GetJobsAsync(jobIDs, true).GetAwaiter().GetResult();
        }

        public Task<IReadOnlyCollection<Job>> GetJobsAsync(IEnumerable<string> jobIDs)
        {
            return GetJobsAsync(jobIDs, false);
        }

        private async Task<IReadOnlyCollection<Job>> GetJobsAsync(IEnumerable<string> jobIDs, bool isSync)
        {
            var jobList = new List<Job>();
            foreach (var jobID in jobIDs)
            {
                var hashEntry = isSync ? RedisDatabase.HashGetAll(JobKeyPrefix + jobID) : await RedisDatabase.HashGetAllAsync(JobKeyPrefix + jobID);
                var job = RedisHelpers.ConvertFromRedis<Job>(hashEntry);
                if(job != null)
                    jobList.Add(job);
            }

            return jobList;
        }

        /// <summary>
        ///  Get JobView by specific jobID.
        /// </summary>
        /// <param name="jobID">The existing unique jobID</param>
        /// <returns>JobView</returns>
        public JobView GetJobView(string jobID)
        {
            return GetJobViewAsync(jobID, true).GetAwaiter().GetResult();
        }

        public Task<JobView> GetJobViewAsync(string jobID)
        {
            return GetJobViewAsync(jobID, false);
        }

        private async Task<JobView> GetJobViewAsync(string jobID, bool isSync)
        {
            var hashEntry = isSync ? RedisDatabase.HashGetAll(JobKeyPrefix + jobID) : await RedisDatabase.HashGetAllAsync(JobKeyPrefix + jobID);
            var jobView = RedisHelpers.ConvertFromRedis<JobView>(hashEntry);
            return jobView;
        }

        /// <summary>
        /// Get ready to run jobs by specified job IDs.
        /// </summary>
        /// <param name="jobIDs"></param>
        /// <returns></returns>
        public IReadOnlyCollection<Job> GetNonRunningJobsByIDs(IEnumerable<string> jobIDs)
        {
            return GetNonRunningJobsByIDsAsync(jobIDs, true).GetAwaiter().GetResult();
        }

        public Task<IReadOnlyCollection<Job>> GetNonRunningJobsByIDsAsync(IEnumerable<string> jobIDs)
        {
            return GetNonRunningJobsByIDsAsync(jobIDs, false);
        }

        private async Task<IReadOnlyCollection<Job>> GetNonRunningJobsByIDsAsync(IEnumerable<string> jobIDs, bool isSync)
        {
            var jobList = new List<Job>();
            foreach(var jobID in jobIDs)
            {
                var jobKey = JobKeyPrefix + jobID;
                var score = isSync ? RedisDatabase.SortedSetScore(JobQueue, jobKey) : await RedisDatabase.SortedSetScoreAsync(JobQueue, jobKey);
                if (score != null) {
                    if(isSync)
                        RedisDatabase.SortedSetRemove(JobQueue, jobKey);
                    else
                        await RedisDatabase.SortedSetRemoveAsync(JobQueue, jobKey);

                    var job = isSync ? GetJob(jobID) : await GetJobAsync(jobID);
                    if(job != null)
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
        public IReadOnlyCollection<string> GetJobIdsByProcessAndCommand(string processID, string command)
        {
            return GetJobIdsByProcessAndCommandAsync(processID, command, true).GetAwaiter().GetResult();
        }

        public Task<IReadOnlyCollection<string>> GetJobIdsByProcessAndCommandAsync(string processID, string command)
        {
            return GetJobIdsByProcessAndCommandAsync(processID, command, false);
        }

        private  async Task<IReadOnlyCollection<string>> GetJobIdsByProcessAndCommandAsync(string processID, string command, bool isSync)
        {
            var jobIDs = new List<string>();

            var key = "";
            //get from index: job-[command]-index
            key = JobCommandIndexTemplate.Replace("[command]", command.ToLower());
            var elements = isSync ? RedisDatabase.SetMembers(key) : await RedisDatabase.SetMembersAsync(key);
            if (elements != null && elements.Count() > 0)
            {
                jobIDs = GetJobIDs(RedisHelpers.ToStringArray(elements)).ToList();
            }
            //get from process: job-[command]:[processid]
            key = JobCommandProcessTemplate
                .Replace("[command]", command.ToLower())
                .Replace("[processid]", processID);
            elements = isSync ? RedisDatabase.SetMembers(key) : await RedisDatabase.SetMembersAsync(key);
            if (elements != null && elements.Count() > 0)
            {
                var jobIDs2 = GetJobIDs(RedisHelpers.ToStringArray(elements)).ToList();
                jobIDs = jobIDs.Concat(jobIDs2).ToList();
            }

            return jobIDs;
        }

        private IReadOnlyCollection<string> GetJobIDs(string[] values)
        {
            var jobIDs = new List<string>();
            foreach (var item in values)
            {
                var arr = item.Split(':');
                if (arr.Count() == 2)
                    jobIDs.Add(arr[1]); //arr[0] = "job" ; arr[1] = ####
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
            return GetJobsByProcessAndStatusAsync(processID, status, true).GetAwaiter().GetResult();
        }

        public Task<IReadOnlyCollection<Job>> GetJobsByProcessAndStatusAsync(string processID, JobStatus status)
        {
            return GetJobsByProcessAndStatusAsync(processID, status, false);
        }

        private async Task<IReadOnlyCollection<Job>> GetJobsByProcessAndStatusAsync(string processID, JobStatus status, bool isSync)
        {
            var key = JobStatusProcessTemplate
                .Replace("[status]", status.ToString().ToLower())
                .Replace("[processid]", processID);
            var elements = isSync ? RedisHelpers.ToStringArray(RedisDatabase.SetMembers(key)) : RedisHelpers.ToStringArray(await RedisDatabase.SetMembersAsync(key));
            var jobList = new List<Job>();
            if (elements != null && elements.Count() > 0)
            {
                var jobIDs = GetJobIDs(elements);
                jobList = (isSync ? GetJobs(jobIDs) : await GetJobsAsync(jobIDs)).ToList();
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
            return GetJobViewsAsync(pageIndex, pageSize, true).GetAwaiter().GetResult();
        }

        public Task<JobViewList> GetJobViewsAsync(int? pageIndex, int? pageSize)
        {
            return GetJobViewsAsync(pageIndex, pageSize, false);
        }

        private async Task<JobViewList> GetJobViewsAsync(int? pageIndex, int? pageSize, bool isSync)
        {
            var result = new List<JobView>();

            pageIndex = pageIndex == null || pageIndex == 0 ? 1 : pageIndex; //default to 1
            pageSize = pageSize == null || pageSize == 0 ? 10 : pageSize; //default to 10

            var start = (pageIndex.Value - 1) * pageSize.Value;
            var stop = (pageIndex.Value * pageSize.Value) - 1;
            var sortedSetArray = isSync ? RedisDatabase.SortedSetRangeByRankWithScores(JobSorted, start, stop, Order.Ascending)
                : await RedisDatabase.SortedSetRangeByRankWithScoresAsync(JobSorted, start, stop, Order.Ascending);

            foreach(var sortedSet in sortedSetArray)
            {
                var hashEntry = isSync ? RedisDatabase.HashGetAll(sortedSet.Element.ToString()) : await RedisDatabase.HashGetAllAsync(sortedSet.Element.ToString());
                var jobView = RedisHelpers.ConvertFromRedis<JobView>(hashEntry);
                if(jobView != null)
                    result.Add(jobView);
            }
            var totalCount = isSync ? RedisDatabase.SortedSetLength(JobSorted) : await RedisDatabase.SortedSetLengthAsync(JobSorted);

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
        public int SetToRunning(string processID, string jobID)
        {
            return SetToRunningAsync(processID, jobID, true).GetAwaiter().GetResult();
        }

        public Task<int> SetToRunningAsync(string processID, string jobID)
        {
            return SetToRunningAsync(processID, jobID, false);
        }

        private async Task<int> SetToRunningAsync(string processID, string jobID, bool isSync)
        {
            var count = 0;
            var jspKey = JobStatusProcessTemplate
                .Replace("[status]", JobStatus.Running.ToString().ToLower())
                .Replace("[processid]", processID);

            var jobKey = JobKeyPrefix + jobID;
            var trn = RedisDatabase.CreateTransaction();
            trn.HashSetAsync(jobKey, new HashEntry[] { new HashEntry(JobFields.Status, (int)JobStatus.Running), new HashEntry(JobFields.Start, DateTime.Now.ToString()) });
            trn.SetAddAsync(jspKey, jobKey); //set index
            if (isSync)
            {
                if (trn.Execute())
                    count++;
            }
            else
            {
                if (await trn.ExecuteAsync())
                    count++;
            }

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
            return SetErrorAsync(processID, jobID, error, true).GetAwaiter().GetResult();
        }

        public Task<int> SetErrorAsync(string processID, string jobID, string error)
        {
            return SetErrorAsync(processID, jobID, error, false);
        }

        private async Task<int> SetErrorAsync(string processID, string jobID, string error, bool isSync)
        {
            var count = 0;

            var jobKey = JobKeyPrefix + jobID;

            var trn = RedisDatabase.CreateTransaction();
            trn.HashSetAsync(jobKey, new HashEntry[] { new HashEntry(JobFields.Status, (int)JobStatus.Error), new HashEntry(JobFields.Error, error) });
            trn.SortedSetRemoveAsync(JobQueue, jobKey); //Remove from queue
            trn = CleanUpCommandAndStatusIndex(trn, processID, jobID); //Remove from all stop/running indexes
            if (isSync)
            {
                if (trn.Execute())
                    count++;
            }
            else
            {
                if (await trn.ExecuteAsync())
                    count++;
            }

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
            return SetCompletedAsync(processID, jobID, true).GetAwaiter().GetResult();
        }

        public Task<int> SetCompletedAsync(string processID, string jobID)
        {
            return SetCompletedAsync(processID, jobID, false);
        }

        private async Task<int> SetCompletedAsync(string processID, string jobID, bool isSync)
        {
            var count = 0;

            var jobKey = JobKeyPrefix + jobID;

            var trn = RedisDatabase.CreateTransaction();
            trn.HashSetAsync(jobKey, new HashEntry[] {
                new HashEntry(JobFields.Status, (int)JobStatus.Completed)
                , new HashEntry(JobFields.Command, "")
                , new HashEntry(JobFields.End, DateTime.Now.ToString())
            });
            trn = CleanUpCommandAndStatusIndex(trn, processID, jobID);

            if (isSync)
            {
                if (trn.Execute())
                    count++;
            }
            else
            {
                if (await trn.ExecuteAsync())
                    count++;
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
            return CountRunningJobsAsync(processID, true).GetAwaiter().GetResult();
        }

        public Task<int> CountRunningJobsAsync(string processID)
        {
            return CountRunningJobsAsync(processID, false);
        }

        private async Task<int> CountRunningJobsAsync(string processID, bool isSync)
        {
            var count = 0;
            var jspKey = JobStatusProcessTemplate
            .Replace("[status]", JobStatus.Running.ToString().ToLower())
            .Replace("[processid]", processID);
            count = Convert.ToInt32(isSync ? RedisDatabase.SetLength(jspKey) : await RedisDatabase.SetLengthAsync(jspKey));

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

        public async Task<IReadOnlyCollection<Job>> ClaimJobsToRunAsync(string processID, int maxNum)
        {
            var jobList = await GetJobsToRunAsync(maxNum);
            return await ClaimJobsToRunAsync(processID, jobList.ToList());
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
            return ClaimJobsToRunAsync(processID, jobList, true).GetAwaiter().GetResult();
        }

        public Task<IReadOnlyCollection<Job>> ClaimJobsToRunAsync(string processID, ICollection<Job> jobList)
        {
            return ClaimJobsToRunAsync(processID, jobList, false);
        }

        private async Task<IReadOnlyCollection<Job>> ClaimJobsToRunAsync(string processID, ICollection<Job> jobList, bool isSync)
        {
            var claimedJobs = new List<Job>();
            foreach (var job in jobList)
            {
                try
                {
                    var key = JobKeyPrefix + job.JobID;
                    var result = isSync ? RedisDatabase.HashSet(key, JobFields.ProcessID, processID) : await RedisDatabase.HashSetAsync(key, JobFields.ProcessID, processID);
                }
                catch (Exception exc)
                {
                    //just mark error, don't stop
                    var error = job.Error + " ClaimJobsToRun error: " + exc.ToString();
                    if(isSync)
                        SetError(processID, job.JobID, error); //set error in storage
                    else
                        await SetErrorAsync(processID, job.JobID, error); 
                    job.Status = JobStatus.Error;
                    job.Error = error;
                    continue;
                }

                job.ProcessID = processID; //the job object is old, so set with the new processID
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
            return GetJobsToRunAsync(maxNum, true).GetAwaiter().GetResult();
        }

        private Task<IReadOnlyCollection<Job>> GetJobsToRunAsync(int maxNum)
        {
            return GetJobsToRunAsync(maxNum, false);
        }

        private async Task<IReadOnlyCollection<Job>> GetJobsToRunAsync(int maxNum, bool isSync)
        {
            var jobList = new List<Job>();

            var start = 0;
            var stop = maxNum - 1; //0 based index

            var trn = RedisDatabase.CreateTransaction(); 
            var taskResult = trn.SortedSetRangeByRankWithScoresAsync(JobQueue, start, stop, Order.Ascending);
            trn.SortedSetRemoveRangeByRankAsync(JobQueue, start, stop);
            var successTrn = false;
            if (isSync)
                successTrn = trn.Execute();
            else
                successTrn = await trn.ExecuteAsync();
            if (successTrn)
            {
                var resultSortedSet = await taskResult;
                foreach (var sortedSet in resultSortedSet)
                {
                    var jobKey = sortedSet.Element.ToString();
                    var hashEntry = isSync ? RedisDatabase.HashGetAll(jobKey) : await RedisDatabase.HashGetAllAsync(jobKey);
                    var job = RedisHelpers.ConvertFromRedis<Job>(hashEntry);

                    //conditions
                    if (job != null && job.Status == null && string.IsNullOrWhiteSpace(job.ProcessID) &&
                        (string.IsNullOrWhiteSpace(job.Command) || job.Command == JobCommand.RunNow))
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
        public int SetProgress(string jobID, int? percent, string note, string data)
        {
            return UpdateProgressAsync(jobID, percent, note, data, true).GetAwaiter().GetResult();
        }

        public Task<int> SetProgressAsync(string jobID, int? percent, string note, string data)
        {
            return UpdateProgressAsync(jobID, percent, note, data, false);
        }

        /// <summary>
        /// Update progress, similar to SetProgress() method. No difference in Redis.
        /// </summary>
        /// <param name="jobID">jobID</param>
        /// <param name="percent">% of progress</param>
        /// <param name="note">Any type of note for the progress</param>
        /// <param name="data">Any data for the progress</param>
        /// <returns>0 for no update, 1 for successful update</returns>
        public Task<int> UpdateProgressAsync(string jobID, int? percent, string note, string data)
        {
            return UpdateProgressAsync(jobID, percent, note, data, false);
        }

        private async Task<int> UpdateProgressAsync(string jobID, int? percent, string note, string data, bool isSync)
        {
            var count = 0;
            var jobKey = JobKeyPrefix + jobID;
            var trn = RedisDatabase.CreateTransaction();
            trn.HashSetAsync(jobKey, JobFields.Percent, percent);
            trn.HashSetAsync(jobKey, JobFields.Note, note);
            trn.HashSetAsync(jobKey, JobFields.Data, data);

            if (isSync)
            {
                if (trn.Execute())
                    count++;
            }
            else
            {
                if (await trn.ExecuteAsync())
                    count++;
            }
            return count;
        }

        #endregion


        #region Cache
        /* Use Cache and DB to return progress */
        public JobStatusProgress GetProgress(string jobID)
        {
            return GetProgressAsync(jobID, true).GetAwaiter().GetResult();
        }

        public Task<JobStatusProgress> GetProgressAsync(string jobID)
        {
            return GetProgressAsync(jobID, false);
        }

        private async Task<JobStatusProgress> GetProgressAsync(string jobID, bool isSync)
        {
            //No cache, so always get direct from Redis
            var jsProgress = new JobStatusProgress();
            //try to get from DB
            var jobView = isSync ? GetJobView(jobID) : await GetJobViewAsync(jobID);
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

        public JobStatusProgress GetCachedProgress(string jobID)
        {
            return GetProgress(jobID); //no cache in pure Redis
        }

        public Task<JobStatusProgress> GetCachedProgressAsync(string jobID)
        {
            return GetProgressAsync(jobID); //no cache in pure Redis
        }

        //Set Cached progress similar to the DB SetProgress()
        //Not needed in Redis
        public async Task SetCachedProgressAsync(string jobID, int? percent, string note, string data)
        {
                return;
        }

        //Set cached progress status
        //Not needed in Redis
        public async Task SetCachedProgressStatusAsync(string jobID, JobStatus status)
        {
                return;
        }

        //Not needed in Redis
        public async Task SetCachedProgressStatusAsync(IEnumerable<string> jobIDs, JobStatus status)
        {
                return;
        }

        //Set cached progress error
        //Not needed in Redis
        public async Task SetCachedProgressErrorAsync(string jobID, string error)
        {
                return;
        }

        //Not needed in Redis
        public async Task DeleteCachedProgressAsync(string jobID)
        {
                return;
        }

        //Not needed in Redis
        public async Task DeleteCachedProgressAsync(IEnumerable<string> jobIDs)
        {
                return;
        }

        #endregion 
    }
}
