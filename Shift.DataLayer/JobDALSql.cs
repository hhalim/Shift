using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using System.Data.SqlClient;
using System.Linq.Expressions;

using Newtonsoft.Json;
using Shift.Entities;
using Dapper;

namespace Shift.DataLayer
{
    public class JobDALSql : IJobDAL
    {
        private string connectionString;
        private IJobCache jobCache;
        private string encryptionKey;

        #region Constructor
        public JobDALSql(string connectionString, string encryptionKey)
        {
            this.connectionString = connectionString;
            this.jobCache = null;
            this.encryptionKey = encryptionKey;
        }

        public JobDALSql(string connectionString, IJobCache jobCache, string encryptionKey)
        {
            this.connectionString = connectionString;
            this.jobCache = jobCache;
            this.encryptionKey = encryptionKey;
        }
        #endregion

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
            var job = new Job();
            job.AppID = appID;
            job.UserID = userID;
            job.JobType = jobType;
            job.JobName = string.IsNullOrWhiteSpace(jobName) ? type.Name + "." + methodInfo.Name : jobName;
            job.InvokeMeta = JsonConvert.SerializeObject(invokeMeta, SerializerSettings.Settings);
            job.Parameters = Helpers.Encrypt(JsonConvert.SerializeObject(DALHelpers.SerializeArguments(args), SerializerSettings.Settings), encryptionKey); //ENCRYPT it!!!
            job.Created = now;

            int? jobID = null;
            using (var connection = new SqlConnection(connectionString))
            {
                var query = @"INSERT INTO [Job] ([AppID], [UserID], [JobType], [JobName], [InvokeMeta], [Parameters], [Created]) 
                              VALUES(@AppID, @UserID, @JobType, @JobName, @InvokeMeta, @Parameters, @Created);
                              SELECT CAST(SCOPE_IDENTITY() as int); ";
                jobID = connection.Query<int>(query, job).SingleOrDefault();
            }

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
            var values = new DynamicParameters();
            values.Add("JobID", jobID);
            values.Add("AppID", appID);
            values.Add("UserID", userID);
            values.Add("JobType", jobType);
            values.Add("JobName", string.IsNullOrWhiteSpace(jobName) ? type.Name + "." + methodInfo.Name : jobName);
            values.Add("InvokeMeta", JsonConvert.SerializeObject(invokeMeta, SerializerSettings.Settings));
            values.Add("Parameters", Helpers.Encrypt(JsonConvert.SerializeObject(DALHelpers.SerializeArguments(args), SerializerSettings.Settings), encryptionKey)); //ENCRYPT it!!!
            values.Add("Created", now);
            values.Add("Status", JobStatus.Running);

            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                //Delete job Progress
                var query2 = @"DELETE  
                            FROM JobProgress
                            WHERE JobID = @jobID; ";
                connection.Execute(query2, new { jobID });

                var query = @"
                            UPDATE [Job]
                            SET [AppID] = @AppID
                                ,[UserID] = @UserID
                                ,[ProcessID] = NULL
                                ,[JobType] = @JobType
                                ,[JobName] = @JobName
                                ,[InvokeMeta] = @InvokeMeta
                                ,[Parameters] = @Parameters
                                ,[Command] = NULL
                                ,[Status] = NULL
                                ,[Error] = NULL
                                ,[Start] = NULL
                                ,[End] = NULL
                                ,[Created] = @Created
                            WHERE JobID = @JobID AND (Status != @Status OR Status IS NULL);
                            ";
                count = connection.Execute(query, values);
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
        public int SetCommandStop(IList<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"UPDATE [Job] 
                            SET 
                            Command = @command 
                            WHERE JobID IN @ids 
                            AND (Status = @status OR Status IS NULL);
                            ";
                return connection.Execute(sql, new { command = JobCommand.Stop, ids = jobIDs.ToArray(), status = JobStatus.Running });
            }
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

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"UPDATE [Job] 
                            SET 
                            Command = @command 
                            WHERE JobID IN @ids 
                            AND Status IS NULL
                            AND ProcessID IS NULL;
                            ";
                return connection.Execute(sql, new { command = JobCommand.RunNow, ids = jobIDs.ToArray() });
            }
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

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var sql = @"SELECT j.JobID 
                            FROM Job j
                            WHERE j.JobID IN @ids
                            AND (j.Status != @status OR j.Status IS NULL); ";
                var notRunning = connection.Query<int>(sql, new { ids = jobIDs.ToArray(), status = JobStatus.Running }).ToList<int>();

                if (notRunning.Count > 0)
                {
                    //Reset jobs and progress for NON running jobs
                    sql = @"UPDATE JobProgress 
                            SET 
                            [Percent] = NULL, 
                            Note = NULL,
                            Data = NULL
                            WHERE JobID IN @ids; ";
                    connection.Execute(sql, new { ids = notRunning.ToArray() });

                    sql = @"UPDATE Job 
                        SET 
                        ProcessID = NULL, 
                        Command = NULL, 
                        Status = NULL, 
                        Error = NULL,
                        [Start] = NULL, 
                        [End] = NULL 
                        WHERE JobID IN @ids; ";
                    return connection.Execute(sql, new { ids = notRunning.ToArray() });
                }
            }

            return 0;
        }

        /// <summary>
        /// Delete jobs, only affect non-running jobs.
        /// </summary>
        public int Delete(IList<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                //Get only the NON running jobs
                var sql = @"SELECT j.JobID 
                            FROM Job j
                            WHERE j.JobID IN @ids
                            AND (j.Status != @status OR j.Status IS NULL); ";
                var notRunning = connection.Query<int>(sql, new { ids = jobIDs.ToArray(), status = JobStatus.Running }).ToList<int>();

                if (notRunning.Count > 0)
                {
                    //Delete only the NON running jobs
                    //Delete JobProgress
                    sql = @"DELETE  
                            FROM JobProgress
                            WHERE JobID IN @ids; ";
                    connection.Execute(sql, new { ids = notRunning.ToArray() });

                    //Delete Job
                    sql = @"DELETE  
                            FROM Job
                            WHERE JobID IN @ids; ";
                    count = connection.Execute(sql, new { ids = notRunning.ToArray() });
                }
            }

            return count;
        }

        /// <summary>
        /// Delete past jobs with specified status(es). 
        /// </summary>
        /// <param name="hour">Job create hour in the past</param>
        /// <param name="statusList">A list of job's status to delete. Null job status is valid. Default is JobStatus.Completed.</param>
        public int Delete(int hour, IList<JobStatus?> statusList)
        {
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

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"UPDATE Job 
                            SET 
                            Command = NULL, 
                            Status = @status
                            WHERE JobID IN @ids; ";
                return connection.Execute(sql, new { status = JobStatus.Stopped, ids = jobIDs.ToArray() });
            }
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
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT *
                            FROM Job 
                            WHERE JobID = @jobID; ";
                return connection.Query<Job>(sql, new { jobID }).FirstOrDefault();
            }
        }

        /// <summary>
        ///  Get Jobs object by a group of jobIDs.
        /// </summary>
        /// <param name="jobIDs">group of jobIDs</param>
        /// <returns>List of Jobs</returns>
        public IList<Job> GetJobs(IEnumerable<int> jobIDs)
        {
            var jobList = new List<Job>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT *
                            FROM Job 
                            WHERE JobID IN @ids; ";
                jobList = connection.Query<Job>(sql, new { ids = jobIDs.ToArray() }).ToList();
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
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT *
                            FROM JobView 
                            WHERE JobID = @jobID; ";
                return connection.Query<JobView>(sql, new { jobID }).FirstOrDefault();
            }
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
            var jobIds = new List<int>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT j.JobID 
                            FROM Job j
                            WHERE (j.ProcessID = @processID OR j.ProcessID IS NULL)
                            AND j.Command = @command; ";
                jobIds = connection.Query<int>(sql, new { processID, command }).ToList();
            }

            return jobIds;
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
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var offset = (pageIndex - 1) * pageSize;
                var sqlQuery = @"SELECT COUNT(JobID) FROM JobView;
                                SELECT * 
                                FROM JobView jv 
                                ORDER BY jv.Created, jv.JobID
                                OFFSET " + offset + " ROWS FETCH NEXT " + pageSize + " ROWS ONLY;";
                using (var multiResult = connection.QueryMultiple(sqlQuery))
                {
                    totalCount = multiResult.Read<int>().Single();
                    result = multiResult.Read<JobView>().ToList();
                }
            }

            //Merge the Cached progress with the data in DB
            foreach (JobView row in result)
            {
                if (row.Status == JobStatus.Running)
                {
                    var cached = GetCachedProgress(row.JobID);
                    if (cached != null)
                    {
                        row.Status = cached.Status;
                        row.Percent = cached.Percent;
                        row.Note = cached.Note;
                        row.Data = cached.Data;
                        row.Error = cached.Error;
                    }
                }

            }

            var jobViewList = new JobViewList();
            jobViewList.Total = totalCount;
            jobViewList.Items = result;

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
        public int SetToRunning(string processID, int jobID)
        {
            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = "UPDATE [Job] SET Status = @status, [Start] = @start WHERE JobID = @jobID AND ProcessID = @processID;";
                count = connection.Execute(sql, new { status = JobStatus.Running, start = DateTime.Now, jobID, processID });
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
        public int SetError(string processID, int jobID, string error)
        {
            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var sql = "UPDATE [Job] SET [Error] = @error, [Status] = @status WHERE JobID = @jobID AND ProcessID = @processID;";
                count = connection.Execute(sql, new { error, status=JobStatus.Error, jobID, processID });
            }

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
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = "UPDATE [Job] SET Status = @status, [End] = @end WHERE JobID = @jobID AND ProcessID = @processID;";
                count = connection.Execute(sql, new { status = JobStatus.Completed, end = DateTime.Now, jobID, processID });
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
                    job.ProcessID = processID; //set it similar to DB record!

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
                            ORDER BY j.Command DESC, j.Created, j.JobID
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
        public async Task<int> UpdateProgressAsync(int jobID, int? percent, string note, string data)
        {
            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"UPDATE [JobProgress] SET [Percent] = @percent, Note = @note, Data = @data 
                            WHERE JobID = @jobID; ";
                count = await connection.ExecuteAsync(sql, new { percent, note, data, jobID });
            }

            return count;
        }

        #endregion


        #region Cache
        /* Use Cache and DB to return progress */
        public JobStatusProgress GetProgress(int jobID)
        {
            var jsProgress = GetCachedProgress(jobID);
            if (jsProgress == null)
            {
                jsProgress = new JobStatusProgress();
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
            }

            return jsProgress;
        }

        public JobStatusProgress GetCachedProgress(int jobID)
        {
            if (jobCache == null)
                return null;
            return jobCache.GetCachedProgress(jobID);
        }

        //Set Cached progress similar to the DB SetProgress()
        public void SetCachedProgress(int jobID, int? percent, string note, string data)
        {
            if (jobCache == null)
                return;
            jobCache.SetCachedProgress(jobID, percent, note, data);
        }

        //Set cached progress status
        public void SetCachedProgressStatus(int jobID, JobStatus status)
        {
            if (jobCache == null)
                return;

            var jsProgress = GetProgress(jobID);
            if (jsProgress != null && jsProgress.ExistsInDB)
            {
                //Update CACHE running/stop status only if it exists in DB
                jobCache.SetCachedProgressStatus(jsProgress, status);
            }
        }

        public void SetCachedProgressStatus(IEnumerable<int> jobIDs, JobStatus status)
        {
            if (jobCache == null)
                return;

            foreach (var jobID in jobIDs)
            {
                SetCachedProgressStatus(jobID, status);
            }
        }

        //Set cached progress error
        public void SetCachedProgressError(int jobID, string error)
        {
            if (jobCache == null)
                return;

            var jsProgress = GetProgress(jobID);
            jobCache.SetCachedProgressError(jsProgress, error);
        }


        public void DeleteCachedProgress(int jobID)
        {
            if (jobCache == null)
                return;

            jobCache.DeleteCachedProgress(jobID);
        }

        public void DeleteCachedProgress(IEnumerable<int> jobIDs)
        {
            if (jobCache == null)
                return;

            foreach (var jobID in jobIDs)
            {
                DeleteCachedProgress(jobID);
            }
        }

        #endregion 
    }
}
