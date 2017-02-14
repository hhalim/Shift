using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;

using System.Data.SqlClient;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

using Newtonsoft.Json;
using Shift.Common;
using Shift.Entities;
using Dapper;

namespace Shift.DataLayer
{
    public class JobDAL
    {
        private string connectionString;
        private IJobCache jobCache;
        private string encryptionKey;

        public JobDAL(string connectionString, string encryptionKey)
        {
            this.connectionString = connectionString;
            this.jobCache = null;
            this.encryptionKey = encryptionKey;
        }

        public JobDAL(string connectionString, IJobCache jobCache, string encryptionKey)
        {
            this.connectionString = connectionString;
            this.jobCache = jobCache;
            this.encryptionKey = encryptionKey;
        }

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
                var value = GetExpressionValue(callExpression.Object);
                if (value == null)
                    throw new InvalidOperationException("Expression object can not be null.");

                type = value.GetType();
            }
            else
            {
                type = callExpression.Method.DeclaringType;
            }

            var methodInfo = callExpression.Method;
            var args = callExpression.Arguments.Select(GetExpressionValue).ToArray();

            if (type == null) throw new ArgumentNullException("type");
            if (methodInfo == null) throw new ArgumentNullException("method");
            if (args == null) throw new ArgumentNullException("args");

            Validate(type, "type", methodInfo, "method", args.Length, "args");

            var invokeMeta = new InvokeMeta(type, methodInfo);

            //Save InvokeMeta and args
            var job = new Job();
            job.AppID = appID;
            job.UserID = userID;
            job.JobType = jobType;
            job.JobName = string.IsNullOrWhiteSpace(jobName) ? type.Name + "." + methodInfo.Name : jobName;
            job.InvokeMeta = JsonConvert.SerializeObject(invokeMeta, SerializerSettings.Settings);
            job.Parameters = Helpers.Encrypt(JsonConvert.SerializeObject(SerializeArguments(args), SerializerSettings.Settings), encryptionKey); //ENCRYPT it!!!
            job.Created = DateTime.Now;

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

        //Update Job, reset some fields, return updated record count.
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
                var value = GetExpressionValue(callExpression.Object);
                if (value == null)
                    throw new InvalidOperationException("Expression object can not be null.");

                type = value.GetType();
            }
            else
            {
                type = callExpression.Method.DeclaringType;
            }

            var methodInfo = callExpression.Method;
            var args = callExpression.Arguments.Select(GetExpressionValue).ToArray();

            if (type == null) throw new ArgumentNullException("type");
            if (methodInfo == null) throw new ArgumentNullException("method");
            if (args == null) throw new ArgumentNullException("args");

            Validate(type, "type", methodInfo, "method", args.Length, "args");

            var invokeMeta = new InvokeMeta(type, methodInfo);

            //Save InvokeMeta and args
            var values = new DynamicParameters();
            values.Add("JobID", jobID);
            values.Add("AppID", appID);
            values.Add("UserID", userID);
            values.Add("JobType", jobType);
            values.Add("JobName", string.IsNullOrWhiteSpace(jobName) ? type.Name + "." + methodInfo.Name : jobName);
            values.Add("InvokeMeta", JsonConvert.SerializeObject(invokeMeta, SerializerSettings.Settings));
            values.Add("Parameters", Helpers.Encrypt(JsonConvert.SerializeObject(SerializeArguments(args), SerializerSettings.Settings), encryptionKey)); //ENCRYPT it!!!
            values.Add("Created", DateTime.Now);
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

        /// <summary>
        /// Set the Command field to stop, only works for running and no status jobs.
        /// </summary>
        /// <remarks>
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
        /// Set the Command field to stop, works for ANY job status.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public int SetCommandStopDelete(IList<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"UPDATE [Job] 
                            SET 
                            Command = @command 
                            WHERE JobID IN @ids ;
                            ";
                return connection.Execute(sql, new { command = JobCommand.StopDelete, ids = jobIDs.ToArray() });
            }
        }

        /// <summary>
        /// Set the Command field to run-now, only works for non-running and no status jobs.
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
                            AND Status IS NULL;
                            ";
                return connection.Execute(sql, new { command = JobCommand.RunNow, ids = jobIDs.ToArray() });
            }
        }

        /*
        * Reset Job data for Non-running jobs
        */
        public int Reset(IList<int> jobIDs)
        {
            if (jobIDs.Count == 0)
                return 0;

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

        /*
        * Delete Jobs and all children for Non-running jobs.
        */
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


        /*
        * Delete past jobs with specified status(es) and all jobs' children. 
        * Null job status is also valid.
        */
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

        public async Task<int> DeleteAsync(int hour, IList<JobStatus?> statusList)
        {
            var count = await Task.Run(() => Delete(hour, statusList));
            return count;
        }

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

        public IList<Job> GetJobsByCommand(string processID, string command)
        {
            var jobList = new List<Job>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT * 
                            FROM Job j
                            WHERE (j.ProcessID = @processID OR j.ProcessID IS NULL)
                            AND j.Command = @command; ";
                jobList = connection.Query<Job>(sql, new { processID, command }).ToList();
            }

            return jobList;
        }

        /* 
        Get Job Status Count based on appID and/or userID 
        Can not get just by userID, because there might be similar userID for different apps (appID).
        */
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

        #region ManageJobs DAC
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

        public IList<Job> GetJobsByStatus(IEnumerable<int> jobIDs, string statusSql)
        {
            var jobList = new List<Job>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT * 
                            FROM Job j
                            WHERE j.JobID IN @ids 
                            AND (" + statusSql + "); ";
                jobList = connection.Query<Job>(sql, new { ids = jobIDs.ToArray() }).ToList();

            }
            return jobList;
        }

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
                jobList = connection.Query<Job>(sql, new { processID, ids = jobIDs.ToArray()}).ToList();
            }

            return jobList;
        }

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

        public IList<Job> ClaimJobsToRun(string processID, int rowsToGet)
        {
            var jobList = GetJobsToRun(rowsToGet);
            return ClaimJobsToRun(processID, jobList);
        }

        //Use Optimistic Concurrency, don't claim if it's already running
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
                                AND [jobID] = @jobID; ";
                    var count = connection.Execute(sql, new { processID, job.JobID });

                    if (count > 0) //successful update
                        claimedJobs.Add(job);
                }
            }

            return claimedJobs; //it's possible to return less than passed jobIDs, since multiple Shift server might run and already claimed the job(s)
        }

        //Get an X amount of jobs ready to run, don't get it if it's already claimed by other processes.
        private IList<Job> GetJobsToRun(int rowsToGet)
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
                            OFFSET 0 ROWS FETCH NEXT @rowsToGet ROWS ONLY; ";
                jobList = connection.Query<Job>(sql, new { runNow = JobCommand.RunNow, rowsToGet }).ToList();

            }

            return jobList;
        }

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

        /* 
         * Update progress without checking if it exist or not first, no insert, use SetProgress instead 
         * Much faster than 2 calls on the DB
        */
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

        #endregion

        #region Helpers
        private static void Validate(
             Type type,
             string typeParameterName,
             MethodInfo method,
             string methodParameterName,
             int argumentCount,
             string argumentParameterName
            )
        {
            if (!method.IsPublic)
            {
                throw new NotSupportedException("Only public methods can be invoked.");
            }

            if (method.ContainsGenericParameters)
            {
                throw new NotSupportedException("Job method can not contain unassigned generic type parameters.");
            }

            if (method.DeclaringType == null)
            {
                throw new NotSupportedException("Global methods are not supported. Use class methods instead.");
            }

            if (!method.DeclaringType.IsAssignableFrom(type))
            {
                throw new ArgumentException(
                    String.Format("The type `{0}` must be derived from the `{1}` type.", method.DeclaringType, type),
                    typeParameterName);
            }

            if (typeof(Task).IsAssignableFrom(method.ReturnType))
            {
                throw new NotSupportedException("Async methods (Task) are not supported . Please make them synchronous.");
            }

            var parameters = method.GetParameters();

            if (parameters.Length != argumentCount)
            {
                throw new ArgumentException("Argument count must be equal to method parameter count.", argumentParameterName);
            }

            foreach (var parameter in parameters)
            {
                // There is no guarantee that specified method will be invoked
                // in the same process. Therefore, output parameters and parameters
                // passed by reference are not supported.

                if (parameter.IsOut)
                {
                    throw new NotSupportedException("Output parameters are not supported: there is no guarantee that specified method will be invoked inside the same process.");
                }

                if (parameter.ParameterType.IsByRef)
                {
                    throw new NotSupportedException("Parameters, passed by reference, are not supported: there is no guarantee that specified method will be invoked inside the same process.");
                }
            }
        }

        private static object GetExpressionValue(Expression expression)
        {
            var constantExpression = expression as ConstantExpression;

            return constantExpression != null
                ? constantExpression.Value
                : CachedExpressionCompiler.Evaluate(expression);
        }

        internal static string[] SerializeArguments(IReadOnlyCollection<object> arguments)
        {
            var serializedArguments = new List<string>(arguments.Count);
            foreach (var argument in arguments)
            {
                string value = null;

                if (argument != null)
                {
                    if (argument is DateTime)
                    {
                        value = ((DateTime)argument).ToString("o", CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        value = JsonConvert.SerializeObject(argument, SerializerSettings.Settings);
                    }
                }

                serializedArguments.Add(value);
            }

            return serializedArguments.ToArray();
        }

        public static object[] DeserializeArguments(IProgress<ProgressInfo> progress, MethodInfo methodInfo, string rawArguments)
        {
            var arguments = JsonConvert.DeserializeObject<string[]>(rawArguments, SerializerSettings.Settings);
            if (arguments.Length == 0)
                return null;

            var parameters = methodInfo.GetParameters();
            var result = new List<object>(arguments.Length);

            for (var i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i];
                var argument = arguments[i];

                var value = parameter.ParameterType.FullName.Contains("System.IProgress") ? progress : DeserializeArgument(argument, parameter.ParameterType);

                result.Add(value);
            }

            return result.ToArray();
        }

        public static object DeserializeArgument(string argument, Type type)
        {
            object value;
            try
            {
                value = argument != null
                    ? JsonConvert.DeserializeObject(argument, type, SerializerSettings.Settings)
                    : null;
            }
            catch (Exception jsonException)
            {
                if (type == typeof(object))
                {
                    // Special case for handling object types, because string can not be converted to object type.
                    value = argument;
                }
                else
                {
                    try
                    {
                        var converter = TypeDescriptor.GetConverter(type);
                        value = converter.ConvertFromInvariantString(argument);
                    }
                    catch (Exception)
                    {
                        throw jsonException;
                    }
                }
            }
            return value;
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
