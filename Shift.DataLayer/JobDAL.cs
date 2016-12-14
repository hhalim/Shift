using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;

using System.Data.SqlClient;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using Annotations;

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

        public JobDAL(string connectionString, IJobCache jobCache, string encryptionKey)
        {
            this.connectionString = connectionString;
            this.jobCache = jobCache;
            this.encryptionKey = encryptionKey;
        }

        public int? Add(string appID, int? userID, string jobType, [NotNull, InstantHandle]Expression<Action> methodCall)
        {
            return Add(appID, userID, jobType, null, methodCall);
        }

        public int? Add(string appID, int? userID, string jobType, string jobName, [NotNull, InstantHandle]Expression<Action> methodCall)
        {
            if (methodCall == null)
                throw new ArgumentNullException("methodCall");

            var callExpression = methodCall.Body as MethodCallExpression;
            if (callExpression == null)
            {
                throw new ArgumentException("Expression body must be 'MethodCallExpression' type.", "methodCall");
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
            var args = GetExpressionValues(callExpression.Arguments);

            if (type == null) throw new ArgumentNullException("type");
            if (methodInfo == null) throw new ArgumentNullException("method");
            if (args == null) throw new ArgumentNullException("args");

            Validate(type, "type", methodInfo, "method", args.Length, "args");

            var invokeMeta = new InvokeMeta(type, methodInfo);

            //Save InvokeMeta and args
            var job = new Job();
            job.AppID = appID;
            job.UserID = (userID == null || userID == 0 ? null : userID);
            job.JobType = jobType;
            job.JobName = string.IsNullOrWhiteSpace(jobName) ? type.Name + "." + methodInfo.Name : jobName;
            job.InvokeMeta = JsonConvert.SerializeObject(invokeMeta, SerializerSettings.Settings);
            job.Parameters = Helpers.Encrypt(JsonConvert.SerializeObject(SerializeArguments(args), SerializerSettings.Settings), encryptionKey); //ENCRYPT it!!!
            job.Created = DateTime.Now;

            int? jobID = null;
            using (var connection = new SqlConnection(connectionString))
            {
                jobID = connection.Query<int>(@"INSERT INTO [Job] ([AppID], [UserID], [JobType], [JobName], [InvokeMeta], [Parameters], [Created]) 
                                            VALUES(@AppID, @UserID, @JobType, @JobName, @InvokeMeta, @Parameters, @Created);
                                            SELECT CAST(SCOPE_IDENTITY() as int); ", job).SingleOrDefault();
            }

            return jobID;
        }

        /// <summary>
        /// Set the Command field to stop, only works for running and no status jobs.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public int SetCommandStop(List<int> jobIDs)
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
        public int SetCommandStopDelete(List<int> jobIDs)
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

        /*
        * Reset Job data for Non-running jobs
        */
        public int Reset(List<int> jobIDs)
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
                    //Delete JobResult
                    sql = @"DELETE  
                            FROM JobResult
                            WHERE JobID IN @ids; ";
                    connection.Execute(sql, new { ids = notRunning.ToArray() });

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
        * Delete Jobs and all children for Non-running jobs
        */
        public int Delete(List<int> jobIDs)
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
                    //Delete JobResult
                    sql = @"DELETE  
                            FROM JobResult
                            WHERE JobID IN @ids; ";
                    connection.Execute(sql, new { ids = notRunning.ToArray() });

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

        public int SetToStopped(List<int> jobIDs)
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

        public List<Job> GetJobsByCommand(int processID, string command)
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

        public JobResult GetJobResult(int jobResultID)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT * 
                            FROM JobResult j
                            WHERE j.JobResultID = @jobResultID; ";
                var jobResult = connection.Query<JobResult>(sql, new { jobResultID }).FirstOrDefault();

                return jobResult;
            }
        }

        /// <summary>
        /// Get a JobResult object using a unique external ID.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public JobResult GetJobResult(string externalID)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT *
                            FROM JobResult j
                            WHERE j.ExternalID = @externalID; ";
                var jobResult = connection.Query<JobResult>(sql, new { externalID }).FirstOrDefault();

                return jobResult;
            }
        }

        /* 
        Get Job Status Count based on appID and/or userID 
        Can not get just by userID, because there might be similar userID for different sites (appID).
        */
        public List<JobStatusCount> GetJobStatusCount(string appID, int? userID)
        {
            var countList = new List<JobStatusCount>();
            using (var connection = new SqlConnection(connectionString))
            {
                var sql = "";
                if (!string.IsNullOrWhiteSpace(appID) && userID != null)
                {
                    sql = @"SELECT [Status], Count([Status]) [Count], sum(case when [Status] IS null then 1 else 0 end) NullCount
                          FROM [Job]
                          WHERE [AppID] = @appID AND [UserID] = @userID
                          GROUP BY [Status]; ";
                }
                else if (!string.IsNullOrWhiteSpace(appID) && userID == null)
                {
                    sql = @"SELECT [Status], Count([Status]) [Count], sum(case when [Status] IS null then 1 else 0 end) NullCount
                          FROM [Job]
                          WHERE [AppID] = @appID
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

        public List<Job> GetJobsByStatus(List<int> jobIDs, string statusSql)
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

        public List<Job> GetJobsByProcessAndStatus(int processID, JobStatus status)
        {
            var jobList = new List<Job>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT * 
                            FROM Job j
                            WHERE (j.ProcessID = @processID OR j.ProcessID IS NULL)
                            AND j.Status = @status; ";
                jobList = connection.Query<Job>(sql, new { processID, status }).ToList();
            }

            return jobList;
        }

        public List<Job> GetJobsByProcess(int processID, List<int> jobIDs)
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

        public int CountRunningJobs(int processID)
        {
            var runningCount = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT COUNT(j.JobID) 
                            FROM Job j
                            WHERE j.Status = @status 
                            AND j.ProcessID = @processID; ";
                runningCount = connection.Query<int>(sql, new { status = JobStatus.Running, ProcessID = processID }).FirstOrDefault();
            }

            return runningCount;
        }

        public int ClaimJobsToRun(int processID, List<int> jobIDs)
        {
            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"UPDATE [Job]
                            SET ProcessID = @processID 
                            WHERE [jobID] IN @ids; ";
                count = connection.Execute(sql, new { processID, ids = jobIDs.ToArray() });
            }

            return count;
        }

        public List<Job> GetJobsToRun(int rowsToGet)
        {
            var jobList = new List<Job>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sql = @"SELECT * 
                            FROM Job j
                            WHERE j.Status IS NULL
                            ORDER BY j.Created, j.JobID 
                            OFFSET 0 ROWS FETCH NEXT @rowsToGet ROWS ONLY; ";
                jobList = connection.Query<Job>(sql, new { rowsToGet }).ToList();

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

        public int InsertResults(List<JobResult> resultList)
        {
            var count = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                count = connection.Execute(@"INSERT INTO [JobResult] (JobID, ExternalID, Name, BinaryContent, ContentType) 
                                            VALUES(@JobID, @ExternalID, @Name, @BinaryContent, @ContentType)", resultList);
            }

            return count;
        }

        #endregion

        #region Helpers
        private static void Validate(
             Type type,
             [InvokerParameterName] string typeParameterName,
             MethodInfo method,
             [InvokerParameterName] string methodParameterName,
             int argumentCount,
             [InvokerParameterName] string argumentParameterName
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

        private static object[] GetExpressionValues(IEnumerable<Expression> expressions)
        {
            return expressions.Select(GetExpressionValue).ToArray();
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
            return jobCache.GetCachedProgress(jobID);
        }

        //Set Cached progress similar to the DB SetProgress()
        public void SetCachedProgress(int jobID, int? percent, string note, string data)
        {
            jobCache.SetCachedProgress(jobID, percent, note, data);
        }

        //Set cached progress status
        public void SetCachedProgressStatus(int jobID, JobStatus status)
        {
            var jsProgress = GetProgress(jobID);
            if (jsProgress != null && jsProgress.ExistsInDB)
            {
                //Update CACHE running/stop status only if it exists in DB
                jobCache.SetCachedProgressStatus(jsProgress, status);
            }
        }


        //Set cached progress error
        public void SetCachedProgressError(int jobID, string error)
        {
            var jsProgress = GetProgress(jobID);
            jobCache.SetCachedProgressError(jsProgress, error);
        }

        public void SetCachedProgressStatus(List<int> jobIDs, JobStatus status)
        {
            foreach(var jobID in jobIDs)
            {
                SetCachedProgressStatus(jobID, status);
            }
        }

        public void DeleteCachedProgress(int jobID)
        {
            jobCache.DeleteCachedProgress(jobID);
        }

        public void DeleteCachedProgress(List<int> jobIDs)
        {
            foreach (var jobID in jobIDs)
            {
                DeleteCachedProgress(jobID);
            }
        }

        #endregion 
    }
}
