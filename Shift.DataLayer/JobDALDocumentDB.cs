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
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;
using System.Collections.ObjectModel;
using Microsoft.Azure.Documents.Linq;

namespace Shift.DataLayer
{
    public class JobDALDocumentDB : IJobDAL
    {
        private const string CollectionID = "Jobs";
        private const string DatabaseID = "ShiftDB";
        private static DocumentClient client = null; //Best practice to use singleton per application server
        private System.Uri collectionLink;

        private string connectionString;
        private string encryptionKey;

        #region Constructor
        public JobDALDocumentDB(string connectionString, string encryptionKey, string authKey)
        {
            this.encryptionKey = encryptionKey;

            InitDocumentDB(connectionString, authKey);
        }

        public JobDALDocumentDB(string connectionString, IJobCache jobCache, string encryptionKey, string authKey)
        {
            this.encryptionKey = encryptionKey;

            InitDocumentDB(connectionString, authKey);
        }

        protected void InitDocumentDB(string connectionString, string authKey)
        {
            if(client == null)
                client = new DocumentClient(new Uri(connectionString), authKey, new ConnectionPolicy { EnableEndpointDiscovery = false });
            collectionLink = UriFactory.CreateDocumentCollectionUri(DatabaseID, CollectionID);
            CreateDatabaseIfNotExistsAsync(client).Wait();
            CreateCollectionIfNotExistsAsync(client).Wait();
        }

        //Create DB if not exists
        private static async Task CreateDatabaseIfNotExistsAsync(DocumentClient client)
        {
            try
            {
                await client.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseID));
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    await client.CreateDatabaseAsync(new Database { Id = DatabaseID });
                }
                else
                {
                    throw;
                }
            }
        }

        private static async Task CreateCollectionIfNotExistsAsync(DocumentClient client)
        {
            try
            {
                await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseID, CollectionID));
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    //collection.Indexes.CreateOne(Builders<Job>.IndexKeys.Ascending(j => j.Score));
                    //collection.Indexes.CreateOne(Builders<Job>.IndexKeys.Ascending(j => j.Created));
                    //collection.Indexes.CreateOne(Builders<Job>.IndexKeys.Ascending(j => j.Status));
                    //collection.Indexes.CreateOne(Builders<Job>.IndexKeys.Ascending(j => j.ProcessID));

                    DocumentCollection collection = new DocumentCollection { Id = CollectionID };
                    collection.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });
                    //collection.IndexingPolicy.IncludedPaths.Add(
                    //    new IncludedPath
                    //    {
                    //        Path = "/Created/?",
                    //        Indexes = new Collection<Index> { new RangeIndex(DataType.String) { Precision = -1 } }
                    //    });
                    collection.IndexingPolicy.IncludedPaths.Add(
                        new IncludedPath
                        {
                            Path = "/Status/?",
                            Indexes = new Collection<Index> { new RangeIndex(DataType.Number) { Precision = -1 } }
                        });
                    //collection.IndexingPolicy.IncludedPaths.Add(
                    //    new IncludedPath
                    //    {
                    //        Path = "/ProcessID/?",
                    //        Indexes = new Collection<Index> { new RangeIndex(DataType.String) { Precision = -1 } }
                    //    });

                    await client.CreateDocumentCollectionAsync(
                        UriFactory.CreateDatabaseUri(DatabaseID),
                        collection,
                        new RequestOptions { OfferThroughput = 1000 });
                }
                else
                {
                    throw;
                }
            }
        }
        #endregion

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
            var job = new Job();
            job.AppID = appID;
            job.UserID = userID;
            job.JobType = jobType;
            job.JobName = string.IsNullOrWhiteSpace(jobName) ? type.Name + "." + methodInfo.Name : jobName;
            job.InvokeMeta = JsonConvert.SerializeObject(invokeMeta, SerializerSettings.Settings);
            job.Parameters = Helpers.Encrypt(JsonConvert.SerializeObject(DALHelpers.SerializeArguments(args), SerializerSettings.Settings), encryptionKey); //ENCRYPT it!!!
            job.Created = now;
            job.Score = (new DateTimeOffset(now)).ToUnixTimeSeconds();

            if (isSync)
                client.CreateDocumentAsync(collectionLink, job).GetAwaiter().GetResult();
            else
                await client.CreateDocumentAsync(collectionLink, job);

            return job.JobID;
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
            ResourceResponse<Document> response;
            if (isSync)
                response = client.ReplaceDocumentAsync(collectionLink, job).GetAwaiter().GetResult();
            else
                response = await client.ReplaceDocumentAsync(collectionLink, job);
            if (response.StatusCode == System.Net.HttpStatusCode.OK)
                count = 1;

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
            var count = 0;

            if (jobIDs.Count == 0)
                return count;

            //var filter = blFilter.In(j => j.JobID, jobIDs) & (blFilter.Eq(j => j.Status, null) | blFilter.Eq(j => j.Status, JobStatus.Running));
            //var update = Builders<Job>.Update.Set("Command", JobCommand.Stop);

            var jobList = client.CreateDocumentQuery<Job>(collectionLink, new FeedOptions { MaxItemCount = -1 })
                    .Where(j => jobIDs.Contains(j.JobID) && (j.Status == null || j.Status == JobStatus.Running))
                    .AsList();
            foreach(var job in jobList)
            {
                job.Command = JobCommand.Stop;
                ResourceResponse<Document> response;
                if (isSync)
                    response = client.ReplaceDocumentAsync(collectionLink, job).GetAwaiter().GetResult();
                else
                    response = await client.ReplaceDocumentAsync(collectionLink, job);
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                    count++;
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
            var count = 0;

            if (jobIDs.Count == 0)
                return count;

            //var filter = blFilter.In(j => j.JobID, jobIDs) & blFilter.Eq(j => j.Status, null) & blFilter.Eq(j => j.ProcessID, null);
            //listUpdate.Add(blUpdate.Set("Command", JobCommand.RunNow));
            //listUpdate.Add(blUpdate.Set<long>("Score", 0));

            var jobList = client.CreateDocumentQuery<Job>(collectionLink, new FeedOptions { MaxItemCount = -1 })
                    .Where(j => jobIDs.Contains(j.JobID) && j.Status == null && j.ProcessID == null)
                    .AsList();
            foreach (var job in jobList)
            {
                job.Command = JobCommand.RunNow;
                job.Score = 0;
                ResourceResponse<Document> response;
                if (isSync)
                    response = client.ReplaceDocumentAsync(collectionLink, job).GetAwaiter().GetResult();
                else
                    response = await client.ReplaceDocumentAsync(collectionLink, job);
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                    count++;
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
            var count = 0;
            if (jobIDs.Count == 0)
                return count;

            //        var filter = blFilter.Eq(j => j.JobID, jobID) & (blFilter.Eq(j => j.Status, null) | !blFilter.Eq(j => j.Status, JobStatus.Running));

            var query = (from j in client.CreateDocumentQuery<JobView>(collectionLink)
                         where jobIDs.Contains(j.JobID) && (j.Status == null || j.Status != JobStatus.Running)
                         select j).AsDocumentQuery();

            while (query.HasMoreResults)
            {
                FeedResponse<JobView> rsp;
                if (isSync)
                {
                    rsp = query.ExecuteNextAsync<JobView>().GetAwaiter().GetResult();
                }
                else
                {
                    rsp = await query.ExecuteNextAsync<JobView>();
                }

                foreach(var job in rsp.AsList())
                {
                    job.Data = null;
                    job.Percent = null;
                    job.Note = null;

                    job.ProcessID = "";
                    job.Command = "";
                    job.Status = null;
                    job.Error = "";
                    job.Start = null;
                    job.End = null;
                    job.Score = ((DateTimeOffset)job.Created).ToUnixTimeSeconds(); //reset score to created;

                    ResourceResponse<Document> response;
                    if (isSync)
                        response = client.ReplaceDocumentAsync(collectionLink, job).GetAwaiter().GetResult();
                    else
                        response = await client.ReplaceDocumentAsync(collectionLink, job);
                    if (response.StatusCode == System.Net.HttpStatusCode.OK)
                        count++;
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
            var count = 0;

            if (jobIDs.Count == 0)
                return count;

            //var filter = blFilter.In(j => j.JobID, jobIDs) & (blFilter.Eq(j => j.Status, null) | !blFilter.Eq(j => j.Status, JobStatus.Running)); //Only NOT Running jobs

            var query = (from j in client.CreateDocumentQuery<Job>(collectionLink)
                         where jobIDs.Contains(j.JobID) && (j.Status == null || j.Status != JobStatus.Running)
                         select j.JobID).AsDocumentQuery();
            while (query.HasMoreResults)
            {
                FeedResponse<string> rsp;
                if (isSync)
                {
                    rsp = query.ExecuteNextAsync<string>().GetAwaiter().GetResult();
                }
                else
                {
                    rsp = await query.ExecuteNextAsync<string>();
                }

                foreach (var jobID in rsp.AsList())
                {
                    var singleLink = UriFactory.CreateDocumentUri(DatabaseID, CollectionID, jobID);
                    ResourceResponse<Document> response;
                    if (isSync)
                        response = client.DeleteDocumentAsync(singleLink).GetAwaiter().GetResult();
                    else
                        response = await client.DeleteDocumentAsync(singleLink);
                    if (response.StatusCode == System.Net.HttpStatusCode.OK)
                        count++;
                }
            }

            return count;
        }

        /// <summary>
        /// Delete past jobs with specified status(es). 
        /// </summary>
        /// <param name="hour">Job create hour in the past</param>
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
            var count = 0;

            var predicate = PredicateBuilder.False<Job>();

            var pastDate = DateTime.Now.AddHours(-hours);
            var query = (from j in client.CreateDocumentQuery<Job>(collectionLink)
                         where (j.Created != null && j.Created < pastDate)
                         select j);

            if (statusList != null)
            {
                foreach (var status in statusList)
                {
                    if (status == null)
                    {
                        predicate = predicate.Or(j => j.Status == null);
                    }
                    else
                    {
                        predicate = predicate.Or(j => j.Status == status);
                    }
                }
            }

            var docQuery = query.Where(predicate).Select(j => j.JobID).AsDocumentQuery();
            while (docQuery.HasMoreResults)
            {
                FeedResponse<string> rsp;
                if (isSync)
                {
                    rsp = docQuery.ExecuteNextAsync<string>().GetAwaiter().GetResult();
                }
                else
                {
                    rsp = await docQuery.ExecuteNextAsync<string>();
                }

                foreach (var jobID in rsp.AsList())
                {
                    var singleLink = UriFactory.CreateDocumentUri(DatabaseID, CollectionID, jobID);
                    ResourceResponse<Document> response;
                    if (isSync)
                        response = client.DeleteDocumentAsync(singleLink).GetAwaiter().GetResult();
                    else
                        response = await client.DeleteDocumentAsync(singleLink);
                    if (response.StatusCode == System.Net.HttpStatusCode.OK)
                        count++;
                }
            }

            return count;
        }

        /// <summary>
        ///  Mark job status to JobStatus.Stopped. 
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
            var count = 0;

            if (jobIDs.Count == 0)
                return count;

            var jobList = client.CreateDocumentQuery<Job>(collectionLink, new FeedOptions { MaxItemCount = -1 })
                    .Where(j => jobIDs.Contains(j.JobID) && j.Status == null && j.ProcessID == null)
                    .AsList();
            foreach (var job in jobList)
            {
                job.Command = null;
                job.Status = JobStatus.Stopped;

                ResourceResponse<Document> response;
                if (isSync)
                    response = client.ReplaceDocumentAsync(collectionLink, job).GetAwaiter().GetResult();
                else
                    response = await client.ReplaceDocumentAsync(collectionLink, job);
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
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
            return GetJobStatusCountAsync(appID, userID).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public async Task<IReadOnlyCollection<JobStatusCount>> GetJobStatusCountAsync(string appID, string userID)
        {
            var groupStatus = new Dictionary<string, JobStatusCount>();


            return groupStatus.Values.ToList();
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
            var query = (from j in client.CreateDocumentQuery<Job>(collectionLink)
                            where j.JobID == jobID
                            select j).AsDocumentQuery();

            Job job = null;
            while (query.HasMoreResults)
            {
                FeedResponse<Job> rsp;
                if (isSync)
                {
                    rsp = query.ExecuteNextAsync<Job>().GetAwaiter().GetResult();
                }
                else
                {
                    rsp = await query.ExecuteNextAsync<Job>();
                }

                if (rsp.Count != 0)
                {
                    job = rsp.SingleOrDefault();
                    break;
                }
            }

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
            var query = (from j in client.CreateDocumentQuery<Job>(collectionLink)
                         where jobIDs.Contains(j.JobID)
                         select j).AsDocumentQuery();

            var jobList = new List<Job>();

            while (query.HasMoreResults)
            {
                if (isSync)
                {
                    jobList.AddRange(query.ExecuteNextAsync<Job>().GetAwaiter().GetResult());
                }
                else
                {
                    jobList.AddRange(await query.ExecuteNextAsync<Job>());
                }

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
            var query = (from j in client.CreateDocumentQuery<JobView>(collectionLink)
                         where j.JobID == jobID
                         select j).AsDocumentQuery();

            JobView jobView = null;
            while (query.HasMoreResults)
            {
                FeedResponse<JobView> rsp;
                if (isSync)
                {
                    rsp = query.ExecuteNextAsync<JobView>().GetAwaiter().GetResult();
                }
                else
                {
                    rsp = await query.ExecuteNextAsync<JobView>();
                }

                if (rsp.Count != 0)
                {
                    jobView = rsp.SingleOrDefault();
                    break;
                }
            }

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
            var query = (from j in client.CreateDocumentQuery<Job>(collectionLink)
                         where jobIDs.Contains(j.JobID) && j.Status == null
                         select j).AsDocumentQuery();

            var jobList = new List<Job>();

            while (query.HasMoreResults)
            {
                if (isSync)
                {
                    jobList.AddRange(query.ExecuteNextAsync<Job>().GetAwaiter().GetResult());
                }
                else
                {
                    jobList.AddRange(await query.ExecuteNextAsync<Job>());
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

        private async Task<IReadOnlyCollection<string>> GetJobIdsByProcessAndCommandAsync(string processID, string command, bool isSync)
        {

            //var query = collection.Find(j => (j.ProcessID == processID || j.ProcessID == null) && j.Command == command)

            var query = (from j in client.CreateDocumentQuery<Job>(collectionLink)
                         where (j.ProcessID == processID || j.ProcessID == null) && j.Command == command
                         select j.JobID).AsDocumentQuery();

            var jobIDs = new List<string>();

            while (query.HasMoreResults)
            {
                if (isSync)
                {
                    jobIDs.AddRange(query.ExecuteNextAsync<string>().GetAwaiter().GetResult());
                }
                else
                {
                    jobIDs.AddRange(await query.ExecuteNextAsync<string>());
                }

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
                //jobList = collection.Find(j => j.ProcessID == processID && j.Status == status).ToList();

            var query = (from j in client.CreateDocumentQuery<Job>(collectionLink)
                         where j.ProcessID == processID && j.Status == status
                         select j).AsDocumentQuery();

            var jobList = new List<Job>();

            while (query.HasMoreResults)
            {
                if (isSync)
                {
                    jobList.AddRange(query.ExecuteNextAsync<Job>().GetAwaiter().GetResult());
                }
                else
                {
                    jobList.AddRange(await query.ExecuteNextAsync<Job>());
                }
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
            pageIndex = pageIndex == null || pageIndex == 0 ? 1 : pageIndex; //default to 1
            pageSize = pageSize == null || pageSize == 0 ? 10 : pageSize; //default to 10

            //var offset = (pageIndex.Value - 1) * pageSize.Value;

            //var collection = database.GetCollection<JobView>(JobCollectionName);
            //var query = collection.Find(j => true).SortBy(j => j.Created).SortBy(j => j.JobID);

            //var totalTask = isSync ? query.Count() : await query.CountAsync();
            //var itemsTask = isSync ? query.Skip(offset).Limit(pageSize).ToList() : await query.Skip(offset).Limit(pageSize).ToListAsync();

            var jobViewList = new JobViewList();

            //Total count
            var sql = @"SELECT VALUE COUNT(1) 
                        FROM Jobs j ";
            var query = client.CreateDocumentQuery<long>(collectionLink, sql).AsDocumentQuery();
            while (query.HasMoreResults)
            {
                FeedResponse<long> rsp;
                if (isSync)
                {
                    rsp = query.ExecuteNextAsync<long>().GetAwaiter().GetResult();
                }
                else
                {
                    rsp = await query.ExecuteNextAsync<long>();
                }
                if (rsp.Count != 0)
                {
                    jobViewList.Total = rsp.SingleOrDefault();
                    break;
                }
            }

            var options = new FeedOptions
            {
                MaxItemCount = pageSize
            };

            var query2 = (from j in client.CreateDocumentQuery<JobView>(collectionLink, options)
                         select j).AsDocumentQuery();

            var index = 1;
            while (query2.HasMoreResults)
            {
                FeedResponse<JobView> rsp;
                if (isSync)
                {
                    rsp = query2.ExecuteNextAsync<JobView>().GetAwaiter().GetResult();
                }
                else
                {
                    rsp = await query2.ExecuteNextAsync<JobView>();
                }

                if (index == pageIndex)
                {
                    jobViewList.Items = rsp.AsList();
                    break;
                }
            }

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
            return SetToRunningAsync(processID, jobID, true).GetAwaiter().GetResult();
        }

        public Task<int> SetToRunningAsync(string processID, string jobID)
        {
            return SetToRunningAsync(processID, jobID, false);
        }

        private async Task<int> SetToRunningAsync(string processID, string jobID, bool isSync)
        {
            var count = 0;

            var job = client.CreateDocumentQuery<Job>(collectionLink, new FeedOptions { MaxItemCount = -1 })
                        .Where(j => j.JobID == jobID && j.ProcessID == processID)
                        .SingleOrDefault();
            if (job != null)
            {
                job.Command = null;
                job.Status = JobStatus.Running;
                job.Start = DateTime.Now;

                ResourceResponse<Document> response;
                if (isSync)
                    response = client.ReplaceDocumentAsync(collectionLink, job).GetAwaiter().GetResult();
                else
                    response = await client.ReplaceDocumentAsync(collectionLink, job);
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
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

            var job = client.CreateDocumentQuery<Job>(collectionLink, new FeedOptions { MaxItemCount = -1 })
                            .Where(j => j.JobID == jobID && j.ProcessID == processID)
                            .SingleOrDefault();
            if (job != null)
            {
                job.Status = JobStatus.Error;
                job.Error= error;

                ResourceResponse<Document> response;
                if (isSync)
                    response = client.ReplaceDocumentAsync(collectionLink, job).GetAwaiter().GetResult();
                else
                    response = await client.ReplaceDocumentAsync(collectionLink, job);
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
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
            var job = client.CreateDocumentQuery<Job>(collectionLink, new FeedOptions { MaxItemCount = -1 })
                            .Where(j => j.JobID == jobID && j.ProcessID == processID)
                            .SingleOrDefault();
            if(job != null)
            { 
                job.Command = null;
                job.Status = JobStatus.Completed;
                job.End = DateTime.Now;

                ResourceResponse<Document> response;
                if (isSync)
                    response = client.ReplaceDocumentAsync(collectionLink, job).GetAwaiter().GetResult();
                else
                    response = await client.ReplaceDocumentAsync(collectionLink, job);
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
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
            var runningCount = 0;

            //var collection = database.GetCollection<Job>(JobCollectionName);
            //var builder = Builders<Job>.Filter;
            //var filter = builder.Eq(j => j.ProcessID, processID) & builder.Eq(j => j.Status, JobStatus.Running);
            //runningCount = isSync ? (int)collection.Count(filter) : (int)await collection.CountAsync(filter);
            var sql = @"SELECT VALUE COUNT(1) 
                        FROM Jobs j 
                        WHERE j.Status = " + JobStatus.Running;
            var query = client.CreateDocumentQuery<int>(collectionLink, sql).AsDocumentQuery();
            while (query.HasMoreResults)
            {
                FeedResponse<int> rsp;
                if (isSync)
                {
                    rsp = query.ExecuteNextAsync<int>().GetAwaiter().GetResult();
                }
                else
                {
                    rsp = await query.ExecuteNextAsync<int>();
                }
                if (rsp.Count != 0)
                {
                    runningCount = rsp.SingleOrDefault();
                    break;
                }
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

            foreach(var job in jobList)
            {
                var count = 0;
                try
                {
                    var collection = database.GetCollection<Job>(JobCollectionName);
                    var blFilter = Builders<Job>.Filter;
                    var filter = blFilter.Eq(j => j.JobID, job.JobID) & blFilter.Eq(j => j.Status, null) & blFilter.Eq(j=> j.ProcessID, null);
                    var update = Builders<Job>.Update.Set("ProcessID", processID);

                    var result = isSync ? collection.UpdateOne(filter, update) : await collection.UpdateOneAsync(filter, update);
                    if (result.IsAcknowledged)
                    {
                        count = (int)result.ModifiedCount;
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
                {
                    job.ProcessID = processID; //set it similar to DB record!
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
 
            var collection = database.GetCollection<Job>(JobCollectionName);
            var query  = collection
                .Find(j => j.Status == null && j.ProcessID == null && (j.Command == JobCommand.RunNow || j.Command == null))
                .SortBy(j => j.Score)
                .Limit(maxNum);

            jobList = isSync ? query.ToList() : await query.ToListAsync();

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
        /// Update progress, similar to SetProgress() method. 
        /// Higher performance than SetProgress(). This method only uses 1 call to the database storage.
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
            var collection = database.GetCollection<Job>(JobCollectionName);
            var blFilter = Builders<Job>.Filter;
            var filter = blFilter.Eq(j => j.JobID, jobID);
            var blUpdate = Builders<Job>.Update;
            var listUpdate = new List<UpdateDefinition<Job>>();
            listUpdate.Add(blUpdate.Set("Percent", percent));
            listUpdate.Add(blUpdate.Set("Note", note));
            listUpdate.Add(blUpdate.Set("Data", data));
            var update = blUpdate.Combine(listUpdate.ToArray());

            var result = isSync ? collection.UpdateOne(filter, update) : await collection.UpdateOneAsync(filter, update);

            if (result.IsAcknowledged)
                count = (int)result.ModifiedCount;

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
            //No cache, so always get direct
            var jsProgress = new JobStatusProgress();
            //try to get from DB
            var jobView = isSync ? GetJobView(jobID): await GetJobViewAsync(jobID);
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
            return GetProgress(jobID); //no cache in pure Mongo
        }

        public Task<JobStatusProgress> GetCachedProgressAsync(string jobID)
        {
            return GetProgressAsync(jobID); 
        }

        //Set Cached progress similar to the DB SetProgress()
        //Not needed in Mongo
        public async Task SetCachedProgressAsync(string jobID, int? percent, string note, string data)
        {
            return;
        }

        //Set cached progress status
        //Not needed in Mongo
        public async Task SetCachedProgressStatusAsync(string jobID, JobStatus status)
        {
            return;
        }

        //Not needed in Mongo
        public async Task SetCachedProgressStatusAsync(IEnumerable<string> jobIDs, JobStatus status)
        {
            return;
        }

        //Set cached progress error
        //Not needed in Mongo
        public async Task SetCachedProgressErrorAsync(string jobID, string error)
        {
            return;
        }

        //Not needed in Mongo
        public async Task DeleteCachedProgressAsync(string jobID)
        {
            return;
        }

        //Not needed in Mongo
        public async Task DeleteCachedProgressAsync(IEnumerable<string> jobIDs)
        {
            return;
        }


        #endregion 
    }
}
