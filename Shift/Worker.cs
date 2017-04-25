using Autofac;
using Shift.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;

using Newtonsoft.Json;
using Shift.DataLayer;

namespace Shift
{
    public class Worker
    {
        private ServerConfig config = null;
        private IJobDAL jobDAL = null;
        private Dictionary<string, TaskInfo> taskList = null; //reference to Tasks

        private int workerID;
        private string workerProcessID;

        public Worker(ServerConfig config, IContainer container, int workerID)
        {
            taskList = new Dictionary<string, TaskInfo>();
            jobDAL = container.Resolve<IJobDAL>();
            this.workerID = workerID;
            this.config = config;

            this.workerProcessID = config.ProcessID + "-" + workerID;
        }

        public async Task<int> CountRunningJobsAsync(bool isSync)
        {
            var runningCount = isSync ? jobDAL.CountRunningJobs(workerProcessID) : await jobDAL.CountRunningJobsAsync(workerProcessID);
            return runningCount;
        }

        public async Task RunJobsAsync(bool isSync)
        {
            //Check max jobs count
            var runningCount = isSync ? CountRunningJobsAsync(isSync).GetAwaiter().GetResult() :  await CountRunningJobsAsync(isSync);
            if (runningCount >= config.MaxRunnableJobs)
            {
                return;
            }

            var rowsToGet = config.MaxRunnableJobs - runningCount;
            var claimedJobs = isSync ? jobDAL.ClaimJobsToRun(workerProcessID, rowsToGet) : await jobDAL.ClaimJobsToRunAsync(workerProcessID, rowsToGet);

            RunClaimedJobsAsync(claimedJobs, isSync);
        }

        public async Task RunJobsAsync(IEnumerable<string> jobIDs, bool isSync)
        {
            //Try to start the selected jobs, ignoring MaxRunableJobs
            var jobList = isSync ? jobDAL.GetNonRunningJobsByIDs(jobIDs) : await jobDAL.GetNonRunningJobsByIDsAsync(jobIDs);
            var claimedJobs = isSync ? jobDAL.ClaimJobsToRun(workerProcessID, jobList.ToList()) : await jobDAL.ClaimJobsToRunAsync(workerProcessID, jobList.ToList());

            RunClaimedJobsAsync(claimedJobs, isSync);
        }
        
        //Finally Run the Jobs
        private async Task RunClaimedJobsAsync(IEnumerable<Job> jobList, bool isSync)
        {
            if (jobList.Count() == 0)
                return;

            foreach (var job in jobList)
            {
                try
                {
                    var decryptedParameters = Entities.Helpers.Decrypt(job.Parameters, config.EncryptionKey);

                    CreateTask(job.ProcessID, job.JobID, job.InvokeMeta, decryptedParameters, isSync); //Use the DecryptedParameters, NOT encrypted Parameters
                }
                catch (Exception exc)
                {
                    var error = job.Error + " " + exc.ToString();
                    var processID = string.IsNullOrWhiteSpace(job.ProcessID) ? workerProcessID : job.ProcessID;
                    var count = isSync ? SetErrorAsync(processID, job.JobID, error, isSync).GetAwaiter().GetResult()
                        : await SetErrorAsync(processID, job.JobID, error, isSync);
                }
            }
        }

        private static Type GetTypeFromAllAssemblies(string typeName)
        {
            //try this domain first
            var type = Type.GetType(typeName);

            if (type != null)
                return type;

            //Get all assemblies
            List<System.Reflection.Assembly> assemblies = AppDomain.CurrentDomain.GetAssemblies().ToList();

            foreach (var assembly in assemblies)
            {
                Type t = assembly.GetType(typeName, false);
                if (t != null)
                    return t;
            }
            throw new ArgumentException("Type " + typeName + " doesn't exist in the current app domain");
        }

        //Create the thread that will run the job
        private void CreateTask(string processID, string jobID, string invokeMeta, string parameters, bool isSync)
        {
            var invokeMetaObj = JsonConvert.DeserializeObject<InvokeMeta>(invokeMeta, SerializerSettings.Settings);

            var type = GetTypeFromAllAssemblies(invokeMetaObj.Type);
            var parameterTypes = JsonConvert.DeserializeObject<Type[]>(invokeMetaObj.ParameterTypes, SerializerSettings.Settings);
            var methodInfo = Helpers.GetNonOpenMatchingMethod(type, invokeMetaObj.Method, parameterTypes);
            if (methodInfo == null)
            {
                throw new InvalidOperationException(string.Format("The type '{0}' has no method with signature '{1}({2})'", type.FullName, invokeMetaObj.Method, string.Join(", ", parameterTypes.Select(x => x.Name))));
            }
            object instance = null;
            if (!methodInfo.IsStatic) //not static?
            {
                instance = Helpers.CreateInstance(type); //create object method instance
            }

            Task jobTask = null;
            if (taskList.ContainsKey(jobID))
            {
                jobTask = taskList[jobID].JobTask;
                if (jobTask != null && !jobTask.IsCompleted) //already running and NOT completed?
                    return;
            }

            //Don't use ConfigureWait(false), since some tasks don't have Cancellation token and must use the original context to return after completion
            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            var taskInfo = new TaskInfo();
            taskInfo.TokenSource = tokenSource;
            //Keep track of running thread 
            //If using Task.Run(), MUST register task in TaskList right away if NOT the CleanUp() method may mark running Task as Error, because it's running but not in list!!!
            taskList[jobID] = taskInfo;
            if (isSync)
            {
                jobTask = Task.Run(() => ExecuteJobAsync(processID, jobID, methodInfo, parameters, instance, token, isSync).GetAwaiter().GetResult(), token)
                    .ContinueWith(t =>
                    {
                        DeleteCachedProgressDelayedAsync(jobID);
                    }, TaskContinuationOptions.RunContinuationsAsynchronously);
            }
            else
            {
                jobTask = Task.Run(async () => await ExecuteJobAsync(processID, jobID, methodInfo, parameters, instance, token, isSync), token)
                    .ContinueWith(t =>
                    {
                        DeleteCachedProgressDelayedAsync(jobID);
                    }, TaskContinuationOptions.RunContinuationsAsynchronously);
            }
            taskInfo.JobTask = jobTask;
            taskList[jobID] = taskInfo; //re-update with filled Task
        }

        private async Task<IProgress<ProgressInfo>> UpdateProgressEventAsync(string jobID, bool isSync)
        {
            //Insert a progress row first for the related jobID if it doesn't exist
            if (isSync)
            {
                jobDAL.SetProgress(jobID, null, null, null);
            }
            else
            {
                await jobDAL.SetProgressAsync(jobID, null, null, null);
            }
            await jobDAL.SetCachedProgressAsync(jobID, null, null, null).ConfigureAwait(false);

            var start = DateTime.Now;
            var updateTs = config.ProgressDBInterval ?? new TimeSpan(0, 0, 10); //default to 10 sec interval for updating DB

            //SynchronousProgress is event based and called regularly by the running job
            SynchronousProgress<ProgressInfo> progress = new SynchronousProgress<ProgressInfo>(progressInfo =>
            {
                jobDAL.SetCachedProgressAsync(jobID, progressInfo.Percent, progressInfo.Note, progressInfo.Data).ConfigureAwait(false); //Update Cache

                var diffTs = DateTime.Now - start;
                if (diffTs >= updateTs || progressInfo.Percent >= 100)
                {
                    jobDAL.UpdateProgressAsync(jobID, progressInfo.Percent, progressInfo.Note, progressInfo.Data).ConfigureAwait(false); //Update DB async, don't wait/don't hold
                    start = DateTime.Now;
                }
            });

            return progress;
        }

        private async Task ExecuteJobAsync(string processID, string jobID, MethodInfo methodInfo, string parameters, object instance, CancellationToken? token, bool isSync)
        {
            try
            {
                //Set job to Running
                if (isSync)
                    jobDAL.SetToRunning(processID, jobID);
                else
                    await jobDAL.SetToRunningAsync(processID, jobID);
                jobDAL.SetCachedProgressStatusAsync(jobID, JobStatus.Running);

                var progress = isSync ? UpdateProgressEventAsync(jobID, true).GetAwaiter().GetResult() : await UpdateProgressEventAsync(jobID, false); //Need this to update the progress of the job's

                //Invoke Method
                if (token == null)
                {
                    var tokenSource = new CancellationTokenSource(); //not doing anything when using thread.Start()
                    token = tokenSource.Token;
                }
                var args = DALHelpers.DeserializeArguments(token.Value, progress, methodInfo, parameters);
                methodInfo.Invoke(instance, args);
            }
            catch (TargetInvocationException exc)
            {
                if (exc.InnerException is OperationCanceledException)
                {
                    if (isSync)
                    {
                        SetToStoppedAsync(new List<string> { jobID }, isSync).GetAwaiter().GetResult();
                    }
                    else
                    {
                        await SetToStoppedAsync(new List<string> { jobID }, isSync);
                    }

                    throw exc.InnerException;
                }
                else
                {
                    var job = isSync ? jobDAL.GetJob(jobID) : await jobDAL.GetJobAsync(jobID);
                    if (job != null)
                    {
                        var error = job.Error + " " + exc.ToString();
                        var count = isSync ? SetErrorAsync(processID, job.JobID, error, isSync).GetAwaiter().GetResult()
                            : await SetErrorAsync(processID, job.JobID, error, isSync);
                    }
                    throw exc;
                }
            }
            catch (Exception exc)
            {
                var job = isSync ? jobDAL.GetJob(jobID) : await jobDAL.GetJobAsync(jobID);
                if (job != null)
                {
                    var error = job.Error + " " + exc.ToString();
                    var count = isSync ? SetErrorAsync(processID, job.JobID, error, isSync).GetAwaiter().GetResult()
                        : await SetErrorAsync(processID, job.JobID, error, isSync);
                }
                throw exc;
            }

            //Completed successfully with no error
            if (isSync)
                jobDAL.SetCompleted(processID, jobID);
            else
                await jobDAL.SetCompletedAsync(processID, jobID);
            jobDAL.SetCachedProgressStatusAsync(jobID, JobStatus.Completed);
        }

        private Task DeleteCachedProgressDelayedAsync(string jobID)
        {
            return Task.Delay(60000).ContinueWith(_ =>
            {
                jobDAL.DeleteCachedProgressAsync(jobID);
            }, TaskContinuationOptions.RunContinuationsAsynchronously);
        }

        private async Task<int> SetErrorAsync(string processID, string jobID, string error, bool isSync)
        {
            jobDAL.SetCachedProgressErrorAsync(jobID, error);
            if (isSync)
                return jobDAL.SetError(processID, jobID, error);
            else
                return await jobDAL.SetErrorAsync(processID, jobID, error);
        }

        //Called when Server is being shut down.
        //Mark all running jobs to stop.
        public async Task SetStopAllRunningJobsAsync(bool isSync)
        {
            if (isSync)
            {
                //Stop all running Jobs
                var runningJobsList = jobDAL.GetJobsByProcessAndStatus(workerProcessID, JobStatus.Running);
                if (runningJobsList.Count() > 0)
                {
                    jobDAL.SetCommandStop(runningJobsList.Select(x => x.JobID).ToList());
                }
            }
            else
            {
                //Stop all running Jobs
                var runningJobsList = await jobDAL.GetJobsByProcessAndStatusAsync(workerProcessID, JobStatus.Running);
                if (runningJobsList.Count() > 0)
                {
                    await jobDAL.SetCommandStopAsync(runningJobsList.Select(x => x.JobID).ToList());
                }
            }
        }

        /// <summary>
        /// Stops jobs.
        /// Only jobs marked with "STOP" command will be acted on.
        /// ThreadMode="task" will use CancellationTokenSource.Cancel()  
        /// Make sure the jobs implement CancellationToken.IsCancellationRequested check for throwing and clean up canceled job.
        /// ThreadMode="thread" will use Thread.Abort.
        /// No clean up is possible when thread is aborted.
        /// </summary>
        public async Task StopJobsAsync(bool isSync)
        {
            var jobIDs = isSync ? jobDAL.GetJobIdsByProcessAndCommand(workerProcessID, JobCommand.Stop) : await jobDAL.GetJobIdsByProcessAndCommandAsync(workerProcessID, JobCommand.Stop);
            if (isSync)
                StopJobsAsync(jobIDs, isSync).GetAwaiter().GetResult();
            else
                await StopJobsAsync(jobIDs, isSync);
        }

        private async Task StopJobsAsync(IReadOnlyCollection<string> jobIDs, bool isSync)
        {
            var nonWaitJobIDs = new List<string>();
            if (taskList.Count > 0)
            {
                foreach (var jobID in jobIDs)
                {
                    var taskInfo = taskList.ContainsKey(jobID) ? taskList[jobID] : null;
                    if (taskInfo != null)
                    {
                        if (!taskInfo.TokenSource.Token.IsCancellationRequested)
                        {
                            taskInfo.TokenSource.Cancel(); //attempt to cancel task
                            Task.Run(async () => await taskInfo.JobTask)
                                .ContinueWith(result => { taskList.Remove(jobID); }); //Don't hold the process, just run another task to wait for cancellable task
                        }
                    }
                    else
                    {
                        nonWaitJobIDs.Add(jobID);
                    }
                }

                //Set to stopped for nonWaitJobIDs
                if (isSync)
                {
                    SetToStoppedAsync(nonWaitJobIDs, true).GetAwaiter().GetResult();
                }
                else
                {
                    await SetToStoppedAsync(nonWaitJobIDs, false);
                }
            }
            else
            {
                if (isSync)
                {
                    SetToStoppedAsync(jobIDs, true).GetAwaiter().GetResult();
                }
                else
                {
                    await SetToStoppedAsync(jobIDs, false);
                }
            }
        }

        private async Task SetToStoppedAsync(IReadOnlyCollection<string> jobIDs, bool isSync)
        {
            if (isSync)
            {
                jobDAL.SetToStopped(jobIDs.ToList());
            }
            else
            {
                await jobDAL.SetToStoppedAsync(jobIDs.ToList());
            }
            jobDAL.SetCachedProgressStatusAsync(jobIDs, JobStatus.Stopped); //redis cached progress
            jobDAL.DeleteCachedProgressAsync(jobIDs);
        }

        /// <summary>
        /// Cleanup and synchronize running jobs and jobs table.
        /// * Job is deleted based on AutoDeletePeriod and AutoDeleteStatus settings.
        /// * Mark job as an error, when job status is "RUNNING" in DB table, but there is no actual running thread in the related server process (Zombie Jobs).
        /// * Remove thread references in memory, when job is deleted or status in DB is: stopped, error, or completed.
        /// </summary>
        public async Task CleanUpAsync(bool isSync)
        {
            if(isSync)
            {
                StopJobsAsync(isSync).GetAwaiter().GetResult();

                //Delete past completed jobs from storage
                if (config.AutoDeletePeriod != null)
                {
                    jobDAL.Delete(config.AutoDeletePeriod.Value, config.AutoDeleteStatus);
                }
            }
            else
            {
                await StopJobsAsync(isSync);

                //Delete past completed jobs from storage
                if (config.AutoDeletePeriod != null)
                {
                    await jobDAL.DeleteAsync(config.AutoDeletePeriod.Value, config.AutoDeleteStatus);
                }
            }

            //Get all running process with ProcessID
            var jobList = isSync ? jobDAL.GetJobsByProcessAndStatus(workerProcessID, JobStatus.Running) : await jobDAL.GetJobsByProcessAndStatusAsync(workerProcessID, JobStatus.Running);
            foreach (var job in jobList)
            {
                if (!taskList.ContainsKey(job.JobID))
                {
                    //Doesn't exist anymore? 
                    var error = "Error: No actual running job process found. Try reset and run again.";
                    var processID = string.IsNullOrWhiteSpace(job.ProcessID) ? workerProcessID : job.ProcessID;
                    var count = isSync ? SetErrorAsync(processID, job.JobID, error, isSync).GetAwaiter().GetResult()
                        : await SetErrorAsync(processID, job.JobID, error, isSync);
                }
            }

            if (taskList.Count > 0)
            {
                var inDBjobIDs = new List<string>();
                jobList = isSync ? jobDAL.GetJobs(taskList.Keys.ToList()) : await jobDAL.GetJobsAsync(taskList.Keys.ToList()); //get all jobs in taskList

                // If jobs doesn't even exists in storage (zombie?), remove from taskList.
                inDBjobIDs = jobList.Select(j => j.JobID).ToList();
                var taskListKeys = new List<string>(taskList.Keys); //copy keys before removal
                foreach (var jobID in taskListKeys)
                {
                    if (!inDBjobIDs.Contains(jobID))
                    {
                        TaskInfo taskInfo = null;
                        if (taskList.Keys.Contains(jobID))
                            taskInfo = taskList[jobID];
                        else
                            continue;

                        taskInfo.TokenSource.Cancel(); //attempt to cancel
                        taskList.Remove(jobID);
                    }
                }

                // For job status that is stopped, error, completed => Remove from thread list, no need to keep track of them anymore.
                var statuses = new List<int>
                {
                    (int)JobStatus.Stopped,
                    (int)JobStatus.Error,
                    (int)JobStatus.Completed
                };

                foreach (var job in jobList)
                {
                    if (job.Status != null
                        && statuses.Contains((int)job.Status)
                        && taskList.ContainsKey(job.JobID))
                    {
                        var taskInfo = taskList[job.JobID];
                        taskInfo.TokenSource.Dispose();
                        taskList.Remove(job.JobID);
                    }
                }

            }

        }

    }

}
