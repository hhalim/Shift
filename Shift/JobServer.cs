//
// This software is distributed WITHOUT ANY WARRANTY and also without the implied warranty of merchantability, capability, or fitness for any particular purposes.
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Reflection;

using Newtonsoft.Json;
using Shift.DataLayer;
using Shift.Entities;

using Autofac;
using Autofac.Features.ResolveAnything;

namespace Shift
{
    public class JobServer
    {
        private IJobDAL jobDAL = null;
        private ServerConfig config = null;
        private readonly ContainerBuilder builder;
        private readonly IContainer container;
        private static System.Timers.Timer timer = null;
        private static System.Timers.Timer timer2 = null;

        private Dictionary<string, Thread> threadList = null; //reference to running thread

        private Dictionary<string, TaskInfo> taskList = null; //reference to Tasks

        ///<summary>
        ///Initializes a new instance of JobServer class, injects data layer with connection and configuration strings.
        ///</summary>
        ///<param name="config">Setup the database connection string, cache configuration, etc.</param>
        ///
        public JobServer(ServerConfig config)
        {
            if (config == null)
            {
                throw new Exception("Unable to create with no configuration.");
            }

            if (string.IsNullOrWhiteSpace(config.StorageMode))
            {
                throw new Exception("The storage mode must not be empty.");

            }

            if (string.IsNullOrWhiteSpace(config.ProcessID))
            {
                throw new Exception("Unable to create with no ProcessID.");
            }

            if (string.IsNullOrWhiteSpace(config.DBConnectionString))
            {
                throw new Exception("Unable to run without DB storage connection string.");
            }

            if (config.UseCache && string.IsNullOrWhiteSpace(config.CacheConfigurationString))
            {
                throw new Exception("Unable to run without Cache configuration string.");
            }

            if (config.MaxRunnableJobs <= 0)
            {
                config.MaxRunnableJobs = 100;
            }

            this.config = config;

            builder = new ContainerBuilder();
            builder.RegisterSource(new AnyConcreteTypeNotAlreadyRegisteredSource());
            RegisterAssembly.RegisterTypes(builder, config.StorageMode, config.DBConnectionString, config.UseCache, config.CacheConfigurationString, config.EncryptionKey);
            container = builder.Build();

            Initialize();
        }

        #region Startup
        /// <summary>
        /// Instantiate the data layer and loads all the referenced assemblies defined in the assembly list text file 
        /// in Options.AssemblyListPath and Options.AssemblyBaseDir
        /// </summary>
        private void Initialize()
        {
            if (config.ThreadMode.ToLower() == ThreadMode.Thread)
            {
                this.threadList = new Dictionary<string, Thread>();
            }
            else
            {
                this.taskList = new Dictionary<string, TaskInfo>();
            }

            jobDAL = container.Resolve<IJobDAL>();

            //OPTIONAL: Load all EXTERNAL DLLs needed by this process
            AppDomain.CurrentDomain.AssemblyResolve += AssemblyHelpers.OnAssemblyResolve;
            AssemblyHelpers.LoadAssemblies(config.AssemblyFolder, config.AssemblyListPath);
        }

        #endregion

        #region Server Run and Manage jobs
        //The region that primarily manage and run/stop/cleanup jobs that were added in the DB table by the clients

        /// <summary>
        /// Run jobs server in a scheduled timer interval.
        /// </summary>
        public void RunServer()
        {
            RunServerAsync(true).GetAwaiter().GetResult();
        }

        public Task RunServerAsync()
        {
            return RunServerAsync(false);
        }

        private async Task RunServerAsync(bool isSync)
        {
            // If some jobs are marked as running in DB, but not actually running in threads/processes, 
            // then the RunJobs won't run when running jobs DB count is >= MaxRunnableJob
            // So, always do a CleanUp first to flag as error for jobs marked in DB as running but no associated running threads.
            if (isSync)
                CleanUp();
            else
                await CleanUpAsync();

            if (timer == null && timer2 == null)
            {
                timer = new System.Timers.Timer();
                timer.Interval = config.ServerTimerInterval;
                timer.Elapsed += async (sender, e) =>
                {
                    if (isSync)
                    {
                        StopJobs();
                        RunJobs();
                    }
                    else
                    {
                        await StopJobsAsync();
                        await RunJobsAsync();
                    }
                };

                timer2 = new System.Timers.Timer();
                timer2.Interval = config.ServerTimerInterval2;
                timer2.Elapsed += async (sender, e) =>
                {
                    if (isSync)
                        CleanUp();
                    else
                        await CleanUpAsync();
                };
            }

            if (config.PollingOnce)
            {
                timer.AutoReset = false;
                timer2.AutoReset = false;
            }
            else
            {
                timer.AutoReset = true;
                timer2.AutoReset = true;
            }

            timer.Start();
            timer2.Start();
        }

        /// <summary>
        /// Stop running jobs server.
        /// </summary>
        public void StopServer()
        {
            //Stop timers
            if (timer != null && timer2 != null)
            {
                timer.Close();
                timer2.Close();
            }

            //Stop all running Jobs
            var runningJobsList = jobDAL.GetJobsByProcessAndStatus(config.ProcessID, JobStatus.Running);
            if(runningJobsList.Count() > 0)
            {
                jobDAL.SetCommandStop(runningJobsList.Select(x => x.JobID).ToList());
            }

            //Stop jobs marked with 'stop' command
            StopJobs();
        }

        public async Task StopServerAsync()
        {
            //Stop timers
            if (timer != null && timer2 != null)
            {
                timer.Close();
                timer2.Close();
            }

            //Stop all running Jobs
            var runningJobsList = await jobDAL.GetJobsByProcessAndStatusAsync(config.ProcessID, JobStatus.Running);
            if (runningJobsList.Count() > 0)
            {
                await jobDAL.SetCommandStopAsync(runningJobsList.Select(x => x.JobID).ToList());
            }

            //Stop jobs marked with 'stop' command
            await StopJobsAsync();
        }

        /// <summary>
        /// Pick up jobs from storage and run them.
        /// </summary>
        public void RunJobs()
        {
            RunJobsAsync(true).GetAwaiter().GetResult();
        }

        public Task RunJobsAsync()
        {
            return RunJobsAsync(false);
        }

        private async Task RunJobsAsync(bool isSync)
        {
            //Check max jobs count
            var runningCount = isSync ? jobDAL.CountRunningJobs(config.ProcessID) : await jobDAL.CountRunningJobsAsync(config.ProcessID);
            if (runningCount >= config.MaxRunnableJobs)
            {
                return;
            }

            var rowsToGet = config.MaxRunnableJobs - runningCount;
            var claimedJobs = isSync ? jobDAL.ClaimJobsToRun(config.ProcessID, rowsToGet) : await jobDAL.ClaimJobsToRunAsync(config.ProcessID, rowsToGet);

            RunClaimedJobsAsync(claimedJobs, isSync);
        }

        /// <summary>
        /// Pick up specific jobs from storage and run them.
        /// </summary>
        /// 
        ///<param name="jobIDs">List of job IDs to run.</param>
        public void RunJobs(IEnumerable<string> jobIDs)
        {
            RunJobsAsync(jobIDs, true).GetAwaiter().GetResult();
        }

        public Task RunJobsAsync(IEnumerable<string> jobIDs)
        {
            return RunJobsAsync(jobIDs, false);
        }

        private async Task RunJobsAsync(IEnumerable<string> jobIDs, bool isSync)
        {
            //Try to start the selected jobs, ignoring MaxRunableJobs
            var jobList = isSync ? jobDAL.GetNonRunningJobsByIDs(jobIDs) : await jobDAL.GetNonRunningJobsByIDsAsync(jobIDs);
            var claimedJobs = isSync ? jobDAL.ClaimJobsToRun(config.ProcessID, jobList.ToList()) : await jobDAL.ClaimJobsToRunAsync(config.ProcessID, jobList.ToList());

            RunClaimedJobsAsync(claimedJobs, isSync);
        }

        //Finally Run the Jobs
        private void RunClaimedJobsAsync(IEnumerable<Job> jobList, bool isSync)
        {
            if (jobList.Count() == 0)
                return;

            foreach (var job in jobList)
            {
                try
                {
                    var decryptedParameters = Entities.Helpers.Decrypt(job.Parameters, config.EncryptionKey);

                    CreateTaskOrThread(config.ThreadMode, job.ProcessID, job.JobID, job.InvokeMeta, decryptedParameters, isSync); //Use the DecryptedParameters, NOT encrypted Parameters
                }
                catch (Exception exc)
                {
                    //just mark as Invoke error, don't stop
                    var error = job.Error + " Invoke error: " + exc.ToString();
                    jobDAL.SetCachedProgressErrorAsync(job.JobID, error);
                    var processID = string.IsNullOrWhiteSpace(job.ProcessID) ? config.ProcessID : job.ProcessID;
                    if(isSync)
                        jobDAL.SetError(processID, job.JobID, error);
                    else
                        jobDAL.SetErrorAsync(processID, job.JobID, error);
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
        private void CreateTaskOrThread(string threadMode, string processID, string jobID, string invokeMeta, string parameters, bool isSync)
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

            if (threadMode.ToLower() == ThreadMode.Thread)
            {
                if (threadList.ContainsKey(jobID))
                {
                    if(threadList[jobID] != null 
                        && (threadList[jobID].ThreadState == ThreadState.Running || threadList[jobID].ThreadState == ThreadState.Unstarted)) //already running or not started?
                        return;
                }

                Thread thread;
                if (isSync)
                    thread = new Thread(() => ExecuteJobAsync(processID, jobID, methodInfo, parameters, instance, null, true).GetAwaiter().GetResult()); //Create the new Job thread
                else
                    thread = new Thread(async () => await ExecuteJobAsync(processID, jobID, methodInfo, parameters, instance, null, false)); //Create the new Job thread

                threadList[jobID] = thread; //Keep track of running thread
                thread.Name = "Shift Thread " + thread.ManagedThreadId;
                thread.IsBackground = true; //keep the main process running https://msdn.microsoft.com/en-us/library/system.threading.thread.isbackground

                thread.Start();
            }
            else
            {
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
                if (isSync) 
                    jobTask = Task.Run(() => ExecuteJobAsync(processID, jobID, methodInfo, parameters, instance, token, isSync).GetAwaiter().GetResult(), token);
                else
                    jobTask = Task.Run(async () => await ExecuteJobAsync(processID, jobID, methodInfo, parameters, instance, token, isSync), token);
                taskInfo.JobTask = jobTask;
                taskList[jobID] = taskInfo; //Keep track of running thread
            }
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
            SynchronousProgress<ProgressInfo> progress;
            if (isSync)
            {
                progress = new SynchronousProgress<ProgressInfo>(progressInfo =>
                {
                    jobDAL.SetCachedProgressAsync(jobID, progressInfo.Percent, progressInfo.Note, progressInfo.Data).ConfigureAwait(false);

                    var diffTs = DateTime.Now - start;
                    if (diffTs >= updateTs || progressInfo.Percent >= 100)
                    {
                        //Update DB and Cache
                        jobDAL.UpdateProgressAsync(jobID, progressInfo.Percent, progressInfo.Note, progressInfo.Data).ConfigureAwait(false); //async, don't wait/don't hold
                        start = DateTime.Now;
                    }
                });
            }
            else
            {
                progress = new SynchronousProgress<ProgressInfo>(async progressInfo =>
                {
                    await jobDAL.SetCachedProgressAsync(jobID, progressInfo.Percent, progressInfo.Note, progressInfo.Data).ConfigureAwait(false);

                    var diffTs = DateTime.Now - start;
                    if (diffTs >= updateTs || progressInfo.Percent >= 100)
                    {
                        //Update DB and Cache
                        await jobDAL.UpdateProgressAsync(jobID, progressInfo.Percent, progressInfo.Note, progressInfo.Data).ConfigureAwait(false); //async, don't wait/don't hold
                        start = DateTime.Now;
                    }
                });
            }

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
                if(token == null)
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
                    return;
                    //throw exc.InnerException; //handle by CancelTaskAndWaitAsync
                }
                else
                {
                    var job = isSync ? jobDAL.GetJob(jobID) : await jobDAL.GetJobAsync(jobID);
                    var error = job.Error + " " + exc.ToString();
                    jobDAL.SetCachedProgressErrorAsync(job.JobID, error);
                    if(isSync)
                        jobDAL.SetError(processID, job.JobID, error);
                    else
                        await jobDAL.SetErrorAsync(processID, job.JobID, error);

                    return; //can't throw to another thread so quit here
                }
            }
            catch (ThreadAbortException txc)
            {
                var job = isSync ? jobDAL.GetJob(jobID) : await jobDAL.GetJobAsync(jobID);
                if (job != null && job.Command != JobCommand.Stop && job.Status != JobStatus.Stopped)
                {
                    var error = job.Error + " " + txc.ToString();
                    jobDAL.SetCachedProgressErrorAsync(job.JobID, error);
                    if (isSync)
                        jobDAL.SetError(processID, job.JobID, error);
                    else
                        await jobDAL.SetErrorAsync(processID, job.JobID, error);
                }
                return; //can't throw to another thread so quit here
            }
            catch (Exception exc)
            {
                var job = isSync ? jobDAL.GetJob(jobID) : await jobDAL.GetJobAsync(jobID);
                var error = job.Error + " " + exc.ToString();
                jobDAL.SetCachedProgressErrorAsync(job.JobID, error);
                if (isSync)
                    jobDAL.SetError(processID, job.JobID, error);
                else
                    await jobDAL.SetErrorAsync(processID, job.JobID, error);

                return; //can't throw to another thread so quit here
            }

            if (isSync)
                jobDAL.SetCompleted(processID, jobID);
            else
                await jobDAL.SetCompletedAsync(processID, jobID);
            
            jobDAL.SetCachedProgressStatusAsync(jobID, JobStatus.Completed);
            var rsTask = Task.Delay(60000).ContinueWith(_ =>
            {
                jobDAL.DeleteCachedProgressAsync(jobID);
            }); //Delay delete to allow real time GetCachedProgress not hitting DB right away.

        }

        /// <summary>
        /// Stops jobs.
        /// Only jobs marked with "STOP" command will be acted on.
        /// ThreadMode="task" will use CancellationTokenSource.Cancel()  
        /// Make sure the jobs implement CancellationToken.IsCancellationRequested check for throwing and clean up canceled job.
        /// ThreadMode="thread" will use Thread.Abort.
        /// No clean up is possible when thread is aborted.
        /// </summary>
        public void StopJobs()
        {
            var jobIDs = jobDAL.GetJobIdsByProcessAndCommand(config.ProcessID, JobCommand.Stop);

            if (config.ThreadMode.ToLower() == ThreadMode.Thread)
            {
                SetToStoppedThreadsAsync(jobIDs, true).GetAwaiter().GetResult();
            }
            else
            {
                SetToStoppedTasksAsync(jobIDs, true).GetAwaiter().GetResult();
            }
        }

        public async Task StopJobsAsync()
        {
            var jobIDs = await jobDAL.GetJobIdsByProcessAndCommandAsync(config.ProcessID, JobCommand.Stop);

            if (config.ThreadMode.ToLower() == ThreadMode.Thread)
            {
                await SetToStoppedThreadsAsync(jobIDs, false);
            }
            else
            {
                await SetToStoppedTasksAsync(jobIDs, false);
            }
        }

        private async Task SetToStoppedTasksAsync(IReadOnlyCollection<string> jobIDs, bool isSync)
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

        private async Task SetToStoppedThreadsAsync(IReadOnlyCollection<string> jobIDs, bool isSync)
        {
            //abort running threads
            if (threadList.Count > 0)
            {
                foreach (var jobID in jobIDs)
                {
                    var thread = threadList.ContainsKey(jobID) ? threadList[jobID] : null;
                    if (thread != null)
                    {
                        thread.Abort();
                        threadList.Remove(jobID);
                    }
                }
            }
            //mark status to stopped
            if(isSync)
            {
                SetToStoppedAsync(jobIDs, true).GetAwaiter().GetResult();
            }
            else
            {
                await SetToStoppedAsync(jobIDs, false);
            }
        }

        private async Task SetToStoppedAsync(IReadOnlyCollection<string> jobIDs, bool isSync)
        {
            if(isSync)
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
        public void CleanUp()
        {
            StopJobs();

            //Delete past completed jobs from storage
            if (config.AutoDeletePeriod != null)
            {
                jobDAL.Delete(config.AutoDeletePeriod.Value, config.AutoDeleteStatus);
            }

            if (config.ThreadMode.ToLower() == ThreadMode.Thread)
            {
                CleanUpThreadsAsync(true).GetAwaiter().GetResult();
            }
            else
            {
                CleanUpTasksAsync(true).GetAwaiter().GetResult();
            }
        }

        public async Task CleanUpAsync()
        {
            await StopJobsAsync();

            //Delete past completed jobs from storage
            if (config.AutoDeletePeriod != null)
            {
                await jobDAL.DeleteAsync(config.AutoDeletePeriod.Value, config.AutoDeleteStatus);
            }

            if (config.ThreadMode.ToLower() == ThreadMode.Thread)
            {
                await CleanUpThreadsAsync(false);
            }
            else
            {
                await CleanUpTasksAsync(false);
            }
        }

        private async Task CleanUpThreadsAsync(bool isSync)
        {
            // For Running jobs, mark as error if no reference in threadList.
            // DB record is marked as Status = Running but NO thread in threadList (crashed, aborted, etc) => Mark as error and add error message
            // If not in threadList, it's a rogue thread or crashed, better to mark as error and restart.
            var jobList = isSync ? jobDAL.GetJobsByProcessAndStatus(config.ProcessID, JobStatus.Running) : await jobDAL.GetJobsByProcessAndStatusAsync(config.ProcessID, JobStatus.Running);
            foreach (var job in jobList)
            {
                if (!threadList.ContainsKey(job.JobID))
                {
                    //Doesn't exist anymore? 
                    var error = "Error: No actual running job process found. Try reset and run again.";
                    jobDAL.SetCachedProgressErrorAsync(job.JobID, error);
                    var processID = string.IsNullOrWhiteSpace(job.ProcessID) ? config.ProcessID : job.ProcessID;
                    if (isSync)
                    {
                        jobDAL.SetError(processID, job.JobID, error);
                    } 
                    else
                    {
                        await jobDAL.SetErrorAsync(processID, job.JobID, error);
                    }
                }
            }

            // Remove reference from ThreadList 
            if (threadList.Count > 0)
            {
                var inDBjobIDs = new List<string>();
                jobList = isSync ? jobDAL.GetJobs(threadList.Keys.ToList()) : await jobDAL.GetJobsAsync(threadList.Keys.ToList()); 

                // If jobs doesn't even exists in storage (zombie?), remove from threadList.
                inDBjobIDs = jobList.Select(j => j.JobID).ToList();
                var threadListKeys = new List<string>(threadList.Keys); //copy keys before removal
                foreach (var jobID in threadListKeys)
                {
                    if (!inDBjobIDs.Contains(jobID))
                    {
                        var thread = threadList[jobID];
                        if (thread.IsAlive)
                            thread.Abort();
                        threadList.Remove(jobID);
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
                        && threadList.ContainsKey(job.JobID))
                    {
                        threadList.Remove(job.JobID);
                    }
                }

            }

        }

        private async Task CleanUpTasksAsync(bool isSync)
        {
            var jobList = isSync ? jobDAL.GetJobsByProcessAndStatus(config.ProcessID, JobStatus.Running) : await jobDAL.GetJobsByProcessAndStatusAsync(config.ProcessID, JobStatus.Running);
            foreach (var job in jobList)
            {
                if (!taskList.ContainsKey(job.JobID))
                {
                    //Doesn't exist anymore? 
                    var error = "Error: No actual running job process found. Try reset and run again.";
                    jobDAL.SetCachedProgressErrorAsync(job.JobID, error);
                    var processID = string.IsNullOrWhiteSpace(job.ProcessID) ? config.ProcessID : job.ProcessID;
                    if (isSync)
                    {
                        jobDAL.SetError(processID, job.JobID, error);
                    }
                    else
                    {
                        await jobDAL.SetErrorAsync(processID, job.JobID, error);
                    }
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
        #endregion
    }

}
