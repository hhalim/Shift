﻿//
// This software is distributed WITHOUT ANY WARRANTY and also without the implied warranty of merchantability, capability, or fitness for any particular purposes.
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Reflection;
using System.IO;

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

        private Dictionary<int, Thread> threadList = null; //reference to running thread

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

            Initialize(config.StorageMode);
        }

        #region Startup
        /// <summary>
        /// Instantiate the data layer and loads all the referenced assemblies defined in the assembly list text file 
        /// in Options.AssemblyListPath and Options.AssemblyBaseDir
        /// </summary>
        private void Initialize(string storageMode)
        {
            this.threadList = new Dictionary<int, Thread>();

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
            // If some jobs are marked as running in DB, but not actually running in threads/processes, 
            // then the RunJobs won't run when running jobs DB count is >= MaxRunnableJob
            // So, always do a CleanUp first to flag as error for jobs marked in DB as running but no associated running threads.
            CleanUp();

            if (timer == null && timer2 == null)
            {
                timer = new System.Timers.Timer();
                timer.Interval = config.ServerTimerInterval;
                timer.Elapsed += (sender, e) => {
                    StopJobs();
                    RunJobs();
                };

                timer2 = new System.Timers.Timer();
                timer2.Interval = config.ServerTimerInterval2;
                timer2.Elapsed += (sender, e) => {
                    CleanUp();
                };
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

        /// <summary>
        /// Pick up jobs from storage and run them.
        /// </summary>
        public void RunJobs()
        {
            //Check max jobs count
            var runningCount = jobDAL.CountRunningJobs(config.ProcessID);
            if (runningCount >= config.MaxRunnableJobs)
            {
                return;
            }

            var rowsToGet = config.MaxRunnableJobs - runningCount;
            var claimedJobs = jobDAL.ClaimJobsToRun(config.ProcessID, rowsToGet);
            RunJobs(claimedJobs);
        }

        /// <summary>
        /// Pick up specific jobs from storage and run them.
        /// </summary>
        /// 
        ///<param name="jobIDs">List of job IDs to run.</param>
        public void RunJobs(IEnumerable<int> jobIDs)
        {
            //Try to start the selected jobs, ignoring MaxRunableJobs
            var jobList = jobDAL.GetNonRunningJobsByIDs(jobIDs);
            var claimedJobs = jobDAL.ClaimJobsToRun(config.ProcessID, jobList);

            RunJobs(claimedJobs);
        }

        //Finally Run the Jobs
        protected void RunJobs(IEnumerable<Job> jobList)
        {
            if (jobList.Count() == 0)
                return;

            foreach (var job in jobList)
            {
                try
                {
                    var decryptedParameters = Entities.Helpers.Decrypt(job.Parameters, config.EncryptionKey);
                    CreateThread(job.ProcessID, job.JobID, job.InvokeMeta, decryptedParameters); //Use the DecryptedParameters, NOT encrypted Parameters
                }
                catch (Exception exc)
                {
                    //just mark as Invoke error, don't stop
                    var error = job.Error + " Invoke error: " + exc.ToString();
                    jobDAL.SetCachedProgressError(job.JobID, error);
                    var processID = string.IsNullOrWhiteSpace(job.ProcessID) ? config.ProcessID : job.ProcessID;
                    jobDAL.SetError(processID, job.JobID, error);
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
        private void CreateThread(string processID, int jobID, string invokeMeta, string parameters)
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

            var thread = new Thread(() => ExecuteJob(processID, jobID, methodInfo, parameters, instance)); //Create the new Job thread

            if (threadList.ContainsKey(jobID))
            {
                //already in threadList, has not been cleaned up, so replace with the new one
                threadList.Remove(jobID);
            }
            threadList.Add(jobID, thread); //Keep track of running thread
            thread.Name = "Shift Thread " + thread.ManagedThreadId;
            thread.IsBackground = true; //keep the main process running https://msdn.microsoft.com/en-us/library/system.threading.thread.isbackground

            thread.Start();
        }

        private IProgress<ProgressInfo> UpdateProgressEvent(int jobID)
        {
            //Insert a progress row first for the related jobID if it doesn't exist
            jobDAL.SetProgress(jobID, null, null, null);
            jobDAL.SetCachedProgress(jobID, null, null, null);

            var start = DateTime.Now;
            var updateTs = config.ProgressDBInterval ?? new TimeSpan(0, 0, 10); //default to 10 sec interval for updating DB

            //SynchronousProgress is event based and called regularly by the running job
            var progress = new SynchronousProgress<ProgressInfo>(progressInfo =>
            {
                jobDAL.SetCachedProgress(jobID, progressInfo.Percent, progressInfo.Note, progressInfo.Data);

                var diffTs = DateTime.Now - start;
                if (diffTs >= updateTs || progressInfo.Percent >= 100)
                {
                    //Update DB and Cache
                    jobDAL.UpdateProgressAsync(jobID, progressInfo.Percent, progressInfo.Note, progressInfo.Data); //async, don't wait/don't hold
                    start = DateTime.Now;
                }
            });

            return progress;
        }

        private void ExecuteJob(string processID, int jobID, MethodInfo methodInfo, string parameters, object instance)
        {
            try
            {
                //Set job to Running
                jobDAL.SetToRunning(processID, jobID);
                jobDAL.SetCachedProgressStatus(jobID, JobStatus.Running);

                var progress = UpdateProgressEvent(jobID); //Need this to update the progress of the job's

                //Invoke Method
                var args = DALHelpers.DeserializeArguments(progress, methodInfo, parameters);
                methodInfo.Invoke(instance, args);
            }
            catch (ThreadAbortException txc)
            {
                var row = jobDAL.GetJob(jobID);
                if (row != null && row.Command != JobCommand.Stop && row.Status != JobStatus.Stopped)
                {
                    var error = row.Error + " " + txc.ToString();
                    jobDAL.SetCachedProgressError(row.JobID, error);
                    jobDAL.SetError(processID, row.JobID, error);
                }
                return; //can't throw to another thread so quit here
            }
            catch (Exception exc)
            {
                var row = jobDAL.GetJob(jobID);
                var error = row.Error + " " + exc.ToString();
                jobDAL.SetCachedProgressError(row.JobID, error);
                jobDAL.SetError(processID, row.JobID, error);

                return; //can't throw to another thread so quit here
            }

            //Entire thread completes successfully
            jobDAL.SetCompleted(processID, jobID);
            jobDAL.SetCachedProgressStatus(jobID, JobStatus.Completed);
            var rsTask = Task.Delay(60000).ContinueWith(_ =>
            {
                jobDAL.DeleteCachedProgress(jobID);
            }); //Delay delete to allow real time GetCachedProgress not hitting DB right away.
        }

        /// <summary>
        /// Stops jobs.
        /// Only jobs marked with "STOP" command will be acted on.
        /// This uses Thread.Abort.
        /// No clean up is possible when thread is aborted.
        /// </summary>
        public void StopJobs()
        {
            var jobIDs = jobDAL.GetJobIdsByProcessAndCommand(config.ProcessID, JobCommand.Stop);

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
            jobDAL.SetToStopped(jobIDs);
            jobDAL.SetCachedProgressStatus(jobIDs, JobStatus.Stopped); //redis cached progress
            jobDAL.DeleteCachedProgress(jobIDs);
        }

        /// <summary>
        /// Cleanup and synchronize running jobs and jobs table.
        /// * Job is deleted based on AutoDeletePeriod and AutoDeleteStatus settings.
        /// * Mark job as an error, when job status is "RUNNING" in DB table, but there is no actual running thread in the related server process (Zombie Jobs).
        /// * Remove thread references in memory, when job is deleted or status in DB is: stopped, error, or completed.
        /// </summary>
        public void CleanUp()
        {
            //Delete past completed jobs from storage
            if (config.AutoDeletePeriod != null)
            {
                var count = jobDAL.Delete(config.AutoDeletePeriod.Value, config.AutoDeleteStatus);
            }

            // For Running jobs, mark as error if no reference in threadList.
            // DB record is marked as Status = Running but NO thread in threadList (crashed, aborted, etc) => Mark as error and add error message
            // If not in threadList, it's a rogue thread or crashed, better to mark as error and restart.
            var jobList = jobDAL.GetJobsByProcessAndStatus(config.ProcessID, JobStatus.Running);
            foreach (var job in jobList)
            {
                if (!threadList.ContainsKey(job.JobID))
                {
                    //Doesn't exist anymore? 
                    var error = "Error: No actual running job process found. Try reset and run again.";
                    jobDAL.SetCachedProgressError(job.JobID, error);
                    var processID = string.IsNullOrWhiteSpace(job.ProcessID) ? config.ProcessID : job.ProcessID;
                    jobDAL.SetError(processID, job.JobID, error);
                }
            }

            // Remove reference from ThreadList 
            if (threadList.Count != 0)
            {
                var inDBjobIDs = new List<int>();
                jobList = jobDAL.GetJobs(threadList.Keys.ToList()); //get all jobs in threadList

                // If jobs doesn't even exists in storage (zombie?), remove from threadList.
                inDBjobIDs = jobList.Select(j => j.JobID).ToList();
                var threadListKeys = new List<int>(threadList.Keys); //copy keys before removal
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
        #endregion
    }
}
