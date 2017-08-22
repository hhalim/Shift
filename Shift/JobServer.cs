//
// This software is distributed WITHOUT ANY WARRANTY and also without the implied warranty of merchantability, capability, or fitness for any particular purposes.
//

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Autofac;
using Autofac.Features.ResolveAnything;
using Shift.Entities;

namespace Shift
{
    public class JobServer
    {
        private ServerConfig config = null;
        private static System.Timers.Timer timer = null;
        private static System.Timers.Timer timer2 = null;

        private List<Worker> workerList = null; //reference to Workers

        ///<summary>
        ///Initializes a new instance of JobServer class, injects data layer with connection and configuration strings.
        ///</summary>
        ///<param name="config">Setup the database connection string, cache configuration, etc.</param>
        ///
        public JobServer(ServerConfig config)
        {
            if (config == null)
            {
                throw new ArgumentNullException("Unable to create with no configuration.");
            }

            if (string.IsNullOrWhiteSpace(config.StorageMode))
            {
                throw new ArgumentNullException("The storage mode must not be empty.");

            }

            if (string.IsNullOrWhiteSpace(config.ProcessID))
            {
                config.ProcessID = ProcessIDGenerator.Generate(true);
            }

            if (string.IsNullOrWhiteSpace(config.DBConnectionString))
            {
                throw new ArgumentNullException("Unable to run without DB storage connection string.");
            }

            if (config.UseCache && string.IsNullOrWhiteSpace(config.CacheConfigurationString))
            {
                throw new ArgumentNullException("Unable to run without Cache configuration string.");
            }

            if (config.MaxRunnableJobs <= 0)
            {
                config.MaxRunnableJobs = 100;
            }

            this.config = config;

            this.workerList = new List<Worker>();

            //OPTIONAL: Load all EXTERNAL DLLs needed by this process
            AppDomain.CurrentDomain.AssemblyResolve += AssemblyHelpers.OnAssemblyResolve;
            AssemblyHelpers.LoadAssemblies(config.AssemblyFolder, config.AssemblyListPath);

            //Create Worker
            var builder = new ContainerBuilder();
            builder.RegisterSource(new AnyConcreteTypeNotAlreadyRegisteredSource());
            RegisterAssembly.RegisterTypes(builder, config.StorageMode, config.DBConnectionString, config.UseCache, config.CacheConfigurationString, config.EncryptionKey, config.DBAuthKey);
            var container = builder.Build();
            //Use lifetime scope to avoid memory leak http://docs.autofac.org/en/latest/resolve/
            using (var scope = container.BeginLifetimeScope())
            {
                var jobDAL = scope.Resolve<IJobDAL>();
                for (var i = 1; i <= config.Workers; i++)
                {
                    var worker = new Worker(config, jobDAL, i);
                    workerList.Add(worker);
                }
            }
        }

        #region Server Run 
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
                await CleanUpAsync(isSync);

            if (timer == null && timer2 == null)
            {
                timer = new System.Timers.Timer();
                timer.Interval = config.ServerTimerInterval;
                timer.Elapsed += async (sender, e) =>
                {
                    if (isSync)
                    {
                        PauseJobs();
                        ContinueJobs();
                        StopJobs();
                        RunJobs();
                    }
                    else
                    {
                        await PauseJobsAsync();
                        await ContinueJobsAsync();
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
                        await CleanUpAsync(isSync);
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

        #endregion

        #region Run Jobs
        /// <summary>
        /// Pick up jobs from storage and run them.
        /// </summary>
        public void RunJobs()
        {
            RunJobsAsync(true).GetAwaiter().GetResult();
        }

        public async Task RunJobsAsync()
        {
            await RunJobsAsync(false);
        }

        public async Task RunJobsAsync(bool isSync)
        {
            foreach (var worker in workerList)
            {
                if (isSync)
                    worker.RunJobsAsync(isSync).GetAwaiter().GetResult();
                else
                    await worker.RunJobsAsync(isSync);
            }
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
            foreach (var worker in workerList)
            {
                if (isSync)
                    worker.RunJobsAsync(jobIDs, isSync).GetAwaiter().GetResult();
                else
                    await worker.RunJobsAsync(jobIDs, isSync);
            }
        }
        #endregion

        #region Stop Jobs
        /// <summary>
        /// Stop running jobs.
        /// </summary>
        public void StopJobs()
        {
            StopJobsAsync(true).GetAwaiter().GetResult();
        }

        public async Task StopJobsAsync()
        {
            await StopJobsAsync(false);
        }

        private async Task StopJobsAsync(bool isSync)
        {
            foreach (var worker in workerList)
            {
                if (isSync)
                    worker.StopJobsAsync(isSync).GetAwaiter().GetResult();
                else
                    await worker.StopJobsAsync(isSync);
            }
        }
        #endregion

        #region Stop Server
        /// <summary>
        /// Stop running server.
        /// </summary>
        public void StopServer()
        {
            //Stop timers
            if (timer != null && timer2 != null)
            {
                timer.Close();
                timer2.Close();
            }

            if (config.ForceStopServer)
            {
                AttemptToStopJobsAsync(true).GetAwaiter().GetResult();
            }
            else
            {
                WaitForAllRunningJobsToStop(true).GetAwaiter().GetResult();
            }
        }

        public async Task StopServerAsync()
        {
            //Stop timers
            if (timer != null && timer2 != null)
            {
                timer.Close();
                timer2.Close();
            }

            if (config.ForceStopServer)
            {
                await AttemptToStopJobsAsync(false);
            }
            else
            {
                await WaitForAllRunningJobsToStop(false);
            }
        }

        private async Task SetStopAllRunningJobsAsync(bool isSync)
        {
            foreach(var worker in workerList)
            {
                if (isSync)
                    worker.SetStopAllRunningJobsAsync(isSync).GetAwaiter().GetResult();
                else
                    await worker.SetStopAllRunningJobsAsync(isSync);
            }
        }
        
        private async Task AttemptToStopJobsAsync(bool isSync)
        {
            if (isSync)
            {
                SetStopAllRunningJobsAsync(true).GetAwaiter().GetResult();
                StopJobsAsync(true).GetAwaiter().GetResult();
                Task.Delay(config.StopServerDelay).GetAwaiter().GetResult(); //Wait before shutting down
            }
            else
            {
                await SetStopAllRunningJobsAsync(false); //mark 'stop' commands
                await StopJobsAsync(false); //stop jobs
                await Task.Delay(config.StopServerDelay); //Wait before shutting down
            }
        }

        //Clean up periodically and wait for all running tasks to Stop before quitting
        private async Task WaitForAllRunningJobsToStop(bool isSync)
        {
            var stopNow = false;
            while (!stopNow)
            {
                var runningCount = 0;

                if (isSync)
                    AttemptToStopJobsAsync(isSync).GetAwaiter().GetResult();
                else
                    await AttemptToStopJobsAsync(isSync);

                //check for still running jobs
                foreach (var worker in workerList)
                {
                    if (isSync)
                    {
                        worker.CleanUpAsync(isSync).GetAwaiter().GetResult();
                        runningCount += worker.CountRunningJobsAsync(isSync).GetAwaiter().GetResult();
                    }
                    else
                    {
                        await worker.CleanUpAsync(isSync);
                        runningCount += await worker.CountRunningJobsAsync(isSync);
                    }
                }

                if (runningCount == 0)
                {
                    stopNow = true;
                    break;
                }
            }

        }
        #endregion

        #region Clean Up
        /// <summary>
        /// Cleanup and synchronize running jobs and jobs table.
        /// * Job is deleted based on AutoDeletePeriod and AutoDeleteStatus settings.
        /// * Mark job as an error, when job status is "RUNNING" in DB table, but there is no actual running thread in the related server process (Zombie Jobs).
        /// * Remove thread references in memory, when job is deleted or status in DB is: stopped, error, or completed.
        /// </summary>
        public void CleanUp()
        {
            CleanUpAsync(true).GetAwaiter().GetResult();
        }

        public async Task CleanUpAsync()
        {
            await CleanUpAsync(false);
        }

        public async Task CleanUpAsync(bool isSync)
        {
            foreach (var worker in workerList)
            {
                if (isSync)
                    worker.CleanUpAsync(isSync).GetAwaiter().GetResult();
                else
                    await worker.CleanUpAsync(isSync);
            }
        }

        #endregion

        #region Pause / Continue Jobs
        /// <summary>
        /// Pause running jobs.
        /// </summary>
        public void PauseJobs()
        {
            PauseJobsAsync(true).GetAwaiter().GetResult();
        }

        public async Task PauseJobsAsync()
        {
            await PauseJobsAsync(false);
        }

        private async Task PauseJobsAsync(bool isSync)
        {
            foreach (var worker in workerList)
            {
                if (isSync)
                    worker.PauseJobsAsync(isSync).GetAwaiter().GetResult();
                else
                    await worker.PauseJobsAsync(isSync);
            }
        }

        /// <summary>
        /// Continue paused jobs.
        /// </summary>
        public void ContinueJobs()
        {
            ContinueJobsAsync(true).GetAwaiter().GetResult();
        }

        public async Task ContinueJobsAsync()
        {
            await ContinueJobsAsync(false);
        }

        private async Task ContinueJobsAsync(bool isSync)
        {
            foreach (var worker in workerList)
            {
                if (isSync)
                    worker.ContinueJobsAsync(isSync).GetAwaiter().GetResult();
                else
                    await worker.ContinueJobsAsync(isSync);
            }
        }
        #endregion

    }

}
