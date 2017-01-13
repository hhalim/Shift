//
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
        private JobDAL jobDAL = null;
        private Options options = null;
        private readonly ContainerBuilder builder;
        private readonly IContainer container;

        private Dictionary<int, Thread> threadList = null; //reference to running thread

        ///<summary>
        ///Initializes a new instance of JobServer class, injects data layer with connection and configuration strings.
        ///</summary>
        ///<param name="options">Setup the database connection string, cache configuration, etc.</param>
        ///
        public JobServer(Options options)
        {
            if (options == null)
            {
                throw new Exception("Unable to start with no options.");
            }

            if (string.IsNullOrWhiteSpace(options.DBConnectionString))
            {
                throw new Exception("Error: unable to start without DB connection string.");

            }

            if (string.IsNullOrWhiteSpace(options.CacheConfigurationString))
            {
                throw new Exception("Error: unable to start without Cache configuration string.");

            }

            if (options.MaxRunnableJobs <= 0)
            {
                options.MaxRunnableJobs = 100;
            }

            this.options = options;

            builder = new ContainerBuilder();
            builder.RegisterSource(new AnyConcreteTypeNotAlreadyRegisteredSource());
            Register.RegisterTypes(builder, options);
            container = builder.Build();
        }

        private static class Register
        {
            public static void RegisterTypes(ContainerBuilder builder, Options options)
            {
                builder.RegisterType<DataLayer.Redis.Cache>().As<IJobCache>().WithParameter("configurationString", options.CacheConfigurationString);
                var parameters = Helpers.GenerateNamedParameters(new Dictionary<string, object> { { "connectionString", options.DBConnectionString }, { "encryptionKey", options.EncryptionKey } });
                builder.RegisterType<JobDAL>().As<JobDAL>().WithParameters(parameters);
            }
        }

        #region Startup
        /// <summary>
        /// Start the server.
        /// Instantiate the data layer and loads all the referenced assemblies defined in the assembly list text file 
        /// in Options.AssemblyListPath and Options.AssemblyBaseDir
        /// </summary>
        public void Start()
        {
            this.threadList = new Dictionary<int, Thread>();

            jobDAL = container.Resolve<JobDAL>();

            //OPTIONAL: Load all EXTERNAL DLLs needed by this process
            AppDomain.CurrentDomain.AssemblyResolve += OnAssemblyResolve;
            LoadAssemblies(options.AssemblyListPath, options.AssemblyBaseDir);
        }

        //Load all assemblies in specified text list
        //Don't do anything if no file is included
        protected static void LoadAssemblies(string path, string baseDir)
        {
            if (string.IsNullOrWhiteSpace(path))
                return;

            var fileList = new System.IO.StreamReader(path);
            try
            {
                string line;

                while ((line = fileList.ReadLine()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line) || line.StartsWith("#"))
                        continue;

                    if (!string.IsNullOrWhiteSpace(baseDir)) //assume no absolute path, just filenames and sub folders
                        line = baseDir + @"\" + line;

                    var filename = Path.GetFileName(line);
                    var directoryName = Path.GetDirectoryName(line);
                    if (string.IsNullOrWhiteSpace(filename))
                        throw new Exception("Error: filename doesn't exist, unable to use folder name only: " + line);

                    //Assume it's a search through a pattern of files Common.* / States.* / etc.
                    if (directoryName == null)
                        continue;
                    var files = Directory.GetFiles(directoryName, filename);
                    if (files.Length == 0) //Still nothing!!!
                        throw new Exception("Error: Unable to find the assembly file(s): " + line);

                    foreach (var file in files)
                    {
                        var extension = Path.GetExtension(file);
                        if (extension != ".dll")
                            continue;
                        var assembly = Assembly.LoadFrom(file);

#if DEBUG
                        Debug.WriteLine("Loaded assembly \"" + line + "\": " + assembly.FullName);
#endif
                    }

                }
            }
            catch (Exception exc)
            {
                throw;
            }
            finally
            {
                fileList.Close();
            }
        }

        protected static Assembly OnAssemblyResolve(object sender, ResolveEventArgs args)
        {
            /*
            Could not load file or assembly '<assembly name>.resources, Version=14.0.1.1, Culture=en-US, PublicKeyToken=null' or one of its dependencies.
            The system cannot find the file specified.

            This turns out to be a bug in OnAssemblyResolve unable to find assembly.
            Apparently the OnAssemblyResolve is being called for *.resources, which will fail, since the LoadFrom do not load *.resources only DLL
            http://stackoverflow.com/a/4977761
            */

            string[] fields = args.Name.Split(',');
            if (fields.Length >= 3)
            {
                var name = fields[0];
                var culture = fields[2];
                if (name.EndsWith(".resources") && !culture.EndsWith("neutral")) return null;
            }

            var asmList = AppDomain.CurrentDomain.GetAssemblies();
            var asm = (from item in asmList
                       where args.Name == item.GetName().FullName || args.Name == item.GetName().Name
                       select item).FirstOrDefault();

            if (asm == null)
                return null; //let the original code blows up, instead of throwing exception here.     

            return asm;
        }

        #endregion

        #region Server Run and Manage jobs
        //The region that primarily manage and run/stop/cleanup jobs that were added in the DB table by the clients

        /// <summary>
        /// Pick up jobs from storage and run them.
        /// </summary>
        public void StartJobs()
        {
            using (var connection = new SqlConnection(options.DBConnectionString))
            {
                connection.Open();

                //Check max jobs count
                var runningCount = jobDAL.CountRunningJobs(options.ProcessID);
                if (runningCount >= options.MaxRunnableJobs)
                {
                    return;
                }

                var rowsToGet = options.MaxRunnableJobs - runningCount;

                var jobList = jobDAL.GetJobsToRun(rowsToGet);
                var claimedJobs = jobDAL.ClaimJobsToRun(options.ProcessID, jobList);

                RunJobs(claimedJobs);
            }
        }

        /// <summary>
        /// Pick up specific jobs from storage and run them.
        /// </summary>
        /// 
        ///<param name="jobIDs">List of job IDs to run.</param>
        public void StartJobs(List<int> jobIDs)
        {
            //Try to start the selected jobs, ignoring MaxRunableJobs
            var jobList = jobDAL.GetJobsByStatus(jobIDs, "Status IS NULL");
            var claimedJobs = jobDAL.ClaimJobsToRun(options.ProcessID, jobList);

            RunJobs(claimedJobs);
        }

        //Finally Run the Jobs
        protected void RunJobs(IEnumerable<Job> jobList)
        {
            if (jobList.Count() == 0)
                return;

            foreach (var row in jobList)
            {
                try
                {
                    var decryptedParameters = Entities.Helpers.Decrypt(row.Parameters, options.EncryptionKey);
                    CreateThread(row.JobID, row.InvokeMeta, decryptedParameters); //Use the DecryptedParameters, NOT encrypted Parameters
                }
                catch (Exception exc)
                {
                    //just mark as Invoke error, don't stop
                    var error = row.Error + " Invoke error: " + exc.ToString();
                    jobDAL.SetCachedProgressError(row.JobID, error);
                    jobDAL.SetError(row.JobID, error);
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
        private void CreateThread(int jobID, string invokeMeta, string parameters)
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

            var thread = new Thread(() => RunJob(jobID, methodInfo, parameters, instance)); //Create the new Job thread

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
            var updateTs = options.ProgressDBInterval ?? new TimeSpan(0, 0, 10); //default to 10 sec interval for updating DB

            var progress = new SynchronousProgress<ProgressInfo>(progressInfo =>
            {
                //SynchronousProgress is event based and called regularly by the running job

                var diffTs = DateTime.Now - start;
                if (diffTs >= updateTs || progressInfo.Percent >= 100)
                {
                    //Update DB and Cache
                    jobDAL.UpdateProgress(jobID, progressInfo.Percent, progressInfo.Note, progressInfo.Data);
                    jobDAL.SetCachedProgress(jobID, progressInfo.Percent, progressInfo.Note, progressInfo.Data);
                    start = DateTime.Now;
                }
                else
                {
                    //Update Redis cache only
                    jobDAL.SetCachedProgress(jobID, progressInfo.Percent, progressInfo.Note, progressInfo.Data);
                }

                //Insert JobResult data
                var resultList = new List<JobResult>();
                if (progressInfo.FileInfoList != null && progressInfo.FileInfoList.Count > 0)
                {
                    foreach (var fileInfo in progressInfo.FileInfoList)
                    {
                        var result = new JobResult
                        {
                            JobID = jobID,
                            ExternalID = fileInfo.ExternalID,
                            BinaryContent = File.ReadAllBytes(fileInfo.FullPath),
                            ContentType = fileInfo.ContentType,
                            Name = fileInfo.FileName
                        };

                        resultList.Add(result);
                    }

                    if (resultList.Count > 0)
                    {
                        jobDAL.InsertResults(resultList);

                        //Delete files if marked delete after upload
                        foreach (var fileInfo in progressInfo.FileInfoList)
                        {
                            if (fileInfo.DeleteAfterUpload)
                            {
                                try
                                {
                                    File.Delete(fileInfo.FullPath);
                                }
                                catch (Exception exc)
                                {
                                    //Don't die if unable to delete, just record the error
                                    var row = jobDAL.GetJob(jobID);
                                    var error = row.Error + " Unable to delete file: " + fileInfo.FullPath;
                                    jobDAL.SetCachedProgressError(row.JobID, error);
                                    jobDAL.SetError(row.JobID, error);
                                }
                            }
                        }
                    }
                }

            });

            return progress;
        }

        private void RunJob(int jobID, MethodInfo methodInfo, string parameters, object instance)
        {
            try
            {
                //Set job to Running
                jobDAL.SetToRunning(jobID);
                jobDAL.SetCachedProgressStatus(jobID, JobStatus.Running);

                var progress = UpdateProgressEvent(jobID); //Need this to update the progress of the job's

                //Invoke Method
                var args = JobDAL.DeserializeArguments(progress, methodInfo, parameters);
                methodInfo.Invoke(instance, args);
            }
            catch (ThreadAbortException txc)
            {
                var row = jobDAL.GetJob(jobID);
                if (row != null && row.Command != JobCommand.Stop && row.Status != JobStatus.Stopped)
                {
                    var error = row.Error + " " + txc.ToString();
                    jobDAL.SetCachedProgressError(row.JobID, error);
                    jobDAL.SetError(row.JobID, error);
                }
                return; //can't throw to another thread so quit here
            }
            catch (Exception exc)
            {
                var row = jobDAL.GetJob(jobID);
                var error = row.Error + " " + exc.ToString();
                jobDAL.SetCachedProgressError(row.JobID, error);
                jobDAL.SetError(row.JobID, error);

                return; //can't throw to another thread so quit here
            }

            //Entire thread completes successfully
            jobDAL.SetCompleted(jobID);
            jobDAL.SetCachedProgressStatus(jobID, JobStatus.Completed);
            var rsTask = Task.Delay(60000).ContinueWith(_ =>
            {
                jobDAL.DeleteCachedProgress(jobID);
            }); //Delay delete to allow realtime GetCachedProgress not hitting DB right away.
        }

        /// <summary>
        /// Stops jobs.
        /// Only jobs marked with "STOP" command will be acted on.
        /// This uses Thread.Abort.
        /// No clean up is possible when thread is aborted.
        /// </summary>
        public void StopJobs()
        {
            var jobList = jobDAL.GetJobsByCommand(options.ProcessID, JobCommand.Stop);

            //abort running threads
            if (threadList.Count > 0)
            {
                foreach (var row in jobList)
                {
                    var thread = threadList.ContainsKey(row.JobID) ? threadList[row.JobID] : null;
                    if (thread != null)
                    {
                        thread.Abort();
                        threadList.Remove(row.JobID);
                    }
                }
            }

            //mark status to stopped
            var ids = jobList.Select(j => j.JobID).ToList<int>();
            jobDAL.SetToStopped(ids);
            jobDAL.SetCachedProgressStatus(ids, JobStatus.Stopped); //redis cached progress
            jobDAL.DeleteCachedProgress(ids);
        }

        //Stop if running and then delete jobs
        //Useful for UI that closes window or Cancel job
        /// <summary>
        /// Stops jobs and then delete them.
        /// Only jobs marked with "STOPDELETE" command will be acted on.
        /// This uses Thread.Abort.
        /// No clean up is possible when thread is aborted.
        /// </summary>
        public void StopDeleteJobs()
        {
            var jobList = jobDAL.GetJobsByCommand(options.ProcessID, JobCommand.StopDelete);

            //abort running threads
            if (threadList.Count > 0)
            {
                foreach (var row in jobList)
                {
                    var thread = threadList.ContainsKey(row.JobID) ? threadList[row.JobID] : null;
                    if (thread != null)
                    {
                        thread.Abort();
                        threadList.Remove(row.JobID);
                    }
                }
            }

            //delete jobs
            var ids = jobList.Select(j => j.JobID).ToList<int>();
            jobDAL.SetToStopped(ids);
            jobDAL.SetCachedProgressStatus(ids, JobStatus.Stopped); //redis cached progress
            jobDAL.Delete(ids);
            jobDAL.DeleteCachedProgress(ids);
        }

        /// <summary>
        /// Cleanup and synchronize running jobs and jobs table.
        /// Cases for cleanup:
        /// * Job is marked as "RUNNING" in DB table, but there is no actual running thread in the related server process (Zombie Job).
        /// * Thread reference still in memory, but actual process is: stopped, error, completed.
        /// </summary>
        public void CleanUp()
        {
            // For Running jobs, mark as error if no reference in threadList.
            // DB record is marked as Status = Running but NO thread in threadList (crashed, aborted, etc) => Mark as error and add error message
            // If not in threadList, it's a rogue thread or crashed, better to mark as error and restart.
            var jobList = jobDAL.GetJobsByProcessAndStatus(options.ProcessID, JobStatus.Running);
            foreach (var job in jobList)
            {
                if (!threadList.ContainsKey(job.JobID))
                {
                    //Doesn't exist anymore? 
                    var error = "Error: No actual running job process found. Try reset and run again.";
                    jobDAL.SetCachedProgressError(job.JobID, error);
                    jobDAL.SetError(job.JobID, error);
                }
            }

            // Remove reference from ThreadList 
            if (threadList.Count != 0)
            {
                // It's possible that threadList still exists for DELETED jobs, so remove from threadList.
                // If thread reference still in list, but actual process is: stopped, error, completed. => Remove from list, no need to keep track of them anymore.
                var jobIDs = new List<int>();
                jobList = jobDAL.GetJobsByProcess(options.ProcessID, threadList.Keys.ToList());

                //Remove Deleted jobs from threadList
                jobIDs = jobList.Select(j => j.JobID).ToList();
                var keys = new List<int>(threadList.Keys); //copy keys before removal
                foreach (var jobID in keys)
                {
                    if (!jobIDs.Contains(jobID))
                    {
                        var thread = threadList[jobID];
                        if (thread.IsAlive)
                            thread.Abort();
                        threadList.Remove(jobID);
                    }
                }

                //Remove stopped / error / completed from threadList
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
