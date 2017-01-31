using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.ServiceProcess;
using System.Text;
using System.Timers;

namespace Shift.WinService
{
    partial class ShiftService : ServiceBase
    {
        private static JobServer jobServer;

        public ShiftService()
        {
            InitializeComponent();
            var appServiceName = ConfigurationManager.AppSettings["ServiceName"];
            var processID = ConfigurationManager.AppSettings["ShiftPID"];

            if (string.IsNullOrWhiteSpace(processID))
                throw new IndexOutOfRangeException("Configuration for ShiftPID is missing or invalid.");

            if (jobServer == null)
            {
                var config = new Shift.ServerConfig();
                config.AssemblyListPath = ConfigurationManager.AppSettings["AssemblyListPath"];
                config.MaxRunnableJobs = Convert.ToInt32(ConfigurationManager.AppSettings["MaxRunableJobs"]);
                config.ProcessID = ConfigurationManager.AppSettings["ShiftPID"];
                config.DBConnectionString = ConfigurationManager.ConnectionStrings["ShiftDBConnection"].ConnectionString;
                config.UseCache = Convert.ToBoolean(ConfigurationManager.AppSettings["UseCache"]);
                config.CacheConfigurationString = ConfigurationManager.AppSettings["RedisConfiguration"];
                config.EncryptionKey = ConfigurationManager.AppSettings["ShiftEncryptionParametersKey"];

                //options.ServerTimerInterval = 5000; //optional: default every 5 sec for getting jobs ready to run and run them
                //options.ServerTimerInterval2 = 10000; //optional: default every 10 sec for server CleanUp()

                jobServer = new Shift.JobServer(config);
            }

            this.ServiceName = appServiceName;
        }

        protected override void OnStart(string[] args)
        {
            if (Array.Find<string>(args, s=> s == "-debug") == "-debug")
            {
                StartWithNoTimer();
            }
            else
            {
                StartWithTimer();
            }
        }

        protected override void OnStop()
        {
            jobServer.StopServer();
        }

        //This is for debugging OnStart and OnStop as Console App
        internal void TestStartAndStop(string[] args)
        {
            this.OnStart(args);
            Console.ReadLine();
            this.OnStop();
        }

        protected void StartWithTimer()
        {
            try
            {
                jobServer.RunServer();
            }
            catch (Exception ex)
            {
                this.EventLog.WriteEntry(ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);

                // Log exception
                this.ExitCode = 1064;
                this.Stop();
                throw;
            }
        }

        //FOR DEBUGGING ONLY
        protected void StartWithNoTimer()
        {
            try
            {
                jobServer.StopJobs();
                jobServer.StopDeleteJobs();
                jobServer.RunJobs();

                jobServer.CleanUp();
            }
            catch (Exception ex)
            {
                this.EventLog.WriteEntry(ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);

                // Log exception
                this.ExitCode = 1064;
                this.Stop();
                throw;
            }
        }

    }
}
