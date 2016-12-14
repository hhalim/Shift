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
        private Timer _timer = null;
        private Timer _timer2 = null;
        private static JobServer jobServer;

        public ShiftService()
        {
            InitializeComponent();
            var appServiceName = ConfigurationManager.AppSettings["ServiceName"];
            var processID = ConfigurationManager.AppSettings["ShiftPID"];

            if (string.IsNullOrWhiteSpace(processID))
                throw new IndexOutOfRangeException("Configuration for AppSettings collection does not contain the ShiftPID key.");

            if (jobServer == null)
            {
                var options = new Shift.Options();
                options.AssemblyListPath = ConfigurationManager.AppSettings["AssemblyListPath"];
                options.MaxRunnableJobs = Convert.ToInt32(ConfigurationManager.AppSettings["MaxRunableJobs"]);
                options.ProcessID = Convert.ToInt32(ConfigurationManager.AppSettings["ShiftPID"]);
                options.DBConnectionString = ConfigurationManager.ConnectionStrings["ShiftDBConnection"].ConnectionString;
                options.CacheConfigurationString = ConfigurationManager.AppSettings["RedisConfiguration"];
                //options.EncryptionKey = ConfigurationManager.AppSettings["ShiftEncryptionParametersKey"];

                jobServer = new Shift.JobServer(options);
            }

            this.ServiceName = appServiceName + (string.IsNullOrWhiteSpace(processID) ? "" : " " + processID);
        }

        protected override void OnStart(string[] args)
        {
            jobServer.Start(); //start server

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
            if (_timer != null && _timer2 != null)
            {
                _timer.Close();
                _timer2.Close();
            }
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
                _timer = new Timer();
                _timer.Enabled = true;
                _timer.Interval = Convert.ToDouble(ConfigurationManager.AppSettings["TimerInterval"]);
                _timer.Elapsed += (sender, e) => {
                    ExecuteCommands();
                    jobServer.StartJobs();
                };

                _timer2 = new Timer();
                _timer2.Enabled = true;
                _timer2.Interval = Convert.ToDouble(ConfigurationManager.AppSettings["CleanUpTimerInterval"]);
                _timer2.Elapsed += (sender, e) => {
                    jobServer.CleanUp();
                };
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
                ExecuteCommands();
                jobServer.StartJobs();

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

        protected void ExecuteCommands()
        {
            jobServer.StopJobs();
            jobServer.StopDeleteJobs();
        }


    }
}
