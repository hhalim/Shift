using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using System.Timers;
using System.Configuration;
using System.IO;
using System.Diagnostics;

namespace Shift.WebJob
{
    class Program
    {
        // Please set the following connection strings in app.config for this WebJob to run:
        // AzureWebJobsDashboard and AzureWebJobsStorage
        static void Main()
        {
            var host = new JobHost();
            StartJob();

            // The following code ensures that the WebJob will be running continuously
            host.RunAndBlock();
        }

        [NoAutomaticTrigger]
        public static void StartJob()
        {
            //Run Jobs
            var pjob = new ProcessJobsService();
            pjob.Start();
        }

    }

    public class ProcessJobsService
    {
        private Timer _timer = null;
        private Timer _timer2 = null;
        private static JobServer jobServer;

        public ProcessJobsService()
        {
            //var processID = ConfigurationManager.AppSettings["JobManagerProcessID"];
            //if (jobServer == null)
            //{
            //    var dbConnection = ConfigurationManager.ConnectionStrings["QueueDBConnection"].ConnectionString;
            //    jobServer = new ManageJobs(Convert.ToInt32(processID), dbConnection);
            //}

            if (jobServer == null)
            {
                //Load Assemblies
                var baseDir = AppDomain.CurrentDomain.BaseDirectory;
                Console.WriteLine("BaseDirectory: " + baseDir);

                var options = new Shift.Options();
                options.AssemblyListPath = baseDir + @"\assemblies\assemblylist.txt";
                options.AssemblyBaseDir = baseDir + @"\assemblies\";
                options.MaxRunnableJobs = Convert.ToInt32(ConfigurationManager.AppSettings["MaxRunableJobs"]);
                options.ProcessID = Convert.ToInt32(ConfigurationManager.AppSettings["ShiftPID"]);
                options.DBConnectionString = ConfigurationManager.ConnectionStrings["ShiftDBConnection"].ConnectionString;
                options.RedisConnectionString = ConfigurationManager.AppSettings["RedisConfiguration"];

                jobServer = new Shift.JobServer(options);
            }

        }

        public void Start()
        {
            try
            {
                var maxRunableJobs = Convert.ToInt32(ConfigurationManager.AppSettings["MaxRunableJobs"]);
                _timer = new Timer();
                _timer.Enabled = true;
                _timer.Interval = Convert.ToDouble(5000);
                _timer.Elapsed += (sender, e) => {
                    ExecuteCommands();
                    jobServer.StartJobs();

                    //Console.WriteLine("StartJobs");
                };

                _timer2 = new Timer();
                _timer2.Enabled = true;
                _timer2.Interval = Convert.ToDouble(ConfigurationManager.AppSettings["CleanUpTimerInterval"]);
                _timer2.Elapsed += (sender, e) => {
                    jobServer.CleanUp();
                    //Console.WriteLine("CleanUp");
                };
                }
            catch (Exception ex)
            {
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
