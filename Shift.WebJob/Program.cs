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
            var pjob = new ShiftService();
            pjob.Start();
        }

    }

    public class ShiftService
    {
        private static JobServer jobServer; //only one server per WebJob

        public ShiftService()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            //Console.WriteLine("BaseDirectory: " + baseDir);

            var config = new Shift.ServerConfig();
            config.AssemblyListPath = baseDir + @"\client-assemblies\assemblylist.txt";
            config.AssemblyBaseDir = baseDir + @"\client-assemblies\"; //drop DLL dependencies for jobs here
            config.MaxRunnableJobs = Convert.ToInt32(ConfigurationManager.AppSettings["MaxRunableJobs"]);
            config.ProcessID = ConfigurationManager.AppSettings["ShiftPID"];
            config.DBConnectionString = ConfigurationManager.ConnectionStrings["ShiftDBConnection"].ConnectionString;
            config.UseCache = Convert.ToBoolean(ConfigurationManager.AppSettings["UseCache"]);
            config.CacheConfigurationString = ConfigurationManager.AppSettings["RedisConfiguration"];
            config.EncryptionKey = ConfigurationManager.AppSettings["ShiftEncryptionParametersKey"]; //optional

            //options.ServerTimerInterval = 5000; //optional: default every 5 sec for getting jobs ready to run and run them
            //options.ServerTimerInterval2 = 10000; //optional: default every 10 sec for server CleanUp()

            jobServer = new Shift.JobServer(config);

        }

        public void Start()
        {
            try
            {
                jobServer.RunServer();
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        public void Stop()
        {
            jobServer.StopServer();
        }
    }
}
