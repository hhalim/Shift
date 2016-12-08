using System;
using System.Configuration.Install;
using System.Linq;
using System.Reflection;
using System.ServiceProcess;

namespace Shift.WinService
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            if (Environment.UserInteractive)
            {
                try
                {
                    if (args.Count() == 1)
                    {
                        if (args[0] == "-install")
                        {
                            ManagedInstallerClass.InstallHelper(new string[] { Assembly.GetExecutingAssembly().Location });
                            Console.WriteLine("Service installed.");
                        }

                        if (args[0] == "-uninstall")
                        {
                            ManagedInstallerClass.InstallHelper(new string[] { "/u", Assembly.GetExecutingAssembly().Location });
                            Console.WriteLine("Service un-installed.");
                        }

                        if (args[0] == "-debug")
                        {
                            var debugService = new ShiftService();
                            debugService.TestStartAndStop(args);
                        }
                    }
                }
                catch(Exception exc)
                {
                    Console.WriteLine("Error: " + exc.Message);
                }

                return;
            }

            var servicesToRun = new ServiceBase[]
            {
                new ShiftService()
            };
            ServiceBase.Run(servicesToRun);
        }

    }
}
