using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift
{
    public class ServerConfig
    {
        [Required]
        public string ProcessID { get; set; }

        [Required]
        public string DBConnectionString { get; set; }

        //Maximum jobs to run for each server
        public int MaxRunnableJobs { get; set; } = 100;

        //A list of DLLs to reference and load, one DLL per line. If no full path is set, then define the base path in AssemblyBaseDir
        //DO NOT mix full path and no full path DLLs, must be consistent
        //Example:
        //Shift.Demo.Jobs.dll
        //Shift.Demo.*.dll
        public string AssemblyListPath { get; set; } //Optional

        //if false, the progress update depends on ProgressDBInterval time to update the DB
        public bool UseCache { get; set; } = false;
        public string CacheConfigurationString { get; set; } //required if UseCache == true

        public string EncryptionKey { get; set; } //optional, if set, then parameters will be encrypted/decrypted automatically during storage

        //Time interval when updating job's progress in JobServer DB. 
        //If UseCache is true, then server will cache progress in between the interval. 
        //If UseCache is false, possibly need to lower this interval value, instead of default to 10 sec.  
        //Avoid hitting the DB too much, since the Progress event update can be very chatty and rapid.
        public TimeSpan? ProgressDBInterval { get; set; } = new TimeSpan(0, 0, 10);

        public int ServerTimerInterval { get; set; } = 5000;//interval timer for server running jobs
        public int ServerTimerInterval2 { get; set; } = 10000;//interval timer2 for server running cleanup
    }
}
