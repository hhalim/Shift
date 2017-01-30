using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift
{
    public class Options
    {
        //Time interval when updating job's progress in JobServer DB, cache progress instead. 
        //Avoid hitting the DB too much, since the Progress event update can be very chatty and rapid.
        public TimeSpan? ProgressDBInterval; 

        public int MaxRunnableJobs { get; set; }
        public int ProcessID { get; set; }
        public string DBConnectionString { get; set; }

        //A list of DLLs to reference and load, one DLL per line. If no full path is set, then define the base path in AssemblyBaseDir
        //DO NOT mix full path and no full path DLLs, must be consistent
        //Example:
        //Shift.Demo.Jobs.dll
        //Shift.Demo.*.dll
        public string AssemblyListPath { get; set; } //Optional

        //Defines the base directory for the DLLs inside AssemblyListPath file
        //REQUIRED if full path is NOT included in the list file DLL
        //DO NOT use if full path is already included in the list file.
        public string AssemblyBaseDir { get; set; } //Optional

        public bool UseCache { get; set; } = false; //if false, the progress update depends on ProgressDBInterval time to update the DB

        public string CacheConfigurationString { get; set; }
        public string EncryptionKey { get; set; } //optional, if set, then parameters will be encrypted/decrypted automatically during storage
    }
}
