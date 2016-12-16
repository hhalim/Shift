using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift
{
    public class Options
    {
        public int MaxRunnableJobs { get; set; }
        public int ProcessID { get; set; }
        public string DBConnectionString { get; set; }

        //A list of DLLs to reference and load, one DLL per line. If no full path is set, then define the base path in AssemblyBaseDir
        public string AssemblyListPath { get; set; } //Optional
        //Defines the base directory for the DLLs inside AssemblyListPath file, DO NOT use if full path is already included in the list file.
        public string AssemblyBaseDir { get; set; } //Optional

        public string CacheConfigurationString { get; set; }
        public string EncryptionKey { get; set; } //optional, if set, then parameters will be encrypted/decrypted automatically during storage
    }
}
