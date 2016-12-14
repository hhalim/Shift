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

        public string AssemblyListPath { get; set; } //Optional
        public string AssemblyBaseDir { get; set; } //Optional

        public string CacheConfigurationString { get; set; }
        public string EncryptionKey { get; set; } //optional, if set, then parameters will be encrypted/decrypted automatically during storage
    }
}
