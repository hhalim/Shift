using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift
{
    public class ClientConfig
    {
        public string DBConnectionString { get; set; }

        public bool UseCache { get; set; } = false; //if false, the progress update depends on ProgressDBInterval time to update the DB
        public string CacheConfigurationString { get; set; }
        public string EncryptionKey { get; set; } //optional, if set, then parameters will be encrypted/decrypted automatically during storage

    }
}
