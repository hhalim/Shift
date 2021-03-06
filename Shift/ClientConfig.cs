﻿using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift
{
    public class ClientConfig
    {
        [Required]
        public string StorageMode { get; set; }  //mssql, redis, etc... 

        [Required]
        public string DBConnectionString { get; set; }
        public string DBAuthKey { get; set; }

        public string EncryptionKey { get; set; } //optional, if set, then parameters will be encrypted/decrypted automatically during storage

    }
}
