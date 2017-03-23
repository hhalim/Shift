using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading.Tasks;

namespace Shift
{
    public enum JobStatus
    {
        Running = 1,
        Completed = 2,
        Stopped = -1,
        Error = -99
    }

    public static class SerializerSettings
    {
        public static JsonSerializerSettings Settings = new JsonSerializerSettings() {
            TypeNameHandling = TypeNameHandling.Objects,
            TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple,
        };
    }

    public static class EncryptionSecret
    {
        public static string ParametersKey = ConfigurationManager.AppSettings["ShiftEncryptionParametersKey"];
    }

    public static class ThreadMode
    {
        public const string Thread = "thread";
        public const string Task = "task";
    }

    public static class StorageMode
    {
        public const string MSSql = "mssql";
        public const string Redis = "redis";
    }
}
