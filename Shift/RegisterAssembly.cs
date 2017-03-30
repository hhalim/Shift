using Autofac;
using Shift.DataLayer;
using Shift.Entities;

using System;
using System.Collections.Generic;

namespace Shift
{
    public static class RegisterAssembly
    {
        public static void RegisterTypes(ContainerBuilder builder, string storageMode, string dbConnectionString, bool useCache, string cacheConfigurationString, string encryptionKey)
        {
            var parameters = Helpers.GenerateNamedParameters(new Dictionary<string, object> { { "connectionString", dbConnectionString }, { "encryptionKey", encryptionKey} });
            switch (storageMode.ToLower())
            {
                case StorageMode.MSSql:
                    if (useCache)
                    {
                        builder.RegisterType<Cache.Redis.JobCache>().As<IJobCache>().WithParameter("configurationString", cacheConfigurationString);
                        builder.RegisterType<JobDALSql>().As<IJobDAL>().WithParameters(parameters);
                    }
                    else
                    {
                        builder.RegisterType<JobDALSql>().As<IJobDAL>().UsingConstructor(typeof(string), typeof(string)).WithParameters(parameters);
                    }
                    break;
                case StorageMode.Redis:
                    //builder.RegisterType<JobDALRedis>().As<IJobDAL>().UsingConstructor(typeof(string), typeof(string)).WithParameters(parameters);
                    break;
                case StorageMode.MongoDB:
                    //builder.RegisterType<JobDALMongo>().As<IJobDAL>().UsingConstructor(typeof(string), typeof(string)).WithParameters(parameters);
                    break;
                default:
                    throw new ArgumentNullException("The storage mode configuration must not be empty or null.");
                    break;
            }
        }

    }
}
