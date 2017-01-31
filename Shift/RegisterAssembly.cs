using Autofac;
using Shift.DataLayer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift
{
    public static class RegisterAssembly
    {
        public static void RegisterTypes(ContainerBuilder builder, string dbConnectionString, bool useCache, string cacheConfigurationString, string encryptionKey)
        {
            var parameters = Helpers.GenerateNamedParameters(new Dictionary<string, object> { { "connectionString", dbConnectionString }, { "encryptionKey", encryptionKey } });
            if (useCache)
            {
                builder.RegisterType<DataLayer.Redis.Cache>().As<IJobCache>().WithParameter("configurationString", cacheConfigurationString);
                builder.RegisterType<JobDAL>().As<JobDAL>().WithParameters(parameters);
            }
            else
            {
                builder.RegisterType<JobDAL>().As<JobDAL>().UsingConstructor(typeof(string), typeof(string)).WithParameters(parameters);
            }
        }
    }
}
