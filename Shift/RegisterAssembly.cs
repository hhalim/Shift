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
        public static void RegisterTypes(ContainerBuilder builder, Options options)
        {
            var parameters = Helpers.GenerateNamedParameters(new Dictionary<string, object> { { "connectionString", options.DBConnectionString }, { "encryptionKey", options.EncryptionKey } });
            if (options.UseCache)
            {
                builder.RegisterType<DataLayer.Redis.Cache>().As<IJobCache>().WithParameter("configurationString", options.CacheConfigurationString);
                builder.RegisterType<JobDAL>().As<JobDAL>().WithParameters(parameters);
            }
            else
            {
                builder.RegisterType<JobDAL>().As<JobDAL>().UsingConstructor(typeof(string), typeof(string)).WithParameters(parameters);
            }
        }
    }
}
