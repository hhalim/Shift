using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift.DataLayer
{
    public class RedisCache
    {
        private static string _configuration;

        public RedisCache(string configuration)
        {
            if (string.IsNullOrWhiteSpace(configuration))
                throw new ArgumentNullException("configuration");
            _configuration = configuration;
        }

        public IDatabase Database
        {
            get 
            {
                return Connection.GetDatabase();
            }
        }

        private static readonly Lazy<ConnectionMultiplexer> LazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(_configuration));
        public static ConnectionMultiplexer Connection
        {
            get
            {
                return LazyConnection.Value;
            }
        }

    }
}
