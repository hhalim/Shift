using System;
using Xunit;

namespace Shift.UnitTest
{
     
    public class ClientConfigTest
    {
        [Fact]
        public void DBConnectionStringTest()
        {
            var config = new ClientConfig();
            config.DBConnectionString = "localhost:6400";

            Assert.Equal("localhost:6400", config.DBConnectionString);
        }

        [Fact]
        public void StorageModeRedisTest()
        {
            var config = new ClientConfig();
            config.StorageMode = "redis";

            Assert.Equal(StorageMode.Redis, config.StorageMode);
        }

        [Fact]
        public void StorageModeMssqlTest()
        {
            var config = new ClientConfig();
            config.StorageMode = "mssql";

            Assert.Equal(StorageMode.MSSql, config.StorageMode);
        }

        [Fact]
        public void StorageModeMongoDBTest()
        {
            var config = new ClientConfig();
            config.StorageMode = "mongo";

            Assert.Equal(StorageMode.MongoDB, config.StorageMode);
        }

        [Fact]
        public void UseCacheTest()
        {
            var config = new ClientConfig();
            config.UseCache = true;

            Assert.Equal(true, config.UseCache);
        }

        [Fact]
        public void UseCacheDefaultTest()
        {
            var config = new ClientConfig();

            Assert.Equal(false, config.UseCache);
        }

        [Fact]
        public void CacheConfigurationStringTest()
        {
            var config = new ClientConfig();
            config.CacheConfigurationString = "localhost:6370";

            Assert.Equal("localhost:6370", config.CacheConfigurationString);
        }

        [Fact]
        public void EncryptionKeyTest()
        {
            var config = new ClientConfig();
            config.EncryptionKey = "123$asdflouwrtasrthj";

            Assert.Equal("123$asdflouwrtasrthj", config.EncryptionKey);
        }



    }
}
