using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Shift.UnitTest
{
    [TestClass]
    public class ClientConfigTest
    {
        [TestMethod]
        public void DBConnectionStringTest()
        {
            var config = new ClientConfig();
            config.DBConnectionString = "localhost:6400";

            Assert.AreEqual("localhost:6400", config.DBConnectionString);
        }

        [TestMethod]
        public void StorageModeRedisTest()
        {
            var config = new ClientConfig();
            config.StorageMode = "redis";

            Assert.AreEqual(StorageMode.Redis, config.StorageMode);
        }

        [TestMethod]
        public void StorageModeMssqlTest()
        {
            var config = new ClientConfig();
            config.StorageMode = "mssql";

            Assert.AreEqual(StorageMode.MSSql, config.StorageMode);
        }

        [TestMethod]
        public void StorageModeMongoDBTest()
        {
            var config = new ClientConfig();
            config.StorageMode = "mongo";

            Assert.AreEqual(StorageMode.MongoDB, config.StorageMode);
        }

        [TestMethod]
        public void UseCacheTest()
        {
            var config = new ClientConfig();
            config.UseCache = true;

            Assert.AreEqual(true, config.UseCache);
        }

        [TestMethod]
        public void UseCacheDefaultTest()
        {
            var config = new ClientConfig();

            Assert.AreEqual(false, config.UseCache);
        }

        [TestMethod]
        public void CacheConfigurationStringTest()
        {
            var config = new ClientConfig();
            config.CacheConfigurationString = "localhost:6370";

            Assert.AreEqual("localhost:6370", config.CacheConfigurationString);
        }

        [TestMethod]
        public void EncryptionKeyTest()
        {
            var config = new ClientConfig();
            config.EncryptionKey = "123$asdflouwrtasrthj";

            Assert.AreEqual("123$asdflouwrtasrthj", config.EncryptionKey);
        }



    }
}
