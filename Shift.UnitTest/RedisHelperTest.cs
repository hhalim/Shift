using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shift.DataLayer;

namespace Shift.UnitTest.DataLayer
{
    [TestClass]
    public class RedisHelperTest
    {
        private enum TestStatus
        {
            Running = 1,
            Completed = 2,
            Stopped = -1,
            Error = -99
        }

        [TestMethod]
        public void GetValueDateTimeValid()
        {
            Type type = typeof(DateTime?);
            string value = "1/1/2017 12:00:00";
            var expected = DateTime.Parse(value);

            var actual = RedisHelpers.GetValue(type, value);

            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void GetValueDateTimeIsNull()
        {
            Type type = typeof(DateTime?);
            string value = null;

            var actual = RedisHelpers.GetValue(type, value);

            Assert.IsNull(actual);
        }

        [TestMethod]
        public void GetValueDateTimeIsNull2()
        {
            Type type = typeof(DateTime?);
            string value = "ABCDEF123";

            var actual = RedisHelpers.GetValue(type, value);

            Assert.IsNull(actual);
        }

        [TestMethod]
        public void GetValueGUIDIsValid()
        {
            Type type = typeof(Guid);
            string value = "c5adb885-04a3-4b9d-861d-13e4c7669d0b";
            var expected = Guid.Parse(value);

            var actual = RedisHelpers.GetValue(type, value);

            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void GetValueGUIDIsEmpty()
        {
            Type type = typeof(Guid);
            string value = "ABCDEF123";
            var expected = Guid.Empty;

            var actual = RedisHelpers.GetValue(type, value);

            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void GetValueGUIDIsEmpty2()
        {
            Type type = typeof(Guid);
            string value = "";
            var expected = Guid.Empty;

            var actual = RedisHelpers.GetValue(type, value);

            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void GetValueEnumIsValid()
        {
            Type type = typeof(TestStatus);
            string value = "1";
            var expected = TestStatus.Running;

            var actual = RedisHelpers.GetValue(type, value);

            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void GetValueEnumIsNull()
        {
            Type type = typeof(TestStatus);
            string value = "";

            var actual = RedisHelpers.GetValue(type, value);

            Assert.IsNull(actual);
        }

        [TestMethod]
        public void GetValueEnumIsNull2()
        {
            Type type = typeof(TestStatus);
            string value = "ABC#$";

            var actual = RedisHelpers.GetValue(type, value);

            Assert.IsNull(actual);
        }
    }

}
