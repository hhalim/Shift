using System;
using System.Text;
using System.Collections.Generic;
using Xunit;
using Shift.DataLayer;

namespace Shift.UnitTest.DataLayer
{
     
    public class RedisHelperTest
    {
        private enum TestStatus
        {
            Running = 1,
            Completed = 2,
            Stopped = -1,
            Error = -99
        }

        #region DateTime
        [Fact]
        public void GetValueDateTimeValid()
        {
            Type type = typeof(DateTime?);
            string value = "1/1/2017 12:00:00";
            var expected = DateTime.Parse(value);

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetValueDateTimeIsNull()
        {
            Type type = typeof(DateTime?);
            string value = null;

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Null(actual);
        }

        [Fact]
        public void GetValueDateTimeIsNull2()
        {
            Type type = typeof(DateTime?);
            string value = "ABCDEF123";

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Null(actual);
        }
        #endregion

        #region GUID
        [Fact]
        public void GetValueGUIDIsValid()
        {
            Type type = typeof(Guid);
            string value = "c5adb885-04a3-4b9d-861d-13e4c7669d0b";
            var expected = Guid.Parse(value);

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetValueGUIDIsEmpty()
        {
            Type type = typeof(Guid);
            string value = "ABCDEF123";
            var expected = Guid.Empty;

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetValueGUIDIsEmpty2()
        {
            Type type = typeof(Guid);
            string value = "";
            var expected = Guid.Empty;

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Equal(expected, actual);
        }
        #endregion

        #region ENUM
        [Fact]
        public void GetValueEnumIsValid()
        {
            Type type = typeof(TestStatus);
            string value = "1";
            var expected = TestStatus.Running;

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetValueEnumIsNull()
        {
            Type type = typeof(TestStatus);
            string value = "";

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Null(actual);
        }

        [Fact]
        public void GetValueEnumIsNull2()
        {
            Type type = typeof(TestStatus);
            string value = "ABC#$";

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Null(actual);
        }
        #endregion

        #region Int32
        [Fact]
        public void GetValueInt32Valid()
        {
            Type type = typeof(int?);
            string value = "123";
            var expected = int.Parse(value);

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetValueInt32IsNull()
        {
            Type type = typeof(int?);
            string value = null;

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Null(actual);
        }

        [Fact]
        public void GetValueInt32IsNull2()
        {
            Type type = typeof(int?);
            string value = "ABC123";

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Null(actual);
        }
        #endregion

        #region Int64
        [Fact]
        public void GetValueInt64Valid()
        {
            Type type = typeof(long?);
            string value = "123";
            var expected = long.Parse(value);

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GetValueInt64IsNull()
        {
            Type type = typeof(long?);
            string value = null;

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Null(actual);
        }

        [Fact]
        public void GetValueInt64IsNull2()
        {
            Type type = typeof(long?);
            string value = "ABC123";

            var actual = RedisHelpers.GetValue(type, value);

            Assert.Null(actual);
        }
        #endregion

    }

}
