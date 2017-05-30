using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;

namespace Shift.UnitTest
{
    [TestClass]
    public class PIDGeneratorTest
    {

        [TestMethod]
        public void GenerateProcessIDNotUseExisting()
        {
            var newPID = ProcessIDGenerator.Generate(false);

            Guid outGuid;
            var isValid = Guid.TryParse(newPID, out outGuid);

            Assert.IsTrue(isValid);
            Assert.IsNotNull(outGuid);
            Assert.AreEqual(newPID, outGuid.ToString("N").ToUpper());
        }


        [TestMethod]
        public void GenerateProcessIDUseExisting()
        {
            ProcessIDGenerator.DeleteExistingProcessID(); //ensures generating new PID
            var newPID = ProcessIDGenerator.Generate(true);
            var existingPID = ProcessIDGenerator.Generate(true); //should return the existing PID

            Assert.IsNotNull(newPID);
            Assert.IsNotNull(existingPID);
            Assert.AreEqual(newPID, existingPID);
        }

        [TestMethod]
        public async Task GenerateAsyncProcessIDNotUseExisting()
        {
            var newPID = await ProcessIDGenerator.GenerateAsync(false);

            Guid outGuid;
            var isValid = Guid.TryParse(newPID, out outGuid);

            Assert.IsTrue(isValid);
            Assert.IsNotNull(outGuid);
            Assert.AreEqual(newPID, outGuid.ToString("N").ToUpper());
        }


        [TestMethod]
        public async Task GenerateAsyncProcessIDUseExisting()
        {
            ProcessIDGenerator.DeleteExistingProcessID(); //ensures generating new PID
            var newPID = await ProcessIDGenerator.GenerateAsync(true);
            var existingPID = await ProcessIDGenerator.GenerateAsync(true); //should return the existing PID

            Assert.IsNotNull(newPID);
            Assert.IsNotNull(existingPID);
            Assert.AreEqual(newPID, existingPID);
        }


    }
}
