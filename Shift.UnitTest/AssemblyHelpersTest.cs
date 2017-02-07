using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shift;
using System.Text.RegularExpressions;
using System.IO;
using System.Reflection;

namespace Shift.UnitTest
{
    [TestClass]
    public class AssemblyHelpersTest : AssemblyHelpers
    {
        [TestMethod]
        public void GetFullPathTest1()
        {
            var currentDir = Directory.GetCurrentDirectory();
            var linePath = @"C:\mydll\test.dll"; //full path

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.AreEqual(filename, "test.dll");
            Assert.AreEqual(directory, @"C:\mydll");
            Assert.AreEqual(newLinePath, linePath); //linePath is not affected, should be the same as newLinePath
        }

        //Relative path
        [TestMethod]
        public void GetFullPathTest2()
        {
            var currentDir = Directory.GetCurrentDirectory();
            var linePath = @"test.dll"; 

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.AreEqual(filename, "test.dll");
            Assert.AreEqual(directory, currentDir);
            Assert.AreEqual(newLinePath, CleanPath(currentDir + "\\" + linePath));
        }

        [TestMethod]
        public void GetFullPathTest3()
        {
            var rootPath = Path.GetFullPath("\\");  // C:\
            var linePath = @"\test.dll"; 

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.AreEqual(filename, "test.dll");
            Assert.AreEqual(directory, rootPath);
            Assert.AreEqual(newLinePath, CleanPath(rootPath + @"\test.dll" )); // C:\test.dll
        }

        [TestMethod]
        public void GetFullPathTest4()
        {
            var currentDir = Directory.GetCurrentDirectory();
            var rootPath = Path.GetFullPath("\\");  // C:\
            var linePath = @".\test.dll"; 

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.AreEqual(filename, "test.dll");
            Assert.AreEqual(directory, currentDir);
            Assert.AreEqual(newLinePath, CleanPath(currentDir + @"\test.dll"));
        }

        [TestMethod]
        public void GetFullPathTest5()
        {
            var upOneDir = Path.GetFullPath("..");  //one folder up
            var linePath = @"..\test.dll"; 

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.AreEqual(filename, "test.dll");
            Assert.AreEqual(directory, CleanPath(upOneDir));
            Assert.AreEqual(newLinePath, CleanPath(upOneDir + @"\test.dll"));
        }

        [TestMethod]
        public void GetFullPathTest6()
        {
            var upTwoDir = Path.GetFullPath(@"..\.."); //two folder up
            var linePath = @"..\..\test.dll";

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.AreEqual(filename, "test.dll");
            Assert.AreEqual(directory, CleanPath(upTwoDir));
            Assert.AreEqual(newLinePath, CleanPath(upTwoDir + @"\test.dll"));
        }

        [TestMethod]
        public void GetFullPathTest7()
        {
            var currentDir = Directory.GetCurrentDirectory();
            var linePath = @"test*.dll";

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.AreEqual(filename, "test*.dll");
            Assert.AreEqual(directory, currentDir);
            Assert.AreEqual(newLinePath, CleanPath(currentDir + "\\" + linePath));
        }

        [TestMethod]
        public void GetFullPathTest8()
        {
            var currentDir = Directory.GetCurrentDirectory();
            var linePath = @"mydll\test*";

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.AreEqual(filename, "test*");
            Assert.AreEqual(directory, CleanPath(currentDir + @"\mydll"));
            Assert.AreEqual(newLinePath, CleanPath(currentDir + "\\" + linePath));
        }
    }
}
