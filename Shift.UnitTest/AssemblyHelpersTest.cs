using System;
using Xunit;
using Shift;
using System.Text.RegularExpressions;
using System.IO;
using System.Reflection;

namespace Shift.UnitTest
{
     
    public class AssemblyHelpersTest : AssemblyHelpers
    {
        [Fact]
        public void GetFullPathTest1()
        {
            var linePath = @"C:\mydll\test.dll"; //full path

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.Equal(filename, "test.dll");
            Assert.Equal(directory, @"C:\mydll");
            Assert.Equal(newLinePath, linePath); //linePath is not affected, should be the same as newLinePath
        }

        //Relative path
        [Fact]
        public void GetFullPathTest2()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory; 
            var linePath = @"test.dll"; 

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.Equal(filename, "test.dll");
            Assert.Equal(directory, baseDir);
            Assert.Equal(newLinePath, CleanPath(baseDir + "\\" + linePath));
        }

        [Fact]
        public void GetFullPathTest3()
        {
            var rootPath = Path.GetFullPath("\\");  // C:\
            var linePath = @"\test.dll"; 

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.Equal(filename, "test.dll");
            Assert.Equal(directory, rootPath);
            Assert.Equal(newLinePath, CleanPath(rootPath + @"\test.dll" )); // C:\test.dll
        }

        [Fact]
        public void GetFullPathTest4()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var rootPath = Path.GetFullPath("\\");  // C:\
            var linePath = @".\test.dll"; 

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.Equal(filename, "test.dll");
            Assert.Equal(directory, baseDir);
            Assert.Equal(newLinePath, CleanPath(baseDir + @"\test.dll"));
        }

        [Fact]
        public void GetFullPathTest5()
        {
            var upOneDir = Path.GetFullPath("..");  //one folder up
            var linePath = @"..\test.dll"; 

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.Equal(filename, "test.dll");
            Assert.Equal(directory, CleanPath(upOneDir));
            Assert.Equal(newLinePath, CleanPath(upOneDir + @"\test.dll"));
        }

        [Fact]
        public void GetFullPathTest6()
        {
            var upTwoDir = Path.GetFullPath(@"..\.."); //two folder up
            var linePath = @"..\..\test.dll";

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.Equal(filename, "test.dll");
            Assert.Equal(directory, CleanPath(upTwoDir));
            Assert.Equal(newLinePath, CleanPath(upTwoDir + @"\test.dll"));
        }

        [Fact]
        public void GetFullPathTest7()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var linePath = @"test*.dll";

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.Equal(filename, "test*.dll");
            Assert.Equal(directory, baseDir);
            Assert.Equal(newLinePath, CleanPath(baseDir + "\\" + linePath));
        }

        [Fact]
        public void GetFullPathTest8()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var linePath = @"mydll\test*";

            string directory, filename;
            var newLinePath = ConvertToFullPath(linePath, out directory, out filename);

            Assert.Equal(filename, "test*");
            Assert.Equal(directory, CleanPath(baseDir + @"\mydll"));
            Assert.Equal(newLinePath, CleanPath(baseDir + "\\" + linePath));
        }

        //Test folder only, no filename
        [Fact]
        public void GetFullPathTest9()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var folder = "mydll\\";

            var newPath = ConvertToFullPath(folder);

            Assert.Equal(newPath, CleanPath(baseDir + "\\" + folder));
        }

        [Fact]
        public void GetFullPathTest10()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var folder = "libs\\mydll";

            var newPath = ConvertToFullPath(folder);

            Assert.Equal(newPath, CleanPath(baseDir + "\\" + folder));
        }

        [Fact]
        public void GetFullPathTest11()
        {
            var rootPath = Path.GetFullPath("\\");  // C:\
            var folder = @"\mydll";

            var newPath = ConvertToFullPath(folder);

            Assert.Equal(newPath, CleanPath(rootPath + "\\" + folder)); // C:\mydll
        }

        [Fact]
        public void GetFullPathTest12()
        {
            var upOneDir = Path.GetFullPath("..");  //one folder up
            var folder = @"..\mydll";

            var newPath = ConvertToFullPath(folder);

            Assert.Equal(newPath, CleanPath(upOneDir + @"\mydll"));
        }

        [Fact]
        public void GetFullPathNullTest()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;

            string directory, filename;
            var newLinePath = ConvertToFullPath(null, out directory, out filename);
            Assert.Null(newLinePath);

            newLinePath = ConvertToFullPath("", out directory, out filename);
            Assert.Null(newLinePath);

            newLinePath = ConvertToFullPath("   ", out directory, out filename);
            Assert.Null(newLinePath);
        }

        [Fact]
        public void GetFullPathFolderNull()
        {
            var newPath = ConvertToFullPath(null);
            Assert.Null(newPath);
        }

        [Fact]
        public void GetFullPathAbsolutePathTest()
        {
            var folder = @"C:\mydll\"; //full path
            var newPath = ConvertToFullPath(folder);

            Assert.Equal(@"C:\mydll\", newPath);
        }

        [Fact]
        public void LoadAssembliesFromFolderNotFoundTest()
        {
            var folder = @"C:\myShiftTestDll\"; //full path
            var ex = Assert.Throws<DirectoryNotFoundException>(() => LoadAssemblies(folder, null));
        }
    }
}
