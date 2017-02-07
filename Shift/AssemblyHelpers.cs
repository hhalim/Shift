using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Shift
{
    public class AssemblyHelpers
    {

        //Load all assemblies in specified text list
        //Don't do anything if no file is included
        public static void LoadAssemblies(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                return;

            if (!File.Exists(filePath))
            {
                filePath = Helpers.NormalizePath(filePath); //try to use app domain base path
                if (!File.Exists(filePath)) //still nothing?
                    throw new Exception("File is not found. " + filePath);
            }

            var fileList = new System.IO.StreamReader(filePath);
            try
            {
                string linePath;

                while ((linePath = fileList.ReadLine()) != null)
                {
                    if (string.IsNullOrWhiteSpace(linePath) || linePath.StartsWith("#"))
                        continue;

                    string directory, filename;
                    var absPath = ConvertToFullPath(linePath, out directory, out filename);

                    //Assume it's a search through a pattern of files Common.* / States.* / etc.
                    var files = Directory.GetFiles(directory, filename);
                    if (files.Length == 0) //Still nothing!!!
                        throw new Exception("Error: Unable to find the assembly file(s): " + absPath);

                    foreach (var file in files)
                    {
                        var extension = Path.GetExtension(file);
                        if (extension != ".dll")
                            continue;
                        var assembly = Assembly.LoadFrom(file);

#if DEBUG
                        Debug.WriteLine("Loaded assembly \"" + absPath + "\": " + assembly.FullName);
#endif
                    }

                }
            }
            finally
            {
                fileList.Close();
            }
        }

        //remove duplicate back slashes for comparison
        public static string CleanPath(string path)
        {
            return Regex.Replace(path, @"\\+", @"\");
        }

        public static Assembly OnAssemblyResolve(object sender, ResolveEventArgs args)
        {
            /*
            Could not load file or assembly '<assembly name>.resources, Version=14.0.1.1, Culture=en-US, PublicKeyToken=null' or one of its dependencies.
            The system cannot find the file specified.

            This turns out to be a bug in OnAssemblyResolve unable to find assembly.
            Apparently the OnAssemblyResolve is being called for *.resources, which will fail, since the LoadFrom do not load *.resources only DLL
            http://stackoverflow.com/a/4977761
            */

            string[] fields = args.Name.Split(',');
            if (fields.Length >= 3)
            {
                var name = fields[0];
                var culture = fields[2];
                if (name.EndsWith(".resources") && !culture.EndsWith("neutral")) return null;
            }

            var asmList = AppDomain.CurrentDomain.GetAssemblies();
            var asm = (from item in asmList
                       where args.Name == item.GetName().FullName || args.Name == item.GetName().Name
                       select item).FirstOrDefault();

            if (asm == null)
                return null; //let the original code blows up, instead of throwing exception here.     

            return asm;
        }

        protected static bool IsFullPath(string path)
        {
            return !String.IsNullOrWhiteSpace(path)
                && path.IndexOfAny(System.IO.Path.GetInvalidPathChars().ToArray()) == -1
                && Path.IsPathRooted(path)
                && !Path.GetPathRoot(path).Equals(Path.DirectorySeparatorChar.ToString(), StringComparison.Ordinal);
        }

        protected static string ConvertToFullPath(string linePath, out string directory, out string filename)
        {
            var partDir = Path.GetDirectoryName(linePath); //get directory first, to avoid error when filename has * or ?
            if (string.IsNullOrWhiteSpace(partDir))
                partDir = "."; //use current working directory if it's empty

            filename = Path.GetFileName(linePath);

            var absPath = "";
            if (!IsFullPath(partDir))
            {
                //convert to absolute full path
                directory = Helpers.NormalizePath(partDir); 
                absPath = CleanPath(directory + @"\" + filename);
            }
            else
            {
                directory = partDir;
                absPath = linePath;
            }

            return absPath;
        }

    }
}
