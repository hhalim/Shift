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
        public static int LoadAssemblies(string folder, string filePath)
        {
            if(!string.IsNullOrWhiteSpace(folder))
            {
                return LoadAssembliesFromFolder(folder);
            }
            else
            {
                return LoadAssembliesFromFile(filePath);
            }
        }

        protected static int LoadAssembliesFromFolder(string folder)
        {
            var count = 0;
            if (string.IsNullOrWhiteSpace(folder))
                return count;

            string directory, filename;
            var absPath = ConvertToFullPath(folder, out directory, out filename);
            if(string.IsNullOrWhiteSpace(filename))
            {
                filename = "*.dll"; //assume loading all dlls
            }

            //Assume it's a search through a pattern of files Common.* / States.* / etc.
            var files = Directory.GetFiles(directory, filename);
            if (files.Length == 0)
            {
                throw new Exception("Error: Unable to find the assembly file(s) in folder: " + absPath);
            }

            foreach (var file in files)
            {
                var extension = Path.GetExtension(file);
                if (extension != ".dll")
                    continue;
                var assembly = Assembly.LoadFrom(file);
                count++;

#if DEBUG
                Debug.WriteLine("Loaded assembly \"" + absPath + "\": " + assembly.FullName);
#endif
            }

            return count;
        }

        protected static int LoadAssembliesFromFile(string filePath)
        {
            var count = 0;
            if (string.IsNullOrWhiteSpace(filePath))
                return count;


            if (!File.Exists(filePath))
            {
                filePath = Helpers.NormalizePath(filePath); //try to use app domain base path
                if (!File.Exists(filePath)) //still nothing?
                {
                    throw new Exception("File is not found. " + filePath);
                }
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
                    if (files.Length == 0)
                    {
                        throw new Exception("Error: Unable to find the assembly file(s): " + absPath);
                    }

                    foreach (var file in files)
                    {
                        var extension = Path.GetExtension(file);
                        if (extension != ".dll")
                            continue;
                        var assembly = Assembly.LoadFrom(file);
                        count++;

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

            return count;
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
            var fullName = args.Name; 
            var asm = (from item in asmList
                       where fullName == item.GetName().FullName || fullName == item.GetName().Name
                       select item).FirstOrDefault();

            //try to use simpler name, version may not match, but this allows jobs to still run with different version
            if (asm== null)
            {
                var shortName = args.Name.Split(',').FirstOrDefault(); 
                asm = (from item in asmList
                        where shortName == item.GetName().Name
                        select item).FirstOrDefault();
            }

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
            directory = "";
            filename = "";

            if (string.IsNullOrWhiteSpace(linePath))
                return null;

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

        protected static string ConvertToFullPath(string folder)
        {
            if (string.IsNullOrWhiteSpace(folder))
                return null;

            var absPath = "";
            if (!IsFullPath(folder))
            {
                //convert to absolute full path
                absPath = Helpers.NormalizePath(folder);
            }
            else
            {
                absPath = folder;
            }

            return absPath;
        }

    }
}
