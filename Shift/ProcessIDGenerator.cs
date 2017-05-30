using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

namespace Shift
{
    public class ProcessIDGenerator
    {
        static readonly string PIDFileName = "shift_pid.txt";
        static readonly string CurrentPath = "";

        static ProcessIDGenerator()
        {
            var currentDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().GetName().CodeBase);
            currentDir = currentDir.Replace("file:\\", ""); 
            CurrentPath = Path.Combine(new string[] { currentDir, PIDFileName });
        }

        /// <summary>
        /// Generates new Process ID file.
        /// If the useExisting is set to true, attempts to return the previously generated Process ID if exists or generates a new one.
        /// </summary>
        /// <param name="useExisting">true/false to use the existing Process ID</param>
        /// <returns>Process ID GUID</returns>
        public static string Generate(bool useExisting)
        {
            return GenerateAsync(useExisting).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Generates new Process ID file asynchronously.
        /// If the useExisting is set to true, attempts to return the previously generated Process ID if exists or generates a new one.
        /// </summary>
        /// <param name="useExisting">true/false to use the existing Process ID</param>
        /// <returns>Process ID GUID</returns>
        public static async Task<string> GenerateAsync(bool useExisting)
        {
            var existingPID = useExisting ? await ReadProcessID() : null;
            if(string.IsNullOrWhiteSpace(existingPID))
            {
                var newPID = Guid.NewGuid().ToString("N").ToUpper();
                if(useExisting)
                {
                    //write for later
                    await WriteProcessID(newPID);
                }

                return newPID;
            }
            else
            {
                return existingPID;
            }
        }

        protected static async Task<string> ReadProcessID()
        {
            if (!(new FileInfo(CurrentPath).Exists))
                return null;

            var line = "";
            using (StreamReader reader = new StreamReader(CurrentPath))
            {
                line = await reader.ReadLineAsync();
            }

            if (!string.IsNullOrWhiteSpace(line))
                return line;

            return null;
        }

        protected static async Task WriteProcessID(string newPID)
        {
            using (StreamWriter writer = new StreamWriter(CurrentPath, false))
            {
                await writer.WriteLineAsync(newPID);
            }
        }

        /// <summary>
        /// Deletes the existing Process ID file.
        /// </summary>
        /// <returns></returns>
        public static void  DeleteExistingProcessID()
        {
            var fileInfo = new FileInfo(CurrentPath);
            if (fileInfo.Exists)
                fileInfo.Delete();

            return;
        }
    }
}
