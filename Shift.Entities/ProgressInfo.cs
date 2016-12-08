using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift.Entities
{
    public class ProgressInfo
    {
        public int Percent { get; set; }
        public string Note { get; set; }
        public string Data { get; set; }

        //List of files to be INSERTED in the JobResult, not using byte so not taking so much memory space until time to upload files
        //ONLY support INSERT, clear it after insertion for NO duplicate
        public List<FileInfo> FileInfoList {get; set;}
    }

}
