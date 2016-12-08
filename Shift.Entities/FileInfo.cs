using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shift.Entities
{
    public class FileInfo
    {
        public string ExternalID { get; set; }
        public string FullPath { get; set; } //fullpath including filename eg: C:\FLS\PrintFiles\12345\testfile1.pdf
        public string FileName { get; set; }
        public string ContentType { get; set; }
        public bool DeleteAfterUpload { get; set; } = false;
    }
}
