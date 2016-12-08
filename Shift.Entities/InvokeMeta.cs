using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Shift.Entities
{
    public class InvokeMeta
    {
        public string Type { get; set; }
        public string Method { get; set; }
        public string ParameterTypes { get; set; }

        public InvokeMeta()
        {
        }

        public InvokeMeta(Type type, MethodInfo methodInfo)
        {
            Type = type.AssemblyQualifiedName; //this embeds the version and culture, will require the same assembly DLLs to run serialized processes
            Method = methodInfo.Name;
            var prmArray = methodInfo.GetParameters().Select(x => x.ParameterType).ToArray();
            var prmTypes = JsonConvert.SerializeObject(prmArray, SerializerSettings.Settings);
            ParameterTypes = prmTypes;
        }

    }
}
