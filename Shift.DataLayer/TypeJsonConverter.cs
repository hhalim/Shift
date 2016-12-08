using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ProcessJobs.DataLayer
{
    public class TypeJsonConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return typeof(Type).GetTypeInfo().IsAssignableFrom(objectType.GetTypeInfo());
        }

        /// <summary>Parses the json to the specified type.</summary>
        /// <param name="reader">Newtonsoft.Json.JsonReader</param>
        /// <param name="objectType">Target type.</param>
        /// <param name="existingValue">Ignored</param>
        /// <param name="serializer">Newtonsoft.Json.JsonSerializer to use.</param>
        /// <returns>Deserialized Object</returns>
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
                return null;

            var jObject = JObject.Load(reader);

            // Create target object based on JObject
            Type targetType = GetTypeFromDomain(reader.Value.ToString());
            var target = Activator.CreateInstance(targetType);
            serializer.Populate(jObject.CreateReader(), target);

            return target;
        }

        /// <summary>Serializes to the specified type</summary>
        /// <param name="writer">Newtonsoft.Json.JsonWriter</param>
        /// <param name="value">Object to serialize.</param>
        /// <param name="serializer">Newtonsoft.Json.JsonSerializer to use.</param>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public static Type GetTypeFromDomain(string assemblyQualifiedName)
        {
            // Throws exception if type was not found
            // Returns the assembly of the type by enumerating loaded assemblies in the app domain            
            // http://stackoverflow.com/questions/11430654/is-it-possible-to-use-type-gettype-with-a-dynamically-loaded-assembly
            return Type.GetType(
            assemblyQualifiedName,
                (name) =>
                {
                    return AppDomain.CurrentDomain.GetAssemblies().FirstOrDefault(z => string.Equals(z.FullName, name.FullName, StringComparison.OrdinalIgnoreCase));
                },
                null,
                true);
        }

    }


}
