using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Shift.DataLayer
{
    //http://stackoverflow.com/a/32048306/2437862
    public static class RedisHelpers
    {
        //Serialize in Redis format:
        public static HashEntry[] ToHashEntries(this object obj)
        {
            PropertyInfo[] properties = obj.GetType().GetProperties();
            return properties
                .Where(x => x.GetValue(obj) != null) // <-- PREVENT NullReferenceException
                .Select(property => new HashEntry(property.Name, property.GetValue(obj)
                .ToString())).ToArray();
        }

        //Deserialize from Redis format
        public static T ConvertFromRedis<T>(this HashEntry[] hashEntries)
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            var obj = Activator.CreateInstance(typeof(T));
            foreach (var property in properties)
            {
                HashEntry entry = hashEntries.FirstOrDefault(g => g.Name.ToString().Equals(property.Name));
                if (entry.Equals(new HashEntry())) continue;
                if (!property.CanWrite) continue; //skip read only

                var type = property.PropertyType;
                var value = GetValue(type, entry.Value);
                property.SetValue(obj, value);
            }

            return (T)obj;
        }

        public static object GetValue(Type t, object value)
        {
            if (value == null || DBNull.Value.Equals(value) )
                return null;

            t = Nullable.GetUnderlyingType(t) ?? t;

            //Enum handling
            if (t.IsEnum)
            {
                var strValue = Convert.ToString(value);
                if (!string.IsNullOrWhiteSpace(strValue))
                {
                    try
                    {
                        return Enum.Parse(t, strValue);
                    }
                    catch
                    {
                        return null;
                    }
                }
                else
                    return null;
            }

            //DateTime handling
            if(t.IsAssignableFrom(typeof(DateTime)))
            {
                DateTime dtResult;
                if (DateTime.TryParse(Convert.ToString(value), out dtResult))
                    return dtResult;
                else
                    return null;
            }

            //GUID
            if (t.IsAssignableFrom(typeof(Guid)))
            {
                Guid outValue;
                if (!Guid.TryParse(Convert.ToString(value), out outValue))
                {
                    try
                    {
                        outValue = new Guid((byte[])value);
                    }
                    catch
                    {
                        outValue = Guid.Empty;
                    }
                }

                return outValue;
            }

            return Convert.ChangeType(value, t);
        }

        /// <summary>
        /// Create an array of strings from an array of values
        /// </summary>
        public static string[] ToStringArray(this RedisValue[] values)
        {
            if (values == null) return null;
            if (values.Length == 0) return null;
            return Array.ConvertAll(values, x => (string)x);
        }
    }
}
