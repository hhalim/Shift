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
            var entries = new List<HashEntry>();
            foreach(var item in properties)
            {
                var value = item.GetValue(obj);
                entries.Add(new HashEntry(item.Name, value == null || DBNull.Value.Equals(value) ? "" : Convert.ToString(value)));
            }

            return entries.ToArray();
        }

        //Deserialize from Redis format
        public static T ConvertFromRedis<T>(this HashEntry[] hashEntries) 
        {
            if (!hashEntries.Any())
                return default(T); //null for nullable value, 0 for int, '\0' for char etc

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
            //is type nullable?
            if(Nullable.GetUnderlyingType(t) != null)
            {
                if (value == null || DBNull.Value.Equals(value) )
                    return null;
            }

            //Int32/64 does not understand value == null?
            if(t.IsAssignableFrom(typeof(Int32)) && Nullable.GetUnderlyingType(t) != null)
            {
                Int32 intResult;
                if (Int32.TryParse(Convert.ToString(value), out intResult))
                    return intResult;
                else
                    return null;
            }

            if (t.IsAssignableFrom(typeof(Int64)) && Nullable.GetUnderlyingType(t) != null)
            {
                Int64 intResult;
                if (Int64.TryParse(Convert.ToString(value), out intResult))
                    return intResult;
                else
                    return null;
            }

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
