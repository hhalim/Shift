using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Runtime.Serialization;

namespace Shift
{
    public static class Helpers
    {

        private static IEnumerable<MethodInfo> GetAllMethods(Type type)
        {
            var methods = new List<MethodInfo>(type.GetMethods());

            if (type.IsInterface)
            {
                methods.AddRange(type.GetInterfaces().SelectMany(x => x.GetMethods()));
            }

            return methods;
        }

        public static MethodInfo GetNonOpenMatchingMethod(Type type, string name, Type[] parameterTypes)
        {
            var methodCandidates = GetAllMethods(type);

            foreach (var methodCandidate in methodCandidates)
            {
                if (!methodCandidate.Name.Equals(name, StringComparison.Ordinal))
                {
                    continue;
                }

                var parameters = methodCandidate.GetParameters();
                if (parameters.Length != parameterTypes.Length)
                {
                    continue;
                }

                var parameterTypesMatched = true;
                var genericArguments = new List<Type>();

                // Determining whether we can use this method candidate with
                // current parameter types.
                for (var i = 0; i < parameters.Length; i++)
                {
                    var parameter = parameters[i];
                    var parameterType = parameter.ParameterType;
                    var actualType = parameterTypes[i];

                    // Skipping generic parameters as we can use actual type.
                    if (parameterType.IsGenericParameter)
                    {
                        genericArguments.Add(actualType);
                        continue;
                    }

                    // Skipping non-generic parameters of assignable types.
                    if (parameterType.IsAssignableFrom(actualType)) continue;

                    parameterTypesMatched = false;
                    break;
                }

                if (!parameterTypesMatched) continue;

                // Return first found method candidate with matching parameters.
                return methodCandidate.ContainsGenericParameters
                    ? methodCandidate.MakeGenericMethod(genericArguments.ToArray())
                    : methodCandidate;
            }

            return null;
        }

        /*
        * Instead of Activator.CreateInstance
        * http://stackoverflow.com/questions/6582259/fast-creation-of-objects-instead-of-activator-createinstancetype/16162809#16162809
        */
        public static object CreateInstance(Type type)
        {
            if (type == typeof(string))
                return string.Empty;

            if (type.HasDefaultConstructor())
                return Activator.CreateInstance(type);

            return FormatterServices.GetUninitializedObject(type); //Class with no parameterless constructor
        }

        public static bool HasDefaultConstructor(this Type t)
        {
            return t.IsValueType || t.GetConstructor(Type.EmptyTypes) != null;
        }
    }

}
