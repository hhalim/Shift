using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Globalization;

using Shift.Common;
using Newtonsoft.Json;
using System.ComponentModel;
using Shift.Entities;
using System.Threading;

namespace Shift.DataLayer
{
    public class DALHelpers
    {
        public static void Validate(
             Type type,
             string typeParameterName,
             MethodInfo method,
             string methodParameterName,
             int argumentCount,
             string argumentParameterName
            )
        {
            if (!method.IsPublic)
            {
                throw new NotSupportedException("Only public methods can be invoked.");
            }

            if (method.ContainsGenericParameters)
            {
                throw new NotSupportedException("Job method can not contain unassigned generic type parameters.");
            }

            if (method.DeclaringType == null)
            {
                throw new NotSupportedException("Global methods are not supported. Use class methods instead.");
            }

            if (!method.DeclaringType.IsAssignableFrom(type))
            {
                throw new ArgumentException(
                    String.Format("The type `{0}` must be derived from the `{1}` type.", method.DeclaringType, type),
                    typeParameterName);
            }

            if (typeof(Task).IsAssignableFrom(method.ReturnType))
            {
                throw new NotSupportedException("Async methods (Task) are not supported . Please make them synchronous.");
            }

            var parameters = method.GetParameters();

            if (parameters.Length != argumentCount)
            {
                throw new ArgumentException("Argument count must be equal to method parameter count.", argumentParameterName);
            }

            foreach (var parameter in parameters)
            {
                // There is no guarantee that specified method will be invoked
                // in the same process. Therefore, output parameters and parameters
                // passed by reference are not supported.

                if (parameter.IsOut)
                {
                    throw new NotSupportedException("Output parameters are not supported: there is no guarantee that specified method will be invoked inside the same process.");
                }

                if (parameter.ParameterType.IsByRef)
                {
                    throw new NotSupportedException("Parameters, passed by reference, are not supported: there is no guarantee that specified method will be invoked inside the same process.");
                }
            }
        }

        public static object GetExpressionValue(Expression expression)
        {
            var constantExpression = expression as ConstantExpression;

            return constantExpression != null
                ? constantExpression.Value
                : CachedExpressionCompiler.Evaluate(expression);
        }

        internal static string[] SerializeArguments(IReadOnlyCollection<object> arguments)
        {
            var serializedArguments = new List<string>(arguments.Count);
            foreach (var argument in arguments)
            {
                string value = null;

                if (argument != null)
                {
                    if (argument is DateTime)
                    {
                        value = ((DateTime)argument).ToString("o", CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        value = JsonConvert.SerializeObject(argument, SerializerSettings.Settings);
                    }
                }

                serializedArguments.Add(value);
            }

            return serializedArguments.ToArray();
        }

        public static object[] DeserializeArguments(CancellationToken token, IProgress<ProgressInfo> progress, MethodInfo methodInfo, string rawArguments)
        {
            var arguments = JsonConvert.DeserializeObject<string[]>(rawArguments, SerializerSettings.Settings);
            if (arguments.Length == 0)
                return null;

            var parameters = methodInfo.GetParameters();
            var result = new List<object>(arguments.Length);

            for (var i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i];
                var argument = arguments[i];

                object value = null;
                if (parameter.ParameterType.FullName.Contains("System.IProgress")) {
                    value = progress;
                }
                else if(parameter.ParameterType.FullName.Contains("System.Threading.CancellationToken")) 
                {
                    value = token;
                }
                else
                {
                    value = DeserializeArgument(argument, parameter.ParameterType);
                };

                result.Add(value);
            }

            return result.ToArray();
        }

        public static object DeserializeArgument(string argument, Type type)
        {
            object value;
            try
            {
                value = argument != null
                    ? JsonConvert.DeserializeObject(argument, type, SerializerSettings.Settings)
                    : null;
            }
            catch (Exception jsonException)
            {
                if (type == typeof(object))
                {
                    // Special case for handling object types, because string can not be converted to object type.
                    value = argument;
                }
                else
                {
                    try
                    {
                        var converter = TypeDescriptor.GetConverter(type);
                        value = converter.ConvertFromInvariantString(argument);
                    }
                    catch (Exception)
                    {
                        throw jsonException;
                    }
                }
            }
            return value;
        }

    }
}
