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
using System.Runtime.CompilerServices;

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

            if (!method.DeclaringType.GetTypeInfo().IsAssignableFrom(type.GetTypeInfo()))
            {
                throw new ArgumentException(String.Format("The type `{0}` must be derived from the `{1}` type.", method.DeclaringType, type), typeParameterName);
            }

            if (method.ReturnType == typeof(void) && method.GetCustomAttribute<AsyncStateMachineAttribute>() != null)
            {
                throw new NotSupportedException("Async void methods are not supported. Use async Task instead.");
            }

            var parameters = method.GetParameters();

            if (parameters.Length != argumentCount)
            {
                throw new ArgumentException("Argument count must be equal to method parameter count.", argumentParameterName);
            }

            foreach (var parameter in parameters)
            {
                if (parameter.ParameterType.IsByRef)
                {
                    throw new NotSupportedException("Passed by reference parameters, are not supported: no guarantee that method will be invoked in the same process.");
                }

                if (parameter.IsOut)
                {
                    throw new NotSupportedException("Output parameters (out) are not supported: no guarantee that method will be invoked in the same process.");
                }

                var parameterTypeInfo = parameter.ParameterType.GetTypeInfo();

                if (parameterTypeInfo.IsSubclassOf(typeof(Delegate)) || parameterTypeInfo.IsSubclassOf(typeof(Expression)))
                {
                    throw new NotSupportedException("Anonymous functions, delegates and lambda expressions aren't supported in job method parameters.");
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
                    else if (argument is CancellationToken 
                        || argument is PauseToken 
                        || argument is IProgress<ProgressInfo>)
                    {
                        //These types will be replaced during invocation with the real objects
                        value = null;
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

        public static object[] DeserializeArguments(CancellationToken cancelToken, PauseToken pauseToken, IProgress<ProgressInfo> progress, MethodInfo methodInfo, string rawArguments)
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
                if (parameter.ParameterType.FullName.ToUpper().Contains("System.IProgress".ToUpper()))
                {
                    value = progress;
                }
                else if(parameter.ParameterType.FullName.ToUpper().Contains("System.Threading.CancellationToken".ToUpper())) 
                {
                    value = cancelToken;
                }
                else if (parameter.ParameterType.FullName.ToUpper().Contains("Shift.Entities.PauseToken".ToUpper()))
                {
                    value = pauseToken;
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

                        if (converter.GetType() == typeof(ReferenceConverter))
                        {
                            throw;
                        }

                        value = converter.ConvertFromInvariantString(argument);
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
            }
            return value;
        }

        public static Job CreateJobFromExpression(string encryptionKey, string appID, string userID, string jobType, string jobName, LambdaExpression methodCall)
        {
            if (methodCall == null)
                throw new ArgumentNullException("methodCall");

            var callExpression = methodCall.Body as MethodCallExpression;
            if (callExpression == null)
            {
                throw new ArgumentException("Expression body must be 'MethodCallExpression' type.", "methodCall");
            }

            var type = callExpression.Method.DeclaringType;
            var methodInfo = callExpression.Method;
            if (callExpression.Object != null)
            {
                var objectValue = GetExpressionValue(callExpression.Object);
                if (objectValue == null)
                {
                    throw new InvalidOperationException("Expression object should be not null.");
                }

                type = objectValue.GetType();

                methodInfo = type.GetNonOpenMatchingMethod(callExpression.Method.Name, callExpression.Method.GetParameters().Select(x => x.ParameterType).ToArray());
            }

            var args = callExpression.Arguments.Select(GetExpressionValue).ToArray();

            if (type == null) throw new ArgumentNullException("type");
            if (methodInfo == null) throw new ArgumentNullException("method");
            if (args == null) throw new ArgumentNullException("args");

            Validate(type, "type", methodInfo, "method", args.Length, "args");

            var invokeMeta = new InvokeMeta(type, methodInfo);

            //Save InvokeMeta and args
            var job = new Job();
            job.AppID = appID;
            job.UserID = userID;
            job.JobType = jobType;
            job.JobName = string.IsNullOrWhiteSpace(jobName) ? type.Name + "." + methodInfo.Name : jobName;
            job.InvokeMeta = JsonConvert.SerializeObject(invokeMeta, SerializerSettings.Settings);
            job.Parameters = Helpers.Encrypt(JsonConvert.SerializeObject(SerializeArguments(args), SerializerSettings.Settings), encryptionKey); //ENCRYPT it!!!
            job.Created = DateTime.Now;

            return job;
        }


    }
}
