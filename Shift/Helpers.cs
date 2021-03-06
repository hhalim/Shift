﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Autofac.Core;
using Autofac;
using System.IO;

namespace Shift
{
    public static class Helpers
    {

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

        //Autofac DI for multiple parameters
        public static IEnumerable<Parameter> GenerateNamedParameters(IDictionary<string, object> parameters)
        {
            var _parameters = new List<Parameter>();
            foreach (var parameter in parameters)
            {
                _parameters.Add(new NamedParameter(parameter.Key, parameter.Value));
            }

            return _parameters;
        }

        //Use BaseDirectory, not CurrentDirectory
        public static string NormalizePath(string path)
        {
            //Use the current assembly location instead of relative working directory (System.Environment.CurrentDirectory)
            //The windows service is using C:\Windows\system32 for Environment.CurrentDirectory
            //http://stackoverflow.com/a/23661766/2437862
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;

            //Normalize the current path http://stackoverflow.com/a/27786368/2437862
            var relativeToBase = Path.Combine(baseDir, path);
            var normalizedPath = Path.GetFullPath(relativeToBase);

            return normalizedPath;
        }

        public static bool HasToken(ParameterInfo[] parameters, string tokenName)
        {
            var count = (from p in parameters
                         where p.ParameterType.FullName.ToUpper().Contains(tokenName.ToUpper())
                         select p).Count();
            if (count > 0)
                return true;

            return false;
        }

    }

}
