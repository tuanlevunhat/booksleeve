﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Reflection;

namespace Tests
{
    class Program
    {
        static void Main()
        {
            // why is this here? because some dumbass forgot to install a decent test-runner before going to the airport
            var epicFail = new List<string>();
            var testTypes = from type in typeof(Program).Assembly.GetTypes()
                            where Attribute.IsDefined(type, typeof(TestFixtureAttribute))
                            && !Attribute.IsDefined(type, typeof(IgnoreAttribute))
                            let methods = type.GetMethods()
                            select new
                            {
                                Type = type,
                                Methods = methods,
                                ActiveMethods = methods.Where(x => Attribute.IsDefined(x, typeof(ActiveTestAttribute))).ToArray(),
                                Setup = methods.SingleOrDefault(x => Attribute.IsDefined(x, typeof(TestFixtureSetUpAttribute))),
                                TearDown = methods.SingleOrDefault(x => Attribute.IsDefined(x, typeof(TestFixtureTearDownAttribute)))
                            };
            int pass = 0, fail = 0;

            bool activeOnly = testTypes.SelectMany(x => x.ActiveMethods).Any();

            foreach (var type in testTypes)
            {
                var tests = (from method in (activeOnly ? type.ActiveMethods : type.Methods)
                             where Attribute.IsDefined(method, typeof(TestAttribute))
                             && !Attribute.IsDefined(method, typeof(IgnoreAttribute))
                             select method).ToArray();

                if (tests.Length == 0) continue;

                Console.WriteLine(type.Type.FullName);
                object obj;
                try
                {
                    obj = Activator.CreateInstance(type.Type);
                    if (obj == null) throw new InvalidOperationException("the world has gone mad");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    continue;
                }
                using (obj as IDisposable)
                {
                    if (type.Setup != null)
                    {
                        try { type.Setup.Invoke(obj, null); }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Test fixture startup failed: " + ex.Message);
                            fail++;
                            epicFail.Add(type.Setup.DeclaringType.FullName + "." + type.Setup.Name);
                            continue;
                        }
                    }
                    
                    foreach (var test in tests)
                    {
                        var expectedFail = Attribute.GetCustomAttribute(test, typeof(ExpectedExceptionAttribute)) as ExpectedExceptionAttribute;
                        Console.Write(test.Name + ": ");
                        Exception err = null;
                        try
                        {
                            test.Invoke(obj, null);
                        }
                        catch (TargetInvocationException ex)
                        {
                            err = ex.InnerException;
                        }
                        catch (Exception ex)
                        {
                            err = ex;
                        }

                        if (expectedFail != null)
                        {
                            if (err == null)
                            {
                                err = new NUnit.Framework.AssertionException("failed to fail");
                            }
                            else
                            {
                                int issues = 0;
                                if (expectedFail.ExpectedException != null && !expectedFail.ExpectedException.IsAssignableFrom(err.GetType()))
                                {
                                    issues++;
                                }
                                if (expectedFail.ExpectedExceptionName != null && err.GetType().FullName != expectedFail.ExpectedExceptionName)
                                {
                                    issues++;
                                }
                                if (expectedFail.ExpectedMessage != null && err.Message != expectedFail.ExpectedMessage)
                                {
                                    issues++;
                                }
                                if (issues == 0) err = null;
                                else
                                {
                                    err = new InvalidOperationException("Failed in a different way", err);
                                }
                            }
                        }

                        if (err == null)
                        {
                            Console.WriteLine("pass");
                            pass++;
                        }
                        else
                        {
                            Console.WriteLine(err.Message);
                            fail++;
                            epicFail.Add(test.DeclaringType.FullName + "." + test.Name);
                        }
                    }
                    if (type.TearDown != null)
                    {
                        try { type.TearDown.Invoke(obj, null); }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Test fixture teardown failed: " + ex.Message);
                            fail++;
                            epicFail.Add(type.TearDown.DeclaringType.FullName + "." + type.TearDown.Name);
                        }
                    }
                }
            }
            Console.WriteLine("Passed: {0}; Failed: {1}", pass, fail);
            foreach (var msg in epicFail) Console.WriteLine(msg);
        }
    }
}

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class ActiveTestAttribute : Attribute { }