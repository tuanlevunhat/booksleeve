using NUnit.Framework;
using BookSleeve;
using System.Threading;

namespace Tests
{
    [TestFixture]
    public class Keys // http://redis.io/commands#generic
    {
        // note we don't expose EXPIREAT as it raises all sorts of problems with
        // time synchronisation, UTC vs local, DST, etc; easier for the caller
        // to use EXPIRE

        [Test]
        public void TestDeleteValidKey()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Set(0, "del", "abcdef");
                var x = conn.GetString(0, "del");
                var del = conn.Remove(0, "del");
                var y = conn.GetString(0, "del");
                conn.WaitAll(x, del, y);
                Assert.AreEqual("abcdef", x.Result);
                Assert.IsTrue(del.Result);
                Assert.AreEqual(null, y.Result);
            }
        }

        [Test]
        public void TestDeleteInvalidKey()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Set(0, "exists", "abcdef");
                var x = conn.Remove(0, "exists");
                var y = conn.Remove(0, "exists");
                conn.WaitAll(x, y);
                Assert.IsTrue(x.Result);
                Assert.IsFalse(y.Result);
            }
        }

        [Test]
        public void TestDeleteMultiple()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Set(0, "del", "abcdef");
                var x = conn.Remove(0, "del");
                var y = conn.Remove(0, "del");
                conn.WaitAll(x, y);
                Assert.IsTrue(x.Result);
                Assert.IsFalse(y.Result);
            }
        }


        [Test]
        public void TestExpireAgainstInvalidKey()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Set(0, "delA", "abcdef");
                conn.Remove(0, "delB");
                conn.Set(0, "delC", "abcdef");

                var del = conn.Remove(0, new[] {"delA", "delB", "delC"});
                Assert.AreEqual(2, conn.Wait(del));
            }
        }

        [Test]
        public void TestExists()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Set(0, "exists", "abcdef");
                var x = conn.ContainsKey(0, "exists");
                conn.Remove(0, "exists");
                var y = conn.ContainsKey(0, "exists");
                conn.WaitAll(x, y);
                Assert.IsTrue(x.Result);
                Assert.IsFalse (y.Result);
            }
        }

        [Test]
        public void TestExpireAgainstValidKey()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Set(0, "expire", "abcdef");
                var x = conn.TimeToLive(0, "expire");
                var exp1 = conn.Expire(0, "expire", 100);
                var y = conn.TimeToLive(0, "expire");
                var exp2 = conn.Expire(0, "expire", 150);
                var z = conn.TimeToLive(0, "expire");

                conn.WaitAll(x, exp1, y, exp2, z);
                
                Assert.AreEqual(-1, x.Result);
                Assert.IsTrue(exp1.Result);
                Assert.GreaterOrEqual(y.Result, 90);
                Assert.LessOrEqual(y.Result, 100);

                if (conn.Features.ExpireOverwrite)
                {
                    Assert.IsTrue(exp2.Result);
                    Assert.GreaterOrEqual(z.Result, 140);
                    Assert.LessOrEqual(z.Result, 150);
                }
                else
                {
                    Assert.IsFalse(exp2.Result);
                    Assert.GreaterOrEqual(z.Result, 90);
                    Assert.LessOrEqual(z.Result, 100);
                }
            }
        }

        [Test]
        public void TestSuccessfulMove()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Set(1, "move", "move-value");
                conn.Remove(2, "move");

                var succ = conn.Move(1, "move", 2);
                var in1 = conn.GetString(1, "move");
                var in2 = conn.GetString(2, "move");

                Assert.IsTrue(conn.Wait(succ));
                Assert.IsNull(conn.Wait(in1));
                Assert.AreEqual("move-value", conn.Wait(in2));
            }
        }

        [Test]
        public void TestFailedMoveWhenNotExistsInSource()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Remove(1, "move");
                conn.Set(2, "move", "move-value");
                
                var succ = conn.Move(1, "move", 2);
                var in1 = conn.GetString(1, "move");
                var in2 = conn.GetString(2, "move");

                Assert.IsFalse(conn.Wait(succ));
                Assert.IsNull(conn.Wait(in1));
                Assert.AreEqual("move-value", conn.Wait(in2));
            }
        }

        [Test]
        public void TestFailedMoveWhenNotExistsInTarget()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Set(1, "move", "move-valueA");
                conn.Set(2, "move", "move-valueB");

                var succ = conn.Move(1, "move", 2);
                var in1 = conn.GetString(1, "move");
                var in2 = conn.GetString(2, "move");

                Assert.IsFalse(conn.Wait(succ));
                Assert.AreEqual("move-valueA", conn.Wait(in1));
                Assert.AreEqual("move-valueB", conn.Wait(in2));
            }
        }

        [Test]
        public void RemoveExpiry()
        {
            int errors = 0, expectedErrors;
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Set(1, "persist", "persist");
                var persist1 = conn.Persist(1, "persist");
                conn.Expire(1, "persist", 100);
                var before = conn.TimeToLive(1, "persist");
                var persist2 = conn.Persist(1, "persist");
                var after = conn.TimeToLive(1, "persist");
                
                conn.Error += delegate
                {
                    Interlocked.Increment(ref errors);
                };
                Assert.GreaterOrEqual(conn.Wait(before), 90);
                if (conn.Features.Persist)
                {
                    Assert.IsFalse(conn.Wait(persist1));   
                    Assert.IsTrue(conn.Wait(persist2));
                    Assert.AreEqual(-1, conn.Wait(after));
                    expectedErrors = 0;
                }
                else
                {
                    try{
                        conn.Wait(persist1);
                        Assert.Fail();
                    }
                    catch (RedisException){}
                    try
                    {
                        conn.Wait(persist2);
                        Assert.Fail();
                    }
                    catch (RedisException) { }
                    Assert.GreaterOrEqual(conn.Wait(after), 90);
                    expectedErrors = 2;
                }
            }

            Assert.AreEqual(expectedErrors, Interlocked.CompareExchange(ref errors,0,0));
        }


        [Test]
        public void RandomKeys()
        {
            using (var conn = Config.GetUnsecuredConnection(allowAdmin: true))
            {
                conn.FlushDb(7);
                var key1 = conn.RandomKey(7);
                conn.Set(7, "random1", "random1");
                var key2 = conn.RandomKey(7);
                for (int i = 2; i < 100; i++)
                {
                    string key = "random" + i;
                    conn.Set(7, key, key);
                }
                var key3 = conn.RandomKey(7);

                Assert.IsNull(conn.Wait(key1));
                Assert.AreEqual("random1", conn.Wait(key2));
                string s = conn.Wait(key3);

                Assert.IsTrue(s.StartsWith("random"));
                s = s.Substring(6);
                int result = int.Parse(s);
                Assert.GreaterOrEqual(result, 1);
                Assert.Less(result, 100);
            }

        }

        [Test]
        public void RenameKeyWithOverwrite()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Remove(1, "foo");
                conn.Remove(1, "bar");

                var check1 = conn.Rename(1, "foo", "bar"); // neither
                var after1_foo = conn.GetString(1, "foo");
                var after1_bar = conn.GetString(1, "bar");

                conn.Set(1, "foo", "foo-value");

                var check2 = conn.Rename(1, "foo", "bar"); // source only
                var after2_foo = conn.GetString(1, "foo");
                var after2_bar = conn.GetString(1, "bar");

                var check3 = conn.Rename(1, "foo", "bar"); // dest only
                var after3_foo = conn.GetString(1, "foo");
                var after3_bar = conn.GetString(1, "bar");

                conn.Set(1, "foo", "new-value");
                var check4 = conn.Rename(1, "foo", "bar"); // both
                var after4_foo = conn.GetString(1, "foo");
                var after4_bar = conn.GetString(1, "bar");

                try
                {
                    conn.Wait(check1);
                    Assert.Fail();
                }
                catch (RedisException) { }
                Assert.IsNull(conn.Wait(after1_foo));
                Assert.IsNull(conn.Wait(after1_bar));

                conn.Wait(check2);
                Assert.IsNull(conn.Wait(after2_foo));
                Assert.AreEqual("foo-value", conn.Wait(after2_bar));

                try
                {
                    conn.Wait(check3);
                    Assert.Fail();
                }
                catch (RedisException) { }
                Assert.IsNull(conn.Wait(after3_foo));
                Assert.AreEqual("foo-value", conn.Wait(after3_bar));

                conn.Wait(check4);
                Assert.IsNull(conn.Wait(after4_foo));
                Assert.AreEqual("new-value", conn.Wait(after4_bar));

            }
        }

        [Test]
        public void RenameKeyWithoutOverwrite()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Remove(1, "foo");
                conn.Remove(1, "bar");

                var check1 = conn.RenameIfNotExists(1, "foo", "bar"); // neither
                var after1_foo = conn.GetString(1, "foo");
                var after1_bar = conn.GetString(1, "bar");

                conn.Set(1, "foo", "foo-value");

                var check2 = conn.RenameIfNotExists(1, "foo", "bar"); // source only
                var after2_foo = conn.GetString(1, "foo");
                var after2_bar = conn.GetString(1, "bar");

                var check3 = conn.RenameIfNotExists(1, "foo", "bar"); // dest only
                var after3_foo = conn.GetString(1, "foo");
                var after3_bar = conn.GetString(1, "bar");

                conn.Set(1, "foo", "new-value");
                var check4 = conn.RenameIfNotExists(1, "foo", "bar"); // both
                var after4_foo = conn.GetString(1, "foo");
                var after4_bar = conn.GetString(1, "bar");

                try
                {
                    conn.Wait(check1);
                    Assert.Fail();
                }
                catch (RedisException) { }
                Assert.IsNull(conn.Wait(after1_foo));
                Assert.IsNull(conn.Wait(after1_bar));

                Assert.IsTrue(conn.Wait(check2));
                Assert.IsNull(conn.Wait(after2_foo));
                Assert.AreEqual("foo-value", conn.Wait(after2_bar));

                try
                {
                    conn.Wait(check3);
                    Assert.Fail();
                }
                catch (RedisException) { }
                Assert.IsNull(conn.Wait(after3_foo));
                Assert.AreEqual("foo-value", conn.Wait(after3_bar));

                Assert.IsFalse(conn.Wait(check4));
                Assert.AreEqual("new-value", conn.Wait(after4_foo));
                Assert.AreEqual("foo-value", conn.Wait(after4_bar));

            }
        }
    }
}




