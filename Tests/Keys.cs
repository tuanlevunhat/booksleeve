using NUnit.Framework;

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
        public void TestExpireAgainstInvalidKey()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Remove(0, "expire");
                var exp = conn.Expire(0, "expire", 100);
                Assert.IsFalse(conn.Wait(exp));
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
    }
}


