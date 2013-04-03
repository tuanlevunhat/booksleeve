using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BookSleeve;
using NUnit.Framework;
using System.Threading;
using System.Threading.Tasks;

namespace Tests
{
    [TestFixture]
    public class Locking
    {
        [Test]
        public void AggressiveParallel()
        {
            int count = 2;
            int errorCount = 0;
            ManualResetEvent evt = new ManualResetEvent(false);
            using (var c1 = Config.GetUnsecuredConnection(waitForOpen: true))
            using (var c2 = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                WaitCallback cb = obj =>
                {
                    var conn = (RedisConnection)obj;
                    conn.Error += delegate { Interlocked.Increment(ref errorCount); };
                    for(int i = 0 ; i < 1000 ; i++)
                    {
                        conn.Strings.TakeLock(2, "abc", "def", 5);
                    }
                    conn.Wait(conn.Server.Ping());
                    conn.Close(false);
                    if (Interlocked.Decrement(ref count) == 0) evt.Set();
                };
                ThreadPool.QueueUserWorkItem(cb, c1);
                ThreadPool.QueueUserWorkItem(cb, c2);
                evt.WaitOne(8000);
            }
            Assert.AreEqual(0, Interlocked.CompareExchange(ref errorCount, 0, 0));
        }

        [Test]
        public void TestOpCountByVersionLocal()
        {
            using (var conn = Config.GetUnsecuredConnection(open: false))
            {
                TestOpCountByVersion(conn, 5, false);
                TestOpCountByVersion(conn, 3, true);
            }
        }

        [Test]
        public void TestOpCountByVersionRemote()
        {
            using (var conn = new RedisConnection("192.168.0.6"))
            {
                TestOpCountByVersion(conn, 1, false);
                TestOpCountByVersion(conn, 1, true);
            }
        }
        public void TestOpCountByVersion(RedisConnection conn, int expected, bool existFirst)
        {
            const int DB = 0, LockDuration = 30;
            const string Key = "TestOpCountByVersion";
            conn.Wait(conn.Open());
            conn.Keys.Remove(DB, Key);
            var newVal = "us:" + Guid.NewGuid().ToString();
            string expectedVal = newVal;
            if (existFirst)
            {
                expectedVal = "other:" + Guid.NewGuid().ToString();
                conn.Strings.Set(DB, Key, expectedVal, LockDuration);
            }
            int countBefore = conn.GetCounters().MessagesSent;
            var taken = conn.Wait(conn.Strings.TakeLock(DB, Key, newVal, LockDuration));
            int countAfter = conn.GetCounters().MessagesSent;
            var valAfter = conn.Wait(conn.GetString(DB, Key));
            Assert.AreEqual(!existFirst, taken, "lock taken");
            Assert.AreEqual(expectedVal, valAfter, "taker");
            Assert.AreEqual(expected, (countAfter - countBefore) - 1, "expected ops (including ping)");
            // note we get a ping from GetCounters
        }

        [Test]
        public void TestBasicLockNotTaken()
        {
            using(var conn = Config.GetUnsecuredConnection())
            {
                int errorCount = 0;
                conn.Error += delegate { Interlocked.Increment(ref errorCount); };
                Task<bool> taken = null;
                Task<string> newValue = null;
                Task<long> ttl = null;

                const int LOOP = 50;
                for (int i = 0; i < LOOP; i++)
                {
                    conn.Keys.Remove(0, "lock-not-exists");
                    taken = conn.Strings.TakeLock(0, "lock-not-exists", "new-value", 10);
                    newValue = conn.Strings.GetString(0, "lock-not-exists");
                    ttl = conn.Keys.TimeToLive(0, "lock-not-exists");
                }
                Assert.IsTrue(conn.Wait(taken), "taken");
                Assert.AreEqual("new-value", conn.Wait(newValue));
                var ttlValue = conn.Wait(ttl);
                Assert.IsTrue(ttlValue >= 8 && ttlValue <= 10, "ttl");

                Assert.AreEqual(0, errorCount);
            }
        }

        [Test]
        public void TestBasicLockTaken()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(0, "lock-exists");
                conn.Strings.Set(0, "lock-exists", "old-value", expirySeconds: 20);
                var taken = conn.Strings.TakeLock(0, "lock-exists", "new-value", 10);
                var newValue = conn.Strings.GetString(0, "lock-exists");
                var ttl = conn.Keys.TimeToLive(0, "lock-exists");

                Assert.IsFalse(conn.Wait(taken), "taken");
                Assert.AreEqual("old-value", conn.Wait(newValue));
                var ttlValue = conn.Wait(ttl);
                Assert.IsTrue(ttlValue >= 18 && ttlValue <= 20, "ttl");
            }
        }
    }
}
