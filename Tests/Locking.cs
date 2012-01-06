using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tests
{
    [TestFixture]
    public class Locking
    {
        [Test]
        public void TestBasicLockNotTaken()
        {
            using(var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(0, "lock-not-exists");
                var taken = conn.Strings.TakeLock(0, "lock-not-exists", "new-value", 10);
                var newValue = conn.Strings.GetString(0, "lock-not-exists");
                var ttl = conn.Keys.TimeToLive(0, "lock-not-exists");

                Assert.IsTrue(conn.Wait(taken), "taken");
                Assert.AreEqual("new-value", conn.Wait(newValue));
                var ttlValue = conn.Wait(ttl);
                Assert.IsTrue(ttlValue >= 8 && ttlValue <= 10, "ttl");
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
