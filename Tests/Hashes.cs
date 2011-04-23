using NUnit.Framework;
using System.Collections.Generic;
using System;
using System.Text;

namespace Tests
{
    [TestFixture]
    public class Hashes // http://redis.io/commands#hash
    {
        [Test]
        public void TestIncrBy()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                for (int i = 1; i < 1000; i++)
                {
                    Assert.AreEqual(i, conn.IncrementHash(5, "hash-test", "a", 1).Result);
                    Assert.AreEqual(-1 * i, conn.IncrementHash(5, "hash-test", "b", -1).Result);
                }
            }
        }

        [Test]
        public void TestGetAll()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                var key = "hash test";

                var shouldMatch = new Dictionary<Guid, int>();
                var random = new Random();

                for (int i = 1; i < 1000; i++)
                {
                    var guid = Guid.NewGuid();
                    var value = random.Next(Int32.MaxValue);

                    shouldMatch[guid] = value;

                    var x = conn.IncrementHash(6, key, guid.ToString(), value).Result; // Kill Async
                }

                var inRedisRaw = conn.GetHash(6, key).Result;
                var inRedis = new Dictionary<Guid, int>();

                for (var i = 0; i < inRedisRaw.Length; i += 2)
                {
                    var guid = inRedisRaw[i];
                    var num = inRedisRaw[i + 1];

                    inRedis[Guid.Parse(Encoding.ASCII.GetString(guid))] = int.Parse(Encoding.ASCII.GetString(num));
                }

                Assert.AreEqual(shouldMatch.Count, inRedis.Count);

                foreach (var k in shouldMatch.Keys)
                {
                    Assert.AreEqual(shouldMatch[k], inRedis[k]);
                }
            }
        }

        [Test]
        public void TestGet()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                var key = "hash test";

                var shouldMatch = new Dictionary<Guid, int>();
                var random = new Random();

                for (int i = 1; i < 1000; i++)
                {
                    var guid = Guid.NewGuid();
                    var value = random.Next(Int32.MaxValue);

                    shouldMatch[guid] = value;

                    var x = conn.IncrementHash(7, key, guid.ToString(), value).Result; // Kill Async
                }

                foreach (var k in shouldMatch.Keys)
                {
                    var inRedis = conn.GetFromHash(7, key, k.ToString()).Result;
                    var num = int.Parse(Encoding.ASCII.GetString(inRedis));

                    Assert.AreEqual(shouldMatch[k], num);
                }
            }
        }
    }
}
