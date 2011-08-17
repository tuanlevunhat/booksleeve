using NUnit.Framework;

namespace Tests
{
    [TestFixture]
    public class Strings // http://redis.io/commands#string
    {
        [Test]
        public void Append()
        {
            using(var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(2, "append");
                var s0 = conn.Strings.GetString(2, "append");

                conn.Strings.Set(2, "append", "abc");
                var s1 = conn.Strings.GetString(2, "append");

                var result = conn.Strings.Append(2, "append", "defgh");
                var s3 = conn.Strings.GetString(2, "append");

                Assert.AreEqual(null, conn.Wait(s0));
                Assert.AreEqual("abc", conn.Wait(s1));
                Assert.AreEqual(8, conn.Wait(result));
                Assert.AreEqual("abcdefgh", conn.Wait(s3));
            }
        }
    }
}
