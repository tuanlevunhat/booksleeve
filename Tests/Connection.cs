using NUnit.Framework;
using System;

namespace Tests
{
    [TestFixture]
    public class Connections // http://redis.io/commands#connection
    {
        // AUTH is already tested by secured connection

        // QUIT is implicit in dispose

        // ECHO has little utility in an application

        [Test]
        public void TestGetSetOnDifferentDbHasDifferentValues()
        {
            // note: we don't expose SELECT directly, but we can verify that we have different DBs in play:

            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Set(1, "select", "abc");
                conn.Set(2, "select", "def");
                var x = conn.GetString(1, "select");
                var y = conn.GetString(2, "select");
                conn.WaitAll(x, y);
                Assert.AreEqual("abc", x.Result);
                Assert.AreEqual("def", y.Result);
            }
        }
        [Test, ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void TestGetOnInvalidDbThrows()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.GetString(-1, "select");                
            }
        }


        [Test]
        public void Ping()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                var ms = conn.GetValue(conn.Ping());
                Assert.GreaterOrEqual(ms, 0);
            }
        }

        
    }
}
