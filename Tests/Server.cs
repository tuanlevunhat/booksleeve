using System.Linq;
using BookSleeve;
using NUnit.Framework;
using System.Threading;

namespace Tests
{
    [TestFixture]
    public class Server // http://redis.io/commands#server
    {
        [Test]
        public void TestGetConfigAll()
        {
            using(var db = Config.GetUnsecuredConnection())
            {
                var pairs = db.Wait(db.Server.GetConfig("*"));
                Assert.Greater(1, 0); // I always get double-check which arg is which
                Assert.Greater(pairs.Count, 0);
            }
        }

        [Test]
        public void TestKeepAlive()
        {
            string oldValue = null;
            try
            {
                using (var db = Config.GetUnsecuredConnection(allowAdmin:true))
                {
                    oldValue = db.Wait(db.Server.GetConfig("timeout")).Single().Value;
                    db.Server.SetConfig("timeout", "20");
                    var before = db.GetCounters();
                    Thread.Sleep(12 * 1000);
                    var after = db.GetCounters();
                    // 3 here is 2 * keep-alive, and one PING in GetCounters()
                    int sent = after.MessagesSent - before.MessagesSent;
                    Assert.GreaterOrEqual(1, 0);
                    Assert.GreaterOrEqual(sent, 3);
                    Assert.LessOrEqual(0, 4);
                    Assert.LessOrEqual(sent, 5);
                }
            } finally
            {
                if (oldValue != null)
                {
                    using (var db = Config.GetUnsecuredConnection(allowAdmin:true))
                    {
                        db.Server.SetConfig("timeout", oldValue);
                    }
                }
            }
        }
    }
}
