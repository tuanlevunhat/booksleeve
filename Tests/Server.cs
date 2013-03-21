using System.Linq;
using BookSleeve;
using NUnit.Framework;
using System.Threading;
using System;
using System.Threading.Tasks;

namespace Tests
{
    [TestFixture]
    public class Server // http://redis.io/commands#server
    {
        [Test]
        public void TestGetConfigAll()
        {
            using (var db = Config.GetUnsecuredConnection())
            {
                var pairs = db.Wait(db.Server.GetConfig("*"));
                Assert.Greater(1, 0); // I always get double-check which arg is which
                Assert.Greater(pairs.Count, 0);
            }
        }

        [Test]
        public void TestTime()
        {
            using (var db = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                Assert.IsNotNull(db.Features); // we waited, after all
                if (db.Features.Time)
                {
                    var local = DateTime.UtcNow;
                    var server = db.Wait(db.Server.Time());

                    Assert.True(Math.Abs((local - server).TotalMilliseconds) < 10);

                }
            }
        }

        [Test]
        public void TestTimeWithExplicitVersion()
        {
            using (var db = Config.GetUnsecuredConnection(open: false))
            {
                db.SetServerVersion(new Version("2.6.9"), ServerType.Master);
                db.SetKeepAlive(10);
                Assert.IsNotNull(db.Features, "Features"); // we waited, after all
                Assert.IsTrue(db.Features.ClientName, "ClientName");
                Assert.IsTrue(db.Features.Time, "Time");
                db.Name = "FooFoo";
                db.Wait(db.Open());                
                
                var local = DateTime.UtcNow;
                var server = db.Wait(db.Server.Time());

                Assert.True(Math.Abs((local - server).TotalMilliseconds) < 10);
            }
        }

        [Test, ExpectedException(typeof(TimeoutException), ExpectedMessage = "The operation has timed out; the connection is not open")]
        public void TimeoutMessageNotOpened()
        {
            using (var conn = Config.GetUnsecuredConnection(open: false))
            {
                conn.Wait(conn.Strings.Get(0, "abc"));
            }
        }

        [Test, ExpectedException(typeof(TimeoutException), ExpectedMessage = "The operation has timed out.")]
        public void TimeoutMessageNoDetail()
        {
            using (var conn = Config.GetUnsecuredConnection(open: true))
            {
                conn.IncludeDetailInTimeouts = false;
                conn.Keys.Remove(0, "noexist");
                conn.Lists.BlockingRemoveFirst(0, new[] { "noexist" }, 5);
                conn.Wait(conn.Strings.Get(0, "abc"));
            }
        }

        [Test, ExpectedException(typeof(TimeoutException), ExpectedMessage = "The operation has timed out; possibly blocked by: 0: BLPOP \"noexist\" 5")]
        public void TimeoutMessageWithDetail()
        {
            using (var conn = Config.GetUnsecuredConnection(open: true, waitForOpen: true))
            {
                conn.IncludeDetailInTimeouts = true;
                conn.Keys.Remove(0, "noexist");
                conn.Lists.BlockingRemoveFirst(0, new[] { "noexist" }, 5);
                conn.Wait(conn.Strings.Get(0, "abc"));
            }
        }

        [Test]
        public void ClientList()
        {
            using (var killMe = Config.GetUnsecuredConnection())
            using (var conn = Config.GetUnsecuredConnection(allowAdmin: true))
            {
                killMe.Wait(killMe.Strings.GetString(7, "kill me quick"));
                var clients = conn.Wait(conn.Server.ListClients());
                var target = clients.Single(x => x.Database == 7);
                conn.Wait(conn.Server.KillClient(target.Address));
                Assert.IsTrue(clients.Length > 0);

                try
                {
                    killMe.Wait(killMe.Strings.GetString(7, "kill me quick"));
                    Assert.Fail("Should have been dead");
                }
                catch (Exception) { }
            }
        }

        [Test]
        public void TestKeepAlive()
        {
            string oldValue = null;
            try
            {
                using (var db = Config.GetUnsecuredConnection(allowAdmin: true))
                {
                    oldValue = db.Wait(db.Server.GetConfig("timeout")).Single().Value;
                    db.Server.SetConfig("timeout", "20");
                }
                using (var db  = Config.GetUnsecuredConnection(allowAdmin: false, waitForOpen:true))
                {
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
            }
            finally
            {
                if (oldValue != null)
                {
                    Task t;
                    using (var db = Config.GetUnsecuredConnection(allowAdmin: true))
                    {
                        t = db.Server.SetConfig("timeout", oldValue);
                    }
                    Assert.IsTrue(t.Wait(5000));
                    if (t.Exception != null) throw t.Exception;
                }
            }
        }

        [Test]
        public void SetValueWhileDisposing()
        {
            const int LOOP = 10;
            for (int i = 0; i < LOOP; i++)
            {
                var guid = Guid.NewGuid().ToString();
                Task t1, t3;
                Task<string> t2;
                const string key = "SetValueWhileDisposing";
                using (var db = Config.GetUnsecuredConnection(open: true))
                {
                    t1 = db.Strings.Set(0, key, guid);
                }
                using (var db = Config.GetUnsecuredConnection())
                {
                    t2 = db.Strings.GetString(0, key);
                    t3 = db.Keys.Remove(0, key);
                }
                Assert.IsTrue(t1.Wait(500));
                Assert.IsTrue(t2.Wait(500));
                Assert.AreEqual(guid, t2.Result);
                Assert.IsTrue(t3.Wait(500));
            }
        }

        [Test]
        public void TestMasterSlaveSetup()
        {
            using (var unsec = Config.GetUnsecuredConnection(true, true, true))
            using (var sec = Config.GetUnsecuredConnection(true, true, true))
            {
                try
                {
                    var makeSlave = sec.Server.MakeSlave(unsec.Host, unsec.Port);
                    var info = sec.Wait(sec.Server.GetInfo());
                    sec.Wait(makeSlave);
                    Assert.AreEqual("slave", info["role"], "slave");
                    Assert.AreEqual(unsec.Host, info["master_host"], "host");
                    Assert.AreEqual(unsec.Port.ToString(), info["master_port"], "port");
                    var makeMaster = sec.Server.MakeMaster();
                    info = sec.Wait(sec.Server.GetInfo());
                    sec.Wait(makeMaster);
                    Assert.AreEqual("master", info["role"], "master");
                }
                finally
                {
                    sec.Server.MakeMaster();
                }

            }
        }
    }
}
