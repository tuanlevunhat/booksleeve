using NUnit.Framework;
using System.Threading;
using System.Text;

namespace Tests
{
    [TestFixture]
    public class PubSub // http://redis.io/commands#pubsub
    {
        [Test]
        public void TestPublishWithNoSubscribers()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                Assert.AreEqual(0, conn.Wait(conn.Publish("channel", "message")));
            }
        }
     
        [Test]
        public void TestPublishWithSubscribers()
        {
            using(var listenA = Config.GetSubscriberConnection())
            using(var listenB = Config.GetSubscriberConnection())
            using (var conn = Config.GetUnsecuredConnection())
            {
                var t1 = listenA.Subscribe("channel", delegate { });
                var t2 = listenB.Subscribe("channel", delegate { });

                listenA.Wait(t1);
                Assert.AreEqual(1, listenA.SubscriptionCount, "A subscriptions");

                listenB.Wait(t2);
                Assert.AreEqual(1, listenB.SubscriptionCount, "B subscriptions");
                
                var pub = conn.Publish("channel", "message");
                Assert.AreEqual(2, conn.Wait(pub), "delivery count");
            }
        }

        [Test]
        public void TestMultipleSubscribersGetMessage()
        {
            using (var listenA = Config.GetSubscriberConnection())
            using (var listenB = Config.GetSubscriberConnection())
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Wait(conn.Server.Ping());
                int gotA = 0, gotB = 0;
                var tA = listenA.Subscribe("channel", (s, msg) => { if (Encoding.UTF8.GetString(msg) == "message") Interlocked.Increment(ref gotA); });
                var tB = listenB.Subscribe("channel", (s, msg) => { if (Encoding.UTF8.GetString(msg) == "message") Interlocked.Increment(ref gotB); });
                listenA.Wait(tA);
                listenB.Wait(tB);
                Assert.AreEqual(2, conn.Wait(conn.Publish("channel", "message")));
                AllowReasonableTimeToPublishAndProcess();
                Assert.AreEqual(1, Interlocked.CompareExchange(ref gotA, 0, 0));
                Assert.AreEqual(1, Interlocked.CompareExchange(ref gotB, 0, 0));

                // and unsubscibe...
                tA = listenA.Unsubscribe("channel");
                listenA.Wait(tA);
                Assert.AreEqual(1, conn.Wait(conn.Publish("channel", "message")));
                AllowReasonableTimeToPublishAndProcess();
                Assert.AreEqual(1, Interlocked.CompareExchange(ref gotA, 0, 0));
                Assert.AreEqual(2, Interlocked.CompareExchange(ref gotB, 0, 0));
            }
        }

        internal static void AllowReasonableTimeToPublishAndProcess()
        {
            Thread.Sleep(50);
        }

        [Test]
        public void TestPartialSubscriberGetMessage()
        {
            using (var listenA = Config.GetSubscriberConnection())
            using (var listenB = Config.GetSubscriberConnection())
            using (var conn = Config.GetUnsecuredConnection())
            {
                int gotA = 0, gotB = 0;
                var tA = listenA.Subscribe("channel", (s, msg) => { if (s=="channel" && Encoding.UTF8.GetString(msg) == "message") Interlocked.Increment(ref gotA); });
                var tB = listenB.PatternSubscribe("chann*", (s, msg) => { if (s=="channel" && Encoding.UTF8.GetString(msg) == "message") Interlocked.Increment(ref gotB); });
                listenA.Wait(tA);
                listenB.Wait(tB);
                Assert.AreEqual(2, conn.Wait(conn.Publish("channel", "message")));
                AllowReasonableTimeToPublishAndProcess();
                Assert.AreEqual(1, Interlocked.CompareExchange(ref gotA, 0, 0));
                Assert.AreEqual(1, Interlocked.CompareExchange(ref gotB, 0, 0));

                // and unsubscibe...
                tB = listenB.PatternUnsubscribe("chann*");
                listenB.Wait(tB);
                Assert.AreEqual(1, conn.Wait(conn.Publish("channel", "message")));
                AllowReasonableTimeToPublishAndProcess();
                Assert.AreEqual(2, Interlocked.CompareExchange(ref gotA, 0, 0));
                Assert.AreEqual(1, Interlocked.CompareExchange(ref gotB, 0, 0));
            }
        }

        [Test]
        public void TestSubscribeUnsubscribeAndSubscribeAgain()
        {
            using(var pub = Config.GetUnsecuredConnection())
            using(var sub = Config.GetSubscriberConnection())
            {
                int x = 0, y = 0;
                var t1 = sub.Subscribe("abc", delegate { Interlocked.Increment(ref x); });
                var t2 = sub.PatternSubscribe("ab*", delegate { Interlocked.Increment(ref y); });
                sub.WaitAll(t1, t2);
                pub.Publish("abc", "");
                AllowReasonableTimeToPublishAndProcess();
                Assert.AreEqual(1, Thread.VolatileRead(ref x));
                Assert.AreEqual(1, Thread.VolatileRead(ref y));
                t1 = sub.Unsubscribe("abc");
                t2 = sub.PatternUnsubscribe("ab*");
                sub.WaitAll(t1, t2);
                pub.Publish("abc", "");
                Assert.AreEqual(1, Thread.VolatileRead(ref x));
                Assert.AreEqual(1, Thread.VolatileRead(ref y));
                t1 = sub.Subscribe("abc", delegate { Interlocked.Increment(ref x); });
                t2 = sub.PatternSubscribe("ab*", delegate { Interlocked.Increment(ref y); });
                sub.WaitAll(t1, t2);
                pub.Publish("abc", "");
                AllowReasonableTimeToPublishAndProcess();
                Assert.AreEqual(2, Thread.VolatileRead(ref x));
                Assert.AreEqual(2, Thread.VolatileRead(ref y));
                
            }
        }
    }
}
