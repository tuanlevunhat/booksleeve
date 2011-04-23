using NUnit.Framework;
using System.Text;

namespace Tests
{
    [TestFixture]
    public class Lists // http://redis.io/commands#list
    {
        [Test]
        public void CheckEmptyLength()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Remove(4, "mylist");
                var len = conn.ListLength(4, "mylist");

                Assert.AreEqual(0, conn.Wait(len));
            }
        }

        [Test]
        public void CheckLengthWithContents()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Remove(4, "mylist");
                for (int i = 0; i < 100; i++)
                    conn.RightPush(4, "mylist", new[] { (byte)i });
                var len = conn.ListLength(4, "mylist");

                Assert.AreEqual(100, conn.Wait(len));
            }
        }

        static byte[] Encode(string value) { return Encoding.UTF8.GetBytes(value); }
        static string Decode(byte[] value) { return Encoding.UTF8.GetString(value); }
        [Test]
        public void CheckRightPush()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Remove(4, "mylist");
                var len1 = conn.RightPush(4, "mylist", Encode("value1"));
                var len2 = conn.RightPush(4, "mylist", Encode("value2"));
                var items = conn.ListRange(4, "mylist", 0, -1);
                Assert.AreEqual(1, conn.Wait(len1));
                Assert.AreEqual(2, conn.Wait(len2));
                var arr = conn.Wait(items);
                Assert.AreEqual(2, arr.Length);
                Assert.AreEqual("value1", Decode(arr[0]));
                Assert.AreEqual("value2", Decode(arr[1]));
            }
        }

        [Test]
        public void CheckLeftPush()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Remove(4, "mylist");
                var len1 = conn.LeftPush(4, "mylist", Encode("value1"));
                var len2 = conn.LeftPush(4, "mylist", Encode("value2"));
                var items = conn.ListRange(4, "mylist", 0, -1);
                Assert.AreEqual(1, conn.Wait(len1));
                Assert.AreEqual(2, conn.Wait(len2));
                var arr = conn.Wait(items);
                Assert.AreEqual(2, arr.Length);
                Assert.AreEqual("value2", Decode(arr[0]));
                Assert.AreEqual("value1", Decode(arr[1]));
            }
        }
        [Test]
        public void CheckLeftPop()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Remove(4, "mylist");
                conn.RightPush(4, "mylist", Encode("value1"));
                conn.RightPush(4, "mylist", Encode("value2"));

                var first = conn.LeftPop(4, "mylist");
                var second = conn.LeftPop(4, "mylist");
                var third = conn.LeftPop(4, "mylist");
                var len = conn.ListLength(4, "mylist");

                Assert.AreEqual("value1", Decode(conn.Wait(first)));
                Assert.AreEqual("value2", Decode(conn.Wait(second)));
                Assert.IsNull(conn.Wait(third));
                Assert.AreEqual(0, conn.Wait(len));
            }
        }
        [Test]
        public void CheckRightPop()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Remove(4, "mylist");
                conn.RightPush(4, "mylist", Encode("value1"));
                conn.RightPush(4, "mylist", Encode("value2"));

                var first = conn.RightPop(4, "mylist");
                var second = conn.RightPop(4, "mylist");
                var third = conn.RightPop(4, "mylist");
                var len = conn.ListLength(4, "mylist");

                Assert.AreEqual("value2", Decode(conn.Wait(first)));
                Assert.AreEqual("value1", Decode(conn.Wait(second)));
                Assert.IsNull(conn.Wait(third));
                Assert.AreEqual(0, conn.Wait(len));
            }
        }
    }
}
