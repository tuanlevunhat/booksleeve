using NUnit.Framework;
using System.Text;

namespace Tests
{
    [TestFixture]
    public class Sets // http://redis.io/commands#set
    {
        [Test]
        public void AddSingle()
        {
            using(var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(3, "set");
                var r0 = conn.Sets.Add(3, "set", "abc");
                var r1 = conn.Sets.Add(3, "set", "abc");
                var len = conn.Sets.GetLength(3, "set");

                Assert.AreEqual(true, r0.Result);
                Assert.AreEqual(false, r1.Result);
                Assert.AreEqual(1, len.Result);
            }
        }
        [Test]
        public void AddSingleBinary()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(3, "set");
                var r0 = conn.Sets.Add(3, "set", Encode("abc"));
                var r1 = conn.Sets.Add(3, "set", Encode("abc"));
                var len = conn.Sets.GetLength(3, "set");

                Assert.AreEqual(true, r0.Result);
                Assert.AreEqual(false, r1.Result);
                Assert.AreEqual(1, len.Result);
            }
        }
        static byte[] Encode(string value) { return Encoding.UTF8.GetBytes(value); }
        static string Decode(byte[] value) { return Encoding.UTF8.GetString(value); }
        [Test]
        public void RemoveSingle()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(3, "set");
                conn.Sets.Add(3, "set", "abc");
                conn.Sets.Add(3, "set", "def");

                var r0 = conn.Sets.Remove(3, "set", "abc");
                var r1 = conn.Sets.Remove(3, "set", "abc");
                var len = conn.Sets.GetLength(3, "set");

                Assert.AreEqual(true, r0.Result);
                Assert.AreEqual(false, r1.Result);
                Assert.AreEqual(1, len.Result);
            }
        }

        [Test]
        public void RemoveSingleBinary()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(3, "set");
                conn.Sets.Add(3, "set", Encode("abc"));
                conn.Sets.Add(3, "set", Encode("def"));

                var r0 = conn.Sets.Remove(3, "set", Encode("abc"));
                var r1 = conn.Sets.Remove(3, "set", Encode("abc"));
                var len = conn.Sets.GetLength(3, "set");

                Assert.AreEqual(true, r0.Result);
                Assert.AreEqual(false, r1.Result);
                Assert.AreEqual(1, len.Result);
            }
        }

        [Test]
        public void AddMulti()
        {
            using (var conn = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                conn.Keys.Remove(3, "set");
                var r0 = conn.Sets.Add(3, "set", "abc");
                var r1 = conn.Sets.Add(3, "set", new[] {"abc", "def"});
                var len = conn.Sets.GetLength(3, "set");

                Assert.AreEqual(true, r0.Result);
                Assert.AreEqual(1, r1.Result);
                Assert.AreEqual(2, len.Result);
            }
        }

        [Test]
        public void RemoveMulti()
        {
            using (var conn = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                conn.Keys.Remove(3, "set");
                conn.Sets.Add(3, "set", "abc");
                conn.Sets.Add(3, "set", "ghi");

                var r0 = conn.Sets.Remove(3, "set", new[] {"abc", "def"});
                var len = conn.Sets.GetLength(3, "set");

                Assert.AreEqual(1, r0.Result);
                Assert.AreEqual(1, len.Result);
            }
        }

        [Test]
        public void AddMultiBinary()
        {
            using (var conn = Config.GetUnsecuredConnection(waitForOpen:true))
            {
                conn.Keys.Remove(3, "set");
                var r0 = conn.Sets.Add(3, "set", Encode("abc"));
                var r1 = conn.Sets.Add(3, "set", new[] { Encode("abc"), Encode("def") });
                var len = conn.Sets.GetLength(3, "set");

                Assert.AreEqual(true, r0.Result);
                Assert.AreEqual(1, r1.Result);
                Assert.AreEqual(2, len.Result);
            }
        }

        [Test]
        public void RemoveMultiBinary()
        {
            using (var conn = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                conn.Keys.Remove(3, "set");
                conn.Sets.Add(3, "set", Encode("abc"));
                conn.Sets.Add(3, "set", Encode("ghi"));

                var r0 = conn.Sets.Remove(3, "set", new[] { Encode("abc"), Encode("def") });
                var len = conn.Sets.GetLength(3, "set");

                Assert.AreEqual(1, r0.Result);
                Assert.AreEqual(1, len.Result);
            }
        }

        [Test]
        public void Exists()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(3, "set");
                var r0 = conn.Sets.Contains(3, "set", "def");

                conn.Sets.Add(3, "set", "abc");
                var r1 = conn.Sets.Contains(3, "set", "def");

                conn.Sets.Add(3, "set", "def");
                var r2 = conn.Sets.Contains(3, "set", "def");

                conn.Sets.Remove(3, "set", "def");
                var r3 = conn.Sets.Contains(3, "set", "def");

                Assert.AreEqual(false, r0.Result);
                Assert.AreEqual(false, r1.Result);
                Assert.AreEqual(true, r2.Result);
                Assert.AreEqual(false, r3.Result);
            }
        }


        [Test]
        public void ExistsBinary()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(3, "set");
                var r0 = conn.Sets.Contains(3, "set", Encode("def"));

                conn.Sets.Add(3, "set", "abc");
                var r1 = conn.Sets.Contains(3, "set", Encode("def"));

                conn.Sets.Add(3, "set", "def");
                var r2 = conn.Sets.Contains(3, "set", Encode("def"));

                conn.Sets.Remove(3, "set", "def");
                var r3 = conn.Sets.Contains(3, "set", Encode("def"));

                Assert.AreEqual(false, r0.Result);
                Assert.AreEqual(false, r1.Result);
                Assert.AreEqual(true, r2.Result);
                Assert.AreEqual(false, r3.Result);
            }
        }
    }
}
