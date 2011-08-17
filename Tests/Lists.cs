using System.Text;
using NUnit.Framework;
using System.Linq;
using System;
namespace Tests
{
    [TestFixture]
    public class Lists // http://redis.io/commands#list
    {
        [Test]
        public void CheckLengthWhenEmpty()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(4, "mylist");
                var len = conn.Lists.GetLength(4, "mylist");

                Assert.AreEqual(0, conn.Wait(len));
            }
        }

        [Test]
        public void CheckLengthWithContents()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(4, "mylist");
                for (int i = 0; i < 100; i++)
                    conn.Lists.AddLast(4, "mylist", new[] { (byte)i });
                var len = conn.Lists.GetLength(4, "mylist");

                Assert.AreEqual(100, conn.Wait(len));
            }
        }

        static byte[] Encode(string value) { return Encoding.UTF8.GetBytes(value); }
        static string Decode(byte[] value) { return Encoding.UTF8.GetString(value); }
        [Test]
        public void CheckRightPush()
        {
            using (var conn = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                conn.Keys.Remove(4, "mylist");
                var lenNil = conn.Features.PushIfNotExists ? conn.Lists.AddLast(4, "mylist", Encode("value1"), createIfMissing: false) : null;
                var len1 = conn.Lists.AddLast(4, "mylist", Encode("value1"));
                var len2 = conn.Lists.AddLast(4, "mylist", Encode("value2"));
                var items = conn.Lists.Range(4, "mylist", 0, -1);

                if (lenNil != null) Assert.AreEqual(-1, conn.Wait(lenNil));
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
            using (var conn = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                conn.Keys.Remove(4, "mylist");
                var lenNil = conn.Features.PushIfNotExists ? conn.Lists.AddLast(4, "mylist", Encode("value1"), createIfMissing: false) : null;
                var len1 = conn.Lists.AddFirst(4, "mylist", Encode("value1"));
                var len2 = conn.Lists.AddFirst(4, "mylist", Encode("value2"));
                var items = conn.Lists.Range(4, "mylist", 0, -1);

                if(lenNil != null) Assert.AreEqual(-1, conn.Wait(lenNil));
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
                conn.Keys.Remove(4, "mylist");
                conn.Lists.AddLast(4, "mylist", Encode("value1"));
                conn.Lists.AddLast(4, "mylist", Encode("value2"));

                var first = conn.Lists.RemoveFirst(4, "mylist");
                var second = conn.Lists.RemoveFirstString(4, "mylist");
                var third = conn.Lists.RemoveFirst(4, "mylist");
                var len = conn.Lists.GetLength(4, "mylist");

                Assert.AreEqual("value1", Decode(conn.Wait(first)));
                Assert.AreEqual("value2", conn.Wait(second));
                Assert.IsNull(conn.Wait(third));
                Assert.AreEqual(0, conn.Wait(len));
            }
        }
        [Test]
        public void CheckRightPop()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(4, "mylist");
                conn.Lists.AddLast(4, "mylist", Encode("value1"));
                conn.Lists.AddLast(4, "mylist", Encode("value2"));

                var first = conn.Lists.RemoveLast(4, "mylist");
                var second = conn.Lists.RemoveLastString(4, "mylist");
                var third = conn.Lists.RemoveLast(4, "mylist");
                var len = conn.Lists.GetLength(4, "mylist");

                Assert.AreEqual("value2", Decode(conn.Wait(first)));
                Assert.AreEqual("value1", conn.Wait(second));
                Assert.IsNull(conn.Wait(third));
                Assert.AreEqual(0, conn.Wait(len));
            }
        }


        [Test]
        public void CheckPushPop()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(4, "source");
                conn.Keys.Remove(4, "dest");

                var empty0 = conn.Lists.RemoveLastAndAddFirst(4, "source", "dest");
                var empty1 = conn.Lists.RemoveLastAndAddFirstString(4, "source", "dest");
                conn.Lists.AddLast(4, "source", "abc");
                conn.Lists.AddLast(4, "source", "def");

                var s = conn.Lists.RemoveLastAndAddFirstString(4, "source", "dest");
                var b = conn.Lists.RemoveLastAndAddFirst(4, "source", "dest");
                var l0 = conn.Lists.GetLength(4, "source");
                var l1 = conn.Lists.GetLength(4, "dest");
                var final = conn.Lists.RangeString(4, "dest", 0, 3);

                Assert.IsNull(conn.Wait(empty0));
                Assert.IsNull(conn.Wait(empty1));
                Assert.AreEqual("def", conn.Wait(s));
                Assert.AreEqual("abc", Decode(conn.Wait(b)));
                Assert.AreEqual(0, conn.Wait(l0));
                Assert.AreEqual(2, conn.Wait(l1));

                var arr = conn.Wait(final);
                Assert.AreEqual(2, arr.Length);
                Assert.AreEqual("abc", arr[0]);
                Assert.AreEqual("def", arr[1]);


            }
        }


        // BLPOP, BRPOP and BRPOPLPUSH are intentionally not implemented;
        // blocking operations have little place in a multiplexer

        [Test]
        public void GetStringFromList()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(4, "mylist");
                var missing = conn.Lists.GetString(4, "mylist", 0);

                conn.Lists.AddLast(4, "mylist", "abc");
                conn.Lists.AddLast(4, "mylist", "def");
                conn.Lists.AddLast(4, "mylist", "ghi");

                var x0 = conn.Lists.GetString(4, "mylist", 0);
                var x1 = conn.Lists.GetString(4, "mylist", 1);
                var x2 = conn.Lists.GetString(4, "mylist", 2);
                var x3 = conn.Lists.GetString(4, "mylist", 3);

                var m1 = conn.Lists.GetString(4, "mylist", -1);
                var m2 = conn.Lists.GetString(4, "mylist", -2);
                var m3 = conn.Lists.GetString(4, "mylist", -3);
                var m4 = conn.Lists.GetString(4, "mylist", -4);

                Assert.IsNull(conn.Wait(missing));

                Assert.AreEqual("abc", conn.Wait(x0));
                Assert.AreEqual("def", conn.Wait(x1));
                Assert.AreEqual("ghi", conn.Wait(x2));
                Assert.IsNull(conn.Wait(x3));

                Assert.AreEqual("ghi", conn.Wait(m1));
                Assert.AreEqual("def", conn.Wait(m2));
                Assert.AreEqual("abc", conn.Wait(m3));
                Assert.IsNull(conn.Wait(m4));

            }
        }

        [Test]
        public void GetBytesFromList()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(4, "mylist");
                var missing = conn.Lists.GetString(4, "mylist", 0);

                conn.Lists.AddLast(4, "mylist", Encode("abc"));
                conn.Lists.AddLast(4, "mylist", Encode("def"));
                conn.Lists.AddLast(4, "mylist", Encode("ghi"));

                var x0 = conn.Lists.Get(4, "mylist", 0);
                var x1 = conn.Lists.Get(4, "mylist", 1);
                var x2 = conn.Lists.Get(4, "mylist", 2);
                var x3 = conn.Lists.Get(4, "mylist", 3);

                var m1 = conn.Lists.Get(4, "mylist", -1);
                var m2 = conn.Lists.Get(4, "mylist", -2);
                var m3 = conn.Lists.Get(4, "mylist", -3);
                var m4 = conn.Lists.Get(4, "mylist", -4);

                Assert.IsNull(conn.Wait(missing));

                Assert.AreEqual("abc", Decode(conn.Wait(x0)));
                Assert.AreEqual("def", Decode(conn.Wait(x1)));
                Assert.AreEqual("ghi", Decode(conn.Wait(x2)));
                Assert.IsNull(conn.Wait(x3));

                Assert.AreEqual("ghi", Decode(conn.Wait(m1)));
                Assert.AreEqual("def", Decode(conn.Wait(m2)));
                Assert.AreEqual("abc", Decode(conn.Wait(m3)));
                Assert.IsNull(conn.Wait(m4));

            }
        }

        [Test]
        public void TestListInsertString()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(2, "ins");
                conn.Lists.AddFirst(2, "ins", "x");
                var missingB = conn.Lists.InsertBefore(2, "ins", "abc", "AAA");
                var missingA = conn.Lists.InsertAfter(2, "ins", "abc", "BBB");
                
                conn.Lists.AddFirst(2, "ins", "abc");
                conn.Lists.AddFirst(2, "ins", "y");
                var existB = conn.Lists.InsertBefore(2, "ins", "abc", "CCC");
                var existA = conn.Lists.InsertAfter(2, "ins", "abc", "DDD");
                var all = conn.Lists.RangeString(2, "ins", 0, -1);
                Assert.AreEqual(-1, conn.Wait(missingB));
                Assert.AreEqual(-1, conn.Wait(missingA));

                Assert.AreEqual(4, conn.Wait(existB));
                Assert.AreEqual(5, conn.Wait(existA));

                Assert.IsTrue(conn.Wait(all).SequenceEqual(new[] {"x","CCC","abc","DDD","y"}));
            }
        }
        [Test]
        public void TestListInsertBlob()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(2, "ins");
                conn.Lists.AddFirst(2, "ins", Encode("x"));
                var missingB = conn.Lists.InsertBefore(2, "ins", Encode("abc"), Encode("AAA"));
                var missingA = conn.Lists.InsertAfter(2, "ins", Encode("abc"), Encode("BBB"));

                conn.Lists.AddFirst(2, "ins", Encode("abc"));
                conn.Lists.AddFirst(2, "ins", Encode("y"));
                var existB = conn.Lists.InsertBefore(2, "ins", Encode("abc"), Encode("CCC"));
                var existA = conn.Lists.InsertAfter(2, "ins", Encode("abc"), Encode("DDD"));
                var all = conn.Lists.Range(2, "ins", 0, -1);
                Assert.AreEqual(-1, conn.Wait(missingB));
                Assert.AreEqual(-1, conn.Wait(missingA));

                Assert.AreEqual(4, conn.Wait(existB));
                Assert.AreEqual(5, conn.Wait(existA));

                Assert.IsTrue(conn.Wait(all).Select(Decode).SequenceEqual(new[] { "x", "CCC", "abc", "DDD", "y" }));
            }
        }
        [Test]
        public void TestListByIndexString()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(2, "byindex");
                var notExists = conn.Lists.GetString(2, "byindex", 1);
                conn.Lists.AddLast(2, "byindex", "a");
                conn.Lists.AddLast(2, "byindex", "b");
                conn.Lists.AddLast(2, "byindex", "c");

                var item = conn.Lists.GetString(2, "byindex", 1);
                var outOfRange = conn.Lists.GetString(3, "byindex", 1);
                Assert.IsNull(conn.Wait(notExists));
                Assert.AreEqual("b", conn.Wait(item));
                Assert.IsNull(conn.Wait(outOfRange));
            }
        }
        [Test]
        public void TestListByIndexBlob()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(2, "byindex");
                var notExists = conn.Lists.Get(2, "byindex", 1);
                conn.Lists.AddLast(2, "byindex", Encode("a"));
                conn.Lists.AddLast(2, "byindex", Encode("b"));
                conn.Lists.AddLast(2, "byindex", Encode("c"));

                var item = conn.Lists.Get(2, "byindex", 1);
                var outOfRange = conn.Lists.Get(3, "byindex", 1);
                Assert.IsNull(conn.Wait(notExists));
                Assert.AreEqual("b", Decode(conn.Wait(item)));
                Assert.IsNull(conn.Wait(outOfRange));
            }
        }
        [Test]
        public void TestTrim()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(2, "trim");

                conn.Lists.Trim(2, "trim", 1);
                var ne = conn.Lists.GetLength(2, "trim");

                conn.Lists.AddLast(2, "trim", Encode("a"));
                conn.Lists.AddLast(2, "trim", Encode("b"));
                conn.Lists.AddLast(2, "trim", Encode("c"));


                conn.Lists.Trim(2, "trim", 1);
                var e = conn.Lists.GetLength(2, "trim");

                Assert.AreEqual(0, conn.Wait(ne));
                Assert.AreEqual(1, conn.Wait(e));
            }
        }
        [Test]
        public void TestSetByIndexString()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(2, "setbyindex");

                conn.Lists.AddLast(2, "setbyindex", "a");
                conn.Lists.AddLast(2, "setbyindex", "b");
                conn.Lists.AddLast(2, "setbyindex", "c");

                conn.Lists.Set(2, "setbyindex", 1, "d");
                var item = conn.Lists.GetString(2, "setbyindex", 1);

                Assert.AreEqual("d", conn.Wait(item));
            }
        }
        [Test]
        public void TestSetByIndexBlob()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(2, "setbyindex");

                conn.Lists.AddLast(2, "setbyindex", Encode("a"));
                conn.Lists.AddLast(2, "setbyindex", Encode("b"));
                conn.Lists.AddLast(2, "setbyindex", Encode("c"));

                conn.Lists.Set(2, "setbyindex", 1, Encode("d"));
                var item = conn.Lists.Get(2, "setbyindex", 1);

                Assert.AreEqual("d", Decode(conn.Wait(item)));
            }
        }
        [Test]
        public void TestRemoveString()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(2, "remove");
                var ne = conn.Lists.Remove(2, "remove", "b");

                conn.Lists.AddLast(2, "remove", "b");
                conn.Lists.AddLast(2, "remove", "a");
                conn.Lists.AddLast(2, "remove", "b");
                conn.Lists.AddLast(2, "remove", "c");
                conn.Lists.AddLast(2, "remove", "b");

                var e = conn.Lists.Remove(2, "remove", "b", count: 2);
                var count = conn.Lists.GetLength(2, "remove");
                Assert.AreEqual(0, conn.Wait(ne));
                Assert.AreEqual(2, conn.Wait(e));
                Assert.AreEqual(3, conn.Wait(count));
            }
        }
        [Test]
        public void TestRemoveBlob()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(2, "remove");
                var ne = conn.Lists.Remove(2, "remove", Encode("b"));

                conn.Lists.AddLast(2, "remove", Encode("b"));
                conn.Lists.AddLast(2, "remove", Encode("a"));
                conn.Lists.AddLast(2, "remove", Encode("b"));
                conn.Lists.AddLast(2, "remove", Encode("c"));
                conn.Lists.AddLast(2, "remove", Encode("b"));

                var e = conn.Lists.Remove(2, "remove", Encode("b"), count: 2);
                var count = conn.Lists.GetLength(2, "remove");
                Assert.AreEqual(0, conn.Wait(ne));
                Assert.AreEqual(2, conn.Wait(e));
                Assert.AreEqual(3, conn.Wait(count));
            }
        }

    }
}
