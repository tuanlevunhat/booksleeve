using System.Linq;
using NUnit.Framework;
using System;
using System.Text;

namespace Tests
{
    [TestFixture]
    public class SortedSets // http://redis.io/commands#sorted_set
    {
        [Test]
        public void Range() // http://code.google.com/p/booksleeve/issues/detail?id=12
        {
            using(var conn = Config.GetUnsecuredConnection())
            {
                const double value = 634614442154715;
                conn.SortedSets.Add(3, "zset", "abc", value);
                var range = conn.SortedSets.Range(3, "zset", 0, -1);

                Assert.AreEqual(value, conn.Wait(range).Single().Value);
            }
        }

        [Test]
        public void UnionAndStore()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(3, "key1");
                conn.Keys.Remove(3, "key2");
                conn.Keys.Remove(3, "to");

                conn.SortedSets.Add(3, "key1", "a", 1);
                conn.SortedSets.Add(3, "key1", "b", 2);
                conn.SortedSets.Add(3, "key1", "c", 3);

                conn.SortedSets.Add(3, "key2", "a", 1);
                conn.SortedSets.Add(3, "key2", "b", 2);
                conn.SortedSets.Add(3, "key2", "c", 3);

                var numberOfElementsT = conn.SortedSets.UnionAndStore(3, "to", new string[] { "key1", "key2" }, BookSleeve.RedisAggregate.Sum);
                var resultSetT = conn.SortedSets.RangeString(3, "to", 0, -1);

                var numberOfElements = conn.Wait(numberOfElementsT);
                Assert.AreEqual(3, numberOfElements);

                var s = conn.Wait(resultSetT);

                Assert.AreEqual("a", s[0].Key);
                Assert.AreEqual("b", s[1].Key);
                Assert.AreEqual("c", s[2].Key);

                Assert.AreEqual(2, s[0].Value);
                Assert.AreEqual(4, s[1].Value);
                Assert.AreEqual(6, s[2].Value);
            }
        }

        [Test]
        public void UnionAndStoreMax()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(3, "key1");
                conn.Keys.Remove(3, "key2");
                conn.Keys.Remove(3, "to");

                conn.SortedSets.Add(3, "key1", "a", 1);
                conn.SortedSets.Add(3, "key1", "b", 2);
                conn.SortedSets.Add(3, "key1", "c", 3);

                conn.SortedSets.Add(3, "key2", "a", 4);
                conn.SortedSets.Add(3, "key2", "b", 5);
                conn.SortedSets.Add(3, "key2", "c", 6);

                var numberOfElementsT = conn.SortedSets.UnionAndStore(3, "to", new string[] { "key1", "key2" }, BookSleeve.RedisAggregate.Max);
                var resultSetT = conn.SortedSets.RangeString(3, "to", 0, -1);

                var numberOfElements = conn.Wait(numberOfElementsT);
                Assert.AreEqual(3, numberOfElements);

                var s = conn.Wait(resultSetT);

                Assert.AreEqual("a", s[0].Key);
                Assert.AreEqual("b", s[1].Key);
                Assert.AreEqual("c", s[2].Key);

                Assert.AreEqual(4, s[0].Value);
                Assert.AreEqual(5, s[1].Value);
                Assert.AreEqual(6, s[2].Value);
            }
        }

        [Test]
        public void UnionAndStoreMin()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(3, "key1");
                conn.Keys.Remove(3, "key2");
                conn.Keys.Remove(3, "to");

                conn.SortedSets.Add(3, "key1", "a", 1);
                conn.SortedSets.Add(3, "key1", "b", 2);
                conn.SortedSets.Add(3, "key1", "c", 3);

                conn.SortedSets.Add(3, "key2", "a", 4);
                conn.SortedSets.Add(3, "key2", "b", 5);
                conn.SortedSets.Add(3, "key2", "c", 6);

                var numberOfElementsT = conn.SortedSets.UnionAndStore(3, "to", new string[] { "key1", "key2" }, BookSleeve.RedisAggregate.Min);
                var resultSetT = conn.SortedSets.RangeString(3, "to", 0, -1);

                var numberOfElements = conn.Wait(numberOfElementsT);
                Assert.AreEqual(3, numberOfElements);

                var s = conn.Wait(resultSetT);

                Assert.AreEqual("a", s[0].Key);
                Assert.AreEqual("b", s[1].Key);
                Assert.AreEqual("c", s[2].Key);

                Assert.AreEqual(1, s[0].Value);
                Assert.AreEqual(2, s[1].Value);
                Assert.AreEqual(3, s[2].Value);
            }
        }
    }
}
