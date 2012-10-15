using System.Linq;
using NUnit.Framework;
using System;
using System.Text;
using BookSleeve;

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

        static string SeedRange(RedisConnection connection, out double min, out double max)
        {
            var rand = new Random(123456);
            const string key = "somerange";
            connection.Keys.Remove(0, key);
            min = max = 0;
            for (int i = 0; i < 50; i++)
            {
                double value = rand.NextDouble();
                if (i == 0)
                {
                    min = max = value;
                }
                else
                {
                    if (value < min) min = value;
                    if (value > max) max= value;
                }
                connection.SortedSets.Add(0, key, "item " + i, value);
            }
            return key;
        }
        [Test]
        public void FindMinMax()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                double minActual, maxActual;
                string key = SeedRange(conn, out minActual, out maxActual);

                var min = conn.SortedSets.Range(0, key,
                    ascending: true, count: 1);
                var max = conn.SortedSets.Range(0, key,
                    ascending: false, count: 1);

                var minScore = conn.Wait(min).Single().Value;
                var maxScore = conn.Wait(max).Single().Value;

                Assert.Less(1, 2); // I *always* get these args the wrong way around
                Assert.Less(Math.Abs(minActual - minScore), 0.0000001, "min");
                Assert.Less(Math.Abs(maxActual - maxScore), 0.0000001, "max");
            }
        }

        [Test]
        public void CheckInfinity()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Keys.Remove(0, "infs");
                conn.SortedSets.Add(0, "infs", "neg", double.NegativeInfinity);
                conn.SortedSets.Add(0, "infs", "pos", double.PositiveInfinity);
                conn.SortedSets.Add(0, "infs", "zero", 0.0);
                var pairs = conn.Wait(conn.SortedSets.RangeString(0, "infs", 0, -1));
                Assert.AreEqual(3, pairs.Length);
                Assert.AreEqual("neg", pairs[0].Key);
                Assert.AreEqual("zero", pairs[1].Key);
                Assert.AreEqual("pos", pairs[2].Key);
                Assert.IsTrue(double.IsNegativeInfinity(pairs[0].Value), "-inf");
                Assert.AreEqual(0.0, pairs[1].Value);
                Assert.IsTrue(double.IsPositiveInfinity(pairs[2].Value), "+inf");
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
