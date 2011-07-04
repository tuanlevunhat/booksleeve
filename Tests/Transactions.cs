using NUnit.Framework;
using System.Threading.Tasks;

namespace Tests
{
    [TestFixture]
    public class Transactions // http://redis.io/commands#transactions
    {
        [Test]
        public void TestBasicMultiExec()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                conn.Remove(1, "tran");
                conn.Remove(2, "tran");

                using (var tran = conn.CreateTransaction())
                {
                    var s1 = tran.Set(1, "tran", "abc");
                    var s2 = tran.Set(2, "tran", "def");
                    var g1 = tran.GetString(1, "tran");
                    var g2 = tran.GetString(2, "tran");

                    var outsideTran = conn.GetString(1, "tran");

                    var exec = tran.Execute();

                    Assert.IsNull(conn.Wait(outsideTran));
                    Assert.AreEqual("abc", conn.Wait(g1));
                    Assert.AreEqual("def", conn.Wait(g2));
                    conn.Wait(s1);
                    conn.Wait(s2);
                    conn.Wait(exec);
                }

            }
        }

        [Test]
        public void TestRollback()
        {
            using (var conn = Config.GetUnsecuredConnection())
            using (var tran = conn.CreateTransaction())
            {
                var task = tran.Set(4, "abc", "def");
                tran.Discard();

                Assert.IsTrue(task.IsCanceled, "should be cancelled");
                try
                {
                    conn.Wait(task);
                }
                catch (TaskCanceledException)
                { }// ok, else boom!

            }
        }
    }
}

