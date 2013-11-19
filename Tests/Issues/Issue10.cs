using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests.Issues
{
    [TestFixture]
    public class Issue10
    {
        [Test]
        public void Execute()
        {
            using (var conn = Config.GetUnsecuredConnection())
            {
                const int DB = 5;
                const string Key = "issue-10-list";
                conn.Keys.Remove(DB, Key); // contents: nil
                conn.Lists.AddFirst(DB, Key, "abc"); // "abc"
                conn.Lists.AddFirst(DB, Key, "def"); // "def", "abc"
                conn.Lists.AddFirst(DB, Key, "ghi"); // "ghi", "def", "abc",
                conn.Lists.Set(DB, Key, 1, "jkl"); // "ghi", "jkl", "abc"

                var contents = conn.Wait(conn.Lists.RangeString(DB, Key, 0, -1));
                Assert.AreEqual(3, contents.Length);
                Assert.AreEqual("ghi", contents[0]);
                Assert.AreEqual("jkl", contents[1]);
                Assert.AreEqual("abc", contents[2]);
            }
        }
    }
}
