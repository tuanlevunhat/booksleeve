using BookSleeve;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests
{
    [TestFixture]
    public class Scripting
    {
        static RedisConnection GetScriptConn(bool allowAdmin = false)
        {
            var conn = Config.GetUnsecuredConnection(waitForOpen: true, allowAdmin: allowAdmin);
            if (!conn.Features.Scripting)
            {
                using (conn) { return null; }
            }
            return conn;

        }
        [Test]
        public void BasicScripting()
        {
            using (var conn = GetScriptConn())
            {
                if (conn == null) return;

                var noCache = conn.Scripting.Eval(0, "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
                    new[] { "key1", "key2" }, new[] { "first", "second" }, useCache: false);
                var cache = conn.Scripting.Eval(0, "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
                    new[] { "key1", "key2" }, new[] { "first", "second" }, useCache: true);
                var results = (object[])conn.Wait(noCache);
                Assert.AreEqual(4, results.Length);
                Assert.AreEqual("key1", results[0]);
                Assert.AreEqual("key2", results[1]);
                Assert.AreEqual("first", results[2]);
                Assert.AreEqual("second", results[3]);

                results = (object[])conn.Wait(cache);
                Assert.AreEqual(4, results.Length);
                Assert.AreEqual("key1", results[0]);
                Assert.AreEqual("key2", results[1]);
                Assert.AreEqual("first", results[2]);
                Assert.AreEqual("second", results[3]);
            }
        }
        [Test]
        public void KeysScripting()
        {
            using (var conn = GetScriptConn())
            {
                if (conn == null) return;
                conn.Strings.Set(0, "foo", "bar");
                var result = conn.Wait(conn.Scripting.Eval(0, "return redis.call('get', KEYS[1])", new[] { "foo" }, null));
                Assert.AreEqual("bar", result);
            }
        }

        [Test]
        public void FlushDetection()
        { // we don't expect this to handle everything; we just expect it to be predictable
            using (var conn = GetScriptConn(allowAdmin: true))
            {
                if (conn == null) return;
                conn.Strings.Set(0, "foo", "bar");
                var result = conn.Wait(conn.Scripting.Eval(0, "return redis.call('get', KEYS[1])", new[] { "foo" }, null));
                Assert.AreEqual("bar", result);

                // now cause all kinds of problems
                conn.Server.FlushScriptCache();

                // expect this one to fail
                try {
                    conn.Wait(conn.Scripting.Eval(0, "return redis.call('get', KEYS[1])", new[] { "foo" }, null));
                    Assert.Fail("Shouldn't have got here");
                }
                catch (RedisException) { }
                catch { Assert.Fail("Expected RedisException"); }

                result = conn.Wait(conn.Scripting.Eval(0, "return redis.call('get', KEYS[1])", new[] { "foo" }, null));
                Assert.AreEqual("bar", result);
            }
        }

        [Test]
        public void PrepareScript()
        {
            string[] scripts = { "return redis.call('get', KEYS[1])", "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}" };
            using (var conn = GetScriptConn(allowAdmin: true))
            {
                if (conn == null) return;
                conn.Server.FlushScriptCache();

                // when vanilla
                conn.Wait(conn.Scripting.Prepare(scripts));

                // when known to exist
                conn.Wait(conn.Scripting.Prepare(scripts));
            }
            using (var conn = GetScriptConn())
            {
                // when vanilla
                conn.Wait(conn.Scripting.Prepare(scripts));

                // when known to exist
                conn.Wait(conn.Scripting.Prepare(scripts));

                // when known to exist
                conn.Wait(conn.Scripting.Prepare(scripts));
            }
        }
    }
}
