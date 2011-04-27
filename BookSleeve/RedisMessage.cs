using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BookSleeve
{
    internal abstract class Message
    {
        private int messageState;
        internal bool ChangeState(MessageState from, MessageState to)
        {
            return Interlocked.CompareExchange(ref messageState, (int)to, (int)from) == (int)from;
        }

        public virtual bool MustSucceed { get { return false; } }
        private static readonly byte[] Crlf = Encoding.ASCII.GetBytes("\r\n");
        protected static readonly byte[] Ok = Encoding.ASCII.GetBytes("OK");
        private readonly byte[] command;
        private readonly int db = -1;
        public int Db { get { return db; } }

        private readonly byte[] expected;
        public byte[] Expected { get { return expected; } }
        public Message(int db, byte[] command)
        {
            this.db = db;
            this.command = command;
        }
        public override string ToString()
        {

            return Db >= 0 ? (Db.ToString() + ": " + Encoding.ASCII.GetString(command)) : Encoding.ASCII.GetString(command);

        }
        public string Body()
        {
            using (var ms = new MemoryStream())
            {
                Write(ms);
                return Encoding.UTF8.GetString(ms.GetBuffer(), 0, (int)ms.Length);
            }

        }
        public Message(int db, byte[] command, byte[] expected)
        {
            this.db = db;
            this.expected = expected;
            this.command = command;
        }
        private IMessageResult messageResult;
        internal void SetMessageResult(IMessageResult messageResult)
        {
            if (Interlocked.CompareExchange(ref this.messageResult, messageResult, null) != null)
            {
                throw new InvalidOperationException("A message-result is already assigned");
            }
        }
        protected void WriteCommand(Stream stream, int argCount)
        {
            stream.WriteByte((byte)'*');
            WriteRaw(stream, argCount + 1);
            WriteUnified(stream, command);
        }
        protected void WriteUnified(Stream stream, string value)
        {
            WriteUnified(stream, Encoding.UTF8.GetBytes(value));
        }
        protected void WriteUnified(Stream stream, byte[] value)
        {
            stream.WriteByte((byte)'$');
            WriteRaw(stream, value.Length);
            stream.Write(value, 0, value.Length);
            stream.Write(Crlf, 0, 2);
        }
        public abstract void Write(Stream stream);
        internal virtual void Complete(RedisResult result)
        {       
            var snapshot = Interlocked.Exchange(ref messageResult, null); // only run once
            ChangeState(MessageState.Sent, MessageState.Complete);
            if (snapshot != null)
            {
                snapshot.Complete(result);
            }
            
        }

        protected void WriteRaw(Stream stream, long value)
        {
            if (value >= 0 && value <= 9)
            {
                stream.WriteByte((byte)((int)'0' + (int)value));
            }
            else if (value < 0 && value >= -9)
            {
                stream.WriteByte((byte)'-');
                stream.WriteByte((byte)((int)'0' - (int)value));
            }
            else
            {
                var bytes = Encoding.ASCII.GetBytes(value.ToString());
                stream.Write(bytes, 0, bytes.Length);
            }
            stream.Write(Crlf, 0, 2);
        }
        private static readonly byte[]
            oneByteIntegerPrefix = Encoding.ASCII.GetBytes("$1\r\n"),
            twoByteIntegerPrefix = Encoding.ASCII.GetBytes("$2\r\n");
        protected void WriteUnified(Stream stream, long value)
        {
            // note: need to use string version "${len}\r\n{data}\r\n", not intger version ":{data}\r\n"
            // when this is part of a multi-block message (which unified *is*)
            if (value >= 0 && value <= 99)
            { // low positive integers are very common; special-case them
                int i = (int)value;
                if (i <= 9)
                {
                    stream.Write(oneByteIntegerPrefix, 0, oneByteIntegerPrefix.Length);
                    stream.WriteByte((byte)((int)'0' + i));
                }
                else
                {
                    stream.Write(twoByteIntegerPrefix, 0, twoByteIntegerPrefix.Length);
                    stream.WriteByte((byte)((int)'0' + (i / 10)));
                    stream.WriteByte((byte)((int)'0' + (i % 10)));
                }
            }
            else
            {
                // not *quite* as efficient, but fine
                var bytes = Encoding.ASCII.GetBytes(value.ToString());
                stream.WriteByte((byte)'$');
                WriteRaw(stream, bytes.Length);
                stream.Write(bytes, 0, bytes.Length);
            }
            stream.Write(Crlf, 0, 2);
        }
        protected void WriteUnified(Stream stream, double value)
        {
            int i;
            if (value >= int.MinValue && value <= int.MaxValue && (i = (int)value) == value)
            {
                WriteUnified(stream, i); // use integer handling
            }
            else
            {
                WriteUnified(stream, value.ToString("G", CultureInfo.InvariantCulture));
            }
        }
    }

    internal class SelectMessage : Message
    {
        public override bool MustSucceed { get { return true; } }
        public SelectMessage(int db) : base(db, select, Ok) { }
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 1);
            WriteUnified(stream, Db);
        }
        private static readonly byte[] select = Encoding.ASCII.GetBytes("SELECT");
    }

    internal class VanillaMessage : Message
    {
        private readonly bool mustSucceed;
        public static Message Info() { return new VanillaMessage(-1, info); }
        public static Message Quit() { return new VanillaMessage(-1, quit, Ok); }
        public static Message FlushDb(int db) { return new VanillaMessage(db, flushdb, Ok); }
        public static Message FlushAll() { return new VanillaMessage(-1, flushall, Ok); }
        public static Message RandomKey(int db) { return new VanillaMessage(db, randomkey); }
        private VanillaMessage(int db, byte[] command, byte[] expected) : base(db, command, expected) { this.mustSucceed = true; }
        private VanillaMessage(int db, byte[] command) : base(db, command) { }
        public override bool MustSucceed { get { return mustSucceed; } }
        private static readonly byte[]
            quit = Encoding.ASCII.GetBytes("QUIT"),
            flushdb = Encoding.ASCII.GetBytes("FLUSHDB"),
            flushall = Encoding.ASCII.GetBytes("FLUSHALL"),
            info = Encoding.ASCII.GetBytes("INFO"),
            randomkey = Encoding.ASCII.GetBytes("RANDOMKEY");
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 0);
        }
    }
    internal class MultiKeyMessage : Message
    {
        private readonly string[] keys;
        public static Message Del(int db, string[] keys)
        {
            return new MultiKeyMessage(db, del, keys);
        }
        public static Message Intersect(int db, string[] keys)
        {
            return new MultiKeyMessage(db, sinter, keys);
        }
        public static Message Union(int db, string[] keys)
        {
            return new MultiKeyMessage(db, sunion, keys);
        }
        static string[] Combine(string head, string[] tail)
        {
            if (tail == null || tail.Length == 0) return new[] { head };
            string[] result = new string[tail.Length + 1];
            result[0] = head;
            Array.Copy(tail, 0, result, 1, tail.Length);
            return result;
        }
        public static Message IntersectAndStore(int db, string to, string[] from)
        {
            return new MultiKeyMessage(db, sinterstore, Combine(to, from));
        }
        public static Message UnionAndStore(int db, string to, string[] from)
        {
            return new MultiKeyMessage(db, sunionstore, Combine(to, from));
        }
        public static Message Subscribe(string[] keys)
        {
            return new MultiKeyMessage(-1, subscribe, keys);
        }
        public static Message Unsubscribe(string[] keys)
        {
            return new MultiKeyMessage(-1, unsubscribe, keys);
        }
        public static Message PatternSubscribe(string[] keys)
        {
            return new MultiKeyMessage(-1, psubscribe, keys);
        }
        public static Message PatternUnsubscribe(string[] keys)
        {
            return new MultiKeyMessage(-1, punsubscribe, keys);
        }
        public static Message GetFromHash(int db, string hash, string subKey)
        {
            return new MultiKeyMessage(db, hget, new string[] { hash, subKey });
        }
        private MultiKeyMessage(int db, byte[] command, string[] keys)
            : base(db, command)
        {
            this.keys = keys;
        }
        public override void Write(Stream stream)
        {
            WriteCommand(stream, keys.Length);
            for (int i = 0; i < keys.Length; i++)
            {
                WriteUnified(stream, keys[i]);
            }
        }
        private static readonly byte[]
            del = Encoding.ASCII.GetBytes("DEL"),
            subscribe = Encoding.ASCII.GetBytes("SUBSCRIBE"),
            unsubscribe = Encoding.ASCII.GetBytes("UNSUBSCRIBE"),
            psubscribe = Encoding.ASCII.GetBytes("PSUBSCRIBE"),
            punsubscribe = Encoding.ASCII.GetBytes("PUNSUBSCRIBE"),
            sinter = Encoding.ASCII.GetBytes("SINTER"),
            sunion = Encoding.ASCII.GetBytes("SUNION"),
            sinterstore = Encoding.ASCII.GetBytes("SINTERSTORE"),
            sunionstore = Encoding.ASCII.GetBytes("SUNIONSTORE"),
            hget = Encoding.ASCII.GetBytes("HGET");
    }
    internal class RangeMessage : Message
    {
        public static Message SortedSetRange(int db, string key, int start, int stop, bool includeScores)
        {
            return new RangeMessage(db, zrange, key, start, stop, includeScores);
        }
        public static Message SortedSetRangeDescending(int db, string key, int start, int stop, bool includeScores)
        {
            return new RangeMessage(db, zrevrange, key, start, stop, includeScores);
        }
        public static Message RemoveFromSortedSetByScore(int db, string key, int start, int stop)
        {
            return new RangeMessage(db, zremrangebyscore, key, start, stop, false);
        }
        public static Message ListRange(int db, string key, int start, int stop)
        {
            return new RangeMessage(db, lrange, key, start, stop, false);
        }
        private RangeMessage(int db, byte[] command, string key, int start, int stop, bool includeScores)
            : base(db, command)
        {
            this.key = key;
            this.start = start;
            this.stop = stop;
            this.includeScores = includeScores;
        }
        private readonly bool includeScores;
        private readonly string key;
        private readonly int start, stop;
        private static readonly byte[]
            zrange = Encoding.ASCII.GetBytes("ZRANGE"),
            zrevrange = Encoding.ASCII.GetBytes("ZREVRANGE"),
            lrange = Encoding.ASCII.GetBytes("LRANGE"),
            zremrangebyscore = Encoding.ASCII.GetBytes("ZREMRANGEBYSCORE"),
            withscores = Encoding.ASCII.GetBytes("WITHSCORES");
        public override void Write(Stream stream)
        {
            WriteCommand(stream, includeScores ? 4 : 3);
            WriteUnified(stream, key);
            WriteUnified(stream, start);
            WriteUnified(stream, stop);
            if (includeScores) WriteUnified(stream, withscores);
        }
    }
    internal class KeyMessage : Message
    {
        public override string ToString()
        {
            return base.ToString() + " " + key;
        }
        private KeyMessage(int db, byte[] command, string key)
            : base(db, command)
        {
            this.key = key;
        }
        private KeyMessage(int db, byte[] command, string key, byte[] expected, bool mustSucceed = false)
            : base(db, command, expected)
        {
            this.key = key;
            this.mustSucceed = mustSucceed;
        }
        private readonly bool mustSucceed;
        public override bool MustSucceed { get { return mustSucceed; } }
        private readonly string key;
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 1);
            WriteUnified(stream, key);
        }

        internal static Message Del(int db, string key)
        {
            return new KeyMessage(db, del, key);
        }
        internal static Message Keys(int db, string pattern)
        {
            return new KeyMessage(db, keys, pattern);
        }
        internal static Message SetMembers(int db, string key)
        {
            return new KeyMessage(db, smembers, key);
        }
        internal static Message Subscribe(string key)
        {
            return new KeyMessage(-1, subscribe, key);
        }
        internal static Message Unsubscribe(string key)
        {
            return new KeyMessage(-1, unsubscribe, key);
        }
        public static Message PatternSubscribe(string key)
        {
            return new KeyMessage(-1, psubscribe, key);
        }
        public static Message PatternUnsubscribe(string key)
        {
            return new KeyMessage(-1, punsubscribe, key);
        }

        internal static Message Exists(int db, string key)
        {
            return new KeyMessage(db, exists, key);
        }

        internal static Message Ttl(int db, string key)
        {
            return new KeyMessage(db, ttl, key);
        }

        internal static Message Get(int db, string key)
        {
            return new KeyMessage(db, get, key);
        }

        internal static Message Incr(int db, string key)
        {
            return new KeyMessage(db, incr, key);
        }

        internal static Message Decr(int db, string key)
        {
            return new KeyMessage(db, decr, key);
        }
        internal static Message Auth(string password)
        {
            return new KeyMessage(-1, auth, password, Ok, true);
        }
        internal static Message ListLength(int db, string key)
        {
            return new KeyMessage(db, llen, key);
        }
        internal static Message CardinalityOfSet(int db, string key)
        {
            return new KeyMessage(db, scard, key);
        }
        internal static Message CardinalityOfSortedSet(int db, string key)
        {
            return new KeyMessage(db, zcard, key);
        }
        internal static Message LeftPop(int db, string key)
        {
            return new KeyMessage(db, lpop, key);
        }
        internal static Message RightPop(int db, string key)
        {
            return new KeyMessage(db, rpop, key);
        }
        internal static Message Persist(int db, string key)
        {
            return new KeyMessage(db, persist, key);
        }
        internal static Message GetHash(int db, string key)
        {
            return new KeyMessage(db, hgetall, key);
        }

        private readonly static byte[]
            get = Encoding.ASCII.GetBytes("GET"),
            ttl = Encoding.ASCII.GetBytes("TTL"),
            del = Encoding.ASCII.GetBytes("DEL"),
            llen = Encoding.ASCII.GetBytes("LLEN"),
            scard = Encoding.ASCII.GetBytes("SCARD"),
            zcard = Encoding.ASCII.GetBytes("ZCARD"),
            exists = Encoding.ASCII.GetBytes("EXISTS"),
            incr = Encoding.ASCII.GetBytes("INCR"),
            decr = Encoding.ASCII.GetBytes("DECR"),
            auth = Encoding.ASCII.GetBytes("AUTH"),
            keys = Encoding.ASCII.GetBytes("KEYS"),
            smembers = Encoding.ASCII.GetBytes("SMEMBERS"),
            persist = Encoding.ASCII.GetBytes("PERSIST"),
            subscribe = Encoding.ASCII.GetBytes("SUBSCRIBE"),
            unsubscribe = Encoding.ASCII.GetBytes("UNSUBSCRIBE"),
            psubscribe = Encoding.ASCII.GetBytes("PSUBSCRIBE"),
            punsubscribe = Encoding.ASCII.GetBytes("PUNSUBSCRIBE"),
            lpop = Encoding.ASCII.GetBytes("LPOP"),
            rpop = Encoding.ASCII.GetBytes("RPOP"),
            hgetall = Encoding.ASCII.GetBytes("HGETALL");
    }
    internal class KeyScoreMessage : Message
    {
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 2);
            WriteUnified(stream, key);
            WriteUnified(stream, score);
        }
        public static Message Expire(int db, string key, int seconds)
        {
            return new KeyScoreMessage(db, expire, key, seconds);
        }
        public static Message Move(int db, string key, int targetDb)
        {
            return new KeyScoreMessage(db, move, key, targetDb);
        }
        private KeyScoreMessage(int db, byte[] cmd, string key, int score)
            : base(db, cmd)
        {
            this.key = key;
            this.score = score;
        }
        private readonly static byte[]
            expire = Encoding.ASCII.GetBytes("EXPIRE"),
            move = Encoding.ASCII.GetBytes("MOVE");
        private readonly string key;
        private readonly int score;
        public override string ToString()
        {
            return base.ToString() + " " + key;
        }
    }

    internal class KeyValueMessage : Message
    {
        public override string ToString()
        {
            return base.ToString() + " " + key;
        }
        public static Message Set(int db, string key, string value) { return new KeyValueMessage(db, set, key, value, true); }
        public static Message Set(int db, string key, byte[] value) { return new KeyValueMessage(db, set, key, value, true); }
        public static Message SetIfNotExists(int db, string key, string value) { return new KeyValueMessage(db, setnx, key, value, false); }
        public static Message SetIfNotExists(int db, string key, byte[] value) { return new KeyValueMessage(db, setnx, key, value, false); }
        public static Message Append(int db, string key, string value) { return new KeyValueMessage(db, append, key, value, false); }
        public static Message Append(int db, string key, byte[] value) { return new KeyValueMessage(db, append, key, value, false); }
        public static Message LeftPush(int db, string key, string value) { return new KeyValueMessage(db, lpush, key, value, false); }
        public static Message LeftPush(int db, string key, byte[] value) { return new KeyValueMessage(db, lpush, key, value, false); }
        public static Message RightPush(int db, string key, string value) { return new KeyValueMessage(db, rpush, key, value, false); }
        public static Message RightPush(int db, string key, byte[] value) { return new KeyValueMessage(db, rpush, key, value, false); }
        public static Message AddToSet(int db, string key, string value) { return new KeyValueMessage(db, sadd, key, value, false); }
        public static Message AddToSet(int db, string key, byte[] value) { return new KeyValueMessage(db, sadd, key, value, false); }
        public static Message RemoveFromSet(int db, string key, string value) { return new KeyValueMessage(db, srem, key, value, false); }
        public static Message RemoveFromSet(int db, string key, byte[] value) { return new KeyValueMessage(db, srem, key, value, false); }
        public static Message Publish(string key, string value) { return new KeyValueMessage(-1, publish, key, value, false); }
        public static Message Publish(string key, byte[] value) { return new KeyValueMessage(-1, publish, key, value, false); }
        public static Message IsMemberOfSet(int db, string key, string value) { return new KeyValueMessage(db, sismember, key, value, false); }
        public static Message IsMemberOfSet(int db, string key, byte[] value) { return new KeyValueMessage(db, sismember, key, value, false); }
        public static Message PopFromListPushToList(int db, string from, string to) { return new KeyValueMessage(db, rpoplpush, from, to,false); }

        public static Message Rename(int db, string from, string to) { return new KeyValueMessage(db, rename, from, to, true); }
        public static Message RenameIfNotExists(int db, string from, string to) { return new KeyValueMessage(db, renamenx, from, to, false); }

        private KeyValueMessage(int db, byte[] command, string key, string value, bool ok)
            : this(db, command, key, value == null ? (byte[])null : Encoding.UTF8.GetBytes(value), ok) { }
        private KeyValueMessage(int db, byte[] command, string key, byte[] value, bool ok)
            : base(db, command, ok ? Ok : null)
        {
            if (value == null) throw new ArgumentNullException("value");
            this.key = key;
            this.value = value;
        }
        private readonly static byte[]
            set = Encoding.ASCII.GetBytes("SET"),
            setnx = Encoding.ASCII.GetBytes("SETNX"),
            append = Encoding.ASCII.GetBytes("APPEND"),
            sadd = Encoding.ASCII.GetBytes("SADD"),
            srem = Encoding.ASCII.GetBytes("SREM"),
            publish = Encoding.ASCII.GetBytes("PUBLISH"),
            lpush = Encoding.ASCII.GetBytes("LPUSH"),
            rpush = Encoding.ASCII.GetBytes("RPUSH"),
            sismember = Encoding.ASCII.GetBytes("SISMEMBER"),
            rpoplpush = Encoding.ASCII.GetBytes("RPOPLPUSH"),
            rename = Encoding.ASCII.GetBytes("RENAME"),
            renamenx = Encoding.ASCII.GetBytes("RENAMENX");
        private readonly string key;
        private readonly byte[] value;
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 2);
            WriteUnified(stream, key);
            WriteUnified(stream, value);
        }
    }
    internal class KeyScoreValueMessage : Message
    {
        public override string ToString()
        {
            return base.ToString() + " " + key;
        }
        public static Message AddToSortedSet(int db, string key, double score, string value) { return new KeyScoreValueMessage(db, zadd, key, score, value, false); }
        public static Message AddToSortedSet(int db, string key, double score, byte[] value) { return new KeyScoreValueMessage(db, zadd, key, score, value, false); }

        public static Message IncrementSortedSet(int db, string key, double score, string value) { return new KeyScoreValueMessage(db, zincrby, key, score, value, false); }
        public static Message IncrementSortedSet(int db, string key, double score, byte[] value) { return new KeyScoreValueMessage(db, zincrby, key, score, value, false); }

        public static Message SetWithExpiry(int db, string key, int seconds, string value) { return new KeyScoreValueMessage(db, setex, key, seconds, value, true); }
        public static Message SetWithExpiry(int db, string key, int seconds, byte[] value) { return new KeyScoreValueMessage(db, setex, key, seconds, value, true); }
        
        private KeyScoreValueMessage(int db, byte[] command, string key, double score, string value, bool ok)
            : this(db, command, key, score, value == null ? (byte[])null : Encoding.UTF8.GetBytes(value), ok) { }
        private KeyScoreValueMessage(int db, byte[] command, string key, double score, byte[] value, bool ok)
            : base(db, command, ok ? Ok : null)
        {
            if (value == null) throw new ArgumentNullException("value");
            this.key = key;
            this.score = score;
            this.value = value;
        }
        private readonly static byte[]
            zadd = Encoding.ASCII.GetBytes("ZADD"),
            zincrby = Encoding.ASCII.GetBytes("ZINCRBY"),
            setex = Encoding.ASCII.GetBytes("SETEX");
        private readonly string key;
        private readonly double score;
        private readonly byte[] value;
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 3);
            WriteUnified(stream, key);
            WriteUnified(stream, score);
            WriteUnified(stream, value);
        }
    }
    internal class MultiKeyValueMessage : Message
    {
        public static Message IncrementHash(int db, string hashKey, string subKey, int by) 
        {
            return new MultiKeyValueMessage(db, hincrby, hashKey, subKey, by.ToString());
        }

        private MultiKeyValueMessage(int db, byte[] command, string key, string subKey, string value)
            : this(db, command, key, subKey, value == null ? (byte[])null : Encoding.UTF8.GetBytes(value)) { }
        private MultiKeyValueMessage(int db, byte[] command, string key, string subKey, byte[] value)
            : base(db, command, null)
        {
            if (value == null) throw new ArgumentNullException("value");

            this.key = key;
            this.subKey = subKey;
            this.value = value;
        }
        private readonly string key;
        private readonly string subKey;
        private readonly byte[] value;
        private readonly static byte[]
            hincrby = Encoding.ASCII.GetBytes("HINCRBY");
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 3);
            WriteUnified(stream, key);
            WriteUnified(stream, subKey);
            WriteUnified(stream, value);
        }
    }
    internal class DeltaMessage : Message
    {
        public override string ToString()
        {
            return base.ToString() + " " + key + " " + value;
        }
        private readonly static byte[]
            incrby = Encoding.ASCII.GetBytes("INCRBY"),
            decrby = Encoding.ASCII.GetBytes("DECRBY");
        private readonly string key;
        private readonly long value;
        public DeltaMessage(int db, string key, long value)
            : base(db, value < 0 ? decrby : incrby)
        {
            this.key = key;
            this.value = value < 0 ? -value : value;
        }
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 2);
            WriteUnified(stream, key);
            WriteUnified(stream, value);
        }

    }

    internal class SlaveMessage : Message
    {
        private readonly string host, port;
        public SlaveMessage(string host, int port) : this(host, port.ToString())
        {
            if(port <= 0) throw new ArgumentOutOfRangeException("port");
        }
        private SlaveMessage(string host, string port) : base(-1, slaveof,Ok)
        {
            this.host = host;
            this.port = port;
        }
        public override bool MustSucceed
        {
            get{return true;}
        }
        private readonly static byte[]
           slaveof = Encoding.ASCII.GetBytes("SLAVEOF");
        public static SlaveMessage Master() { return new SlaveMessage("NO", "ONE"); }
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 2);
            WriteUnified(stream, host);
            WriteUnified(stream, port);
        }
    }
    internal class PingMessage : Message
    {
        private DateTime created, sent, received;
        private readonly static byte[]
           ping = Encoding.ASCII.GetBytes("PING"),
           pong = Encoding.ASCII.GetBytes("PONG");
        public PingMessage()
            : base(-1, ping, pong)
        {
            created = DateTime.UtcNow;
        }
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 0);
            if (sent == DateTime.MinValue) sent = DateTime.UtcNow;
     
        }
        internal override void Complete(RedisResult result)
        {
            received = DateTime.UtcNow;
            base.Complete(result.IsError ? result : new RedisResult.TimingRedisResult(
                sent - created, received - sent));
        }
        public override bool MustSucceed { get { return true; } }
    }
}
