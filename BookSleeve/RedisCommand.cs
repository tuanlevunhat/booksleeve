﻿using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BookSleeve
{
    abstract class RedisMessage
    {
        private static readonly byte[][] literals;
        static RedisMessage()
        {
            var arr = Enum.GetValues(typeof (RedisLiteral));
            literals = new byte[arr.Length][];
            foreach(RedisLiteral literal in arr)
            {
                literals[(int) literal] = Encoding.ASCII.GetBytes(literal.ToString().ToUpperInvariant());
            }
        }

        private readonly int db;
        private readonly RedisLiteral command;
        private RedisLiteral expected = RedisLiteral.None;
        private bool critical;
        public bool MustSucceed
        {
            get { return critical; }
        }
        public RedisMessage Critical()
        {
            critical = true;
            return this;
        }
        public RedisMessage ExpectOk()
        {
            return Expect(RedisLiteral.OK);
        }
        public RedisMessage Expect(RedisLiteral result)
        {
            if (expected == RedisLiteral.None)
            {
                expected = result;
            } else
            {
                throw new InvalidOperationException();
            }
            return this;
        }
        public byte[] Expected
        {
            get
            {
                return expected == RedisLiteral.None ? null : literals[(int) expected];
            }
        }
        private IMessageResult messageResult;
        internal void SetMessageResult(IMessageResult messageResult)
        {
            if (Interlocked.CompareExchange(ref this.messageResult, messageResult, null) != null)
            {
                throw new InvalidOperationException("A message-result is already assigned");
            }
        }
        internal virtual void Complete(RedisResult result)
        {
            var snapshot = Interlocked.Exchange(ref messageResult, null); // only run once
            ChangeState(MessageState.Sent, MessageState.Complete);
            if (snapshot != null)
            {
                snapshot.Complete(result);
            }
        }
        private int messageState;
        internal bool ChangeState(MessageState from, MessageState to)
        {
            return Interlocked.CompareExchange(ref messageState, (int)to, (int)from) == (int)from;
        }
        public int Db { get { return db; } }
        public RedisLiteral Command { get { return command; } }
        protected RedisMessage(int db, RedisLiteral command)
        {
            this.db = db;
            this.command = command;
        }
        public static RedisMessage Create(int db, RedisLiteral command)
        {
            return new RedisMessageNix(db, command);
        }
        public static RedisMessage Create(int db, RedisLiteral command, RedisParameter arg0)
        {
            return new RedisMessageUni(db, command, arg0);
        }
        public static RedisMessage Create(int db, RedisLiteral command, string arg0)
        {
            return new RedisMessageUniString(db, command, arg0);
        }
        public static RedisMessage Create(int db, RedisLiteral command, string arg0, string arg1)
        {
            return new RedisMessageBiString(db, command, arg0, arg1);
        }
        public static RedisMessage Create(int db, RedisLiteral command, string arg0, string[] args)
        {
            if (args == null) return Create(db, command, arg0);
            switch(args.Length)
            {
                case 0:
                    return Create(db, command, arg0);
                case 1:
                    return Create(db, command, arg0, args[0]);
                default:
                    return new RedisMessageMultiString(db, command, arg0, args);
            }
        }
        public static RedisMessage Create(int db, RedisLiteral command, RedisParameter arg0, RedisParameter arg1)
        {
            return new RedisMessageBi(db, command, arg0, arg1);
        }
        public static RedisMessage Create(int db, RedisLiteral command, RedisParameter arg0, RedisParameter arg1, RedisParameter arg2)
        {
            return new RedisMessageTri(db, command, arg0, arg1, arg2);
        }
        public static RedisMessage Create(int db, RedisLiteral command, RedisParameter arg0, RedisParameter arg1, RedisParameter arg2, RedisParameter arg3)
        {
            return new RedisMessageQuad(db, command, arg0, arg1, arg2, arg3);
        }
        public abstract void Write(Stream stream);

        public static RedisMessage Create(int db, RedisLiteral command, string[] args)
        {
            if (args == null) return new RedisMessageNix(db, command);
            switch (args.Length)
            {
                case 0: return new RedisMessageNix(db, command);
                case 1: return new RedisMessageUni(db, command, args[0]);
                case 2: return new RedisMessageBi(db, command, args[0], args[1]);
                case 3: return new RedisMessageTri(db, command, args[0], args[1], args[2]);
                case 4: return new RedisMessageQuad(db, command, args[0], args[1], args[2], args[3]);
                default: return new RedisMessageMulti(db, command, Array.ConvertAll(args, s => (RedisParameter)s));
            }
        }
        public static RedisMessage Create(int db, RedisLiteral command, params RedisParameter[] args)
        {
            if (args == null) return new RedisMessageNix(db, command);
            switch(args.Length)
            {
                case 0: return new RedisMessageNix(db, command);
                case 1: return new RedisMessageUni(db, command, args[0]);
                case 2: return new RedisMessageBi(db, command, args[0], args[1]);
                case 3: return new RedisMessageTri(db, command, args[0], args[1], args[2]);
                case 4: return new RedisMessageQuad(db, command, args[0], args[1], args[2], args[3]);
                default: return new RedisMessageMulti(db, command, args);
            }
        }
        public override string ToString()
        {
            return db >= 0 ? (db + ": " + command) : command.ToString();
        }
        protected void WriteCommand(Stream stream, int argCount)
        {
            stream.WriteByte((byte)'*');
            WriteRaw(stream, argCount + 1);
            WriteUnified(stream, command);
        }
        protected static void WriteUnified(Stream stream, RedisLiteral value)
        {
            WriteUnified(stream, literals[(int)value]);
        }
        protected static void WriteUnified(Stream stream, string value)
        {
            WriteUnified(stream, Encoding.UTF8.GetBytes(value));
        }
        protected static void WriteUnified(Stream stream, byte[] value)
        {
            stream.WriteByte((byte)'$');
            WriteRaw(stream, value.Length);
            stream.Write(value, 0, value.Length);
            stream.Write(Crlf, 0, 2);
        }
        protected static void WriteUnified(Stream stream, long value)
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
        protected static void WriteUnified(Stream stream, double value)
        {
            int i;
            if (value >= int.MinValue && value <= int.MaxValue && (i = (int)value) == value)
            {
                WriteUnified(stream, i); // use integer handling
            }
            else
            {
                WriteUnified(stream, ToString(value));
            }
        }
        private static string ToString(long value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }
        private static string ToString(double value)
        {
            return value.ToString("G", CultureInfo.InvariantCulture);
        }
        protected static void WriteRaw(Stream stream, long value)
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
        private static readonly byte[] Crlf = Encoding.ASCII.GetBytes("\r\n");

        sealed class RedisMessageNix : RedisMessage
        {
            public RedisMessageNix(int db, RedisLiteral command)
                : base(db, command)
            {}
            public override void Write(Stream stream)
            {
                WriteCommand(stream, 0);
            }
        }
        sealed class RedisMessageUni : RedisMessage
        {
            private readonly RedisParameter arg0;
            public RedisMessageUni(int db, RedisLiteral command, RedisParameter arg0) : base(db, command)
            {
                this.arg0 = arg0;
            }
            public override void Write(Stream stream)
            {
                WriteCommand(stream, 1);
                arg0.Write(stream);
            }
        }
        sealed class RedisMessageUniString : RedisMessage
        {
            private readonly string arg0;
            public RedisMessageUniString(int db, RedisLiteral command, string arg0)
                : base(db, command)
            {
                this.arg0 = arg0;
            }
            public override void Write(Stream stream)
            {
                WriteCommand(stream, 1);
                WriteUnified(stream, arg0);
            }
        }
        sealed class RedisMessageBiString : RedisMessage
        {
            private readonly string arg0, arg1;
            public RedisMessageBiString(int db, RedisLiteral command, string arg0, string arg1)
                : base(db, command)
            {
                this.arg0 = arg0;
                this.arg1 = arg1;
            }
            public override void Write(Stream stream)
            {
                WriteCommand(stream, 2);
                WriteUnified(stream, arg0);
                WriteUnified(stream, arg1);
            }
        }
        sealed class RedisMessageMultiString : RedisMessage
        {
            private readonly string arg0;
            private readonly string[] args;
            public RedisMessageMultiString(int db, RedisLiteral command, string arg0, string[] args)
                : base(db, command)
            {
                this.arg0 = arg0;
                this.args = args;
            }
            public override void Write(Stream stream)
            {
                WriteCommand(stream, 1 + args.Length);
                WriteUnified(stream, arg0);
                for (int i = 0; i < args.Length; i++ )
                    WriteUnified(stream, args[i]);
            }
        }
        sealed class RedisMessageBi : RedisMessage
        {
            private readonly RedisParameter arg0, arg1;
            public RedisMessageBi(int db, RedisLiteral command, RedisParameter arg0, RedisParameter arg1)
                : base(db, command)
            {
                this.arg0 = arg0;
                this.arg1 = arg1;
            }
            public override void Write(Stream stream)
            {
                WriteCommand(stream, 2);
                arg0.Write(stream);
                arg1.Write(stream);
            }
        }
        sealed class RedisMessageTri : RedisMessage
        {
            private readonly RedisParameter arg0, arg1, arg2;
            public RedisMessageTri(int db, RedisLiteral command, RedisParameter arg0, RedisParameter arg1, RedisParameter arg2)
                : base(db, command)
            {
                this.arg0 = arg0;
                this.arg1 = arg1;
                this.arg2 = arg2;
            }
            public override void Write(Stream stream)
            {
                WriteCommand(stream, 3);
                arg0.Write(stream);
                arg1.Write(stream);
                arg2.Write(stream);
            }
        }
        sealed class RedisMessageQuad : RedisMessage
        {
            private readonly RedisParameter arg0, arg1, arg2, arg3;
            public RedisMessageQuad(int db, RedisLiteral command, RedisParameter arg0, RedisParameter arg1, RedisParameter arg2, RedisParameter arg3)
                : base(db, command)
            {
                this.arg0 = arg0;
                this.arg1 = arg1;
                this.arg2 = arg2;
                this.arg3 = arg3;
            }
            public override void Write(Stream stream)
            {
                WriteCommand(stream, 4);
                arg0.Write(stream);
                arg1.Write(stream);
                arg2.Write(stream);
                arg3.Write(stream);
            }
        }
        sealed class RedisMessageMulti : RedisMessage
        {
            private readonly RedisParameter[] args;
            public RedisMessageMulti(int db, RedisLiteral command, RedisParameter[] args) : base(db, command)
            {
                this.args = args;
            }
            public override void Write(Stream stream)
            {
                if(args == null)
                {
                    WriteCommand(stream, 0);
                }
                else
                {
                    WriteCommand(stream, args.Length);
                    for (int i = 0; i < args.Length; i++)
                        args[i].Write(stream);
                }
            }
        }
        internal abstract class RedisParameter
        {
            public static implicit operator RedisParameter(RedisLiteral value) { return new RedisLiteralParameter(value); }
            public static implicit operator RedisParameter(string value) { return new RedisStringParameter(value); }
            public static implicit operator RedisParameter(byte[] value) { return new RedisBlobParameter(value); }
            public static implicit operator RedisParameter(long value) { return new RedisInt64Parameter(value); }
            public static implicit operator RedisParameter(double value) { return new RedisDoubleParameter(value); }
            public static RedisParameter Range(long value, bool inclusive)
            {
                if(inclusive) return new RedisInt64Parameter(value);
                return new RedisStringParameter("(" + RedisMessage.ToString(value));
            }
            public static RedisParameter Range(double value, bool inclusive)
            {
                if (inclusive) return new RedisDoubleParameter(value);
                return new RedisStringParameter("(" + RedisMessage.ToString(value));
            }
            public abstract void Write(Stream stream);
            class RedisLiteralParameter : RedisParameter
            {
                private readonly RedisLiteral value;
                public RedisLiteralParameter(RedisLiteral value) { this.value = value; }
                public override void Write(Stream stream)
                {
                    WriteUnified(stream, value);
                }
            }
            class RedisStringParameter : RedisParameter
            {
                private readonly string value;
                public RedisStringParameter(string value) { this.value = value; }
                public override void Write(Stream stream)
                {
                    WriteUnified(stream, value);
                }
            }
            class RedisBlobParameter : RedisParameter
            {
                private readonly byte[] value;
                public RedisBlobParameter(byte[] value) { this.value = value; }
                public override void Write(Stream stream)
                {
                    WriteUnified(stream, value);
                }
            }
            class RedisInt64Parameter : RedisParameter
            {
                private readonly long value;
                public RedisInt64Parameter(long value) { this.value = value; }
                public override void Write(Stream stream)
                {
                    WriteUnified(stream, value);
                }
            }
            class RedisDoubleParameter : RedisParameter
            {
                private readonly double value;
                public RedisDoubleParameter(double value) { this.value = value; }
                public override void Write(Stream stream)
                {
                    WriteUnified(stream, value);
                }
            }
        }
    }
    internal class QueuedMessage : RedisMessage
    {
        private readonly RedisMessage innnerMessage;

        public RedisMessage InnerMessage { get { return innnerMessage; } }
        public QueuedMessage(RedisMessage innnerMessage)
            : base(innnerMessage.Db, innnerMessage.Command)
        {
            if (innnerMessage == null) throw new ArgumentNullException("innnerMessage");
            this.innnerMessage = innnerMessage;
            Expect(RedisLiteral.QUEUED).Critical();
        }
        public override void Write(Stream stream)
        {
            innnerMessage.Write(stream);
        }
    }
    internal class MultiMessage : RedisMessage
    {
        public MultiMessage(RedisConnection parent, RedisMessage[] messages)
            : base(-1, RedisLiteral.MULTI)
        {
            exec = new ExecMessage(parent);
            this.messages = messages;
            ExpectOk();
        }
        private RedisMessage[] messages;
        public RedisMessage[] GetPendingMessages() { return messages; }
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 0);
        }
        private readonly ExecMessage exec;
        public RedisMessage Execute(List<QueuedMessage> queued)
        {
            exec.SetQueued(queued);
            return exec;
        }
        public Task Completion { get { return exec.Completion; } }
        private readonly static byte[]
            multi = Encoding.ASCII.GetBytes("MULTI");
    }
    internal class ExecMessage : RedisMessage, IMessageResult
    {
        private RedisConnection parent;
        public ExecMessage(RedisConnection parent)
            : base(-1, RedisLiteral.EXEC)
        {
            if (parent == null) throw new ArgumentNullException("parent");
            SetMessageResult(this);
            this.parent = parent;
        }
        private readonly TaskCompletionSource<bool> completion = new TaskCompletionSource<bool>();
        private readonly static byte[]
            exec = Encoding.ASCII.GetBytes("EXEC");
        public override void Write(Stream stream)
        {
            WriteCommand(stream, 0);
        }
        public Task Completion { get { return completion.Task; } }
        private QueuedMessage[] queued;
        internal void SetQueued(List<QueuedMessage> queued)
        {
            if (queued == null) throw new ArgumentNullException("queued");
            if (this.queued != null) throw new InvalidOperationException();
            this.queued = queued.ToArray();
        }

        void IMessageResult.Complete(RedisResult result)
        {
            if (result.IsCancellation)
            {
                completion.SetCanceled();
            }
            else if (result.IsError)
            {
                completion.SetException(result.Error());
            }
            else
            {
                try
                {
                    if (queued == null) throw new InvalidOperationException("Nothing was queued (null)!");
                    var items = result.ValueItems;
                    if (items.Length != queued.Length) throw new InvalidOperationException(string.Format("{0} results expected, {1} received", queued.Length, items.Length));

                    for (int i = 0; i < items.Length; i++)
                    {
                        RedisResult reply = items[i];
                        var ctx = parent.ProcessReply(ref reply, queued[i].InnerMessage);
                        parent.ProcessCallbacks(ctx, reply);
                    }
                    completion.SetResult(true);
                }
                catch (Exception ex)
                {
                    completion.SetException(ex);
                    throw;
                }
            }
        }
    }
    internal class PingMessage : RedisMessage
    {
        private readonly DateTime created;
        private DateTime sent, received;
        public PingMessage()
            : base(-1, RedisLiteral.PING)
        {
            created = DateTime.UtcNow;
            Expect(RedisLiteral.PONG).Critical();
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
    }
    enum RedisLiteral
    {
        None = 0,
        // responses
        OK,QUEUED,PONG,
        // commands (extracted from http://redis.io/commands)
        APPEND,AUTH,BGREWRITEAOF,BGSAVE,BLPOP,BRPOP,BRPOPLPUSH,CONFIG,GET,SET,RESETSTAT,DBSIZE,DEBUG,OBJECT,SEGFAULT,DECR,DECRBY,DEL,DISCARD,ECHO,EXEC,EXISTS,EXPIRE,EXPIREAT,FLUSHALL,FLUSHDB,GETBIT,GETRANGE,GETSET,HDEL,HEXISTS,HGET,HGETALL,HINCRBY,HKEYS,HLEN,HMGET,HMSET,HSET,HSETNX,HVALS,INCR,INCRBY,INFO,KEYS,LASTSAVE,LINDEX,LINSERT,LLEN,LPOP,LPUSH,LPUSHX,LRANGE,LREM,LSET,LTRIM,MGET,MONITOR,MOVE,MSET,MSETNX,MULTI,PERSIST,PING,PSUBSCRIBE,PUBLISH,PUNSUBSCRIBE,QUIT,RANDOMKEY,RENAME,RENAMENX,RPOP,RPOPLPUSH,RPUSH,RPUSHX,SADD,SAVE,SCARD,SDIFF,SDIFFSTORE,SELECT,SETBIT,SETEX,SETNX,SETRANGE,SHUTDOWN,SINTER,SINTERSTORE,SISMEMBER,SLAVEOF,SLOWLOG,SMEMBERS,SMOVE,SORT,SPOP,SRANDMEMBER,SREM,STRLEN,SUBSCRIBE,SUNION,SUNIONSTORE,SYNC,TTL,TYPE,UNSUBSCRIBE,UNWATCH,WATCH,ZADD,ZCARD,ZCOUNT,ZINCRBY,ZINTERSTORE,ZRANGE,ZRANGEBYSCORE,ZRANK,ZREM,ZREMRANGEBYRANK,ZREMRANGEBYSCORE,ZREVRANGE,ZREVRANGEBYSCORE,ZREVRANK,ZSCORE,ZUNIONSTORE,
        // other
        NO,ONE,WITHSCORES,BEFORE,AFTER
        
    }
}