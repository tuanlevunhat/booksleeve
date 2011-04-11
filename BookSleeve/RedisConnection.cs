using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BookSleeve
{
    public sealed class RedisConnection : RedisConnectionBase
    {
        internal const bool DefaultAllowAdmin = false;
        public RedisConnection(string host, int port = 6379, int ioTimeout = -1, string password = null, int maxUnsent = int.MaxValue, bool allowAdmin = DefaultAllowAdmin, int syncTimeout = DefaultSyncTimeout)
            : base(host, port, ioTimeout, password, maxUnsent, syncTimeout)
        {
            this.allowAdmin = allowAdmin;
            this.sent = new Queue<Message>();
        }

        private RedisSubscriberConnection subscriberChannel;

        private RedisSubscriberConnection SubscriberFactory()
        {
            var conn = new RedisSubscriberConnection(Host, Port, IOTimeout, Password, 100);
            conn.Error += OnError;
            conn.Open();
            return conn;
        }
        public RedisSubscriberConnection GetOpenSubscriberChannel()
        {
            // use (atomic) reference test for a lazy quick answer
            if (subscriberChannel != null) return subscriberChannel;
            RedisSubscriberConnection newValue = null;
            try
            {
                newValue = SubscriberFactory();
                if (Interlocked.CompareExchange(ref subscriberChannel, newValue, null) == null)
                {
                    // the field was null; we won the race; happy happy
                    var tmp = newValue;
                    newValue = null;
                    return tmp;
                }
                else
                {
                    // we lost the race; use Interlocked to be sure we report the right thing
                    return Interlocked.CompareExchange(ref subscriberChannel, null, null);
                }
            }
            finally
            {
                // if newValue still has a value, we failed to swap it; perhaps we
                // lost the thread race, or perhaps an exception was thrown; either way,
                // that sucka is toast
                using (newValue as IDisposable) 
                {
                }
            }
        }

        public override void Dispose()
        {
            var subscribers = subscriberChannel;
            if (subscribers != null) subscribers.Dispose();
            base.Dispose();
        }
        internal override object ProcessReply(ref RedisResult result)
        {
            Message message;
            lock (sent)
            {
                int count = sent.Count;
                if (count == 0) throw new RedisException("Data received with no matching message");
                message = sent.Dequeue();
                if (count == 1) Monitor.Pulse(sent); // in case the outbound stream is closing and needs to know we're up-to-date
            }
            byte[] expected;
            if (!result.IsError && (expected = message.Expected) != null)
            {
                result = result.IsMatch(expected)
                ? RedisResult.Pass : RedisResult.Error(result.ValueString);
            }


            if (result.IsError && message.MustSucceed)
            {
                throw new RedisException("A critical operation failed: " + message.ToString());
            }
            return message;
        }
        internal override void ProcessCallbacks(object ctx, RedisResult result)
        {
            CompleteMessage((Message)ctx, result);
        }
        protected override void ShuttingDown(Exception error)
        {
            base.ShuttingDown(error);
            Message message;
            RedisResult result = null;

            lock (sent)
            {
                if (sent.Count > 0)
                {
                    result = RedisResult.Error(
                        error == null ? "The server terminated before a reply was received"
                        : ("Error processing data: " + error.Message));
                }
                while (sent.Count > 0)
                { // notify clients of things that just didn't happen

                    message = sent.Dequeue();
                    CompleteMessage(message, result);
                }
            }
        }
        private readonly Queue<Message> sent;
        private readonly bool allowAdmin;
        public Counters GetCounters()
        {
            int messagesSent, messagesReceived, queueJumpers, messagesCancelled, unsent, errorMessages, timeouts;
            GetCounterValues(out messagesSent, out messagesReceived, out queueJumpers, out messagesCancelled, out unsent, out errorMessages, out timeouts);
            return new Counters(
                messagesSent, messagesReceived, queueJumpers, messagesCancelled,
                timeouts, unsent, errorMessages,
                GetSentCount(),
                GetDbUsage(),
                // important that ping happens last, as this may artificially drain the queues
                (int)Wait(Ping())
            );
        }
        private int GetSentCount() { lock (sent) { return sent.Count; } }
        private DateTime opened;
        internal override void RecordSent(Message message, bool drainFirst)
        {
            base.RecordSent(message, drainFirst);

            lock (sent)
            {
                if (drainFirst && sent.Count != 0)
                {
                    // drain it down; the dequeuer will wake us
                    Monitor.Wait(sent);
                }
                sent.Enqueue(message);
            }
        }
        protected override void OnOpened()
        {
            base.OnOpened();
            this.opened = DateTime.UtcNow;
        }

        public Task<byte[]> Get(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBytes(KeyMessage.Get(db, key), false);
        }
        public Task<string> GetString(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteString(KeyMessage.Get(db, key), false);
        }
        public Task<long> TimeToLive(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyMessage.Ttl(db, key), queueJump);
        }
        public Task<bool> ContainsKey(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyMessage.Exists(db, key), queueJump);
        }


        public Task<string[]> GetKeysSync(int db, string pattern, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiString(KeyMessage.Keys(db, pattern), queueJump);
        }

        public Task<byte[][]> GetMembersOfSet(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiBytes(KeyMessage.SetMembers(db, key), queueJump);
        }

        public Task<long> Increment(int db, string key, bool queueJump = false)
        {
            return ExecuteInt64(GetDelta(db, key, 1), queueJump);
        }
        public Task<long> IncrementBy(int db, string key, long value, bool queueJump = false)
        {
            return ExecuteInt64(GetDelta(db, key, value), queueJump);
        }
        public Task<long> DecrementBy(int db, string key, long value, bool queueJump = false)
        {
            return ExecuteInt64(GetDelta(db, key, -value), queueJump);
        }
        public Task<long> Decrement(int db, string key, bool queueJump = false)
        {
            return ExecuteInt64(GetDelta(db, key, -1), queueJump);
        }
        static Message GetDelta(int db, string key, long value)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            switch (value)
            {
                case -1: return KeyMessage.Decr(db, key);
                case 1: return KeyMessage.Incr(db, key);
                default: return new DeltaMessage(db, key, value);
            }
        }

        public Task PromoteToMaster()
        {
            return ExecuteVoid(SlaveMessage.Master(), false);
        }
 
        public Task Set(int db, string key, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteVoid(KeyValueMessage.Set(db, key, value), queueJump);
        }
        public Task Set(int db, string key, string value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteVoid(KeyValueMessage.Set(db, key, value), queueJump);
        }
        public Task<bool> SetIfNotExists(int db, string key, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyValueMessage.SetIfNotExists(db, key, value), queueJump);
        }

        public Task<string> Append(int db, string key, string value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteString(KeyValueMessage.Append(db, key, value), queueJump);
        }
        public Task<byte[]> Append(int db, string key, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBytes(KeyValueMessage.Append(db, key, value), queueJump);
        }
        public Task<bool> AddToSet(int db, string key, string value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyValueMessage.AddToSet(db, key, value), queueJump);
        }
        public Task<bool> AddToSet(int db, string key, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyValueMessage.AddToSet(db, key, value), queueJump);
        }

        public Task<bool> IsMemberOfSet(int db, string key, string value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyValueMessage.IsMemberOfSet(db, key, value), queueJump);
        }
        public Task<bool> IsMemberOfSet(int db, string key, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyValueMessage.IsMemberOfSet(db, key, value), queueJump);
        }


        public Task<bool> AddToSortedSet(int db, string key, string value, double score, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyScoreValueMessage.AddToSortedSet(db, key, score, value), false);
        }
        public Task<bool> AddToSortedSet(int db, string key, byte[] value, double score, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyScoreValueMessage.AddToSortedSet(db, key, score, value), false);
        }

        public Task SetWithExpiry(int db, string key, int seconds, string value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteVoid(KeyScoreValueMessage.SetWithExpiry(db, key, seconds, value), queueJump);
        }
        public Task SetWithExpiry(int db, string key, int seconds, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteVoid(KeyScoreValueMessage.SetWithExpiry(db, key, seconds, value), queueJump);
        }
        public Task<bool> Expire(int db, string key, int seconds, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyScoreMessage.Expire(db, key, seconds), queueJump);
        }

        public Task<double> IncrementSortedSet(int db, string key, byte[] value, double score, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteDouble(KeyScoreValueMessage.IncrementSortedSet(db, key, score, value), queueJump);
        }
        public Task<double> IncrementSortedSet(int db, string key, string value, double score, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteDouble(KeyScoreValueMessage.IncrementSortedSet(db, key, score, value), queueJump);
        }

        public Task<bool> RemoveFromSet(int db, string key, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyValueMessage.RemoveFromSet(db, key, value), queueJump);
        }
        public Task<long> LeftPush(int db, string key, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyValueMessage.LeftPush(db, key, value), queueJump);
        }
        public Task<long> RightPush(int db, string key, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyValueMessage.RightPush(db, key, value), queueJump);
        }

        public Task<long> ListLength(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyMessage.ListLength(db, key), queueJump);
        }
        public Task<long> CardinalityOfSet(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyMessage.CardinalityOfSet(db, key), queueJump);
        }

        public Task<long> CardinalityOfSortedSet(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyMessage.CardinalityOfSortedSet(db, key), queueJump);
        }

        public Task<bool> Remove(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyMessage.Del(db, key), queueJump);
        }
        public Task<long> Remove(int db, string[] keys, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64((keys.Length == 1 ? KeyMessage.Del(db, keys[0]) : MultiKeyMessage.Del(db, keys)), queueJump);
        }

        public Task<byte[][]> Intersect(int db, string[] keys, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiBytes(MultiKeyMessage.Intersect(db, keys), queueJump);
        }

        public Task<byte[][]> Union(int db, string[] keys, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiBytes(MultiKeyMessage.Union(db, keys), queueJump);
        }

        public Task<long> IntersectAndStore(int db, string to, string[] from, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(MultiKeyMessage.IntersectAndStore(db, to, from), queueJump);
        }

        public Task<long> UnionAndStore(int db, string to, string[] from, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(MultiKeyMessage.UnionAndStore(db, to, from), false);
        }

        public Task<byte[]> LeftPop(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBytes(KeyMessage.LeftPop(db, key), queueJump);
        }
        public Task<byte[]> RightPop(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBytes(KeyMessage.RightPop(db, key), queueJump);
        }

        public Task<long> Publish(string key, string value, bool queueJump = false)
        {
            return ExecuteInt64(KeyValueMessage.Publish(key, value), queueJump);
        }
        public Task<long> Publish(string key, byte[] value, bool queueJump = false)
        {
            return ExecuteInt64(KeyValueMessage.Publish(key, value), queueJump);
        }


        public override int OutstandingCount
        {
            get
            {
                return base.OutstandingCount + GetSentCount();
            }
        }
        public Task FlushDb(int db)
        {
            if (allowAdmin)
            {
                if (db < 0) throw new ArgumentOutOfRangeException("db");
                return ExecuteVoid(VanillaMessage.FlushDb(db), false);
            }
            else
                throw new InvalidOperationException("Flush is not enabled");
        }
        public Task FlushAll()
        {
            if (allowAdmin)
            {
                return ExecuteVoid(VanillaMessage.FlushAll(), false);
            }
            else
                throw new InvalidOperationException("Flush is not enabled");
        }
        public new Task<long> Ping(bool queueJump = false) { return base.Ping(queueJump); }
        public Task<double>[] IncrementSortedSet(int db, string key, double score, string[] values, bool queueJump = false)
        {

            if (values == null) throw new ArgumentNullException("values");
            Task<double>[] result = new Task<double>[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                result[i] = IncrementSortedSet(db, key, values[i], score, queueJump);
            }
            return result;
        }
        public Task<double>[] IncrementSortedSet(int db, string key, double score, byte[][] values, bool queueJump = false)
        {
            if (values == null) throw new ArgumentNullException("values");
            Task<double>[] result = new Task<double>[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                result[i] = IncrementSortedSet(db, key, values[i], score, queueJump);
            }
            return result;
        }

        public Task<KeyValuePair<byte[], double>[]> GetRangeOfSortedSetDescending(int db, string key, int start, int stop, bool queueJump = false)
        {
            return ExecutePairs(RangeMessage.SortedSetRangeDescending(db, key, start, stop, true), queueJump);
        }

        public Task<KeyValuePair<byte[], double>[]> GetRangeOfSortedSet(int db, string key, int start, int stop, bool queueJump = false)
        {
            return ExecutePairs(RangeMessage.SortedSetRange(db, key, start, stop, true), queueJump);
        }
        public Task<long> RemoveFromSortedSetByScore(int db, string key, int start, int stop, bool queueJump = false)
        {
            return ExecuteInt64(RangeMessage.RemoveFromSortedSetByScore(db, key, start, stop), queueJump);
        }

        public Task<byte[][]> ListRange(int db, string key, int start, int stop, bool queueJump = false)
        {
            return ExecuteMultiBytes(RangeMessage.ListRange(db, key, start, stop), queueJump);
        }

        public Task<byte[]> PopFromListPushToList(int db, string from, string to, bool queueJump = false)
        {
            return ExecuteBytes(KeyValueMessage.PopFromListPushToList(db, from, to), queueJump);
        }


        public Task<bool> Move(int db, string key, int targetDb)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            if (targetDb < 0) throw new ArgumentOutOfRangeException("targetDb");
            return ExecuteBoolean(KeyScoreMessage.Move(db, key, targetDb));
        }
    }
}
