using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.ComponentModel;

namespace BookSleeve
{
    /// <summary>
    /// A thread-safe, multiplexed connection to a Redis server; each connection
    /// should be cached and re-used (without synchronization) from multiple
    /// callers for maximum efficiency. Usually only a single RedisConnection
    /// is required
    /// </summary>
    public class RedisConnection : RedisConnectionBase
    {
        internal const bool DefaultAllowAdmin = false;
        /// <summary>
        /// Creates a new RedisConnection to a designated server
        /// </summary>
        public RedisConnection(string host, int port = 6379, int ioTimeout = -1, string password = null, int maxUnsent = int.MaxValue, bool allowAdmin = DefaultAllowAdmin, int syncTimeout = DefaultSyncTimeout)
            : base(host, port, ioTimeout, password, maxUnsent, syncTimeout)
        {
            this.allowAdmin = allowAdmin;
            this.sent = new Queue<Message>();
        }
        /// <summary>
        /// Creates a child RedisConnection, such as for a RedisTransaction
        /// </summary>
        protected RedisConnection(RedisConnection parent) : base(
            parent.Host, parent.Port, parent.IOTimeout, parent.Password, int.MaxValue, parent.SyncTimeout)
        {
            this.allowAdmin = parent.allowAdmin;
            this.sent = new Queue<Message>();
        }
        /// <summary>
        /// Allows multiple commands to be buffered and sent to redis as a single atomic unit
        /// </summary>
        public virtual RedisTransaction CreateTransaction()
        {
            return new RedisTransaction(this);
        }
        private RedisSubscriberConnection subscriberChannel;

        private RedisSubscriberConnection SubscriberFactory()
        {
            var conn = new RedisSubscriberConnection(Host, Port, IOTimeout, Password, 100);
            conn.Error += OnError;
            conn.Open();
            return conn;
        }
        /// <summary>
        /// Creates a pub/sub connection to the same redis server
        /// </summary>
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
        /// <summary>
        /// Releases any resources associated with the connection
        /// </summary>
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
            return ProcessReply(ref result, message);
        }

        internal object ProcessReply(ref RedisResult result, Message message)
        {
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
        /// <summary>
        /// Invoked when the server is terminating
        /// </summary>
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
        /// <summary>
        /// Query usage metrics for this connection
        /// </summary>
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
        /// <summary>
        /// Called after opening a connection
        /// </summary>
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

        [Obsolete("Please use GetKeys instead", false)]
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<string[]> GetKeysSync(int db, string pattern, bool queueJump = false)
        {
            return GetKeys(db, pattern, queueJump);
        }

        public Task<string[]> GetKeys(int db, string pattern, bool queueJump = false)
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
        public Task<bool> SetIfNotExists(int db, string key, string value, bool queueJump = false)
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

        /// <summary>
        /// Removes the expiry against a key
        /// </summary>
        /// <returns>True if the expiry was removed (it existed with expiry), else False</returns>
        /// <remarks>Available with 2.1.2 and above only</remarks>
        public Task<bool> Persist(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyMessage.Persist(db, key), queueJump);
        }

        /// <summary>
        /// Obtains a random key from the database, or null otherwise (empty database)
        /// </summary>
        public Task<string> RandomKey(int db, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteString(VanillaMessage.RandomKey(db), queueJump);
        }

        /// <summary>
        /// Renames a key in the database, overwriting any existing value; the source key must exist and be different to the destination.
        /// </summary>
        public Task Rename(int db, string fromKey, string toKey, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteVoid(KeyValueMessage.Rename(db, fromKey, toKey), queueJump);
        }


        /// <summary>
        /// Renames a key in the database, overwriting any existing value; the source key must exist and be different to the destination.
        /// </summary>
        public Task<bool> RenameIfNotExists(int db, string fromKey, string toKey, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyValueMessage.RenameIfNotExists(db, fromKey, toKey), queueJump);
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
        /// <summary>
        /// Prepend an item to a list
        /// </summary>
        /// <param name="db">The database to operate on</param>
        /// <param name="key">The key of the list</param>
        /// <param name="value">The item to add</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The number of items now in the list</returns>
        public Task<long> LeftPush(int db, string key, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyValueMessage.LeftPush(db, key, value), queueJump);
        }
        /// <summary>
        /// Append an item to a list
        /// </summary>
        /// <param name="db">The database to operate on</param>
        /// <param name="key">The key of the list</param>
        /// <param name="value">The item to add</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The number of items now in the list</returns>
        public Task<long> RightPush(int db, string key, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyValueMessage.RightPush(db, key, value), queueJump);
        }

        /// <summary>
        /// Query the number of items in a list
        /// </summary>
        /// <param name="db">The database to operate on</param>
        /// <param name="key">The key of the list</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The number of items in the list, or 0 if it does not exist</returns>
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
        /// <summary>
        /// Removes a key from the database.</summary>
        /// <returns>True if the key was successfully removed, false otherwise (i.e. it didn't exist)</returns>
        public Task<bool> Remove(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(KeyMessage.Del(db, key), queueJump);
        }
        /// <summary>
        /// Removes multiple keys from the database.</summary>
        /// <returns>The number of keys successfully removed (i.e. that existed)</returns>
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

        /// <summary>
        /// Removes an item from the start of a list
        /// </summary>
        /// <param name="db">The database to operatate on</param>
        /// <param name="key">The list to remove an item from</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The contents of the item removed, or null if empty</returns>
        public Task<byte[]> LeftPop(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBytes(KeyMessage.LeftPop(db, key), queueJump);
        }
        /// <summary>
        /// Removes an item from the end of a list
        /// </summary>
        /// <param name="db">The database to operatate on</param>
        /// <param name="key">The list to remove an item from</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The contents of the item removed, or null if empty</returns>
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

        /// <summary>
        /// Moves a key between databases; the key must exist at the source and not exist at the destination.
        /// </summary>
        /// <returns>True if successful; false otherwise (didn't exist at source, or already existed at destination).</returns>
        public Task<bool> Move(int db, string key, int targetDb)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            if (targetDb < 0) throw new ArgumentOutOfRangeException("targetDb");
            return ExecuteBoolean(KeyScoreMessage.Move(db, key, targetDb));
        }

        /// <summary>
        /// Enumerate all keys in a hash.
        /// </summary>
        [Obsolete("This method is being deprecated; please use GetHashPairs", false)]
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<byte[][]> GetHash(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");

            return ExecuteMultiBytes(KeyMessage.GetHash(db, key), queueJump);
        }

        /// <summary>
        /// Returns all fields and values of the hash stored at key.
        /// </summary>
        /// <returns>list of fields and their values stored in the hash, or an empty list when key does not exist.</returns>
        public Task<Dictionary<string,byte[]>> GetHashPairs(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");

            return ExecuteHashPairs(KeyMessage.GetHash(db, key), queueJump);
        }

        /// <summary>
        /// Increment a field on a hash by an amount (1 by default)
        /// </summary>
        public Task<long> IncrementHash(int db, string key, string field, int value = 1, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");

            return ExecuteInt64(MultiKeyValueMessage.IncrementHash(db, key, field, value), queueJump);
        }

        /// <summary>
        /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created. If field already exists in the hash, it is overwritten.
        /// </summary>
        /// /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and the value was updated.</returns>
        public Task<bool> SetHash(int db, string key, string field, string value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyValueMessage.SetHash(db, key, field, value), queueJump);
        }

        /// <summary>
        /// Sets the specified fields to their respective values in the hash stored at key. This command overwrites any existing fields in the hash. If key does not exist, a new key holding a hash is created.
        /// </summary>
        public Task SetHash(int db, string key, Dictionary<string,byte[]> values, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteVoid(KeyMultiValueMessage.SetHashMulti(db, key, values), queueJump);
        }

        /// <summary>
        /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created. If field already exists in the hash, it is overwritten.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and the value was updated.</returns>
        public Task<bool> SetHash(int db, string key, string field, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyValueMessage.SetHash(db, key, field, value), queueJump);
        }
        /// <summary>
        /// Sets field in the hash stored at key to value, only if field does not yet exist. If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and no operation was performed.</returns>
        public Task<bool> SetHashIfNotExists(int db, string key, string field, string value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyValueMessage.SetHashIfNotExists(db, key, field, value), queueJump);
        }
        /// <summary>
        /// Sets field in the hash stored at key to value, only if field does not yet exist. If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and no operation was performed.</returns>
        public Task<bool> SetHashIfNotExists(int db, string key, string field, byte[] value, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyValueMessage.SetHashIfNotExists(db, key, field, value), queueJump);
        }
        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <returns>the value associated with field, or nil when field is not present in the hash or key does not exist.</returns>
        public Task<string> GetFromHashString(int db, string key, string field, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteString(MultiKeyMessage.GetFromHash(db, key, field), queueJump);
        }
        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <returns>the value associated with field, or nil when field is not present in the hash or key does not exist.</returns>
        public Task<byte[]> GetFromHash(int db, string key, string field, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBytes(MultiKeyMessage.GetFromHash(db, key, field), queueJump);
        }
        /// <summary>
        /// Returns the values associated with the specified fields in the hash stored at key. For every field that does not exist in the hash, a nil value is returned.
        /// </summary>
        /// <returns>list of values associated with the given fields, in the same order as they are requested.</returns>
        public Task<string[]> GetFromHashString(int db, string key, string[] fields, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiString(MultiKeyMessage.GetFromHashMulti(db, key, fields), queueJump);
        }
        /// <summary>
        /// Returns the values associated with the specified fields in the hash stored at key. For every field that does not exist in the hash, a nil value is returned.
        /// </summary>
        /// <returns>list of values associated with the given fields, in the same order as they are requested.</returns>
        public Task<byte[][]> GetFromHash(int db, string key, string[] fields, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiBytes(MultiKeyMessage.GetFromHashMulti(db, key, fields), queueJump);
        }

        /// <summary>
        /// Removes the specified fields from the hash stored at key. Non-existing fields are ignored. Non-existing keys are treated as empty hashes and this command returns 0.
        /// </summary>
        public Task<bool> RemoveHash(int db, string key, string field, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyMessage.RemoveHash(db, key, field), queueJump);
        }

        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <returns>1 if the hash contains field. 0 if the hash does not contain field, or key does not exist.</returns>
        public Task<bool> ContainsHash(int db, string key, string field, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyMessage.ContainsHash(db, key, field), queueJump);
        }

        /// <summary>
        /// Returns all field names in the hash stored at key.
        /// </summary>
        /// <returns>list of fields in the hash, or an empty list when key does not exist.</returns>
        public Task<string[]> GetHashKeys(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiString(KeyMessage.HashKeys(db, key), queueJump);
        }
        /// <summary>
        /// Returns all values in the hash stored at key.
        /// </summary>
        /// <returns> list of values in the hash, or an empty list when key does not exist.</returns>
        public Task<byte[][]> GetHashValues(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiBytes(KeyMessage.HashValues(db, key), queueJump);
        }
        /// <summary>
        /// Returns the number of fields contained in the hash stored at key.
        /// </summary>
        /// <returns>number of fields in the hash, or 0 when key does not exist.</returns>
        public Task<long> GetHashLength(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyMessage.HashLength(db, key), queueJump);
        }
    }
}
