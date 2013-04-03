using System;
using System.Collections.Generic;
using System.Globalization;
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
    public partial class RedisConnection : RedisConnectionBase
    {
        /// <summary>
        /// Constants representing the different storage devices in redis
        /// </summary>
        public static class ItemTypes
        {
            /// <summary>
            /// Returned for a key that does not exist
            /// </summary>
            public const string None = "none";
            /// <summary>
            /// Redis Lists are simply lists of strings, sorted by insertion order. It is possible to add elements to a Redis List pushing new elements on the head (on the left) or on the tail (on the right) of the list.
            /// </summary>
            /// <remarks>http://redis.io/topics/data-types#lists</remarks>
            public const string List = "list";
            /// <summary>
            /// Strings are the most basic kind of Redis value. Redis Strings are binary safe, this means that a Redis string can contain any kind of data, for instance a JPEG image or a serialized Ruby object.
            /// </summary>
            /// <remarks>http://redis.io/topics/data-types#strings</remarks>
            public const string String = "string";
            /// <summary>
            /// Redis Sets are an unordered collection of Strings. It is possible to add, remove, and test for existence of members in O(1) (constant time regardless of the number of elements contained inside the Set).
            /// </summary>
            /// <remarks>http://redis.io/topics/data-types#sets</remarks>
            public const string Set = "set";
            /// <summary>
            /// Redis Sorted Sets are, similarly to Redis Sets, non repeating collections of Strings. The difference is that every member of a Sorted Set is associated with score, that is used in order to take the sorted set ordered, from the smallest to the greatest score.
            /// </summary>
            /// <remarks>http://redis.io/topics/data-types#sorted-sets</remarks>
            public const string SortedSet = "zset";
            /// <summary>
            /// Redis Hashes are maps between string field and string values, so they are the perfect data type to represent objects (for instance Users with a number of fields like name, surname, age, and so forth)
            /// </summary>
            /// <remarks>http://redis.io/topics/data-types#hashes</remarks>
            public const string Hash = "hash";
        }
        internal const bool DefaultAllowAdmin = false;
        /// <summary>
        /// Creates a new RedisConnection to a designated server
        /// </summary>
        public RedisConnection(string host, int port = 6379, int ioTimeout = -1, string password = null, int maxUnsent = int.MaxValue, bool allowAdmin = DefaultAllowAdmin, int syncTimeout = DefaultSyncTimeout)
            : base(host, port, ioTimeout, password, maxUnsent, syncTimeout)
        {
            this.allowAdmin = allowAdmin;
        }
        /// <summary>
        /// Creates a child RedisConnection, such as for a RedisTransaction
        /// </summary>
        protected RedisConnection(RedisConnection parent) : base(
            parent.Host, parent.Port, parent.IOTimeout, parent.Password, int.MaxValue, parent.SyncTimeout)
        {
            this.allowAdmin = parent.allowAdmin;
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
            conn.Name = Name;
            conn.SetServerVersion(this.ServerVersion, this.ServerType);
            conn.Error += OnError;
            conn.Open();
            return conn;
        }
        /// <summary>
        /// Configures an automatic keep-alive PING at a pre-determined interval; this is especially
        /// useful if CONFIG GET is not available.
        /// </summary>
        public void SetKeepAlive(int seconds)
        {
            keepAliveSeconds = seconds;
            StopKeepAlive();
            if (seconds > 0)
            {
                Trace("keep-alive", "set to {0} seconds", seconds);
                timer = new System.Timers.Timer(seconds * 1000);
                timer.Elapsed += (tick ?? (tick = Tick));
                timer.Start();
            }
        }
        private System.Timers.ElapsedEventHandler tick;
        void Tick(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (State == ConnectionState.Open)
            {
                // ping if nothing sent in *half* the interval; for example, if keep-alive is every 3 seconds we'll
                // send a PING if nothing was written in the last 1.5 seconds

                int then = lastSentTicks, now = Environment.TickCount;
                const int MSB = 1 << 31;
                if ((now - then) > (keepAliveSeconds * 500)
                    || (now & MSB) != (then & MSB)) // <=== has the sign flipped? Ticks is only the same siugn for 24.9 days at a time
                {
                    Trace("keep-alive", "ping");
                    PingImpl(true, duringInit: false);
                }
            }
        }
        private volatile int lastSentTicks;

        void StopKeepAlive()
        {
            var tmp = timer;
            timer = null;
            using (tmp)
            {
                if (tmp != null)
                {
                    tmp.Stop();
                    tmp.Close();
                }
            }
        }
        System.Timers.Timer timer;
        int keepAliveSeconds = -1;

        /// <summary>
        /// Closes the connection; either draining the unsent queue (to completion), or abandoning the unsent queue.
        /// </summary>
        public override void Close(bool abort)
        {
            StopKeepAlive();
            base.Close(abort);
        }
        /// <summary>
        /// Called during connection init, but after the AUTH is sent (if needed)
        /// </summary>
        protected override bool OnInitConnection()
        {
            var result = base.OnInitConnection();

            if (keepAliveSeconds < 0) // not known
            {
                var options = GetConfigImpl("timeout", true);
                options.ContinueWith(x =>
                {
                    if (x.IsFaulted)
                    {
                        var ex = x.Exception; // need to yank this to make TPL happy, but not going to get excited about it
                    }
                    else if (x.IsCompleted)
                    {
                        int timeout;
                        string text;
                        if (x.Result.TryGetValue("timeout", out text) && int.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out timeout)
                            && timeout > 0)
                        {
                            SetKeepAlive(Math.Max(1, timeout - 15)); // allow a few seconds contingency
                        }
                        else
                        {
                            SetKeepAlive(0);
                        }
                    }
                });
            }

            return result;
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

        private readonly bool allowAdmin;
        /// <summary>
        /// Query usage metrics for this connection
        /// </summary>
        public Counters GetCounters()
        {
            int messagesSent, messagesReceived, queueJumpers, messagesCancelled, unsent, errorMessages, timeouts, syncCallbacks, asyncCallbacks;
            GetCounterValues(out messagesSent, out messagesReceived, out queueJumpers, out messagesCancelled, out unsent, out errorMessages, out timeouts, out syncCallbacks, out asyncCallbacks);
            return new Counters(
                messagesSent, messagesReceived, queueJumpers, messagesCancelled,
                timeouts, unsent, errorMessages, syncCallbacks, asyncCallbacks,
                GetSentCount(),
                GetDbUsage(),
                // important that ping happens last, as this may artificially drain the queues
                (int)Wait(Server.Ping())
            );
        }
        private DateTime opened;
        internal override void RecordSent(RedisMessage message, bool drainFirst)
        {
            base.RecordSent(message, drainFirst);
            lastSentTicks = Environment.TickCount;
        }


        /// <summary>
        /// Give some information about the oldest incomplete (but sent) message on the server
        /// </summary>
        protected override string GetTimeoutSummary()
        {
            var msg = PeekSent();
            return msg == null ? null : msg.ToString();
        }

        /// <summary>
        /// Called after opening a connection
        /// </summary>
        protected override void OnOpened()
        {
            base.OnOpened();
            this.opened = DateTime.UtcNow;
        }



        /// <summary>
        /// Takes a server out of "slave" mode, to act as a replication master.
        /// </summary>
        [Obsolete("Please use the Server API")]
        public Task PromoteToMaster()
        {
            return Server.MakeMaster();
        }
 
       

        /// <summary>
        /// Posts a message to the given channel.
        /// </summary>
        /// <returns>the number of clients that received the message.</returns>
        public Task<long> Publish(string key, string value, bool queueJump = false)
        {
            return ExecuteInt64(RedisMessage.Create(-1, RedisLiteral.PUBLISH, key, value), queueJump);
        }
        /// <summary>
        /// Posts a message to the given channel.
        /// </summary>
        /// <returns>the number of clients that received the message.</returns>
        public Task<long> Publish(string key, byte[] value, bool queueJump = false)
        {
            return ExecuteInt64(RedisMessage.Create(-1, RedisLiteral.PUBLISH, key, value), queueJump);
        }

        /// <summary>
        /// Indicates the number of messages that have not yet been sent to the server.
        /// </summary>
        public override int OutstandingCount
        {
            get
            {
                return base.OutstandingCount + GetSentCount();
            }
        }

        internal Task<Tuple<string,int>> QuerySentinelMaster(string serviceName)
        {
           if(string.IsNullOrEmpty(serviceName)) throw new ArgumentNullException("serviceName");
           TaskCompletionSource<Tuple<string,int>> taskSource = new TaskCompletionSource<Tuple<string,int>>();
           ExecuteMultiString(RedisMessage.Create(-1, RedisLiteral.SENTINEL, "get-master-addr-by-name", serviceName), false, taskSource)
                .ContinueWith(querySentinelMasterCallback);
           return taskSource.Task;
        }
        static readonly Action<Task<string[]>> querySentinelMasterCallback = task =>
        {
            var state = (TaskCompletionSource<Tuple<string, int>>)task.AsyncState;
            if (Condition.ShouldSetResult(task, state))
            {
                var arr = task.Result;
                int i;
                if (arr == null)
                {
                    state.SetResult(null);
                }
                else if (arr.Length == 2 && int.TryParse(arr[1], out i))
                {
                    state.SetResult(Tuple.Create(arr[0], i));
                }
                else
                {
                    state.SetException(new InvalidOperationException("Invalid sentinel result: " + string.Join(",", arr)));
                }
            }
        };
    }
}
