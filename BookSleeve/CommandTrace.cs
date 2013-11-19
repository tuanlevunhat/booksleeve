using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BookSleeve
{
    /// <summary>
    /// Represents the information known about long-running commands
    /// </summary>    
    public sealed class CommandTrace
    {
        internal CommandTrace(long uniqueId, long time, long duration, string[] arguments)
        {
            this.UniqueId = uniqueId;
            this.Time = RedisConnection.FromUnixTime(time);
            // duration = The amount of time needed for its execution, in microseconds.
            // A tick is equal to 100 nanoseconds, or one ten-millionth of a second. 
            // So 1 microsecond = 10 ticks
            this.Duration = TimeSpan.FromTicks(duration * 10);
            this.Arguments = arguments;
        }
        /// <summary>
        /// A unique progressive identifier for every slow log entry.
        /// </summary>
        /// <remarks>The entry's unique ID can be used in order to avoid processing slow log entries multiple times (for instance you may have a script sending you an email alert for every new slow log entry). The ID is never reset in the course of the Redis server execution, only a server restart will reset it.</remarks>
        public long UniqueId { get; private set; }
        /// <summary>
        /// The time at which the logged command was processed.
        /// </summary>
        public DateTime Time { get;private set; }

        /// <summary>
        /// The amount of time needed for its execution
        /// </summary>
        public TimeSpan Duration { get;private set; }
        /// <summary>
        /// The array composing the arguments of the command.
        /// </summary>
        public string[] Arguments { get;private set; }

        /// <summary>
        /// Deduces a link to the redis documentation about the specified command
        /// </summary>
        public string GetHelpUrl()
        {
            if (Arguments == null || Arguments.Length == 0) return null;

            const string BaseUrl = "http://redis.io/commands/";

            string encoded0 = Uri.EscapeUriString(Arguments[0].ToLowerInvariant());

            if (Arguments.Length > 1)
            {

                switch (encoded0)
                {
                    case "script":
                    case "client":
                    case "config":
                    case "debug":
                    case "pubsub":
                        string encoded1 = Uri.EscapeUriString(Arguments[1].ToLowerInvariant());
                        return BaseUrl + encoded0 + "-" + encoded1;
                }
            }
            return BaseUrl + encoded0;
        }
    }
}
