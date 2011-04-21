using System;
using System.Collections.Generic;
using System.Text;

namespace BookSleeve
{

    public sealed class Counters
    {
        private readonly IDictionary<int, int> dbUsage;
        private readonly int messagesSent, messagesReceived, queueJumpers, messagesCancelled, timeouts, unsentQueue, sentQueue, errorMessages, ping;
        internal Counters(int messagesSent, int messagesReceived, int queueJumpers, int messagesCancelled, int timeouts,
            int unsentQueue, int errorMessages, int sentQueue,
            IDictionary<int, int> dbUsage, int ping)
        {
            this.messagesSent = messagesSent;
            this.messagesReceived = messagesReceived;
            this.queueJumpers = queueJumpers;
            this.messagesCancelled = messagesCancelled;
            this.timeouts = timeouts;
            this.unsentQueue = unsentQueue;
            this.errorMessages = errorMessages;
            this.sentQueue = sentQueue;
            this.dbUsage = dbUsage;
            this.ping = ping;
        }
        public int MessagesSent { get { return messagesSent; } }
        public int MessagesReceived { get { return messagesReceived; } }
        public int MessagesCancelled { get { return messagesCancelled; } }
        public int Timeouts { get { return timeouts; } }
        public int QueueJumpers { get { return queueJumpers; } }
        public int UnsentQueue { get { return unsentQueue; } }
        public int ErrorMessages { get { return errorMessages; } }
        public int SentQueue { get { return sentQueue; } }
        public int Ping { get { return ping; } }
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder()
                 .Append("Sent: ").Append(MessagesSent).AppendLine()
                 .Append("Received: ").Append(MessagesReceived).AppendLine()
                 .Append("Cancelled: ").Append(MessagesCancelled).AppendLine()
                 .Append("Timeouts: ").Append(Timeouts).AppendLine()
                 .Append("Queue jumpers: ").Append(QueueJumpers).AppendLine()
                 .Append("Ping ms: ").Append(Ping).AppendLine()
                 .Append("Sent queue: ").Append(SentQueue).AppendLine()
                 .Append("Unsent queue: ").Append(UnsentQueue).AppendLine()
                 .Append("Error messages: ").Append(ErrorMessages).AppendLine();
            int[] keys = new int[dbUsage.Count], values = new int[dbUsage.Count];
            dbUsage.Keys.CopyTo(keys, 0);
            dbUsage.Values.CopyTo(values, 0);
            Array.Sort(values, keys); // sort both arrays based on the counts (ascending)
            for (int i = keys.Length - 1; i >= 0; i--)
            {
                sb.Append("DB ").Append(keys[i]).Append(": ").Append(values[i]).AppendLine();
            }
            return sb.ToString();
        }
    }
}
