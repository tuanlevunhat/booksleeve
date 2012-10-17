using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace BookSleeve
{
    /// <summary>
    /// Represents the state of an individual client connection to redis
    /// </summary>
    public sealed class ClientInfo
    {
        internal static ClientInfo[] Parse(string input)
        {
            if (input == null) return null;

            var clients = new List<ClientInfo>();
            using (var reader = new StringReader(input))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var client = new ClientInfo();
                    string[] tokens = line.Split(' ');
                    for (int i = 0; i < tokens.Length; i++)
                    {
                        string tok = tokens[i];
                        int idx = tok.IndexOf('=');
                        if (idx < 0) continue;
                        string key = tok.Substring(0, idx), value = tok.Substring(idx + 1);

                        switch (key)
                        {
                            case "addr": client.Address = value; break;
                            case "age": client.AgeSeconds = int.Parse(value, CultureInfo.InvariantCulture); break;
                            case "idle": client.IdleSeconds = int.Parse(value, CultureInfo.InvariantCulture); break;
                            case "db": client.Database = int.Parse(value, CultureInfo.InvariantCulture); break;
                            case "sub": client.SubscriptionCount = int.Parse(value, CultureInfo.InvariantCulture); break;
                            case "psub": client.PatternSubscriptionCount = int.Parse(value, CultureInfo.InvariantCulture); break;
                            case "multi": client.TransactionCommandLength = int.Parse(value, CultureInfo.InvariantCulture); break;
                            case "cmd": client.LastCommand = value; break;
                            case "flags": client.Flags = value; break;
                        }

                    }
                    clients.Add(client);
                }
            }

            return clients.ToArray();
        }
        public override string ToString()
        {
            return Address + ": " + Database + "@" + LastCommand;
        }

        /// <summary>
        /// address/port of the client
        /// </summary>
        public string Address { get; private set; }
        /// <summary>
        /// total duration of the connection in seconds
        /// </summary>
        public int AgeSeconds { get; private set; }
        /// <summary>
        /// idle time of the connection in seconds
        /// </summary>
        public int IdleSeconds { get; private set; }
        /// <summary>
        /// current database ID
        /// </summary>
        public int Database { get; private set; }
        /// <summary>
        /// number of channel subscriptions
        /// </summary>
        public int SubscriptionCount { get; private set; }
        /// <summary>
        /// number of pattern matching subscriptions
        /// </summary>
        public int PatternSubscriptionCount { get; private set; }
        /// <summary>
        /// number of commands in a MULTI/EXEC context
        /// </summary>
        public int TransactionCommandLength { get; private set; }
        /// <summary>
        /// The client flags can be a combination of:
        /// O: the client is a slave in MONITOR mode
        /// S: the client is a normal slave server
        /// M: the client is a master
        /// x: the client is in a MULTI/EXEC context
        /// b: the client is waiting in a blocking operation
        /// i: the client is waiting for a VM I/O (deprecated)
        /// d: a watched keys has been modified - EXEC will fail
        /// c: connection to be closed after writing entire reply
        /// u: the client is unblocked
        /// A: connection to be closed ASAP
        /// N: no specific flag set
        /// </summary>
        public string Flags { get; private set; }
        /// <summary>
        ///  last command played
        /// </summary>
        public string LastCommand { get; private set; }
    }
}
