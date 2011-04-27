using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BookSleeve
{
    public abstract class RedisConnectionBase : IDisposable
    {
        private Socket socket;
        private NetworkStream redisStream;

        private readonly BlockingQueue<Message> unsent;
        private readonly int port, ioTimeout, syncTimeout;
        private readonly string host, password;
        public int SyncTimeout { get { return syncTimeout; } }
        public string Host { get { return host; } }
        protected string Password { get { return password; } }
        public int Port { get { return port; } }
        protected int IOTimeout { get { return ioTimeout; } }
        private RedisFeatures features;
        public RedisFeatures Features { get { return features; } }

        public Version ServerVersion
        {
            get
            {
                var tmp = features;
                return tmp == null ? null : tmp.Version;
            }
            set
            {
                features = new RedisFeatures(value);
            }
        }

        protected void GetCounterValues(out int messagesSent, out int messagesReceived,
            out int queueJumpers, out int messagesCancelled, out int unsent, out int errorMessages, out int timeouts)
        {
            messagesSent = Interlocked.CompareExchange(ref this.messagesSent, 0, 0);
            messagesReceived = Interlocked.CompareExchange(ref this.messagesReceived, 0, 0);
            queueJumpers = Interlocked.CompareExchange(ref this.queueJumpers, 0, 0);
            messagesCancelled = Interlocked.CompareExchange(ref this.messagesCancelled, 0, 0);
            messagesSent = Interlocked.CompareExchange(ref this.messagesSent, 0, 0);
            errorMessages = Interlocked.CompareExchange(ref this.errorMessages, 0, 0);
            timeouts = Interlocked.CompareExchange(ref this.timeouts, 0, 0);
            unsent = this.unsent.GetCount();
        }
        protected Task<long> Ping(bool queueJump = true)
        {
            return ExecuteInt64(new PingMessage(), queueJump);
        }
        protected const int DefaultSyncTimeout = 10000;
        // dont' really want external subclasses
        internal RedisConnectionBase(string host, int port = 6379, int ioTimeout = -1, string password = null, int maxUnsent = int.MaxValue, int syncTimeout = DefaultSyncTimeout)
        {
            if(syncTimeout <= 0) throw new ArgumentOutOfRangeException("syncTimeout");
            this.syncTimeout = syncTimeout;
            this.unsent = new BlockingQueue<Message>(maxUnsent);
            this.host = host;
            this.port = port;
            this.ioTimeout = ioTimeout;
            this.password = password;

            this.readReplyHeader = ReadReplyHeader;
        }
        static bool TryParseVersion(string value, out Version version)
        {  // .NET 4.0 has Version.TryParse, but 3.5 CP does not
            try
            {
                version = new Version(value);
                return true;
            }
            catch
            {
                version = default(Version);
                return false;
            }
        }

        private int state;
        public ConnectionState State
        {
            get { return (ConnectionState)state; }
        }

        public virtual void Dispose()
        {
            abort = true;
            try { if (redisStream != null) redisStream.Dispose(); }
            catch { }
            try { if (socket != null) socket.Close(); }
            catch { }
            socket = null;
            redisStream = null;
            Error = null;         
        }
        protected virtual void OnOpened() { }
        public Task Open()
        {
            if (Interlocked.CompareExchange(ref state, (int)ConnectionState.Opening, (int)ConnectionState.Shiny) != (int)ConnectionState.Shiny)
                throw new InvalidOperationException(); // not shiny
            try
            {
                socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                socket.NoDelay = true;
                socket.SendTimeout = ioTimeout;
                socket.Connect(host, port);

                redisStream = new NetworkStream(socket);
                redisStream.ReadTimeout = redisStream.WriteTimeout = ioTimeout;



                Thread thread = new Thread(Outgoing);
                thread.IsBackground = true;
                thread.Name = "Redis:outgoing";
                thread.Start();

                if (!string.IsNullOrEmpty(password)) EnqueueMessage(KeyMessage.Auth(password), true);
                var info = GetInfo();

                ReadMoreAsync();

                return ContinueWith(info, done =>
                {
                    try
                    {
                        // process this when available
                        var parsed = ParseInfo(done.Result);
                        string s;
                        Version version;
                        if (parsed.TryGetValue("redis_version", out s) && TryParseVersion(s, out version))
                        {
                            this.ServerVersion = version;
                        }
                        Interlocked.CompareExchange(ref state, (int)ConnectionState.Open, (int)ConnectionState.Opening);
                    }
                    catch
                    {
                        Close(true);
                        Interlocked.CompareExchange(ref state, (int)ConnectionState.Closed, (int)ConnectionState.Opening);
                    }
                });
            }
            catch
            {
                Interlocked.CompareExchange(ref state, (int)ConnectionState.Closed, (int)ConnectionState.Opening);
                throw;
            }
        }
        public Task<string> GetInfo(bool queueJump = false)
        {
            return ExecuteString(VanillaMessage.Info(), queueJump);
        }
        static Dictionary<string, string> ParseInfo(string result)
        {
            string[] lines = result.Split(new[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries);
            var data = new Dictionary<string, string>();
            for (int i = 0; i < lines.Length; i++)
            {
                string line = lines[i];
                int idx = line.IndexOf(':');
                data.Add(line.Substring(0, idx), line.Substring(idx + 1));
            }
            return data;
        }
        //internal Func<long> ExecuteFutureInt(Message message, bool queueJump = false)
        //{
        //    var future = Future<long>.Limited(syncTimeout);
        //    Enqueue(message.WithCallback(rr =>
        //    {
        //        future.SetValue(() => rr.ValueInt64);
        //    }), queueJump);
        //    Interlocked.Increment(ref futures);
        //    return future.GetValue;
        //}
        //internal Func<byte[]> ExecuteFutureBytes(Message message, bool queueJump = false)
        //{
        //    var future = Future<byte[]>.Limited(syncTimeout);
        //    Enqueue(message.WithCallback(rr =>
        //    {
        //        future.SetValue(() => rr.ValueBytes);
        //    }), queueJump);
        //    Interlocked.Increment(ref futures);
        //    return future.GetValue;
        //}
        //internal Func<string> ExecuteFutureString(Message message, bool queueJump = false)
        //{
        //    var future = Future<string>.Limited(syncTimeout);
        //    Enqueue(message.WithCallback(rr =>
        //    {
        //        future.SetValue(() => rr.ValueString);
        //    }), queueJump);
        //    Interlocked.Increment(ref futures);
        //    return future.GetValue;
        //}
        //internal Func<bool> ExecuteFutureBool(Message message, bool queueJump = false)
        //{
        //    var future = Future<bool>.Limited(syncTimeout);
        //    Enqueue(message.WithCallback(rr =>
        //    {
        //        future.SetValue(() => rr.ValueBoolean);
        //    }), queueJump);
        //    Interlocked.Increment(ref futures);
        //    return future.GetValue;
        //}

        int timeouts;


        public virtual int OutstandingCount { get { return unsent.GetCount(); } }
        private readonly AsyncCallback readReplyHeader;
        public event EventHandler Closed;
        volatile bool abort;
        public void Close(bool abort)
        {
            this.abort = abort;
            unsent.Close();
        }
        private void ReadMoreAsync()
        {
            bufferOffset = bufferCount = 0;
            var tmp = redisStream;
            if (tmp != null)
            {
                tmp.BeginRead(buffer, 0, BufferSize, readReplyHeader, tmp); // read more IO here (in parallel)
            }
        }
        private bool ReadMoreSync()
        {
            var tmp = redisStream;
            if (tmp == null) return false;
            bufferOffset = bufferCount = 0;
            int bytesRead = tmp.Read(buffer, 0, BufferSize);
            if (bytesRead > 0)
            {
                bufferCount = bytesRead;
                return true;
            }
            return false;
        }
        private void ReadReplyHeader(IAsyncResult asyncResult)
        {
            try
            {
                int bytesRead;
                try
                {
                    bytesRead = ((NetworkStream)asyncResult.AsyncState).EndRead(asyncResult);
                }
                catch (ObjectDisposedException)
                {
                    bytesRead = 0; // simulate EOF
                }
                catch (NullReferenceException)
                {
                    bytesRead = 0; // simulate EOF
                }
                if (bytesRead <= 0 || redisStream == null) 
                {   // EOF
                    Shutdown("End of stream", null);
                }
                else
                {
                    bool isEof = false;
                    bufferCount += bytesRead;
                    while (bufferCount > 0)
                    {
                        RedisResult result = ReadSingleResult();
                        Interlocked.Increment(ref messagesReceived);
                        object ctx = ProcessReply(ref result);

                        if (result.IsError)
                        {
                            Interlocked.Increment(ref errorMessages);
                            OnError("Redis server", result.Error(), false);
                        }
                        try
                        {
                            ProcessCallbacks(ctx, result);
                        }
                        catch (Exception ex)
                        {
                            OnError("Processing callbacks", ex, false);
                        }
                        isEof = false;
                        NetworkStream tmp = redisStream;
                        if (bufferCount == 0 && tmp != null && tmp.DataAvailable)
                        {
                            isEof = !ReadMoreSync();
                        }
                    }
                    if (isEof)
                    {   // EOF
                        Shutdown("End of stream", null);
                    }
                    else
                    {
                        ReadMoreAsync();
                    } 
                }
            }
            catch (Exception ex)
            {
                Shutdown("Invalid inbound stream", ex);
            }        
        }
        internal abstract object ProcessReply(ref RedisResult result);
        internal abstract void ProcessCallbacks(object ctx, RedisResult result);

        private RedisResult ReadSingleResult()
        {
            byte b = ReadByteOrFail();
            switch ((char)b)
            {
                case '+':
                    return RedisResult.Message(ReadBytesToCrlf());
                case '-':
                    return RedisResult.Error(ReadStringToCrlf());
                case ':':
                    return RedisResult.Integer(ReadInt64());
                case '$':
                    return RedisResult.Bytes(ReadBulkBytes());
                case '*':
                    int count = (int)ReadInt64();
                    RedisResult[] inner = new RedisResult[count];
                    for (int i = 0; i < count; i++)
                    {
                        inner[i] = ReadSingleResult();                        
                    }
                    return RedisResult.Multi(inner);
                default:
                    throw new RedisException("Not expecting header: &x" + b.ToString("x2"));
            }
        }
        internal void CompleteMessage(Message message, RedisResult result)
        {
            try
            {
                message.Complete(result);
            }
            catch (Exception ex)
            {
                OnError("Completing message", ex, false);
            }
        }
        private void Shutdown(string cause, Exception error)
        {
            if (error != null)
            {
                Debugger.Break();
            }
            Close(error != null);
            Interlocked.CompareExchange(ref state, (int)ConnectionState.Closed, (int)ConnectionState.Closing);

            if (error != null) OnError(cause, error, true);
            ShuttingDown(error);
            Dispose();
            var handler = Closed;
            if (handler != null) handler(this, EventArgs.Empty);

        }
        protected virtual void ShuttingDown(Exception error) { }
        private static readonly byte[] empty = new byte[0];
        private int Read(byte[] scratch, int offset, int maxBytes)
        {
            if(bufferCount > 0 || ReadMoreSync())
            {
                int count = Math.Min(maxBytes, bufferCount);
                Buffer.BlockCopy(buffer, bufferOffset, scratch, offset, count);
                bufferOffset += count;
                bufferCount -= count;
                return count;
            }
            else
            {
                return 0;
            }
        }
        private byte[] ReadBulkBytes()
        {
            int len;
            checked
            {
                len = (int)ReadInt64();
            }
            switch (len)
            {
                case -1: return null;
                case 0: BurnCrlf(); return empty;
            }
            byte[] data = new byte[len];
            int bytesRead, offset = 0;
            while (len > 0 && (bytesRead = Read(data, offset, len)) > 0)
            {
                len -= bytesRead;
                offset += bytesRead;
            }
            if (len > 0) throw new EndOfStreamException("EOF reading bulk-bytes");
            BurnCrlf();
            return data;
        }
        private byte ReadByteOrFail()
        {
            if (bufferCount > 0 || ReadMoreSync())
            {
                bufferCount--;
                return buffer[bufferOffset++];
            }
            throw new EndOfStreamException();
        }
        private void BurnCrlf()
        {
            if (ReadByteOrFail() != (byte)'\r' || ReadByteOrFail() != (byte)'\n') throw new InvalidDataException("Expected crlf terminator not found");
        }

        const int BufferSize = 2048;
        private readonly byte[] buffer = new byte[BufferSize];
        int bufferOffset = 0, bufferCount = 0;

        private byte[] ReadBytesToCrlf()
        {
            // check for data inside the buffer first
            int bytes = FindCrlfInBuffer();
            byte[] result;
            if (bytes >= 0)
            {
                result = new byte[bytes];
                Buffer.BlockCopy(buffer, bufferOffset, result, 0, bytes);
                // subtract the data; don't forget to include the CRLF
                bufferCount -= (bytes + 2);
                bufferOffset += (bytes + 2);
            }
            else
            {
                byte[] oversizedBuffer;
                int len = FillBodyBufferToCrlf(out oversizedBuffer);
                result = new byte[len];
                Buffer.BlockCopy(oversizedBuffer, 0, result, 0, len);
            }

            
            return result;
        }
        int FindCrlfInBuffer()
        {
            int max = bufferOffset + bufferCount - 1;
            for (int i = bufferOffset; i < max; i++)
            {
                if (buffer[i] == (byte)'\r' && buffer[i + 1] == (byte)'\n')
                {
                    int bytes = i - bufferOffset;
                    return bytes;
                }
            }
            return -1;
        }
        private string ReadStringToCrlf()
        {
            // check for data inside the buffer first
            int bytes = FindCrlfInBuffer();
            string result;
            if (bytes >= 0)
            {
                result = Encoding.UTF8.GetString(buffer, bufferOffset, bytes);
                // subtract the data; don't forget to include the CRLF
                bufferCount -= (bytes + 2);
                bufferOffset += (bytes + 2);
            }
            else
            {
                // check for data that steps over the buffer
                byte[] oversizedBuffer;
                int len = FillBodyBufferToCrlf(out oversizedBuffer);
                result = Encoding.UTF8.GetString(oversizedBuffer, 0, len);
            }
            return result;
        }

        private int FillBodyBufferToCrlf(out byte[] oversizedBuffer)
        {
            bool haveCr = false;
            bodyBuffer.SetLength(0);
            byte b;
            do
            {
                b = ReadByteOrFail();
                if (haveCr)
                {
                    if (b == (byte)'\n')
                    {// we have our string
                        oversizedBuffer = bodyBuffer.GetBuffer();
                        return (int)bodyBuffer.Length;
                    }
                    else
                    {
                        bodyBuffer.WriteByte((byte)'\r');
                        haveCr = false;
                    }
                }
                if (b == (byte)'\r')
                {
                    haveCr = true;
                }
                else
                {
                    bodyBuffer.WriteByte(b);
                }
            } while (true);
        }

        private long ReadInt64()
        {
            byte[] oversizedBuffer;
            int len = FillBodyBufferToCrlf(out oversizedBuffer);
            // crank our own int parser... why not...
            int tmp;
            switch (len)
            {
                case 0:
                    throw new EndOfStreamException("No data parsing integer");
                case 1:
                    if ((tmp = ((int)oversizedBuffer[0] - '0')) >= 0 && tmp <= 9)
                    {
                        return tmp;
                    }
                    break;
            }
            bool isNeg = oversizedBuffer[0] == (byte)'-';
            if (isNeg && len == 2 && (tmp = ((int)oversizedBuffer[1] - '0')) >= 0 && tmp <= 9)
            {
                return -tmp;
            }

            long value = 0;
            for (int i = isNeg ? 1 : 0; i < len; i++)
            {
                if ((tmp = ((int)oversizedBuffer[i] - '0')) >= 0 && tmp <= 9)
                {
                    value = (value * 10) + tmp;
                }
                else
                {
                    throw new FormatException("Unable to parse integer: " + Encoding.UTF8.GetString(oversizedBuffer, 0, len));
                }
            }
            return isNeg ? -value : value;
        }
 

        protected Dictionary<int, int> GetDbUsage()
        {
            lock (dbUsage)
            {
                return new Dictionary<int, int>(dbUsage);
            }
        }
        int messagesSent, messagesReceived, queueJumpers, messagesCancelled, errorMessages;
        private readonly Dictionary<int, int> dbUsage = new Dictionary<int, int>();
        private void LogUsage(int db)
        {
            lock (dbUsage)
            {
                int count;
                if (dbUsage.TryGetValue(db, out count))
                {
                    dbUsage[db] = count + 1;
                }
                else
                {
                    dbUsage.Add(db, 1);
                }
            }
        }
        public event EventHandler<ErrorEventArgs> Error;
        protected void OnError(object sender, ErrorEventArgs args)
        {
            var handler = Error;
            if (handler != null)
            {
                handler(sender, args);
            }
        }
        protected void OnError(string cause, Exception ex, bool isFatal)
        {
            var handler = Error;
            if (handler == null)
            {
                Trace.WriteLine(ex.Message, cause);
            }
            else
            {
                handler(this, new ErrorEventArgs(ex, cause, isFatal));
            }
        }
        private void Outgoing()
        {
            try
            {
                OnOpened();
                int db = 0;
                Message next;
                Trace.WriteLine("Redis send-pump is starting");
                bool isHigh;
                while (unsent.TryDequeue(false, out next, out isHigh))
                {
                    if (abort)
                    {
                        CompleteMessage(next, RedisResult.Error("The system aborted before this message was sent"));
                        continue;
                    }
                    if (!next.ChangeState(MessageState.NotSent, MessageState.Sent))
                    {
                        // already cancelled; not our problem any more...
                        Interlocked.Increment(ref messagesCancelled);
                        continue;
                    }
                    if (isHigh) Interlocked.Increment(ref queueJumpers);
                    
                    if (next.Db >= 0)
                    {
                        if (db != next.Db)
                        {
                            var changeDb = new SelectMessage(db = next.Db);
                            RecordSent(changeDb);
                            changeDb.Write(redisStream);
                            Interlocked.Increment(ref messagesSent);
                        }
                        LogUsage(db);
                    }
                    if (next is SelectMessage)
                    {
                        // dealt with above; no need to send SELECT, SELECT
                    }
                    else
                    {
                        RecordSent(next);
                        next.Write(redisStream);
                        Interlocked.Increment(ref messagesSent);
                    }
                    redisStream.Flush();
                    
                }
                Interlocked.CompareExchange(ref state, (int)ConnectionState.Closing, (int)ConnectionState.Open);
                if (redisStream != null)
                {
                    var quit = VanillaMessage.Quit();

                    RecordSent(quit, !abort);
                    quit.Write(redisStream);
                    redisStream.Flush();
                    Interlocked.Increment(ref messagesSent);
                }
                Trace.WriteLine("Redis send-pump is exiting");
            }
            catch (Exception ex)
            {
                OnError("Outgoing queue", ex, true);
            }

        }
        internal virtual void RecordSent(Message message, bool drainFirst = false) { }
        public enum ConnectionState
        {
            Shiny, Opening, Open, Closing, Closed
        }
        private readonly MemoryStream bodyBuffer = new MemoryStream();


        internal Task<bool> ExecuteBoolean(Message message, bool queueJump = false)
        {
            var msgResult = new MessageResultBoolean();
            message.SetMessageResult(msgResult);
            EnqueueMessage(message, queueJump);
            return msgResult.Task;
        }

        internal Task<long> ExecuteInt64(Message message, bool queueJump = false)
        {
            var msgResult = new MessageResultInt64();
            message.SetMessageResult(msgResult);
            EnqueueMessage(message, queueJump);
            return msgResult.Task;
        }

        internal Task ExecuteVoid(Message message, bool queueJump = false)
        {
            var msgResult = new MessageResultVoid();
            message.SetMessageResult(msgResult);
            EnqueueMessage(message, queueJump);
            return msgResult.Task;
        }

        internal Task<double> ExecuteDouble(Message message, bool queueJump = false)
        {
            var msgResult = new MessageResultDouble();
            message.SetMessageResult(msgResult);
            EnqueueMessage(message, queueJump);
            return msgResult.Task;
        }

        internal Task<byte[]> ExecuteBytes(Message message, bool queueJump = false)
        {
            var msgResult = new MessageResultBytes();
            message.SetMessageResult(msgResult);
            EnqueueMessage(message, queueJump);
            return msgResult.Task;
        }

        internal Task<string> ExecuteString(Message message, bool queueJump = false)
        {
            var msgResult = new MessageResultString();
            message.SetMessageResult(msgResult);
            EnqueueMessage(message, queueJump);
            return msgResult.Task;
        }

        internal Task<byte[][]> ExecuteMultiBytes(Message message, bool queueJump = false)
        {
            var msgResult = new MessageResultMultiBytes();
            message.SetMessageResult(msgResult);
            EnqueueMessage(message, queueJump);
            return msgResult.Task;
        }

        internal Task<string[]> ExecuteMultiString(Message message, bool queueJump = false)
        {
            var msgResult = new MessageResultMultiString();
            message.SetMessageResult(msgResult);
            EnqueueMessage(message, queueJump);
            return msgResult.Task;
        }

        internal Task<KeyValuePair<byte[], double>[]> ExecutePairs(Message message, bool queueJump = false)
        {
            var msgResult = new MessageResultPairs();
            message.SetMessageResult(msgResult);
            EnqueueMessage(message, queueJump);
            return msgResult.Task;
        }
        internal void EnqueueMessage(Message message, bool queueJump = false)
        {
            unsent.Enqueue(message, queueJump);
        }
        /// <summary>
        /// If the task is not yet completed, blocks the caller until completion up to a maximum of SyncTimeout milliseconds.
        /// Once a task is completed, the result is returned.
        /// </summary>
        /// <param name="task">The task to wait on</param>
        /// <returns>The return value of the task.</returns>
        /// <exception cref="TimeoutException">If SyncTimeout milliseconds is exceeded.</exception>
        public T Wait<T>(Task<T> task)
        {
            Wait((Task)task);
            return task.Result;
        }
        /// <summary>
        /// If the task is not yet completed, blocks the caller until completion up to a maximum of SyncTimeout milliseconds.
        /// </summary>
        /// <param name="task">The task to wait on</param>
        /// <exception cref="TimeoutException">If SyncTimeout milliseconds is exceeded.</exception>
        /// <remarks>If an exception is throw, it is extracted from the AggregateException (unless multiple exceptions are found)</remarks>
        public void Wait(Task task)
        {
            if (task == null) throw new ArgumentNullException("task");
            try
            {
                if (!task.Wait(syncTimeout))
                {
                    throw new TimeoutException();
                }
            }
            catch (AggregateException ex)
            {
                if (ex.InnerExceptions.Count == 1)
                {
                    throw ex.InnerExceptions[0];
                }
                throw;
            }
        }
        /// <summary>
        /// Waits for all of a set of tasks to complete, up to a maximum of SyncTimeout milliseconds.
        /// </summary>
        /// <param name="tasks">The tasks to wait on</param>
        /// <exception cref="TimeoutException">If SyncTimeout milliseconds is exceeded.</exception>
        public void WaitAll(params Task[] tasks)
        {
            if (tasks == null) throw new ArgumentNullException("tasks");
            if (!Task.WaitAll(tasks, syncTimeout))
            {
                throw new TimeoutException();
            }
        }
        /// <summary>
        /// Waits for any of a set of tasks to complete, up to a maximum of SyncTimeout milliseconds.
        /// </summary>
        /// <param name="tasks">The tasks to wait on</param>
        /// <returns>The index of a completed task</returns>
        /// <exception cref="TimeoutException">If SyncTimeout milliseconds is exceeded.</exception>        
        public int WaitAny(params Task[] tasks)
        {
            if (tasks == null) throw new ArgumentNullException("tasks");
            return Task.WaitAny(tasks, syncTimeout);
        }
        /// <summary>
        /// Add a continuation (a callback), to be executed once a task has completed
        /// </summary>
        /// <param name="task">The task to add a continuation to</param>
        /// <param name="action">The continuation to perform once completed</param>
        /// <returns>A new task representing the composed operation</returns>
        public Task ContinueWith<T>(Task<T> task, Action<Task<T>> action)
        {
            return task.ContinueWith(action, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }
        /// <summary>
        /// Add a continuation (a callback), to be executed once a task has completed
        /// </summary>
        /// <param name="task">The task to add a continuation to</param>
        /// <param name="action">The continuation to perform once completed</param>
        /// <returns>A new task representing the composed operation</returns>
        public Task ContinueWith(Task task, Action<Task> action)
        {
            return task.ContinueWith(action, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }

    }
}

