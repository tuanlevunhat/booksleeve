using System;
using System.Collections.Generic;
using System.Threading;

namespace BookSleeve
{
    
    
 
    /// <summary>
    /// Implements a thread-safe queue for use in a producer/consumer scenario
    /// </summary>
    /// <remarks> This is based on http://stackoverflow.com/questions/530211/creating-a-blocking-queuet-in-net/530228#530228 </remarks>
    internal class BlockingQueue<T>
    {
        bool closed;
        public void Close()
        {
            lock (stdPriority)
            {
                closed = true;
                Monitor.PulseAll(stdPriority);
            }
        }
        public void Open()
        {
            lock (stdPriority)
            {
                closed = false;
                Monitor.PulseAll(stdPriority);
            }
        }
        private readonly Queue<T> stdPriority = new Queue<T>(), // we'll use stdPriority as the sync-lock for both
            highPriority = new Queue<T>();
        private readonly int maxSize;
        public BlockingQueue(int maxSize) { this.maxSize = maxSize; }

        public void Enqueue(T item, bool highPri)
        {
            lock (stdPriority)
            {
                if (closed)
                {
                    throw new InvalidOperationException("The queue is closed");
                }
                if (highPri)
                {
                    highPriority.Enqueue(item);
                }
                else
                {
                    while (stdPriority.Count >= maxSize)
                    {
                        Monitor.Wait(stdPriority);
                    }
                    stdPriority.Enqueue(item);
                }
                if (stdPriority.Count + highPriority.Count == 1)
                {
                    // wake up any blocked dequeue
                    Monitor.PulseAll(stdPriority);
                }
            }
        }
        public T[] DequeueAll()
        {
            lock (stdPriority)
            {
                T[] result = new T[highPriority.Count + stdPriority.Count];
                highPriority.CopyTo(result, 0);
                stdPriority.CopyTo(result, highPriority.Count);
                highPriority.Clear();
                stdPriority.Clear();
                // wake up any blocked enqueue
                Monitor.PulseAll(stdPriority);
                return result;
            }
        }
        public bool TryDequeue(bool noWait, out T value, out bool isHigh)
        {
            lock (stdPriority)
            {
                while (highPriority.Count == 0 && stdPriority.Count == 0)
                {
                    if (closed || noWait)
                    {
                        value = default(T);
                        isHigh = false;
                        return false;
                    }
                    Monitor.Wait(stdPriority);
                }
                isHigh = highPriority.Count != 0;
                value = isHigh ? highPriority.Dequeue() : stdPriority.Dequeue();
                if ((!isHigh && stdPriority.Count == maxSize - 1) || (stdPriority.Count == 0 && highPriority.Count == 0))
                {
                    // wake up any blocked enqueue
                    Monitor.PulseAll(stdPriority);
                }
                if (isHigh && stdPriority.Count == 0) isHigh = false;//can't be high if it didn't overtake
                return true;
            }
        }

        internal int GetCount()
        {
            lock (stdPriority)
            {
                return stdPriority.Count + highPriority.Count;
            }
        }
    }
}
