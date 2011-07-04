using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSleeve
{
    public sealed class RedisMultiConnection : RedisConnection
    {
        private RedisConnection parent;
        internal RedisMultiConnection(RedisConnection parent) : base(parent)
        {
            this.parent = parent;
        }
        
        public Task Execute(bool queueJump = false)
        {
            var all = DequeueAll();
            if (all.Length == 0)
            {
                TaskCompletionSource<bool> nix = new TaskCompletionSource<bool>();
                nix.SetResult(true);
                return nix.Task;
            }
            var multiMessage = new MultiMessage(all);
            parent.EnqueueMessage(multiMessage, queueJump);
            return multiMessage.Completion;
        }
        public void Discard()
        {
            ClearQueue();
        }
    }
}
