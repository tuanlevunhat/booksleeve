
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
namespace BookSleeve
{
    /// <summary>
    /// Describes a pre-condition used in a redis transaction
    /// </summary>
    public abstract class Condition
    {
        /// <summary>
        /// Enforces that the given key must exist
        /// </summary>
        public static Condition KeyExists(int db, string key)
        {
            return new ExistsCondition(db, key, true);
        }
        /// <summary>
        /// Enforces that the given key must not exist
        /// </summary>
        public static Condition KeyNotExists(int db, string key)
        {
            return new ExistsCondition(db, key, false);
        }
        internal abstract Task<bool> Task { get; }
        internal bool Validate()
        {
            var task = Task;
            return task.Status == TaskStatus.RanToCompletion && task.Result;
        }
        private Condition() { }

        internal abstract IEnumerable<RedisMessage> CreateMessages();
        internal static bool ShouldSetResult(Task task, TaskCompletionSource<bool> source)
        {
            if(task.IsFaulted) {
                source.TrySetException(task.Exception);
            } else if(task.IsCanceled) {
                source.TrySetCanceled();
            } else if(task.IsCompleted) {
                return true;
            }
            return false;
        }
        private class ExistsCondition : Condition
        {
            readonly TaskCompletionSource<bool> result = new TaskCompletionSource<bool>();
            readonly bool expectedResult;
            readonly int db;
            readonly string key;
            public ExistsCondition(int db, string key, bool expectedResult)
            {
                this.key = key;
                this.db = db;
                this.expectedResult = expectedResult;
            }
            internal override Task<bool> Task { get { return result.Task; } }

            // avoid lots of delegate creations
            static readonly Action<Task<bool>> markComplete =
                task =>
                {
                    var state = (ExistsCondition)task.AsyncState;
                    if(ShouldSetResult(task, state.result)) state.result.TrySetResult(task.Result == state.expectedResult);
                };
            internal override IEnumerable<RedisMessage> CreateMessages()
            {
                yield return RedisMessage.Create(db, RedisLiteral.WATCH, key); 
                var msgResult = new MessageResultBoolean(this);
                msgResult.Task.ContinueWith(markComplete);
                var message = RedisMessage.Create(db, RedisLiteral.EXISTS, key); 
                message.SetMessageResult(msgResult);   
                yield return message;
            }

        }
    }
}
