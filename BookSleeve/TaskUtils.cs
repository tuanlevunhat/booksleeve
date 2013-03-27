using System.Threading.Tasks;

namespace BookSleeve
{
    /// <summary>
    /// Utility classes for working safely with tasks
    /// </summary>
    public static class TaskUtils
    {
        /// <summary>
        /// Create a task wrapper that is safe to use with "await", by avoiding callback-inlining
        /// </summary>
        public static Task SafeAwaitable(this Task task)
        {
            if (task.IsCompleted || task.IsCanceled) return task;
            var source = new TaskCompletionSource<bool>();
            task.ContinueWith(t =>
            {
                if (Condition.ShouldSetResult(t, source)) source.TrySetResult(true);
            }, TaskContinuationOptions.LongRunning);
            return source.Task;
        }
        /// <summary>
        /// Create a task wrapper that is safe to use with "await", by avoiding callback-inlining 
        /// </summary>
        public static Task<T> SafeAwaitable<T>(this Task<T> task)
        {
            if (task.IsCompleted || task.IsCanceled) return task;
            var source = new TaskCompletionSource<T>();
            task.ContinueWith(t =>
            {
                if (Condition.ShouldSetResult(t, source)) source.TrySetResult(t.Result);
            }, TaskContinuationOptions.LongRunning);
            return source.Task;
        }
    }
}
