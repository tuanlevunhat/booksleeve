
namespace BookSleeve
{
    /// <summary>
    /// Commands that apply to sets of items per key; sets
    /// have no defined order and are strictly unique. Duplicates
    /// are not allowed (typically, duplicates are silently discarded).
    /// </summary>
    /// <see cref="http://redis.io/commands#set"/>
    public interface ISetCommands
    {
    }

    partial class RedisConnection : ISetCommands
    {
        /// <summary>
        /// Commands that apply to sets of items per key; sets
        /// have no defined order and are strictly unique. Duplicates
        /// are not allowed (typically, duplicates are silently discarded).
        /// </summary>
        /// <see cref="http://redis.io/commands#set"/>
        public ISetCommands Sets
        {
            get { return this; }
        }
    }
}
