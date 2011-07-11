
namespace BookSleeve
{
    /// <summary>
    /// Commands that apply to sorted sets per key. A sorted set keeps a "score"
    /// per element, and this score is used to order the elements. Duplicates
    /// are not allowed (typically, the score of the duplicate is added to the
    /// pre-existing element instead).
    /// </summary>
    /// <see cref="http://redis.io/commands#sorted_set"/>
    public interface ISortedSetCommands
    {
    }

    partial class RedisConnection : ISortedSetCommands
    {
        /// <summary>
        /// Commands that apply to sorted sets per key. A sorted set keeps a "score"
        /// per element, and this score is used to order the elements. Duplicates
        /// are not allowed (typically, the score of the duplicate is added to the
        /// pre-existing element instead).
        /// </summary>
        /// <see cref="http://redis.io/commands#sorted_set"/>
        public ISortedSetCommands SortedSets
        {
            get { return this; }
        }
    }
}
