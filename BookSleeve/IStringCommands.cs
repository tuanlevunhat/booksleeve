
namespace BookSleeve
{
    /// <summary>
    /// Commands that apply to key/value pairs, where the value
    /// can be a string, a BLOB, or interpreted as a number
    /// </summary>
    /// <see cref="http://redis.io/commands#string"/>
    public interface IStringCommands
    {
    }

    partial class RedisConnection : IStringCommands
    {
        /// <summary>
        /// Commands that apply to key/value pairs, where the value
        /// can be a string, a BLOB, or interpreted as a number
        /// </summary>
        /// <see cref="http://redis.io/commands#string"/>
        public IStringCommands Strings
        {
            get { return this; }
        }
    }
}
