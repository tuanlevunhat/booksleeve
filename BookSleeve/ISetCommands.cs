
using System;
using System.ComponentModel;
using System.Threading.Tasks;
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
        /// <summary>
        /// Add member to the set stored at key. If member is already a member of this set, no operation is performed. If key does not exist, a new set is created with member as its sole member.
        /// </summary>
        /// <returns>true if added</returns>
        /// <see cref="http://redis.io/commands/sadd"/>
        Task<bool> Add(int db, string key, string value, bool queueJump = false);
        /// <summary>
        /// Add member to the set stored at key. If member is already a member of this set, no operation is performed. If key does not exist, a new set is created with member as its sole member.
        /// </summary>
        /// <returns>true if added</returns>
        /// <see cref="http://redis.io/commands/sadd"/>
        Task<bool> Add(int db, string key, byte[] value, bool queueJump = false);
        /// <summary>
        /// Add member to the set stored at key. If member is already a member of this set, no operation is performed. If key does not exist, a new set is created with member as its sole member.
        /// </summary>
        /// <returns>the number of elements actually added to the set.</returns>
        /// <see cref="http://redis.io/commands/sadd"/>
        Task<long> Add(int db, string key, string[] values, bool queueJump = false);
        /// <summary>
        /// Add member to the set stored at key. If member is already a member of this set, no operation is performed. If key does not exist, a new set is created with member as its sole member.
        /// </summary>
        /// <returns>the number of elements actually added to the set.</returns>
        /// <see cref="http://redis.io/commands/sadd"/>
        Task<long> Add(int db, string key, byte[][] values, bool queueJump = false);

        /// <summary>
        /// Returns the set cardinality (number of elements) of the set stored at key.
        /// </summary>
        /// <returns>the cardinality (number of elements) of the set, or 0 if key does not exist.</returns>
        /// <see cref="http://redis.io/commands/scard"/>
        Task<long> GetLength(int db, string key, bool queueJump = false);

        /// <summary>
        /// Returns the members of the set resulting from the difference between the first set and all the successive sets.
        /// </summary>
        /// <returns>list with members of the resulting set.</returns>
        /// <see cref="http://redis.io/commands/sdiff"/>
        Task<string[]> DifferenceString(int db, string[] keys, bool queueJump = false);
        /// <summary>
        /// Returns the members of the set resulting from the difference between the first set and all the successive sets.
        /// </summary>
        /// <returns>list with members of the resulting set.</returns>
        /// <see cref="http://redis.io/commands/sdiff"/>
        Task<byte[][]> Difference(int db, string[] keys, bool queueJump = false);
        /// <summary>
        /// This command is equal to SDIFF, but instead of returning the resulting set, it is stored in destination.
        /// </summary>
        /// <remarks> If destination already exists, it is overwritten.</remarks>
        /// <returns>the number of elements in the resulting set.</returns>
        /// <see cref="http://redis.io/commands/sdiffstore"/>
        Task<long> DifferenceAndStore(int db, string destination, string[] keys, bool queueJump = false);

        /// <summary>
        /// Returns the members of the set resulting from the intersection of all the given sets.
        /// </summary>
        /// <returns>list with members of the resulting set.</returns>
        /// <see cref="http://redis.io/commands/sinter"/>
        Task<string[]> IntersectString(int db, string[] keys, bool queueJump = false);
        /// <summary>
        /// Returns the members of the set resulting from the intersection of all the given sets.
        /// </summary>
        /// <returns>list with members of the resulting set.</returns>
        /// <see cref="http://redis.io/commands/sinter"/>
        Task<byte[][]> Intersect(int db, string[] keys, bool queueJump = false);
        /// <summary>
        /// This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
        /// </summary>
        /// <remarks>If destination already exists, it is overwritten.</remarks>
        /// <returns>the number of elements in the resulting set.</returns>
        /// <see cref="http://redis.io/commands/sinterstore"/>
        Task<long> IntersectAndStore(int db, string destination, string[] keys, bool queueJump = false);

        /// <summary>
        /// Returns the members of the set resulting from the union of all the given sets.
        /// </summary>
        /// <returns>list with members of the resulting set.</returns>
        /// <see cref="http://redis.io/commands/sunion"/>
        Task<string[]> UnionString(int db, string[] keys, bool queueJump = false);
        /// <summary>
        /// Returns the members of the set resulting from the union of all the given sets.
        /// </summary>
        /// <returns>list with members of the resulting set.</returns>
        /// <see cref="http://redis.io/commands/sunion"/>
        Task<byte[][]> Union(int db, string[] keys, bool queueJump = false);
        /// <summary>
        /// This command is equal to SUNION, but instead of returning the resulting set, it is stored in destination.
        /// </summary>
        /// <remarks>If destination already exists, it is overwritten.</remarks>
        /// <returns>the number of elements in the resulting set.</returns>
        /// <see cref="http://redis.io/commands/sunionstore"/>
        Task<long> UnionAndStore(int db, string destination, string[] keys, bool queueJump = false);

        /// <summary>
        /// Returns if member is a member of the set stored at key.
        /// </summary>
        /// <returns>1 if the element is a member of the set. 0 if the element is not a member of the set, or if key does not exist.</returns>
        /// <see cref="http://redis.io/commands/sismember"/>
        Task<bool> Contains(int db, string key, string value, bool queueJump = false);
        /// <summary>
        /// Returns if member is a member of the set stored at key.
        /// </summary>
        /// <returns>1 if the element is a member of the set. 0 if the element is not a member of the set, or if key does not exist.</returns>
        /// <see cref="http://redis.io/commands/sismember"/>
        Task<bool> Contains(int db, string key, byte[] value, bool queueJump = false);


        /// <summary>
        /// Returns all the members of the set value stored at key.
        /// </summary>
        /// <returns>all elements of the set.</returns>
        /// <see cref="http://redis.io/commands/smembers"/>
        Task<string[]> GetAllString(int db, string key, bool queueJump = false);
        /// <summary>
        /// Returns all the members of the set value stored at key.
        /// </summary>
        /// <returns>all elements of the set.</returns>
        /// <see cref="http://redis.io/commands/smembers"/>
        Task<byte[][]> GetAll(int db, string key, bool queueJump = false);

        /// <summary>
        /// Move member from the set at source to the set at destination. This operation is atomic. In every given moment the element will appear to be a member of source or destination for other clients.
        /// </summary>
        /// <remarks>If the source set does not exist or does not contain the specified element, no operation is performed and 0 is returned. Otherwise, the element is removed from the source set and added to the destination set. When the specified element already exists in the destination set, it is only removed from the source set.</remarks>
        /// <returns>1 if the element is moved. 0 if the element is not a member of source and no operation was performed.</returns>
        /// <see cref="http://redis.io/commands/smove"/>
        Task<bool> Move(int db, string source, string destination, string value, bool queueJump = false);
        /// <summary>
        /// Move member from the set at source to the set at destination. This operation is atomic. In every given moment the element will appear to be a member of source or destination for other clients.
        /// </summary>
        /// <remarks>If the source set does not exist or does not contain the specified element, no operation is performed and 0 is returned. Otherwise, the element is removed from the source set and added to the destination set. When the specified element already exists in the destination set, it is only removed from the source set.</remarks>
        /// <returns>1 if the element is moved. 0 if the element is not a member of source and no operation was performed.</returns>
        /// <see cref="http://redis.io/commands/smove"/>
        Task<bool> Move(int db, string source, string destination, byte[] value, bool queueJump = false);

        /// <summary>
        /// Removes and returns a random element from the set value stored at key.
        /// </summary>
        /// <returns>the removed element, or nil when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/spop"/>
        Task<string> RemoveRandomString(int db, string key, bool queueJump = false);
        /// <summary>
        /// Removes and returns a random element from the set value stored at key.
        /// </summary>
        /// <returns>the removed element, or nil when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/spop"/>
        Task<byte[]> RemoveRandom(int db, string key, bool queueJump = false);

        /// <summary>
        /// Return a random element from the set value stored at key.
        /// </summary>
        /// <returns>the randomly selected element, or nil when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/srandmember"/>
        Task<string> GetRandomString(int db, string key, bool queueJump = false);
        /// <summary>
        /// Return a random element from the set value stored at key.
        /// </summary>
        /// <returns>the randomly selected element, or nil when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/srandmember"/>
        Task<byte[]> GetRandom(int db, string key, bool queueJump = false);

        /// <summary>
        /// Remove member from the set stored at key. If member is not a member of this set, no operation is performed.
        /// </summary>
        /// <returns>1 if the element was removed. 0 if the element was not a member of the set.</returns>
        /// <see cref="http://redis.io/commands/srem"/>
        Task<bool> Remove(int db, string key, string value, bool queueJump = false);
        /// <summary>
        /// Remove member from the set stored at key. If member is not a member of this set, no operation is performed.
        /// </summary>
        /// <returns>1 if the element was removed. 0 if the element was not a member of the set.</returns>
        /// <see cref="http://redis.io/commands/srem"/>
        Task<bool> Remove(int db, string key, byte[] value, bool queueJump = false);
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
        /// <summary>
        /// Add an item to a set
        /// </summary>
        [Obsolete("Please use the Sets API", false), EditorBrowsable(EditorBrowsableState.Never)] 
        public Task<bool> AddToSet(int db, string key, string value, bool queueJump = false)
        {
            return Sets.Add(db, key, value, queueJump);
        }
        /// <summary>
        /// Add an item to a set
        /// </summary>
        [Obsolete("Please use the Sets API", false), EditorBrowsable(EditorBrowsableState.Never)] 
        public Task<bool> AddToSet(int db, string key, byte[] value, bool queueJump = false)
        {
            return Sets.Add(db, key, value, queueJump);
        }

        Task<bool> ISetCommands.Add(int db, string key, string value, bool queueJump)
        {
            
            return ExecuteBoolean(RedisMessage.Create(db, RedisLiteral.SADD, key, value), queueJump);
        }

        Task<bool> ISetCommands.Add(int db, string key, byte[] value, bool queueJump)
        {
            
            return ExecuteBoolean(RedisMessage.Create(db, RedisLiteral.SADD, key, value), queueJump);
        }

        Task<long> ISetCommands.Add(int db, string key, string[] values, bool queueJump)
        {
            throw new System.NotImplementedException();
        }

        Task<long> ISetCommands.Add(int db, string key, byte[][] values, bool queueJump)
        {
            throw new System.NotImplementedException();
        }
        /// <summary>
        /// Returns the number of items in a set
        /// </summary>
        [Obsolete("Please use the Sets API", false), EditorBrowsable(EditorBrowsableState.Never)] 
        public Task<long> CardinalityOfSet(int db, string key, bool queueJump = false)
        {
            return Sets.GetLength(db, key, queueJump);
        }

        Task<long> ISetCommands.GetLength(int db, string key, bool queueJump)
        {
            
            return ExecuteInt64(RedisMessage.Create(db, RedisLiteral.SCARD, key), queueJump);
        }
        /// <summary>
        /// Intersect multiple sets
        /// </summary>
        [Obsolete("Please use the Sets API", false), EditorBrowsable(EditorBrowsableState.Never)] 
        public Task<byte[][]> Intersect(int db, string[] keys, bool queueJump = false)
        {
            return Sets.Intersect(db, keys, queueJump);
        }
        /// <summary>
        /// Union multiple sets
        /// </summary>
        [Obsolete("Please use the Sets API", false), EditorBrowsable(EditorBrowsableState.Never)] 
        public Task<byte[][]> Union(int db, string[] keys, bool queueJump = false)
        {
            return Sets.Union(db, keys, queueJump);
        }
        /// <summary>
        /// Intersect multiple sets, storing the result
        /// </summary>
        [Obsolete("Please use the Sets API", false), EditorBrowsable(EditorBrowsableState.Never)] 
        public Task<long> IntersectAndStore(int db, string to, string[] from, bool queueJump = false)
        {
            return Sets.IntersectAndStore(db, to, from, queueJump);
        }
        /// <summary>
        /// Intersect multiple sets, storing the result
        /// </summary>
        [Obsolete("Please use the Sets API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<long> UnionAndStore(int db, string to, string[] from, bool queueJump = false)
        {
            return Sets.UnionAndStore(db, to, from, queueJump);
        }

        Task<string[]> ISetCommands.DifferenceString(int db, string[] keys, bool queueJump)
        {
            
            return ExecuteMultiString(RedisMessage.Create(db, RedisLiteral.SDIFF, keys), queueJump);
        }

        Task<byte[][]> ISetCommands.Difference(int db, string[] keys, bool queueJump)
        {
            
            return ExecuteMultiBytes(RedisMessage.Create(db, RedisLiteral.SDIFF, keys), queueJump);
        }

        Task<long> ISetCommands.DifferenceAndStore(int db, string destination, string[] keys, bool queueJump)
        {
            
            return ExecuteInt64(RedisMessage.Create(db, RedisLiteral.SDIFFSTORE, destination, keys), queueJump);
        }

        Task<string[]> ISetCommands.IntersectString(int db, string[] keys, bool queueJump)
        {
            
            return ExecuteMultiString(RedisMessage.Create(db, RedisLiteral.SINTER, keys), queueJump);
        }

        Task<byte[][]> ISetCommands.Intersect(int db, string[] keys, bool queueJump)
        {
            
            return ExecuteMultiBytes(RedisMessage.Create(db, RedisLiteral.SINTER, keys), queueJump);
        }

        Task<long> ISetCommands.IntersectAndStore(int db, string destination, string[] keys, bool queueJump)
        {
            
            return ExecuteInt64(RedisMessage.Create(db, RedisLiteral.SINTERSTORE, destination, keys), queueJump);
        }

        Task<string[]> ISetCommands.UnionString(int db, string[] keys, bool queueJump)
        {
            
            return ExecuteMultiString(RedisMessage.Create(db, RedisLiteral.SUNION, keys), queueJump);
        }

        Task<byte[][]> ISetCommands.Union(int db, string[] keys, bool queueJump)
        {
            
            return ExecuteMultiBytes(RedisMessage.Create(db, RedisLiteral.SUNION, keys), queueJump);
        }

        Task<long> ISetCommands.UnionAndStore(int db, string destination, string[] keys, bool queueJump)
        {
            
            return ExecuteInt64(RedisMessage.Create(db, RedisLiteral.SUNIONSTORE, destination, keys), queueJump);
        }

        /// <summary>
        /// Is the given value in the set?
        /// </summary>
        [Obsolete("Please use the Sets API", false), EditorBrowsable(EditorBrowsableState.Never)] 
        public Task<bool> IsMemberOfSet(int db, string key, string value, bool queueJump = false)
        {
            return Sets.Contains(db, key, value, queueJump);
        }
        /// <summary>
        /// Is the given value in the set?
        /// </summary>
        [Obsolete("Please use the Sets API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<bool> IsMemberOfSet(int db, string key, byte[] value, bool queueJump = false)
        {
            return Sets.Contains(db, key, value, queueJump);
        }
        Task<bool> ISetCommands.Contains(int db, string key, string value, bool queueJump)
        {
            
            return ExecuteBoolean(RedisMessage.Create(db, RedisLiteral.SISMEMBER, key, value), queueJump);
        }
        Task<bool> ISetCommands.Contains(int db, string key, byte[] value, bool queueJump)
        {
            
            return ExecuteBoolean(RedisMessage.Create(db, RedisLiteral.SISMEMBER, key, value), queueJump);
        }

        Task<string[]> ISetCommands.GetAllString(int db, string key, bool queueJump)
        {
            
            return ExecuteMultiString(RedisMessage.Create(db, RedisLiteral.SMEMBERS, key), queueJump);
        }
        /// <summary>
        /// Gets all members of a set
        /// </summary>
        [Obsolete("Please use the Sets API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<byte[][]> GetMembersOfSet(int db, string key, bool queueJump = false)
        {
            return Sets.GetAll(db, key, queueJump);
        }
        Task<byte[][]> ISetCommands.GetAll(int db, string key, bool queueJump)
        {
            
            return ExecuteMultiBytes(RedisMessage.Create(db, RedisLiteral.SMEMBERS, key), queueJump);
        }

        Task<bool> ISetCommands.Move(int db, string source, string destination, string value, bool queueJump)
        {
            
            return ExecuteBoolean(RedisMessage.Create(db, RedisLiteral.SMOVE, source, destination, value), queueJump);
        }

        Task<bool> ISetCommands.Move(int db, string source, string destination, byte[] value, bool queueJump)
        {
            
            return ExecuteBoolean(RedisMessage.Create(db, RedisLiteral.SMOVE, source, destination, value), queueJump);
        }

        Task<string> ISetCommands.RemoveRandomString(int db, string key, bool queueJump)
        {
            
            return ExecuteString(RedisMessage.Create(db, RedisLiteral.SPOP, key), queueJump);
        }

        Task<byte[]> ISetCommands.RemoveRandom(int db, string key, bool queueJump)
        {
            
            return ExecuteBytes(RedisMessage.Create(db, RedisLiteral.SPOP, key), queueJump);
        }

        Task<string> ISetCommands.GetRandomString(int db, string key, bool queueJump)
        {
            
            return ExecuteString(RedisMessage.Create(db, RedisLiteral.SRANDMEMBER, key), queueJump);
        }

        Task<byte[]> ISetCommands.GetRandom(int db, string key, bool queueJump)
        {
            
            return ExecuteBytes(RedisMessage.Create(db, RedisLiteral.SRANDMEMBER, key), queueJump);
        }

        /// <summary>
        /// Remove an item from a set
        /// </summary>
        [Obsolete("Please use the Sets API", false), EditorBrowsable(EditorBrowsableState.Never)] 
        public Task<bool> RemoveFromSet(int db, string key, byte[] value, bool queueJump = false)
        {
            return Sets.Remove(db, key, value, queueJump);
        }
        Task<bool> ISetCommands.Remove(int db, string key, string value, bool queueJump)
        {
            
            return ExecuteBoolean(RedisMessage.Create(db, RedisLiteral.SREM, key, value), queueJump);
        }

        Task<bool> ISetCommands.Remove(int db, string key, byte[] value, bool queueJump)
        {
            
            return ExecuteBoolean(RedisMessage.Create(db, RedisLiteral.SREM, key, value), queueJump);
        }
    }
}
