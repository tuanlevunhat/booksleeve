
using System.Threading.Tasks;
using System;
using System.ComponentModel;
namespace BookSleeve
{
    /// <summary>
    /// Commands that apply to basic lists of items per key; lists
    /// preserve insertion order and have no enforced uniqueness (duplicates
    /// are allowed)
    /// </summary>
    /// <see cref="http://redis.io/commands#list"/>
    public interface IListCommands
    {
        /// <summary>
        /// Inserts value in the list stored at key either before or after the reference value pivot.
        /// </summary>
        /// <remarks>When key does not exist, it is considered an empty list and no operation is performed.</remarks>
        /// <returns>the length of the list after the insert operation, or -1 when the value pivot was not found.</returns>
        /// <see cref="http://redis.io/commands/linsert"/>
        Task<long> InsertBefore(int db, string key, byte[] pivot, byte[] value, bool queueJump = false);
        /// <summary>
        /// Inserts value in the list stored at key either before or after the reference value pivot.
        /// </summary>
        /// <remarks>When key does not exist, it is considered an empty list and no operation is performed.</remarks>
        /// <returns>the length of the list after the insert operation, or -1 when the value pivot was not found.</returns>
        /// <see cref="http://redis.io/commands/linsert"/>
        Task<long> InsertBefore(int db, string key, string pivot, string value, bool queueJump = false);
        /// <summary>
        /// Inserts value in the list stored at key either before or after the reference value pivot.
        /// </summary>
        /// <remarks>When key does not exist, it is considered an empty list and no operation is performed.</remarks>
        /// <returns>the length of the list after the insert operation, or -1 when the value pivot was not found.</returns>
        /// <see cref="http://redis.io/commands/linsert"/>
        Task<long> InsertAfter(int db, string key, byte[] pivot, byte[] value, bool queueJump = false);
        /// <summary>
        /// Inserts value in the list stored at key either before or after the reference value pivot.
        /// </summary>
        /// <remarks>When key does not exist, it is considered an empty list and no operation is performed.</remarks>
        /// <returns>the length of the list after the insert operation, or -1 when the value pivot was not found.</returns>
        /// <see cref="http://redis.io/commands/linsert"/>
        Task<long> InsertAfter(int db, string key, string pivot, string value, bool queueJump = false);

        /// <summary>
        /// Returns the element at index index in the list stored at key. The index is zero-based, so 0 means the first element, 1 the second element and so on. Negative indices can be used to designate elements starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate and so forth.
        /// </summary>
        /// <returns>the requested element, or nil when index is out of range.</returns>
        /// <see cref="http://redis.io/commands/lindex"/>
        Task<byte[]> Get(int db, string key, int index, bool queueJump = false);
        /// <summary>
        /// Returns the element at index index in the list stored at key. The index is zero-based, so 0 means the first element, 1 the second element and so on. Negative indices can be used to designate elements starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate and so forth.
        /// </summary>
        /// <returns>the requested element, or nil when index is out of range.</returns>
        /// <see cref="http://redis.io/commands/lindex"/>
        Task<string> GetString(int db, string key, int index, bool queueJump = false);
        /// <summary>
        /// Sets the list element at index to value. For more information on the index argument, see LINDEX.
        /// </summary>
        /// <remarks>An error is returned for out of range indexes.</remarks>
        /// <see cref="http://redis.io/commands/lset"/>
        Task Set(int db, string key, int index, string value, bool queueJump = false);
        /// <summary>
        /// Sets the list element at index to value. For more information on the index argument, see LINDEX.
        /// </summary>
        /// <remarks>An error is returned for out of range indexes.</remarks>
        /// <see cref="http://redis.io/commands/lset"/>
        Task Set(int db, string key, int index, byte[] value, bool queueJump = false);
        /// <summary>
        /// Returns the length of the list stored at key. If key does not exist, it is interpreted as an empty list and 0 is returned. 
        /// </summary>
        /// <returns>the length of the list at key.</returns>
        /// <see cref="http://redis.io/commands/llen"/>
        Task<long> GetLength(int db, string key, bool queueJump = false);
        /// <summary>
        /// Removes and returns the first element of the list stored at key.
        /// </summary>
        /// <returns>the value of the first element, or nil when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/lpop"/>
        Task<string> RemoveFirstString(int db, string key, bool queueJump = false);
        /// <summary>
        /// Removes and returns the first element of the list stored at key.
        /// </summary>
        /// <returns>the value of the first element, or nil when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/lpop"/>
        Task<byte[]> RemoveFirst(int db, string key, bool queueJump = false);
        /// <summary>
        /// Removes and returns the last element of the list stored at key.
        /// </summary>
        /// <returns>the value of the first element, or nil when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/rpop"/>
        Task<string> RemoveLastString(int db, string key, bool queueJump = false);
        /// <summary>
        /// Removes and returns the last element of the list stored at key.
        /// </summary>
        /// <returns>the value of the first element, or nil when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/rpop"/>
        Task<byte[]> RemoveLast(int db, string key, bool queueJump = false);
        /// <summary>
        /// Inserts value at the head of the list stored at key. If key does not exist and createIfMissing is true, it is created as empty list before performing the push operation. 
        /// </summary>
        /// <returns> the length of the list after the push operation.</returns>
        /// <see cref="http://redis.io/commands/lpush"/>
        /// <see cref="http://redis.io/commands/lpushx"/>
        Task<long> AddFirst(int db, string key, string value, bool createIfMissing = true, bool queueJump = false);
        /// <summary>
        /// Inserts value at the head of the list stored at key. If key does not exist and createIfMissing is true, it is created as empty list before performing the push operation. 
        /// </summary>
        /// <returns> the length of the list after the push operation.</returns>
        /// <see cref="http://redis.io/commands/lpush"/>
        /// <see cref="http://redis.io/commands/lpushx"/>
        Task<long> AddFirst(int db, string key, byte[] value, bool createIfMissing = true, bool queueJump = false);
        /// <summary>
        /// Inserts value at the tail of the list stored at key. If key does not exist and createIfMissing is true, it is created as empty list before performing the push operation. 
        /// </summary>
        /// <returns> the length of the list after the push operation.</returns>
        /// <see cref="http://redis.io/commands/rpush"/>
        /// <see cref="http://redis.io/commands/rpushx"/>
        Task<long> AddLast(int db, string key, string value, bool createIfMissing = true, bool queueJump = false);
        /// <summary>
        /// Inserts value at the tail of the list stored at key. If key does not exist and createIfMissing is true, it is created as empty list before performing the push operation. 
        /// </summary>
        /// <returns> the length of the list after the push operation.</returns>
        /// <see cref="http://redis.io/commands/rpush"/>
        /// <see cref="http://redis.io/commands/rpushx"/>
        Task<long> AddLast(int db, string key, byte[] value, bool createIfMissing = true, bool queueJump = false);
        /// <summary>
        /// Removes the first count occurrences of elements equal to value from the list stored at key.
        /// </summary>
        /// <remarks>The count argument influences the operation in the following ways:
        /// count &gt; 0: Remove elements equal to value moving from head to tail.
        /// count &lt; 0: Remove elements equal to value moving from tail to head.
        /// count = 0: Remove all elements equal to value.
        /// For example, LREM list -2 "hello" will remove the last two occurrences of "hello" in the list stored at list.</remarks>
        /// <returns>the number of removed elements.</returns>
        /// <see cref="http://redis.io/commands/lrem"/>
        Task<long> Remove(int db, string key, string value, int count = 1, bool queueJump = false);
        /// <summary>
        /// Removes the first count occurrences of elements equal to value from the list stored at key.
        /// </summary>
        /// <remarks>The count argument influences the operation in the following ways:
        /// count &gt; 0: Remove elements equal to value moving from head to tail.
        /// count &lt; 0: Remove elements equal to value moving from tail to head.
        /// count = 0: Remove all elements equal to value.
        /// For example, LREM list -2 "hello" will remove the last two occurrences of "hello" in the list stored at list.</remarks>
        /// <returns>the number of removed elements.</returns>
        /// <see cref="http://redis.io/commands/lrem"/>
        Task<long> Remove(int db, string key, byte[] value, int count = 1, bool queueJump = false);

        /// <summary>
        /// Trim an existing list so that it will contain only the specified range of elements specified. Both start and stop are zero-based indexes, where 0 is the first element of the list (the head), 1 the next element and so on.
        /// start and end can also be negative numbers indicating offsets from the end of the list, where -1 is the last element of the list, -2 the penultimate element and so on.
        /// </summary>
        /// <example>For example: LTRIM foobar 0 2 will modify the list stored at foobar so that only the first three elements of the list will remain.</example>
        /// <remarks>Out of range indexes will not produce an error: if start is larger than the end of the list, or start > end, the result will be an empty list (which causes key to be removed). If end is larger than the end of the list, Redis will treat it like the last element of the list.</remarks>
        /// <see cref="http://redis.io/commands/ltrim"/>
        Task Trim(int db, string key, int start, int stop, bool queueJump = false);
        /// <summary>
        /// Trim an existing list so that it will contain only the specified count.
        /// </summary>
        /// <see cref="http://redis.io/commands/ltrim"/>
        Task Trim(int db, string key, int count, bool queueJump = false);

        /// <summary>
        /// Atomically returns and removes the last element (tail) of the list stored at source, and pushes the element at the first element (head) of the list stored at destination.
        /// </summary>
        /// <string>For example: consider source holding the list a,b,c, and destination holding the list x,y,z. Executing RPOPLPUSH results in source holding a,b and destination holding c,x,y,z.</string>
        /// <remarks>If source does not exist, the value nil is returned and no operation is performed. If source and destination are the same, the operation is equivalent to removing the last element from the list and pushing it as first element of the list, so it can be considered as a list rotation command.</remarks>
        /// <returns>the element being popped and pushed.</returns>
        /// <see cref="http://redis.io/commands/rpoplpush"/>
        Task<byte[]> RemoveLastAndAddFirst(int db, string source, string destination, bool queueJump = false);
        /// <summary>
        /// Atomically returns and removes the last element (tail) of the list stored at source, and pushes the element at the first element (head) of the list stored at destination.
        /// </summary>
        /// <string>For example: consider source holding the list a,b,c, and destination holding the list x,y,z. Executing RPOPLPUSH results in source holding a,b and destination holding c,x,y,z.</string>
        /// <remarks>If source does not exist, the value nil is returned and no operation is performed. If source and destination are the same, the operation is equivalent to removing the last element from the list and pushing it as first element of the list, so it can be considered as a list rotation command.</remarks>
        /// <returns>the element being popped and pushed.</returns>
        /// <see cref="http://redis.io/commands/rpoplpush"/>
        Task<string> RemoveLastAndAddFirstString(int db, string source, string destination, bool queueJump = false);
        /// <summary>
        /// Returns the specified elements of the list stored at key. The offsets start and end are zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
        /// </summary>
        /// <remarks>These offsets can also be negative numbers indicating offsets starting at the end of the list. For example, -1 is the last element of the list, -2 the penultimate, and so on.</remarks>
        /// <returns>list of elements in the specified range.</returns>
        /// <see cref="http://redis.io/commands/lrange"/>
        Task<string[]> RangeString(int db, string key, int start, int stop, bool queueJump = false);
        /// <summary>
        /// Returns the specified elements of the list stored at key. The offsets start and end are zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
        /// </summary>
        /// <remarks>These offsets can also be negative numbers indicating offsets starting at the end of the list. For example, -1 is the last element of the list, -2 the penultimate, and so on.</remarks>
        /// <returns>list of elements in the specified range.</returns>
        /// <see cref="http://redis.io/commands/lrange"/>
        Task<byte[][]> Range(int db, string key, int start, int stop, bool queueJump = false);
    }

    partial class RedisConnection : IListCommands
    {
        /// <summary>
        /// Commands that apply to basic lists of items per key; lists
        /// preserve insertion order and have no enforced uniqueness (duplicates
        /// are allowed)
        /// </summary>
        /// <see cref="http://redis.io/commands#list"/>
        public IListCommands Lists
        {
            get { return this; }
        }

        Task<long> IListCommands.InsertBefore(int db, string key, byte[] pivot, byte[] value, bool queueJump)
        {
            throw new System.NotImplementedException();
        }

        Task<long> IListCommands.InsertBefore(int db, string key, string pivot, string value, bool queueJump)
        {
            throw new System.NotImplementedException();
        }

        Task<long> IListCommands.InsertAfter(int db, string key, byte[] pivot, byte[] value, bool queueJump)
        {
            throw new System.NotImplementedException();
        }

        Task<long> IListCommands.InsertAfter(int db, string key, string pivot, string value, bool queueJump)
        {
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// Returns the element at index index in the list stored at key. The index is zero-based, so 0 means the first element, 1 the second element and so on. Negative indices can be used to designate elements starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate and so forth.
        /// </summary>
        /// <returns> the requested element, or nil when index is out of range.</returns>
        [Obsolete("Please use the Lists API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<string> GetStringFromList(int db, string key, int index, bool queueJump = false)
        {
            return Lists.GetString(db, key, index, queueJump);
        }
        /// <summary>
        /// Returns the element at index index in the list stored at key. The index is zero-based, so 0 means the first element, 1 the second element and so on. Negative indices can be used to designate elements starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate and so forth.
        /// </summary>
        /// <returns> the requested element, or nil when index is out of range.</returns>
        [Obsolete("Please use the Lists API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<byte[]> GetFromList(int db, string key, int index, bool queueJump = false)
        {
            return Lists.Get(db, key, index, queueJump);
        }
        Task<byte[]> IListCommands.Get(int db, string key, int index, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBytes(KeyScoreMessage.GetFromList(db, key, index), queueJump);
        }

        Task<string> IListCommands.GetString(int db, string key, int index, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteString(KeyScoreMessage.GetFromList(db, key, index), queueJump);
        }

        Task IListCommands.Set(int db, string key, int index, string value, bool queueJump)
        {
            throw new System.NotImplementedException();
        }

        Task IListCommands.Set(int db, string key, int index, byte[] value, bool queueJump)
        {
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// Query the number of items in a list
        /// </summary>
        /// <param name="db">The database to operate on</param>
        /// <param name="key">The key of the list</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The number of items in the list, or 0 if it does not exist</returns>
        [Obsolete("Please use the Lists API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<long> ListLength(int db, string key, bool queueJump = false)
        {
            return Lists.GetLength(db, key, queueJump);
        }
        Task<long> IListCommands.GetLength(int db, string key, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyMessage.ListLength(db, key), queueJump);
        }
        /// <summary>
        /// Removes an item from the start of a list
        /// </summary>
        /// <param name="db">The database to operatate on</param>
        /// <param name="key">The list to remove an item from</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The contents of the item removed, or null if empty</returns>
        [Obsolete("Please use the Lists API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<byte[]> LeftPop(int db, string key, bool queueJump = false)
        {
            return Lists.RemoveFirst(db, key, queueJump);
        }
        /// <summary>
        /// Removes an item from the end of a list
        /// </summary>
        /// <param name="db">The database to operatate on</param>
        /// <param name="key">The list to remove an item from</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The contents of the item removed, or null if empty</returns>
        [Obsolete("Please use the Lists API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<byte[]> RightPop(int db, string key, bool queueJump = false)
        {
            return Lists.RemoveLast(db, key, queueJump);
        }


        Task<string> IListCommands.RemoveFirstString(int db, string key, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteString(KeyMessage.LeftPop(db, key), queueJump);
        }

        Task<byte[]> IListCommands.RemoveFirst(int db, string key, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBytes(KeyMessage.LeftPop(db, key), queueJump);
        }

        Task<string> IListCommands.RemoveLastString(int db, string key, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteString(KeyMessage.RightPop(db, key), queueJump);
        }

        Task<byte[]> IListCommands.RemoveLast(int db, string key, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBytes(KeyMessage.RightPop(db, key), queueJump);
        }

        /// <summary>
        /// Prepend an item to a list
        /// </summary>
        /// <param name="db">The database to operate on</param>
        /// <param name="key">The key of the list</param>
        /// <param name="value">The item to add</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The number of items now in the list</returns>
        [Obsolete("Please use the Lists API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<long> LeftPush(int db, string key, byte[] value, bool queueJump = false)
        {
            return Lists.AddFirst(db, key, value, true, queueJump);
        }
        /// <summary>
        /// Prepend an item to a list
        /// </summary>
        /// <param name="db">The database to operate on</param>
        /// <param name="key">The key of the list</param>
        /// <param name="value">The item to add</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The number of items now in the list</returns>
        [Obsolete("Please use the Lists API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<long> LeftPush(int db, string key, string value, bool queueJump = false)
        {
            return Lists.AddFirst(db, key, value, true, queueJump);
        }
        Task<long> IListCommands.AddFirst(int db, string key, string value, bool createIfMissing, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyValueMessage.LeftPush(db, key, value, createIfMissing), queueJump);
        }

        Task<long> IListCommands.AddFirst(int db, string key, byte[] value, bool createIfMissing, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyValueMessage.LeftPush(db, key, value, createIfMissing), queueJump);
        }
        /// <summary>
        /// Append an item to a list
        /// </summary>
        /// <param name="db">The database to operate on</param>
        /// <param name="key">The key of the list</param>
        /// <param name="value">The item to add</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The number of items now in the list</returns>
        [Obsolete("Please use the Lists API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<long> RightPush(int db, string key, byte[] value, bool queueJump = false)
        {
            return Lists.AddLast(db, key, value, true, queueJump);
        }

        /// <summary>
        /// Append an item to a list
        /// </summary>
        /// <param name="db">The database to operate on</param>
        /// <param name="key">The key of the list</param>
        /// <param name="value">The item to add</param>
        /// <param name="queueJump">Whether to overtake unsent messages</param>
        /// <returns>The number of items now in the list</returns>
        [Obsolete("Please use the Lists API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<long> RightPush(int db, string key, string value, bool queueJump = false)
        {
            return Lists.AddLast(db, key, value, true, queueJump);
        }
        Task<long> IListCommands.AddLast(int db, string key, string value, bool createIfMissing, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyValueMessage.RightPush(db, key, value, createIfMissing), queueJump);
        }

        Task<long> IListCommands.AddLast(int db, string key, byte[] value, bool createIfMissing, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyValueMessage.RightPush(db, key, value, createIfMissing), queueJump);
        }

        Task<long> IListCommands.Remove(int db, string key, string value, int count, bool queueJump)
        {
            throw new System.NotImplementedException();
        }

        Task<long> IListCommands.Remove(int db, string key, byte[] value, int count, bool queueJump)
        {
            throw new System.NotImplementedException();
        }
        Task IListCommands.Trim(int db, string key, int count, bool queueJump)
        {
            if (count < 0) throw new ArgumentOutOfRangeException("count");
            if (count == 0) return Keys.Remove(db, key, queueJump);
            return Lists.Trim(db, key, 0, count - 1, queueJump);
        }
        Task IListCommands.Trim(int db, string key, int start, int stop, bool queueJump)
        {
            throw new System.NotImplementedException();
        }

        [Obsolete("Please use the Lists API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<byte[]> PopFromListPushToList(int db, string from, string to, bool queueJump = false)
        {
            return Lists.RemoveLastAndAddFirst(db, from, to, queueJump);
        }
        Task<byte[]> IListCommands.RemoveLastAndAddFirst(int db, string source, string destination, bool queueJump)
        {
            return ExecuteBytes(KeyValueMessage.PopFromListPushToList(db, source, destination), queueJump);
        }

        Task<string> IListCommands.RemoveLastAndAddFirstString(int db, string source, string destination, bool queueJump)
        {
            return ExecuteString(KeyValueMessage.PopFromListPushToList(db, source, destination), queueJump);
        }



        [Obsolete("Please use the Lists API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<byte[][]> ListRange(int db, string key, int start, int stop, bool queueJump = false)
        {
            return Lists.Range(db, key, start, stop, queueJump);    
        }
        Task<string[]> IListCommands.RangeString(int db, string key, int start, int stop, bool queueJump)
        {
            return ExecuteMultiString(RangeMessage.ListRange(db, key, start, stop), queueJump);
        }

        Task<byte[][]> IListCommands.Range(int db, string key, int start, int stop, bool queueJump)
        {
            return ExecuteMultiBytes(RangeMessage.ListRange(db, key, start, stop), queueJump);
        }




    }
}
