
using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using System.ComponentModel;
namespace BookSleeve
{
    /// <summary>
    /// Commands that apply to key/sub-key/value tuples, i.e. where
    /// the item is a dictionary of inner values. This can be useful for
    /// modelling members of an entity, for example.
    /// </summary>
    /// <see cref="http://redis.io/commands#hash"/>
    public interface IHashCommands
    {
        /// <summary>
        /// Removes the specified fields from the hash stored at key. Non-existing fields are ignored. Non-existing keys are treated as empty hashes and this command returns 0.
        /// </summary>
        /// <see cref="http://redis.io/commands/hdel"/>
        /// <returns>The number of fields that were removed.</returns>
        Task<bool> Remove(int db, string key, string field, bool queueJump = false);
        /// <summary>
        /// Removes the specified fields from the hash stored at key. Non-existing fields are ignored. Non-existing keys are treated as empty hashes and this command returns 0.
        /// </summary>
        /// <see cref="http://redis.io/commands/hdel"/>
        /// <returns>The number of fields that were removed.</returns>
        Task<long> Remove(int db, string key, string[] fields, bool queueJump = false);
        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <returns>1 if the hash contains field. 0 if the hash does not contain field, or key does not exist.</returns>
        /// <see cref="http://redis.io/commands/hexists"/>
        Task<bool> Exists(int db, string key, string field, bool queueJump = false);

        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <returns>the value associated with field, or nil when field is not present in the hash or key does not exist.</returns>
        /// <see cref="http://redis.io/commands/hget"/>
        Task<string> GetString(int db, string key, string field, bool queueJump = false);
        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <returns>the value associated with field, or nil when field is not present in the hash or key does not exist.</returns>
        /// <see cref="http://redis.io/commands/hget"/>
        Task<byte[]> Get(int db, string key, string field, bool queueJump = false);
        /// <summary>
        /// Returns the values associated with the specified fields in the hash stored at key. For every field that does not exist in the hash, a nil value is returned.
        /// </summary>
        /// <returns>list of values associated with the given fields, in the same order as they are requested.</returns>
        /// <see cref="http://redis.io/commands/hmget"/>
        Task<string[]> GetString(int db, string key, string[] fields, bool queueJump = false);
        /// <summary>
        /// Returns the values associated with the specified fields in the hash stored at key. For every field that does not exist in the hash, a nil value is returned.
        /// </summary>
        /// <returns>list of values associated with the given fields, in the same order as they are requested.</returns>
        /// <see cref="http://redis.io/commands/hmget"/>
        Task<byte[][]> Get(int db, string key, string[] fields, bool queueJump = false);

        /// <summary>
        /// Returns all fields and values of the hash stored at key. 
        /// </summary>
        /// <returns>list of fields and their values stored in the hash, or an empty list when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/hgetall"/>
        Task<Dictionary<string, byte[]>> GetAll(int db, string key, bool queueJump = false);

        /// <summary>
        /// Increments the number stored at field in the hash stored at key by increment. If key does not exist, a new key holding a hash is created. If field does not exist or holds a string that cannot be interpreted as integer, the value is set to 0 before the operation is performed.
        /// </summary>
        /// <remarks>The range of values supported by HINCRBY is limited to 64 bit signed integers.</remarks>
        /// <returns>the value at field after the increment operation.</returns>
        /// <see cref="http://redis.io/commands/hincrby"/>
        Task<long> Increment(int db, string key, string field, int value = 1, bool queueJump = false);
        /// <summary>
        /// Returns all field names in the hash stored at key.
        /// </summary>
        /// <returns>list of fields in the hash, or an empty list when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/hkeys"/>
        Task<string[]> GetKeys(int db, string key, bool queueJump = false);
        /// <summary>
        /// Returns all values in the hash stored at key.
        /// </summary>
        /// <returns>list of values in the hash, or an empty list when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/hvals"/>
        Task<byte[][]> GetValues(int db, string key, bool queueJump = false);

        /// <summary>
        /// Returns the number of fields contained in the hash stored at key.
        /// </summary>
        /// <returns>number of fields in the hash, or 0 when key does not exist.</returns>
        /// <see cref="http://redis.io/commands/hlen"/>
        Task<long> GetLength(int db, string key, bool queueJump = false);

        /// <summary>
        /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created. If field already exists in the hash, it is overwritten.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and the value was updated.</returns>
        /// <see cref="http://redis.io/commands/hset"/>
        Task<bool> Set(int db, string key, string field, string value, bool queueJump = false);
        /// <summary>
        /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created. If field already exists in the hash, it is overwritten.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and the value was updated.</returns>
        /// <see cref="http://redis.io/commands/hset"/>
        Task<bool> Set(int db, string key, string field, byte[] value, bool queueJump = false);
        /// <summary>
        /// Sets the specified fields to their respective values in the hash stored at key. This command overwrites any existing fields in the hash. If key does not exist, a new key holding a hash is created.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and the value was updated.</returns>
        /// <see cref="http://redis.io/commands/hmset"/>
        Task Set(int db, string key, Dictionary<string, byte[]> values, bool queueJump = false);
        /// <summary>
        /// Sets field in the hash stored at key to value, only if field does not yet exist. If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and no operation was performed.</returns>
        /// <see cref="http://redis.io/commands/hsetnx"/>
        Task<bool> SetIfNotExists(int db, string key, string field, string value, bool queueJump = false);
        /// <summary>
        /// Sets field in the hash stored at key to value, only if field does not yet exist. If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and no operation was performed.</returns>
        /// <see cref="http://redis.io/commands/hsetnx"/>
        Task<bool> SetIfNotExists(int db, string key, string field, byte[] value, bool queueJump = false);
    }

    partial class RedisConnection : IHashCommands
    {
        /// <summary>
        /// Enumerate all keys in a hash.
        /// </summary>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<byte[][]> GetHash(int db, string key, bool queueJump = false)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiBytes(KeyMessage.GetHash(db, key), queueJump);
        }

        /// <summary>
        /// Returns all fields and values of the hash stored at key.
        /// </summary>
        /// <returns>list of fields and their values stored in the hash, or an empty list when key does not exist.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<Dictionary<string, byte[]>> GetHashPairs(int db, string key, bool queueJump = false)
        {
            return Hashes.GetAll(db, key, queueJump);
        }
        Task<Dictionary<string, byte[]>> IHashCommands.GetAll(int db, string key, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");

            return ExecuteHashPairs(KeyMessage.GetHash(db, key), queueJump);
        }

        /// <summary>
        /// Increment a field on a hash by an amount (1 by default)
        /// </summary>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<long> IncrementHash(int db, string key, string field, int value = 1, bool queueJump = false)
        {
            return Hashes.Increment(db, key, field, value, queueJump);
        }
        Task<long> IHashCommands.Increment(int db, string key, string field, int value, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");

            return ExecuteInt64(MultiKeyValueMessage.IncrementHash(db, key, field, value), queueJump);
        }

        /// <summary>
        /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created. If field already exists in the hash, it is overwritten.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and the value was updated.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<bool> SetHash(int db, string key, string field, string value, bool queueJump = false)
        {
            return Hashes.Set(db, key, field, value, queueJump);
        }
        Task<bool> IHashCommands.Set(int db, string key, string field, string value, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyValueMessage.SetHash(db, key, field, value), queueJump);
        }
        /// <summary>
        /// Sets the specified fields to their respective values in the hash stored at key. This command overwrites any existing fields in the hash. If key does not exist, a new key holding a hash is created.
        /// </summary>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task SetHash(int db, string key, Dictionary<string, byte[]> values, bool queueJump = false)
        {
            return Hashes.Set(db, key, values, queueJump);
        }
        Task IHashCommands.Set(int db, string key, Dictionary<string, byte[]> values, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteVoid(KeyMultiValueMessage.SetHashMulti(db, key, values), queueJump);
        }
        /// <summary>
        /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created. If field already exists in the hash, it is overwritten.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and the value was updated.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<bool> SetHash(int db, string key, string field, byte[] value, bool queueJump = false)
        {
            return Hashes.Set(db, key, field, value, queueJump);
        }
        Task<bool> IHashCommands.Set(int db, string key, string field, byte[] value, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyValueMessage.SetHash(db, key, field, value), queueJump);
        }
        /// <summary>
        /// Sets field in the hash stored at key to value, only if field does not yet exist. If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and no operation was performed.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<bool> SetHashIfNotExists(int db, string key, string field, string value, bool queueJump = false)
        {
            return Hashes.SetIfNotExists(db, key, field, value, queueJump);
        }
        Task<bool> IHashCommands.SetIfNotExists(int db, string key, string field, string value, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyValueMessage.SetHashIfNotExists(db, key, field, value), queueJump);
        }
        /// <summary>
        /// Sets field in the hash stored at key to value, only if field does not yet exist. If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
        /// </summary>
        /// <returns>1 if field is a new field in the hash and value was set. 0 if field already exists in the hash and no operation was performed.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<bool> SetHashIfNotExists(int db, string key, string field, byte[] value, bool queueJump = false)
        {
            return Hashes.SetIfNotExists(db, key, field, value, queueJump);
        }
        Task<bool> IHashCommands.SetIfNotExists(int db, string key, string field, byte[] value, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyValueMessage.SetHashIfNotExists(db, key, field, value), queueJump);
        }
        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <returns>the value associated with field, or nil when field is not present in the hash or key does not exist.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<string> GetFromHashString(int db, string key, string field, bool queueJump = false)
        {
            return Hashes.GetString(db, key, field, queueJump);
        }
        Task<string> IHashCommands.GetString(int db, string key, string field, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteString(MultiKeyMessage.GetFromHash(db, key, field), queueJump);
        }
        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <returns>the value associated with field, or nil when field is not present in the hash or key does not exist.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<byte[]> GetFromHash(int db, string key, string field, bool queueJump = false)
        {
            return Hashes.Get(db, key, field, queueJump);
        }
        Task<byte[]> IHashCommands.Get(int db, string key, string field, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBytes(MultiKeyMessage.GetFromHash(db, key, field), queueJump);
        }

        /// <summary>
        /// Returns the values associated with the specified fields in the hash stored at key. For every field that does not exist in the hash, a nil value is returned.
        /// </summary>
        /// <returns>list of values associated with the given fields, in the same order as they are requested.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<string[]> GetFromHashString(int db, string key, string[] fields, bool queueJump = false)
        {
            return Hashes.GetString(db, key, fields, queueJump);
        }
        Task<string[]> IHashCommands.GetString(int db, string key, string[] fields, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiString(MultiKeyMessage.GetFromHashMulti(db, key, fields), queueJump);
        }
        /// <summary>
        /// Returns the values associated with the specified fields in the hash stored at key. For every field that does not exist in the hash, a nil value is returned.
        /// </summary>
        /// <returns>list of values associated with the given fields, in the same order as they are requested.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<byte[][]> GetFromHash(int db, string key, string[] fields, bool queueJump = false)
        {
            return Hashes.Get(db, key, fields, queueJump);
        }
        Task<byte[][]> IHashCommands.Get(int db, string key, string[] fields, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiBytes(MultiKeyMessage.GetFromHashMulti(db, key, fields), queueJump);
        }

        /// <summary>
        /// Removes the specified field from the hash stored at key. Non-existing fields are ignored. Non-existing keys are treated as empty hashes and this command returns 0.
        /// </summary>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<bool> RemoveHash(int db, string key, string field, bool queueJump = false)
        {
            return Hashes.Remove(db, key, field, queueJump);
        }
        Task<bool> IHashCommands.Remove(int db, string key, string field, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyMessage.RemoveHash(db, key, field), queueJump);
        }
        /// <summary>
        /// Removes the specified fields from the hash stored at key. Non-existing fields are ignored. Non-existing keys are treated as empty hashes and this command returns 0.
        /// </summary>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<long> RemoveHash(int db, string key, string[] fields, bool queueJump = false)
        {
            return Hashes.Remove(db, key, fields, queueJump);
        }
        Task<long> IHashCommands.Remove(int db, string key, string[] fields, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");

            RedisFeatures features;
            if (fields.Length > 1 && ((features = Features) == null || !features.HashVaradicDelete))
            {
                RedisTransaction tran = this as RedisTransaction;
                bool execute = false;
                if (tran == null)
                {
                    tran = CreateTransaction();
                    execute = true;
                }
                Task<bool>[] tasks = new Task<bool>[fields.Length];

                var hashes = tran.Hashes;
                for (int i = 0; i < fields.Length; i++)
                {
                    tasks[i] = hashes.Remove(db, key, fields[i], queueJump);
                }
                TaskCompletionSource<long> final = new TaskCompletionSource<long>();
                tasks[fields.Length - 1].ContinueWith(t =>
                {
                    if (t.IsFaulted) final.SetException(t.Exception);
                    try
                    {
                        long count = 0;
                        for (int i = 0; i < tasks.Length; i++)
                        {
                            if (tran.Wait(tasks[i]))
                            {
                                count++;
                            }
                        }
                        final.SetResult(count);
                    }
                    catch (Exception ex)
                    {
                        final.SetException(ex);
                    }
                });
                if (execute) tran.Execute(queueJump);
                return final.Task;
            }
            else
            {
                return ExecuteInt64(MultiKeyMessage.RemoveHash(db, key, fields), queueJump);
            }
        }

        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <returns>1 if the hash contains field. 0 if the hash does not contain field, or key does not exist.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<bool> ContainsHash(int db, string key, string field, bool queueJump = false)
        {
            return Hashes.Exists(db, key, field, queueJump);
        }
        Task<bool> IHashCommands.Exists(int db, string key, string field, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteBoolean(MultiKeyMessage.ContainsHash(db, key, field), queueJump);
        }

        /// <summary>
        /// Returns all field names in the hash stored at key.
        /// </summary>
        /// <returns>list of fields in the hash, or an empty list when key does not exist.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<string[]> GetHashKeys(int db, string key, bool queueJump = false)
        {
            return Hashes.GetKeys(db, key, queueJump);
        }
        Task<string[]> IHashCommands.GetKeys(int db, string key, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiString(KeyMessage.HashKeys(db, key), queueJump);
        }

        /// <summary>
        /// Returns all values in the hash stored at key.
        /// </summary>
        /// <returns> list of values in the hash, or an empty list when key does not exist.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<byte[][]> GetHashValues(int db, string key, bool queueJump = false)
        {
            return Hashes.GetValues(db, key, queueJump);
        }
        Task<byte[][]> IHashCommands.GetValues(int db, string key, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteMultiBytes(KeyMessage.HashValues(db, key), queueJump);
        }
        /// <summary>
        /// Returns the number of fields contained in the hash stored at key.
        /// </summary>
        /// <returns>number of fields in the hash, or 0 when key does not exist.</returns>
        [Obsolete("Please use the Hashes API", false), EditorBrowsable(EditorBrowsableState.Never)]
        public Task<long> GetHashLength(int db, string key, bool queueJump = false)
        {
            return Hashes.GetLength(db, key, queueJump);
        }
        Task<long> IHashCommands.GetLength(int db, string key, bool queueJump)
        {
            if (db < 0) throw new ArgumentOutOfRangeException("db");
            return ExecuteInt64(KeyMessage.HashLength(db, key), queueJump);
        }


        /// <summary>
        /// Commands that apply to key/sub-key/value tuples, i.e. where
        /// the item is a dictionary of inner values. This can be useful for
        /// modelling members of an entity, for example.
        /// </summary>
        /// <see cref="http://redis.io/commands#hash"/>
        public IHashCommands Hashes
        {
            get { return this; }
        }
    }
}
