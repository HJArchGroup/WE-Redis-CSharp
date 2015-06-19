using System;
using System.Collections.Generic;
using ServiceStack.Redis;

namespace ServiceStack.Cluster
{
    public class RedisClient
    {
        private ConnectionPool pool = null;

        public RedisClient(string ip, ushort port)
        {
            this.pool = new ConnectionPool(ip, port);
        }

        public void Close()
        {
            if (this.pool != null)
            {
                this.pool.Close();
            }
        }

        //------------------------------------------------------------------
        // Assistant methods for handling strings and bytes.
        private bool IsValidValue(string value)
        {
            if (value == null)
            {
                return false;
            }

            int len = value.Length;
            if (len <= 0 || len >= Global.MaxValueLength)
            {
                return false;
            }

            return true;
        }
        private bool IsValidValues(string[] values)
        {
            for (var i = 0; i < values.Length; i++)
            {
                if (!this.IsValidField(values[i]))
                {
                    return false;
                }
            }
            return true;
        }
        private bool IsValidValues(List<string> values)
        {
            foreach (string item in values)
            {
                if (!this.IsValidField(item))
                {
                    return false;
                }
            }
            return true;
        }
        private bool IsValidField(string field)
        {
            return (field == null || field.Length <= 0) ? false : true;
        }
        private bool IsValidFields(string[] fields)
        {
            for (var i = 0; i < fields.Length; i++)
            {
                if (!this.IsValidField(fields[i]))
                {
                    return false;
                }
            }
            return true;
        }

        //------------------------------------------------------------------
        // STRING

        public bool SetValue(string key, string value)
        {
            if (!this.IsValidValue(value))
            {
                return false;
            }

            // Get the corresponding Redis client.
            RedisNativeClient client = this.pool.GetRedisClient(key);
            if (client == null)
            {
                return false;
            }

            client.Set(key, UTF8String.ToBytes(value));
            return true;
        }
        public int SetAll(Dictionary<string, string> map)
        {
            if (map == null || map.Count == 0)
            {
                return 0;
            }

            var cnt = 0;
            foreach (var key in map.Keys)
            {
                var value = map[key];
                if (this.SetValue(key, value))
                {
                    cnt++;
                }
            }
            return cnt;
        }
        public bool SetValue(string key, string value, int expirySeconds)
        {
            if (!this.IsValidValue(value))
            {
                return false;
            }

            // Get the corresponding Redis client.
            RedisNativeClient client = this.pool.GetRedisClient(key);
            if (client == null)
            {
                return false;
            }

            client.Set(key, UTF8String.ToBytes(value), expirySeconds);
            return true;
        }
        public bool SetValueIfExists(string key, string value)
        {
            if (!this.IsValidValue(value))
            {
                return false;
            }

            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? false : client.Set(key, UTF8String.ToBytes(value), exists: true);
        }
        public bool SetValueIfExists(string key, string value, int expirySeconds)
        {
            if (!this.IsValidValue(value))
            {
                return false;
            }

            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? false : client.Set(key, UTF8String.ToBytes(value), exists: true, expirySeconds: expirySeconds);
        }
        public bool SetValueIfNotExists(string key, string value)
        {
            if (!this.IsValidValue(value))
            {
                return false;
            }

            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? false : client.Set(key, UTF8String.ToBytes(value), exists: false);
        }
        public bool SetValueIfNotExists(string key, string value, int expirySeconds)
        {
            if (!this.IsValidValue(value))
            {
                return false;
            }

            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? false : client.Set(key, UTF8String.ToBytes(value), exists: false, expirySeconds: expirySeconds);
        }
        public long AppendToValue(string key, string value)
        {
            if (!this.IsValidValue(value))
            {
                return 0L;
            }

            // Get the corresponding Redis client.
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.Append(key, UTF8String.ToBytes(value));
        }
        public string GetValue(string key)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? null : UTF8String.ToString(client.Get(key));
        }

        //------------------------------------------------------------------
        // LIST

        public string LIndex(string key, int index)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? null : UTF8String.ToString(client.LIndex(key, index));
        }
        public bool LInsert(string key, bool insertBefore, byte[] pivot, byte[] value)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            if (client == null)
            {
                return false;
            }

            client.LInsert(key, insertBefore, pivot, value);
            return true;
        }
        public long LLen(string key)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.LLen(key);
        }
        public string RemoveStartFromList(string listId)
        {
            RedisNativeClient client = this.pool.GetRedisClient(listId);
            return (client == null) ? null : UTF8String.ToString(client.LPop(listId));
        }
        public long PrependItemToList(string listId, string value)
        {
            RedisNativeClient client = this.pool.GetRedisClient(listId);
            return (client == null) ? 0L : client.LPush(listId, UTF8String.ToBytes(value));
        }
        public long PrependRangeToList(string listId, List<string> values)
        {
            if (!this.IsValidValues(values))
            {
                return 0L;
            }
            RedisNativeClient client = this.pool.GetRedisClient(listId);
            return (client == null) ? 0L : client.LPush(listId, UTF8String.ToBytesArray(values));
        }
        public long PrependRangeToList(string listId, string[] values)
        {
            if (!this.IsValidValues(values))
            {
                return 0L;
            }
            RedisNativeClient client = this.pool.GetRedisClient(listId);
            return (client == null) ? 0L : client.LPush(listId, UTF8String.ToBytesArray(values));
        }
        public long LPushX(string key, string value)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.LPushX(key, UTF8String.ToBytes(value));
        }
        public List<string> GetRangeFromList(string key, int start, int end)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? null : UTF8String.ToStringList(client.LRange(key, start, end));
        }
        public List<string> GetAllItemsFromList(string listId)
        {
            return this.GetRangeFromList(listId, 0, -1);
        }
        public long LRem(string key, int removeNoOfMatches, string value)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.LRem(key, removeNoOfMatches, UTF8String.ToBytes(value));
        }
        public bool LSet(string key, int index, string value)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            if (client == null)
            {
                return false;
            }

            client.LSet(key, index, UTF8String.ToBytes(value));
            return true;
        }
        public bool RemoveAllFromList(string listId)
        {
            return this.LTrim(listId, 0, -1);
        }
        public bool LTrim(string key, int keepStartingFrom, int keepEndingAt)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            if (client == null)
            {
                return false;
            }

            client.LTrim(key, keepStartingFrom, keepEndingAt);
            return true;
        }

        public string RemoveEndFromList(string listId)
        {
            RedisNativeClient client = this.pool.GetRedisClient(listId);
            return (client == null) ? null : UTF8String.ToString(client.RPop(listId));
        }
        public string RPopLPush(string key, string value)
        {
            if (!this.IsValidValue(value))
            {
                return null;
            }

            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? null : UTF8String.ToString(client.RPopLPush(key, value));
        }
        public long AddItemToList(string key, string value)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.RPush(key, UTF8String.ToBytes(value));
        }
        public long RPush(string key, string[] value)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.RPush(key, UTF8String.ToBytesArray(value));
        }
        public long RPushX(string key, string value)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.RPushX(key, UTF8String.ToBytes(value));
        }
        public string[] BLPop(string key, int timeOutSecs)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? null : UTF8String.ToStringArray(client.BLPop(key, timeOutSecs));
        }
        public string[] BRPop(string key, int timeOutSecs)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? null : UTF8String.ToStringArray(client.BRPop(key, timeOutSecs));
        }
        public string BRPopLPush(string fromKey, string toKey, int timeOutSecs)
        {
            RedisNativeClient client = this.pool.GetRedisClient(fromKey, toKey);
            return (client == null) ? null : UTF8String.ToString(client.BRPopLPush(fromKey, toKey, timeOutSecs));
        }

        //------------------------------------------------------------------
        // HASH SET

        public long HDel(string key, string field)
        {
            if (!this.IsValidField(field))
            {
                return 0L;
            }

            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.HDel(key, UTF8String.ToBytes(field));
        }
        public long HExists(string key, string field)
        {
            if (!this.IsValidField(field))
            {
                return 0L;
            }

            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.HExists(key, UTF8String.ToBytes(field));
        }
        public string GetValueFromHash(string hashId, string key)
        {
            if (!this.IsValidField(key))
            {
                return null;
            }

            RedisNativeClient client = this.pool.GetRedisClient(hashId);
            return (client == null) ? null : UTF8String.ToString(client.HGet(hashId, UTF8String.ToBytes(key)));
        }
        public string[] HGetAll(string key)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? null : UTF8String.ToStringArray(client.HGetAll(key));
        }
        public long HIncrBy(string key, string field, int incrementBy)
        {
            if (!this.IsValidField(field))
            {
                return 0L;
            }

            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.HIncrby(key, UTF8String.ToBytes(field), incrementBy);
        }
        public double HIncrByFloat(string key, string field, double incrementBy)
        {
            if (!this.IsValidField(field))
            {
                return 0.0;
            }

            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0.0 : client.HIncrbyFloat(key, UTF8String.ToBytes(field), incrementBy);
        }
        public string[] HKeys(string key)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? null : UTF8String.ToStringArray(client.HKeys(key));
        }
        public long HLen(string key)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.HLen(key);
        }
        public string[] HMGet(string key, params string[] fields)
        {
            if (!this.IsValidFields(fields))
            {
                return null;
            }

            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? null : UTF8String.ToStringArray(client.HMGet(key, UTF8String.ToBytesArray(fields)));
        }
        public bool HMSet(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            int len = 0;
            foreach (KeyValuePair<string, string> node in keyValuePairs)
            {
                if (!this.IsValidField(node.Key) || !this.IsValidValue(node.Value))
                {
                    return false;
                }
                len++;
            }
            if (len == 0)
            {
                return false;
            }

            RedisNativeClient client = this.pool.GetRedisClient(key);
            if (client == null)
            {
                return false;
            }

            var keys = new byte[len][];
            var values = new byte[len][];
            int i = 0;
            foreach (KeyValuePair<string, string> node in keyValuePairs)
            {
                keys[i] = UTF8String.ToBytes(node.Key);
                values[i] = UTF8String.ToBytes(node.Value);
            }

            client.HMSet(key, keys, values);
            return true;
        }
        public long HSet(string key, string field, string value)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.HSet(key, UTF8String.ToBytes(field), UTF8String.ToBytes(value));
        }
        public long HSetNx(string key, string field, string value)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.HSetNX(key, UTF8String.ToBytes(field), UTF8String.ToBytes(value));
        }
        public string[] Hvals(string key)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? null : UTF8String.ToStringArray(client.HVals(key));
        }

        //------------------------------------------------------------------
        // INTEGER

        public long IncrementValue(string key)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.Incr(key);
        }
        public long IncrementValueBy(string key, int count)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.IncrBy(key, count);
        }
        public long DecrementValue(string key)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.Decr(key);
        }
        public long DecrementValueBy(string key, int count)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.DecrBy(key, count);
        }

        //------------------------------------------------------------------
        // GENERAL

        public long Del(string key)
        {
            RedisNativeClient client = this.pool.GetRedisClient(key);
            return (client == null) ? 0L : client.Del(key);
        }
        public void FlushAll()
        {
            this.pool.FlushAll();
        }
    }
}
