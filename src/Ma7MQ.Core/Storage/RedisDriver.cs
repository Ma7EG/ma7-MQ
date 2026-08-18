using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using StackExchange.Redis;
using Ma7MQ.Core.Types;

namespace Ma7MQ.Core.Storage
{
    public class RedisDriver : IStorageDriver, IDisposable
    {
        private readonly ConnectionMultiplexer _redis;
        private readonly IDatabase _db;

        public RedisDriver(string connectionString)
        {
            var options = ConfigurationOptions.Parse(connectionString);
            options.AbortOnConnectFail = true;
            options.ConnectTimeout = 1000;
            options.AsyncTimeout = 5000;
            options.SyncTimeout = 2000;
            options.ConnectRetry = 1;
            _redis = ConnectionMultiplexer.Connect(options);
            _db = _redis.GetDatabase();
        }

        public bool IsHealthy()
        {
            try
            {
                return _redis != null && _redis.IsConnected;
            }
            catch
            {
                return false;
            }
        }

        public async Task SaveMessageAsync(MQMessage msg)
        {
            string json = JsonSerializer.Serialize(msg);
            var batch = _db.CreateBatch();

            string msgKey = $"mq:msg:{msg.ID}";
            string topicMessagesKey = $"mq:topic:{msg.Topic}:messages";
            string statsKey = $"mq:topic:{msg.Topic}:stats";

            var t1 = batch.StringSetAsync(msgKey, json);
            var t2 = batch.SortedSetAddAsync(topicMessagesKey, msg.ID, DateTime.UtcNow.Ticks);
            // Fire-and-forget for non-critical stats
            batch.HashIncrementAsync(statsKey, "message_count", 1, CommandFlags.FireAndForget);
            batch.HashIncrementAsync(statsKey, "bytes_in", msg.Payload.Length, CommandFlags.FireAndForget);

            if (msg.ExpiresAt.HasValue)
            {
                var ttl = msg.ExpiresAt.Value - DateTime.UtcNow;
                if (ttl > TimeSpan.Zero)
                {
                    batch.KeyExpireAsync(msgKey, ttl, CommandFlags.FireAndForget);
                }
            }

            batch.Execute();
            await Task.WhenAll(t1, t2);
        }

        public async Task SaveMessageBatchAsync(IList<MQMessage> messages)
        {
            if (messages.Count == 0) return;

            var batch = _db.CreateBatch();
            var criticalTasks = new List<Task>(messages.Count * 2);
            long nowTicks = DateTime.UtcNow.Ticks;

            foreach (var msg in messages)
            {
                string json = JsonSerializer.Serialize(msg);
                string msgKey = $"mq:msg:{msg.ID}";
                string topicMessagesKey = $"mq:topic:{msg.Topic}:messages";
                string statsKey = $"mq:topic:{msg.Topic}:stats";

                criticalTasks.Add(batch.StringSetAsync(msgKey, json));
                criticalTasks.Add(batch.SortedSetAddAsync(topicMessagesKey, msg.ID, nowTicks++));

                batch.HashIncrementAsync(statsKey, "message_count", 1, CommandFlags.FireAndForget);
                batch.HashIncrementAsync(statsKey, "bytes_in", msg.Payload.Length, CommandFlags.FireAndForget);

                if (msg.ExpiresAt.HasValue)
                {
                    var ttl = msg.ExpiresAt.Value - DateTime.UtcNow;
                    if (ttl > TimeSpan.Zero)
                    {
                        batch.KeyExpireAsync(msgKey, ttl, CommandFlags.FireAndForget);
                    }
                }
            }

            batch.Execute();
            await Task.WhenAll(criticalTasks);
        }

        public async Task<List<MQMessage>> GetMessagesAsync(string topic, int limit)
        {
            string topicMessagesKey = $"mq:topic:{topic}:messages";

            // Read message IDs chronologically
            var ids = await _db.SortedSetRangeByRankAsync(topicMessagesKey, 0, limit - 1);
            var list = new List<MQMessage>();

            foreach (var id in ids)
            {
                string msgKey = $"mq:msg:{id}";
                string json = await _db.StringGetAsync(msgKey);
                if (string.IsNullOrEmpty(json)) continue;

                var msg = JsonSerializer.Deserialize<MQMessage>(json);
                if (msg != null)
                {
                    list.Add(msg);
                }
            }

            return list;
        }

        public async Task SaveTopicAsync(MQTopic topic)
        {
            string json = JsonSerializer.Serialize(topic);
            var batch = _db.CreateBatch();
            var t1 = batch.StringSetAsync($"mq:meta:topic:{topic.Name}", json);
            var t2 = batch.SetAddAsync("mq:meta:topics:list", topic.Name);
            batch.Execute();
            await Task.WhenAll(t1, t2);
        }

        public async Task<MQTopic> GetTopicAsync(string name)
        {
            string json = await _db.StringGetAsync($"mq:meta:topic:{name}");
            if (string.IsNullOrEmpty(json)) return null;
            return JsonSerializer.Deserialize<MQTopic>(json);
        }

        public async Task RegisterConsumerAsync(string groupName, string consumerID)
        {
            var batch = _db.CreateBatch();
            var t1 = batch.SetAddAsync($"mq:group:{groupName}:consumers", consumerID);
            var t2 = batch.SetAddAsync("mq:meta:consumers:list", consumerID);
            var t3 = batch.StringSetAsync($"mq:consumer:{consumerID}:group", groupName);
            batch.Execute();
            await Task.WhenAll(t1, t2, t3);
        }

        public async Task RemoveConsumerAsync(string groupName, string consumerID)
        {
            var batch = _db.CreateBatch();
            var t1 = batch.SetRemoveAsync($"mq:group:{groupName}:consumers", consumerID);
            var t2 = batch.SetRemoveAsync("mq:meta:consumers:list", consumerID);
            batch.KeyDeleteAsync($"mq:group:{groupName}:consumer:{consumerID}:heartbeat", CommandFlags.FireAndForget);
            batch.KeyDeleteAsync($"mq:group:{groupName}:consumer:{consumerID}:partitions", CommandFlags.FireAndForget);
            batch.KeyDeleteAsync($"mq:consumer:{consumerID}:group", CommandFlags.FireAndForget);
            batch.Execute();
            await Task.WhenAll(t1, t2);
        }

        public async Task UpdateHeartbeatAsync(string groupName, string consumerID)
        {
            string key = $"mq:group:{groupName}:consumer:{consumerID}:heartbeat";
            await _db.StringSetAsync(key, "alive", TimeSpan.FromSeconds(30));
        }

        public async Task<bool> IsConsumerAliveAsync(string groupName, string consumerID)
        {
            string key = $"mq:group:{groupName}:consumer:{consumerID}:heartbeat";
            return await _db.KeyExistsAsync(key);
        }

        public async Task<List<string>> GetGroupConsumersAsync(string groupName)
        {
            var members = await _db.SetMembersAsync($"mq:group:{groupName}:consumers");
            return members.Select(m => m.ToString()).ToList();
        }

        public async Task<long> GetTopicsCountAsync()
        {
            return await _db.SetLengthAsync("mq:meta:topics:list");
        }

        public async Task<long> GetActiveConsumersCountAsync()
        {
            return await _db.SetLengthAsync("mq:meta:consumers:list");
        }

        public async Task<List<string>> GetTopicNamesAsync()
        {
            var members = await _db.SetMembersAsync("mq:meta:topics:list");
            return members.Select(m => m.ToString()).ToList();
        }

        public async Task<List<MQConsumer>> GetActiveConsumersAsync()
        {
            var members = await _db.SetMembersAsync("mq:meta:consumers:list");
            var list = new List<MQConsumer>();

            foreach (var member in members)
            {
                string consumerID = member.ToString();
                var groupVal = await _db.StringGetAsync($"mq:consumer:{consumerID}:group");
                string groupName = groupVal.HasValue ? groupVal.ToString() : "default";
                var partitions = await GetAssignedPartitionsAsync(groupName, consumerID);

                list.Add(new MQConsumer
                {
                    ID = consumerID,
                    LastHeartbeat = DateTime.UtcNow,
                    State = ConsumerState.Active,
                    AssignedPartitions = partitions
                });
            }

            return list;
        }

        public async Task SaveAssignedPartitionsAsync(string groupName, string consumerID, List<int> partitions)
        {
            string key = $"mq:group:{groupName}:consumer:{consumerID}:partitions";
            string json = JsonSerializer.Serialize(partitions);
            await _db.StringSetAsync(key, json);
        }

        public async Task<List<int>> GetAssignedPartitionsAsync(string groupName, string consumerID)
        {
            string key = $"mq:group:{groupName}:consumer:{consumerID}:partitions";
            string json = await _db.StringGetAsync(key);
            if (string.IsNullOrEmpty(json)) return new List<int>();
            return JsonSerializer.Deserialize<List<int>>(json) ?? new List<int>();
        }

        public async Task<List<string>> GetConsumerGroupsAsync()
        {
            var members = await _db.SetMembersAsync("mq:meta:groups:list");
            return members.Select(m => m.ToString()).ToList();
        }

        public async Task RegisterConsumerGroupAsync(string groupName, string topicName)
        {
            var batch = _db.CreateBatch();
            var t1 = batch.SetAddAsync("mq:meta:groups:list", groupName);
            var t2 = batch.StringSetAsync($"mq:group:{groupName}:topic", topicName);
            batch.Execute();
            await Task.WhenAll(t1, t2);
        }

        public async Task<string> GetGroupTopicAsync(string groupName)
        {
            var topicVal = await _db.StringGetAsync($"mq:group:{groupName}:topic");
            return topicVal.HasValue ? topicVal.ToString() : "default";
        }

        public async Task DeleteTopicAsync(string name)
        {
            var batch = _db.CreateBatch();
            batch.KeyDeleteAsync($"mq:meta:topic:{name}", CommandFlags.FireAndForget);
            batch.SetRemoveAsync("mq:meta:topics:list", name, CommandFlags.FireAndForget);
            batch.KeyDeleteAsync($"mq:topic:{name}:messages", CommandFlags.FireAndForget);
            batch.KeyDeleteAsync($"mq:topic:{name}:stats", CommandFlags.FireAndForget);
            batch.Execute();
            await Task.CompletedTask;
        }

        public async Task<long> GetTopicMessageCountAsync(string name)
        {
            return await _db.SortedSetLengthAsync($"mq:topic:{name}:messages");
        }

        public async Task<long> GetDLQMessageCountAsync()
        {
            return await _db.SortedSetLengthAsync("mq:topic:dlq:messages");
        }

        public async Task FlushDLQAsync()
        {
            await _db.KeyDeleteAsync("mq:topic:dlq:messages");
        }

        public void Dispose()
        {
            _redis.Dispose();
        }
    }
}
