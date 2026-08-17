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
            _redis = ConnectionMultiplexer.Connect(connectionString);
            _db = _redis.GetDatabase();
        }

        public async Task SaveMessageAsync(MQMessage msg)
        {
            string json = JsonSerializer.Serialize(msg);
            var transaction = _db.CreateTransaction();

            string msgKey = $"mq:msg:{msg.ID}";
            string topicMessagesKey = $"mq:topic:{msg.Topic}:messages";
            string statsKey = $"mq:topic:{msg.Topic}:stats";

            // Store message payload
            transaction.StringSetAsync(msgKey, json);

            // Add to topic ZSET chronologically
            transaction.SortedSetAddAsync(topicMessagesKey, msg.ID, DateTime.UtcNow.Ticks);

            // Increment stats Hash
            transaction.HashIncrementAsync(statsKey, "message_count", 1);
            transaction.HashIncrementAsync(statsKey, "bytes_in", msg.Payload.Length);

            // Apply TTL if ExpiresAt is defined
            if (msg.ExpiresAt.HasValue)
            {
                var ttl = msg.ExpiresAt.Value - DateTime.UtcNow;
                if (ttl > TimeSpan.Zero)
                {
                    transaction.KeyExpireAsync(msgKey, ttl);
                }
            }

            await transaction.ExecuteAsync();
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
            await _db.StringSetAsync($"mq:meta:topic:{topic.Name}", json);
            await _db.SetAddAsync("mq:meta:topics:list", topic.Name);
        }

        public async Task RegisterConsumerAsync(string groupName, string consumerID)
        {
            await _db.SetAddAsync($"mq:group:{groupName}:consumers", consumerID);
            await _db.SetAddAsync("mq:meta:consumers:list", consumerID);
        }

        public async Task UpdateHeartbeatAsync(string groupName, string consumerID)
        {
            string key = $"mq:group:{groupName}:consumer:{consumerID}:heartbeat";
            await _db.StringSetAsync(key, "alive", TimeSpan.FromSeconds(30));
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
                list.Add(new MQConsumer
                {
                    ID = consumerID,
                    LastHeartbeat = DateTime.UtcNow,
                    State = ConsumerState.Active
                });
            }

            return list;
        }

        public void Dispose()
        {
            _redis.Dispose();
        }
    }
}
