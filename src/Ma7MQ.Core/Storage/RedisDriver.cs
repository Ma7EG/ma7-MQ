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
            
            await _db.StreamAddAsync($"mq:topic:{msg.Topic}", new[]
            {
                new NameValueEntry("id", msg.ID),
                new NameValueEntry("payload", json),
                new NameValueEntry("timestamp", DateTime.UtcNow.Ticks)
            });
        }

        public async Task<List<MQMessage>> GetMessagesAsync(string topic, int limit)
        {
            var entries = await _db.StreamReadAsync($"mq:topic:{topic}", "0-0", limit);
            var list = new List<MQMessage>();

            foreach (var entry in entries)
            {
                string json = entry["payload"];
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
