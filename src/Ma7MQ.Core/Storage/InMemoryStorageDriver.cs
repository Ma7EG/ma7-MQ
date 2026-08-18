using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ma7MQ.Core.Types;

namespace Ma7MQ.Core.Storage
{
    public class InMemoryStorageDriver : IStorageDriver
    {
        private readonly ConcurrentDictionary<string, MQMessage> _messages = new();
        private readonly ConcurrentDictionary<string, List<string>> _topicMessages = new();
        private readonly ConcurrentDictionary<string, MQTopic> _topics = new();
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, DateTime>> _consumerHeartbeats = new();
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, List<int>>> _consumerPartitions = new();
        private readonly ConcurrentDictionary<string, string> _groupTopics = new();
        private readonly object _lock = new();

        public Task SaveMessageAsync(MQMessage msg)
        {
            _messages[msg.ID] = msg;
            lock (_lock)
            {
                if (!_topicMessages.TryGetValue(msg.Topic, out var list))
                {
                    list = new List<string>();
                    _topicMessages[msg.Topic] = list;
                }
                list.Add(msg.ID);
            }
            return Task.CompletedTask;
        }

        public Task SaveMessageBatchAsync(IList<MQMessage> messages)
        {
            if (messages == null || messages.Count == 0) return Task.CompletedTask;

            lock (_lock)
            {
                for (int i = 0; i < messages.Count; i++)
                {
                    var msg = messages[i];
                    _messages[msg.ID] = msg;
                    if (!_topicMessages.TryGetValue(msg.Topic, out var list))
                    {
                        list = new List<string>();
                        _topicMessages[msg.Topic] = list;
                    }
                    list.Add(msg.ID);
                }
            }
            return Task.CompletedTask;
        }

        public Task<List<MQMessage>> GetMessagesAsync(string topic, int limit)
        {
            var result = new List<MQMessage>();
            lock (_lock)
            {
                if (_topicMessages.TryGetValue(topic, out var ids))
                {
                    int take = Math.Min(limit, ids.Count);
                    for (int i = 0; i < take; i++)
                    {
                        if (_messages.TryGetValue(ids[i], out var msg))
                        {
                            result.Add(msg);
                        }
                    }
                }
            }
            return Task.FromResult(result);
        }

        public Task SaveTopicAsync(MQTopic topic)
        {
            _topics[topic.Name] = topic;
            return Task.CompletedTask;
        }

        public Task<MQTopic> GetTopicAsync(string name)
        {
            _topics.TryGetValue(name, out var topic);
            return Task.FromResult(topic);
        }

        public Task DeleteTopicAsync(string name)
        {
            _topics.TryRemove(name, out _);
            lock (_lock)
            {
                if (_topicMessages.TryRemove(name, out var ids))
                {
                    foreach (var id in ids)
                    {
                        _messages.TryRemove(id, out _);
                    }
                }
            }
            return Task.CompletedTask;
        }

        public Task<long> GetTopicMessageCountAsync(string name)
        {
            lock (_lock)
            {
                if (_topicMessages.TryGetValue(name, out var list))
                {
                    return Task.FromResult((long)list.Count);
                }
            }
            return Task.FromResult(0L);
        }

        public Task<long> GetDLQMessageCountAsync()
        {
            long count = 0;
            lock (_lock)
            {
                foreach (var kvp in _topicMessages)
                {
                    if (kvp.Key.StartsWith("dlq:", StringComparison.OrdinalIgnoreCase))
                    {
                        count += kvp.Value.Count;
                    }
                }
            }
            return Task.FromResult(count);
        }

        public Task FlushDLQAsync()
        {
            lock (_lock)
            {
                var dlqKeys = _topicMessages.Keys.Where(k => k.StartsWith("dlq:", StringComparison.OrdinalIgnoreCase)).ToList();
                foreach (var key in dlqKeys)
                {
                    if (_topicMessages.TryRemove(key, out var ids))
                    {
                        foreach (var id in ids)
                        {
                            _messages.TryRemove(id, out _);
                        }
                    }
                }
            }
            return Task.CompletedTask;
        }

        public Task RegisterConsumerAsync(string groupName, string consumerID)
        {
            var group = _consumerHeartbeats.GetOrAdd(groupName, _ => new ConcurrentDictionary<string, DateTime>());
            group[consumerID] = DateTime.UtcNow;
            return Task.CompletedTask;
        }

        public Task RemoveConsumerAsync(string groupName, string consumerID)
        {
            if (_consumerHeartbeats.TryGetValue(groupName, out var group))
            {
                group.TryRemove(consumerID, out _);
            }
            if (_consumerPartitions.TryGetValue(groupName, out var parts))
            {
                parts.TryRemove(consumerID, out _);
            }
            return Task.CompletedTask;
        }

        public Task UpdateHeartbeatAsync(string groupName, string consumerID)
        {
            var group = _consumerHeartbeats.GetOrAdd(groupName, _ => new ConcurrentDictionary<string, DateTime>());
            group[consumerID] = DateTime.UtcNow;
            return Task.CompletedTask;
        }

        public Task<bool> IsConsumerAliveAsync(string groupName, string consumerID)
        {
            if (_consumerHeartbeats.TryGetValue(groupName, out var group) && group.TryGetValue(consumerID, out var time))
            {
                return Task.FromResult((DateTime.UtcNow - time).TotalSeconds < 30);
            }
            return Task.FromResult(false);
        }

        public Task<List<string>> GetGroupConsumersAsync(string groupName)
        {
            if (_consumerHeartbeats.TryGetValue(groupName, out var group))
            {
                return Task.FromResult(group.Keys.ToList());
            }
            return Task.FromResult(new List<string>());
        }

        public Task<long> GetTopicsCountAsync()
        {
            return Task.FromResult((long)_topics.Count);
        }

        public Task<long> GetActiveConsumersCountAsync()
        {
            long count = 0;
            foreach (var group in _consumerHeartbeats.Values)
            {
                count += group.Count;
            }
            return Task.FromResult(count);
        }

        public Task<List<string>> GetTopicNamesAsync()
        {
            return Task.FromResult(_topics.Keys.ToList());
        }

        public Task<List<MQConsumer>> GetActiveConsumersAsync()
        {
            var result = new List<MQConsumer>();
            foreach (var groupKvp in _consumerHeartbeats)
            {
                string groupName = groupKvp.Key;
                foreach (var consumerKvp in groupKvp.Value)
                {
                    string consumerID = consumerKvp.Key;
                    var partitions = new List<int>();
                    if (_consumerPartitions.TryGetValue(groupName, out var parts) && parts.TryGetValue(consumerID, out var pList))
                    {
                        partitions = pList;
                    }

                    result.Add(new MQConsumer
                    {
                        ID = consumerID,
                        LastHeartbeat = consumerKvp.Value,
                        State = ConsumerState.Active,
                        AssignedPartitions = partitions
                    });
                }
            }
            return Task.FromResult(result);
        }

        public Task SaveAssignedPartitionsAsync(string groupName, string consumerID, List<int> partitions)
        {
            var parts = _consumerPartitions.GetOrAdd(groupName, _ => new ConcurrentDictionary<string, List<int>>());
            parts[consumerID] = partitions;
            return Task.CompletedTask;
        }

        public Task<List<int>> GetAssignedPartitionsAsync(string groupName, string consumerID)
        {
            if (_consumerPartitions.TryGetValue(groupName, out var parts) && parts.TryGetValue(consumerID, out var pList))
            {
                return Task.FromResult(pList);
            }
            return Task.FromResult(new List<int>());
        }

        public Task<List<string>> GetConsumerGroupsAsync()
        {
            return Task.FromResult(_consumerHeartbeats.Keys.ToList());
        }

        public Task RegisterConsumerGroupAsync(string groupName, string topicName)
        {
            _groupTopics[groupName] = topicName;
            _consumerHeartbeats.GetOrAdd(groupName, _ => new ConcurrentDictionary<string, DateTime>());
            return Task.CompletedTask;
        }

        public Task<string> GetGroupTopicAsync(string groupName)
        {
            if (_groupTopics.TryGetValue(groupName, out var topic))
            {
                return Task.FromResult(topic);
            }
            return Task.FromResult("default");
        }
    }
}
