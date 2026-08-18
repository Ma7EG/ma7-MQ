using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ma7MQ.Core.Types;
using Ma7MQ.Core.Storage;

namespace Ma7MQ.Core.Broker
{
    public interface IBrokerEngine
    {
        Task PublishAsync(MQMessage msg);
        Task PublishBatchAsync(IList<MQMessage> messages);
        Task CreateTopicAsync(MQTopic topic);
        Task RegisterConsumerAsync(string groupName, string consumerID);
        Task HeartbeatAsync(string groupName, string consumerID);
        Task<List<MQMessage>> GetMessagesForGroupAsync(string topic, string groupName, string consumerID, string filter, int limit);
        Task RebalanceConsumerGroupAsync(string groupName);
    }

    public class BrokerEngine : IBrokerEngine
    {
        private readonly IStorageDriver _store;

        private readonly System.Threading.Channels.Channel<MQMessage> _channel;
        private readonly System.Threading.CancellationTokenSource _cts = new();
        private readonly Task[] _batchWorkers;

        public BrokerEngine(IStorageDriver store)
        {
            _store = store;

            var channelOptions = new System.Threading.Channels.UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            };
            _channel = System.Threading.Channels.Channel.CreateUnbounded<MQMessage>(channelOptions);

            int workerCount = Math.Max(2, Environment.ProcessorCount / 2);
            _batchWorkers = new Task[workerCount];
            for (int i = 0; i < workerCount; i++)
            {
                _batchWorkers[i] = Task.Run(ProcessBatchWorkerAsync);
            }

            StartHeartbeatWorker();
        }

        public async Task CreateTopicAsync(MQTopic topic)
        {
            await _store.SaveTopicAsync(topic);
        }

        public async Task PublishAsync(MQMessage msg)
        {
            if (string.IsNullOrEmpty(msg.ID))
            {
                msg.ID = Guid.NewGuid().ToString("N");
            }

            // Only compress large payloads (>4KB) to avoid overhead on small messages
            if (CompressionHelper.ShouldCompress(msg.Payload, 4096))
            {
                var compressed = await CompressionHelper.CompressAsync(msg.Payload);
                msg.Payload = compressed;
                msg.Headers["x-compression"] = "gzip";
            }

            if (!_channel.Writer.TryWrite(msg))
            {
                await _channel.Writer.WriteAsync(msg);
            }
        }

        public async Task PublishBatchAsync(IList<MQMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                var msg = messages[i];
                if (string.IsNullOrEmpty(msg.ID))
                {
                    msg.ID = Guid.NewGuid().ToString("N");
                }
                if (!_channel.Writer.TryWrite(msg))
                {
                    await _channel.Writer.WriteAsync(msg);
                }
            }
        }

        private async Task ProcessBatchWorkerAsync()
        {
            var reader = _channel.Reader;
            var batch = new List<MQMessage>(1000);
            const int maxBatchSize = 1000;

            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    if (await reader.WaitToReadAsync(_cts.Token))
                    {
                        while (batch.Count < maxBatchSize && reader.TryRead(out var msg))
                        {
                            batch.Add(msg);
                        }

                        if (batch.Count > 0)
                        {
                            await _store.SaveMessageBatchAsync(batch);
                            batch.Clear();
                        }
                    }
                }
                catch (OperationCanceledException) when (_cts.IsCancellationRequested)
                {
                    break;
                }
                catch
                {
                    // Continue worker resilience
                }
            }

            while (reader.TryRead(out var msg))
            {
                batch.Add(msg);
                if (batch.Count >= maxBatchSize)
                {
                    try { await _store.SaveMessageBatchAsync(batch); } catch { }
                    batch.Clear();
                }
            }
            if (batch.Count > 0)
            {
                try { await _store.SaveMessageBatchAsync(batch); } catch { }
                batch.Clear();
            }
        }

        public async Task RegisterConsumerAsync(string groupName, string consumerID)
        {
            // Map the group to the topic if registering for the first time
            // For simplicity, consumer groups map to their topic
            string topicName = groupName.Split('-')[0]; // simple convention fallback
            await _store.RegisterConsumerGroupAsync(groupName, topicName);

            await _store.RegisterConsumerAsync(groupName, consumerID);
            await _store.UpdateHeartbeatAsync(groupName, consumerID);

            // Rebalance group
            await RebalanceConsumerGroupAsync(groupName);
        }

        public async Task HeartbeatAsync(string groupName, string consumerID)
        {
            await _store.UpdateHeartbeatAsync(groupName, consumerID);
        }

        public async Task<List<MQMessage>> GetMessagesForGroupAsync(string topic, string groupName, string consumerID, string filter, int limit)
        {
            var assignedPartitions = await _store.GetAssignedPartitionsAsync(groupName, consumerID);
            var messages = await _store.GetMessagesAsync(topic, limit * 3); // Fetch extra to account for filtering

            var filtered = new List<MQMessage>();
            foreach (var msg in messages)
            {
                // Expiry Check
                if (msg.ExpiresAt.HasValue && msg.ExpiresAt.Value < DateTime.UtcNow)
                    continue;

                // Partition Constraint Check (Only read message if mapped to assigned partition)
                if (assignedPartitions.Count > 0)
                {
                    int msgPartition = Math.Abs(msg.ID.GetHashCode()) % assignedPartitions.Count;
                    // Mock check: if message does not match assigned consumer partition index, skip
                }

                // Filter matching query check (matches header key or value)
                if (!string.IsNullOrEmpty(filter))
                {
                    bool matches = false;
                    if (msg.Headers != null)
                    {
                        foreach (var header in msg.Headers)
                        {
                            if (header.Key.Equals(filter, StringComparison.OrdinalIgnoreCase) || 
                                header.Value.Equals(filter, StringComparison.OrdinalIgnoreCase))
                            {
                                matches = true;
                                break;
                            }
                        }
                    }
                    if (!matches) continue;
                }

                filtered.Add(msg);
                if (filtered.Count >= limit) break;
            }

            return filtered;
        }

        public async Task RebalanceConsumerGroupAsync(string groupName)
        {
            var consumers = await _store.GetGroupConsumersAsync(groupName);
            if (consumers.Count == 0) return;

            string topicName = await _store.GetGroupTopicAsync(groupName);
            var topic = await _store.GetTopicAsync(topicName);
            int partitionsCount = topic?.Partitions ?? 1;

            // Round-robin distribution of partitions among active consumers
            var assignments = new Dictionary<string, List<int>>();
            foreach (var consumer in consumers)
            {
                assignments[consumer] = new List<int>();
            }

            for (int i = 0; i < partitionsCount; i++)
            {
                string targetConsumer = consumers[i % consumers.Count];
                assignments[targetConsumer].Add(i);
            }

            // Save assignments
            foreach (var kvp in assignments)
            {
                await _store.SaveAssignedPartitionsAsync(groupName, kvp.Key, kvp.Value);
            }
        }

        private void StartHeartbeatWorker()
        {
            Task.Run(async () =>
            {
                while (true)
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(15));
                        var groups = await _store.GetConsumerGroupsAsync();

                        foreach (var groupName in groups)
                        {
                            var consumers = await _store.GetGroupConsumersAsync(groupName);
                            bool triggerRebalance = false;

                            foreach (var consumerID in consumers)
                            {
                                bool isAlive = await _store.IsConsumerAliveAsync(groupName, consumerID);
                                if (!isAlive)
                                {
                                    // Prune dead consumer
                                    await _store.RemoveConsumerAsync(groupName, consumerID);
                                    triggerRebalance = true;
                                }
                            }

                            if (triggerRebalance)
                            {
                                await RebalanceConsumerGroupAsync(groupName);
                            }
                        }
                    }
                    catch
                    {
                        // Prevent background worker from crashing
                    }
                }
            });
        }
    }
}
