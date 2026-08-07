using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Ma7MQ.Core.Types;

namespace Ma7MQ.Core.Broker
{
    public class Coordinator
    {
        private readonly ConcurrentDictionary<string, MQConsumerGroup> _groups = new();
        private readonly TimeSpan _checkInterval;
        private Timer _timer;

        public Coordinator(TimeSpan checkInterval)
        {
            _checkInterval = checkInterval;
        }

        public void Start(CancellationToken cancellationToken)
        {
            _timer = new Timer(RebalanceDeadConsumers, null, TimeSpan.Zero, _checkInterval);
            cancellationToken.Register(() => _timer?.Dispose());
        }

        public void RegisterConsumer(string groupName, string topic, string consumerID)
        {
            var group = _groups.GetOrAdd(groupName, name => new MQConsumerGroup
            {
                Name = groupName,
                Topic = topic
            });

            lock (group)
            {
                group.Consumers[consumerID] = new MQConsumer
                {
                    ID = consumerID,
                    LastHeartbeat = DateTime.UtcNow,
                    State = ConsumerState.Active
                };
            }
        }

        public void UpdateHeartbeat(string groupName, string consumerID)
        {
            if (_groups.TryGetValue(groupName, out var group))
            {
                lock (group)
                {
                    if (group.Consumers.TryGetValue(consumerID, out var consumer))
                    {
                        consumer.LastHeartbeat = DateTime.UtcNow;
                        consumer.State = ConsumerState.Active;
                    }
                }
            }
        }

        private void RebalanceDeadConsumers(object state)
        {
            foreach (var group in _groups.Values)
            {
                bool needsRebalance = false;
                lock (group)
                {
                    foreach (var consumer in group.Consumers.Values)
                    {
                        if (consumer.State == ConsumerState.Rebalancing)
                            continue;

                        if (DateTime.UtcNow - consumer.LastHeartbeat > TimeSpan.FromSeconds(30))
                        {
                            consumer.State = ConsumerState.Inactive;
                            needsRebalance = true;
                        }
                    }

                    if (needsRebalance)
                    {
                        TriggerRebalance(group);
                    }
                }
            }
        }

        private void TriggerRebalance(MQConsumerGroup group)
        {
            foreach (var consumer in group.Consumers.Values)
            {
                if (consumer.State == ConsumerState.Active)
                {
                    consumer.State = ConsumerState.Rebalancing;
                }
            }
        }
    }
}
