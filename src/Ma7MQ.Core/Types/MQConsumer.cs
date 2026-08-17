using System;
using System.Collections.Generic;

namespace Ma7MQ.Core.Types
{
    public enum ConsumerState
    {
        Active,
        Inactive,
        Rebalancing
    }

    public class MQConsumer
    {
        public string ID { get; set; }
        public DateTime LastHeartbeat { get; set; }
        public ConsumerState State { get; set; }
        public List<int> AssignedPartitions { get; set; } = new List<int>();
    }

    public class MQConsumerGroup
    {
        public string Name { get; set; }
        public string Topic { get; set; }
        public Dictionary<string, MQConsumer> Consumers { get; set; } = new Dictionary<string, MQConsumer>();
    }
}
