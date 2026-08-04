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
    }

    public class MQConsumerGroup
    {
        public string Name { get; set; }
        public string Topic { get; set; }
        public Dictionary<string, MQConsumer> Consumers { get; set; } = new Dictionary<string, MQConsumer>();
    }
}
