using System;

namespace Ma7MQ.Core.Types
{
    public class MQTopicConfig
    {
        public int MaxPayloadSize { get; set; } = 1024 * 1024; // 1MB default
        public bool Compression { get; set; } = true;
    }

    public class MQTopic
    {
        public string Name { get; set; }
        public int Partitions { get; set; } = 1;
        public TimeSpan RetentionTime { get; set; } = TimeSpan.FromDays(7);
        public MQTopicConfig Config { get; set; } = new MQTopicConfig();
    }
}
