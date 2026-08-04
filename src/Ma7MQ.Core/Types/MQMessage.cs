using System;
using System.Collections.Generic;

namespace Ma7MQ.Core.Types
{
    public enum MessagePriority
    {
        Low = 0,
        Normal = 1,
        High = 2
    }

    public enum DeliveryGuarantee
    {
        AtMostOnce = 0,
        AtLeastOnce = 1,
        ExactlyOnce = 2
    }

    public enum MessageStatus
    {
        Queued,
        Processing,
        Acked,
        Failed,
        DLQ
    }

    public class MQMessage
    {
        public string ID { get; set; }
        public string Topic { get; set; }
        public byte[] Payload { get; set; }
        public Dictionary<string, string> Headers { get; set; }
        public MessagePriority Priority { get; set; }
        public DeliveryGuarantee Guarantee { get; set; }
        public int Retries { get; set; }
        public DateTime CreatedAt { get; set; }
        public MessageStatus Status { get; set; }

        public MQMessage()
        {
            ID = Guid.NewGuid().ToString();
            Headers = new Dictionary<string, string>();
            CreatedAt = DateTime.UtcNow;
            Status = MessageStatus.Queued;
        }
    }
}
