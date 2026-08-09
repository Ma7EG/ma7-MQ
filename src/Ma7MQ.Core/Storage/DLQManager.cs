using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ma7MQ.Core.Types;

namespace Ma7MQ.Core.Storage
{
    public class DLQManager
    {
        private readonly IStorageDriver _driver;
        private readonly int _limit;

        public DLQManager(IStorageDriver driver, int limit)
        {
            _driver = driver;
            _limit = limit;
        }

        public async Task HandleFailureAsync(MQMessage msg, string reason)
        {
            msg.Retries++;
            if (msg.Retries >= _limit)
            {
                msg.Status = MessageStatus.DLQ;
                var dlqMsg = new MQMessage
                {
                    ID = Guid.NewGuid().ToString(),
                    Topic = $"dlq:{msg.Topic}",
                    Payload = msg.Payload,
                    Headers = new Dictionary<string, string>(),
                    Priority = MessagePriority.High,
                    Guarantee = msg.Guarantee,
                    Retries = 0,
                    CreatedAt = msg.CreatedAt,
                    Status = MessageStatus.DLQ
                };

                dlqMsg.Headers["original_id"] = msg.ID;
                dlqMsg.Headers["original_topic"] = msg.Topic;
                dlqMsg.Headers["dlq_reason"] = reason;

                await _driver.SaveMessageAsync(dlqMsg);
            }
            else
            {
                msg.Status = MessageStatus.Failed;
                await _driver.SaveMessageAsync(msg);
            }
        }
    }
}
