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
        private readonly string _dlqPrefix = "dlq:";

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
                
                // Create enriched DLQ message preserving headers and payload
                var dlqMsg = new MQMessage
                {
                    ID = Guid.NewGuid().ToString(),
                    Topic = $"{_dlqPrefix}{msg.Topic}",
                    Payload = msg.Payload,
                    Headers = new Dictionary<string, string>(),
                    Priority = MessagePriority.High,
                    Guarantee = msg.Guarantee,
                    Retries = 0,
                    CreatedAt = DateTime.UtcNow,
                    Status = MessageStatus.DLQ
                };

                // Preserve original headers
                if (msg.Headers != null)
                {
                    foreach (var header in msg.Headers)
                    {
                        dlqMsg.Headers[header.Key] = header.Value;
                    }
                }

                // Add deep forensic metadata
                dlqMsg.Headers["dlq_reason"] = reason;
                dlqMsg.Headers["dlq_timestamp"] = DateTime.UtcNow.ToString("o");
                dlqMsg.Headers["original_message_id"] = msg.ID;
                dlqMsg.Headers["original_topic"] = msg.Topic;
                dlqMsg.Headers["original_retry_count"] = msg.Retries.ToString();
                dlqMsg.Headers["original_created_at"] = msg.CreatedAt.ToString("o");

                // Save DLQ message
                await _driver.SaveMessageAsync(dlqMsg);

                // Update and store the original message status history
                msg.Headers["dlq_moved_at"] = DateTime.UtcNow.ToString("o");
                msg.Headers["dlq_target_topic"] = dlqMsg.Topic;
                msg.Headers["dlq_target_id"] = dlqMsg.ID;
                msg.Headers["dlq_reason"] = reason;

                await _driver.SaveMessageAsync(msg);
            }
            else
            {
                msg.Status = MessageStatus.Failed;
                await _driver.SaveMessageAsync(msg);
            }
        }
    }
}
