using System;
using System.Threading.Tasks;
using Ma7MQ.Core.Types;
using Ma7MQ.Core.Storage;

namespace Ma7MQ.Core.Broker
{
    public interface IBrokerEngine
    {
        Task PublishAsync(MQMessage msg);
        Task CreateTopicAsync(MQTopic topic);
        Task RegisterConsumerAsync(string groupName, string consumerID);
        Task HeartbeatAsync(string groupName, string consumerID);
    }

    public class BrokerEngine : IBrokerEngine
    {
        private readonly IStorageDriver _store;

        public BrokerEngine(IStorageDriver store)
        {
            _store = store;
        }

        public Task CreateTopicAsync(MQTopic topic)
        {
            return _store.SaveTopicAsync(topic);
        }

        public async Task PublishAsync(MQMessage msg)
        {
            if (string.IsNullOrEmpty(msg.ID))
            {
                msg.ID = Guid.NewGuid().ToString();
            }

            // Shannon entropy-based compression check
            if (CompressionHelper.ShouldCompress(msg.Payload, 512))
            {
                var compressed = await CompressionHelper.CompressAsync(msg.Payload);
                msg.Payload = compressed;
                msg.Headers["x-compression"] = "gzip";
            }

            await _store.SaveMessageAsync(msg);
        }

        public Task RegisterConsumerAsync(string groupName, string consumerID)
        {
            return _store.RegisterConsumerAsync(groupName, consumerID);
        }

        public Task HeartbeatAsync(string groupName, string consumerID)
        {
            return _store.UpdateHeartbeatAsync(groupName, consumerID);
        }
    }
}
