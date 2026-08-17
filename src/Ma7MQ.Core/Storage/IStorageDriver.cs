using System.Collections.Generic;
using System.Threading.Tasks;
using Ma7MQ.Core.Types;

namespace Ma7MQ.Core.Storage
{
    public interface IStorageDriver
    {
        Task SaveMessageAsync(MQMessage msg);
        Task<List<MQMessage>> GetMessagesAsync(string topic, int limit);
        Task SaveTopicAsync(MQTopic topic);
        Task RegisterConsumerAsync(string groupName, string consumerID);
        Task UpdateHeartbeatAsync(string groupName, string consumerID);
        Task<long> GetTopicsCountAsync();
        Task<long> GetActiveConsumersCountAsync();
        Task<List<string>> GetTopicNamesAsync();
        Task<List<MQConsumer>> GetActiveConsumersAsync();
    }
}
