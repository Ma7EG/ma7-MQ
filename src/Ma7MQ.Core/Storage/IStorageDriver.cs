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
        Task<MQTopic> GetTopicAsync(string name);
        Task RegisterConsumerAsync(string groupName, string consumerID);
        Task RemoveConsumerAsync(string groupName, string consumerID);
        Task UpdateHeartbeatAsync(string groupName, string consumerID);
        Task<List<string>> GetGroupConsumersAsync(string groupName);
        Task<long> GetTopicsCountAsync();
        Task<long> GetActiveConsumersCountAsync();
        Task<List<string>> GetTopicNamesAsync();
        Task<List<MQConsumer>> GetActiveConsumersAsync();
        Task<bool> IsConsumerAliveAsync(string groupName, string consumerID);
        Task SaveAssignedPartitionsAsync(string groupName, string consumerID, List<int> partitions);
        Task<List<int>> GetAssignedPartitionsAsync(string groupName, string consumerID);
        Task<List<string>> GetConsumerGroupsAsync();
        Task RegisterConsumerGroupAsync(string groupName, string topicName);
        Task<string> GetGroupTopicAsync(string groupName);
    }
}
