using Prometheus;

namespace Ma7MQ.Server
{
    public class MetricsExporter
    {
        public Counter MessagesPublished { get; }
        public Counter MessagesDelivered { get; }
        public Counter MessagesFailed { get; }
        public Histogram PublishLatency { get; }

        public MetricsExporter()
        {
            MessagesPublished = Metrics.CreateCounter("mq_messages_published_total", "The total number of published messages");
            MessagesDelivered = Metrics.CreateCounter("mq_messages_delivered_total", "The total number of successfully delivered messages");
            MessagesFailed = Metrics.CreateCounter("mq_messages_failed_total", "The total number of message failures");
            PublishLatency = Metrics.CreateHistogram("mq_publish_latency_seconds", "Latency distributions for published messages");
        }
    }
}
