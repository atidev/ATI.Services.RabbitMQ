using System;
using System.Threading.Tasks;
using EasyNetQ;
using EasyNetQ.Consumer;

namespace ATI.Services.RabbitMQ;

public class SubscriptionInfo
{
    public QueueExchangeBinding Binding { get; set; }

    public Func<byte[], MessageProperties, MessageReceivedInfo, Task<AckStrategy>> EventbusSubscriptionHandler { get; set; }

    public string MetricsEntity { get; set; }
    public IDisposable Consumer { get; set; }

    public Task ResubscribeTask { get; set; }
    public object ResubscribeLock { get; } = new();
}