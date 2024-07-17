using System;
using System.Threading.Tasks;
using EasyNetQ;
using EasyNetQ.Consumer;

namespace ATI.Services.RabbitMQ;

public class SubscriptionInfo
{
    public required QueueExchangeBinding Binding { get; init; }

    public required Func<byte[], MessageProperties, MessageReceivedInfo, Task<AckStrategy>> EventbusSubscriptionHandler { get; init; }

    public string? MetricsEntity { get; init; }
    public required IDisposable Consumer { get; set; }

    public Task? ResubscribeTask { get; set; }
    public object ResubscribeLock { get; } = new();
}