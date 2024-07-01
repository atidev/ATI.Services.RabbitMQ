using System;
using EasyNetQ;
using EasyNetQ.Topology;

namespace ATI.Services.RabbitMQ;

/// <param name="queueConfiguration">Configuration for queue declare. This configuration will be overriden by params in SubscribeAsync method</param>
/// <param name="consumerConfiguration">Configuration for consumer</param>
public class QueueExchangeBinding(
    ExchangeInfo exchange,
    Queue queue,
    string routingKey,
    string queueType = QueueType.Quorum,
    Action<IQueueDeclareConfiguration> queueConfiguration = null,
    Action<ISimpleConsumeConfiguration> consumerConfiguration = null)
{
    [Obsolete("Use constructor with queueConfiguration and consumerConfiguration")]
    public QueueExchangeBinding(ExchangeInfo exchange,
                                Queue queue,
                                string routingKey,
                                string queueType = EasyNetQ.QueueType.Quorum)
        :this(exchange, queue, routingKey, queueType, null, null)
    {
    }
    
    public Queue Queue { get; } = queue;
    public string RoutingKey { get; } = routingKey;
    public ExchangeInfo Exchange { get; } = exchange;
    public string QueueType { get; } = queueType;
    /// <summary>
    /// Configuration for queue declare
    /// This configuration will be overriden by params in SubscribeAsync method
    /// </summary>
    public Action<IQueueDeclareConfiguration> QueueConfiguration { get; } = queueConfiguration;
    /// <summary>
    /// Configuration for consumer
    /// </summary>
    public Action<ISimpleConsumeConfiguration> ConsumerConfiguration { get; } = consumerConfiguration;
}