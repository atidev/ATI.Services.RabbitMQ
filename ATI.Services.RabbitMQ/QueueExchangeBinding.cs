using EasyNetQ.Topology;

namespace ATI.Services.RabbitMQ;

public class QueueExchangeBinding
{
    public QueueExchangeBinding(ExchangeInfo exchange, Queue queue, string routingKey, string queueType = EasyNetQ.QueueType.Quorum)
    {
        Queue = queue;
        RoutingKey = routingKey;
        Exchange = exchange;
        QueueType = queueType;
    }

    public Queue Queue { get; }
    public string RoutingKey { get; }
    public ExchangeInfo Exchange { get; }
    public string QueueType { get; set; }
}