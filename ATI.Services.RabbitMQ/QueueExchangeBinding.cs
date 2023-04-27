using EasyNetQ.Topology;

namespace ATI.Services.RabbitMQ
{
    public class QueueExchangeBinding
    {
        public QueueExchangeBinding(ExchangeInfo exchange, Queue queue, string routingKey)
        {
            Queue = queue;
            RoutingKey = routingKey;
            Exchange = exchange;
        }

        public Queue Queue { get; }
        public string RoutingKey { get; }
        public ExchangeInfo Exchange { get; }
    }
}
