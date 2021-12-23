using System;
using System.Threading.Tasks;
using ATI.Services.Serialization;
using Microsoft.Extensions.Logging;

namespace ATI.Services.RabbitMQ
{
    internal sealed class InternalRmqConsumer<T> : BaseRmqConsumer<T>
    {
        private readonly Func<T, Task> _onReceivedAsync;

        public InternalRmqConsumer(
            ILogger logger,
            Func<T, Task> onReceivedAsync,
            ExchangeType exchangeType,
            string exchangeName,
            string routingKey,
            ISerializer serializer,
            string queueName,
            bool autoDelete,
            bool durableQueue) : base(logger)
        {
            _onReceivedAsync = onReceivedAsync;
            ExchangeType = exchangeType;
            Serializer = serializer;
            ExchangeName = exchangeName;
            QueueName = queueName;
            AutoDelete = autoDelete;
            RoutingKey = routingKey;
            DurableQueue = durableQueue;
        }

        protected override ExchangeType ExchangeType { get; }
        protected override ISerializer Serializer { get; }
        protected override string ExchangeName { get; }
        protected override string QueueName { get; }
        protected override bool AutoDelete { get; }
        protected override string RoutingKey { get; }
        protected override bool DurableQueue { get; }
        protected override Task OnReceivedAsync(T obj)
        {
            return _onReceivedAsync.Invoke(obj);
        }
    }
}