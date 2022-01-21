using System;
using System.Threading.Tasks;
using ATI.Services.Common.Logging;
using ATI.Services.Serialization;
using NLog;
using RabbitMQ.Client.Events;

namespace ATI.Services.RabbitMQ.Consumers
{
    internal sealed class RmqConsumer<T> : BaseRmqConsumer
    {
        private readonly ILogger _logger;
        private readonly Func<T, Task> _onReceivedAsync;
        
        protected override ExchangeType ExchangeType { get; }
        protected override string ExchangeName { get; }
        protected override string QueueName { get; }
        protected override bool AutoDelete { get; }
        protected override string RoutingKey { get; }
        protected override bool DurableQueue { get; }
        private ISerializer Serializer { get; }

        public RmqConsumer(
            ILogger logger,
            Func<T, Task> onReceivedAsync,
            ExchangeType exchangeType,
            string exchangeName,
            string routingKey,
            ISerializer serializer,
            string queueName,
            bool autoDelete,
            bool durableQueue)
        {
            _logger = logger;
            _onReceivedAsync = onReceivedAsync;
            ExchangeType = exchangeType;
            Serializer = serializer;
            ExchangeName = exchangeName;
            QueueName = queueName;
            AutoDelete = autoDelete;
            RoutingKey = routingKey;
            DurableQueue = durableQueue;
        }


        protected override async Task OnReceivedInternalAsync(BasicDeliverEventArgs ea)
        {
            try
            {
                var body = ea.Body.ToArray();
                var obj = Serializer.Deserialize<T>(body);
                await _onReceivedAsync(obj).ConfigureAwait(false);
                if (!AutoAck)
                {
                    Channel.BasicAck(ea.DeliveryTag, false);
                }
            }
            catch (Exception e)
            {
                _logger.ErrorWithObject(e, $"Error during message processing {GetType()}", ea);
                if (!AutoAck)
                {
                    Channel.BasicNack(ea.DeliveryTag, false, RequeueOnError);
                }
            }
        }
    }
}