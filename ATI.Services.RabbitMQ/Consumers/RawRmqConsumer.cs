using System;
using System.Threading.Tasks;
using ATI.Services.Common.Logging;
using JetBrains.Annotations;
using NLog;
using RabbitMQ.Client.Events;

namespace ATI.Services.RabbitMQ.Consumers
{
    [PublicAPI]
    internal sealed class RawRmqConsumer : BaseRmqConsumer
    {
        private readonly ILogger _logger;
        private readonly Func<byte[], Task> _onReceivedAsync;

        public RawRmqConsumer(
            ILogger logger,
            Func<byte[], Task> onReceivedAsync,
            ExchangeType exchangeType,
            string exchangeName,
            string routingKey,
            string queueName,
            bool autoDelete,
            bool durableQueue)
        {
            _logger = logger;
            _onReceivedAsync = onReceivedAsync;
            ExchangeType = exchangeType;
            ExchangeName = exchangeName;
            QueueName = queueName;
            AutoDelete = autoDelete;
            RoutingKey = routingKey;
            DurableQueue = durableQueue;
        }

        protected override ExchangeType ExchangeType { get; }
        protected override string ExchangeName { get; }
        protected override string QueueName { get; }
        protected override bool AutoDelete { get; }
        protected override string RoutingKey { get; }
        protected override bool DurableQueue { get; }

        protected override async Task OnReceivedInternalAsync(BasicDeliverEventArgs ea)
        {
            try
            {
                await _onReceivedAsync(ea.Body.ToArray()).ConfigureAwait(false);
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