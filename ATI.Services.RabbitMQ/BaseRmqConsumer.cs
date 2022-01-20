using System;
using System.Threading.Tasks;
using ATI.Services.Common.Logging;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ATI.Services.RabbitMQ
{
    public abstract class BaseRmqConsumer<T> : BaseRmqProvider, IRmqConsumer
    {
        private IModel _channel;
        private AsyncEventingBasicConsumer _consumer;
        private readonly ILogger _logger;
        protected abstract string QueueName { get; }
        protected abstract bool AutoDelete { get; }
        protected virtual bool RequeueOnError => false;
        protected virtual bool DurableQueue => true;
        protected virtual bool AutoAck => true;
        protected abstract string RoutingKey { get; }

        protected BaseRmqConsumer(ILogger logger)
        {
            _logger = logger;
        }

        protected abstract Task OnReceivedAsync(T obj);

        public void Init(IConnection connection)
        {
            _channel = connection.CreateModel();
            _channel.ExchangeDeclare(exchange: ExchangeName, type: GetExchangeType(), durable: DurableExchange);
            _channel.QueueDeclare(queue: QueueName, durable: DurableQueue, exclusive: false, autoDelete: AutoDelete);
            _channel.QueueBind(QueueName, ExchangeName, RoutingKey);

            _consumer = new AsyncEventingBasicConsumer(_channel);
            _consumer.Received += async (_, args) => await OnReceivedInternalAsync(args).ConfigureAwait(false);

            _channel.BasicConsume(queue: QueueName, autoAck: AutoAck, consumer: _consumer);

            RabbitMqDeclaredQueues.DeclaredQueues.Add(new QueueInfo { QueueName = QueueName });
        }

        private async Task OnReceivedInternalAsync(BasicDeliverEventArgs ea)
        {
            try
            {
                var body = ea.Body.ToArray();
                var obj = Serializer.Deserialize<T>(body);
                await OnReceivedAsync(obj).ConfigureAwait(false);
                if (!AutoAck)
                {
                    _channel.BasicAck(ea.DeliveryTag, false);
                }
            }
            catch (Exception e)
            {
                _logger.ErrorWithObject(e, $"Error during message processing {GetType()}", ea);
                if (!AutoAck)
                {
                    _channel.BasicNack(ea.DeliveryTag, false, RequeueOnError);
                }
            }
        }

        public void Dispose()
        {
            _channel?.Dispose();
        }
    }
}