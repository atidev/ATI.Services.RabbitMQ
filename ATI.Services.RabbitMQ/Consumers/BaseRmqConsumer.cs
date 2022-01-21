using System.Threading.Tasks;
using JetBrains.Annotations;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ATI.Services.RabbitMQ.Consumers
{
    [PublicAPI]
    public abstract class BaseRmqConsumer : BaseRmqProvider, IRmqConsumer
    {
        protected IModel Channel;
        private AsyncEventingBasicConsumer _consumer;
        protected abstract string QueueName { get; }
        protected abstract bool AutoDelete { get; }
        protected virtual bool RequeueOnError => false;
        protected virtual bool DurableQueue => true;
        protected virtual bool AutoAck => true;
        protected abstract string RoutingKey { get; }
        
        public void Init(IConnection connection)
        {
            Channel = connection.CreateModel();
            Channel.ExchangeDeclare(exchange: ExchangeName, type: GetExchangeType(), durable: DurableExchange);
            Channel.QueueDeclare(queue: QueueName, durable: DurableQueue, exclusive: false, autoDelete: AutoDelete);
            Channel.QueueBind(QueueName, ExchangeName, RoutingKey);

            _consumer = new AsyncEventingBasicConsumer(Channel);
            _consumer.Received += async (_, args) => await OnReceivedInternalAsync(args).ConfigureAwait(false);

            Channel.BasicConsume(queue: QueueName, autoAck: AutoAck, consumer: _consumer);

            RabbitMqDeclaredQueues.DeclaredQueues.Add(new QueueInfo { QueueName = QueueName });
        }

        protected abstract Task OnReceivedInternalAsync(BasicDeliverEventArgs ea);
        public void Dispose()
        {
            Channel?.Dispose();
        }
    }
}