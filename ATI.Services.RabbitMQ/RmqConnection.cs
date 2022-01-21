using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using ATI.Services.Common.Initializers;
using ATI.Services.Common.Initializers.Interfaces;
using ATI.Services.Common.Logging;
using ATI.Services.RabbitMQ.Consumers;
using ATI.Services.RabbitMQ.Producers;
using ATI.Services.Serialization;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NLog;
using RabbitMQ.Client;
using ILogger = NLog.ILogger;

namespace ATI.Services.RabbitMQ
{
    [PublicAPI]
    [InitializeOrder(Order = InitializeOrder.First)]
    public class RmqConnection : IDisposable, IInitializer
    {
        private readonly ILogger _logger = LogManager.GetCurrentClassLogger();
        private readonly RmqConnectionConfig _config;
        private IConnection _connection;
        private readonly IServiceProvider _serviceProvider;

        private readonly ConcurrentDictionary<string, RmqProducer> _customRmqProducers =
            new ConcurrentDictionary<string, RmqProducer>();

        private readonly ConcurrentBag<IRmqConsumer> _customRmqConsumers = new ConcurrentBag<IRmqConsumer>();

        private readonly object _initializationLock = new object();

        public RmqConnection(IOptions<RmqConnectionConfig> config,
            IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            _config = config.Value;
        }

        public void RegisterProducer(
            string exchangeName,
            string defaultRoutingKey,
            ISerializer serializer,
            bool durable = true,
            ExchangeType exchangeType = ExchangeType.Topic,
            TimeSpan timeout = default)
        {
            var producer = new RmqProducer(_logger, exchangeType, serializer, exchangeName, defaultRoutingKey, durable);
            _customRmqProducers.GetOrAdd(exchangeName, producer);

            lock (_initializationLock)
            {
                producer.Init(_connection, timeout == default ? _config.PublishMessageTimeout : timeout);
            }
        }

        public Task PublishBytesAsync(
            byte[] publishBody,
            string exchangeName,
            string routingKey,
            CancellationToken cancellationToken = default,
            bool durable = true,
            ExchangeType exchangeType = ExchangeType.Topic,
            TimeSpan timeout = default,
            TimeSpan expiration = default)
        {
            if (timeout == default)
            {
                timeout = _config.PublishMessageTimeout;
            }

            var producer = _customRmqProducers[exchangeName];

            return producer.PublishBytesAsync(publishBody, cancellationToken, routingKey, timeout, expiration);
        }

        public Task PublishAsync<T>(
            T publishBody,
            string exchangeName,
            string routingKey,
            CancellationToken cancellationToken = default,
            bool durable = true,
            ExchangeType exchangeType = ExchangeType.Topic,
            ISerializer serializer = default,
            TimeSpan timeout = default,
            TimeSpan expiration = default)
        {
            if (timeout == default)
            {
                timeout = _config.PublishMessageTimeout;
            }

            var producer = _customRmqProducers[exchangeName];

            return producer.PublishAsync(publishBody, cancellationToken, routingKey, timeout, expiration);
        }

        public void Subscribe<T>(
            string exchangeName,
            string queueName,
            string routingKey,
            Func<T, Task> onReceivedAsync,
            bool autoDelete,
            bool durable = true,
            ExchangeType exchangeType = ExchangeType.Topic,
            ISerializer serializer = default)
        {
            serializer ??= NewtonsoftJsonSerializer.SnakeCase;
            var consumer = new RmqConsumer<T>(
                _logger, onReceivedAsync, exchangeType, exchangeName, routingKey, serializer, queueName, autoDelete,
                durable);

            lock (_initializationLock)
            {
                _customRmqConsumers.Add(consumer);
                consumer.Init(_connection);
            }
        }

        public void SubscribeRaw(
            string exchangeName,
            string queueName,
            string routingKey,
            Func<byte[], Task> onReceivedAsync,
            bool autoDelete,
            bool durable = true,
            ExchangeType exchangeType = ExchangeType.Topic)
        {
            var consumer = new RawRmqConsumer(
                _logger, onReceivedAsync, exchangeType, exchangeName, routingKey, queueName, autoDelete,
                durable);

            lock (_initializationLock)
            {
                _customRmqConsumers.Add(consumer);
                consumer.Init(_connection);
            }
        }

        private static List<AmqpTcpEndpoint> GetAmqpTcpEndpoints(Uri connectionUri)
        {
            var ipAddress = Dns.GetHostAddresses(connectionUri.Host);
            var amqpTcpEndpoints =
                ipAddress.Select(ip => new AmqpTcpEndpoint(ip.ToString(), connectionUri.Port)).ToList();
            return amqpTcpEndpoints;
        }

        private static void FillUserInfo(Uri connectionUri, IConnectionFactory factory)
        {
            var userInfo = connectionUri.UserInfo;
            if (!string.IsNullOrEmpty(userInfo))
            {
                var strArray = userInfo.Split(':');
                if (strArray.Length > 2)
                {
                    throw new ArgumentException("Bad user info in AMQP URI: " + userInfo);
                }

                factory.UserName = UriDecode(strArray[0]);
                if (strArray.Length == 2)
                {
                    factory.Password = UriDecode(strArray[1]);
                }
            }
        }

        // Этот метод взят из RabbitMQ.Client
        // https://github.com/rabbitmq/rabbitmq-dotnet-client/blob/master/projects/RabbitMQ.Client/client/api/ConnectionFactory.cs#L600
        private static string UriDecode(string uri)
        {
            return Uri.UnescapeDataString(uri.Replace("+", "%2B"));
        }

        public void Dispose()
        {
            _connection?.Dispose();
            foreach (var rmqConsumer in _customRmqConsumers)
            {
                rmqConsumer.Dispose();
            }

            foreach (var (_, rmqProducer) in _customRmqProducers)
            {
                rmqProducer.Dispose();
            }
        }

        public Task InitializeAsync()
        {
            var producers = _serviceProvider.GetServices<IRmqProducer>();
            var consumers = _serviceProvider.GetServices<IRmqConsumer>();
            
            var factory = new ConnectionFactory
            {
                AutomaticRecoveryEnabled = true,
                DispatchConsumersAsync = true
            };

            var connectionUri = new Uri(_config.ConnectionString);
            FillUserInfo(connectionUri, factory);

            var amqpTcpEndpoints = GetAmqpTcpEndpoints(connectionUri);
            var connection = factory.CreateConnection(amqpTcpEndpoints);

            foreach (var producer in producers)
            {
                producer.Init(connection, _config.PublishMessageTimeout);
            }

            foreach (var consumer in consumers)
            {
                consumer.Init(connection);
            }

            connection.ConnectionShutdown += (_, args) =>
            {
                _logger.Error($"Rmq connection shutdown. {args.ReplyText}");
            };

            connection.CallbackException += (_, args) => { _logger.Error(args.Exception, "Rmq callback exception."); };

            connection.ConnectionBlocked += (_, args) => { _logger.Error($"Rmq connection blocked. {args.Reason}"); };

            connection.ConnectionUnblocked += (obj, args) =>
            {
                _logger.WarnWithObject("Rmq connection unblocked", obj, args);
            };
            
            lock (_initializationLock)
            {
                _connection = connection;
            }
            
            return Task.CompletedTask;
        }
    }
}