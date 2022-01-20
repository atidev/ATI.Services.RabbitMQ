using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using ATI.Services.Common.Initializers;
using ATI.Services.Common.Initializers.Interfaces;
using ATI.Services.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace ATI.Services.RabbitMQ
{
    [InitializeOrder(Order = InitializeOrder.First)]
    public class RmqConnection : IDisposable, IInitializer
    {
        private readonly ILogger<RmqConnection> _logger;
        private readonly RmqConnectionConfig _config;
        private IConnection _connection;
        private readonly IServiceProvider _serviceProvider;
        private readonly ConcurrentDictionary<string, InternalRmqProducer> _customRmqProducers = new ConcurrentDictionary<string, InternalRmqProducer>();
        private readonly ConcurrentBag<IRmqConsumer> _customRmqConsumers = new ConcurrentBag<IRmqConsumer>();

        private readonly object _initializationLock = new();
        private bool _initialized;

        public RmqConnection(IOptions<RmqConnectionConfig> config, ILogger<RmqConnection> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _config = config.Value;
        }

        public void Init()
        {
            if (_initialized)
            {
                return;
            }

            lock (_initializationLock)
            {
                if (_initialized)
                {
                    return;
                }

                var producers = _serviceProvider.GetServices<IRmqProducer>();
                var consumers = _serviceProvider.GetServices<IRmqConsumer>();

                Init(producers.Concat(_customRmqProducers.Values), consumers.Concat(_customRmqConsumers));
                _initialized = true;
            }
        }

        public void RegisterProducer(
            string exchangeName,
            string defaultRoutingKey,
            ISerializer serializer,
            bool durable = true,
            ExchangeType exchangeType = ExchangeType.Topic)
        {
            var producer = new InternalRmqProducer(_logger, exchangeType, serializer, exchangeName, defaultRoutingKey, durable);
            _customRmqProducers.GetOrAdd(exchangeName, producer);
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

            // Проверяем, что создан
            var producer = _customRmqProducers.GetOrAdd(exchangeName, _ =>
            {
                var serializer = NewtonsoftJsonSerializer.SnakeCase;
                return new InternalRmqProducer(_logger, exchangeType, serializer, exchangeName, routingKey, durable);
            });

            // Проверяем, что заиничен
            producer.EnsureInitialized(_connection, timeout);

            // отправляем
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

            // Проверяем, что создан
            var producer = _customRmqProducers.GetOrAdd(exchangeName, _ =>
            {
                serializer ??= NewtonsoftJsonSerializer.SnakeCase;
                return new InternalRmqProducer(_logger, exchangeType, serializer, exchangeName, routingKey, durable);
            });

            // Проверяем, что заиничен
            producer.EnsureInitialized(_connection, timeout);

            // отправляем
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
            var consumer = new InternalRmqConsumer<T>(
                _logger, onReceivedAsync, exchangeType, exchangeName, routingKey, serializer, queueName, autoDelete, durable);

            lock (_initializationLock)
            {
                _customRmqConsumers.Add(consumer);
                if (_initialized)
                {
                    consumer.Init(_connection);
                }
            }
        }

        private void Init(IEnumerable<IRmqProducer> producers, IEnumerable<IRmqConsumer> consumers)
        {
            var factory = new ConnectionFactory
            {
                AutomaticRecoveryEnabled = true,
                DispatchConsumersAsync = true
            };

            var connectionUri = new Uri(_config.ConnectionString);
            FillUserInfo(connectionUri, factory);

            var amqpTcpEndpoints = GetAmqpTcpEndpoints(connectionUri);
            _connection = factory.CreateConnection(amqpTcpEndpoints);

            foreach (var producer in producers)
            {
                producer.Init(_connection, _config.PublishMessageTimeout);
            }

            foreach (var consumer in consumers)
            {
                consumer.Init(_connection);
            }

            _connection.ConnectionShutdown += (obj, args) =>
            {
                _logger.LogError($"Rmq connection shutdown. {args.ReplyText}");
            };

            _connection.CallbackException += (obj, args) =>
            {
                _logger.LogError(args.Exception, "Rmq callback exception.");
            };

            _connection.ConnectionBlocked += (obj, args) =>
            {
                _logger.LogError($"Rmq connection blocked. {args.Reason}");
            };

            _connection.ConnectionUnblocked += (obj, args) =>
            {
                _logger.LogWarning("Rmq connection unblocked");
            };
        }

        private static List<AmqpTcpEndpoint> GetAmqpTcpEndpoints(Uri connectionUri)
        {
            var ipAddress = Dns.GetHostAddresses(connectionUri.Host);
            var amqpTcpEndpoints = ipAddress.Select(ip => new AmqpTcpEndpoint(ip.ToString(), connectionUri.Port)).ToList();
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
            Init();
            return Task.CompletedTask;
        }
    }
}