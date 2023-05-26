using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using ATI.Services.Common.Context;
using ATI.Services.Common.Extensions;
using ATI.Services.Common.Initializers;
using ATI.Services.Common.Initializers.Interfaces;
using ATI.Services.Common.Localization;
using ATI.Services.Common.Logging;
using ATI.Services.Common.Metrics;
using ATI.Services.Common.Variables;
using EasyNetQ;
using EasyNetQ.Topology;
using JetBrains.Annotations;
using Microsoft.Extensions.Options;
using NLog;
using Polly;
using Polly.Retry;
using Polly.Wrap;
using JsonSerializer = Newtonsoft.Json.JsonSerializer;

namespace ATI.Services.RabbitMQ
{
    [PublicAPI]
    [InitializeOrder(Order = InitializeOrder.First)]
    public class EventbusManager : IDisposable, IInitializer
    {
        private IAdvancedBus _busClient;
        private const int RetryAttemptMax = 3;
        private const int MaxRetryDelayPow = 2;
        private readonly JsonSerializer _jsonSerializer;
        private readonly string _connectionString;

        private readonly MetricsFactory _metricsTracingFactory =
            MetricsFactory.CreateRepositoryMetricsFactory(nameof(EventbusManager));

        private readonly ILogger _logger = LogManager.GetCurrentClassLogger();
        private List<SubscriptionInfo> _subscriptions = new();
        private readonly AsyncRetryPolicy _retryForeverPolicy;
        private readonly AsyncRetryPolicy _subscribePolicy;
        private readonly EventbusOptions _options;
        private static readonly UTF8Encoding BodyEncoding = new(false);
        private readonly RmqTopology _rmqTopology;

        private const string AcceptLangHeaderName = "accept_language";

        public EventbusManager(JsonSerializer jsonSerializer,
                               IOptions<EventbusOptions> options, RmqTopology rmqTopology)
        {
            _options = options.Value;
            _connectionString = options.Value.ConnectionString;
            _jsonSerializer = jsonSerializer;
            _rmqTopology = rmqTopology;

            _subscribePolicy = Policy.Handle<Exception>()
                .WaitAndRetryForeverAsync(
                    _ => _options.RabbitConnectInterval,
                    (exception, _) => _logger.Error(exception));

            _retryForeverPolicy =
                Policy.Handle<Exception>()
                    .WaitAndRetryForeverAsync(
                        retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, Math.Min(retryAttempt, MaxRetryDelayPow))),
                        (exception, _) => _logger.ErrorWithObject(exception, _subscriptions));
        }

        public Task InitializeAsync()
        {
            try
            {
                _busClient = RabbitHutch.CreateBus(_connectionString,
                    serviceRegister =>
                    {
                        serviceRegister.Register<IConventions>(c =>
                            new RabbitMqConventions(c.Resolve<ITypeNameSerializer>(), _options));
                    }).Advanced;

                _busClient.Connected += async (_, _) => await ResubscribeOnReconnect();
                _busClient.Disconnected += (_, _) => { _logger.Error("Disconnected from RMQ for some reason!"); };
            }
            catch (Exception exception)
            {
                _logger.Error(exception);
            }

            return Task.CompletedTask;
        }

        public Task<IExchange> DeclareExchangeTopicAsync(string exchangeName, bool durable, bool autoDelete)
        {
            return _busClient.ExchangeDeclareAsync(exchangeName, ExchangeType.Topic, durable, autoDelete);
        }

        public async Task PublishRawAsync(
            string publishBody,
            string exchangeName,
            string routingKey,
            string metricEntity,
            Dictionary<string, object> additionalHeaders = null,
            bool mandatory = false,
            TimeSpan? timeout = null,
            bool withAcceptLang = true)
        {
            if (string.IsNullOrWhiteSpace(exchangeName) || string.IsNullOrWhiteSpace(routingKey) ||
                string.IsNullOrWhiteSpace(publishBody))
                return;

            using (_metricsTracingFactory.CreateLoggingMetricsTimer(metricEntity))
            {
                var messageProperties = GetProperties(additionalHeaders, withAcceptLang);
                var exchange = new Exchange(exchangeName);
                var body = BodyEncoding.GetBytes(publishBody);

                var sendingResult = await SetupPolicy(timeout).ExecuteAndCaptureAsync(async () =>
                    await _busClient.PublishAsync(
                        exchange,
                        routingKey,
                        mandatory,
                        messageProperties,
                        body));

                if (sendingResult.FinalException != null)
                {
                    _logger.ErrorWithObject(sendingResult.FinalException,
                        new { publishBody, exchangeName, routingKey, metricEntity, mandatory });
                }
            }
        }

        public async Task PublishAsync<T>(
            T publishObject,
            string exchangeName,
            string routingKey,
            string metricEntity,
            Dictionary<string, object> additionalHeaders = null,
            bool mandatory = false,
            JsonSerializer serializer = null,
            TimeSpan? timeout = null,
            bool withAcceptLang = true)
        {
            if (string.IsNullOrWhiteSpace(exchangeName) || string.IsNullOrWhiteSpace(routingKey) ||
                publishObject == null)
                return;

            using (_metricsTracingFactory.CreateLoggingMetricsTimer(metricEntity))
            {
                var messageProperties = GetProperties(additionalHeaders, withAcceptLang);
                var exchange = new Exchange(exchangeName);
                var bodySerializer = serializer ?? _jsonSerializer;
                var body = bodySerializer.ToJsonBytes(publishObject);

                var sendingResult = await SetupPolicy(timeout).ExecuteAndCaptureAsync(async () =>
                    await _busClient.PublishAsync(
                        exchange,
                        routingKey,
                        mandatory,
                        messageProperties,
                        body));

                if (sendingResult.FinalException != null)
                {
                    _logger.ErrorWithObject(sendingResult.FinalException,
                        new { publishObject, exchangeName, routingKey, metricEntity, mandatory });
                }
            }
        }

        public Task SubscribeAsync(
            string exchangeName,
            string routingKey,
            bool isExclusive,
            bool isDurable,
            bool isAutoDelete,
            Func<byte[], MessageProperties, MessageReceivedInfo, Task> handler,
            bool isExclusiveQueueName = false,
            string customQueueName = null,
            string metricEntity = null)
        {
            var binding = _rmqTopology.CreateBinding(exchangeName, routingKey, isExclusive, isDurable, isAutoDelete,
                isExclusiveQueueName, customQueueName);
            return SubscribeAsync(binding, handler, metricEntity);
        }

        public async Task SubscribeAsync(
            QueueExchangeBinding bindingInfo,
            Func<byte[], MessageProperties, MessageReceivedInfo, Task> handler,
            string metricEntity = null)
        {
            _subscriptions.Add(new SubscriptionInfo
            {
                Binding = bindingInfo,
                EventbusSubscriptionHandler = handler,
                MetricsEntity = metricEntity
            });

            RabbitMqDeclaredQueues.DeclaredQueues.Add(bindingInfo.Queue);

            if (_busClient.IsConnected)
            {
                try
                {
                    await SubscribePrivateAsync(bindingInfo, handler, metricEntity);
                }
                // В интервале между проверкой _busClient.IsConnected и SubscribeAsyncPrivate Rabbit может отвалиться, поэтому запускаем в бекграунд потоке
                catch (Exception ex)
                {
                    _logger.Error(ex);
                    _subscribePolicy.ExecuteAsync(async () =>
                        await SubscribePrivateAsync(bindingInfo, handler, metricEntity)).Forget();
                }
            }
            else
            {
                _subscribePolicy.ExecuteAsync(async () =>
                    await SubscribePrivateAsync(bindingInfo, handler, metricEntity)).Forget();
            }
        }

        private AsyncPolicyWrap SetupPolicy(TimeSpan? timeout = null) =>
            Policy.WrapAsync(Policy.TimeoutAsync(timeout ?? TimeSpan.FromSeconds(2)),
                Policy.Handle<Exception>()
                    .WaitAndRetryAsync(3, _ => TimeSpan.FromSeconds(3)));

        private async Task ExecuteWithPolicy(Func<Task> action)
        {
            var policy = Policy.Handle<TimeoutException>()
                .WaitAndRetryAsync(
                    RetryAttemptMax,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (exception, timeSpan, retryCount, _) =>
                    {
                        _logger.ErrorWithObject(exception, new { TimeSpan = timeSpan, RetryCount = retryCount });
                    });

            var policyResult = await policy.ExecuteAndCaptureAsync(async () => await action.Invoke());

            if (policyResult.FinalException != null)
            {
                _logger.ErrorWithObject(policyResult.FinalException, action);
            }
        }

        private async Task ResubscribeOnReconnect()
        {
            try
            {
                foreach (var subscription in _subscriptions)
                {
                    await _retryForeverPolicy.ExecuteAsync(async () => await SubscribePrivateAsync(
                                                                           subscription.Binding,
                                                                           subscription.EventbusSubscriptionHandler,
                                                                           subscription.MetricsEntity));
                }
            }
            catch (Exception e)
            {
                _logger.ErrorWithObject(e, _subscriptions);
            }
        }

        private async Task SubscribePrivateAsync(
            QueueExchangeBinding bindingInfo,
            Func<byte[], MessageProperties, MessageReceivedInfo, Task> handler,
            string metricEntity)
        {
            var queue = await _busClient.QueueDeclareAsync(
                name: bindingInfo.Queue.Name,
                autoDelete: bindingInfo.Queue.IsAutoDelete,
                durable: bindingInfo.Queue.IsDurable,
                exclusive: bindingInfo.Queue.IsExclusive);
            
            var exchange = new Exchange(bindingInfo.Exchange.Name, bindingInfo.Exchange.Type,
                bindingInfo.Queue.IsDurable, bindingInfo.Queue.IsAutoDelete);

            _busClient.Bind(exchange, bindingInfo.Queue, bindingInfo.RoutingKey);
            _busClient.Consume(queue,
                async (body, props, info) =>
                    await HandleEventBusMessageWithPolicy(body, props, info));

            async Task HandleEventBusMessageWithPolicy(byte[] body, MessageProperties props,
                MessageReceivedInfo info)
            {
                using (_metricsTracingFactory.CreateLoggingMetricsTimer(metricEntity ?? "Eventbus"))
                {
                    HandleMessageProps(props);
                    await ExecuteWithPolicy(async () => await handler.Invoke(body, props, info));
                }
            }
        }

        private void HandleMessageProps(MessageProperties props)
        {
            if (!props.HeadersPresent)
                return;

            GetAcceptLanguageFromProperties(props);
        }

        private void GetAcceptLanguageFromProperties(MessageProperties props)
        {
            if (!props.Headers.TryGetValue(MessagePropertiesNames.AcceptLang, out var acceptLanguage))
                return;

            var acceptLanguageStr = BodyEncoding.GetString((byte[])acceptLanguage);
            FlowContext<RequestMetaData>.Current =
                new RequestMetaData
                {
                    RabbitAcceptLanguage = acceptLanguageStr
                };

            if (LocaleHelper.TryGetFromString(acceptLanguageStr, out var cultureInfo))
                CultureInfo.CurrentUICulture = cultureInfo;
        }

        private MessageProperties GetProperties(Dictionary<string, object> additionalHeaders, bool withAcceptLang)
        {
            var messageProperties = new MessageProperties
            {
                AppId = ServiceVariables.ServiceAsClientName
            };

            SetTraceHeadersFromActivity(messageProperties);
            if (withAcceptLang)
                SetAcceptLanguageHeader(messageProperties);

            if (additionalHeaders != null)
            {
                foreach (var additionalHeader in additionalHeaders)
                {
                    messageProperties.Headers.TryAdd(additionalHeader.Key, additionalHeader.Value);
                }
            }

            return messageProperties;
        }

        private void SetTraceHeadersFromActivity(MessageProperties properties)
        {
            if (Activity.Current is not { Baggage: { } bgg })
                return;

            var baggageArray = bgg.ToArray();
            if (baggageArray.Length == 0)
                return;

            var baggageProperties = baggageArray
                .Select(b => $"{WebUtility.UrlEncode(b.Key)}={WebUtility.UrlEncode(b.Value)}");

            properties.Headers.Add(MessagePropertiesNames.Baggage, string.Join(", ", baggageProperties));
        }

        private void SetAcceptLanguageHeader(MessageProperties properties)
        {
            var flowAcceptLang = FlowContext<RequestMetaData>.Current.AcceptLanguage;
            if (flowAcceptLang != null)
                properties.Headers.Add(MessagePropertiesNames.AcceptLang, flowAcceptLang);
        }

        public void Dispose()
        {
            // Сделано для удобства локального тестирования, удаляем наши созданные очереди
            if (_options.DeleteQueuesOnApplicationShutdown && _busClient != null)
            {
                foreach (var queue in RabbitMqDeclaredQueues.DeclaredQueues)
                {
                    _busClient.QueueDelete(queue);
                }
            }

            _busClient?.Dispose();
        }

        public string InitStartConsoleMessage()
        {
            return "Start Eventbus initializer";
        }

        public string InitEndConsoleMessage()
        {
            return "End Eventbus initializer";
        }
    }
}