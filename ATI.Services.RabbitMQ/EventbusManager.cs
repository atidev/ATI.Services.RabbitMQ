using System;
using System.Collections.Concurrent;
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
using EasyNetQ.Consumer;
using EasyNetQ.DI;
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
        private const string DelayQueueSuffix = "_delay";
        private const string PoisonQueueSuffix = "_poison";
        private readonly JsonSerializer _jsonSerializer;
        private readonly string _connectionString;

        private readonly MetricsFactory _metricsTracingFactory =
            MetricsFactory.CreateRepositoryMetricsFactory(nameof(EventbusManager));

        private readonly ILogger _logger = LogManager.GetCurrentClassLogger();
        private ConcurrentBag<SubscriptionInfo> _subscriptions = new();
        private readonly AsyncRetryPolicy _retryForeverPolicy;
        private readonly AsyncRetryPolicy _subscribePolicy;
        private readonly EventbusOptions _options;
        private static readonly UTF8Encoding BodyEncoding = new(false);
        private readonly RmqTopology _rmqTopology;

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
                        (exception, _) => _logger.ErrorWithObject(exception));
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

        public Task<Exchange> DeclareExchangeTopicAsync(string exchangeName, bool durable, bool autoDelete) => 
            _busClient.ExchangeDeclareAsync(exchangeName, ExchangeType.Topic, durable, autoDelete);

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

            try
            {
                await SubscribePrivateAsync(bindingInfo, handler, metricEntity);
            }
            catch (Exception ex)
            {
                _logger.ErrorWithObject(ex,
                                        "Initial subscription failed, trying to subscribe in background",
                                        logObjects: bindingInfo.Queue.Name);
                _subscribePolicy.ExecuteAsync(() => SubscribePrivateAsync(bindingInfo, handler, metricEntity)).Forget();
            }
        }

#nullable enable
        public async Task SubscribeAsync(
            QueueExchangeBinding mainQueueBinding,
            bool durable,
            bool autoDelete,
            Func<byte[], MessageProperties, MessageReceivedInfo, Task<Acknowledgements>> handler,
            Func<byte[], MessageProperties, MessageReceivedInfo, Task<Acknowledgements>>? poisonHandler = null,
            DelayedRequeueConfiguration? requeueConfig = null,
            string? metricEntity = null)
        {
            RabbitMqDeclaredQueues.DeclaredQueues.Add(mainQueueBinding.Queue);

            if (requeueConfig?.MaxRetryRequeueCount == 0)
            {
                throw new ArgumentException(
                    $"{nameof(DelayedRequeueConfiguration.MaxRetryRequeueCount)} должен содержать себе минимум одну попытку",
                    nameof(DelayedRequeueConfiguration.MaxRetryRequeueCount));
            }

            QueueExchangeBinding? delayQueue = null;
            QueueExchangeBinding? poisonQueue = null;
            if (requeueConfig != null)
            {
                delayQueue = _rmqTopology.CreateBinding(
                    mainQueueBinding.Exchange.Name,
                    $"{mainQueueBinding.RoutingKey}{DelayQueueSuffix}",
                    isExclusive: false,
                    isDurable: true,
                    isAutoDelete: false);

                poisonQueue = _rmqTopology.CreateBinding(
                    mainQueueBinding.Exchange.Name,
                    $"{mainQueueBinding.RoutingKey}{PoisonQueueSuffix}",
                    isExclusive: false,
                    isDurable: true,
                    isAutoDelete: false);

                RabbitMqDeclaredQueues.DeclaredQueues.AddRange(new[] {delayQueue.Queue, poisonQueue.Queue});
            }

            if (_busClient.IsConnected)
            {
                try
                {
                    await (requeueConfig != null && delayQueue != null && poisonQueue != null
                            ? BindConsumerAsync(
                                mainQueueBinding,
                                delayQueue,
                                poisonQueue,
                                handler,
                                poisonHandler,
                                requeueConfig,
                                metricEntity)
                            : SubscribePrivateAsync(
                                mainQueueBinding,
                                handler,
                                metricEntity)
                        );
                }
                catch (Exception ex)
                {
                    _logger.Error(ex);
                    _logger.Info(
                        "В интервале между проверкой _busClient.IsConnected и SubscribeAsyncPrivate Rabbit может отвалиться, поэтому запускаем в бекграунд потоке"
                    );
                    var bindConsumerTask = requeueConfig != null && delayQueue != null && poisonQueue != null
                        ? BindConsumerAsync(
                            mainQueueBinding,
                            delayQueue,
                            poisonQueue,
                            handler,
                            poisonHandler,
                            requeueConfig,
                            metricEntity)
                        : SubscribePrivateAsync(
                            mainQueueBinding,
                            handler,
                            metricEntity);

                    bindConsumerTask.Forget();
                }
            }
            else
            {
                _logger.Info(" _busClient.IsConnected == false, Rabbit будет запущен в бэкграунд потоке");
                var bindConsumerTask = requeueConfig != null && delayQueue != null && poisonQueue != null
                    ? BindConsumerAsync(
                        mainQueueBinding,
                        delayQueue,
                        poisonQueue,
                        handler,
                        poisonHandler,
                        requeueConfig,
                        metricEntity)
                    : SubscribePrivateAsync(
                        mainQueueBinding,
                        handler,
                        metricEntity);
                bindConsumerTask.Forget();
            }
        }
#nullable disable

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

        private async Task<Acknowledgements> ExecuteWithPolicy(Func<Task<Acknowledgements>> action)
        {
            var policy = Policy.Handle<TimeoutException>()
                .WaitAndRetryAsync(
                    RetryAttemptMax,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (exception, timeSpan, retryCount, _) =>
                    {
                        _logger.ErrorWithObject(exception, new {TimeSpan = timeSpan, RetryCount = retryCount});
                    });

            var policyResult = await policy.ExecuteAndCaptureAsync(async () => await action.Invoke());

            if (policyResult.FinalException != null)
            {
                _logger.ErrorWithObject(policyResult.FinalException, action);
            }

            return policyResult.Result;
        }

        private async Task ResubscribeOnReconnect()
        {
            _logger.Warn("Reconnect happened, start resubscribing");
            foreach (var subscription in _subscriptions)
            {
                try
                {
                    await ResubscribeInternalAsync(subscription);
                }
                catch (Exception e)
                {
                    _logger.ErrorWithObject(e, "Failed to resubscribe", logObjects: subscription.Binding.Queue.Name);
                    _retryForeverPolicy.ExecuteAsync(() => ResubscribeInternalAsync(subscription)).Forget();
                }
            }

            async Task ResubscribeInternalAsync(SubscriptionInfo sub)
            {
                if (sub.Binding.Queue.IsExclusive)
                {
                    await SubscribePrivateAsync(sub.Binding,
                                                sub.EventbusSubscriptionHandler,
                                                sub.MetricsEntity);
                }
                else
                {
                    // for non exclusive queues we reuse existing consumer
                    // alternative is to dispose old consumer and create new consumer
                    await DeclareBindQueue(sub.Binding);
                }
            }
        }

        private async Task SubscribePrivateAsync(
            QueueExchangeBinding bindingInfo,
            Func<byte[], MessageProperties, MessageReceivedInfo, Task> handler,
            string metricEntity)
        {
            var queue = await DeclareBindQueue(bindingInfo);
            _busClient.Consume(queue, HandleEventBusMessageWithPolicy);

            async Task HandleEventBusMessageWithPolicy(ReadOnlyMemory<byte> body, MessageProperties props, MessageReceivedInfo info)
            {
                using (_metricsTracingFactory.CreateLoggingMetricsTimer(metricEntity ?? "Eventbus"))
                {
                    HandleMessageProps(props);
                    await ExecuteWithPolicy(async () => await handler.Invoke(body.ToArray(), props, info));
                }
            }
        }

        private async Task BindConsumerAsync(QueueExchangeBinding mainQueueBinding,
            QueueExchangeBinding delayQueueBinding,
            QueueExchangeBinding poisonQueueBinding,
            Func<byte[], MessageProperties, MessageReceivedInfo, Task<Acknowledgements>> handler,
            Func<byte[], MessageProperties, MessageReceivedInfo, Task<Acknowledgements>> poisonHandler,
            DelayedRequeueConfiguration delayedConfig,
            string metricEntity)
        {
            var mainQueue = await DeclareBindQueue(mainQueueBinding);
            _busClient.Consume(mainQueue, HandleEventBusMessageWithPolicyAndRequeue());

            if (poisonHandler != null)
            {
                var poisonQueue = await DeclareBindQueue(poisonQueueBinding);
                _busClient.Consume(poisonQueue, HandlePoisonQueueMessages());
            }

            Func<ReadOnlyMemory<byte>, MessageProperties, MessageReceivedInfo, Task<AckStrategy>>
                HandleEventBusMessageWithPolicyAndRequeue()
            {
                return async (body, props, info) =>
                {
                    using (_metricsTracingFactory.CreateLoggingMetricsTimer(metricEntity ?? "Eventbus"))
                    {
                        HandleMessageProps(props);
                        var handlerAcknowledgeResponse = await ExecuteWithPolicy(
                            async () => await handler.Invoke(body.ToArray(), props, info)
                        );

                        return handlerAcknowledgeResponse switch
                        {
                            Acknowledgements.Ack => AckStrategies.Ack,

                            Acknowledgements.Nack => await HandleNackResponse(
                                mainQueueBinding,
                                delayQueueBinding,
                                poisonQueueBinding,
                                delayedConfig,
                                props,
                                body.ToArray()),

                            Acknowledgements.Reject => AckStrategies.NackWithRequeue,

                            _ => AckStrategies.Ack
                        };
                    }
                };
            }

            Func<ReadOnlyMemory<byte>, MessageProperties, MessageReceivedInfo, Task> HandlePoisonQueueMessages()
            {
                return async (body, props, info) =>
                {
                    using (_metricsTracingFactory.CreateLoggingMetricsTimer($"{metricEntity ?? "Eventbus"}-Poison"))
                    {
                        HandleMessageProps(props);
                        await ExecuteWithPolicy(
                            async () => await poisonHandler.Invoke(body.ToArray(), props, info)
                        );
                    }
                };
            }
        }

        private async Task<Queue> DeclareBindQueue(QueueExchangeBinding bindingInfo)
        {
            var queue = await _busClient.QueueDeclareAsync(
                            name: bindingInfo.Queue.Name,
                            autoDelete: bindingInfo.Queue.IsAutoDelete,
                            durable: bindingInfo.Queue.IsDurable,
                            exclusive: bindingInfo.Queue.IsExclusive);

            var exchange = new Exchange(bindingInfo.Exchange.Name,
                                        bindingInfo.Exchange.Type,
                                        bindingInfo.Queue.IsDurable,
                                        bindingInfo.Queue.IsAutoDelete);

            await _busClient.BindAsync(exchange, bindingInfo.Queue, bindingInfo.RoutingKey);
            return queue;
        }

        private void HandleMessageProps(MessageProperties props)
        {
            if (!props.HeadersPresent)
                return;

            GetAcceptLanguageFromProperties(props);
        }

#nullable enable
        private async Task<AckStrategy> HandleNackResponse(
            QueueExchangeBinding mainQueueBinding,
            QueueExchangeBinding delayQueueBinding,
            QueueExchangeBinding poisonQueueBinding,
            DelayedRequeueConfiguration delayedConfig,
            MessageProperties props,
            byte[] body)
        {
            var counter = props.Headers.TryGetValue("x-counter", out object? xCounterHeader)
                          && int.TryParse(xCounterHeader.ToString(), out int headerCounter)
                ? headerCounter
                : 0;

            if (counter >= delayedConfig.MaxRetryRequeueCount)
            {
                await PublishToPoisonQueueAsync(poisonQueueBinding, body);
            }
            else
            {
                await PublishToDelayQueueAsync(
                    delayQueueBinding,
                    mainQueueBinding,
                    counter,
                    delayedConfig.DelayedQueueRequeueTtl,
                    body);
            }

            return AckStrategies.Ack;
        }
#nullable disable

        private async Task PublishToPoisonQueueAsync(QueueExchangeBinding poisonQueueBinding, byte[] messageBody)
        {
            try
            {
                await _busClient.QueueDeclareAsync(
                    poisonQueueBinding.Queue.Name,
                    c => c.AsAutoDelete(poisonQueueBinding.Queue.IsAutoDelete)
                        .AsDurable(poisonQueueBinding.Queue.IsDurable)
                        .AsExclusive(poisonQueueBinding.Queue.IsExclusive));
            }
            catch (Exception exception)
            {
                _logger.ErrorWithObject(
                    exception,
                    "Не удалось создать очередь задержки."
                );

                return;
            }

            var delayExchange = await _busClient.ExchangeDeclareAsync(
                poisonQueueBinding.Exchange.Name,
                poisonQueueBinding.Exchange.Type);

            await _busClient.BindAsync(delayExchange, poisonQueueBinding.Queue, poisonQueueBinding.RoutingKey);
            await SetupPolicy().ExecuteAndCaptureAsync(async () =>
                await _busClient.PublishAsync(
                    delayExchange,
                    poisonQueueBinding.RoutingKey,
                    false,
                    GetProperties(null, true),
                    messageBody)
            );
        }

        private async Task PublishToDelayQueueAsync(
        QueueExchangeBinding delayQueueBinding,
        QueueExchangeBinding mainQueue,
        int counter,
        int  delayedQueueRequeueTtl,
        byte[] messageBody)
    {
        try
        {
            await _busClient.QueueDeclareAsync(
                delayQueueBinding.Queue.Name,
                c => c.WithArgument("x-dead-letter-exchange", String.Empty)
                    .WithArgument("x-dead-letter-routing-key", mainQueue.Queue.Name)
                    .WithArgument("x-message-ttl", delayedQueueRequeueTtl)
                    .AsAutoDelete(delayQueueBinding.Queue.IsAutoDelete)
                    .AsDurable(delayQueueBinding.Queue.IsDurable)
                    .AsExclusive(delayQueueBinding.Queue.IsExclusive));
        }
        catch (Exception exception)
        {
            _logger.ErrorWithObject(
                exception,
                "Не удалось создать очередь задержки. Причина, скорее всего, в существующей очереди."
            );

            return;
        }

        var delayExchange = await _busClient.ExchangeDeclareAsync(
            delayQueueBinding.Exchange.Name,
            delayQueueBinding.Exchange.Type);

        _busClient.Bind(delayExchange, delayQueueBinding.Queue, delayQueueBinding.RoutingKey);
        await SetupPolicy().ExecuteAndCaptureAsync(async () =>
            await _busClient.PublishAsync(
                delayExchange,
                delayQueueBinding.RoutingKey,
                false,
                GetProperties(new Dictionary<string, object>
                    {
                        {"x-counter", ++counter}
                    },
                    true),
                messageBody)
        );
    }

    private void GetAcceptLanguageFromProperties(MessageProperties props)
        {
            try
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
            catch (Exception e)
            {
                _logger.ErrorWithObject(e, message: "Error while parsing accept_language from properties", props);
            }
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
                    _busClient.QueueDelete(queue.Name);
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
