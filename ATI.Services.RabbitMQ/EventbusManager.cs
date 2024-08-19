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

namespace ATI.Services.RabbitMQ;

[PublicAPI]
[InitializeOrder(Order = InitializeOrder.First)]
public class EventbusManager : IDisposable, IInitializer
{
    private IAdvancedBus _busClient = null!;
    private const int RetryAttemptMax = 3;
    private const int MaxRetryDelayPow = 2;
    private const string DelayQueueSuffix = "_delay";
    private const string PoisonQueueSuffix = "_poison";
    private readonly JsonSerializer _jsonSerializer;
    private readonly string _connectionString;

    private readonly MetricsInstance _inMetrics;
    private readonly MetricsInstance _outMetrics;

    private readonly ILogger _logger = LogManager.GetCurrentClassLogger();
    private readonly ConcurrentBag<SubscriptionInfo> _subscriptions = [];
    private readonly AsyncRetryPolicy _retryForeverPolicy;
    private readonly AsyncRetryPolicy _subscribePolicy;
    private readonly EventbusOptions _options;
    private readonly RmqTopology _rmqTopology;

    public EventbusManager(
        JsonSerializer jsonSerializer, 
        IOptions<EventbusOptions> options, 
        RmqTopology rmqTopology,
        MetricsFactory metricsFactory)
    {
        _options = options.Value;
        _connectionString = options.Value.ConnectionString;
        _jsonSerializer = jsonSerializer;
        _rmqTopology = rmqTopology;
        
        _inMetrics = metricsFactory.CreateRabbitMqMetricsFactory(RabbitMetricsType.Subscribe, nameof(EventbusManager), additionalSummaryLabels: "rmq_app_id");
        _outMetrics = metricsFactory.CreateRabbitMqMetricsFactory(RabbitMetricsType.Publish, nameof(EventbusManager));

        _subscribePolicy = Policy.Handle<Exception>()
                                 .WaitAndRetryForeverAsync(_ => _options.RabbitConnectInterval,
                                                           (exception, _) => _logger.Error(exception));

        _retryForeverPolicy =
            Policy.Handle<Exception>()
                  .WaitAndRetryForeverAsync(
                      retryAttempt => TimeSpan.FromSeconds(1 << Math.Min(retryAttempt, MaxRetryDelayPow)),
                      (exception, _) => _logger.Error(exception));
    }

    public Task InitializeAsync()
    {
        try
        {
            _busClient = RabbitHutch.CreateBus(_connectionString,
                                               serviceRegister =>
                                               {
                                                   serviceRegister.Register<IConventions>(c =>
                                                       new RabbitMqConventions(c.Resolve<ITypeNameSerializer>(), _options)
                                                   );

                                                   if (_options.EnableConsoleLogger)
                                                       serviceRegister.EnableConsoleLogger();
                                               }).Advanced;

            _busClient.Connected += (_, _) => ResubscribeOnReconnect();
            _busClient.Disconnected += (_, b) => _logger.ErrorWithObject(
                "Disconnected from RMQ for some reason!", b.Hostname, b.Port, b.Reason, b.Type.ToString("G"));
        }
        catch (Exception exception)
        {
            _logger.Error(exception);
        }

        return Task.CompletedTask;
    }

    public Task<Exchange> DeclareExchangeTopicAsync(string exchangeName, bool durable, bool autoDelete) 
        => _busClient.ExchangeDeclareAsync(exchangeName, ExchangeType.Topic, durable, autoDelete);
    
    public Task<Exchange> DeclareExchangeTypedAsync(string exchangeName,bool durable, bool autoDelete, 
        string type = ExchangeType.Topic) => _busClient.ExchangeDeclareAsync(exchangeName, type, durable, autoDelete);

    public async Task PublishRawAsync(
        string publishBody,
        string exchangeName,
        string routingKey,
        string metricEntity,
        Dictionary<string, object>? additionalHeaders = null,
        bool mandatory = false,
        TimeSpan? timeout = null,
        bool withAcceptLang = true)
    {
        if (string.IsNullOrWhiteSpace(exchangeName)
            || string.IsNullOrWhiteSpace(routingKey)
            || string.IsNullOrWhiteSpace(publishBody))
            return;

        using var timer = _outMetrics.CreateLoggingMetricsTimer(metricEntity, $"{exchangeName}:{routingKey}");

        var messageProperties = GetProperties(additionalHeaders, withAcceptLang);
        var exchange = new Exchange(exchangeName);
        var body = Encoding.UTF8.GetBytes(publishBody);

        var sendingResult = await SetupPolicy(timeout)
            .ExecuteAndCaptureAsync(async () =>
                await _busClient.PublishAsync(
                    exchange,
                    routingKey,
                    mandatory,
                    messageProperties,
                    body)
            );

        if (sendingResult.FinalException != null)
        {
            _logger.ErrorWithObject(sendingResult.FinalException,
                new { publishBody, exchangeName, routingKey, metricEntity, mandatory });
        }
    }

    public async Task PublishAsync<T>(
        T publishObject,
        string exchangeName,
        string routingKey,
        string metricEntity,
        Dictionary<string, object>? additionalHeaders = null,
        bool mandatory = false,
        JsonSerializer? serializer = null,
        TimeSpan? timeout = null,
        bool withAcceptLang = true)
    {
        if (string.IsNullOrWhiteSpace(exchangeName)
            || string.IsNullOrWhiteSpace(routingKey)
            || publishObject == null)
            return;

        using var timer = _outMetrics.CreateLoggingMetricsTimer(metricEntity, $"{exchangeName}:{routingKey}");

        var messageProperties = GetProperties(additionalHeaders, withAcceptLang);
        var exchange = new Exchange(exchangeName);
        var bodySerializer = serializer ?? _jsonSerializer;
        var body = bodySerializer.ToJsonBytes(publishObject);

        var sendingResult = await SetupPolicy(timeout)
            .ExecuteAndCaptureAsync(async () =>
                await _busClient.PublishAsync(
                    exchange,
                    routingKey,
                    mandatory,
                    messageProperties,
                    body
                )
            );

        if (sendingResult.FinalException != null)
        {
            _logger.ErrorWithObject(sendingResult.FinalException,
                new { publishObject, exchangeName, routingKey, metricEntity, mandatory });
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
        string? customQueueName = null,
        string? metricEntity = null)
    {
        var binding = _rmqTopology.CreateBinding(exchangeName, routingKey, isExclusive, isDurable, isAutoDelete,
                                                 isExclusiveQueueName, customQueueName);

        return SubscribeAsync(binding, handler, metricEntity);
    }

    public Task SubscribeAsync(QueueExchangeBinding bindingInfo,
                               Func<byte[], MessageProperties, MessageReceivedInfo, Task> handler,
                               string? metricEntity = null)
    {
        return SubscribeAsync(bindingInfo,
                              async (body, props, info) =>
                              {
                                  await handler(body, props, info);
                                  return AckStrategies.Ack;
                              },
                              metricEntity);
    }

    public Task SubscribeAsync(QueueExchangeBinding bindingInfo,
                               Func<byte[], MessageProperties, MessageReceivedInfo, Task<AckStrategy>> handler,
                               string? metricEntity = null)
    {
        RabbitMqDeclaredQueues.DeclaredQueues.Add(bindingInfo.Queue);

        //wait for 1 sec to return else subscribe in background
        return Task.WhenAny(
            Task.Delay(TimeSpan.FromSeconds(1)),
            _subscribePolicy.ExecuteAsync(async () =>
            {
                var consumer = await SubscribePrivateAsync(bindingInfo, handler, metricEntity);
                _subscriptions.Add(new SubscriptionInfo
                {
                    Binding = bindingInfo,
                    Consumer = consumer,
                    EventbusSubscriptionHandler = handler,
                    MetricsEntity = metricEntity
                });
            }));
    }

    private AsyncPolicyWrap SetupPolicy(TimeSpan? timeout = null) =>
        Policy.WrapAsync(
            Policy.TimeoutAsync(timeout ?? TimeSpan.FromSeconds(3)),
            Policy.Handle<Exception>()
                  .WaitAndRetryAsync(
                     3, 
                     _ => TimeSpan.FromSeconds(1),
                     (exception, timeSpan, retryCount, _) =>
                     {
                         if(_options.LogInnerExceptionsInRetryPolicy)
                            _logger.ErrorWithObject(exception, new { TimeSpan = timeSpan, RetryCount = retryCount });
                     }
                   )
        );

    private async Task<Acknowledgements> ExecuteWithPolicy(Func<Task<Acknowledgements>> action)
    {
        var policy = Policy.Handle<TimeoutException>()
                           .WaitAndRetryAsync(
                                RetryAttemptMax,
                                retryAttempt => TimeSpan.FromSeconds(1 << retryAttempt),
                                (exception, timeSpan, retryCount, _) =>
                                {
                                    _logger.ErrorWithObject(exception, new {TimeSpan = timeSpan, RetryCount = retryCount});
                                }
                            );

        var policyResult = await policy.ExecuteAndCaptureAsync(async () => await action.Invoke());

        if (policyResult.FinalException != null)
        {
            _logger.ErrorWithObject(policyResult.FinalException, action);
        }

        return policyResult.Result;
    }

    private void ResubscribeOnReconnect()
    {
        _logger.Warn("Connected to rmq");

        foreach (var subscription in _subscriptions)
        {
            _logger.WarnWithObject("Acquire resubscribing lock", subscription.Binding.Exchange, subscription.Binding.RoutingKey);

            lock (subscription.ResubscribeLock)
            {
                if (subscription.ResubscribeTask is { IsCompleted: false })
                    continue;

                _logger.WarnWithObject("Acquired lock, start resubscribing", subscription.Binding.Exchange, subscription.Binding.RoutingKey);

                subscription.ResubscribeTask = _retryForeverPolicy.ExecuteAsync(
                    async () =>
                    {
                        var newConsumer = await SubscribePrivateAsync(subscription.Binding,
                                                                      subscription.EventbusSubscriptionHandler,
                                                                      subscription.MetricsEntity);
                        subscription.Consumer.Dispose();
                        subscription.Consumer = newConsumer;
                    }
                );
            }
        }
    }

    private async Task<IDisposable> SubscribePrivateAsync(
        QueueExchangeBinding bindingInfo,
        Func<byte[], MessageProperties, MessageReceivedInfo, Task<AckStrategy>>? handler,
        string? metricEntity)
    {
        var queue = await DeclareBindQueue(bindingInfo);
        var consumer = _busClient.Consume(queue,
                                          HandleEventBusMessageWithPolicyAckStrategy,
                                          bindingInfo.ConsumerConfiguration ?? SkipConsumerConfiguration);
        return consumer;

        async Task<AckStrategy> HandleEventBusMessageWithPolicyAckStrategy(ReadOnlyMemory<byte> body,
                                                                           MessageProperties props,
                                                                           MessageReceivedInfo info)
        {
            using var timer = _inMetrics.CreateLoggingMetricsTimer(metricEntity ?? "Eventbus",
                       $"{info.Exchange}:{info.RoutingKey}",
                       additionalLabels: props.AppId ?? "Unknown");

            HandleMessageProps(props);

            try
            {
                return await handler!(body.ToArray(), props, info);
            }
            catch (Exception e)
            {
                _logger.ErrorWithObject(e, "Error while handling eventbus message", new {info.Exchange, info.RoutingKey});
                throw;
            }
        }

        static void SkipConsumerConfiguration(ISimpleConsumeConfiguration _) { }
    }

    public async Task BindConsumerAsync(
        QueueExchangeBinding mainQueueBinding,
        QueueExchangeBinding delayQueueBinding,
        QueueExchangeBinding poisonQueueBinding,
        Func<byte[], MessageProperties, MessageReceivedInfo, Task<Acknowledgements>> handler,
        Func<byte[], MessageProperties, MessageReceivedInfo, Task<Acknowledgements>>? poisonHandler,
        DelayedRequeueConfiguration delayedConfig,
        string? metricEntity)
    {
        var mainQueue = await DeclareBindQueue(mainQueueBinding);
        _busClient.Consume(mainQueue, HandleEventBusMessageWithPolicyAndRequeue());

        if (poisonHandler != null)
        {
            var poisonQueue = await DeclareBindQueue(poisonQueueBinding);
            _busClient.Consume(poisonQueue, HandlePoisonQueueMessages());
        }

        return;

        Func<ReadOnlyMemory<byte>, MessageProperties, MessageReceivedInfo, Task<AckStrategy>>
            HandleEventBusMessageWithPolicyAndRequeue()
        {
            return async (body, props, info) =>
            {
                using (_inMetrics.CreateLoggingMetricsTimer(metricEntity ?? "Eventbus",
                           $"{info.Exchange}:{info.RoutingKey}",
                           additionalLabels: props.AppId ?? "Unknown"))
                {
                    HandleMessageProps(props);
                    var handlerAcknowledgeResponse = await ExecuteWithPolicy(
                        async () => await handler.Invoke(body.ToArray(), props, info)
                    );

                    return handlerAcknowledgeResponse switch
                    {
                        Acknowledgements.Nack => await HandleNackResponse(
                            mainQueueBinding,
                            delayQueueBinding,
                            poisonQueueBinding,
                            delayedConfig,
                            props,
                            body.ToArray()
                        ),

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
                using (_outMetrics.CreateLoggingMetricsTimer($"{metricEntity ?? "Eventbus"}-Poison", $"{info.Exchange}:{info.RoutingKey}",
                           additionalLabels: props.AppId ?? "Unknown"))
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
        var queue = await _busClient.QueueDeclareAsync(bindingInfo.Queue.Name,
                                                       c =>
                                                       {
                                                           bindingInfo.QueueConfiguration?.Invoke(c);
                                                           c.AsAutoDelete(bindingInfo.Queue.IsAutoDelete)
                                                            .AsDurable(bindingInfo.Queue.IsDurable)
                                                            .AsExclusive(bindingInfo.Queue.IsExclusive)
                                                            .WithQueueType(bindingInfo.QueueType);
                                                       });

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

    private async Task<AckStrategy> HandleNackResponse(
        QueueExchangeBinding mainQueueBinding,
        QueueExchangeBinding delayQueueBinding,
        QueueExchangeBinding poisonQueueBinding,
        DelayedRequeueConfiguration delayedConfig,
        MessageProperties props,
        byte[] body)
    {
        var counter = props.Headers.TryGetValue("x-counter", out var xCounterHeader)
                      && int.TryParse(xCounterHeader?.ToString(), out var headerCounter)
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

    private async Task PublishToPoisonQueueAsync(QueueExchangeBinding poisonQueueBinding, byte[] messageBody)
    {
        try
        {
            await _busClient.QueueDeclareAsync(
                poisonQueueBinding.Queue.Name,
                c => c.AsAutoDelete(poisonQueueBinding.Queue.IsAutoDelete)
                      .AsDurable(poisonQueueBinding.Queue.IsDurable)
                      .AsExclusive(poisonQueueBinding.Queue.IsExclusive)
                      .WithQueueType(poisonQueueBinding.QueueType));
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
        int delayedQueueRequeueTtl,
        byte[] messageBody)
    {
        try
        {
            await _busClient.QueueDeclareAsync(
                delayQueueBinding.Queue.Name,
                c => c.WithArgument("x-dead-letter-exchange", string.Empty)
                      .WithArgument("x-dead-letter-routing-key", mainQueue.Queue.Name)
                      .WithArgument("x-message-ttl", delayedQueueRequeueTtl)
                      .AsAutoDelete(delayQueueBinding.Queue.IsAutoDelete)
                      .AsDurable(delayQueueBinding.Queue.IsDurable)
                      .AsExclusive(delayQueueBinding.Queue.IsExclusive)
                      .WithQueueType(delayQueueBinding.QueueType));
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

        await _busClient.BindAsync(delayExchange, delayQueueBinding.Queue, delayQueueBinding.RoutingKey);
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

            var acceptLanguageStr = Encoding.UTF8.GetString((byte[]) acceptLanguage);
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

    private static MessageProperties GetProperties(Dictionary<string, object>? additionalHeaders, bool withAcceptLang)
    {
        var messageProperties = new MessageProperties
        {
            AppId = ServiceVariables.ServiceAsClientName
        };

        SetTraceHeadersFromActivity(messageProperties);
        if (withAcceptLang)
            SetAcceptLanguageHeader(messageProperties);

        if (additionalHeaders == null) 
            return messageProperties;

        foreach (var additionalHeader in additionalHeaders)
        {
            messageProperties.Headers.TryAdd(additionalHeader.Key, additionalHeader.Value);
        }

        return messageProperties;
    }

    private static void SetTraceHeadersFromActivity(MessageProperties properties)
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

    private static void SetAcceptLanguageHeader(MessageProperties properties)
    {
        var flowAcceptLang = FlowContext<RequestMetaData>.Current.AcceptLanguage;
        if (flowAcceptLang != null)
            properties.Headers.Add(MessagePropertiesNames.AcceptLang, flowAcceptLang);
    }

    public void Dispose()
    {
        // Сделано для удобства локального тестирования, удаляем наши созданные очереди
        if (_options.DeleteQueuesOnApplicationShutdown)
        {
            foreach (var queue in RabbitMqDeclaredQueues.DeclaredQueues)
            {
                _busClient?.QueueDelete(queue.Name);
            }
        }
        
        foreach (var subscription in _subscriptions) 
            subscription.Consumer.Dispose();

        _busClient?.Dispose();
    }

    public string InitStartConsoleMessage() => "Start Eventbus initializer";
    public string InitEndConsoleMessage() => "End Eventbus initializer";
}