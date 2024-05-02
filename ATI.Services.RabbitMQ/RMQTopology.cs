using System;
using System.Linq;
using System.Net;
using ATI.Services.Common.Behaviors;
using ATI.Services.Common.Extensions;
using EasyNetQ.Topology;
using JetBrains.Annotations;
using Microsoft.Extensions.Options;

namespace ATI.Services.RabbitMQ;

[PublicAPI]
public class RmqTopology
{
    private readonly EventbusOptions _eventbusOptions;

    private const string SubscriptionType = "eventbus";

    public RmqTopology(IOptions<EventbusOptions> options)
    {
        _eventbusOptions = options.Value;
    }

    /// <summary>
    /// </summary>
    /// <param name="exchangeName"></param>
    /// <param name="routingKey"></param>
    /// <param name="isExclusiveQueueName">Если true, то к имени очереди добавится постфикс с именем машины+порт</param>
    /// <param name="isExclusive"></param>
    /// <param name="customQueueName"></param>
    /// <param name="entityName">Будет в названии очереди вместо exchangeName</param>
    /// <returns></returns>
    public QueueExchangeBinding CreateBinding(
        string exchangeName,
        string routingKey,
        bool isExclusive,
        bool isDurable,
        bool isAutoDelete,
        bool isExclusiveQueueName = false,
        string customQueueName = null,
        string entityName = null)
    {
        var queueName =
            EventbusQueueNameTemplate(exchangeName, routingKey, customQueueName, isExclusiveQueueName,
                entityName: entityName);

        var createdQueue = new Queue(queueName, isDurable, isExclusive, isAutoDelete);

        var subscribeExchange = new ExchangeInfo
        {
            Name = exchangeName,
            Type = ExchangeType.Topic
        };
        return new QueueExchangeBinding(subscribeExchange, createdQueue, routingKey);
    }

    private readonly string _queuePostfixName = $"-{Dns.GetHostName()}-{ConfigurationManager.GetApplicationPort()}";

    private string EventbusQueueNameTemplate(
        string rabbitService, 
        string routingKey,
        string customQueueName, 
        bool isExclusiveQueueName,
        string entityName = null)
    {
        //отделяем env от exchangeName
        var exchangeNameWithoutEnv = rabbitService[(rabbitService.IndexOf('.') + 1)..];
        if (string.IsNullOrEmpty(exchangeNameWithoutEnv))
            exchangeNameWithoutEnv = rabbitService;
        
        var queueName = $"{_eventbusOptions.Environment}.{SubscriptionType}." +
                        (!customQueueName.IsNullOrEmpty()
                            ? customQueueName
                            : $"{_eventbusOptions.ServiceName}." + $"{entityName ?? exchangeNameWithoutEnv}." + $"{routingKey}");


        if (_eventbusOptions.AddHostnamePostfixToQueues || isExclusiveQueueName)
            queueName += _queuePostfixName;

        return queueName;
    }
}