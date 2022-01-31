﻿using ATI.Services.Serialization;
using NLog;

namespace ATI.Services.RabbitMQ.Producers
{
    internal sealed class RmqProducer : BaseRmqProducer
    {
        public RmqProducer(
            ILogger logger,
            ExchangeType exchangeType,
            ISerializer serializer,
            string exchangeName,
            string defaultRoutingKey,
            bool durableExchange) : base(logger)
        {
            ExchangeType = exchangeType;
            Serializer = serializer;
            ExchangeName = exchangeName;
            DefaultRoutingKey = defaultRoutingKey;
            DurableExchange = durableExchange;
        }

        protected override ExchangeType ExchangeType { get; }
        protected override ISerializer Serializer { get; }
        protected override string ExchangeName { get; }
        protected override string DefaultRoutingKey { get; }
        protected override bool DurableExchange { get; }
    }
}