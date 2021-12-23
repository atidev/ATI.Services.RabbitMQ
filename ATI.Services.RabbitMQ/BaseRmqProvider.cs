using ATI.Services.Serialization;

namespace ATI.Services.RabbitMQ
{
    public abstract class BaseRmqProvider
    {
        protected abstract ExchangeType ExchangeType { get; }
        protected abstract ISerializer Serializer { get; }
        protected abstract string ExchangeName { get; }
        protected virtual bool DurableExchange => true;

        protected string GetExchangeType()
        {
            switch (ExchangeType)
            {
                case ExchangeType.Topic:
                    return "topic";
                case ExchangeType.Direct:
                    return "direct";
                case ExchangeType.Fanout:
                    return "fanout";
                default:
                    return "topic";
            }
        }

    }
}