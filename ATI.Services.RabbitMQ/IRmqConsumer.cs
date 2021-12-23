using System;
using RabbitMQ.Client;

namespace ATI.Services.RabbitMQ
{
    public interface IRmqConsumer : IDisposable
    {
        void Init(IConnection connection);
    }
}