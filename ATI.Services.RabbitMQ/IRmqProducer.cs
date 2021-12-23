using System;
using RabbitMQ.Client;

namespace ATI.Services.RabbitMQ
{
    public interface IRmqProducer: IDisposable
    {
        void Init(IConnection connection, TimeSpan timeout);
    }
}