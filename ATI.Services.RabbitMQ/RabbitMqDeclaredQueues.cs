using System.Collections.Generic;

namespace ATI.Services.RabbitMQ
{
    public static class RabbitMqDeclaredQueues
    {
        public static List<QueueInfo> DeclaredQueues { get; } = new();
    }
}