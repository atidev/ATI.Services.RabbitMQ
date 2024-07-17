using System.Collections.Generic;
using EasyNetQ.Topology;

namespace ATI.Services.RabbitMQ;

public static class RabbitMqDeclaredQueues
{
    public static List<Queue> DeclaredQueues { get; } = [];
}