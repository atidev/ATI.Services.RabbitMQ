using System;

namespace ATI.Services.RabbitMQ;

public class EventbusOptions 
{
    public required string ServiceName { get; init; }
    public required string ConnectionString { get; init; }
    public required string Environment { get; init; }
    public required TimeSpan RabbitConnectInterval { get; set; } = TimeSpan.FromSeconds(5);
    public required bool Enabled { get; init; }
    public string? ErrorQueueName { get; init; }

    #region ForLocalTesting

    public bool AddHostnamePostfixToQueues { get; init; }
    public bool DeleteQueuesOnApplicationShutdown { get; init; }

    #endregion
}