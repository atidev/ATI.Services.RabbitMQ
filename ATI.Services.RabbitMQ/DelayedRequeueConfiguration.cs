namespace ATI.Services.RabbitMQ;

public record DelayedRequeueConfiguration(int MaxRetryRequeueCount, int DelayedQueueRequeueTtl);