using System;

namespace ATI.Services.RabbitMQ
{
    public class RmqConnectionConfig
    {
        /// <summary>
        /// AMQP format expected
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Timeout for PublishAsync
        /// </summary>
        public TimeSpan PublishMessageTimeout { get; set; }
    }
}