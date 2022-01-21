using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using ATI.Services.Common.Extensions;
using ATI.Services.Common.Logging;
using ATI.Services.Common.ServiceVariables;
using ATI.Services.Serialization;
using JetBrains.Annotations;
using NLog;
using RabbitMQ.Client;

namespace ATI.Services.RabbitMQ.Producers
{
    [PublicAPI]
    public abstract class BaseRmqProducer : BaseRmqProvider, IRmqProducer
    {
        private readonly
            BlockingCollection<(byte[] body, string routingKey, TimeSpan expiration, TaskCompletionSource<bool>
                taskCompletionSource)> _messageQueue;

        private IModel _channel;
        private TimeSpan _defaultTimeout;
        private readonly ILogger _logger;
        protected abstract string DefaultRoutingKey { get; }
        private bool _initialized;
        private ISerializer Serializer { get; }
        private readonly object _lock = new object();


        protected BaseRmqProducer(ILogger logger)
        {
            _logger = logger;
            _messageQueue =
                new BlockingCollection<(byte[] body, string routingKey, TimeSpan expiration, TaskCompletionSource<bool>
                    taskCompletionSource)>();
        }

        public void Init(IConnection connection, TimeSpan timeout)
        {
            if (_initialized)
                return;

            _defaultTimeout = timeout;
            _channel = connection.CreateModel();
            _channel.ExchangeDeclare(ExchangeName, GetExchangeType(), DurableExchange);
            _channel.ConfirmSelect();
            _channel.BasicNacks += (_, _) =>
                _logger.Error(
                    $"Ошибка при подтверждении отправки сообщения в exchange {ExchangeName} паблишера {GetType().FullName}.");

            new Thread(ProcessAllMessages) { IsBackground = true }.Start();
            _initialized = true;
        }

        public void PublishAndForget<T>(T model, string routingKey = null, TimeSpan expiration = default)
        {
            AddMessageToQueue(model, routingKey, expiration, CancellationToken.None);
        }

        public void PublishBytesAndForget(byte[] body, string routingKey = null, TimeSpan expiration = default)
        {
            AddMessageToQueue(body, routingKey, expiration, CancellationToken.None);
        }

        public Task<bool> PublishAsync<T>(T model, string routingKey = null, TimeSpan timeout = default,
            TimeSpan expiration = default)
        {
            return PublishAsync(model, CancellationToken.None, routingKey, timeout, expiration);
        }

        public Task<bool> PublishAsync<T>(T model, CancellationToken token, string routingKey = null,
            TimeSpan timeout = default, TimeSpan expiration = default)
        {
            var body = Serializer.Serialize(model);
            return PublishBytesAsync(body, token, routingKey, timeout, expiration);
        }

        public Task<bool> PublishBytesAsync(byte[] body, string routingKey = null, TimeSpan timeout = default,
            TimeSpan expiration = default)
        {
            return PublishBytesAsync(body, CancellationToken.None, routingKey, timeout, expiration);
        }

        public async Task<bool> PublishBytesAsync(byte[] body, CancellationToken token, string routingKey = null,
            TimeSpan timeout = default, TimeSpan expiration = default)
        {
            using var cts = new CancellationTokenSource();
            var taskCompletionSource =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var cancellationToken = token.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(cts.Token, token).Token
                : cts.Token;

            if (timeout == default)
            {
                timeout = _defaultTimeout;
            }

            var timeoutTask = Task.Delay(timeout, cancellationToken);

            AddMessageToQueue(body, routingKey, expiration, cancellationToken, taskCompletionSource);

            var resultTask = await Task.WhenAny(taskCompletionSource.Task, timeoutTask).ConfigureAwait(false);
            if (resultTask == timeoutTask)
            {
                throw new OperationCanceledException($"Publish message to exchange: {ExchangeName}");
            }

            // Cancel the timer task so that it does not fire
            cts.Cancel();

            return taskCompletionSource.Task.Result;
        }

        private void AddMessageToQueue<T>(T model, string routingKey, TimeSpan expiration,
            CancellationToken cancellationToken, TaskCompletionSource<bool> taskCompletionSource = null)
        {
            var body = Serializer.Serialize(model);
            AddBytesToQueue(body, routingKey, expiration, cancellationToken, taskCompletionSource);
        }

        private void AddBytesToQueue(byte[] body, string routingKey, TimeSpan expiration,
            CancellationToken cancellationToken, TaskCompletionSource<bool> taskCompletionSource = null)
        {
            routingKey ??= DefaultRoutingKey;
            _messageQueue.Add((body, routingKey, expiration, taskCompletionSource), cancellationToken);
        }

        private void ProcessAllMessages()
        {
            foreach (var (body, routingKey, expiration, taskCompletionSource) in _messageQueue.GetConsumingEnumerable())
            {
                try
                {
                    var properties = _channel.CreateBasicProperties();
                    if (expiration != default)
                    {
                        properties.Expiration = ((int)expiration.TotalMilliseconds).ToString();
                    }

                    if (!ServiceVariables.ServiceAsClientName.IsNullOrEmpty())
                    {
                        properties.AppId = ServiceVariables.ServiceAsClientName;
                    }

                    // BasicPublish не потокобезопасен, но lock не нужен,
                    // потому что все сообщения отправляются в одном потоке
                    _channel.BasicPublish(
                        ExchangeName,
                        routingKey,
                        properties,
                        body);

                    taskCompletionSource?.TrySetResult(true);
                }
                catch (Exception e)
                {
                    _logger.ErrorWithObject(e,
                        $"Ошибка при отправке сообщения в exchange {ExchangeName} паблишера {GetType()}",
                        body,
                        routingKey,
                        expiration,
                        taskCompletionSource);

                    // Если упала ошибка - надо прокинуть её наверх
                    taskCompletionSource?.SetException(e);
                }
            }
        }

        public void Dispose()
        {
            _channel?.Dispose();
        }
    }
}