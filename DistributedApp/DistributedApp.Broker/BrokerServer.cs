using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Broker;

public sealed class BrokerServer
{
    private static readonly JsonSerializerOptions StatusJsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly BrokerSettings _settings;
    private readonly PersistentBrokerStore _store;
    private readonly TcpListener _listener;
    private readonly ConcurrentDictionary<string, BrokerClientConnection>
        _subscribers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, InFlightDelivery>
        _inFlight = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim>
        _topicLocks = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _knownTopics =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTimeOffset>
        _pendingRetryAt = new(StringComparer.Ordinal);
    private readonly BrokerDispatchThread _dispatchThread;

    public BrokerServer(
        BrokerSettings settings,
        PersistentBrokerStore store)
    {
        _settings = settings;
        _store = store;
        _listener = new TcpListener(IPAddress.Any, settings.Port);
        _dispatchThread = new BrokerDispatchThread(
            TryDispatchAsync,
            _store.RedriveDeadLetterAsync);
    }

    public int BoundPort =>
        _listener.LocalEndpoint is IPEndPoint endpoint
            ? endpoint.Port
            : 0;

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _listener.Start();
        BrokerLog.Write(
            "Information",
            "broker_started",
            "listening",
            detail: $"port={BoundPort}");

        Task timeoutMonitor = MonitorTimeoutsAsync(cancellationToken);
        _dispatchThread.Start(cancellationToken);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient client;

                try
                {
                    client = await _listener.AcceptTcpClientAsync(
                        cancellationToken);
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                _ = HandleClientAsync(client, cancellationToken);
            }
        }
        finally
        {
            _listener.Stop();
            _dispatchThread.Stop();

            try
            {
                await timeoutMonitor;
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
            }

            BrokerLog.Write(
                "Information",
                "broker_stopped",
                "success");
        }
    }

    private async Task HandleClientAsync(
        TcpClient client,
        CancellationToken cancellationToken)
    {
        await using BrokerClientConnection connection = new(client);
        HashSet<string> subscriptions = new(StringComparer.Ordinal);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                string? line = await connection.Reader.ReadLineAsync(
                    cancellationToken);

                if (line == null)
                {
                    break;
                }

                Message message;

                try
                {
                    message = MessageJson.Deserialize(line);
                }
                catch (JsonException exception)
                {
                    BrokerLog.Write(
                        "Warning",
                        "message_rejected",
                        "invalid_json",
                        detail: exception.Message);
                    continue;
                }

                await ProcessMessageAsync(
                    connection,
                    subscriptions,
                    message,
                    cancellationToken);
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (IOException exception)
        {
            BrokerLog.Write(
                "Warning",
                "client_disconnected",
                "io_error",
                detail: exception.Message);
        }
        finally
        {
            foreach (string topic in subscriptions)
            {
                _subscribers.TryRemove(
                    new KeyValuePair<string, BrokerClientConnection>(
                        topic,
                        connection));

                if (!cancellationToken.IsCancellationRequested)
                {
                    await FailInFlightAsync(
                        topic,
                        "Consumer disconnected before acknowledgement.",
                        cancellationToken);
                }
            }
        }
    }

    private async Task ProcessMessageAsync(
        BrokerClientConnection connection,
        HashSet<string> subscriptions,
        Message message,
        CancellationToken cancellationToken)
    {
        MessageValidationResult validation = MessageValidator.Validate(message);

        if (!validation.IsValid)
        {
            BrokerLog.Write(
                "Warning",
                "message_rejected",
                "invalid_contract",
                message,
                string.Join(", ", validation.Errors));
            return;
        }

        switch (message.Type)
        {
            case MessageType.Publish:
                _knownTopics.TryAdd(message.Topic, 0);
                Message delivery = await _store.EnqueueAsync(
                    message,
                    cancellationToken);
                await connection.SendAsync(
                    CreateResponse(message, MessageType.Ack, null),
                    cancellationToken);
                BrokerLog.Write(
                    "Information",
                    "message_enqueued",
                    "accepted",
                    delivery);
                _dispatchThread.RequestDispatch(message.Topic);
                break;

            case MessageType.Subscribe:
                _knownTopics.TryAdd(message.Topic, 0);
                _pendingRetryAt.TryRemove(message.Topic, out _);
                _subscribers[message.Topic] = connection;
                subscriptions.Add(message.Topic);
                BrokerLog.Write(
                    "Information",
                    "consumer_subscribed",
                    "success",
                    message);
                _dispatchThread.RequestDispatch(message.Topic);
                break;

            case MessageType.Ack:
                await HandleAcknowledgementAsync(
                    message,
                    cancellationToken);
                break;

            case MessageType.Nack:
                await FailInFlightAsync(
                    message.Topic,
                    message.Reason ?? "Consumer returned Nack.",
                    cancellationToken,
                    message.RelatedMessageId);
                break;

            case MessageType.RedriveRequest:
                await HandleRedriveAsync(
                    connection,
                    message,
                    cancellationToken);
                break;
            case MessageType.StatusRequest:
                _knownTopics.TryAdd(message.Topic, 0);
                await SendStatusAsync(
                    connection,
                    message,
                    cancellationToken);
                break;

            default:
                BrokerLog.Write(
                    "Warning",
                    "message_rejected",
                    "unsupported_type",
                    message);
                break;
        }
    }

    private async Task HandleRedriveAsync(
        BrokerClientConnection connection,
        Message request,
        CancellationToken cancellationToken)
    {
        Guid messageId = request.RelatedMessageId!.Value;

        if (!_subscribers.ContainsKey(request.Topic))
        {
            await connection.SendAsync(
                CreateResponse(
                    request,
                    MessageType.Nack,
                    "Consumerul trebuie pornit înainte de redrive."),
                cancellationToken);
            BrokerLog.Write(
                "Warning",
                "dead_letter_redrive_rejected",
                "consumer_offline",
                request,
                detail: $"deadLetterMessageId={messageId}");
            return;
        }

        Message? redriven = await _dispatchThread.RedriveAsync(
            request.Topic,
            messageId,
            cancellationToken);

        bool succeeded = redriven != null;
        await connection.SendAsync(
            CreateResponse(
                request,
                succeeded ? MessageType.Ack : MessageType.Nack,
                succeeded
                    ? null
                    : "Mesajul nu a fost găsit în dead-letter queue."),
            cancellationToken);

        BrokerLog.Write(
            succeeded ? "Information" : "Warning",
            "dead_letter_redrive_requested",
            succeeded ? "requeued" : "not_found",
            request,
            detail: $"deadLetterMessageId={messageId}");
    }
    private async Task SendStatusAsync(
        BrokerClientConnection connection,
        Message request,
        CancellationToken cancellationToken)
    {
        BrokerStoreSnapshot storeSnapshot =
            await _store.GetSnapshotAsync(
                request.Topic,
                cancellationToken);
        int inFlightMessages = _inFlight.ContainsKey(request.Topic)
            ? 1
            : 0;
        BrokerStatusSnapshot status = new()
        {
            Topic = request.Topic,
            ConsumerConnected = _subscribers.ContainsKey(request.Topic),
            PendingMessages = Math.Max(
                0,
                storeSnapshot.QueuedMessages - inFlightMessages),
            InFlightMessages = inFlightMessages,
            DeadLetterMessages = storeSnapshot.DeadLetters.Count,
            AcknowledgedMessages = storeSnapshot.AcknowledgedMessages,
            RecentAcknowledgements = storeSnapshot.RecentAcknowledgements
                .OrderByDescending(entry => entry.AcknowledgedAtUtc)
                .Select(entry => new AcknowledgementSummary
                {
                    MessageId = entry.MessageId,
                    CorrelationId = entry.CorrelationId,
                    Topic = entry.Topic,
                    AcknowledgedAtUtc = entry.AcknowledgedAtUtc
                })
                .ToList(),
            DeadLetters = storeSnapshot.DeadLetters
                .OrderByDescending(entry => entry.DeadLetteredAtUtc)
                .Select(entry => new DeadLetterSummary
                {
                    MessageId = entry.Message.MessageId,
                    CorrelationId = entry.Message.CorrelationId,
                    Topic = entry.Message.Topic,
                    RetryCount = entry.Message.RetryCount,
                    Reason = entry.Reason,
                    DeadLetteredAtUtc = entry.DeadLetteredAtUtc
                })
                .ToList(),
            ObservedAtUtc = DateTimeOffset.UtcNow
        };
        string payload = JsonSerializer.Serialize(
            status,
            StatusJsonOptions);

        await connection.SendAsync(
            CreateResponse(
                request,
                MessageType.StatusResponse,
                null,
                payload),
            cancellationToken);
    }

    private async Task HandleAcknowledgementAsync(
        Message acknowledgement,
        CancellationToken cancellationToken)
    {
        string topic = acknowledgement.Topic;
        SemaphoreSlim topicLock = GetTopicLock(topic);
        await topicLock.WaitAsync(cancellationToken);

        try
        {
            if (!_inFlight.TryGetValue(
                    topic,
                    out InFlightDelivery? active)
                || acknowledgement.RelatedMessageId != active.MessageId)
            {
                BrokerLog.Write(
                    "Warning",
                    "ack_ignored",
                    "not_in_flight",
                    acknowledgement);
                return;
            }

            bool removed = await _store.AcknowledgeAsync(
                topic,
                active.MessageId,
                cancellationToken);
            _inFlight.TryRemove(topic, out _);

            BrokerLog.Write(
                removed ? "Information" : "Warning",
                "ack_received",
                removed ? "completed" : "not_found",
                acknowledgement);
        }
        finally
        {
            topicLock.Release();
        }

        _dispatchThread.RequestDispatch(topic);
    }

    private async Task FailInFlightAsync(
        string topic,
        string reason,
        CancellationToken cancellationToken,
        Guid? expectedMessageId = null)
    {
        SemaphoreSlim topicLock = GetTopicLock(topic);
        TimeSpan? retryDelay = null;
        await topicLock.WaitAsync(cancellationToken);

        try
        {
            if (!_inFlight.TryGetValue(topic, out InFlightDelivery? active)
                || (expectedMessageId.HasValue
                    && expectedMessageId.Value != active.MessageId))
            {
                return;
            }

            _inFlight.TryRemove(topic, out _);
            DeliveryFailureStatus status =
                await _store.RecordFailureAsync(
                    topic,
                    active.MessageId,
                    reason,
                    _settings.MaxRetries,
                    cancellationToken);

            BrokerLog.Write(
                status == DeliveryFailureStatus.DeadLettered
                    ? "Error"
                    : "Warning",
                status == DeliveryFailureStatus.DeadLettered
                    ? "message_dead_lettered"
                    : "message_retry_scheduled",
                status.ToString(),
                detail: $"messageId={active.MessageId}; reason={reason}");

            if (status == DeliveryFailureStatus.Retrying)
            {
                Message? retry = await _store.PeekAsync(
                    topic,
                    cancellationToken);

                if (retry != null)
                {
                    retryDelay = CalculateRetryDelay(retry.RetryCount);
                }
            }
        }
        finally
        {
            topicLock.Release();
        }

        // Waiting outside the topic lock allows acknowledgements and new
        // subscribers to be handled while this retry is backed off.
        if (retryDelay.HasValue)
        {
            await Task.Delay(retryDelay.Value, cancellationToken);
        }

        _dispatchThread.RequestDispatch(topic);
    }

    private async Task TryDispatchAsync(
        string topic,
        CancellationToken cancellationToken)
    {
        SemaphoreSlim topicLock = GetTopicLock(topic);
        await topicLock.WaitAsync(cancellationToken);

        try
        {
            if (_inFlight.ContainsKey(topic)
                || !_subscribers.TryGetValue(
                    topic,
                    out BrokerClientConnection? subscriber))
            {
                return;
            }

            Message? message = await _store.PeekAsync(
                topic,
                cancellationToken);

            if (message == null)
            {
                return;
            }

            _inFlight[topic] = new InFlightDelivery(
                message.MessageId,
                DateTimeOffset.UtcNow);

            try
            {
                await subscriber.SendAsync(message, cancellationToken);
                BrokerLog.Write(
                    "Information",
                    "message_dispatched",
                    "awaiting_ack",
                    message);
            }
            catch (IOException)
            {
                _inFlight.TryRemove(topic, out _);
                _subscribers.TryRemove(
                    new KeyValuePair<string, BrokerClientConnection>(
                        topic,
                        subscriber));
                await _store.RecordFailureAsync(
                    topic,
                    message.MessageId,
                    "Delivery connection failed.",
                    _settings.MaxRetries,
                    cancellationToken);
            }
        }
        finally
        {
            topicLock.Release();
        }
    }

    private async Task MonitorTimeoutsAsync(
        CancellationToken cancellationToken)
    {
        using PeriodicTimer timer = new(
            TimeSpan.FromMilliseconds(
                _settings.RetryScanIntervalMilliseconds));

        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            DateTimeOffset now = DateTimeOffset.UtcNow;

            foreach ((string topic, InFlightDelivery delivery) in _inFlight)
            {
                if ((now - delivery.SentAtUtc).TotalMilliseconds
                    >= _settings.AcknowledgementTimeoutMilliseconds)
                {
                    await FailInFlightAsync(
                        topic,
                        "Acknowledgement timeout.",
                        cancellationToken,
                        delivery.MessageId);
                }
            }

            await MonitorPendingWithoutConsumerAsync(
                now,
                cancellationToken);
        }
    }

    private async Task MonitorPendingWithoutConsumerAsync(
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        foreach (string topic in _knownTopics.Keys)
        {
            if (_subscribers.ContainsKey(topic)
                || _inFlight.ContainsKey(topic))
            {
                _pendingRetryAt.TryRemove(topic, out _);
                continue;
            }

            Message? pending = await _store.PeekAsync(
                topic,
                cancellationToken);

            if (pending == null)
            {
                _pendingRetryAt.TryRemove(topic, out _);
                continue;
            }

            DateTimeOffset retryAt = _pendingRetryAt.GetOrAdd(
                topic,
                now.AddMilliseconds(
                    _settings.AcknowledgementTimeoutMilliseconds));

            if (now >= retryAt)
            {
                await FailPendingWithoutConsumerAsync(
                    topic,
                    now,
                    cancellationToken);
            }
        }
    }

    private async Task FailPendingWithoutConsumerAsync(
        string topic,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        SemaphoreSlim topicLock = GetTopicLock(topic);
        await topicLock.WaitAsync(cancellationToken);

        try
        {
            if (_subscribers.ContainsKey(topic)
                || _inFlight.ContainsKey(topic))
            {
                _pendingRetryAt.TryRemove(topic, out _);
                return;
            }

            Message? pending = await _store.PeekAsync(
                topic,
                cancellationToken);

            if (pending == null)
            {
                _pendingRetryAt.TryRemove(topic, out _);
                return;
            }

            DeliveryFailureStatus status =
                await _store.RecordFailureAsync(
                    topic,
                    pending.MessageId,
                    "No consumer connected.",
                    _settings.MaxRetries,
                    cancellationToken);

            BrokerLog.Write(
                status == DeliveryFailureStatus.DeadLettered
                    ? "Error"
                    : "Warning",
                status == DeliveryFailureStatus.DeadLettered
                    ? "message_dead_lettered"
                    : "message_retry_scheduled",
                status.ToString(),
                pending,
                "No consumer connected.");

            Message? next = await _store.PeekAsync(
                topic,
                cancellationToken);

            if (next == null)
            {
                _pendingRetryAt.TryRemove(topic, out _);
            }
            else if (status == DeliveryFailureStatus.Retrying)
            {
                _pendingRetryAt[topic] =
                    now.Add(CalculateRetryDelay(next.RetryCount));
            }
            else
            {
                _pendingRetryAt[topic] = now.AddMilliseconds(
                    _settings.AcknowledgementTimeoutMilliseconds);
            }
        }
        finally
        {
            topicLock.Release();
        }
    }
    private SemaphoreSlim GetTopicLock(string topic)
    {
        return _topicLocks.GetOrAdd(topic, _ => new SemaphoreSlim(1, 1));
    }

    private TimeSpan CalculateRetryDelay(int retryCount)
    {
        int exponent = Math.Clamp(retryCount - 1, 0, 20);
        double exponentialDelay =
            _settings.BaseRetryDelayMilliseconds * Math.Pow(2, exponent);
        int cappedDelay = (int)Math.Min(
            exponentialDelay,
            _settings.MaxRetryDelayMilliseconds);
        int jitter = Random.Shared.Next(
            0,
            Math.Max(2, cappedDelay / 4));

        return TimeSpan.FromMilliseconds(cappedDelay + jitter);
    }

    private static Message CreateResponse(
        Message incoming,
        MessageType type,
        string? reason,
        string payload = "")
    {
        return new Message
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = incoming.CorrelationId,
            Type = type,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = incoming.Topic,
            SequenceNumber = incoming.SequenceNumber,
            RetryCount = 0,
            Payload = payload,
            RelatedMessageId = incoming.MessageId,
            Reason = reason
        };
    }
}
internal sealed class BrokerDispatchThread
{
    private readonly BlockingCollection<BrokerThreadWorkItem> _workItems =
        new();
    private readonly ConcurrentDictionary<string, byte> _scheduledTopics =
        new(StringComparer.Ordinal);
    private readonly Func<string, CancellationToken, Task> _dispatchAsync;
    private readonly Func<string, Guid, CancellationToken, Task<Message?>>
        _redriveAsync;
    private Thread? _thread;

    public BrokerDispatchThread(
        Func<string, CancellationToken, Task> dispatchAsync,
        Func<string, Guid, CancellationToken, Task<Message?>> redriveAsync)
    {
        _dispatchAsync = dispatchAsync
            ?? throw new ArgumentNullException(nameof(dispatchAsync));
        _redriveAsync = redriveAsync
            ?? throw new ArgumentNullException(nameof(redriveAsync));
    }

    public void Start(CancellationToken cancellationToken)
    {
        if (_thread != null)
        {
            throw new InvalidOperationException(
                "The Broker dispatch thread is already running.");
        }

        _thread = new Thread(() => Run(cancellationToken))
        {
            IsBackground = true,
            Name = "BrokerQueueDispatcher"
        };
        _thread.Start();
    }

    public void RequestDispatch(string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        if (_workItems.IsAddingCompleted
            || !_scheduledTopics.TryAdd(topic, 0))
        {
            return;
        }

        try
        {
            _workItems.Add(new BrokerThreadWorkItem(topic));
        }
        catch (InvalidOperationException)
        {
            _scheduledTopics.TryRemove(topic, out _);
        }
    }

    public Task<Message?> RedriveAsync(
        string topic,
        Guid messageId,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        if (messageId == Guid.Empty)
        {
            throw new ArgumentException(
                "Message ID cannot be empty.",
                nameof(messageId));
        }

        TaskCompletionSource<Message?> completion = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        try
        {
            _workItems.Add(
                new BrokerThreadWorkItem(
                    topic,
                    messageId,
                    completion),
                cancellationToken);
        }
        catch (InvalidOperationException)
        {
            completion.TrySetException(
                new InvalidOperationException(
                    "The Broker dispatch thread is stopping."));
        }

        return completion.Task.WaitAsync(cancellationToken);
    }

    public void Stop()
    {
        _workItems.CompleteAdding();

        if (_thread is { IsAlive: true }
            && Thread.CurrentThread != _thread)
        {
            _thread.Join();
        }
    }

    private void Run(CancellationToken cancellationToken)
    {
        BrokerLog.Write(
            "Information",
            "dispatcher_thread_started",
            "running",
            detail: $"threadId={Environment.CurrentManagedThreadId}");

        try
        {
            foreach (BrokerThreadWorkItem workItem in
                     _workItems.GetConsumingEnumerable(cancellationToken))
            {
                if (workItem.DeadLetterMessageId.HasValue)
                {
                    RedriveDeadLetter(workItem, cancellationToken);
                }
                else
                {
                    DispatchTopic(workItem.Topic, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            BrokerLog.Write(
                "Information",
                "dispatcher_thread_stopped",
                "completed",
                detail: $"threadId={Environment.CurrentManagedThreadId}");
        }
    }

    private void DispatchTopic(
        string topic,
        CancellationToken cancellationToken)
    {
        _scheduledTopics.TryRemove(topic, out _);

        try
        {
            _dispatchAsync(topic, cancellationToken)
                .GetAwaiter()
                .GetResult();
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            BrokerLog.Write(
                "Error",
                "dispatcher_thread_failed",
                "dispatch_error",
                detail: $"topic={topic}; {exception.Message}");
        }
    }

    private void RedriveDeadLetter(
        BrokerThreadWorkItem workItem,
        CancellationToken cancellationToken)
    {
        try
        {
            Message? redriven = _redriveAsync(
                    workItem.Topic,
                    workItem.DeadLetterMessageId!.Value,
                    cancellationToken)
                .GetAwaiter()
                .GetResult();

            if (redriven != null)
            {
                _dispatchAsync(workItem.Topic, cancellationToken)
                    .GetAwaiter()
                    .GetResult();
            }

            workItem.Completion!.TrySetResult(redriven);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            workItem.Completion!.TrySetCanceled(cancellationToken);
        }
        catch (Exception exception)
        {
            workItem.Completion!.TrySetException(exception);
            BrokerLog.Write(
                "Error",
                "dead_letter_redrive_failed",
                "thread_error",
                detail:
                    $"messageId={workItem.DeadLetterMessageId}; " +
                    exception.Message);
        }
    }

    private sealed record BrokerThreadWorkItem(
        string Topic,
        Guid? DeadLetterMessageId = null,
        TaskCompletionSource<Message?>? Completion = null);
}