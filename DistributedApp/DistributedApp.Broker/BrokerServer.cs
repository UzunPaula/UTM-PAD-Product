using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using DistributedApp.Contracts;

namespace DistributedApp.Broker;

public sealed class BrokerServer
{
    private const int MaxRetries = 3;
    private static readonly TimeSpan AckTimeout = TimeSpan.FromSeconds(2);
    private readonly object _sync = new();
    private readonly TcpListener _listener;
    private readonly string _stateFile = Path.Combine(AppContext.BaseDirectory, "broker-state.json");
    private readonly Dictionary<string, ConsumerConnection> _onlineConsumers = [];
    private readonly ConcurrentQueue<RedriveRequest> _redriveRequests = new();
    private BrokerState _state;
    private bool _running;
    private Thread? _deliveryThread;

    public BrokerServer(int port)
    {
        _listener = new TcpListener(IPAddress.Any, port);
        _state = LoadState();
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _listener.Start();
        _running = true;
        _deliveryThread = new Thread(DeliveryLoop)
        {
            IsBackground = true,
            Name = "BrokerDeliveryThread"
        };
        _deliveryThread.Start();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient client = await _listener.AcceptTcpClientAsync(cancellationToken);
                _ = HandleClientAsync(client, cancellationToken);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            _running = false;
            _listener.Stop();
            _deliveryThread.Join();
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken token)
    {
        ConsumerConnection connection = new(client);
        string? registeredConsumer = null;
        Console.WriteLine($"CONNECT: {connection.RemoteEndpoint}");

        try
        {
            while (!token.IsCancellationRequested)
            {
                string? line = await connection.Reader.ReadLineAsync(token);
                if (line is null) break;

                Message message = MessageJson.Deserialize<Message>(line);
                switch (message.Type)
                {
                    case MessageType.Publish:
                        Publish(message);
                        await connection.SendAsync(Reply(message, MessageType.Ack), token);
                        break;

                    case MessageType.Subscribe:
                        registeredConsumer = Subscribe(message, connection);
                        break;

                    case MessageType.Ack:
                        CompleteDelivery(message, true);
                        break;

                    case MessageType.Nack:
                        CompleteDelivery(message, false);
                        break;

                    case MessageType.StatusRequest:
                        await connection.SendAsync(StatusReply(message), token);
                        break;

                    case MessageType.RedriveRequest:
                        bool redriven = await RedriveAsync(message, token);
                        await connection.SendAsync(
                            Reply(message, redriven ? MessageType.Ack : MessageType.Nack,
                                redriven ? null : "Consumer offline or message not found."),
                            token);
                        break;
                }
            }
        }
        catch (Exception exception) when (exception is IOException or SocketException or OperationCanceledException)
        {
            Console.WriteLine($"DISCONNECT: {connection.RemoteEndpoint}");
        }
        finally
        {
            if (registeredConsumer is not null)
            {
                lock (_sync)
                {
                    if (_onlineConsumers.TryGetValue(registeredConsumer, out ConsumerConnection? current)
                        && ReferenceEquals(current, connection))
                        _onlineConsumers.Remove(registeredConsumer);
                }
            }
            connection.Dispose();
        }
    }

    private void Publish(Message message)
    {
        if (string.IsNullOrWhiteSpace(message.SenderId))
            message.SenderId = "producer-" + Guid.NewGuid().ToString("N")[..8];

        lock (_sync)
        {
            _state.PublishedMessages.Add(message);
            foreach (Subscription subscription in _state.Subscriptions.Where(x => x.Topic == message.Topic))
                AddDeliveryIfMissing(message, subscription.ConsumerId);
            SaveState();
        }

        Console.WriteLine($"PUBLISH: {message.MessageId} from {message.SenderId}");
    }

    private string Subscribe(Message message, ConsumerConnection connection)
    {
        string consumerId = string.IsNullOrWhiteSpace(message.SenderId)
            ? "consumer-" + Guid.NewGuid().ToString("N")[..8]
            : message.SenderId;

        lock (_sync)
        {
            _onlineConsumers[consumerId] = connection;
            if (!_state.Subscriptions.Any(x => x.ConsumerId == consumerId && x.Topic == message.Topic))
                _state.Subscriptions.Add(new Subscription { ConsumerId = consumerId, Topic = message.Topic });

            foreach (Message published in _state.PublishedMessages.Where(x => x.Topic == message.Topic))
                AddDeliveryIfMissing(published, consumerId);
            SaveState();
        }

        Console.WriteLine($"SUBSCRIBE: {consumerId} -> {message.Topic} at {connection.RemoteEndpoint}");
        return consumerId;
    }

    private void AddDeliveryIfMissing(Message message, string consumerId)
    {
        if (_state.Deliveries.Any(x => x.Message.MessageId == message.MessageId && x.ConsumerId == consumerId))
            return;

        _state.Deliveries.Add(new Delivery
        {
            Message = message,
            ConsumerId = consumerId,
            Status = DeliveryStatus.Pending,
            NextAttemptUtc = DateTimeOffset.UtcNow
        });
    }

    private void DeliveryLoop()
    {
        Console.WriteLine($"THREAD: {_deliveryThread?.ManagedThreadId} started");

        while (_running)
        {
            ProcessRedriveRequests();
            List<(Delivery Delivery, ConsumerConnection Connection)> toSend = [];

            lock (_sync)
            {
                DateTimeOffset now = DateTimeOffset.UtcNow;

                foreach (Delivery delivery in _state.Deliveries.Where(x =>
                             x.Status == DeliveryStatus.WaitingAck && now - x.SentAtUtc >= AckTimeout).ToList())
                    Fail(delivery, "ACK timeout.");

                foreach (Delivery delivery in _state.Deliveries.Where(x =>
                             x.Status == DeliveryStatus.Pending && x.NextAttemptUtc <= now).ToList())
                {
                    if (_onlineConsumers.TryGetValue(delivery.ConsumerId, out ConsumerConnection? consumer))
                    {
                        delivery.Status = DeliveryStatus.WaitingAck;
                        delivery.SentAtUtc = now;
                        toSend.Add((delivery, consumer));
                    }
                    else
                    {
                        Fail(delivery, "Consumer offline.");
                    }
                }

                if (toSend.Count > 0) SaveState();
            }

            foreach ((Delivery delivery, ConsumerConnection consumer) in toSend)
            {
                try
                {
                    Message copy = new()
                    {
                        MessageId = delivery.Message.MessageId,
                        CorrelationId = delivery.Message.CorrelationId,
                        Type = MessageType.Delivery,
                        SenderId = delivery.Message.SenderId,
                        Topic = delivery.Message.Topic,
                        Payload = delivery.Message.Payload
                    };
                    consumer.Send(copy);
                    Console.WriteLine($"DELIVER: {copy.MessageId} -> {delivery.ConsumerId}");
                }
                catch (IOException)
                {
                    lock (_sync) Fail(delivery, "TCP connection lost.");
                }
            }

            Thread.Sleep(100);
        }
    }

    private void CompleteDelivery(Message response, bool accepted)
    {
        if (response.RelatedMessageId is null) return;

        lock (_sync)
        {
            Delivery? delivery = _state.Deliveries.FirstOrDefault(x =>
                x.Message.MessageId == response.RelatedMessageId
                && x.ConsumerId == response.SenderId
                && x.Status == DeliveryStatus.WaitingAck);
            if (delivery is null) return;

            if (accepted)
            {
                delivery.Status = DeliveryStatus.Acknowledged;
                _state.Acknowledgements.Add(new AcknowledgementInfo
                {
                    MessageId = delivery.Message.MessageId,
                    CorrelationId = delivery.Message.CorrelationId,
                    ConsumerId = delivery.ConsumerId,
                    AcknowledgedAtUtc = DateTimeOffset.UtcNow
                });
                Console.WriteLine($"ACK: {delivery.Message.MessageId} from {delivery.ConsumerId}");
            }
            else
            {
                Fail(delivery, response.Reason ?? "Consumer returned NACK.");
            }
            SaveState();
        }
    }

    private void Fail(Delivery delivery, string reason)
    {
        delivery.RetryCount++;
        delivery.Reason = reason;

        if (delivery.RetryCount >= MaxRetries)
        {
            delivery.Status = DeliveryStatus.DeadLetter;
            delivery.DeadLetteredAtUtc = DateTimeOffset.UtcNow;
            Console.WriteLine($"DLQ: {delivery.Message.MessageId} for {delivery.ConsumerId}");
        }
        else
        {
            delivery.Status = DeliveryStatus.Pending;
            delivery.NextAttemptUtc = DateTimeOffset.UtcNow.AddMilliseconds(500);
        }
        SaveState();
    }

    private Task<bool> RedriveAsync(Message request, CancellationToken token)
    {
        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _redriveRequests.Enqueue(new RedriveRequest(request.RelatedMessageId, completion));
        return completion.Task.WaitAsync(token);
    }

    private void ProcessRedriveRequests()
    {
        while (_redriveRequests.TryDequeue(out RedriveRequest? request))
        {
            bool success = false;
            lock (_sync)
            {
                Delivery? delivery = _state.Deliveries.FirstOrDefault(x =>
                    x.Message.MessageId == request.MessageId
                    && x.Status == DeliveryStatus.DeadLetter
                    && _onlineConsumers.ContainsKey(x.ConsumerId));

                if (delivery is not null)
                {
                    delivery.Status = DeliveryStatus.Pending;
                    delivery.RetryCount = 0;
                    delivery.Reason = string.Empty;
                    delivery.NextAttemptUtc = DateTimeOffset.UtcNow;
                    delivery.DeadLetteredAtUtc = null;
                    SaveState();
                    success = true;
                    Console.WriteLine($"REDRIVE by thread {_deliveryThread?.ManagedThreadId}: {delivery.Message.MessageId}");
                }
            }
            request.Completion.SetResult(success);
        }
    }

    private Message StatusReply(Message request)
    {
        BrokerStatus status;
        lock (_sync)
        {
            List<Delivery> topicDeliveries = _state.Deliveries.Where(x => x.Message.Topic == request.Topic).ToList();
            HashSet<string> topicConsumers = _state.Subscriptions
                .Where(x => x.Topic == request.Topic && _onlineConsumers.ContainsKey(x.ConsumerId))
                .Select(x => x.ConsumerId)
                .ToHashSet();
            status = new BrokerStatus
            {
                ConsumerConnected = topicConsumers.Count > 0,
                ConsumerCount = topicConsumers.Count,
                PendingMessages = topicDeliveries.Count(x => x.Status == DeliveryStatus.Pending),
                InFlightMessages = topicDeliveries.Count(x => x.Status == DeliveryStatus.WaitingAck),
                DeadLetterMessages = topicDeliveries.Count(x => x.Status == DeliveryStatus.DeadLetter),
                AcknowledgedMessages = topicDeliveries.Count(x => x.Status == DeliveryStatus.Acknowledged),
                ConsumerEndpoints = topicConsumers.Select(id => $"{id} = {_onlineConsumers[id].RemoteEndpoint}").ToList(),
                RecentAcknowledgements = _state.Acknowledgements.TakeLast(20).ToList(),
                DeadLetters = topicDeliveries.Where(x => x.Status == DeliveryStatus.DeadLetter).Select(x => new DeadLetterInfo
                {
                    MessageId = x.Message.MessageId,
                    CorrelationId = x.Message.CorrelationId,
                    Topic = x.Message.Topic,
                    ConsumerId = x.ConsumerId,
                    RetryCount = x.RetryCount,
                    Reason = x.Reason,
                    DeadLetteredAtUtc = x.DeadLetteredAtUtc ?? DateTimeOffset.UtcNow
                }).ToList()
            };
        }

        Message reply = Reply(request, MessageType.StatusResponse);
        reply.Payload = MessageJson.Serialize(status);
        return reply;
    }

    private static Message Reply(Message request, MessageType type, string? reason = null) => new()
    {
        Type = type,
        SenderId = "broker",
        Topic = request.Topic,
        CorrelationId = request.CorrelationId,
        RelatedMessageId = request.MessageId,
        Reason = reason
    };

    private BrokerState LoadState()
    {
        try
        {
            return File.Exists(_stateFile)
                ? MessageJson.Deserialize<BrokerState>(File.ReadAllText(_stateFile))
                : new BrokerState();
        }
        catch
        {
            return new BrokerState();
        }
    }

    private void SaveState()
    {
        Directory.CreateDirectory(Path.GetDirectoryName(_stateFile)!);
        File.WriteAllText(_stateFile, MessageJson.Serialize(_state));
    }
}

internal sealed class ConsumerConnection : IDisposable
{
    private readonly TcpClient _client;
    private readonly StreamWriter _writer;
    private readonly object _sendLock = new();

    public ConsumerConnection(TcpClient client)
    {
        _client = client;
        RemoteEndpoint = client.Client.RemoteEndPoint?.ToString() ?? "unknown";
        NetworkStream stream = client.GetStream();
        Reader = new StreamReader(stream, Encoding.UTF8, false, leaveOpen: true);
        _writer = new StreamWriter(stream, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
    }

    public string RemoteEndpoint { get; }
    public StreamReader Reader { get; }
    public void Send(Message message)
    {
        lock (_sendLock) _writer.WriteLine(MessageJson.Serialize(message));
    }
    public Task SendAsync(Message message, CancellationToken token)
    {
        lock (_sendLock) _writer.WriteLine(MessageJson.Serialize(message));
        return Task.CompletedTask;
    }
    public void Dispose()
    {
        Reader.Dispose();
        _writer.Dispose();
        _client.Dispose();
    }
}

public sealed class BrokerState
{
    public List<Message> PublishedMessages { get; set; } = [];
    public List<Subscription> Subscriptions { get; set; } = [];
    public List<Delivery> Deliveries { get; set; } = [];
    public List<AcknowledgementInfo> Acknowledgements { get; set; } = [];
}

public sealed class Subscription
{
    public string ConsumerId { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
}

public sealed class Delivery
{
    public Message Message { get; set; } = new();
    public string ConsumerId { get; set; } = string.Empty;
    public DeliveryStatus Status { get; set; }
    public int RetryCount { get; set; }
    public string Reason { get; set; } = string.Empty;
    public DateTimeOffset NextAttemptUtc { get; set; }
    public DateTimeOffset SentAtUtc { get; set; }
    public DateTimeOffset? DeadLetteredAtUtc { get; set; }
}

public enum DeliveryStatus { Pending, WaitingAck, Acknowledged, DeadLetter }
internal sealed record RedriveRequest(Guid? MessageId, TaskCompletionSource<bool> Completion);
