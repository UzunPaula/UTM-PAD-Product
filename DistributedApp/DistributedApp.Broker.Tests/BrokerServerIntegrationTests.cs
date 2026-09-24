using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using DistributedApp.Consumer;
using DistributedApp.Contracts;

namespace DistributedApp.Broker.Tests;

public class BrokerServerIntegrationTests
{
    [Fact]
    public async Task RunAsync_PublishesInOrderAndCompletesOnAck()
    {
        using TestDirectory directory = new();
        using CancellationTokenSource cancellation =
            new(TimeSpan.FromSeconds(10));
        PersistentBrokerStore store = new(directory.StatePath);
        BrokerServer server = new(
            CreateSettings(directory.StatePath),
            store);
        Task serverTask = server.RunAsync(cancellation.Token);

        await using TestClient producer =
            await TestClient.ConnectAsync(server.BoundPort);
        Message first = CreateMessage("first");
        Message second = CreateMessage("second");

        await producer.SendAsync(first);
        Assert.Equal(
            first.MessageId,
            (await producer.ReceiveAsync()).RelatedMessageId);
        await producer.SendAsync(second);
        Assert.Equal(
            second.MessageId,
            (await producer.ReceiveAsync()).RelatedMessageId);

        await using TestClient consumer =
            await TestClient.ConnectAsync(server.BoundPort);
        await consumer.SendAsync(CreateSubscription());

        Message firstDelivery = await consumer.ReceiveAsync();
        Assert.Equal(first.MessageId, firstDelivery.MessageId);
        Assert.Equal(1, firstDelivery.SequenceNumber);
        await consumer.SendAsync(CreateAck(firstDelivery));

        Message secondDelivery = await consumer.ReceiveAsync();
        Assert.Equal(second.MessageId, secondDelivery.MessageId);
        Assert.Equal(2, secondDelivery.SequenceNumber);
        await consumer.SendAsync(CreateAck(secondDelivery));

        await WaitUntilAsync(
            async () => await store.PeekAsync("orders") == null);

        cancellation.Cancel();
        await serverTask;
    }

    [Fact]
    public async Task RunAsync_MovesTimedOutMessageToDeadLetterQueue()
    {
        using TestDirectory directory = new();
        using CancellationTokenSource cancellation =
            new(TimeSpan.FromSeconds(10));
        BrokerSettings settings = CreateSettings(directory.StatePath);
        settings.MaxRetries = 0;
        settings.AcknowledgementTimeoutMilliseconds = 100;
        settings.RetryScanIntervalMilliseconds = 20;
        PersistentBrokerStore store = new(directory.StatePath);
        BrokerServer server = new(settings, store);
        Task serverTask = server.RunAsync(cancellation.Token);

        await using TestClient producer =
            await TestClient.ConnectAsync(server.BoundPort);
        Message published = CreateMessage("timeout");
        await producer.SendAsync(published);
        await producer.ReceiveAsync();

        await using TestClient consumer =
            await TestClient.ConnectAsync(server.BoundPort);
        await consumer.SendAsync(CreateSubscription());
        Message delivered = await consumer.ReceiveAsync();
        Assert.Equal(published.MessageId, delivered.MessageId);

        await WaitUntilAsync(
            async () => (await store.GetDeadLettersAsync()).Count == 1);

        DeadLetterMessage deadLetter = Assert.Single(
            await store.GetDeadLettersAsync());
        Assert.Equal(published.MessageId, deadLetter.Message.MessageId);
        Assert.Equal("Acknowledgement timeout.", deadLetter.Reason);

        cancellation.Cancel();
        await serverTask;
    }

    [Fact]
    public async Task RunAsync_MovesNackedMessageToDeadLetterQueue()
    {
        using TestDirectory directory = new();
        using CancellationTokenSource cancellation =
            new(TimeSpan.FromSeconds(10));
        BrokerSettings settings = CreateSettings(directory.StatePath);
        settings.MaxRetries = 0;
        PersistentBrokerStore store = new(directory.StatePath);
        BrokerServer server = new(settings, store);
        Task serverTask = server.RunAsync(cancellation.Token);

        await using TestClient producer =
            await TestClient.ConnectAsync(server.BoundPort);
        Message published = CreateMessage("nack");
        await producer.SendAsync(published);
        await producer.ReceiveAsync();

        await using TestClient consumer =
            await TestClient.ConnectAsync(server.BoundPort);
        await consumer.SendAsync(CreateSubscription());
        Message delivered = await consumer.ReceiveAsync();
        await consumer.SendAsync(CreateNack(delivered, "Invalid order."));

        await WaitUntilAsync(
            async () => (await store.GetDeadLettersAsync()).Count == 1);

        DeadLetterMessage deadLetter = Assert.Single(
            await store.GetDeadLettersAsync());
        Assert.Equal(published.MessageId, deadLetter.Message.MessageId);
        Assert.Equal("Invalid order.", deadLetter.Reason);

        cancellation.Cancel();
        await serverTask;
    }

    [Fact]
    public async Task RunAsync_ReportsLiveQueueConsumerAndDeadLetterState()
    {
        using TestDirectory directory = new();
        using CancellationTokenSource cancellation =
            new(TimeSpan.FromSeconds(10));
        BrokerSettings settings = CreateSettings(directory.StatePath);
        settings.MaxRetries = 0;
        PersistentBrokerStore store = new(directory.StatePath);
        BrokerServer server = new(settings, store);
        Task serverTask = server.RunAsync(cancellation.Token);

        await using TestClient monitor =
            await TestClient.ConnectAsync(server.BoundPort);
        BrokerStatusSnapshot initial =
            await RequestStatusAsync(monitor);
        Assert.False(initial.ConsumerConnected);
        Assert.Equal(0, initial.PendingMessages);
        Assert.Equal(0, initial.InFlightMessages);
        Assert.Equal(0, initial.DeadLetterMessages);

        await using TestClient producer =
            await TestClient.ConnectAsync(server.BoundPort);
        Message published = CreateMessage("monitored");
        await producer.SendAsync(published);
        await producer.ReceiveAsync();

        BrokerStatusSnapshot queued =
            await RequestStatusAsync(monitor);
        Assert.False(queued.ConsumerConnected);
        Assert.Equal(1, queued.PendingMessages);
        Assert.Equal(0, queued.InFlightMessages);

        await using TestClient consumer =
            await TestClient.ConnectAsync(server.BoundPort);
        await consumer.SendAsync(CreateSubscription());
        Message delivered = await consumer.ReceiveAsync();

        BrokerStatusSnapshot delivering =
            await RequestStatusAsync(monitor);
        Assert.True(delivering.ConsumerConnected);
        Assert.Equal(0, delivering.PendingMessages);
        Assert.Equal(1, delivering.InFlightMessages);

        await consumer.SendAsync(
            CreateNack(delivered, "Rejected for monitoring test."));
        await WaitUntilAsync(
            async () => (await store.GetDeadLettersAsync()).Count == 1);

        BrokerStatusSnapshot failed =
            await RequestStatusAsync(monitor);
        Assert.Equal(0, failed.InFlightMessages);
        Assert.Equal(1, failed.DeadLetterMessages);
        DeadLetterSummary deadLetter =
            Assert.Single(failed.DeadLetters);
        Assert.Equal(published.MessageId, deadLetter.MessageId);
        Assert.Equal("Rejected for monitoring test.", deadLetter.Reason);

        Message completed = CreateMessage("completed");
        await producer.SendAsync(completed);
        await producer.ReceiveAsync();
        Message completedDelivery = await consumer.ReceiveAsync();
        await consumer.SendAsync(CreateAck(completedDelivery));
        await WaitUntilAsync(
            async () => await store.PeekAsync("orders") == null);

        BrokerStatusSnapshot completedStatus =
            await RequestStatusAsync(monitor);
        Assert.Equal(1, completedStatus.AcknowledgedMessages);
        AcknowledgementSummary acknowledgement =
            Assert.Single(completedStatus.RecentAcknowledgements);
        Assert.Equal(completed.MessageId, acknowledgement.MessageId);

        cancellation.Cancel();
        await serverTask;
    }

    [Fact]
    public async Task RunAsync_RedeliversAfterConsumerCrashWithoutDuplicateEffect()
    {
        using TestDirectory directory = new();
        using CancellationTokenSource cancellation =
            new(TimeSpan.FromSeconds(10));
        string consumerStatePath = Path.Combine(
            directory.PathValue,
            "consumer-state.json");
        PersistentBrokerStore brokerStore = new(directory.StatePath);
        BrokerServer server = new(
            CreateSettings(directory.StatePath),
            brokerStore);
        Task serverTask = server.RunAsync(cancellation.Token);

        ProcessedMessageStore firstConsumerStore =
            new(consumerStatePath);
        ConsumerClient crashingConsumer = new(
            "127.0.0.1",
            server.BoundPort,
            new ConsumerMessageHandler(
                new MessageProcessor(firstConsumerStore),
                simulateCrashBeforeAcknowledgement: true));
        Task crashingTask = crashingConsumer.RunAsync(
            "orders",
            cancellation.Token);

        await using TestClient producer =
            await TestClient.ConnectAsync(server.BoundPort);
        Message published = CreateMessage("crash-before-ack");
        await producer.SendAsync(published);
        await producer.ReceiveAsync();

        await Assert.ThrowsAsync<SimulatedConsumerCrashException>(
            async () => await crashingTask);
        Assert.Equal(1, firstConsumerStore.Count);

        ProcessedMessageStore restartedStore = new(consumerStatePath);
        ConsumerClient restartedConsumer = new(
            "127.0.0.1",
            server.BoundPort,
            new ConsumerMessageHandler(
                new MessageProcessor(restartedStore)));
        Task restartedTask = restartedConsumer.RunAsync(
            "orders",
            cancellation.Token);

        await WaitUntilAsync(
            async () => await brokerStore.PeekAsync("orders") == null);

        Assert.Equal(1, restartedStore.Count);
        Assert.True(restartedStore.IsProcessed(published.MessageId));

        cancellation.Cancel();
        await restartedTask;
        await serverTask;
    }

    private static BrokerSettings CreateSettings(string statePath)
    {
        return new BrokerSettings
        {
            Port = 0,
            StateFilePath = statePath,
            AcknowledgementTimeoutMilliseconds = 2000,
            RetryScanIntervalMilliseconds = 25,
            MaxRetries = 2
        };
    }

    private static Message CreateMessage(string orderId)
    {
        return new Message
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Type = MessageType.Publish,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = "orders",
            Payload =
                $"{{\"orderId\":\"{orderId}\",\"product\":\"Book\",\"quantity\":1}}"
        };
    }

    private static Message CreateSubscription()
    {
        return new Message
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Type = MessageType.Subscribe,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = "orders"
        };
    }

    private static Message CreateAck(Message delivered)
    {
        return new Message
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = delivered.CorrelationId,
            Type = MessageType.Ack,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = delivered.Topic,
            SequenceNumber = delivered.SequenceNumber,
            RelatedMessageId = delivered.MessageId
        };
    }

    private static Message CreateNack(Message delivered, string reason)
    {
        Message nack = CreateAck(delivered);
        nack.Type = MessageType.Nack;
        nack.Reason = reason;
        return nack;
    }

    private static async Task<BrokerStatusSnapshot> RequestStatusAsync(
        TestClient client)
    {
        Message request = new()
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Type = MessageType.StatusRequest,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = "orders"
        };
        await client.SendAsync(request);
        Message response = await client.ReceiveAsync();

        Assert.Equal(MessageType.StatusResponse, response.Type);
        Assert.Equal(request.MessageId, response.RelatedMessageId);
        return JsonSerializer.Deserialize<BrokerStatusSnapshot>(
                   response.Payload)
               ?? throw new InvalidDataException(
                   "Expected a broker status payload.");
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        DateTimeOffset deadline = DateTimeOffset.UtcNow.AddSeconds(3);

        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail("The expected broker state was not reached.");
    }

    private sealed class TestClient : IAsyncDisposable
    {
        private readonly TcpClient _client;
        private readonly StreamReader _reader;
        private readonly StreamWriter _writer;

        private TestClient(TcpClient client)
        {
            _client = client;
            NetworkStream stream = client.GetStream();
            _reader = new StreamReader(
                stream,
                Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                leaveOpen: true);
            _writer = new StreamWriter(
                stream,
                new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
                leaveOpen: true)
            {
                AutoFlush = true,
                NewLine = "\n"
            };
        }

        public static async Task<TestClient> ConnectAsync(int port)
        {
            TcpClient client = new();
            await client.ConnectAsync("127.0.0.1", port);
            return new TestClient(client);
        }

        public Task SendAsync(Message message)
        {
            return _writer.WriteLineAsync(MessageJson.Serialize(message));
        }

        public async Task<Message> ReceiveAsync()
        {
            string? line = await _reader.ReadLineAsync()
                .WaitAsync(TimeSpan.FromSeconds(3));
            Assert.NotNull(line);
            return MessageJson.Deserialize(line);
        }

        public async ValueTask DisposeAsync()
        {
            _reader.Dispose();
            await _writer.DisposeAsync();
            _client.Dispose();
        }
    }

    private sealed class TestDirectory : IDisposable
    {
        public TestDirectory()
        {
            PathValue = Path.Combine(
                Path.GetTempPath(),
                "distributed-app-broker-tests",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(PathValue);
        }

        public string PathValue { get; }

        public string StatePath => Path.Combine(PathValue, "state.json");

        public void Dispose()
        {
            Directory.Delete(PathValue, recursive: true);
        }
    }
}
