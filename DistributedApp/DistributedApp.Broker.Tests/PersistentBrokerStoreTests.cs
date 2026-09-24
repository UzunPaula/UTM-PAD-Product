using DistributedApp.Contracts;

namespace DistributedApp.Broker.Tests;

public class PersistentBrokerStoreTests
{
    [Fact]
    public async Task EnqueueAsync_AssignsSequenceAndPreservesFifoOrder()
    {
        using TestDirectory directory = new();
        PersistentBrokerStore store = new(directory.StatePath);
        Message first = CreatePublishedMessage("first");
        Message second = CreatePublishedMessage("second");

        Message storedFirst = await store.EnqueueAsync(first);
        Message storedSecond = await store.EnqueueAsync(second);

        Assert.Equal(1, storedFirst.SequenceNumber);
        Assert.Equal(2, storedSecond.SequenceNumber);
        Assert.Equal(MessageType.Message, storedFirst.Type);
        Assert.Equal(
            first.MessageId,
            (await store.PeekAsync("orders"))!.MessageId);
    }

    [Fact]
    public async Task AcknowledgeAsync_RemovesOnlyTheQueueHead()
    {
        using TestDirectory directory = new();
        PersistentBrokerStore store = new(directory.StatePath);
        Message first = await store.EnqueueAsync(
            CreatePublishedMessage("first"));
        Message second = await store.EnqueueAsync(
            CreatePublishedMessage("second"));

        Assert.False(
            await store.AcknowledgeAsync("orders", second.MessageId));
        Assert.True(
            await store.AcknowledgeAsync("orders", first.MessageId));
        Assert.Equal(
            second.MessageId,
            (await store.PeekAsync("orders"))!.MessageId);
    }

    [Fact]
    public async Task RecordFailureAsync_RetriesThenMovesToDeadLetterQueue()
    {
        using TestDirectory directory = new();
        PersistentBrokerStore store = new(directory.StatePath);
        Message message = await store.EnqueueAsync(
            CreatePublishedMessage("failed"));

        DeliveryFailureStatus first = await store.RecordFailureAsync(
            "orders",
            message.MessageId,
            "Nack",
            maxRetries: 1);
        DeliveryFailureStatus second = await store.RecordFailureAsync(
            "orders",
            message.MessageId,
            "Nack",
            maxRetries: 1);

        Assert.Equal(DeliveryFailureStatus.Retrying, first);
        Assert.Equal(DeliveryFailureStatus.DeadLettered, second);
        Assert.Null(await store.PeekAsync("orders"));

        DeadLetterMessage deadLetter = Assert.Single(
            await store.GetDeadLettersAsync());
        Assert.Equal(message.MessageId, deadLetter.Message.MessageId);
        Assert.Equal(2, deadLetter.Message.RetryCount);
        Assert.Equal("Nack", deadLetter.Reason);
    }

    [Fact]
    public async Task Constructor_ReloadsUnacknowledgedMessage()
    {
        using TestDirectory directory = new();
        Message published = CreatePublishedMessage("persistent");
        PersistentBrokerStore firstStore = new(directory.StatePath);
        await firstStore.EnqueueAsync(published);

        PersistentBrokerStore restartedStore = new(directory.StatePath);
        Message? restored = await restartedStore.PeekAsync("orders");

        Assert.NotNull(restored);
        Assert.Equal(published.MessageId, restored.MessageId);
    }

    [Fact]
    public void Constructor_RejectsCorruptedState()
    {
        using TestDirectory directory = new();
        File.WriteAllText(directory.StatePath, "{invalid-json");

        Assert.Throws<InvalidDataException>(
            () => new PersistentBrokerStore(directory.StatePath));
    }

    private static Message CreatePublishedMessage(string orderId)
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
