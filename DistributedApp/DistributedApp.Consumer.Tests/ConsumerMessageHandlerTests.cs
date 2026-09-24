using DistributedApp.Contracts;

namespace DistributedApp.Consumer.Tests;

public class ConsumerMessageHandlerTests
{
    [Fact]
    public async Task HandleAsync_ProcessesOrderAndReturnsAck()
    {
        string directory = CreateTestDirectory();

        try
        {
            ProcessedMessageStore store =
                new(Path.Combine(directory, "state.json"));
            ConsumerMessageHandler handler =
                new(new MessageProcessor(store));
            Message incoming = CreateMessage();

            Message response = await handler.HandleAsync(incoming);

            Assert.Equal(MessageType.Ack, response.Type);
            Assert.Equal(incoming.MessageId, response.RelatedMessageId);
            Assert.Equal(incoming.CorrelationId, response.CorrelationId);
            Assert.True(store.IsProcessed(incoming.MessageId));
            Assert.Equal(1, store.Count);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task HandleAsync_AcknowledgesDuplicateWithoutRepeatingEffect()
    {
        string directory = CreateTestDirectory();

        try
        {
            ProcessedMessageStore store =
                new(Path.Combine(directory, "state.json"));
            ConsumerMessageHandler handler =
                new(new MessageProcessor(store));
            Message incoming = CreateMessage();

            Message firstResponse = await handler.HandleAsync(incoming);
            Message duplicateResponse = await handler.HandleAsync(incoming);

            Assert.Equal(MessageType.Ack, firstResponse.Type);
            Assert.Equal(MessageType.Ack, duplicateResponse.Type);
            Assert.Equal(1, store.Count);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task HandleAsync_ReturnsNackWhenPayloadIsInvalid()
    {
        string directory = CreateTestDirectory();

        try
        {
            ProcessedMessageStore store =
                new(Path.Combine(directory, "state.json"));
            ConsumerMessageHandler handler =
                new(new MessageProcessor(store));
            Message incoming = CreateMessage();
            incoming.Payload = "{invalid-json";

            Message response = await handler.HandleAsync(incoming);

            Assert.Equal(MessageType.Nack, response.Type);
            Assert.Equal(incoming.MessageId, response.RelatedMessageId);
            Assert.False(string.IsNullOrWhiteSpace(response.Reason));
            Assert.False(store.IsProcessed(incoming.MessageId));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task CrashBeforeAck_RestartAcknowledgesWithoutRepeatingEffect()
    {
        string directory = CreateTestDirectory();
        string statePath = Path.Combine(directory, "state.json");

        try
        {
            Message incoming = CreateMessage();
            ProcessedMessageStore firstStore = new(statePath);
            ConsumerMessageHandler crashingHandler =
                new(
                    new MessageProcessor(firstStore),
                    simulateCrashBeforeAcknowledgement: true);

            await Assert.ThrowsAsync<SimulatedConsumerCrashException>(
                () => crashingHandler.HandleAsync(incoming));

            Assert.True(firstStore.IsProcessed(incoming.MessageId));
            Assert.Equal(1, firstStore.Count);

            ProcessedMessageStore restartedStore = new(statePath);
            ConsumerMessageHandler restartedHandler =
                new(new MessageProcessor(restartedStore));

            Message response = await restartedHandler.HandleAsync(incoming);

            Assert.Equal(MessageType.Ack, response.Type);
            Assert.Equal(incoming.MessageId, response.RelatedMessageId);
            Assert.Equal(1, restartedStore.Count);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static Message CreateMessage()
    {
        return new Message
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Type = MessageType.Message,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = "orders",
            SequenceNumber = 1,
            RetryCount = 0,
            Payload =
                "{\"orderId\":\"ORD-1\",\"product\":\"Laptop\",\"quantity\":2}"
        };
    }

    private static string CreateTestDirectory()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "distributed-app-consumer-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);
        return directory;
    }
}
