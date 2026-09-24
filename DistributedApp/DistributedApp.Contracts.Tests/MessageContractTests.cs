using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Contracts.Tests;

public class MessageContractTests
{
    [Fact]
    public void Serialize_UsesTheDocumentedFieldNames()
    {
        Message message = CreateValidMessage();

        string json = MessageJson.Serialize(message);
        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement root = document.RootElement;

        Assert.Equal(message.MessageId, root.GetProperty("messageId").GetGuid());
        Assert.Equal(message.CorrelationId, root.GetProperty("correlationId").GetGuid());
        Assert.Equal("Publish", root.GetProperty("messageType").GetString());
        Assert.Equal(MessageSchema.CurrentVersion, root.GetProperty("schemaVersion").GetString());
        Assert.Equal(TimeSpan.Zero, root.GetProperty("occurredAt").GetDateTimeOffset().Offset);
        Assert.Equal("orders", root.GetProperty("topic").GetString());
        Assert.Equal(7, root.GetProperty("sequenceNumber").GetInt64());
        Assert.Equal(0, root.GetProperty("retryCount").GetInt32());
        Assert.Equal(message.Payload, root.GetProperty("payload").GetString());
        Assert.False(root.TryGetProperty("type", out _));
        Assert.False(root.TryGetProperty("occurredAtUtc", out _));
    }

    [Fact]
    public void Deserialize_RoundTripsTheMessage()
    {
        Message expected = CreateValidMessage();

        Message actual = MessageJson.Deserialize(MessageJson.Serialize(expected));

        Assert.Equal(expected.MessageId, actual.MessageId);
        Assert.Equal(expected.CorrelationId, actual.CorrelationId);
        Assert.Equal(expected.Type, actual.Type);
        Assert.Equal(expected.SchemaVersion, actual.SchemaVersion);
        Assert.Equal(expected.OccurredAtUtc, actual.OccurredAtUtc);
        Assert.Equal(expected.Topic, actual.Topic);
        Assert.Equal(expected.SequenceNumber, actual.SequenceNumber);
        Assert.Equal(expected.RetryCount, actual.RetryCount);
        Assert.Equal(expected.Payload, actual.Payload);
    }

    [Fact]
    public void Deserialize_AcceptsUnknownFieldsForForwardCompatibility()
    {
        string json = MessageJson.Serialize(CreateValidMessage());
        json = json.Insert(json.Length - 1, ",\"futureField\":\"future-value\"");

        Message message = MessageJson.Deserialize(json);

        Assert.Equal(MessageType.Publish, message.Type);
    }

    [Fact]
    public void Deserialize_RejectsNumericMessageTypes()
    {
        string json = MessageJson.Serialize(CreateValidMessage());
        json = json.Replace("\"Publish\"", "0", StringComparison.Ordinal);

        Assert.Throws<JsonException>(() => MessageJson.Deserialize(json));
    }

    private static Message CreateValidMessage()
    {
        return new Message
        {
            MessageId = Guid.Parse("11111111-1111-1111-1111-111111111111"),
            CorrelationId = Guid.Parse("22222222-2222-2222-2222-222222222222"),
            Type = MessageType.Publish,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = new DateTimeOffset(2026, 9, 23, 10, 30, 0, TimeSpan.Zero),
            Topic = "orders",
            SequenceNumber = 7,
            RetryCount = 0,
            Payload = "{\"orderId\":\"ORD-7\",\"product\":\"Laptop\",\"quantity\":2}"
        };
    }
}
