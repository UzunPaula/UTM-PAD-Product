using System.Text.Json;
using System.Text.Json.Serialization;

namespace DistributedApp.Contracts;

// One JSON line on TCP is one complete application message.
public sealed class Message
{
    public Guid MessageId { get; set; } = Guid.NewGuid();
    public Guid CorrelationId { get; set; } = Guid.NewGuid();
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public MessageType Type { get; set; }
    public string SenderId { get; set; } = string.Empty;
    public string Topic { get; set; } = "orders-producer";
    public string Payload { get; set; } = string.Empty;
    public Guid? RelatedMessageId { get; set; }
    public string? Reason { get; set; }
}

public enum MessageType { Publish, Subscribe, Delivery, Ack, Nack, StatusRequest, StatusResponse, RedriveRequest }
public sealed record OrderPayload(string OrderId, string Product, int Quantity);

public sealed class BrokerStatus
{
    public bool ConsumerConnected { get; set; }
    public int ConsumerCount { get; set; }
    public int PendingMessages { get; set; }
    public int InFlightMessages { get; set; }
    public int DeadLetterMessages { get; set; }
    public int AcknowledgedMessages { get; set; }
    public List<string> ConsumerEndpoints { get; set; } = [];
    public List<AcknowledgementInfo> RecentAcknowledgements { get; set; } = [];
    public List<DeadLetterInfo> DeadLetters { get; set; } = [];
    public DateTimeOffset ObservedAtUtc { get; set; } = DateTimeOffset.UtcNow;
}

public sealed class AcknowledgementInfo
{
    public Guid MessageId { get; set; }
    public Guid CorrelationId { get; set; }
    public string ConsumerId { get; set; } = string.Empty;
    public DateTimeOffset AcknowledgedAtUtc { get; set; }
}

public sealed class DeadLetterInfo
{
    public Guid MessageId { get; set; }
    public Guid CorrelationId { get; set; }
    public string Topic { get; set; } = string.Empty;
    public string ConsumerId { get; set; } = string.Empty;
    public int RetryCount { get; set; }
    public string Reason { get; set; } = string.Empty;
    public DateTimeOffset DeadLetteredAtUtc { get; set; }
}

public static class MessageJson
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
        WriteIndented = false
    };
    public static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Options);
    public static T Deserialize<T>(string json) => JsonSerializer.Deserialize<T>(json, Options)
        ?? throw new JsonException("Empty JSON message.");
}
