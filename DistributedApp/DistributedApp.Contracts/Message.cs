using System.Text.Json.Serialization;

namespace DistributedApp.Contracts;

public class Message
{
    [JsonPropertyName("messageId")]
    public Guid MessageId { get; set; }

    [JsonPropertyName("correlationId")]
    public Guid CorrelationId { get; set; }

    [JsonPropertyName("messageType")]
    public MessageType Type { get; set; }

    [JsonPropertyName("schemaVersion")]
    public string SchemaVersion { get; set; } = MessageSchema.CurrentVersion;

    [JsonPropertyName("occurredAt")]
    public DateTimeOffset OccurredAtUtc { get; set; }

    [JsonPropertyName("topic")]
    public string Topic { get; set; } = string.Empty;

    [JsonPropertyName("sequenceNumber")]
    public long SequenceNumber { get; set; }

    [JsonPropertyName("retryCount")]
    public int RetryCount { get; set; }

    [JsonPropertyName("payload")]
    public string Payload { get; set; } = string.Empty;

    [JsonPropertyName("relatedMessageId")]
    public Guid? RelatedMessageId { get; set; }

    [JsonPropertyName("reason")]
    public string? Reason { get; set; }
}
