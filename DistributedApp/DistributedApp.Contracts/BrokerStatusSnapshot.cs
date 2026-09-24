using System.Text.Json.Serialization;

namespace DistributedApp.Contracts;

public sealed class BrokerStatusSnapshot
{
    [JsonPropertyName("topic")]
    public string Topic { get; set; } = string.Empty;

    [JsonPropertyName("consumerConnected")]
    public bool ConsumerConnected { get; set; }

    [JsonPropertyName("pendingMessages")]
    public int PendingMessages { get; set; }

    [JsonPropertyName("inFlightMessages")]
    public int InFlightMessages { get; set; }

    [JsonPropertyName("deadLetterMessages")]
    public int DeadLetterMessages { get; set; }

    [JsonPropertyName("acknowledgedMessages")]
    public long AcknowledgedMessages { get; set; }

    [JsonPropertyName("recentAcknowledgements")]
    public List<AcknowledgementSummary> RecentAcknowledgements { get; set; } =
        new();

    [JsonPropertyName("deadLetters")]
    public List<DeadLetterSummary> DeadLetters { get; set; } = new();

    [JsonPropertyName("observedAtUtc")]
    public DateTimeOffset ObservedAtUtc { get; set; }
}

public sealed class AcknowledgementSummary
{
    [JsonPropertyName("messageId")]
    public Guid MessageId { get; set; }

    [JsonPropertyName("correlationId")]
    public Guid CorrelationId { get; set; }

    [JsonPropertyName("topic")]
    public string Topic { get; set; } = string.Empty;

    [JsonPropertyName("acknowledgedAtUtc")]
    public DateTimeOffset AcknowledgedAtUtc { get; set; }
}

public sealed class DeadLetterSummary
{
    [JsonPropertyName("messageId")]
    public Guid MessageId { get; set; }

    [JsonPropertyName("correlationId")]
    public Guid CorrelationId { get; set; }

    [JsonPropertyName("topic")]
    public string Topic { get; set; } = string.Empty;

    [JsonPropertyName("retryCount")]
    public int RetryCount { get; set; }

    [JsonPropertyName("reason")]
    public string Reason { get; set; } = string.Empty;

    [JsonPropertyName("deadLetteredAtUtc")]
    public DateTimeOffset DeadLetteredAtUtc { get; set; }
}
