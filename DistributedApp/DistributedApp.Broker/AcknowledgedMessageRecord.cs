namespace DistributedApp.Broker;

public sealed class AcknowledgedMessageRecord
{
    public Guid MessageId { get; set; }

    public Guid CorrelationId { get; set; }

    public string Topic { get; set; } = string.Empty;

    public DateTimeOffset AcknowledgedAtUtc { get; set; }
}
