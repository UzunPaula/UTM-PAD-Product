namespace DistributedApp.Contracts;

public class Message
{
    public Guid MessageId { get; set; }

    public Guid CorrelationId { get; set; }
    public MessageType Type { get; set; }

    public string Topic { get; set; } = string.Empty;

    public string SchemaVersion { get; set; } = "1.0";

    public long SequenceNumber { get; set; }

    public int RetryCount { get; set; }

    public DateTime CreatedAt { get; set; }

    public string Payload { get; set; } = string.Empty;
    
    public string Reason { get; set; } = string.Empty;
}