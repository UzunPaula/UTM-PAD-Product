using DistributedApp.Contracts;

namespace DistributedApp.Broker;

public sealed class DeadLetterMessage
{
    public Message Message { get; set; } = new();

    public string Reason { get; set; } = string.Empty;

    public DateTimeOffset DeadLetteredAtUtc { get; set; }
}
