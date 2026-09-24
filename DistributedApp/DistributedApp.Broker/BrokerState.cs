using DistributedApp.Contracts;

namespace DistributedApp.Broker;

public sealed class BrokerState
{
    public Dictionary<string, List<Message>> Queues { get; set; } = new();

    public Dictionary<string, long> NextSequenceNumbers { get; set; } = new();

    public List<DeadLetterMessage> DeadLetters { get; set; } = new();

    public Dictionary<string, long> AcknowledgedMessageCounts { get; set; } =
        new();

    public List<AcknowledgedMessageRecord> RecentAcknowledgements { get; set; } =
        new();
}
