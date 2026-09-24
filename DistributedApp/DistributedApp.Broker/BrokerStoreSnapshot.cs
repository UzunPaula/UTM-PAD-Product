namespace DistributedApp.Broker;

public sealed record BrokerStoreSnapshot(
    int QueuedMessages,
    IReadOnlyList<DeadLetterMessage> DeadLetters,
    long AcknowledgedMessages,
    IReadOnlyList<AcknowledgedMessageRecord> RecentAcknowledgements);
