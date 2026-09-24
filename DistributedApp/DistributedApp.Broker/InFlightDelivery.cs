namespace DistributedApp.Broker;

internal sealed record InFlightDelivery(
    Guid MessageId,
    DateTimeOffset SentAtUtc);
