namespace DistributedApp.Producer;

public sealed class ProducerSettings
{
    public string BrokerHost { get; init; } = "127.0.0.1";

    public int BrokerPort { get; init; } = 5000;

    public int RequestTimeoutMilliseconds { get; init; } = 3000;

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(BrokerHost))
        {
            throw new InvalidDataException(
                "Broker:Host is required.");
        }

        if (BrokerPort is < 1 or > 65535)
        {
            throw new InvalidDataException(
                "Broker:Port must be between 1 and 65535.");
        }

        if (RequestTimeoutMilliseconds is < 100 or > 60000)
        {
            throw new InvalidDataException(
                "Broker:RequestTimeoutMilliseconds must be between 100 and 60000.");
        }
    }
}
