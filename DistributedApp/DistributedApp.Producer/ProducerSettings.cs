namespace DistributedApp.Producer;

public sealed class ProducerSettings
{
    public string ProducerId { get; init; } = "producer-1";
    public string BrokerHost { get; init; } = "127.0.0.1";
    public int BrokerPort { get; init; } = 5000;
    public string BrokerAddress { get; init; } = "http://127.0.0.1:5001";
    public int RequestTimeoutMilliseconds { get; init; } = 3000;
}