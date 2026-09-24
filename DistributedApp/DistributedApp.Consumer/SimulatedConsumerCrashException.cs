namespace DistributedApp.Consumer;

public sealed class SimulatedConsumerCrashException : Exception
{
    public SimulatedConsumerCrashException(Guid messageId)
        : base(
            $"Simulated crash after local effect for message {messageId} " +
            "and before acknowledgement.")
    {
        MessageId = messageId;
    }

    public Guid MessageId { get; }
}
