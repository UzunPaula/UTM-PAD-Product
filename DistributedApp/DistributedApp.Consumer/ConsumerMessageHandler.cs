using DistributedApp.Contracts;

namespace DistributedApp.Consumer;

public sealed class ConsumerMessageHandler
{
    private readonly MessageProcessor _messageProcessor;
    private readonly bool _simulateCrashBeforeAcknowledgement;
    private readonly int _processingDelayMilliseconds;
    private readonly bool _simulateNack;
    private bool _crashWasSimulated;

    public ConsumerMessageHandler(
        MessageProcessor messageProcessor,
        bool simulateCrashBeforeAcknowledgement = false,
        int processingDelayMilliseconds = 0,
        bool simulateNack = false)
    {
        _messageProcessor = messageProcessor;
        _simulateCrashBeforeAcknowledgement =
            simulateCrashBeforeAcknowledgement;
        _processingDelayMilliseconds = processingDelayMilliseconds;
        _simulateNack = simulateNack;
    }

    public async Task<Message> HandleAsync(
        Message message,
        CancellationToken cancellationToken = default)
    {
        EnsureResponseCanReference(message);

        MessageValidationResult validation =
            MessageValidator.Validate(message);

        if (!validation.IsValid)
        {
            return CreateResponse(
                message,
                MessageType.Nack,
                "Invalid message fields: " +
                string.Join(", ", validation.Errors));
        }

        if (message.Type != MessageType.Message)
        {
            return CreateResponse(
                message,
                MessageType.Nack,
                $"Unsupported message type: {message.Type}.");
        }

        if (_processingDelayMilliseconds > 0)
        {
            await Task.Delay(
                _processingDelayMilliseconds,
                cancellationToken);
        }

        if (_simulateNack)
        {
            return CreateResponse(
                message,
                MessageType.Nack,
                "Consumer rejection was enabled in external settings.");
        }

        ProcessingResult result = await _messageProcessor.ProcessAsync(
            message,
            cancellationToken);

        if (result.Status == ProcessingStatus.Failed)
        {
            return CreateResponse(
                message,
                MessageType.Nack,
                result.Error ?? "Message processing failed.");
        }

        if (result.Status == ProcessingStatus.Processed &&
            _simulateCrashBeforeAcknowledgement &&
            !_crashWasSimulated)
        {
            // Failure injection reproduces the critical interval after the
            // local effect is durable but before the broker receives Ack.
            _crashWasSimulated = true;
            throw new SimulatedConsumerCrashException(message.MessageId);
        }

        return CreateResponse(message, MessageType.Ack, reason: null);
    }

    private static void EnsureResponseCanReference(Message message)
    {
        if (message.MessageId == Guid.Empty ||
            message.CorrelationId == Guid.Empty)
        {
            throw new InvalidDataException(
                "The message and correlation identifiers are required.");
        }
    }

    private static Message CreateResponse(
        Message incoming,
        MessageType responseType,
        string? reason)
    {
        return new Message
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = incoming.CorrelationId,
            Type = responseType,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = incoming.Topic,
            SequenceNumber = incoming.SequenceNumber,
            RetryCount = 0,
            Payload = string.Empty,
            RelatedMessageId = incoming.MessageId,
            Reason = reason
        };
    }
}
