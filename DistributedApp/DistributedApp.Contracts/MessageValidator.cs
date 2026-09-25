namespace DistributedApp.Contracts;

public static class MessageValidator
{
    public static MessageValidationResult Validate(Message message)
    {
        ArgumentNullException.ThrowIfNull(message);

        List<string> errors = new();

        if (message.MessageId == Guid.Empty)
        {
            errors.Add(nameof(Message.MessageId));
        }

        if (message.CorrelationId == Guid.Empty)
        {
            errors.Add(nameof(Message.CorrelationId));
        }

        if (!Enum.IsDefined(message.Type))
        {
            errors.Add(nameof(Message.Type));
        }

        if (!MessageSchema.IsSupported(message.SchemaVersion))
        {
            errors.Add(nameof(Message.SchemaVersion));
        }

        if (message.OccurredAtUtc == default ||
            message.OccurredAtUtc.Offset != TimeSpan.Zero)
        {
            errors.Add(nameof(Message.OccurredAtUtc));
        }

        if (string.IsNullOrWhiteSpace(message.Topic))
        {
            errors.Add(nameof(Message.Topic));
        }

        if (message.SequenceNumber < 0)
        {
            errors.Add(nameof(Message.SequenceNumber));
        }

        if (message.RetryCount < 0)
        {
            errors.Add(nameof(Message.RetryCount));
        }

        if (RequiresPayload(message.Type) &&
            string.IsNullOrWhiteSpace(message.Payload))
        {
            errors.Add(nameof(Message.Payload));
        }

        if (RequiresRelatedMessage(message.Type) &&
            (!message.RelatedMessageId.HasValue ||
             message.RelatedMessageId.Value == Guid.Empty))
        {
            errors.Add(nameof(Message.RelatedMessageId));
        }

        if (message.Type == MessageType.Nack &&
            string.IsNullOrWhiteSpace(message.Reason))
        {
            errors.Add(nameof(Message.Reason));
        }

        return new MessageValidationResult(errors);
    }

    private static bool RequiresPayload(MessageType type)
    {
        return type is MessageType.Publish
            or MessageType.Message
            or MessageType.StatusResponse;
    }

    private static bool RequiresRelatedMessage(MessageType type)
    {
        return type is MessageType.Ack
            or MessageType.Nack
            or MessageType.StatusResponse
            or MessageType.RedriveRequest;
    }
}
