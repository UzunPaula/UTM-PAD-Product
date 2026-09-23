using DistributedApp.Contracts;

namespace DistributedApp.Contracts.Tests;

public class MessageValidatorTests
{
    [Fact]
    public void Validate_AcceptsACompletePublishMessage()
    {
        Message message = CreateValidMessage();

        MessageValidationResult result = MessageValidator.Validate(message);

        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Validate_ReportsAllInvalidEnvelopeFields()
    {
        Message message = CreateValidMessage();
        message.MessageId = Guid.Empty;
        message.CorrelationId = Guid.Empty;
        message.SchemaVersion = "2.0";
        message.OccurredAtUtc =
            new DateTimeOffset(2026, 9, 23, 10, 30, 0, TimeSpan.FromHours(2));
        message.Topic = " ";
        message.SequenceNumber = -1;
        message.RetryCount = -1;
        message.Payload = string.Empty;

        MessageValidationResult result = MessageValidator.Validate(message);

        Assert.False(result.IsValid);
        Assert.Contains(nameof(Message.MessageId), result.Errors);
        Assert.Contains(nameof(Message.CorrelationId), result.Errors);
        Assert.Contains(nameof(Message.SchemaVersion), result.Errors);
        Assert.Contains(nameof(Message.OccurredAtUtc), result.Errors);
        Assert.Contains(nameof(Message.Topic), result.Errors);
        Assert.Contains(nameof(Message.SequenceNumber), result.Errors);
        Assert.Contains(nameof(Message.RetryCount), result.Errors);
        Assert.Contains(nameof(Message.Payload), result.Errors);
    }

    [Fact]
    public void Validate_RequiresRelatedMessageAndReasonForNack()
    {
        Message message = CreateValidMessage();
        message.Type = MessageType.Nack;
        message.Payload = string.Empty;
        message.RelatedMessageId = null;
        message.Reason = string.Empty;

        MessageValidationResult result = MessageValidator.Validate(message);

        Assert.False(result.IsValid);
        Assert.Contains(nameof(Message.RelatedMessageId), result.Errors);
        Assert.Contains(nameof(Message.Reason), result.Errors);
    }

    [Fact]
    public void Validate_AllowsSubscribeWithoutPayload()
    {
        Message message = CreateValidMessage();
        message.Type = MessageType.Subscribe;
        message.Payload = string.Empty;

        MessageValidationResult result = MessageValidator.Validate(message);

        Assert.True(result.IsValid);
    }

    private static Message CreateValidMessage()
    {
        return new Message
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Type = MessageType.Publish,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = "orders",
            SequenceNumber = 1,
            RetryCount = 0,
            Payload = "{}"
        };
    }
}
