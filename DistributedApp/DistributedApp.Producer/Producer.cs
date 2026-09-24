using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Producer;

public sealed class Producer
{
    private readonly TcpClientService _tcpClient;

    public Producer(TcpClientService tcpClient)
    {
        _tcpClient = tcpClient;
    }

    public async Task<PublishResult> PublishOrderAsync(
        OrderPayload order,
        CancellationToken cancellationToken = default)
    {
        string? validationError = ValidateOrder(order);
        if (validationError is not null)
        {
            throw new ArgumentException(validationError, nameof(order));
        }

        Message message = new()
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Type = MessageType.Publish,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = "orders",
            SequenceNumber = 0,
            RetryCount = 0,
            Payload = JsonSerializer.Serialize(order)
        };

        Message response = await _tcpClient.SendAsync(
            message,
            cancellationToken);

        if (response.RelatedMessageId != message.MessageId)
        {
            throw new InvalidDataException(
                "Răspunsul Brokerului nu corespunde mesajului trimis.");
        }

        return response.Type switch
        {
            MessageType.Ack => new PublishResult(
                true,
                message.MessageId,
                message.CorrelationId,
                null),
            MessageType.Nack => new PublishResult(
                false,
                message.MessageId,
                message.CorrelationId,
                response.Reason ?? "Brokerul a respins mesajul."),
            _ => throw new InvalidDataException(
                $"Răspuns neașteptat de la Broker: {response.Type}.")
        };
    }

    public static string? ValidateOrder(OrderPayload order)
    {
        ArgumentNullException.ThrowIfNull(order);

        if (string.IsNullOrWhiteSpace(order.OrderId)
            || order.OrderId.Length > 64)
        {
            return "ID-ul comenzii trebuie să conțină între 1 și 64 de caractere.";
        }

        if (string.IsNullOrWhiteSpace(order.Product)
            || order.Product.Length > 100)
        {
            return "Produsul trebuie să conțină între 1 și 100 de caractere.";
        }

        return order.Quantity is < 1 or > 1000
            ? "Cantitatea trebuie să fie între 1 și 1000."
            : null;
    }
}

public sealed record PublishResult(
    bool Accepted,
    Guid MessageId,
    Guid CorrelationId,
    string? Reason);
