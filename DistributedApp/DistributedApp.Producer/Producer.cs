using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Producer;

public sealed class Producer(TcpClientService tcp, ProducerSettings settings)
{
    public async Task<PublishResult> PublishOrderAsync(OrderPayload order, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(order.OrderId) || string.IsNullOrWhiteSpace(order.Product) || order.Quantity < 1)
            return new PublishResult(false, Guid.Empty, Guid.Empty, "Completeaza corect comanda.");

        Message message = new()
        {
            Type = MessageType.Publish,
            SenderId = settings.ProducerId,
            Topic = settings.ProducerId,
            Payload = JsonSerializer.Serialize(order)
        };
        Message reply = await tcp.SendAsync(message, token);
        return new PublishResult(reply.Type == MessageType.Ack, message.MessageId, message.CorrelationId, reply.Reason);
    }
}

public sealed record PublishResult(bool Accepted, Guid MessageId, Guid CorrelationId, string? Reason);
