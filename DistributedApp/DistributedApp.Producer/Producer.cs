using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Producer;

public sealed class Producer(GrpcClientService grpc, ProducerSettings settings)
{
    public async Task<PublishResult> PublishOrderAsync(
        OrderPayload order, CancellationToken token, bool simulateLostResponse = false)
    {
        if (string.IsNullOrWhiteSpace(order.OrderId)
            || string.IsNullOrWhiteSpace(order.Product)
            || string.IsNullOrWhiteSpace(order.Description))
            return new PublishResult(false, Guid.Empty, Guid.Empty, "Completeaza corect comanda.",
                PublishOutcome.Rejected);

        Message message = new()
        {
            Type = MessageType.Publish,
            SenderId = settings.ProducerId,
            Topic = settings.ProducerId,
            Payload = JsonSerializer.Serialize(order)
        };

        PublishOutcome outcome = await grpc.SendAsync(message, simulateLostResponse, token);

        return outcome switch
        {
            PublishOutcome.Accepted => new PublishResult(true, message.MessageId, message.CorrelationId, null,
                PublishOutcome.Accepted),
            PublishOutcome.Duplicate => new PublishResult(true, message.MessageId, message.CorrelationId,
                "Mesajul fusese deja primit de broker (duplicat ignorat).", PublishOutcome.Duplicate),
            PublishOutcome.Rejected => new PublishResult(false, message.MessageId, message.CorrelationId,
                "Brokerul a respins mesajul.", PublishOutcome.Rejected),
            _ => new PublishResult(false, message.MessageId, message.CorrelationId,
                "Rezultat necunoscut: brokerul nu a raspuns, nu se stie daca mesajul a fost primit.",
                PublishOutcome.Unknown)
        };
    }
}

public sealed record PublishResult(
    bool Accepted, Guid MessageId, Guid CorrelationId, string? Reason, PublishOutcome Outcome);
