using System.Diagnostics;
using DistributedApp.Broker.Grpc.V1;
using DistributedApp.Contracts;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DistributedApp.Broker;

public sealed class BrokerGrpcService(BrokerServer broker, ILogger<BrokerGrpcService> logger)
    : BrokerRpcV1.BrokerRpcV1Base
{
    public const string ContractVersion = "1.0";

    public override async Task<PublishReply> Publish(PublishRequest request, ServerCallContext context)
    {
        Stopwatch timer = Stopwatch.StartNew();
        // Validarea și anularea se verifică înainte de orice modificare a stării.
        Message message = Validate(request);
        context.CancellationToken.ThrowIfCancellationRequested();

        bool duplicate;
        try
        {
            duplicate = broker.PublishIdempotent(message);
        }
        catch (InvalidOperationException exception)
        {
            throw new RpcException(new Status(StatusCode.AlreadyExists, exception.Message));
        }

        logger.LogInformation(
            "broker_publish result={Result} requestId={RequestId} correlationId={CorrelationId} messageId={MessageId} durationMs={DurationMs}",
            duplicate ? "duplicate" : "accepted", request.RequestId, request.CorrelationId,
            request.MessageId, timer.ElapsedMilliseconds);

        // Doar pentru test: mesajul este salvat, dar răspunsul se pierde la deadline.
        if (context.RequestHeaders.Any(x => x.Key == "x-simulate-lost-response" && x.Value == "true"))
        {
            logger.LogWarning(
                "broker_response_loss_injected requestId={RequestId} correlationId={CorrelationId} messageId={MessageId}",
                request.RequestId, request.CorrelationId, request.MessageId);
            await Task.Delay(Timeout.InfiniteTimeSpan, context.CancellationToken);
        }

        return new PublishReply
        {
            RequestId = request.RequestId,
            MessageId = request.MessageId,
            CorrelationId = request.CorrelationId,
            Accepted = true,
            Duplicate = duplicate,
            Status = "Accepted",
            SchemaVersion = ContractVersion
        };
    }

    public override Task<PublishStatusReply> GetPublishStatus(
        PublishStatusRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.MessageId, out Guid messageId))
            throw Error(StatusCode.InvalidArgument, "message_id must be a GUID.");

        context.CancellationToken.ThrowIfCancellationRequested();
        // Producerul folosește acest apel când nu știe dacă Publish a reușit.
        bool found = broker.ContainsPublished(messageId);
        return Task.FromResult(new PublishStatusReply
        {
            MessageId = request.MessageId,
            Found = found,
            Status = found ? "Accepted" : "NotFound",
            SchemaVersion = ContractVersion
        });
    }

    public override Task<HealthReply> CheckHealth(HealthRequest request, ServerCallContext context) =>
        Task.FromResult(new HealthReply
        {
            Status = "Healthy",
            Component = "Broker",
            SchemaVersion = ContractVersion,
            ObservedAtUtc = DateTimeOffset.UtcNow.ToString("O")
        });

    private static Message Validate(PublishRequest request)
    {
        if (request.SchemaVersion != ContractVersion || request.MessageType != "Publish")
            throw Error(StatusCode.FailedPrecondition, "Unsupported schema version or message type.");
        if (!Guid.TryParse(request.RequestId, out _) || !Guid.TryParse(request.MessageId, out Guid messageId)
            || !Guid.TryParse(request.CorrelationId, out Guid correlationId))
            throw Error(StatusCode.InvalidArgument, "Request identifiers must be GUIDs.");
        if (!DateTimeOffset.TryParse(request.OccurredAtUtc, out DateTimeOffset occurredAt)
            || occurredAt.Offset != TimeSpan.Zero || string.IsNullOrWhiteSpace(request.Topic)
            || string.IsNullOrWhiteSpace(request.Payload) || string.IsNullOrWhiteSpace(request.SenderId))
            throw Error(StatusCode.InvalidArgument, "UTC timestamp, topic, payload and sender_id are required.");

        return new Message
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            Type = MessageType.Publish,
            SenderId = request.SenderId,
            Topic = request.Topic,
            Payload = request.Payload
        };
    }

    private static RpcException Error(StatusCode code, string detail) => new(new Status(code, detail));
}
