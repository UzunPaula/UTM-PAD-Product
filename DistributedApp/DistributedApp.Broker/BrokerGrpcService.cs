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

    public override async Task SubscribeConsumer(
        SubscribeConsumerRequest request, IServerStreamWriter<ConsumerEvent> responseStream, ServerCallContext context)
    {
        if (request.SchemaVersion != ContractVersion)
            throw Error(StatusCode.FailedPrecondition, "Unsupported schema version.");
        if (!Guid.TryParse(request.RequestId, out Guid requestId)
            || string.IsNullOrWhiteSpace(request.ConsumerId) || string.IsNullOrWhiteSpace(request.Topic))
            throw Error(StatusCode.InvalidArgument, "request_id, consumer_id and topic are required.");

        Message subscription = new()
        {
            MessageId = requestId,
            CorrelationId = requestId,
            Type = MessageType.Subscribe,
            SenderId = request.ConsumerId,
            Topic = request.Topic
        };
        GrpcConsumerConnection connection = new(context.Peer);
        if (!broker.SubscribeGrpc(subscription, connection))
        {
            string reason = connection.RejectionReason ?? "Subscription was rejected.";
            connection.Dispose();
            throw Error(StatusCode.AlreadyExists, reason);
        }

        logger.LogInformation("broker_subscribe consumerId={ConsumerId} topic={Topic} sessionId={SessionId}",
            request.ConsumerId, request.Topic, connection.SessionId);
        try
        {
            // Primul eveniment confirmă abonarea; apoi vin doar livrările pentru acest Consumer.
            await foreach (Message message in connection.Messages.ReadAllAsync(context.CancellationToken))
            {
                ConsumerEvent item = message.Type switch
                {
                    MessageType.Ack => new ConsumerEvent
                    {
                        Subscribed = new SubscriptionAccepted
                        {
                            RequestId = request.RequestId,
                            SessionId = connection.SessionId,
                            SchemaVersion = ContractVersion
                        }
                    },
                    MessageType.Delivery => new ConsumerEvent
                    {
                        Delivery = new DeliveryMessage
                        {
                            MessageId = message.MessageId.ToString(),
                            CorrelationId = message.CorrelationId.ToString(),
                            SenderId = message.SenderId,
                            Topic = message.Topic,
                            Payload = message.Payload,
                            SchemaVersion = ContractVersion
                        }
                    },
                    _ => throw new InvalidOperationException("Unexpected consumer stream message.")
                };
                await responseStream.WriteAsync(item);
            }
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) { }
        finally
        {
            broker.DisconnectGrpc(request.ConsumerId, connection);
            logger.LogInformation("broker_unsubscribe consumerId={ConsumerId} sessionId={SessionId}",
                request.ConsumerId, connection.SessionId);
        }
    }

    public override Task<DeliveryAckReply> AcknowledgeDelivery(DeliveryAckRequest request, ServerCallContext context)
    {
        if (request.SchemaVersion != ContractVersion)
            throw Error(StatusCode.FailedPrecondition, "Unsupported schema version.");
        if (!Guid.TryParse(request.RequestId, out _)
            || !Guid.TryParse(request.MessageId, out Guid messageId)
            || !Guid.TryParse(request.CorrelationId, out Guid correlationId)
            || !Guid.TryParse(request.SessionId, out _)
            || string.IsNullOrWhiteSpace(request.ConsumerId))
            throw Error(StatusCode.InvalidArgument, "Request IDs, consumer_id and session_id are required.");
        if (!request.Accepted && string.IsNullOrWhiteSpace(request.Reason))
            throw Error(StatusCode.InvalidArgument, "A rejected delivery requires a reason.");

        context.CancellationToken.ThrowIfCancellationRequested();
        Message response = new()
        {
            Type = request.Accepted ? MessageType.Ack : MessageType.Nack,
            SenderId = request.ConsumerId,
            RelatedMessageId = messageId,
            CorrelationId = correlationId,
            Reason = request.Reason
        };
        DeliveryAckOutcome outcome = broker.CompleteGrpcDelivery(response, request.SessionId);
        if (outcome == DeliveryAckOutcome.InvalidSession)
            throw Error(StatusCode.FailedPrecondition, "Consumer session is no longer active.");
        if (outcome == DeliveryAckOutcome.InvalidCorrelation)
            throw Error(StatusCode.InvalidArgument, "correlation_id does not match the delivery.");
        if (outcome == DeliveryAckOutcome.NotFound)
            throw Error(StatusCode.NotFound, "Delivery was not found for this consumer.");
        if (outcome == DeliveryAckOutcome.NotWaiting)
            throw Error(StatusCode.FailedPrecondition, "Delivery is not waiting for acknowledgement.");

        bool duplicate = outcome == DeliveryAckOutcome.AlreadyAcknowledged;
        logger.LogInformation("broker_delivery_ack consumerId={ConsumerId} messageId={MessageId} result={Result}",
            request.ConsumerId, request.MessageId, duplicate ? "duplicate" : request.Accepted ? "ack" : "nack");
        return Task.FromResult(new DeliveryAckReply
        {
            Duplicate = duplicate,
            SchemaVersion = ContractVersion
        });
    }

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
