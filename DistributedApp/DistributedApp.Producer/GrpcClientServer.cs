using DistributedApp.Broker.Grpc.V1;
using DistributedApp.Contracts;
using Grpc.Core;
using Grpc.Net.Client;

namespace DistributedApp.Producer;

public enum PublishOutcome { Accepted, Duplicate, Rejected, Unknown }

public sealed class GrpcClientService : IDisposable
{
    private const string SchemaVersion = "1.0";

    private readonly ProducerSettings _settings;
    private readonly GrpcChannel _channel;
    private readonly BrokerRpcV1.BrokerRpcV1Client _client;

    public GrpcClientService(ProducerSettings settings)
    {
        _settings = settings;
        _channel = GrpcChannel.ForAddress(settings.BrokerAddress);
        _client = new BrokerRpcV1.BrokerRpcV1Client(_channel);
    }

    public async Task<PublishOutcome> SendAsync(
        Message message, bool simulateLostResponse = false, CancellationToken ct = default)
    {
        PublishRequest request = new()
        {
            RequestId = Guid.NewGuid().ToString(),
            MessageId = message.MessageId.ToString(),        // același la retry (idempotență)
            CorrelationId = message.CorrelationId.ToString(),
            MessageType = "Publish",
            SchemaVersion = SchemaVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow.ToString("O"),
            Topic = message.Topic,
            Payload = message.Payload,
            SenderId = message.SenderId
        };

        Metadata? headers = null;
        if (simulateLostResponse)
            headers = new Metadata { { "x-simulate-lost-response", "true" } };

        try
        {
            PublishReply reply = await _client.PublishAsync(request, headers, DeadlineFromNow(), ct);

            if (reply.Duplicate) return PublishOutcome.Duplicate;
            return reply.Accepted ? PublishOutcome.Accepted : PublishOutcome.Rejected;
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.DeadlineExceeded
                                                     or StatusCode.Unavailable)
        {
            // Rezultat ambiguu: serverul poate a salvat mesajul, dar răspunsul s-a pierdut.
            return await ResolveAmbiguityAsync(request, ct);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.InvalidArgument
                                                     or StatusCode.FailedPrecondition
                                                     or StatusCode.AlreadyExists)
        {
            // Eroare de contract sau conflict: retry nu ajută.
            return PublishOutcome.Rejected;
        }
    }

    private async Task<PublishOutcome> ResolveAmbiguityAsync(PublishRequest request, CancellationToken ct)
    {
        try
        {
            // Pasul 1: întrebăm brokerul dacă a primit mesajul.
            PublishStatusReply status = await _client.GetPublishStatusAsync(
                new PublishStatusRequest { MessageId = request.MessageId },
                deadline: DeadlineFromNow(), cancellationToken: ct);

            if (status.Found) return PublishOutcome.Accepted;

            // Pasul 2: nu există, deci retry sigur cu același message_id (fără headerul de test).
            PublishReply retry = await _client.PublishAsync(
                request, deadline: DeadlineFromNow(), cancellationToken: ct);

            if (retry.Duplicate) return PublishOutcome.Duplicate;
            return retry.Accepted ? PublishOutcome.Accepted : PublishOutcome.Rejected;
        }
        catch (RpcException)
        {
            // Nici verificarea nu a reușit: rezultatul rămâne explicit necunoscut.
            return PublishOutcome.Unknown;
        }
    }

    private DateTime DeadlineFromNow() =>
        DateTime.UtcNow.AddMilliseconds(_settings.RequestTimeoutMilliseconds);

    public void Dispose() => _channel.Dispose();
}