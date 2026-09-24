using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Producer;

public sealed class TcpClientService
{
    private static readonly JsonSerializerOptions StatusJsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly ProducerSettings _settings;

    public TcpClientService(ProducerSettings settings)
    {
        _settings = settings;
    }

    public async Task<Message> SendAsync(
        Message message,
        CancellationToken cancellationToken = default)
    {
        using CancellationTokenSource timeout =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_settings.RequestTimeoutMilliseconds);

        using TcpClient client = new();
        try
        {
            await client.ConnectAsync(
                _settings.BrokerHost,
                _settings.BrokerPort,
                timeout.Token);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                "Conectarea la Broker a depășit timpul permis.");
        }

        await using NetworkStream stream = client.GetStream();
        using StreamReader reader = new(
            stream,
            Encoding.UTF8,
            detectEncodingFromByteOrderMarks: false,
            leaveOpen: true);
        await using StreamWriter writer = new(
            stream,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            leaveOpen: true)
        {
            AutoFlush = true,
            NewLine = "\n"
        };

        await writer.WriteLineAsync(
            MessageJson.Serialize(message).AsMemory(),
            timeout.Token);

        string? responseLine;
        try
        {
            responseLine = await reader.ReadLineAsync(timeout.Token);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                "Brokerul nu a răspuns în timpul permis.");
        }

        if (responseLine is null)
        {
            throw new IOException(
                "Brokerul a închis conexiunea fără un răspuns.");
        }

        return MessageJson.Deserialize(responseLine);
    }

    public async Task<bool> CanConnectAsync(
        CancellationToken cancellationToken = default)
    {
        using CancellationTokenSource timeout =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_settings.RequestTimeoutMilliseconds);

        try
        {
            using TcpClient client = new();
            await client.ConnectAsync(
                _settings.BrokerHost,
                _settings.BrokerPort,
                timeout.Token);
            return true;
        }
        catch (Exception exception) when (
            exception is SocketException or OperationCanceledException)
        {
            return false;
        }
    }

    public async Task<BrokerStatusSnapshot> GetBrokerStatusAsync(
        string topic,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        Message request = new()
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Type = MessageType.StatusRequest,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = topic
        };

        Message response = await SendAsync(request, cancellationToken);

        if (response.Type != MessageType.StatusResponse
            || response.RelatedMessageId != request.MessageId
            || response.CorrelationId != request.CorrelationId)
        {
            throw new InvalidDataException(
                "Răspunsul de stare al Brokerului nu corespunde cererii.");
        }

        BrokerStatusSnapshot status =
            JsonSerializer.Deserialize<BrokerStatusSnapshot>(
                response.Payload,
                StatusJsonOptions)
            ?? throw new InvalidDataException(
                "Brokerul a returnat o stare goală.");

        if (!string.Equals(status.Topic, topic, StringComparison.Ordinal)
            || status.PendingMessages < 0
            || status.InFlightMessages < 0
            || status.DeadLetterMessages < 0
            || status.AcknowledgedMessages < 0)
        {
            throw new InvalidDataException(
                "Brokerul a returnat valori de stare invalide.");
        }

        status.DeadLetters ??= new List<DeadLetterSummary>();
        status.RecentAcknowledgements ??=
            new List<AcknowledgementSummary>();
        return status;
    }
}
