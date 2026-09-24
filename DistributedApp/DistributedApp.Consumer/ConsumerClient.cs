using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Consumer;

public sealed class ConsumerClient
{
    private readonly string _host;
    private readonly int _port;
    private readonly ConsumerMessageHandler _messageHandler;

    public ConsumerClient(
        string host,
        int port,
        ConsumerMessageHandler messageHandler)
    {
        _host = host;
        _port = port;
        _messageHandler = messageHandler;
    }

    public async Task RunAsync(
        string topic,
        CancellationToken cancellationToken)
    {
        using TcpClient client = new();

        ConsumerLog.Write(
            "Information",
            "broker_connection_started",
            "started",
            detail: $"{_host}:{_port}");

        await client.ConnectAsync(_host, _port, cancellationToken);
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

        Message subscription = CreateSubscription(topic);
        await SendAsync(writer, subscription, cancellationToken);

        ConsumerLog.Write(
            "Information",
            "subscription_sent",
            "success",
            subscription);

        // The wire protocol uses one complete JSON envelope per TCP line.
        // This keeps message boundaries explicit on a continuous TCP stream.
        while (!cancellationToken.IsCancellationRequested)
        {
            string? line;

            try
            {
                line = await reader.ReadLineAsync(cancellationToken);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            if (line == null)
            {
                ConsumerLog.Write(
                    "Warning",
                    "broker_disconnected",
                    "connection_closed");
                return;
            }

            Message message;

            try
            {
                message = MessageJson.Deserialize(line);
            }
            catch (JsonException exception)
            {
                ConsumerLog.Write(
                    "Warning",
                    "message_rejected",
                    "invalid_json",
                    detail: exception.Message);
                continue;
            }

            Stopwatch stopwatch = Stopwatch.StartNew();

            ConsumerLog.Write(
                "Information",
                "message_received",
                "received",
                message);

            try
            {
                // The handler persists the local effect before it creates Ack.
                Message response = await _messageHandler.HandleAsync(
                    message,
                    cancellationToken);

                stopwatch.Stop();
                await SendAsync(writer, response, cancellationToken);

                ConsumerLog.Write(
                    response.Type == MessageType.Ack
                        ? "Information"
                        : "Warning",
                    response.Type == MessageType.Ack
                        ? "ack_sent"
                        : "nack_sent",
                    response.Type.ToString(),
                    response,
                    stopwatch.Elapsed.TotalMilliseconds,
                    response.Reason);
            }
            catch (SimulatedConsumerCrashException)
            {
                stopwatch.Stop();

                ConsumerLog.Write(
                    "Critical",
                    "consumer_crash_simulated",
                    "crashed_before_ack",
                    message,
                    stopwatch.Elapsed.TotalMilliseconds);

                throw;
            }
            catch (InvalidDataException exception)
            {
                stopwatch.Stop();

                ConsumerLog.Write(
                    "Warning",
                    "message_rejected",
                    "invalid_contract",
                    message,
                    stopwatch.Elapsed.TotalMilliseconds,
                    exception.Message);
            }
        }
    }

    private static Message CreateSubscription(string topic)
    {
        return new Message
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Type = MessageType.Subscribe,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = topic,
            SequenceNumber = 0,
            RetryCount = 0,
            Payload = string.Empty
        };
    }

    private static async Task SendAsync(
        StreamWriter writer,
        Message message,
        CancellationToken cancellationToken)
    {
        MessageValidationResult validation =
            MessageValidator.Validate(message);

        if (!validation.IsValid)
        {
            throw new InvalidDataException(
                "Cannot send an invalid message: " +
                string.Join(", ", validation.Errors));
        }

        string json = MessageJson.Serialize(message);
        await writer.WriteLineAsync(json.AsMemory(), cancellationToken);
    }
}
