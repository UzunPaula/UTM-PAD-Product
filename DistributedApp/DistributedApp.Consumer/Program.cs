using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using DistributedApp.Consumer;
using DistributedApp.Contracts;

string consumerId = args.ElementAtOrDefault(0) ?? "consumer-1";
string topic = args.ElementAtOrDefault(1) ?? "producer-1";
string mode = args.ElementAtOrDefault(2) ?? string.Empty;
bool rejectMessages = mode.Equals("fail", StringComparison.OrdinalIgnoreCase);
bool dropAckOnce = mode.Equals("drop-ack-once", StringComparison.OrdinalIgnoreCase);
string brokerHost = Environment.GetEnvironmentVariable("CONSUMER_BROKER_HOST") ?? "127.0.0.1";
int brokerPort = int.TryParse(Environment.GetEnvironmentVariable("CONSUMER_BROKER_PORT"), out int configuredPort)
    && configuredPort is > 0 and <= 65535 ? configuredPort : 5000;
string stateFile = Environment.GetEnvironmentVariable("CONSUMER_STATE_FILE")
    ?? Path.Combine(Directory.GetCurrentDirectory(), "data",
        $"consumer-{Uri.EscapeDataString(consumerId)}.json");
ProcessedMessageStore store = new(stateFile);

using CancellationTokenSource stop = new();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    stop.Cancel();
};

Console.WriteLine($"CONSUMER ID: {consumerId}");
Console.WriteLine($"SUBSCRIBED TOPIC: {topic}");
Console.WriteLine($"BROKER: {brokerHost}:{brokerPort}");
Console.WriteLine($"STATE FILE: {Path.GetFullPath(stateFile)}");
Console.WriteLine(rejectMessages ? "MODE: NACK" : dropAckOnce ? "MODE: DROP FIRST ACK" : "MODE: ACK");

while (!stop.IsCancellationRequested)
{
    try
    {
        await RunConnectionAsync(stop.Token);
        Console.WriteLine("Broker disconnected. Reconnecting...");
    }
    catch (OperationCanceledException) when (stop.IsCancellationRequested)
    {
        break;
    }
    catch (Exception exception) when (exception is SocketException or IOException)
    {
        Console.WriteLine($"Broker unavailable: {exception.Message}");
    }

    try
    {
        await Task.Delay(1000, stop.Token);
    }
    catch (OperationCanceledException)
    {
        break;
    }
}

async Task RunConnectionAsync(CancellationToken cancellationToken)
{
    using TcpClient client = new();
    await client.ConnectAsync(brokerHost, brokerPort, cancellationToken);
    Console.WriteLine($"CONNECTED: {client.Client.LocalEndPoint}");

    await using NetworkStream stream = client.GetStream();
    using StreamReader reader = new(stream, Encoding.UTF8, false, leaveOpen: true);
    await using StreamWriter writer = new(stream, new UTF8Encoding(false), leaveOpen: true)
    {
        AutoFlush = true,
        NewLine = "\n"
    };

    Message subscription = new()
    {
        Type = MessageType.Subscribe,
        SenderId = consumerId,
        Topic = topic
    };
    await writer.WriteLineAsync(MessageJson.Serialize(subscription).AsMemory(), cancellationToken);
    Console.WriteLine($"SUBSCRIBED: {consumerId} -> {topic}");

    while (await reader.ReadLineAsync(cancellationToken) is { } line)
    {
        Message message;
        try
        {
            message = MessageJson.Deserialize<Message>(line);
        }
        catch (JsonException exception)
        {
            Console.WriteLine($"INVALID JSON: {exception.Message}");
            continue;
        }

        if (message.Type != MessageType.Delivery) continue;
        if (message.MessageId == Guid.Empty) continue;

        ProcessingResult result;
        if (message.Topic != topic || message.CorrelationId == Guid.Empty)
            result = ProcessingResult.Failed("Delivery topic or CorrelationId is invalid.");
        else if (rejectMessages)
            result = ProcessingResult.Failed("Consumer started in FAIL mode.");
        else
        {
            try
            {
                result = await store.ProcessAsync(message.MessageId, message.Payload, cancellationToken);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                result = ProcessingResult.Failed($"Could not save order: {exception.Message}");
            }
        }

        Console.WriteLine($"RECEIVED: {message.MessageId} | {message.Payload} | "
            + (result.NewlyProcessed ? "processed" : result.Accepted ? "already processed" : "rejected"));

        if (dropAckOnce && result.NewlyProcessed)
        {
            dropAckOnce = false;
            Console.WriteLine("SIMULATED ACK LOSS: order saved; broker will retry this delivery.");
            continue;
        }

        Message response = new()
        {
            Type = result.Accepted ? MessageType.Ack : MessageType.Nack,
            SenderId = consumerId,
            Topic = message.Topic,
            RelatedMessageId = message.MessageId,
            CorrelationId = message.CorrelationId,
            Reason = result.Reason
        };
        await writer.WriteLineAsync(MessageJson.Serialize(response).AsMemory(), cancellationToken);
        Console.WriteLine(result.Accepted ? "ACK sent" : $"NACK sent: {result.Reason}");
    }
}
