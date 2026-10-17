using DistributedApp.Broker.Grpc.V1;
using DistributedApp.Consumer;
using DistributedApp.Contracts;
using Grpc.Core;
using Grpc.Net.Client;

if (args.Length == 0 || !ConsumerStateFiles.IsValidConsumerId(args[0]))
{
    Console.Error.WriteLine("Usage: consumer <consumerId> [topic] [fail|drop-ack-once]");
    Console.Error.WriteLine("consumerId is required and may contain letters, digits, '.', '_' and '-'.");
    Environment.ExitCode = 2;
    return;
}

string consumerId = args[0];
string topic = args.ElementAtOrDefault(1) ?? "producer-1";
string mode = args.ElementAtOrDefault(2) ?? string.Empty;
bool rejectMessages = mode.Equals("fail", StringComparison.OrdinalIgnoreCase);
bool dropAckOnce = mode.Equals("drop-ack-once", StringComparison.OrdinalIgnoreCase);
string brokerAddress = Environment.GetEnvironmentVariable("CONSUMER_BROKER_GRPC_ADDRESS")
    ?? "http://127.0.0.1:5001";
string stateFile;
FileStream stateLock;
try
{
    stateFile = ConsumerStateFiles.ResolvePath(
        consumerId, Environment.GetEnvironmentVariable("CONSUMER_STATE_FILE"));
    stateLock = ConsumerStateFiles.AcquireExclusiveLock(stateFile);
}
catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or ArgumentException)
{
    Console.Error.WriteLine($"Cannot open Consumer state: {exception.Message}");
    Environment.ExitCode = 2;
    return;
}

using FileStream heldStateLock = stateLock;
ProcessedMessageStore store = new(stateFile);
using GrpcChannel channel = GrpcChannel.ForAddress(brokerAddress);
BrokerRpcV1.BrokerRpcV1Client broker = new(channel);

using CancellationTokenSource stop = new();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    stop.Cancel();
};

Console.WriteLine($"CONSUMER ID: {consumerId}");
Console.WriteLine($"SUBSCRIBED TOPIC: {topic}");
Console.WriteLine($"BROKER: {brokerAddress}");
Console.WriteLine($"STATE FILE: {Path.GetFullPath(stateFile)}");
Console.WriteLine(rejectMessages ? "MODE: NACK" : dropAckOnce ? "MODE: DROP FIRST ACK" : "MODE: ACK");

while (!stop.IsCancellationRequested)
{
    try
    {
        if (!await RunSubscriptionAsync(stop.Token))
        {
            Environment.ExitCode = 2;
            break;
        }
        Console.WriteLine("Broker disconnected. Reconnecting...");
    }
    catch (OperationCanceledException) when (stop.IsCancellationRequested)
    {
        break;
    }
    catch (RpcException exception) when (exception.StatusCode == StatusCode.Cancelled && stop.IsCancellationRequested)
    {
        break;
    }
    catch (Exception exception) when (exception is RpcException or IOException)
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

async Task<bool> RunSubscriptionAsync(CancellationToken cancellationToken)
{
    SubscribeConsumerRequest request = new()
    {
        RequestId = Guid.NewGuid().ToString(),
        ConsumerId = consumerId,
        Topic = topic,
        SchemaVersion = "1.0"
    };
    using AsyncServerStreamingCall<ConsumerEvent> stream = broker.SubscribeConsumer(request,
        cancellationToken: cancellationToken);

    bool hasFirstEvent;
    try
    {
        hasFirstEvent = await stream.ResponseStream.MoveNext(cancellationToken);
    }
    catch (RpcException exception) when (exception.StatusCode is StatusCode.AlreadyExists
                                      or StatusCode.InvalidArgument or StatusCode.FailedPrecondition)
    {
        Console.Error.WriteLine($"SUBSCRIBE REJECTED: {exception.Status.Detail}");
        return false;
    }

    if (!hasFirstEvent || stream.ResponseStream.Current.EventCase != ConsumerEvent.EventOneofCase.Subscribed)
        throw new IOException("Broker disconnected before confirming the subscription.");

    SubscriptionAccepted confirmation = stream.ResponseStream.Current.Subscribed;
    if (confirmation.RequestId != request.RequestId || confirmation.SchemaVersion != request.SchemaVersion
        || !Guid.TryParse(confirmation.SessionId, out _))
        throw new IOException("Broker sent an invalid subscription confirmation.");

    string sessionId = confirmation.SessionId;
    Console.WriteLine($"SUBSCRIBED: {consumerId} -> {topic}");

    while (await stream.ResponseStream.MoveNext(cancellationToken))
    {
        ConsumerEvent item = stream.ResponseStream.Current;
        if (item.EventCase != ConsumerEvent.EventOneofCase.Delivery)
            throw new IOException("Broker sent an unexpected consumer event.");

        DeliveryMessage message = item.Delivery;
        if (message.SchemaVersion != request.SchemaVersion)
            throw new IOException("Broker sent an unsupported delivery schema version.");
        if (!Guid.TryParse(message.MessageId, out Guid messageId)
            || !Guid.TryParse(message.CorrelationId, out Guid correlationId))
            throw new IOException("Broker sent a delivery with invalid identifiers.");

        ProcessingResult result;
        if (message.Topic != topic)
            result = ProcessingResult.Failed("Delivery topic is invalid.");
        else if (rejectMessages)
            result = ProcessingResult.Failed("Consumer started in FAIL mode.");
        else
        {
            try
            {
                result = await store.ProcessAsync(messageId, message.Payload, cancellationToken);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                result = ProcessingResult.Failed($"Could not save order: {exception.Message}");
            }
        }

        Console.WriteLine($"RECEIVED: {messageId} | {message.Payload} | "
            + (result.NewlyProcessed ? "processed" : result.Accepted ? "already processed" : "rejected"));

        if (dropAckOnce && result.NewlyProcessed)
        {
            dropAckOnce = false;
            Console.WriteLine("SIMULATED ACK LOSS: order saved; broker will retry this delivery.");
            continue;
        }

        DeliveryAckReply reply = await broker.AcknowledgeDeliveryAsync(new DeliveryAckRequest
        {
            RequestId = Guid.NewGuid().ToString(),
            MessageId = messageId.ToString(),
            CorrelationId = correlationId.ToString(),
            ConsumerId = consumerId,
            SessionId = sessionId,
            Accepted = result.Accepted,
            Reason = result.Reason ?? string.Empty,
            SchemaVersion = request.SchemaVersion
        }, deadline: DateTime.UtcNow.AddSeconds(3), cancellationToken: cancellationToken);
        if (reply.SchemaVersion != request.SchemaVersion)
            throw new IOException("Broker sent an unsupported acknowledgement schema version.");

        Console.WriteLine(result.Accepted ? "ACK sent" : $"NACK sent: {result.Reason}");
    }

    return true;
}
