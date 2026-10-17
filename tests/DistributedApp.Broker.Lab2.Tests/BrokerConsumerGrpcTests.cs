using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using DistributedApp.Broker;
using DistributedApp.Broker.Grpc.V1;
using DistributedApp.Consumer;
using DistributedApp.Contracts;
using Grpc.Core;
using Grpc.Net.Client;
using Xunit;

namespace DistributedApp.Broker.Lab2.Tests;

public sealed class BrokerConsumerGrpcTests
{
    [Fact]
    public async Task SubscriptionIsConfirmedBeforeDeliveryAndAckIsIdempotent()
    {
        int grpcPort = FreePort();
        string stateFile = NewStateFile();
        using BrokerProcess broker = StartBroker(grpcPort, FreePort(), stateFile);
        using GrpcChannel channel = GrpcChannel.ForAddress($"http://127.0.0.1:{grpcPort}");
        BrokerRpcV1.BrokerRpcV1Client client = new(channel);
        await WaitUntilHealthy(client);

        PublishRequest publication = Publication();
        Assert.True((await client.PublishAsync(publication)).Accepted);

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        SubscribeConsumerRequest subscription = Subscription("consumer-1");
        using AsyncServerStreamingCall<ConsumerEvent> stream = client.SubscribeConsumer(
            subscription, cancellationToken: timeout.Token);

        Assert.True(await stream.ResponseStream.MoveNext(timeout.Token));
        SubscriptionAccepted confirmation = Assert.IsType<SubscriptionAccepted>(stream.ResponseStream.Current.Subscribed);
        Assert.Equal(subscription.RequestId, confirmation.RequestId);
        Assert.False(string.IsNullOrWhiteSpace(confirmation.SessionId));

        Assert.True(await stream.ResponseStream.MoveNext(timeout.Token));
        DeliveryMessage delivery = Assert.IsType<DeliveryMessage>(stream.ResponseStream.Current.Delivery);
        Assert.Equal(publication.MessageId, delivery.MessageId);
        Assert.Equal(publication.Payload, delivery.Payload);

        DeliveryAckRequest ack = new()
        {
            RequestId = Guid.NewGuid().ToString(),
            MessageId = delivery.MessageId,
            CorrelationId = delivery.CorrelationId,
            ConsumerId = subscription.ConsumerId,
            SessionId = confirmation.SessionId,
            Accepted = true,
            SchemaVersion = "1.0"
        };
        DeliveryAckReply first = await client.AcknowledgeDeliveryAsync(ack);
        DeliveryAckReply repeated = await client.AcknowledgeDeliveryAsync(ack);

        Assert.False(first.Duplicate);
        Assert.True(repeated.Duplicate);
        BrokerState state = MessageJson.Deserialize<BrokerState>(File.ReadAllText(stateFile));
        Assert.Single(state.Acknowledgements);
        Assert.Equal(DeliveryStatus.Acknowledged, Assert.Single(state.Deliveries).Status);
    }

    [Fact]
    public async Task SecondActiveSubscriptionWithSameConsumerIdIsRejected()
    {
        int grpcPort = FreePort();
        using BrokerProcess broker = StartBroker(grpcPort, FreePort(), NewStateFile());
        using GrpcChannel channel = GrpcChannel.ForAddress($"http://127.0.0.1:{grpcPort}");
        BrokerRpcV1.BrokerRpcV1Client client = new(channel);
        await WaitUntilHealthy(client);

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        using AsyncServerStreamingCall<ConsumerEvent> first = client.SubscribeConsumer(
            Subscription("consumer-1"), cancellationToken: timeout.Token);
        Assert.True(await first.ResponseStream.MoveNext(timeout.Token));
        Assert.NotNull(first.ResponseStream.Current.Subscribed);

        using AsyncServerStreamingCall<ConsumerEvent> second = client.SubscribeConsumer(
            Subscription("consumer-1"), cancellationToken: timeout.Token);
        RpcException error = await Assert.ThrowsAsync<RpcException>(async () =>
            await second.ResponseStream.MoveNext(timeout.Token));
        Assert.Equal(StatusCode.AlreadyExists, error.StatusCode);
    }

    [Fact]
    public async Task NackSchedulesAnotherDeliveryAndStaleSessionCannotAcknowledge()
    {
        int grpcPort = FreePort();
        string stateFile = NewStateFile();
        using BrokerProcess broker = StartBroker(grpcPort, FreePort(), stateFile);
        using GrpcChannel channel = GrpcChannel.ForAddress($"http://127.0.0.1:{grpcPort}");
        BrokerRpcV1.BrokerRpcV1Client client = new(channel);
        await WaitUntilHealthy(client);

        PublishRequest publication = Publication();
        await client.PublishAsync(publication);
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        using AsyncServerStreamingCall<ConsumerEvent> stream = client.SubscribeConsumer(
            Subscription("consumer-1"), cancellationToken: timeout.Token);
        Assert.True(await stream.ResponseStream.MoveNext(timeout.Token));
        string sessionId = stream.ResponseStream.Current.Subscribed.SessionId;
        Assert.True(await stream.ResponseStream.MoveNext(timeout.Token));
        DeliveryMessage firstDelivery = stream.ResponseStream.Current.Delivery;

        DeliveryAckRequest nack = new()
        {
            RequestId = Guid.NewGuid().ToString(),
            MessageId = firstDelivery.MessageId,
            CorrelationId = firstDelivery.CorrelationId,
            ConsumerId = "consumer-1",
            SessionId = Guid.NewGuid().ToString(),
            Accepted = false,
            Reason = "Temporary processing error.",
            SchemaVersion = "1.0"
        };
        RpcException wrongSession = await Assert.ThrowsAsync<RpcException>(async () =>
            await client.AcknowledgeDeliveryAsync(nack));
        Assert.Equal(StatusCode.FailedPrecondition, wrongSession.StatusCode);

        nack.SessionId = sessionId;
        Assert.False((await client.AcknowledgeDeliveryAsync(nack)).Duplicate);
        Assert.True(await stream.ResponseStream.MoveNext(timeout.Token));
        Assert.Equal(firstDelivery.MessageId, stream.ResponseStream.Current.Delivery.MessageId);

        nack.RequestId = Guid.NewGuid().ToString();
        nack.Accepted = true;
        nack.Reason = string.Empty;
        Assert.False((await client.AcknowledgeDeliveryAsync(nack)).Duplicate);
        BrokerState state = MessageJson.Deserialize<BrokerState>(File.ReadAllText(stateFile));
        Assert.Equal(DeliveryStatus.Acknowledged, Assert.Single(state.Deliveries).Status);
    }

    [Fact]
    public async Task LostAckRedeliversButConsumerEffectIsStoredOnlyOnce()
    {
        int grpcPort = FreePort();
        string stateFile = NewStateFile();
        using BrokerProcess broker = StartBroker(grpcPort, FreePort(), stateFile);
        using GrpcChannel channel = GrpcChannel.ForAddress($"http://127.0.0.1:{grpcPort}");
        BrokerRpcV1.BrokerRpcV1Client client = new(channel);
        await WaitUntilHealthy(client);

        PublishRequest publication = Publication();
        publication.Payload = MessageJson.Serialize(new OrderPayload("ORDER-ACK-LOSS", "Laptop", "Test"));
        await client.PublishAsync(publication);
        string consumerStateFile = Path.Combine(Path.GetTempPath(),
            "consumer-grpc-effect-test-" + Guid.NewGuid().ToString("N"), "state.json");
        ProcessedMessageStore store = new(consumerStateFile);

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        using AsyncServerStreamingCall<ConsumerEvent> stream = client.SubscribeConsumer(
            Subscription("consumer-1"), cancellationToken: timeout.Token);
        Assert.True(await stream.ResponseStream.MoveNext(timeout.Token));
        string sessionId = stream.ResponseStream.Current.Subscribed.SessionId;
        Assert.True(await stream.ResponseStream.MoveNext(timeout.Token));
        DeliveryMessage first = stream.ResponseStream.Current.Delivery;
        ProcessingResult firstResult = await store.ProcessAsync(Guid.Parse(first.MessageId), first.Payload, timeout.Token);
        Assert.True(firstResult.NewlyProcessed);

        // Nu trimitem ACK-ul: Brokerul va retransmite după timeout.
        Assert.True(await stream.ResponseStream.MoveNext(timeout.Token));
        DeliveryMessage repeated = stream.ResponseStream.Current.Delivery;
        Assert.Equal(first.MessageId, repeated.MessageId);
        ProcessingResult repeatedResult = await store.ProcessAsync(
            Guid.Parse(repeated.MessageId), repeated.Payload, timeout.Token);
        Assert.True(repeatedResult.Accepted);
        Assert.False(repeatedResult.NewlyProcessed);

        await client.AcknowledgeDeliveryAsync(new DeliveryAckRequest
        {
            RequestId = Guid.NewGuid().ToString(),
            MessageId = repeated.MessageId,
            CorrelationId = repeated.CorrelationId,
            ConsumerId = "consumer-1",
            SessionId = sessionId,
            Accepted = true,
            SchemaVersion = "1.0"
        });

        BrokerState brokerState = MessageJson.Deserialize<BrokerState>(File.ReadAllText(stateFile));
        Assert.Single(brokerState.Acknowledgements);
        Assert.Equal(DeliveryStatus.Acknowledged, Assert.Single(brokerState.Deliveries).Status);
        Assert.Single(MessageJson.Deserialize<Dictionary<Guid, OrderPayload>>(File.ReadAllText(consumerStateFile)));
    }

    private static SubscribeConsumerRequest Subscription(string consumerId) => new()
    {
        RequestId = Guid.NewGuid().ToString(),
        ConsumerId = consumerId,
        Topic = "producer-1",
        SchemaVersion = "1.0"
    };

    private static PublishRequest Publication() => new()
    {
        RequestId = Guid.NewGuid().ToString(),
        MessageId = Guid.NewGuid().ToString(),
        CorrelationId = Guid.NewGuid().ToString(),
        MessageType = "Publish",
        SchemaVersion = "1.0",
        OccurredAtUtc = DateTimeOffset.UtcNow.ToString("O"),
        Topic = "producer-1",
        Payload = "test-order",
        SenderId = "producer-1"
    };

    private static string NewStateFile() => Path.Combine(
        Path.GetTempPath(), "broker-consumer-grpc-test-" + Guid.NewGuid().ToString("N"), "state.json");

    private static BrokerProcess StartBroker(int grpcPort, int tcpPort, string stateFile)
    {
        ProcessStartInfo start = new("dotnet", $"\"{typeof(BrokerServer).Assembly.Location}\"")
        {
            UseShellExecute = false,
            CreateNoWindow = true
        };
        start.Environment["BROKER_GRPC_PORT"] = grpcPort.ToString();
        start.Environment["BROKER_TCP_PORT"] = tcpPort.ToString();
        start.Environment["BROKER_STATE_FILE"] = stateFile;
        return new BrokerProcess(Process.Start(start)!);
    }

    private static async Task WaitUntilHealthy(BrokerRpcV1.BrokerRpcV1Client client)
    {
        for (int attempt = 0; attempt < 50; attempt++)
        {
            try
            {
                if ((await client.CheckHealthAsync(new HealthRequest(),
                        deadline: DateTime.UtcNow.AddSeconds(1))).Status == "Healthy") return;
            }
            catch (RpcException) { }
            await Task.Delay(100);
        }
        throw new TimeoutException("Broker gRPC did not become healthy.");
    }

    private static int FreePort()
    {
        TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class BrokerProcess(Process process) : IDisposable
    {
        public void Dispose()
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                process.WaitForExit(5000);
            }
            process.Dispose();
        }
    }
}
