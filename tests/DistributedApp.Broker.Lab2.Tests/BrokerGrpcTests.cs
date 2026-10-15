using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using DistributedApp.Broker;
using DistributedApp.Broker.Grpc.V1;
using DistributedApp.Contracts;
using Grpc.Core;
using Grpc.Net.Client;
using Xunit;

namespace DistributedApp.Broker.Lab2.Tests;

public sealed class BrokerGrpcTests
{
    [Fact]
    public async Task LostReplyIsResolvedByStatusAndRetryDoesNotDuplicate()
    {
        int grpcPort = FreePort();
        int tcpPort = FreePort();
        string stateFile = Path.Combine(Path.GetTempPath(), "broker-grpc-test-" + Guid.NewGuid().ToString("N"), "state.json");
        using BrokerProcess broker = StartBroker(grpcPort, tcpPort, stateFile);
        using GrpcChannel channel = GrpcChannel.ForAddress($"http://127.0.0.1:{grpcPort}");
        BrokerRpcV1.BrokerRpcV1Client client = new(channel);
        await WaitUntilHealthy(client);

        PublishRequest request = new()
        {
            RequestId = Guid.NewGuid().ToString(),
            MessageId = Guid.NewGuid().ToString(),
            CorrelationId = Guid.NewGuid().ToString(),
            MessageType = "Publish",
            SchemaVersion = "1.0",
            OccurredAtUtc = DateTimeOffset.UtcNow.ToString("O"),
            Topic = "orders-producer",
            Payload = "test-order",
            SenderId = "test-producer"
        };

        await Assert.ThrowsAnyAsync<RpcException>(async () =>
            await client.PublishAsync(request,
                new Metadata { { "x-simulate-lost-response", "true" } },
                deadline: DateTime.UtcNow.AddMilliseconds(500)));

        PublishStatusReply status = await client.GetPublishStatusAsync(new PublishStatusRequest { MessageId = request.MessageId });
        Assert.True(status.Found);
        Assert.Equal("Accepted", status.Status);

        PublishReply duplicate = await client.PublishAsync(request);
        Assert.True(duplicate.Accepted);
        Assert.True(duplicate.Duplicate);
        Assert.Single(MessageJson.Deserialize<BrokerState>(File.ReadAllText(stateFile)).PublishedMessages);
    }

    [Fact]
    public async Task UnsupportedContractVersionIsRejectedWithoutSideEffect()
    {
        int grpcPort = FreePort();
        using BrokerProcess broker = StartBroker(grpcPort, FreePort(), Path.Combine(Path.GetTempPath(), "broker-grpc-test-" + Guid.NewGuid().ToString("N"), "state.json"));
        using GrpcChannel channel = GrpcChannel.ForAddress($"http://127.0.0.1:{grpcPort}");
        BrokerRpcV1.BrokerRpcV1Client client = new(channel);
        await WaitUntilHealthy(client);
        PublishRequest request = new()
        {
            RequestId = Guid.NewGuid().ToString(), MessageId = Guid.NewGuid().ToString(),
            CorrelationId = Guid.NewGuid().ToString(), MessageType = "Publish",
            SchemaVersion = "2.0", OccurredAtUtc = DateTimeOffset.UtcNow.ToString("O"),
            Topic = "orders-producer", Payload = "test-order", SenderId = "test-producer"
        };

        RpcException error = await Assert.ThrowsAsync<RpcException>(async () => await client.PublishAsync(request));
        Assert.Equal(StatusCode.FailedPrecondition, error.StatusCode);
        Assert.False((await client.GetPublishStatusAsync(new PublishStatusRequest { MessageId = request.MessageId })).Found);
    }

    [Fact]
    public async Task ReusingMessageIdWithDifferentContentReturnsAlreadyExists()
    {
        string stateFile = Path.Combine(Path.GetTempPath(), "broker-grpc-test-" + Guid.NewGuid().ToString("N"), "state.json");
        int grpcPort = FreePort();
        using BrokerProcess broker = StartBroker(grpcPort, FreePort(), stateFile);
        using GrpcChannel channel = GrpcChannel.ForAddress($"http://127.0.0.1:{grpcPort}");
        BrokerRpcV1.BrokerRpcV1Client client = new(channel);
        await WaitUntilHealthy(client);

        PublishRequest first = new()
        {
            RequestId = Guid.NewGuid().ToString(), MessageId = Guid.NewGuid().ToString(),
            CorrelationId = Guid.NewGuid().ToString(), MessageType = "Publish",
            SchemaVersion = "1.0", OccurredAtUtc = DateTimeOffset.UtcNow.ToString("O"),
            Topic = "orders-producer", Payload = "original", SenderId = "test-producer"
        };

        Assert.True((await client.PublishAsync(first)).Accepted);
        PublishRequest changed = first.Clone();
        changed.Payload = "different";
        RpcException error = await Assert.ThrowsAsync<RpcException>(async () => await client.PublishAsync(changed));

        Assert.Equal(StatusCode.AlreadyExists, error.StatusCode);
        Assert.Single(MessageJson.Deserialize<BrokerState>(File.ReadAllText(stateFile)).PublishedMessages);
    }

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
                HealthReply health = await client.CheckHealthAsync(new HealthRequest(), deadline: DateTime.UtcNow.AddSeconds(1));
                if (health.Status == "Healthy") return;
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
