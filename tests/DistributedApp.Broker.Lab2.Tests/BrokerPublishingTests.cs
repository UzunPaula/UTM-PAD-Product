using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using DistributedApp.Broker;
using DistributedApp.Broker.Grpc.V1;
using DistributedApp.Contracts;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Xunit;

namespace DistributedApp.Broker.Lab2.Tests;

public sealed class BrokerPublishingTests
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

    [Fact]
    public async Task CancellingAfterSaveDoesNotUndoTheRemoteOperation()
    {
        string stateFile = Path.Combine(Path.GetTempPath(),
            "broker-cancel-test-" + Guid.NewGuid().ToString("N"), "state.json");
        int grpcPort = FreePort();
        using BrokerProcess broker = StartBroker(grpcPort, FreePort(), stateFile);
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
            Topic = "producer-1",
            Payload = "test-order",
            SenderId = "producer-1"
        };
        using CancellationTokenSource cancellation = new();
        Task<PublishReply> call = client.PublishAsync(request,
            new Metadata { { "x-simulate-lost-response", "true" } },
            cancellationToken: cancellation.Token).ResponseAsync;

        using CancellationTokenSource wait = new(TimeSpan.FromSeconds(5));
        while (!File.Exists(stateFile))
        {
            await Task.Delay(20, wait.Token);
        }

        cancellation.Cancel();
        Exception? error = await Record.ExceptionAsync(async () => await call);
        Assert.True(error is OperationCanceledException
            || error is RpcException { StatusCode: StatusCode.Cancelled });
        Assert.True((await client.GetPublishStatusAsync(
            new PublishStatusRequest { MessageId = request.MessageId })).Found);
        Assert.Single(MessageJson.Deserialize<BrokerState>(File.ReadAllText(stateFile)).PublishedMessages);
    }

    [Fact]
    public void DefaultPathIsOutsideBinAndExistingStateIsCopiedOnce()
    {
        string root = Path.Combine(Path.GetTempPath(), "broker-path-test-" + Guid.NewGuid().ToString("N"));
        string solutionDirectory = Path.Combine(root, "DistributedApp");
        string outputDirectory = Path.Combine(solutionDirectory, "DistributedApp.Broker", "bin", "Debug", "net8.0");
        Directory.CreateDirectory(outputDirectory);
        File.WriteAllText(Path.Combine(solutionDirectory, "DistributedApp.sln"), string.Empty);
        string legacyFile = Path.Combine(outputDirectory, "broker-state.json");
        File.WriteAllText(legacyFile, "old-state");

        string selected = BrokerServer.ResolveStatePath(null, outputDirectory);

        Assert.Equal(Path.Combine(root, "data", "broker-state.json"), selected);
        Assert.Equal("old-state", File.ReadAllText(selected));
        Assert.Equal("old-state", File.ReadAllText(legacyFile));

        File.WriteAllText(selected, "new-state");
        Assert.Equal(selected, BrokerServer.ResolveStatePath(null, outputDirectory));
        Assert.Equal("new-state", File.ReadAllText(selected));
    }

    [Fact]
    public void ExplicitStatePathDoesNotTriggerMigration()
    {
        string root = Path.Combine(Path.GetTempPath(), "broker-path-test-" + Guid.NewGuid().ToString("N"));
        string configured = Path.Combine(root, "custom", "state.json");

        Assert.Equal(configured, BrokerServer.ResolveStatePath(configured, root));
        Assert.False(File.Exists(configured));
    }

    [Fact]
    public void RepeatedMessageIdDoesNotCreateAnotherPublication()
    {
        string path = TempStatePath();
        Message message = NewMessage();
        BrokerServer broker = new(0, path);

        Assert.False(broker.PublishIdempotent(message));
        Assert.True(broker.PublishIdempotent(message));
        Assert.Single(MessageJson.Deserialize<BrokerState>(File.ReadAllText(path)).PublishedMessages);
    }

    [Fact]
    public void LostReplyCanBeResolvedByStatusAfterBrokerRestart()
    {
        string path = TempStatePath();
        Message message = NewMessage();
        new BrokerServer(0, path).PublishIdempotent(message);

        BrokerServer restarted = new(0, path);
        Assert.True(restarted.ContainsPublished(message.MessageId));
        Assert.True(restarted.PublishIdempotent(message));
        Assert.Single(MessageJson.Deserialize<BrokerState>(File.ReadAllText(path)).PublishedMessages);
    }

    [Fact]
    public void ReusingMessageIdForDifferentContentIsRejected()
    {
        BrokerServer broker = new(0, TempStatePath());
        Message first = NewMessage();
        broker.PublishIdempotent(first);
        Message changed = NewMessage();
        changed.MessageId = first.MessageId;

        Assert.Throws<InvalidOperationException>(() => broker.PublishIdempotent(changed));
    }

    [Fact]
    public void NewBinaryFieldDoesNotBreakOldReaderOrLoseKnownFields()
    {
        PublishRequest original = new()
        {
            MessageId = Guid.NewGuid().ToString(),
            CorrelationId = Guid.NewGuid().ToString(),
            SchemaVersion = "1.0",
            Topic = "producer-1",
            Payload = "order"
        };

        // Câmpul 99 nu există în contractul curent: simulăm o adăugare viitoare.
        byte[] withFutureField = [.. original.ToByteArray(), 0x98, 0x06, 0x07];
        PublishRequest readByOldContract = PublishRequest.Parser.ParseFrom(withFutureField);

        Assert.Equal(original.MessageId, readByOldContract.MessageId);
        Assert.Equal(original.CorrelationId, readByOldContract.CorrelationId);
        Assert.Equal(original.Payload, readByOldContract.Payload);
        Assert.Equal(withFutureField.Length, readByOldContract.ToByteArray().Length);
    }

    [Fact]
    public void ExistingFieldNumbersStayStable()
    {
        Assert.Equal(2, PublishRequest.Descriptor.FindFieldByName("message_id").FieldNumber);
        Assert.Equal(3, PublishRequest.Descriptor.FindFieldByName("correlation_id").FieldNumber);
        Assert.Equal(8, PublishRequest.Descriptor.FindFieldByName("payload").FieldNumber);
    }

    private static Message NewMessage() => new()
    {
        Type = MessageType.Publish,
        SenderId = "test-producer",
        Topic = "orders-producer",
        Payload = Guid.NewGuid().ToString()
    };

    private static string TempStatePath() => Path.Combine(
        Path.GetTempPath(), "broker-lab2-" + Guid.NewGuid().ToString("N"), "broker-state.json");

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
