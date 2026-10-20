extern alias BrokerApp;

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using DistributedApp.Broker.Grpc.V1;
using DistributedApp.Consumer;
using DistributedApp.Contracts;
using Grpc.Core;
using Grpc.Net.Client;
using Xunit;
using BrokerServer = BrokerApp::DistributedApp.Broker.BrokerServer;
using BrokerState = BrokerApp::DistributedApp.Broker.BrokerState;
using BrokerDeliveryStatus = BrokerApp::DistributedApp.Broker.DeliveryStatus;

namespace DistributedApp.Consumer.Tests;

public sealed class ConsumerDeliveryIntegrationTests
{
    [Fact]
    public async Task ConsumerCannotStartWithoutId()
    {
        ProcessStartInfo start = new("dotnet") { UseShellExecute = false, CreateNoWindow = true };
        start.ArgumentList.Add(typeof(ProcessedMessageStore).Assembly.Location);
        using Process consumer = Process.Start(start) ?? throw new InvalidOperationException("Consumer did not start.");
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));

        await consumer.WaitForExitAsync(timeout.Token);
        Assert.Equal(2, consumer.ExitCode);
    }

    [Fact]
    public async Task RestartAfterLostAckDoesNotProcessOrderTwice()
    {
        using TestBroker broker = await TestBroker.StartAsync();
        string consumerState = NewStateFile();
        Process? consumer = StartConsumer(broker, "consumer-restart", consumerState, "drop-ack-once");
        try
        {
            await WaitForSubscriptionAsync(consumer);
            PublishRequest publication = await broker.PublishAsync();
            await WaitUntilAsync(() => File.Exists(consumerState)
                && ReadOrders(consumerState).ContainsKey(Guid.Parse(publication.MessageId)));
            await StopConsumerAsync(consumer);
            consumer = null;

            consumer = StartConsumer(broker, "consumer-restart", consumerState);
            await WaitForSubscriptionAsync(consumer);
            await broker.WaitForAsync(state => state.Acknowledgements.Any(x =>
                x.MessageId == Guid.Parse(publication.MessageId) && x.ConsumerId == "consumer-restart"));
            Assert.Single(ReadOrders(consumerState));
        }
        finally
        {
            if (consumer is not null) await StopConsumerAsync(consumer);
        }
    }

    [Fact]
    public async Task TwoConsumersWithDifferentIdsProcessTheirOwnDeliveries()
    {
        using TestBroker broker = await TestBroker.StartAsync();
        string firstState = NewStateFile();
        string secondState = NewStateFile();
        Process first = StartConsumer(broker, "consumer-a", firstState);
        Process? second = null;
        try
        {
            second = StartConsumer(broker, "consumer-b", secondState);
            await WaitForSubscriptionAsync(first);
            await WaitForSubscriptionAsync(second);
            PublishRequest publication = await broker.PublishAsync();
            Guid messageId = Guid.Parse(publication.MessageId);
            await broker.WaitForAsync(state => state.Acknowledgements.Count(x => x.MessageId == messageId) == 2);

            Assert.True(ReadOrders(firstState).ContainsKey(messageId));
            Assert.True(ReadOrders(secondState).ContainsKey(messageId));
            Assert.Single(ReadOrders(firstState));
            Assert.Single(ReadOrders(secondState));
        }
        finally
        {
            await StopConsumerAsync(first);
            if (second is not null) await StopConsumerAsync(second);
        }
    }

    [Fact]
    public async Task SecondProcessWithSameStateFileExitsBeforeSubscribing()
    {
        using TestBroker broker = await TestBroker.StartAsync();
        string stateFile = NewStateFile();
        Process first = StartConsumer(broker, "consumer-same", stateFile);
        Process? second = null;
        try
        {
            await WaitForSubscriptionAsync(first);
            second = StartConsumer(broker, "consumer-same", stateFile);
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
            await second.WaitForExitAsync(timeout.Token);
            Assert.Equal(2, second.ExitCode);
            Assert.False(first.HasExited);
        }
        finally
        {
            await StopConsumerAsync(first);
            if (second is not null) await StopConsumerAsync(second);
        }
    }

    [Fact]
    public async Task BrokerRejectionDoesNotClaimSubscription()
    {
        using TestBroker broker = await TestBroker.StartAsync();
        Process first = StartConsumer(broker, "consumer-duplicate", NewStateFile());
        Process? second = null;
        try
        {
            await WaitForSubscriptionAsync(first);
            second = StartConsumer(broker, "consumer-duplicate", NewStateFile());
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
            await second.WaitForExitAsync(timeout.Token);
            Assert.Equal(2, second.ExitCode);
            Assert.Contains("SUBSCRIBE REJECTED: ConsumerId is already connected.",
                await second.StandardError.ReadToEndAsync(timeout.Token));
            Assert.DoesNotContain("SUBSCRIBED:", await second.StandardOutput.ReadToEndAsync(timeout.Token));
            Assert.False(first.HasExited);
        }
        finally
        {
            await StopConsumerAsync(first);
            if (second is not null) await StopConsumerAsync(second);
        }
    }

    [Fact]
    public async Task FailModeNacksDeliveryWithoutSavingOrder()
    {
        using TestBroker broker = await TestBroker.StartAsync();
        string consumerState = NewStateFile();
        Process consumer = StartConsumer(broker, "consumer-fail", consumerState, "fail");
        try
        {
            await WaitForSubscriptionAsync(consumer);
            PublishRequest publication = await broker.PublishAsync();
            await broker.WaitForAsync(state => state.Deliveries.Any(x =>
                x.Message.MessageId == Guid.Parse(publication.MessageId)
                && x.ConsumerId == "consumer-fail" && x.Status == BrokerDeliveryStatus.DeadLetter));
            Assert.False(File.Exists(consumerState));
        }
        finally
        {
            await StopConsumerAsync(consumer);
        }
    }

    private static Process StartConsumer(TestBroker broker, string consumerId, string stateFile, string? mode = null)
    {
        ProcessStartInfo start = new("dotnet")
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        start.ArgumentList.Add(typeof(ProcessedMessageStore).Assembly.Location);
        start.ArgumentList.Add(consumerId);
        start.ArgumentList.Add("producer-1");
        if (mode is not null) start.ArgumentList.Add(mode);
        start.Environment["CONSUMER_BROKER_GRPC_ADDRESS"] = broker.Address;
        start.Environment["CONSUMER_STATE_FILE"] = stateFile;
        return Process.Start(start) ?? throw new InvalidOperationException("Consumer did not start.");
    }

    private static async Task StopConsumerAsync(Process process)
    {
        if (!process.HasExited) process.Kill(entireProcessTree: true);
        await process.WaitForExitAsync();
        process.Dispose();
    }

    private static async Task WaitForSubscriptionAsync(Process consumer)
    {
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        List<string> output = [];
        try
        {
            while (await consumer.StandardOutput.ReadLineAsync(timeout.Token) is { } line)
            {
                output.Add(line);
                if (line.StartsWith("SUBSCRIBED: ", StringComparison.Ordinal)) return;
            }
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            throw new TimeoutException($"Consumer did not confirm its subscription. Output: {string.Join(" | ", output)}");
        }

        string error = await consumer.StandardError.ReadToEndAsync();
        throw new IOException($"Consumer exited before confirming its subscription. Output: {string.Join(" | ", output)} Error: {error}");
    }

    private static async Task WaitUntilAsync(Func<bool> predicate)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                if (predicate()) return;
            }
            catch (IOException) { }
            await Task.Delay(100);
        }
        throw new TimeoutException("Expected Consumer state was not observed.");
    }

    private static string NewStateFile() => Path.Combine(
        Path.GetTempPath(), "distributedapp-consumer-tests", Guid.NewGuid().ToString("N"), "state.json");

    private static Dictionary<Guid, OrderPayload> ReadOrders(string path) =>
        JsonSerializer.Deserialize<Dictionary<Guid, OrderPayload>>(File.ReadAllText(path))!;

    private sealed class TestBroker(Process process, int grpcPort, string stateFile) : IDisposable
    {
        public string Address => $"http://127.0.0.1:{grpcPort}";

        public static async Task<TestBroker> StartAsync()
        {
            int grpcPort = FreePort();
            string stateFile = NewStateFile();
            ProcessStartInfo start = new("dotnet")
            {
                UseShellExecute = false,
                CreateNoWindow = true
            };
            start.ArgumentList.Add(typeof(BrokerServer).Assembly.Location);
            start.Environment["BROKER_GRPC_PORT"] = grpcPort.ToString();
            start.Environment["BROKER_TCP_PORT"] = FreePort().ToString();
            start.Environment["BROKER_STATE_FILE"] = stateFile;
            TestBroker broker = new(Process.Start(start)
                ?? throw new InvalidOperationException("Broker did not start."), grpcPort, stateFile);
            try
            {
                using GrpcChannel channel = GrpcChannel.ForAddress(broker.Address);
                BrokerRpcV1.BrokerRpcV1Client client = new(channel);
                for (int attempt = 0; attempt < 50; attempt++)
                {
                    try
                    {
                        await client.GetPublishStatusAsync(
                            new PublishStatusRequest { MessageId = Guid.NewGuid().ToString() },
                            deadline: DateTime.UtcNow.AddSeconds(1));
                        return broker;
                    }
                    catch (RpcException) { }
                    await Task.Delay(100);
                }
                throw new TimeoutException("Broker gRPC did not become ready.");
            }
            catch
            {
                broker.Dispose();
                throw;
            }
        }

        public async Task<PublishRequest> PublishAsync()
        {
            PublishRequest publication = new()
            {
                RequestId = Guid.NewGuid().ToString(),
                MessageId = Guid.NewGuid().ToString(),
                CorrelationId = Guid.NewGuid().ToString(),
                MessageType = "Publish",
                SchemaVersion = "1.0",
                OccurredAtUtc = DateTimeOffset.UtcNow.ToString("O"),
                Topic = "producer-1",
                Payload = MessageJson.Serialize(new OrderPayload("ORD-1", "Keyboard", "Mechanical")),
                SenderId = "producer-1"
            };
            using GrpcChannel channel = GrpcChannel.ForAddress(Address);
            BrokerRpcV1.BrokerRpcV1Client client = new(channel);
            Assert.True((await client.PublishAsync(publication)).Accepted);
            return publication;
        }

        public async Task WaitForAsync(Func<BrokerState, bool> predicate) =>
            await WaitUntilAsync(() => File.Exists(stateFile)
                && predicate(MessageJson.Deserialize<BrokerState>(File.ReadAllText(stateFile))));

        public void Dispose()
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                process.WaitForExit(5000);
            }
            process.Dispose();
        }

        private static int FreePort()
        {
            TcpListener listener = new(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }
    }
}
