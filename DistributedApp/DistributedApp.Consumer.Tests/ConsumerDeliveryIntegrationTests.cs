using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using DistributedApp.Consumer;
using DistributedApp.Contracts;
using Xunit;

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
        using TcpListener broker = NewBroker();
        string stateFile = NewStateFile();
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(20));
        Message delivery = NewDelivery();
        Process? consumer = null;

        try
        {
            consumer = StartConsumer(broker, "consumer-restart", stateFile, "drop-ack-once");
            using (TcpClient firstConnection = await broker.AcceptTcpClientAsync(timeout.Token))
            {
                await using NetworkStream firstStream = firstConnection.GetStream();
                using StreamReader reader = NewReader(firstStream);
                await using StreamWriter writer = NewWriter(firstStream);
                Message subscription = await ReadMessage(reader, timeout.Token);
                Assert.Equal("consumer-restart", subscription.SenderId);
                await ConfirmSubscription(writer, subscription);

                await writer.WriteLineAsync(MessageJson.Serialize(delivery));
                Task<string?> response = reader.ReadLineAsync(timeout.Token).AsTask();
                await Task.Delay(500, timeout.Token);
                Assert.False(response.IsCompleted, "The first ACK should be deliberately lost.");

                await StopConsumer(consumer);
                consumer = null;
            }

            Assert.Single(ReadOrders(stateFile));

            consumer = StartConsumer(broker, "consumer-restart", stateFile);
            using TcpClient secondConnection = await broker.AcceptTcpClientAsync(timeout.Token);
            await using NetworkStream secondStream = secondConnection.GetStream();
            using StreamReader secondReader = NewReader(secondStream);
            await using StreamWriter secondWriter = NewWriter(secondStream);
            Message secondSubscription = await ReadMessage(secondReader, timeout.Token);
            Assert.Equal("consumer-restart", secondSubscription.SenderId);
            await ConfirmSubscription(secondWriter, secondSubscription);

            await secondWriter.WriteLineAsync(MessageJson.Serialize(delivery));
            Message acknowledgement = await ReadMessage(secondReader, timeout.Token);
            Assert.Equal(MessageType.Ack, acknowledgement.Type);
            Assert.Equal(delivery.MessageId, acknowledgement.RelatedMessageId);
            Assert.Single(ReadOrders(stateFile));
        }
        finally
        {
            if (consumer is not null) await StopConsumer(consumer);
        }
    }

    [Fact]
    public async Task TwoConsumersWithDifferentIdsProcessTheirOwnDeliveries()
    {
        using TcpListener broker = NewBroker();
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(20));
        string firstFile = NewStateFile();
        string secondFile = NewStateFile();
        Message delivery = NewDelivery();
        Process first = StartConsumer(broker, "consumer-a", firstFile);
        Process? second = null;

        try
        {
            using TcpClient firstConnection = await broker.AcceptTcpClientAsync(timeout.Token);
            await using NetworkStream firstStream = firstConnection.GetStream();
            using StreamReader firstReader = NewReader(firstStream);
            await using StreamWriter firstWriter = NewWriter(firstStream);
            Message firstSubscription = await ReadMessage(firstReader, timeout.Token);
            Assert.Equal("consumer-a", firstSubscription.SenderId);
            await ConfirmSubscription(firstWriter, firstSubscription);

            second = StartConsumer(broker, "consumer-b", secondFile);
            using TcpClient secondConnection = await broker.AcceptTcpClientAsync(timeout.Token);
            await using NetworkStream secondStream = secondConnection.GetStream();
            using StreamReader secondReader = NewReader(secondStream);
            await using StreamWriter secondWriter = NewWriter(secondStream);
            Message secondSubscription = await ReadMessage(secondReader, timeout.Token);
            Assert.Equal("consumer-b", secondSubscription.SenderId);
            await ConfirmSubscription(secondWriter, secondSubscription);

            await firstWriter.WriteLineAsync(MessageJson.Serialize(delivery));
            await secondWriter.WriteLineAsync(MessageJson.Serialize(delivery));

            Assert.Equal(MessageType.Ack, (await ReadMessage(firstReader, timeout.Token)).Type);
            Assert.Equal(MessageType.Ack, (await ReadMessage(secondReader, timeout.Token)).Type);
            Assert.Single(ReadOrders(firstFile));
            Assert.Single(ReadOrders(secondFile));
        }
        finally
        {
            await StopConsumer(first);
            if (second is not null) await StopConsumer(second);
        }
    }

    [Fact]
    public async Task SecondProcessWithSameStateFileExitsBeforeSubscribing()
    {
        using TcpListener broker = NewBroker();
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(20));
        string stateFile = NewStateFile();
        Process first = StartConsumer(broker, "consumer-same", stateFile);
        Process? second = null;

        try
        {
            using TcpClient connection = await broker.AcceptTcpClientAsync(timeout.Token);
            await using NetworkStream stream = connection.GetStream();
            using StreamReader reader = NewReader(stream);
            await using StreamWriter writer = NewWriter(stream);
            Message subscription = await ReadMessage(reader, timeout.Token);
            Assert.Equal("consumer-same", subscription.SenderId);
            await ConfirmSubscription(writer, subscription);

            second = StartConsumer(broker, "consumer-same", stateFile);
            await second.WaitForExitAsync(timeout.Token);
            Assert.Equal(2, second.ExitCode);
            Assert.False(first.HasExited);
        }
        finally
        {
            await StopConsumer(first);
            if (second is not null) await StopConsumer(second);
        }
    }

    [Fact]
    public async Task SubscriptionNackShowsReasonWithoutClaimingSubscription()
    {
        using TcpListener broker = NewBroker();
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(20));
        string stateFile = NewStateFile();
        Process consumer = StartConsumer(broker, "consumer-rejected", stateFile);

        try
        {
            using TcpClient connection = await broker.AcceptTcpClientAsync(timeout.Token);
            await using NetworkStream stream = connection.GetStream();
            using StreamReader reader = NewReader(stream);
            await using StreamWriter writer = NewWriter(stream);
            Message subscription = await ReadMessage(reader, timeout.Token);
            await ConfirmSubscription(writer, subscription, MessageType.Nack, "ConsumerId is already connected.");

            await consumer.WaitForExitAsync(timeout.Token);
            Assert.Equal(2, consumer.ExitCode);
            Assert.Contains("SUBSCRIBE REJECTED: ConsumerId is already connected.",
                await consumer.StandardError.ReadToEndAsync(timeout.Token));
            Assert.DoesNotContain("SUBSCRIBED:", await consumer.StandardOutput.ReadToEndAsync(timeout.Token));
            Assert.False(File.Exists(stateFile));
        }
        finally
        {
            await StopConsumer(consumer);
        }
    }

    [Fact]
    public async Task SubscriptionAckMustReferToTheSentRequest()
    {
        using TcpListener broker = NewBroker();
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(20));
        string stateFile = NewStateFile();
        Process consumer = StartConsumer(broker, "consumer-wrong-ack", stateFile);

        try
        {
            using TcpClient connection = await broker.AcceptTcpClientAsync(timeout.Token);
            await using NetworkStream stream = connection.GetStream();
            using StreamReader reader = NewReader(stream);
            await using StreamWriter writer = NewWriter(stream);
            Message subscription = await ReadMessage(reader, timeout.Token);
            await writer.WriteLineAsync(MessageJson.Serialize(new Message
            {
                Type = MessageType.Ack,
                SenderId = "broker",
                Topic = subscription.Topic,
                RelatedMessageId = Guid.NewGuid(),
                CorrelationId = subscription.CorrelationId
            }));

            Assert.Null(await reader.ReadLineAsync(timeout.Token));
            Assert.False(File.Exists(stateFile));
        }
        finally
        {
            await StopConsumer(consumer);
        }
    }

    private static TcpListener NewBroker()
    {
        TcpListener broker = new(IPAddress.Loopback, 0);
        broker.Start();
        return broker;
    }

    private static Process StartConsumer(TcpListener broker, string consumerId, string stateFile, string? mode = null)
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
        start.Environment["CONSUMER_BROKER_HOST"] = "127.0.0.1";
        start.Environment["CONSUMER_BROKER_PORT"] = ((IPEndPoint)broker.LocalEndpoint).Port.ToString();
        start.Environment["CONSUMER_STATE_FILE"] = stateFile;
        return Process.Start(start) ?? throw new InvalidOperationException("Consumer did not start.");
    }

    private static async Task StopConsumer(Process consumer)
    {
        if (!consumer.HasExited) consumer.Kill(entireProcessTree: true);
        await consumer.WaitForExitAsync();
        consumer.Dispose();
    }

    private static StreamReader NewReader(NetworkStream stream) =>
        new(stream, Encoding.UTF8, false, leaveOpen: true);

    private static StreamWriter NewWriter(NetworkStream stream) =>
        new(stream, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true, NewLine = "\n" };

    private static async Task<Message> ReadMessage(StreamReader reader, CancellationToken token) =>
        MessageJson.Deserialize<Message>(await reader.ReadLineAsync(token) ?? throw new IOException("No message."));

    private static Task ConfirmSubscription(StreamWriter writer, Message request,
        MessageType responseType = MessageType.Ack, string? reason = null) =>
        writer.WriteLineAsync(MessageJson.Serialize(new Message
        {
            Type = responseType,
            SenderId = "broker",
            Topic = request.Topic,
            RelatedMessageId = request.MessageId,
            CorrelationId = request.CorrelationId,
            Reason = reason
        }));

    private static Message NewDelivery() => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = Guid.NewGuid(),
        Type = MessageType.Delivery,
        SenderId = "producer-1",
        Topic = "producer-1",
        Payload = JsonSerializer.Serialize(new OrderPayload("ORD-1", "Keyboard", "Mechanical"))
    };

    private static string NewStateFile() => Path.Combine(
        Path.GetTempPath(), "distributedapp-consumer-tests", Guid.NewGuid().ToString("N"), "state.json");

    private static Dictionary<Guid, OrderPayload> ReadOrders(string stateFile) =>
        JsonSerializer.Deserialize<Dictionary<Guid, OrderPayload>>(File.ReadAllText(stateFile))!;
}
