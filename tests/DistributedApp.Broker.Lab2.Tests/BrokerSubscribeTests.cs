using System.Net;
using System.Net.Sockets;
using System.Text;
using DistributedApp.Broker;
using DistributedApp.Contracts;
using Xunit;

namespace DistributedApp.Broker.Lab2.Tests;

public sealed class BrokerSubscribeTests
{
    [Fact]
    public async Task AcceptedSubscriptionIsConfirmedBeforePendingDelivery()
    {
        string stateFile = NewStateFile();
        Message publication = new()
        {
            Type = MessageType.Publish,
            SenderId = "producer-1",
            Topic = "producer-1",
            Payload = "order"
        };
        new BrokerServer(0, stateFile).PublishIdempotent(publication);

        await using BrokerHost broker = new(stateFile);
        using TcpClient client = await broker.ConnectAsync();
        using StreamReader reader = new(client.GetStream(), Encoding.UTF8, leaveOpen: true);
        await using StreamWriter writer = new(client.GetStream(), new UTF8Encoding(false), leaveOpen: true)
        {
            AutoFlush = true
        };
        Message request = Subscribe("consumer-1");

        await writer.WriteLineAsync(MessageJson.Serialize(request));
        Message confirmation = await ReadAsync(reader);

        Assert.Equal(MessageType.Ack, confirmation.Type);
        Assert.Equal("broker", confirmation.SenderId);
        Assert.Equal(request.MessageId, confirmation.RelatedMessageId);
        Assert.Equal(request.CorrelationId, confirmation.CorrelationId);
        Assert.Equal(request.Topic, confirmation.Topic);
        Assert.Equal(MessageType.Delivery, (await ReadAsync(reader)).Type);
    }

    [Fact]
    public async Task DuplicateActiveConsumerIdIsRejectedWithCorrelatedNack()
    {
        await using BrokerHost broker = new(NewStateFile());
        using TcpClient first = await broker.ConnectAsync();
        using StreamReader firstReader = new(first.GetStream(), Encoding.UTF8, leaveOpen: true);
        await using StreamWriter firstWriter = new(first.GetStream(), new UTF8Encoding(false), leaveOpen: true)
        {
            AutoFlush = true
        };
        Message firstRequest = Subscribe("consumer-1");
        await firstWriter.WriteLineAsync(MessageJson.Serialize(firstRequest));
        Assert.Equal(MessageType.Ack, (await ReadAsync(firstReader)).Type);

        using TcpClient second = await broker.ConnectAsync();
        using StreamReader secondReader = new(second.GetStream(), Encoding.UTF8, leaveOpen: true);
        await using StreamWriter secondWriter = new(second.GetStream(), new UTF8Encoding(false), leaveOpen: true)
        {
            AutoFlush = true
        };
        Message secondRequest = Subscribe("consumer-1");
        await secondWriter.WriteLineAsync(MessageJson.Serialize(secondRequest));
        Message rejection = await ReadAsync(secondReader);

        Assert.Equal(MessageType.Nack, rejection.Type);
        Assert.Equal(secondRequest.MessageId, rejection.RelatedMessageId);
        Assert.Equal(secondRequest.CorrelationId, rejection.CorrelationId);
        Assert.False(string.IsNullOrWhiteSpace(rejection.Reason));
    }

    [Fact]
    public async Task SubscriptionWithoutConsumerIdIsRejected()
    {
        await using BrokerHost broker = new(NewStateFile());
        using TcpClient client = await broker.ConnectAsync();
        using StreamReader reader = new(client.GetStream(), Encoding.UTF8, leaveOpen: true);
        await using StreamWriter writer = new(client.GetStream(), new UTF8Encoding(false), leaveOpen: true)
        {
            AutoFlush = true
        };
        Message request = Subscribe(string.Empty);

        await writer.WriteLineAsync(MessageJson.Serialize(request));
        Message rejection = await ReadAsync(reader);

        Assert.Equal(MessageType.Nack, rejection.Type);
        Assert.Equal(request.MessageId, rejection.RelatedMessageId);
        Assert.False(File.Exists(broker.StateFile));
    }

    [Fact]
    public async Task SecondSubscriptionOnSameConnectionIsRejected()
    {
        await using BrokerHost broker = new(NewStateFile());
        using TcpClient client = await broker.ConnectAsync();
        using StreamReader reader = new(client.GetStream(), Encoding.UTF8, leaveOpen: true);
        await using StreamWriter writer = new(client.GetStream(), new UTF8Encoding(false), leaveOpen: true)
        {
            AutoFlush = true
        };
        Message first = Subscribe("consumer-1");
        await writer.WriteLineAsync(MessageJson.Serialize(first));
        Assert.Equal(MessageType.Ack, (await ReadAsync(reader)).Type);

        Message second = Subscribe("consumer-1");
        await writer.WriteLineAsync(MessageJson.Serialize(second));
        Message rejection = await ReadAsync(reader);

        Assert.Equal(MessageType.Nack, rejection.Type);
        Assert.Equal(second.MessageId, rejection.RelatedMessageId);
        Assert.False(string.IsNullOrWhiteSpace(rejection.Reason));
    }

    [Fact]
    public async Task ConsumerIsCountedOnlyOnItsActiveTopic()
    {
        await using BrokerHost broker = new(NewStateFile());

        using (TcpClient oldClient = await broker.ConnectAsync())
        using (StreamReader oldReader = new(oldClient.GetStream(), Encoding.UTF8, leaveOpen: true))
        await using (StreamWriter oldWriter = new(oldClient.GetStream(), new UTF8Encoding(false), leaveOpen: true)
        {
            AutoFlush = true
        })
        {
            await oldWriter.WriteLineAsync(MessageJson.Serialize(Subscribe("consumer-3", "producer-2")));
            Assert.Equal(MessageType.Ack, (await ReadAsync(oldReader)).Type);
        }

        await Task.Delay(100);

        using TcpClient activeClient = await broker.ConnectAsync();
        using StreamReader activeReader = new(activeClient.GetStream(), Encoding.UTF8, leaveOpen: true);
        await using StreamWriter activeWriter = new(activeClient.GetStream(), new UTF8Encoding(false), leaveOpen: true)
        {
            AutoFlush = true
        };
        await activeWriter.WriteLineAsync(MessageJson.Serialize(Subscribe("consumer-3", "producer-1")));
        Assert.Equal(MessageType.Ack, (await ReadAsync(activeReader)).Type);

        BrokerStatus currentTopic = await ReadStatusAsync(activeWriter, activeReader, "producer-1");
        BrokerStatus oldTopic = await ReadStatusAsync(activeWriter, activeReader, "producer-2");

        Assert.Equal(1, currentTopic.ConsumerCount);
        Assert.Equal(0, oldTopic.ConsumerCount);

        BrokerState savedState = MessageJson.Deserialize<BrokerState>(File.ReadAllText(broker.StateFile));
        Subscription savedSubscription = Assert.Single(savedState.Subscriptions,
            x => x.ConsumerId == "consumer-3");
        Assert.Equal("producer-1", savedSubscription.Topic);
    }

    private static Message Subscribe(string consumerId, string topic = "producer-1") => new()
    {
        Type = MessageType.Subscribe,
        SenderId = consumerId,
        Topic = topic
    };

    private static async Task<BrokerStatus> ReadStatusAsync(
        StreamWriter writer, StreamReader reader, string topic)
    {
        Message request = new()
        {
            Type = MessageType.StatusRequest,
            SenderId = "producer-test",
            Topic = topic
        };
        await writer.WriteLineAsync(MessageJson.Serialize(request));
        Message reply = await ReadAsync(reader);
        Assert.Equal(MessageType.StatusResponse, reply.Type);
        return MessageJson.Deserialize<BrokerStatus>(reply.Payload);
    }

    private static async Task<Message> ReadAsync(StreamReader reader)
    {
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(5));
        string? line = await reader.ReadLineAsync(timeout.Token);
        return MessageJson.Deserialize<Message>(line ?? throw new IOException("Connection closed without a response."));
    }

    private static string NewStateFile() => Path.Combine(
        Path.GetTempPath(), "broker-subscribe-test-" + Guid.NewGuid().ToString("N"), "state.json");

    private sealed class BrokerHost : IAsyncDisposable
    {
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _run;

        public BrokerHost(string stateFile)
        {
            StateFile = stateFile;
            TcpListener portReservation = new(IPAddress.Loopback, 0);
            portReservation.Start();
            Port = ((IPEndPoint)portReservation.LocalEndpoint).Port;
            portReservation.Stop();
            _run = new BrokerServer(Port, stateFile).RunAsync(_stop.Token);
        }

        public string StateFile { get; }
        private int Port { get; }

        public async Task<TcpClient> ConnectAsync()
        {
            TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, Port);
            return client;
        }

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            await _run;
            _stop.Dispose();
        }
    }
}
