using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using DistributedApp.Contracts;
using DistributedApp.Producer;

namespace DistributedApp.Producer.Tests;

public class TcpClientServiceTests
{
    [Fact]
    public async Task SendAsync_UsesJsonLineAndReturnsAcknowledgement()
    {
        TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        try
        {
            Task server = RespondWithAcknowledgementAsync(listener);
            TcpClientService service = new(
                new ProducerSettings
                {
                    BrokerHost = "127.0.0.1",
                    BrokerPort = port,
                    RequestTimeoutMilliseconds = 2000
                });
            Message published = CreatePublishMessage();

            Message response = await service.SendAsync(published);
            await server;

            Assert.Equal(MessageType.Ack, response.Type);
            Assert.Equal(published.MessageId, response.RelatedMessageId);
            Assert.Equal(published.CorrelationId, response.CorrelationId);
        }
        finally
        {
            listener.Stop();
        }
    }

    [Fact]
    public async Task GetBrokerStatusAsync_ReturnsValidatedSnapshot()
    {
        TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        try
        {
            Task server = RespondWithStatusAsync(listener);
            TcpClientService service = new(
                new ProducerSettings
                {
                    BrokerHost = "127.0.0.1",
                    BrokerPort = port,
                    RequestTimeoutMilliseconds = 2000
                });

            BrokerStatusSnapshot status =
                await service.GetBrokerStatusAsync("orders");
            await server;

            Assert.True(status.ConsumerConnected);
            Assert.Equal(2, status.PendingMessages);
            Assert.Equal(1, status.InFlightMessages);
            Assert.Equal(1, status.DeadLetterMessages);
            Assert.Equal(4, status.AcknowledgedMessages);
            Assert.Single(status.RecentAcknowledgements);
            Assert.Single(status.DeadLetters);
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task RespondWithAcknowledgementAsync(
        TcpListener listener)
    {
        using TcpClient client = await listener.AcceptTcpClientAsync();
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

        string line = await reader.ReadLineAsync()
                      ?? throw new IOException("Expected a JSON line.");
        Message incoming = MessageJson.Deserialize(line);
        Message acknowledgement = new()
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = incoming.CorrelationId,
            Type = MessageType.Ack,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = incoming.Topic,
            SequenceNumber = incoming.SequenceNumber,
            RetryCount = 0,
            Payload = string.Empty,
            RelatedMessageId = incoming.MessageId
        };

        await writer.WriteLineAsync(MessageJson.Serialize(acknowledgement));
    }

    private static async Task RespondWithStatusAsync(
        TcpListener listener)
    {
        using TcpClient client = await listener.AcceptTcpClientAsync();
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

        string line = await reader.ReadLineAsync()
                      ?? throw new IOException("Expected a JSON line.");
        Message request = MessageJson.Deserialize(line);
        BrokerStatusSnapshot status = new()
        {
            Topic = "orders",
            ConsumerConnected = true,
            PendingMessages = 2,
            InFlightMessages = 1,
            DeadLetterMessages = 1,
            AcknowledgedMessages = 4,
            RecentAcknowledgements =
            [
                new AcknowledgementSummary
                {
                    MessageId = Guid.NewGuid(),
                    CorrelationId = Guid.NewGuid(),
                    Topic = "orders",
                    AcknowledgedAtUtc = DateTimeOffset.UtcNow
                }
            ],
            DeadLetters =
            [
                new DeadLetterSummary
                {
                    MessageId = Guid.NewGuid(),
                    CorrelationId = Guid.NewGuid(),
                    Topic = "orders",
                    RetryCount = 3,
                    Reason = "Rejected",
                    DeadLetteredAtUtc = DateTimeOffset.UtcNow
                }
            ],
            ObservedAtUtc = DateTimeOffset.UtcNow
        };
        Message response = new()
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = request.CorrelationId,
            Type = MessageType.StatusResponse,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = request.Topic,
            Payload = JsonSerializer.Serialize(status),
            RelatedMessageId = request.MessageId
        };

        await writer.WriteLineAsync(MessageJson.Serialize(response));
    }

    private static Message CreatePublishMessage()
    {
        return new Message
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Type = MessageType.Publish,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = "orders",
            SequenceNumber = 0,
            RetryCount = 0,
            Payload = "{}"
        };
    }
}
