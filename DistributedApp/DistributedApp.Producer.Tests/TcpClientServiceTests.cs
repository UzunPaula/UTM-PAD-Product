using System.Net;
using System.Net.Sockets;
using System.Text;
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
