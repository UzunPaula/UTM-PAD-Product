using System.Net;
using System.Net.Sockets;
using System.Text;
using DistributedApp.Contracts;

namespace DistributedApp.Consumer.Tests;

public class ConsumerClientIntegrationTests
{
    [Fact]
    public async Task RunAsync_SubscribesReceivesMessageAndSendsAck()
    {
        string directory = CreateTestDirectory();
        TcpListener broker = new(IPAddress.Loopback, 0);
        broker.Start();

        try
        {
            int port = ((IPEndPoint)broker.LocalEndpoint).Port;
            ProcessedMessageStore store =
                new(Path.Combine(directory, "state.json"));
            ConsumerMessageHandler handler =
                new(new MessageProcessor(store));
            ConsumerClient consumer =
                new("127.0.0.1", port, handler);

            Task consumerTask = consumer.RunAsync(
                "orders",
                CancellationToken.None);

            using TcpClient connection = await broker.AcceptTcpClientAsync();
            await using NetworkStream stream = connection.GetStream();
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

            Message subscribe = MessageJson.Deserialize(
                (await reader.ReadLineAsync())!);

            Assert.Equal(MessageType.Subscribe, subscribe.Type);
            Assert.Equal("orders", subscribe.Topic);

            Message delivered = CreateMessage();
            await writer.WriteLineAsync(MessageJson.Serialize(delivered));

            Message acknowledgement = MessageJson.Deserialize(
                (await reader.ReadLineAsync())!);

            Assert.Equal(MessageType.Ack, acknowledgement.Type);
            Assert.Equal(
                delivered.MessageId,
                acknowledgement.RelatedMessageId);
            Assert.True(store.IsProcessed(delivered.MessageId));

            connection.Close();
            await consumerTask.WaitAsync(TimeSpan.FromSeconds(5));
        }
        finally
        {
            broker.Stop();
            Directory.Delete(directory, recursive: true);
        }
    }

    private static Message CreateMessage()
    {
        return new Message
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Type = MessageType.Message,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = "orders",
            SequenceNumber = 1,
            RetryCount = 0,
            Payload =
                "{\"orderId\":\"ORD-2\",\"product\":\"Monitor\",\"quantity\":1}"
        };
    }

    private static string CreateTestDirectory()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "distributed-app-consumer-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);
        return directory;
    }
}
