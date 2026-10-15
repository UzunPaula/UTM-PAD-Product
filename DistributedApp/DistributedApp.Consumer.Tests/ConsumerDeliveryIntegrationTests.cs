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
    public async Task LostAckCausesRedeliveryWithoutProcessingOrderTwice()
    {
        using TcpListener broker = new(IPAddress.Loopback, 0);
        broker.Start();
        int port = ((IPEndPoint)broker.LocalEndpoint).Port;
        string stateFile = Path.Combine(Path.GetTempPath(), "distributedapp-consumer-tests",
            Guid.NewGuid().ToString("N"), "state.json");

        ProcessStartInfo start = new("dotnet")
        {
            UseShellExecute = false,
            CreateNoWindow = true
        };
        start.ArgumentList.Add(typeof(ProcessedMessageStore).Assembly.Location);
        start.ArgumentList.Add("consumer-test");
        start.ArgumentList.Add("producer-1");
        start.ArgumentList.Add("drop-ack-once");
        start.Environment["CONSUMER_BROKER_HOST"] = "127.0.0.1";
        start.Environment["CONSUMER_BROKER_PORT"] = port.ToString();
        start.Environment["CONSUMER_STATE_FILE"] = stateFile;

        using Process consumer = Process.Start(start) ?? throw new InvalidOperationException("Consumer did not start.");
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(15));
        try
        {
            using TcpClient connection = await broker.AcceptTcpClientAsync(timeout.Token);
            await using NetworkStream stream = connection.GetStream();
            using StreamReader reader = new(stream, Encoding.UTF8, false, leaveOpen: true);
            await using StreamWriter writer = new(stream, new UTF8Encoding(false), leaveOpen: true)
            {
                AutoFlush = true,
                NewLine = "\n"
            };

            Message subscription = MessageJson.Deserialize<Message>(
                await reader.ReadLineAsync(timeout.Token) ?? throw new IOException("No subscription."));
            Assert.Equal(MessageType.Subscribe, subscription.Type);
            Assert.Equal("consumer-test", subscription.SenderId);
            Assert.Equal("producer-1", subscription.Topic);

            Guid messageId = Guid.NewGuid();
            Message delivery = new()
            {
                MessageId = messageId,
                CorrelationId = Guid.NewGuid(),
                Type = MessageType.Delivery,
                SenderId = "producer-1",
                Topic = "producer-1",
                Payload = JsonSerializer.Serialize(new OrderPayload("ORD-1", "Keyboard", "Mechanical"))
            };

            await writer.WriteLineAsync(MessageJson.Serialize(delivery));
            Task<string?> responseLine = reader.ReadLineAsync(timeout.Token).AsTask();
            await Task.Delay(500, timeout.Token);
            Assert.False(responseLine.IsCompleted, "The first ACK should be deliberately lost.");

            await writer.WriteLineAsync(MessageJson.Serialize(delivery));
            Message acknowledgement = MessageJson.Deserialize<Message>(
                await responseLine.WaitAsync(timeout.Token) ?? throw new IOException("No ACK after retry."));

            Assert.Equal(MessageType.Ack, acknowledgement.Type);
            Assert.Equal(messageId, acknowledgement.RelatedMessageId);
            Assert.Single(JsonSerializer.Deserialize<Dictionary<Guid, OrderPayload>>(File.ReadAllText(stateFile))!);
        }
        finally
        {
            if (!consumer.HasExited) consumer.Kill(entireProcessTree: true);
            await consumer.WaitForExitAsync();
        }
    }
}
