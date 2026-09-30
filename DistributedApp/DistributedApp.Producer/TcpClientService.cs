using System.Net.Sockets;
using System.Text;
using DistributedApp.Contracts;

namespace DistributedApp.Producer;

public sealed class TcpClientService(ProducerSettings settings)
{
    public async Task<Message> SendAsync(Message message, CancellationToken token = default)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(settings.RequestTimeoutMilliseconds);
        using TcpClient client = new();
        await client.ConnectAsync(settings.BrokerHost, settings.BrokerPort, timeout.Token);

        await using NetworkStream stream = client.GetStream();
        using StreamReader reader = new(stream, Encoding.UTF8, false, leaveOpen: true);
        await using StreamWriter writer = new(stream, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
        await writer.WriteLineAsync(MessageJson.Serialize(message).AsMemory(), timeout.Token);
        string line = await reader.ReadLineAsync(timeout.Token) ?? throw new IOException("Broker closed connection.");
        return MessageJson.Deserialize<Message>(line);
    }

    public async Task<BrokerStatus> GetStatusAsync(CancellationToken token)
    {
        Message reply = await SendAsync(new Message
        {
            Type = MessageType.StatusRequest,
            SenderId = settings.ProducerId,
            Topic = settings.ProducerId
        }, token);
        return MessageJson.Deserialize<BrokerStatus>(reply.Payload);
    }

    public Task<Message> RedriveAsync(Guid messageId, CancellationToken token) => SendAsync(new Message
    {
        Type = MessageType.RedriveRequest,
        SenderId = settings.ProducerId,
        Topic = settings.ProducerId,
        RelatedMessageId = messageId
    }, token);
}
