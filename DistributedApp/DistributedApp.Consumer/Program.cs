using System.Net.Sockets;
using System.Text;
using DistributedApp.Contracts;

string consumerId = args.ElementAtOrDefault(0) ?? "consumer-" + Guid.NewGuid().ToString("N")[..8];
string producerId = args.ElementAtOrDefault(1) ?? "orders-producer";
bool rejectMessages = args.ElementAtOrDefault(2)?.Equals("fail", StringComparison.OrdinalIgnoreCase) == true;

using TcpClient client = new();
await client.ConnectAsync("127.0.0.1", 5000);
Console.WriteLine($"CONSUMER ID: {consumerId}");
Console.WriteLine($"LOCAL ADDRESS: {client.Client.LocalEndPoint}");
Console.WriteLine($"SUBSCRIBED PRODUCER: {producerId}");
Console.WriteLine(rejectMessages ? "MODE: NACK (DLQ demo)" : "MODE: ACK");

await using NetworkStream stream = client.GetStream();
using StreamReader reader = new(stream, Encoding.UTF8, false, leaveOpen: true);
await using StreamWriter writer = new(stream, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };

await writer.WriteLineAsync(MessageJson.Serialize(new Message
{
    Type = MessageType.Subscribe,
    SenderId = consumerId,
    Topic = producerId
}));

while (await reader.ReadLineAsync() is { } line)
{
    Message message = MessageJson.Deserialize<Message>(line);
    if (message.Type != MessageType.Delivery) continue;

    Console.WriteLine($"RECEIVED: {message.MessageId} | {message.Payload}");
    bool accepted = !rejectMessages;
    Message response = new()
    {
        Type = accepted ? MessageType.Ack : MessageType.Nack,
        SenderId = consumerId,
        Topic = producerId,
        RelatedMessageId = message.MessageId,
        CorrelationId = message.CorrelationId,
        Reason = accepted ? null : "Consumer started in FAIL mode."
    };
    await writer.WriteLineAsync(MessageJson.Serialize(response));
    Console.WriteLine(accepted ? "ACK sent" : "NACK sent");
}
