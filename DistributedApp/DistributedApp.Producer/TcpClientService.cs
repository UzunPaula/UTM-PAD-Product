using System.Net.Sockets;
using System.Text;
using DistributedApp.Contracts;

namespace DistributedApp.Producer;

public sealed class TcpClientService
{
    private const string BrokerHost = "127.0.0.1";
    private const int BrokerPort = 5000;

    public async Task<Message> SendAsync(
        Message message,
        CancellationToken cancellationToken = default)
    {
        using TcpClient client = new();
        await client.ConnectAsync(
            BrokerHost,
            BrokerPort,
            cancellationToken);

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

        await writer.WriteLineAsync(
            MessageJson.Serialize(message).AsMemory(),
            cancellationToken);

        string? responseLine = await reader.ReadLineAsync(cancellationToken);
        if (responseLine is null)
        {
            throw new IOException(
                "Brokerul a închis conexiunea fără un răspuns.");
        }

        return MessageJson.Deserialize(responseLine);
    }
}
