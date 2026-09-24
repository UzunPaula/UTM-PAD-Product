using System.Net.Sockets;
using System.Text;
using DistributedApp.Contracts;

namespace DistributedApp.Broker;

internal sealed class BrokerClientConnection : IAsyncDisposable
{
    private readonly TcpClient _client;
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    public BrokerClientConnection(TcpClient client)
    {
        _client = client;
        NetworkStream stream = client.GetStream();
        Reader = new StreamReader(
            stream,
            Encoding.UTF8,
            detectEncodingFromByteOrderMarks: false,
            leaveOpen: true);
        Writer = new StreamWriter(
            stream,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            leaveOpen: true)
        {
            AutoFlush = true,
            NewLine = "\n"
        };
    }

    public StreamReader Reader { get; }

    private StreamWriter Writer { get; }

    public async Task SendAsync(
        Message message,
        CancellationToken cancellationToken)
    {
        await _sendLock.WaitAsync(cancellationToken);

        try
        {
            await Writer.WriteLineAsync(
                MessageJson.Serialize(message).AsMemory(),
                cancellationToken);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        Reader.Dispose();
        await Writer.DisposeAsync();
        _client.Dispose();
        _sendLock.Dispose();
    }
}
