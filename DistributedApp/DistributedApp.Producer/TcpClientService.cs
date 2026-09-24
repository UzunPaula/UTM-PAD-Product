using System.Net.Sockets;
using System.Text;
using DistributedApp.Contracts;

namespace DistributedApp.Producer;

public sealed class TcpClientService
{
    private readonly ProducerSettings _settings;

    public TcpClientService(ProducerSettings settings)
    {
        _settings = settings;
    }

    public async Task<Message> SendAsync(
        Message message,
        CancellationToken cancellationToken = default)
    {
        using CancellationTokenSource timeout =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_settings.RequestTimeoutMilliseconds);

        using TcpClient client = new();
        try
        {
            await client.ConnectAsync(
                _settings.BrokerHost,
                _settings.BrokerPort,
                timeout.Token);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                "Conectarea la Broker a depășit timpul permis.");
        }

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
            timeout.Token);

        string? responseLine;
        try
        {
            responseLine = await reader.ReadLineAsync(timeout.Token);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                "Brokerul nu a răspuns în timpul permis.");
        }

        if (responseLine is null)
        {
            throw new IOException(
                "Brokerul a închis conexiunea fără un răspuns.");
        }

        return MessageJson.Deserialize(responseLine);
    }

    public async Task<bool> CanConnectAsync(
        CancellationToken cancellationToken = default)
    {
        using CancellationTokenSource timeout =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_settings.RequestTimeoutMilliseconds);

        try
        {
            using TcpClient client = new();
            await client.ConnectAsync(
                _settings.BrokerHost,
                _settings.BrokerPort,
                timeout.Token);
            return true;
        }
        catch (Exception exception) when (
            exception is SocketException or OperationCanceledException)
        {
            return false;
        }
    }
}
