using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Consumer;

public class ConsumerClient
{
    private readonly string _host;
    private readonly int _port;

    private TcpClient? _client;
    private NetworkStream? _stream;

    public ConsumerClient(string host, int port)
    {
        _host = host;
        _port = port;
    }

    public async Task ConnectAsync()
    {
        _client = new TcpClient();

        Console.WriteLine($"Connecting to broker {_host}:{_port}...");

        await _client.ConnectAsync(_host, _port);

        _stream = _client.GetStream();

        Console.WriteLine("Connected to broker successfully.");
    }

    public async Task SubscribeAsync(string topic)
    {
        if (_stream == null)
        {
            throw new InvalidOperationException(
                "Consumer is not connected to the broker.");
        }

        var subscribeRequest = new
        {
            Type = MessageType.Subscribe,
            Topic = topic
        };

        string json = JsonSerializer.Serialize(subscribeRequest);

        byte[] data = Encoding.UTF8.GetBytes(json + "\n");

        await _stream.WriteAsync(data);

        Console.WriteLine($"Subscription request sent for topic '{topic}'.");
    }
}