using System.Net.Sockets;
using System.Text;

namespace DistributedApp.Producer;

public class TcpClientService
{
    private const string BrokerHost = "127.0.0.1";
    private const int BrokerPort = 5000;

    public void Send(string message)
    {
        try
        {
            using TcpClient client = new TcpClient();

            client.Connect(BrokerHost, BrokerPort);

            using NetworkStream stream = client.GetStream();

            byte[] data = Encoding.UTF8.GetBytes(message);

            stream.Write(data, 0, data.Length);

            Console.WriteLine("Mesaj transmis prin TCP.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Eroare: {ex.Message}");
        }
    }
}