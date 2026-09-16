using System.Net.Sockets;

// Se conectează la Broker pe portul 5000
TcpClient client = new TcpClient();

await client.ConnectAsync("127.0.0.1", 5000);

Console.WriteLine("Producer conectat la Broker.");