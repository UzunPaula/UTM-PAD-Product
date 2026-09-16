using DistributedApp.Contracts;

using System.Net;
using System.Net.Sockets;

// Brokerul ascultă conexiuni TCP pe portul 5000
TcpListener server = new TcpListener(IPAddress.Any, 5000);

server.Start();

Console.WriteLine("Broker pornit pe portul 5000.");
Console.WriteLine("Astept conexiuni...");

while (true)
{
    // Așteaptă conectarea unui Producer sau Consumer
    TcpClient client = await server.AcceptTcpClientAsync();

    Console.WriteLine("Un client s-a conectat.");
}