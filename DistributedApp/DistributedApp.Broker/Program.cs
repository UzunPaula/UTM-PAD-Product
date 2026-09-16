using DistributedApp.Contracts;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

// Brokerul ascultă conexiuni TCP pe portul 5000
TcpListener server = new TcpListener(IPAddress.Any, 5000);

// Fiecare topic are propria coadă de mesaje
ConcurrentDictionary<string, ConcurrentQueue<Message>> queues = new();

// Reține ce Consumer este abonat la fiecare topic
ConcurrentDictionary<string, TcpClient> subscribers = new();

server.Start();

Console.WriteLine("Broker pornit pe portul 5000.");
Console.WriteLine("Astept conexiuni...");

while (true)
{
    TcpClient client = await server.AcceptTcpClientAsync();

    Console.WriteLine("Un client s-a conectat.");

    // Fiecare client este procesat separat
    _ = HandleClientAsync(client);
}

async Task HandleClientAsync(TcpClient client)
{
    NetworkStream clientStream = client.GetStream();
    StreamReader reader = new StreamReader(clientStream);

    while (client.Connected)
    {
        string? data = await reader.ReadLineAsync();

        if (data == null)
        {
            break;
        }

        try
        {
            // Transformă JSON-ul primit într-un obiect Message
            Message? message = JsonSerializer.Deserialize<Message>(data);

            if (message == null)
            {
                continue;
            }

            Console.WriteLine(
                $"Mesaj primit: {message.Type}, Topic: {message.Topic}");

            switch (message.Type)
            {
                case MessageType.Publish:

                    // Creează coada topicului dacă nu există
                    if (!queues.ContainsKey(message.Topic))
                    {
                        queues[message.Topic] = new ConcurrentQueue<Message>();
                    }

                    // Adaugă mesajul în coadă
                    queues[message.Topic].Enqueue(message);

                    Console.WriteLine($"Broker: mesaj adaugat in topicul '{message.Topic}'.");

                    // Verifică dacă există un Consumer abonat
                    if (subscribers.ContainsKey(message.Topic))
                    {
                        Message? nextMessage;

                        // Scoate primul mesaj din coadă
                        if (queues[message.Topic].TryDequeue(out nextMessage))
                        {
                            TcpClient consumer = subscribers[message.Topic];

                            NetworkStream consumerStream = consumer.GetStream();

                            StreamWriter writer = new StreamWriter(consumerStream);

                            // Brokerul livrează mesajul Consumerului
                            nextMessage.Type = MessageType.Message;

                            string json = JsonSerializer.Serialize(nextMessage);

                            await writer.WriteLineAsync(json);
                            await writer.FlushAsync();

                            Console.WriteLine($"Broker: mesaj trimis Consumerului pentru topicul '{message.Topic}'.");
                        }
                    }

                    break;

                case MessageType.Subscribe:

                    // Consumerul se abonează la un singur topic
                    subscribers[message.Topic] = client;

                    Console.WriteLine(
                        $"Broker: Consumer abonat la topicul '{message.Topic}'.");

                    break;

                case MessageType.Ack:

                    Console.WriteLine(
                        $"Broker: ACK pentru mesajul {message.MessageId}.");

                    break;

                case MessageType.Nack:

                    Console.WriteLine(
                        $"Broker: NACK pentru mesajul {message.MessageId}. Motiv: {message.Reason}");

                    break;

                default:

                    Console.WriteLine(
                        "Broker: tip de mesaj necunoscut.");

                    break;
            }
        }
        catch (JsonException)
        {
            Console.WriteLine(
                "Broker: mesaj JSON invalid.");
        }
    }

    client.Close();

    Console.WriteLine("Client deconectat.");
}