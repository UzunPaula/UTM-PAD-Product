using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Producer;

public class Producer
{
    private readonly TcpClientService _tcpClient;

    public Producer()
    {
        _tcpClient = new TcpClientService();
    }

    public void Start()
    {
        Console.WriteLine("=== PRODUCER ===");
        Console.WriteLine("Producer pornit.");

        while (true)
        {
            Console.WriteLine();
            Console.WriteLine("1. Publish Order");
            Console.WriteLine("0. Exit");
            Console.Write(">> ");

            string? choice = Console.ReadLine();

            if (choice == "1")
            {
                PublishOrder();
            }
            else if (choice == "0")
            {
                break;
            }
            else
            {
                Console.WriteLine("Opțiune invalidă.");
            }
        }
    }

    private void PublishOrder()
    {
        Console.Write("Order ID: ");
        string orderId = Console.ReadLine() ?? string.Empty;

        Console.Write("Product: ");
        string product = Console.ReadLine() ?? string.Empty;

        Console.Write("Quantity: ");
        int.TryParse(Console.ReadLine(), out int quantity);

        var orderPayload = new OrderPayload
        {
            OrderId = orderId,
            Product = product,
            Quantity = quantity
        };

        var message = new Message
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Topic = "orders",
            SchemaVersion = "1.0",
            SequenceNumber = 1,
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Payload = JsonSerializer.Serialize(orderPayload)
        };

        string json = JsonSerializer.Serialize(message);

        _tcpClient.Send(json);

        Console.WriteLine("Order trimis către Broker.");
    }
}