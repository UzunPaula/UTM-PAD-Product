using System.Net.Sockets;
using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Producer;

public sealed class Producer
{
    private readonly TcpClientService _tcpClient = new();

    public async Task StartAsync(
        CancellationToken cancellationToken = default)
    {
        Console.WriteLine("=== PRODUCER ===");
        Console.WriteLine("Producer pornit.");

        while (!cancellationToken.IsCancellationRequested)
        {
            Console.WriteLine();
            Console.WriteLine("1. Publish Order");
            Console.WriteLine("0. Exit");
            Console.Write(">> ");

            string? choice = Console.ReadLine();

            if (choice == "1")
            {
                await PublishOrderAsync(cancellationToken);
            }
            else if (choice == "0")
            {
                return;
            }
            else
            {
                Console.WriteLine("Opțiune invalidă.");
            }
        }
    }

    private async Task PublishOrderAsync(
        CancellationToken cancellationToken)
    {
        Console.Write("Order ID: ");
        string orderId = Console.ReadLine()?.Trim() ?? string.Empty;

        Console.Write("Product: ");
        string product = Console.ReadLine()?.Trim() ?? string.Empty;

        Console.Write("Quantity: ");
        bool hasValidQuantity = int.TryParse(
            Console.ReadLine(),
            out int quantity) && quantity > 0;

        if (string.IsNullOrWhiteSpace(orderId)
            || string.IsNullOrWhiteSpace(product)
            || !hasValidQuantity)
        {
            Console.WriteLine("Comandă invalidă. Verifică datele introduse.");
            return;
        }

        OrderPayload orderPayload = new()
        {
            OrderId = orderId,
            Product = product,
            Quantity = quantity
        };

        Message message = new()
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Type = MessageType.Publish,
            SchemaVersion = MessageSchema.CurrentVersion,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            Topic = "orders",
            SequenceNumber = 0,
            RetryCount = 0,
            Payload = JsonSerializer.Serialize(orderPayload)
        };

        try
        {
            Message response = await _tcpClient.SendAsync(
                message,
                cancellationToken);

            if (response.RelatedMessageId != message.MessageId)
            {
                throw new InvalidDataException(
                    "Răspunsul Brokerului nu corespunde mesajului trimis.");
            }

            if (response.Type == MessageType.Ack)
            {
                // ACK confirms that the Broker persisted the message.
                Console.WriteLine(
                    $"Comandă acceptată de Broker. messageId={message.MessageId}");
            }
            else if (response.Type == MessageType.Nack)
            {
                Console.WriteLine(
                    $"Comandă respinsă de Broker: {response.Reason ?? "motiv necunoscut"}");
            }
            else
            {
                throw new InvalidDataException(
                    $"Răspuns neașteptat de la Broker: {response.Type}.");
            }
        }
        catch (Exception exception) when (
            exception is IOException
            or SocketException
            or JsonException
            or InvalidDataException)
        {
            Console.WriteLine(
                $"Comanda nu a fost confirmată: {exception.Message}");
        }
    }
}
