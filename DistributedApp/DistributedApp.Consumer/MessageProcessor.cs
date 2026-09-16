using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Consumer;

public class MessageProcessor
{
    private readonly ProcessedMessageStore _processedMessageStore;

    public MessageProcessor(ProcessedMessageStore processedMessageStore)
    {
        _processedMessageStore = processedMessageStore;
    }

    public async Task<bool> ProcessAsync(Message message)
    {
        // Verificăm dacă mesajul a fost deja procesat
        if (_processedMessageStore.IsProcessed(message.MessageId))
        {
            Console.WriteLine(
                $"Duplicate message {message.MessageId}. Processing skipped.");

            return true;
        }

        try
        {
            Console.WriteLine($"Processing message: {message.MessageId}");
            Console.WriteLine($"Correlation ID: {message.CorrelationId}");
            Console.WriteLine($"Topic: {message.Topic}");

            // Transformăm Payload-ul JSON într-un OrderPayload
            var order = JsonSerializer.Deserialize<OrderPayload>(message.Payload);

            if (order == null)
            {
                Console.WriteLine("Invalid order payload.");
                return false;
            }

            Console.WriteLine($"Order ID: {order.OrderId}");
            Console.WriteLine($"Product: {order.Product}");
            Console.WriteLine($"Quantity: {order.Quantity}");

            // Mesajul se marchează ca procesat numai după succes
            await _processedMessageStore.MarkAsProcessedAsync(message.MessageId);

            Console.WriteLine("Message processed successfully.");

            return true;
        }
        catch (JsonException ex)
        {
            Console.WriteLine($"Invalid JSON payload: {ex.Message}");
            return false;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Processing failed: {ex.Message}");
            return false;
        }
    }
}