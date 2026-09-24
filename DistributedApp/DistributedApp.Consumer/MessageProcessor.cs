using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Consumer;

public sealed class MessageProcessor
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly ProcessedMessageStore _processedMessageStore;

    public MessageProcessor(ProcessedMessageStore processedMessageStore)
    {
        _processedMessageStore = processedMessageStore;
    }

    public async Task<ProcessingResult> ProcessAsync(
        Message message,
        CancellationToken cancellationToken = default)
    {
        if (_processedMessageStore.IsProcessed(message.MessageId))
        {
            return ProcessingResult.Duplicate();
        }

        try
        {
            OrderPayload? order =
                JsonSerializer.Deserialize<OrderPayload>(
                    message.Payload,
                    JsonOptions);

            if (order == null)
            {
                return ProcessingResult.Failed(
                    "The order payload cannot be null.");
            }

            string? validationError = Validate(order);

            if (validationError != null)
            {
                return ProcessingResult.Failed(validationError);
            }

            bool saved = await _processedMessageStore.TrySaveAsync(
                message.MessageId,
                order,
                cancellationToken);

            return saved
                ? ProcessingResult.Processed()
                : ProcessingResult.Duplicate();
        }
        catch (JsonException)
        {
            return ProcessingResult.Failed(
                "The order payload is not valid JSON.");
        }
        catch (IOException exception)
        {
            return ProcessingResult.Failed(
                $"The local effect could not be saved: {exception.Message}");
        }
    }

    private static string? Validate(OrderPayload order)
    {
        if (string.IsNullOrWhiteSpace(order.OrderId))
        {
            return "OrderId is required.";
        }

        if (string.IsNullOrWhiteSpace(order.Product))
        {
            return "Product is required.";
        }

        if (order.Quantity <= 0)
        {
            return "Quantity must be greater than zero.";
        }

        return null;
    }
}
