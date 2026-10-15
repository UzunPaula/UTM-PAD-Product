using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Consumer;

public sealed record ProcessingResult(bool Accepted, bool NewlyProcessed, string? Reason)
{
    public static ProcessingResult Processed() => new(true, true, null);
    public static ProcessingResult Duplicate() => new(true, false, null);
    public static ProcessingResult Failed(string reason) => new(false, false, reason);
}

public sealed class ProcessedMessageStore
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly string _filePath;
    private readonly Dictionary<Guid, OrderPayload> _processedOrders;

    public ProcessedMessageStore(string filePath)
    {
        _filePath = Path.GetFullPath(filePath);
        _processedOrders = File.Exists(_filePath)
            ? JsonSerializer.Deserialize<Dictionary<Guid, OrderPayload>>(
                  File.ReadAllText(_filePath), JsonOptions)
              ?? throw new InvalidDataException("Consumer state cannot be null.")
            : [];
    }

    public async Task<ProcessingResult> ProcessAsync(
        Guid messageId, string payload, CancellationToken cancellationToken = default)
    {
        if (messageId == Guid.Empty)
            return ProcessingResult.Failed("MessageId is required.");

        OrderPayload? order;
        try
        {
            order = JsonSerializer.Deserialize<OrderPayload>(payload, JsonOptions);
        }
        catch (JsonException)
        {
            return ProcessingResult.Failed("Order payload is not valid JSON.");
        }

        if (order is null || string.IsNullOrWhiteSpace(order.OrderId)
            || string.IsNullOrWhiteSpace(order.Product)
            || string.IsNullOrWhiteSpace(order.Description))
            return ProcessingResult.Failed("OrderId, Product and Description are required.");

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            if (_processedOrders.TryGetValue(messageId, out OrderPayload? existing))
                return existing == order
                    ? ProcessingResult.Duplicate()
                    : ProcessingResult.Failed("MessageId was already processed with different content.");

            // The processed order and its deduplication key are saved together.
            _processedOrders.Add(messageId, order);
            try
            {
                await SaveAsync(cancellationToken);
            }
            catch
            {
                _processedOrders.Remove(messageId);
                throw;
            }

            return ProcessingResult.Processed();
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task SaveAsync(CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(_filePath)!);
        string temporaryPath = _filePath + ".tmp";
        string json = JsonSerializer.Serialize(_processedOrders, JsonOptions);
        await File.WriteAllTextAsync(temporaryPath, json, cancellationToken);
        File.Move(temporaryPath, _filePath, overwrite: true);
    }
}
