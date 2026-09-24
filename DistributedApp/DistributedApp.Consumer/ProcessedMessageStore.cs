using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Consumer;

public sealed class ProcessedMessageStore
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web)
        {
            WriteIndented = true
        };

    private readonly string _filePath;
    private readonly Dictionary<Guid, OrderPayload> _processedOrders;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public ProcessedMessageStore(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        _filePath = Path.GetFullPath(filePath);
        _processedOrders = Load();
    }

    public int Count => _processedOrders.Count;

    public bool IsProcessed(Guid messageId)
    {
        return _processedOrders.ContainsKey(messageId);
    }

    public async Task<bool> TrySaveAsync(
        Guid messageId,
        OrderPayload order,
        CancellationToken cancellationToken = default)
    {
        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            if (_processedOrders.ContainsKey(messageId))
            {
                return false;
            }

            // The business effect and deduplication key share one durable
            // record, so a restart cannot observe one without the other.
            _processedOrders.Add(messageId, order);

            try
            {
                await SaveAsync(cancellationToken);
                return true;
            }
            catch
            {
                _processedOrders.Remove(messageId);
                throw;
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private Dictionary<Guid, OrderPayload> Load()
    {
        if (!File.Exists(_filePath))
        {
            return new Dictionary<Guid, OrderPayload>();
        }

        try
        {
            string json = File.ReadAllText(_filePath);

            return JsonSerializer.Deserialize<Dictionary<Guid, OrderPayload>>(
                       json,
                       JsonOptions)
                   ?? throw new InvalidDataException(
                       "The consumer state file cannot contain null.");
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                "The consumer state file contains invalid JSON.",
                exception);
        }
    }

    private async Task SaveAsync(CancellationToken cancellationToken)
    {
        string? directory = Path.GetDirectoryName(_filePath);

        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        // Write a complete temporary file first, then replace the state file.
        // A partial write must never become valid deduplication state.
        string temporaryPath = _filePath + ".tmp";
        string json = JsonSerializer.Serialize(_processedOrders, JsonOptions);

        await File.WriteAllTextAsync(
            temporaryPath,
            json,
            cancellationToken);

        File.Move(temporaryPath, _filePath, overwrite: true);
    }
}
