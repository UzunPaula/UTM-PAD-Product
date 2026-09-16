using System.Text.Json;

namespace DistributedApp.Consumer;

public class ProcessedMessageStore
{
    private readonly string _filePath;
    private readonly HashSet<Guid> _processedIds;

    public ProcessedMessageStore(string filePath)
    {
        _filePath = filePath;
        _processedIds = LoadProcessedIds();
    }

    public bool IsProcessed(Guid messageId)
    {
        return _processedIds.Contains(messageId);
    }

    public async Task MarkAsProcessedAsync(Guid messageId)
    {
        if (!_processedIds.Add(messageId))
        {
            return;
        }

        await SaveAsync();
    }

    private HashSet<Guid> LoadProcessedIds()
    {
        if (!File.Exists(_filePath))
        {
            return new HashSet<Guid>();
        }

        try
        {
            string json = File.ReadAllText(_filePath);

            return JsonSerializer.Deserialize<HashSet<Guid>>(json)
                   ?? new HashSet<Guid>();
        }
        catch (JsonException)
        {
            Console.WriteLine("Warning: processed IDs file is invalid.");

            return new HashSet<Guid>();
        }
    }

    private async Task SaveAsync()
    {
        string json = JsonSerializer.Serialize(
            _processedIds,
            new JsonSerializerOptions
            {
                WriteIndented = true
            });

        await File.WriteAllTextAsync(_filePath, json);
    }
}