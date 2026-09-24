using System.Text.Json;

namespace DistributedApp.Consumer;

public sealed class ConsumerSettings
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    public string BrokerHost { get; set; } = "127.0.0.1";

    public int BrokerPort { get; set; } = 5000;

    public string Topic { get; set; } = "orders";

    public string StateFilePath { get; set; } =
        Path.Combine("data", "consumer-state.json");

    public bool SimulateCrashBeforeAcknowledgement { get; set; }

    public int ProcessingDelayMilliseconds { get; set; }

    public bool SimulateNack { get; set; }

    public static ConsumerSettings Load(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        string fullPath = Path.GetFullPath(filePath);

        if (!File.Exists(fullPath))
        {
            throw new FileNotFoundException(
                "The consumer settings file was not found.",
                fullPath);
        }

        ConsumerSettings settings;

        try
        {
            string json = File.ReadAllText(fullPath);
            settings =
                JsonSerializer.Deserialize<ConsumerSettings>(
                    json,
                    JsonOptions)
                ?? throw new InvalidDataException(
                    "The consumer settings cannot contain null.");
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                "The consumer settings contain invalid JSON.",
                exception);
        }

        settings.Validate();

        if (!Path.IsPathRooted(settings.StateFilePath))
        {
            string directory =
                Path.GetDirectoryName(fullPath)
                ?? Directory.GetCurrentDirectory();

            settings.StateFilePath = Path.GetFullPath(
                Path.Combine(directory, settings.StateFilePath));
        }

        return settings;
    }

    private void Validate()
    {
        if (string.IsNullOrWhiteSpace(BrokerHost))
        {
            throw new InvalidDataException(
                "BrokerHost is required.");
        }

        if (BrokerPort is < 1 or > 65535)
        {
            throw new InvalidDataException(
                "BrokerPort must be between 1 and 65535.");
        }

        if (string.IsNullOrWhiteSpace(Topic))
        {
            throw new InvalidDataException(
                "Topic is required.");
        }

        if (string.IsNullOrWhiteSpace(StateFilePath))
        {
            throw new InvalidDataException(
                "StateFilePath is required.");
        }

        if (ProcessingDelayMilliseconds is < 0 or > 2500)
        {
            throw new InvalidDataException(
                "ProcessingDelayMilliseconds must be between 0 and 2500.");
        }
    }
}
