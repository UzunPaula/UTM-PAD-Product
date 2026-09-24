using System.Text.Json;

namespace DistributedApp.Broker;

public sealed class BrokerSettings
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    public int Port { get; set; } = 5000;

    public string StateFilePath { get; set; } =
        Path.Combine("data", "broker-state.json");

    public int AcknowledgementTimeoutMilliseconds { get; set; } = 3000;

    public int RetryScanIntervalMilliseconds { get; set; } = 250;

    public int MaxRetries { get; set; } = 3;

    public int BaseRetryDelayMilliseconds { get; set; } = 100;

    public int MaxRetryDelayMilliseconds { get; set; } = 2000;

    public static BrokerSettings Load(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        string fullPath = Path.GetFullPath(filePath);

        if (!File.Exists(fullPath))
        {
            throw new FileNotFoundException(
                "The broker settings file was not found.",
                fullPath);
        }

        BrokerSettings settings;

        try
        {
            string json = File.ReadAllText(fullPath);
            settings = JsonSerializer.Deserialize<BrokerSettings>(
                           json,
                           JsonOptions)
                       ?? throw new InvalidDataException(
                           "The broker settings cannot contain null.");
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                "The broker settings contain invalid JSON.",
                exception);
        }

        settings.Validate();

        if (!Path.IsPathRooted(settings.StateFilePath))
        {
            string directory = Path.GetDirectoryName(fullPath)
                               ?? Directory.GetCurrentDirectory();
            settings.StateFilePath = Path.GetFullPath(
                Path.Combine(directory, settings.StateFilePath));
        }

        return settings;
    }

    private void Validate()
    {
        if (Port is < 1 or > 65535)
        {
            throw new InvalidDataException(
                "Port must be between 1 and 65535.");
        }

        if (string.IsNullOrWhiteSpace(StateFilePath))
        {
            throw new InvalidDataException("StateFilePath is required.");
        }

        if (AcknowledgementTimeoutMilliseconds <= 0)
        {
            throw new InvalidDataException(
                "AcknowledgementTimeoutMilliseconds must be positive.");
        }

        if (RetryScanIntervalMilliseconds <= 0)
        {
            throw new InvalidDataException(
                "RetryScanIntervalMilliseconds must be positive.");
        }

        if (MaxRetries < 0)
        {
            throw new InvalidDataException("MaxRetries cannot be negative.");
        }

        if (BaseRetryDelayMilliseconds <= 0
            || MaxRetryDelayMilliseconds < BaseRetryDelayMilliseconds)
        {
            throw new InvalidDataException(
                "Retry delays must be positive and correctly ordered.");
        }
    }
}
