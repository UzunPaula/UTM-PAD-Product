namespace DistributedApp.Broker.Tests;

public class BrokerSettingsTests
{
    [Fact]
    public void Load_ReadsConfigurationAndResolvesStatePath()
    {
        string directory = CreateDirectory();
        string settingsPath = Path.Combine(
            directory,
            "broker.settings.json");

        try
        {
            File.WriteAllText(
                settingsPath,
                """
                {
                  "port": 5100,
                  "stateFilePath": "data/state.json",
                  "acknowledgementTimeoutMilliseconds": 1000,
                  "retryScanIntervalMilliseconds": 100,
                  "maxRetries": 2,
                  "baseRetryDelayMilliseconds": 50,
                  "maxRetryDelayMilliseconds": 500
                }
                """);

            BrokerSettings settings = BrokerSettings.Load(settingsPath);

            Assert.Equal(5100, settings.Port);
            Assert.Equal(
                Path.Combine(directory, "data", "state.json"),
                settings.StateFilePath);
            Assert.Equal(2, settings.MaxRetries);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void Load_RejectsNegativeRetryLimit()
    {
        string directory = CreateDirectory();
        string settingsPath = Path.Combine(
            directory,
            "broker.settings.json");

        try
        {
            File.WriteAllText(
                settingsPath,
                """
                {
                  "port": 5000,
                  "stateFilePath": "state.json",
                  "acknowledgementTimeoutMilliseconds": 1000,
                  "retryScanIntervalMilliseconds": 100,
                  "maxRetries": -1,
                  "baseRetryDelayMilliseconds": 50,
                  "maxRetryDelayMilliseconds": 500
                }
                """);

            Assert.Throws<InvalidDataException>(
                () => BrokerSettings.Load(settingsPath));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static string CreateDirectory()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "distributed-app-broker-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        return directory;
    }
}
