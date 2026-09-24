namespace DistributedApp.Consumer.Tests;

public class ConsumerSettingsTests
{
    [Fact]
    public void Load_ReadsExternalConfigurationAndResolvesStatePath()
    {
        string directory = CreateTestDirectory();
        string settingsPath = Path.Combine(
            directory,
            "consumer.settings.json");

        try
        {
            File.WriteAllText(
                settingsPath,
                """
                {
                  "brokerHost": "127.0.0.1",
                  "brokerPort": 5000,
                  "topic": "orders",
                  "stateFilePath": "data/state.json",
                  "simulateCrashBeforeAcknowledgement": true,
                  "processingDelayMilliseconds": 1200,
                  "simulateNack": true
                }
                """);

            ConsumerSettings settings =
                ConsumerSettings.Load(settingsPath);

            Assert.Equal("127.0.0.1", settings.BrokerHost);
            Assert.Equal(5000, settings.BrokerPort);
            Assert.Equal("orders", settings.Topic);
            Assert.Equal(
                Path.Combine(directory, "data", "state.json"),
                settings.StateFilePath);
            Assert.True(settings.SimulateCrashBeforeAcknowledgement);
            Assert.Equal(1200, settings.ProcessingDelayMilliseconds);
            Assert.True(settings.SimulateNack);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void Load_RejectsInvalidPort()
    {
        string directory = CreateTestDirectory();
        string settingsPath = Path.Combine(
            directory,
            "consumer.settings.json");

        try
        {
            File.WriteAllText(
                settingsPath,
                """
                {
                  "brokerHost": "127.0.0.1",
                  "brokerPort": 0,
                  "topic": "orders",
                  "stateFilePath": "data/state.json"
                }
                """);

            Assert.Throws<InvalidDataException>(
                () => ConsumerSettings.Load(settingsPath));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void Load_RejectsProcessingDelayAboveAcknowledgementWindow()
    {
        string directory = CreateTestDirectory();
        string settingsPath = Path.Combine(
            directory,
            "consumer.settings.json");

        try
        {
            File.WriteAllText(
                settingsPath,
                """
                {
                  "brokerHost": "127.0.0.1",
                  "brokerPort": 5000,
                  "topic": "orders",
                  "stateFilePath": "data/state.json",
                  "processingDelayMilliseconds": 3000
                }
                """);

            Assert.Throws<InvalidDataException>(
                () => ConsumerSettings.Load(settingsPath));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static string CreateTestDirectory()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "distributed-app-consumer-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);
        return directory;
    }
}
