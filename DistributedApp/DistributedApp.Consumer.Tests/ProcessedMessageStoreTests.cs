namespace DistributedApp.Consumer.Tests;

public class ProcessedMessageStoreTests
{
    [Fact]
    public void Constructor_RejectsCorruptedStateInsteadOfLosingDeduplication()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "distributed-app-consumer-tests",
            Guid.NewGuid().ToString("N"));
        string statePath = Path.Combine(directory, "state.json");

        Directory.CreateDirectory(directory);

        try
        {
            File.WriteAllText(statePath, "{invalid-json");

            Assert.Throws<InvalidDataException>(
                () => new ProcessedMessageStore(statePath));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
