using DistributedApp.Broker;
using DistributedApp.Contracts;
using Xunit;

namespace DistributedApp.Broker.Lab2.Tests;

public sealed class BrokerPublishTests
{
    [Fact]
    public void DefaultPathIsOutsideBinAndExistingStateIsCopiedOnce()
    {
        string root = Path.Combine(Path.GetTempPath(), "broker-path-test-" + Guid.NewGuid().ToString("N"));
        string solutionDirectory = Path.Combine(root, "DistributedApp");
        string outputDirectory = Path.Combine(solutionDirectory, "DistributedApp.Broker", "bin", "Debug", "net8.0");
        Directory.CreateDirectory(outputDirectory);
        File.WriteAllText(Path.Combine(solutionDirectory, "DistributedApp.sln"), string.Empty);
        string legacyFile = Path.Combine(outputDirectory, "broker-state.json");
        File.WriteAllText(legacyFile, "old-state");

        string selected = BrokerServer.ResolveStatePath(null, outputDirectory);

        Assert.Equal(Path.Combine(root, "data", "broker-state.json"), selected);
        Assert.Equal("old-state", File.ReadAllText(selected));
        Assert.Equal("old-state", File.ReadAllText(legacyFile));

        File.WriteAllText(selected, "new-state");
        Assert.Equal(selected, BrokerServer.ResolveStatePath(null, outputDirectory));
        Assert.Equal("new-state", File.ReadAllText(selected));
    }

    [Fact]
    public void ExplicitStatePathDoesNotTriggerMigration()
    {
        string root = Path.Combine(Path.GetTempPath(), "broker-path-test-" + Guid.NewGuid().ToString("N"));
        string configured = Path.Combine(root, "custom", "state.json");

        Assert.Equal(configured, BrokerServer.ResolveStatePath(configured, root));
        Assert.False(File.Exists(configured));
    }

    [Fact]
    public void RepeatedMessageIdDoesNotCreateAnotherPublication()
    {
        string path = TempStatePath();
        Message message = NewMessage();
        BrokerServer broker = new(0, path);

        Assert.False(broker.PublishIdempotent(message));
        Assert.True(broker.PublishIdempotent(message));
        Assert.Single(MessageJson.Deserialize<BrokerState>(File.ReadAllText(path)).PublishedMessages);
    }

    [Fact]
    public void LostReplyCanBeResolvedByStatusAfterBrokerRestart()
    {
        string path = TempStatePath();
        Message message = NewMessage();
        new BrokerServer(0, path).PublishIdempotent(message);

        BrokerServer restarted = new(0, path);
        Assert.True(restarted.ContainsPublished(message.MessageId));
        Assert.True(restarted.PublishIdempotent(message));
        Assert.Single(MessageJson.Deserialize<BrokerState>(File.ReadAllText(path)).PublishedMessages);
    }

    [Fact]
    public void ReusingMessageIdForDifferentContentIsRejected()
    {
        BrokerServer broker = new(0, TempStatePath());
        Message first = NewMessage();
        broker.PublishIdempotent(first);
        Message changed = NewMessage();
        changed.MessageId = first.MessageId;

        Assert.Throws<InvalidOperationException>(() => broker.PublishIdempotent(changed));
    }

    private static Message NewMessage() => new()
    {
        Type = MessageType.Publish,
        SenderId = "test-producer",
        Topic = "orders-producer",
        Payload = Guid.NewGuid().ToString()
    };

    private static string TempStatePath() => Path.Combine(
        Path.GetTempPath(), "broker-lab2-" + Guid.NewGuid().ToString("N"), "broker-state.json");
}
