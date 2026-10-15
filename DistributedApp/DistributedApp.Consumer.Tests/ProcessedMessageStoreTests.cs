using System.Text.Json;
using DistributedApp.Consumer;
using DistributedApp.Contracts;
using Xunit;

namespace DistributedApp.Consumer.Tests;

public sealed class ProcessedMessageStoreTests
{
    [Fact]
    public async Task SavedOrderIsRecognizedAfterRestart()
    {
        string file = NewStateFile();
        Guid messageId = Guid.NewGuid();
        string payload = JsonSerializer.Serialize(new OrderPayload("ORD-1", "Keyboard", "Mechanical"));

        ProcessingResult first = await new ProcessedMessageStore(file).ProcessAsync(messageId, payload);
        ProcessingResult afterRestart = await new ProcessedMessageStore(file).ProcessAsync(messageId, payload);

        Assert.True(first.Accepted);
        Assert.True(first.NewlyProcessed);
        Assert.True(afterRestart.Accepted);
        Assert.False(afterRestart.NewlyProcessed);
        Assert.Single(JsonSerializer.Deserialize<Dictionary<Guid, OrderPayload>>(File.ReadAllText(file))!);
    }

    [Fact]
    public async Task ReusedMessageIdWithDifferentOrderIsRejected()
    {
        string file = NewStateFile();
        Guid messageId = Guid.NewGuid();
        ProcessedMessageStore store = new(file);
        await store.ProcessAsync(messageId, JsonSerializer.Serialize(
            new OrderPayload("ORD-1", "Keyboard", "Mechanical")));

        ProcessingResult result = await store.ProcessAsync(messageId, JsonSerializer.Serialize(
            new OrderPayload("ORD-2", "Mouse", "Wireless")));

        Assert.False(result.Accepted);
        Assert.Single(JsonSerializer.Deserialize<Dictionary<Guid, OrderPayload>>(File.ReadAllText(file))!);
    }

    [Fact]
    public async Task ConcurrentAttemptsSaveTheEffectOnlyOnce()
    {
        string file = NewStateFile();
        Guid messageId = Guid.NewGuid();
        string payload = JsonSerializer.Serialize(new OrderPayload("ORD-1", "Keyboard", "Mechanical"));
        ProcessedMessageStore store = new(file);

        ProcessingResult[] results = await Task.WhenAll(Enumerable.Range(0, 10)
            .Select(_ => store.ProcessAsync(messageId, payload)));

        Assert.Equal(1, results.Count(x => x.NewlyProcessed));
        Assert.All(results, x => Assert.True(x.Accepted));
        Assert.Single(JsonSerializer.Deserialize<Dictionary<Guid, OrderPayload>>(File.ReadAllText(file))!);
    }

    [Theory]
    [InlineData("not-json")]
    [InlineData("{\"orderId\":\"\",\"product\":\"Keyboard\",\"description\":\"Mechanical\"}")]
    public async Task InvalidOrderIsNotSaved(string payload)
    {
        string file = NewStateFile();

        ProcessingResult result = await new ProcessedMessageStore(file).ProcessAsync(Guid.NewGuid(), payload);

        Assert.False(result.Accepted);
        Assert.False(File.Exists(file));
    }

    private static string NewStateFile() => Path.Combine(
        Path.GetTempPath(), "distributedapp-consumer-tests", Guid.NewGuid().ToString("N"), "state.json");
}
