using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Broker;

public static class BrokerLog
{
    public static void Write(
        string level,
        string eventName,
        string result,
        Message? message = null,
        string? detail = null)
    {
        string json = JsonSerializer.Serialize(new
        {
            timestampUtc = DateTimeOffset.UtcNow,
            level,
            component = "broker",
            eventName,
            correlationId = message?.CorrelationId,
            messageId = message?.MessageId,
            topic = message?.Topic,
            retryCount = message?.RetryCount,
            result,
            detail
        });

        Console.WriteLine(json);
    }
}
