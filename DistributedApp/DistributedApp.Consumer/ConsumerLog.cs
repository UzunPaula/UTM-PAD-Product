using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Consumer;

public static class ConsumerLog
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    public static void Write(
        string level,
        string eventName,
        string result,
        Message? message = null,
        double? durationMs = null,
        string? detail = null)
    {
        string entry = JsonSerializer.Serialize(
            new
            {
                timestamp = DateTimeOffset.UtcNow,
                level,
                component = "consumer",
                eventName,
                correlationId = message?.CorrelationId,
                messageId = message?.MessageId,
                result,
                durationMs,
                detail
            },
            JsonOptions);

        Console.WriteLine(entry);
    }
}
