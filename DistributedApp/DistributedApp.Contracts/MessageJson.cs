using System.Text.Json;
using System.Text.Json.Serialization;

namespace DistributedApp.Contracts;

public static class MessageJson
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = false
    };

    static MessageJson()
    {
        SerializerOptions.Converters.Add(
            new JsonStringEnumConverter(
                namingPolicy: null,
                allowIntegerValues: false));
    }

    public static string Serialize(Message message)
    {
        ArgumentNullException.ThrowIfNull(message);

        return JsonSerializer.Serialize(message, SerializerOptions);
    }

    public static Message Deserialize(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);

        return JsonSerializer.Deserialize<Message>(json, SerializerOptions)
               ?? throw new JsonException("The message body cannot be null.");
    }
}
