using System.Text.Json;
using DistributedApp.Contracts;

namespace DistributedApp.Broker;

public sealed class PersistentBrokerStore
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web)
        {
            WriteIndented = true
        };

    private readonly string _filePath;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly BrokerState _state;

    public PersistentBrokerStore(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        _filePath = Path.GetFullPath(filePath);
        _state = Load();
    }

    public async Task<Message> EnqueueAsync(
        Message published,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);

        try
        {
            if (!_state.Queues.TryGetValue(
                    published.Topic,
                    out List<Message>? queue))
            {
                queue = new List<Message>();
                _state.Queues[published.Topic] = queue;
            }

            long sequenceNumber =
                _state.NextSequenceNumbers.GetValueOrDefault(
                    published.Topic,
                    1);

            Message delivery = CopyForDelivery(published, sequenceNumber);
            queue.Add(delivery);
            _state.NextSequenceNumbers[published.Topic] = sequenceNumber + 1;

            await SaveAsync(cancellationToken);
            return Copy(delivery);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<Message?> PeekAsync(
        string topic,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);

        try
        {
            return _state.Queues.TryGetValue(topic, out List<Message>? queue)
                   && queue.Count > 0
                ? Copy(queue[0])
                : null;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> AcknowledgeAsync(
        string topic,
        Guid messageId,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);

        try
        {
            if (!_state.Queues.TryGetValue(topic, out List<Message>? queue)
                || queue.Count == 0
                || queue[0].MessageId != messageId)
            {
                return false;
            }

            // Only the head can be acknowledged, which preserves topic order.
            queue.RemoveAt(0);
            await SaveAsync(cancellationToken);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<DeliveryFailureStatus> RecordFailureAsync(
        string topic,
        Guid messageId,
        string reason,
        int maxRetries,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);

        try
        {
            if (!_state.Queues.TryGetValue(topic, out List<Message>? queue)
                || queue.Count == 0
                || queue[0].MessageId != messageId)
            {
                return DeliveryFailureStatus.NotFound;
            }

            Message message = queue[0];
            message.RetryCount++;

            if (message.RetryCount <= maxRetries)
            {
                await SaveAsync(cancellationToken);
                return DeliveryFailureStatus.Retrying;
            }

            queue.RemoveAt(0);
            _state.DeadLetters.Add(new DeadLetterMessage
            {
                Message = Copy(message),
                Reason = reason,
                DeadLetteredAtUtc = DateTimeOffset.UtcNow
            });

            await SaveAsync(cancellationToken);
            return DeliveryFailureStatus.DeadLettered;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);

        try
        {
            return _state.DeadLetters
                .Select(entry => new DeadLetterMessage
                {
                    Message = Copy(entry.Message),
                    Reason = entry.Reason,
                    DeadLetteredAtUtc = entry.DeadLetteredAtUtc
                })
                .ToArray();
        }
        finally
        {
            _gate.Release();
        }
    }

    private BrokerState Load()
    {
        if (!File.Exists(_filePath))
        {
            return new BrokerState();
        }

        try
        {
            string json = File.ReadAllText(_filePath);
            return JsonSerializer.Deserialize<BrokerState>(json, JsonOptions)
                   ?? throw new InvalidDataException(
                       "The broker state file cannot contain null.");
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                "The broker state file contains invalid JSON.",
                exception);
        }
    }

    private async Task SaveAsync(CancellationToken cancellationToken)
    {
        string? directory = Path.GetDirectoryName(_filePath);

        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        // The queue head remains persisted until Ack. Replacing a complete
        // temporary file prevents a partial write from becoming broker state.
        string temporaryPath = _filePath + ".tmp";
        string json = JsonSerializer.Serialize(_state, JsonOptions);
        await File.WriteAllTextAsync(temporaryPath, json, cancellationToken);
        File.Move(temporaryPath, _filePath, overwrite: true);
    }

    private static Message CopyForDelivery(
        Message source,
        long sequenceNumber)
    {
        Message copy = Copy(source);
        copy.Type = MessageType.Message;
        copy.SequenceNumber = sequenceNumber;
        copy.RetryCount = 0;
        copy.RelatedMessageId = null;
        copy.Reason = null;
        return copy;
    }

    private static Message Copy(Message source)
    {
        return new Message
        {
            MessageId = source.MessageId,
            CorrelationId = source.CorrelationId,
            Type = source.Type,
            SchemaVersion = source.SchemaVersion,
            OccurredAtUtc = source.OccurredAtUtc,
            Topic = source.Topic,
            SequenceNumber = source.SequenceNumber,
            RetryCount = source.RetryCount,
            Payload = source.Payload,
            RelatedMessageId = source.RelatedMessageId,
            Reason = source.Reason
        };
    }
}
