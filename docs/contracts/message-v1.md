# Message contract 1.0

All components exchange UTF-8 JSON Lines. Each TCP frame contains one JSON
object followed by a line-feed character.

## Envelope

| JSON field | Type | Rule |
| --- | --- | --- |
| `messageId` | UUID | Required and unique for every envelope. |
| `correlationId` | UUID | Required and stable for the complete request flow. |
| `messageType` | string | `Publish`, `Subscribe`, `Message`, `Ack`, or `Nack`. |
| `schemaVersion` | string | The supported Lab 1 version is `1.0`. |
| `occurredAt` | timestamp | Required and expressed in UTC. |
| `topic` | string | Required and non-empty. |
| `sequenceNumber` | integer | Non-negative ordering value inside a topic. |
| `retryCount` | integer | Non-negative delivery retry count. |
| `payload` | string | Required for `Publish` and `Message`. |
| `relatedMessageId` | UUID | Required for `Ack` and `Nack`. |
| `reason` | string | Required for `Nack`. |

An acknowledgement has its own `messageId`. The `relatedMessageId` identifies
the delivered message being acknowledged. This keeps message identity separate
from acknowledgement identity.

## Publish example

```json
{
  "messageId": "11111111-1111-1111-1111-111111111111",
  "correlationId": "22222222-2222-2222-2222-222222222222",
  "messageType": "Publish",
  "schemaVersion": "1.0",
  "occurredAt": "2026-09-23T10:30:00+00:00",
  "topic": "orders",
  "sequenceNumber": 7,
  "retryCount": 0,
  "payload": "{\"orderId\":\"ORD-7\",\"product\":\"Laptop\",\"quantity\":2}",
  "relatedMessageId": null,
  "reason": null
}
```

## Compatibility

Consumers reject unsupported schema versions and invalid required fields.
Unknown JSON fields are ignored so that compatible additions do not break
existing consumers. An incompatible contract change requires a new schema
version.
