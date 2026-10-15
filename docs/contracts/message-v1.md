# Message contract 1.0

This document describes the TCP contract used by Broker and Consumer and by
the Producer status request. Lab 2 publication uses the separate gRPC contract
[`broker_v1.proto`](../../DistributedApp/DistributedApp.Broker/Protos/broker_v1.proto).
Each TCP frame contains one UTF-8 JSON object followed by a line-feed character.

## Envelope

| JSON field | Type | Rule |
| --- | --- | --- |
| `messageId` | UUID | Required and unique for every envelope. |
| `correlationId` | UUID | Required and stable for the complete request flow. |
| `messageType` | string | `Publish`, `Subscribe`, `Message`, `Ack`, `Nack`, `StatusRequest`, or `StatusResponse`. |
| `schemaVersion` | string | The supported Lab 1 version is `1.0`. |
| `occurredAt` | timestamp | Required and expressed in UTC. |
| `topic` | string | Required and non-empty. |
| `sequenceNumber` | integer | Non-negative ordering value inside a topic. |
| `retryCount` | integer | Non-negative delivery retry count. |
| `payload` | string | Required for `Publish`, `Message`, and `StatusResponse`. |
| `relatedMessageId` | UUID | Required for `Ack`, `Nack`, and `StatusResponse`. |
| `reason` | string | Required for `Nack`. |

An acknowledgement has its own `messageId`. For a delivery `Ack` or `Nack`,
`relatedMessageId` identifies the delivered message. For a `Subscribe` reply,
the Broker sends `Ack` after saving the subscription, or `Nack` with a reason
when it rejects the request; `relatedMessageId` then identifies the Subscribe
request. The reply keeps the request's `correlationId` and topic. This lets the
Consumer distinguish subscription confirmation from delivery acknowledgement.

## Monitoring exchange

A `StatusRequest` uses the requested topic and no payload. The Broker replies
with a `StatusResponse` whose `relatedMessageId` identifies the request. Its
payload contains the Consumer connection flag, queued and in-flight counts,
dead-letter count and summaries, and the observation timestamp. Monitoring
uses the same TCP framing and validation rules as delivery traffic.

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
  "payload": "{\"orderId\":\"ORD-7\",\"product\":\"Laptop\",\"description\":\"Portable computer\"}",
  "relatedMessageId": null,
  "reason": null
}
```

## Compatibility

Consumers reject unsupported schema versions and invalid required fields.
Unknown JSON fields are ignored so that compatible additions do not break
existing consumers. An incompatible contract change requires a new schema
version.
