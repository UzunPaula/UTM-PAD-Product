# User interface integration

The user interface is a small dashboard served from the Producer application.
It does not connect directly to the TCP broker. The Producer backend remains
responsible for creating the message envelope, publishing it, and interpreting
the broker acknowledgement.

## Files

Static files are in DistributedApp/DistributedApp.Producer/wwwroot. Keeping the
interface separate from the Producer C# implementation lets the Producer work
continue independently and reduces merge conflicts.

## Required HTTP endpoints

### GET /api/status

Successful response:

```json
{
  "brokerConnected": true,
  "consumerConnected": true,
  "pendingMessages": 2,
  "inFlightMessages": 1,
  "deadLetterMessages": 1,
  "acknowledgedMessages": 7,
  "recentAcknowledgements": [
    {
      "messageId": "44444444-4444-4444-4444-444444444444",
      "correlationId": "55555555-5555-5555-5555-555555555555",
      "topic": "orders",
      "acknowledgedAtUtc": "2026-09-24T17:30:30+00:00"
    }
  ],
  "deadLetters": [
    {
      "messageId": "c2a3e115-6924-4ed1-a677-f3a03dd42931",
      "correlationId": "2683f844-4773-42c2-b0d4-ae770af630a0",
      "topic": "orders",
      "retryCount": 4,
      "reason": "Acknowledgement timeout.",
      "deadLetteredAtUtc": "2026-09-24T17:30:00+00:00"
    }
  ],
  "observedAtUtc": "2026-09-24T17:31:00+00:00"
}
```

The Producer sends a `StatusRequest` over the normal TCP protocol. The Broker
returns a validated `StatusResponse` built from its subscriber registry,
in-flight deliveries, and persistent store. The dashboard refreshes these
values every 500 milliseconds and reconciles session rows with recent ACK and
dead-letter identifiers. If the request fails, the endpoint reports the
Broker and Consumer as offline and returns empty counters.

### POST /api/orders

Request:

```json
{
  "orderId": "ORD-12345678",
  "product": "Keyboard",
  "quantity": 1
}
```

Successful response after the Broker acknowledges durable enqueueing:

```json
{
  "accepted": true,
  "messageId": "c2a3e115-6924-4ed1-a677-f3a03dd42931",
  "correlationId": "2683f844-4773-42c2-b0d4-ae770af630a0"
}
```

Rejected response:

```json
{
  "accepted": false,
  "reason": "Broker is unavailable."
}
```

Use an appropriate 4xx or 5xx HTTP status for rejected requests. The interface
keeps only a session history in memory; it adds no database or client-side
persistence.

## Component boundary

- The browser communicates only with the Producer HTTP API.
- The Producer communicates with the Broker through versioned JSON Lines.
- The Broker owns queue, subscriber, in-flight, retry, and dead-letter state.
- The Consumer has no dependency on the web interface.
