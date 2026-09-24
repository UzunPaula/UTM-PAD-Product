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
  "consumerConnected": null,
  "pendingMessages": null,
  "deadLetterMessages": null
}
```

The endpoint actively checks the TCP connection to the Broker. The current
protocol does not expose Consumer presence, queue length, or dead-letter count,
so those values are null and the interface displays them as unknown. If the
request fails, the interface displays the components as offline instead of
inventing status data.

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

## Ownership boundary

- UI branch: HTML, CSS, JavaScript, and this API agreement.
- Producer branch: web host, endpoint implementation, message creation, TCP
  connection, and Broker acknowledgement handling.
- Broker and Consumer branches: no UI dependency.
