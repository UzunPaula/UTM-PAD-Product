# Consumer

## Responsibility

The consumer maintains a TCP connection to the broker, subscribes to one topic,
processes delivered order messages, and returns an `Ack` or `Nack` message.
Messages use the versioned contract documented in `docs/contracts/message-v1.md`.

## Processing flow

1. Load the external configuration from `consumer.settings.json`.
2. Load the persistent local state before connecting to the broker.
3. Connect to the broker and send a `Subscribe` message.
4. Read one JSON message per line from the TCP stream.
5. Validate the message envelope and order payload.
6. Persist the local effect together with `messageId`.
7. Send `Ack` only after the local effect is durable.

An invalid contract or payload produces `Nack` when the incoming identifiers are
usable. A malformed JSON line is rejected and recorded in the structured log.

## Deduplication and recovery

The state file stores processed orders by `messageId`. If the same message is
delivered again, the consumer does not repeat the local effect and returns
`Ack`. This supports the critical failure scenario in which the consumer saves
the effect and stops before the broker receives the acknowledgement.

Set `simulateCrashBeforeAcknowledgement` to `true` to reproduce that failure.
The first newly processed message is persisted and the process exits before
sending `Ack`. After restarting with the option set to `false`, a redelivered
message is recognized as a duplicate and acknowledged without repeating its
effect.

The state file is replaced atomically. Invalid state is treated as a startup
error instead of silently discarding deduplication information.

## Configuration

`consumer.settings.json` contains:

- `brokerHost`: broker host name or IP address;
- `brokerPort`: TCP port in the range 1-65535;
- `topic`: subscribed topic;
- `stateFilePath`: absolute path or a path relative to the settings file;
- `simulateCrashBeforeAcknowledgement`: controlled failure injection switch.

The repository contains only local example values and no secrets.

## Verification

Run the automated tests from the `DistributedApp` directory:

```text
dotnet test DistributedApp.Consumer.Tests/DistributedApp.Consumer.Tests.csproj
```

The tests verify successful processing, invalid payload handling,
deduplication, recovery after a crash before `Ack`, corrupted state rejection,
configuration validation, and the TCP subscribe-deliver-acknowledge flow.

## Guarantee and limit

For a stable `messageId`, the consumer applies the represented local order
effect at most once, including after restart. End-to-end at-least-once delivery
also depends on the broker retaining unacknowledged messages and redelivering
them. The local JSON state is appropriate for this laboratory increment; it is
not a substitute for a transactional database in a multi-instance deployment.
