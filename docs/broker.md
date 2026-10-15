# Broker

## Responsibility

For the Lab 2 gRPC publishing flow and its failure evidence, see
[`broker-lab2.md`](broker-lab2.md). This page also describes the original TCP
delivery protocol.

The broker accepts published messages over TCP or gRPC, stores them durably, and
delivers them to the consumer subscribed to the matching topic. The wire
protocol for the TCP path is JSON Lines and uses the versioned message contract.

## Delivery flow

1. Validate a Publish envelope.
2. Assign the next sequence number for its topic.
3. Persist the message before acknowledging the producer.
4. Deliver only the head of the topic queue.
5. Keep that message persisted and in flight until Ack.
6. Remove it after a matching Ack, then deliver the next message.

This one-in-flight rule preserves FIFO ordering within each topic.

## Retry and dead-letter queue

Nack, acknowledgement timeout, and consumer disconnection are delivery
failures. Each failure increments retryCount. A retry uses exponential
backoff with jitter and keeps the same messageId. When maxRetries is exceeded,
the message and failure reason are persisted in the dead-letter queue, and
delivery continues with the next queued message.

The current implementation uses three retries after the initial delivery and
an acknowledgement timeout of two seconds. These values are constants in the
Broker code, not external settings.

## Persistence and recovery

Publications, subscriptions, delivery states, retry counts, and dead-letter
entries are stored in `data/broker-state.json` at the repository root during
local development. The path can be overridden with `BROKER_STATE_FILE`.
The broker writes a
complete temporary file and replaces the state file only after the write
finishes. Corrupted state stops startup instead of silently losing messages.

An unacknowledged queue head remains in the state file. Therefore, after broker
restart or consumer reconnection, it is delivered again with the same
messageId. Consumer deduplication prevents the local effect from being
repeated.

## Live monitoring

The Broker handles `StatusRequest` messages without changing persistent
state. A `StatusResponse` reports the requested topic's active Consumer,
queued messages excluding the active in-flight delivery, in-flight count, and
persistent ACK and dead-letter entries. The Producer exposes this snapshot to
the dashboard through its HTTP status endpoint. A bounded list of the latest
100 acknowledgements lets the dashboard move a published session row to its
final delivered state without unbounded history growth.

## Verification

Run from the DistributedApp directory:

    dotnet test DistributedApp.Broker.Tests/DistributedApp.Broker.Tests.csproj

The tests cover persistence, FIFO ordering, acknowledgement, bounded retry,
timeout, Nack, dead-letter handling, corrupted state, configuration, and the
critical crash-before-acknowledgement recovery scenario over real TCP sockets.

## Guarantees and limits

- Messages accepted by the broker survive process restart.
- Delivery is at least once until acknowledgement or dead-lettering.
- FIFO order is guaranteed per topic, not globally across topics.
- The consumer effect is at most once for a stable messageId.
- One active connection is allowed per `consumerId`; different Consumers may
  subscribe to the same topic.
- JSON file persistence is intentionally single-process and is not designed
  for a replicated broker cluster.
