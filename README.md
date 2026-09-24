# Distributed Orders

Distributed Orders demonstrates reliable communication between a Producer, a
TCP message Broker, and a Consumer. The solution uses a shared, versioned JSON
Lines contract and includes a web dashboard for publishing and monitoring
orders.

## Main capabilities

- durable FIFO queues per topic;
- producer acknowledgement only after persistent enqueueing;
- consumer subscriptions and ACK/NACK handling;
- bounded retries with exponential backoff and jitter;
- persistent dead-letter queue;
- consumer deduplication and recovery after a crash before ACK;
- structured component logs;
- live dashboard status for Broker, Consumer, queued, in-flight, and
  dead-letter messages;
- automated contract, storage, component, and TCP integration tests.

Runtime state is stored in local JSON files. No database is required.

## Requirements

- .NET SDK 8.0;
- ports 5000 and 5080 available locally.

## Build and test

Run from the repository root:

```powershell
dotnet build DistributedApp/DistributedApp.sln
dotnet test DistributedApp/DistributedApp.sln
```

## Run the complete application

Open three terminals and start the components in this order:

```powershell
dotnet run --project DistributedApp/DistributedApp.Broker
```

```powershell
dotnet run --project DistributedApp/DistributedApp.Consumer
```

```powershell
dotnet run --project DistributedApp/DistributedApp.Producer
```

Open http://127.0.0.1:5080. A valid order follows this flow:

```text
Dashboard -> Producer -> Broker -> Consumer -> ACK
```

The dashboard refreshes twice per second. It reports whether the Consumer is
subscribed to the `orders` topic, queued and in-flight message counts,
confirmed deliveries, and dead-letter details. A configurable 1200 ms Consumer
processing delay makes the in-flight transition visible during demonstration.

## Demonstrate delivery states

1. Start Broker and Producer while Consumer is stopped.
2. Publish an order. The queued count increases.
3. Start Consumer. The order moves through in-flight to delivered, the queued
   count returns to zero, and confirmed deliveries increases.
4. To demonstrate bounded retries and dead-lettering, stop Consumer, set
   `simulateNack` to `true` in `consumer.settings.json`, rebuild and restart
   Consumer, then publish an order.
5. The dashboard shows every in-flight retry and finally lists the message in
   the dead-letter table. Set `simulateNack` back to `false` after the
   demonstration.

## Persistence and recovery

The Broker persists queues and dead letters in
`DistributedApp.Broker/data/broker-state.json` relative to its deployed
settings file. The Consumer persists processed message identifiers in
`DistributedApp.Consumer/data/consumer-state.json`.

Set `simulateCrashBeforeAcknowledgement` to `true` in
`consumer.settings.json` to demonstrate the crash-before-ACK scenario. After
the controlled crash, set it back to `false` and restart the Consumer. The
Broker redelivers the same message, while Consumer deduplication prevents the
order effect from being applied twice.

## Documentation

- `docs/contracts/message-v1.md`: wire contract;
- `docs/broker.md`: queue, retry, persistence, and dead-letter behavior;
- `docs/consumer.md`: processing, deduplication, and recovery;
- `docs/ui-api.md`: dashboard HTTP API.
