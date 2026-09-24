using DistributedApp.Consumer;

string settingsPath = Path.Combine(
    AppContext.BaseDirectory,
    "consumer.settings.json");

using CancellationTokenSource cancellation = new();

Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellation.Cancel();
};

try
{
    ConsumerSettings settings = ConsumerSettings.Load(settingsPath);
    ProcessedMessageStore store =
        new(settings.StateFilePath);
    MessageProcessor processor = new(store);
    ConsumerMessageHandler handler = new(
        processor,
        settings.SimulateCrashBeforeAcknowledgement);
    ConsumerClient consumer = new(
        settings.BrokerHost,
        settings.BrokerPort,
        handler);

    await consumer.RunAsync(settings.Topic, cancellation.Token);
    return 0;
}
catch (OperationCanceledException)
{
    return 0;
}
catch (SimulatedConsumerCrashException exception)
{
    ConsumerLog.Write(
        "Critical",
        "consumer_stopped",
        "simulated_crash",
        detail: exception.Message);
    return 2;
}
catch (Exception exception)
{
    ConsumerLog.Write(
        "Error",
        "consumer_stopped",
        "failure",
        detail: exception.Message);
    return 1;
}
