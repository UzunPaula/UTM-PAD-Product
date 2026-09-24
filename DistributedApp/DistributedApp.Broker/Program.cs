using DistributedApp.Broker;

string settingsPath = Path.Combine(
    AppContext.BaseDirectory,
    "broker.settings.json");

using CancellationTokenSource cancellation = new();

Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellation.Cancel();
};

try
{
    BrokerSettings settings = BrokerSettings.Load(settingsPath);
    PersistentBrokerStore store = new(settings.StateFilePath);
    BrokerServer server = new(settings, store);

    await server.RunAsync(cancellation.Token);
    return 0;
}
catch (OperationCanceledException)
{
    return 0;
}
catch (Exception exception)
{
    BrokerLog.Write(
        "Error",
        "broker_stopped",
        "failure",
        detail: exception.Message);
    return 1;
}
