using DistributedApp.Broker;

using CancellationTokenSource stop = new();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    stop.Cancel();
};

Console.WriteLine("BROKER: 0.0.0.0:5000");
Console.WriteLine("Ctrl+C opreste aplicatia.");
await new BrokerServer(5000).RunAsync(stop.Token);
