using Microsoft.Extensions.Hosting;

namespace DistributedApp.Broker;

public sealed class BrokerTcpHostedService(BrokerServer broker) : BackgroundService
{
    // Pornește livrarea TCP existentă în același proces cu serviciul gRPC.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        broker.RunAsync(stoppingToken);
}
