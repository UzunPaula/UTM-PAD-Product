using DistributedApp.Broker;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

int grpcPort = Port("BROKER_GRPC_PORT", 5001);
int tcpPort = Port("BROKER_TCP_PORT", 5000);
// Implicit: data/ la rădăcina clonei. BROKER_STATE_FILE poate suprascrie calea.
string stateFile = BrokerServer.ResolveStatePath(
    Environment.GetEnvironmentVariable("BROKER_STATE_FILE"), AppContext.BaseDirectory);

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
// gRPC folosește HTTP/2; listenerul TCP vechi rămâne separat pentru Consumer.
builder.WebHost.ConfigureKestrel(options =>
    options.ListenLocalhost(grpcPort, listen => listen.Protocols = HttpProtocols.Http2));
builder.Services.AddGrpc();
builder.Services.AddSingleton(new BrokerServer(tcpPort, stateFile));
builder.Services.AddHostedService<BrokerTcpHostedService>();

WebApplication app = builder.Build();
app.MapGrpcService<BrokerGrpcService>();
await app.RunAsync();

static int Port(string name, int fallback) =>
    int.TryParse(Environment.GetEnvironmentVariable(name), out int value) && value is > 0 and <= 65535
        ? value : fallback;
