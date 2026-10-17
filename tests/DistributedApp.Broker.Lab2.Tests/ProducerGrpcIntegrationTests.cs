extern alias ProducerApp;

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Net.Http.Json;
using System.Text.Json;
using DistributedApp.Broker;
using DistributedApp.Broker.Grpc.V1;
using DistributedApp.Contracts;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DistributedApp.Broker.Lab2.Tests;

using GrpcClientService = ProducerApp::DistributedApp.Producer.GrpcClientService;
using ProducerSettings = ProducerApp::DistributedApp.Producer.ProducerSettings;
using PublishOutcome = ProducerApp::DistributedApp.Producer.PublishOutcome;
using PublishResult = ProducerApp::DistributedApp.Producer.PublishResult;
using ProducerApplication = ProducerApp::DistributedApp.Producer.Producer;

public sealed class ProducerGrpcIntegrationTests
{
    [Fact]
    public async Task ActualProducerResolvesLostReplyWithoutSecondPublication()
    {
        int grpcPort = FreePort();
        string stateFile = NewStateFile();
        using BrokerProcess broker = StartBroker(grpcPort, FreePort(), stateFile);
        await WaitUntilHealthy(grpcPort);

        ProducerSettings settings = Settings(grpcPort, timeoutMs: 500);
        using GrpcClientService grpc = new(settings);
        ProducerApplication producer = new(grpc, settings);

        PublishResult result = await producer.PublishOrderAsync(
            new OrderPayload("ORDER-LOSS", "Laptop", "Test response loss"),
            CancellationToken.None, simulateLostResponse: true);

        Assert.Equal(PublishOutcome.Accepted, result.Outcome);
        BrokerState state = MessageJson.Deserialize<BrokerState>(File.ReadAllText(stateFile));
        Assert.Equal(result.MessageId, Assert.Single(state.PublishedMessages).MessageId);
    }

    [Fact]
    public async Task UnavailableBrokerProducesExplicitUnknownWithinDeadline()
    {
        int grpcPort = FreePort();
        using GrpcClientService grpc = new(Settings(grpcPort, timeoutMs: 300));
        Message message = NewMessage();

        Stopwatch timer = Stopwatch.StartNew();
        PublishOutcome result = await grpc.SendAsync(message);

        Assert.Equal(PublishOutcome.Unknown, result);
        Assert.True(timer.Elapsed < TimeSpan.FromSeconds(5), "The fallback must not retry forever.");
    }

    [Fact]
    public async Task CancellationBeforePublishDoesNotCreateARecord()
    {
        int grpcPort = FreePort();
        string stateFile = NewStateFile();
        using BrokerProcess broker = StartBroker(grpcPort, FreePort(), stateFile);
        await WaitUntilHealthy(grpcPort);
        using GrpcClientService grpc = new(Settings(grpcPort, timeoutMs: 3000));
        using CancellationTokenSource cancellation = new();
        cancellation.Cancel();

        Exception? error = await Record.ExceptionAsync(() =>
            grpc.SendAsync(NewMessage(), ct: cancellation.Token));

        Assert.NotNull(error);
        Assert.True(error is OperationCanceledException
            || error is RpcException { StatusCode: StatusCode.Cancelled });
        Assert.False(File.Exists(stateFile));
    }

    [Fact]
    public async Task NotFoundFallbackRetriesWithTheSameMessageId()
    {
        int grpcPort = FreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(options =>
            options.ListenLocalhost(grpcPort, listen => listen.Protocols = HttpProtocols.Http2));
        builder.Services.AddGrpc();
        builder.Services.AddSingleton<RetryProbe>();
        WebApplication app = builder.Build();
        app.MapGrpcService<RetryProbeService>();
        await app.StartAsync();
        try
        {
            using GrpcClientService grpc = new(Settings(grpcPort, timeoutMs: 1000));
            PublishOutcome outcome = await grpc.SendAsync(NewMessage());
            RetryProbe probe = app.Services.GetRequiredService<RetryProbe>();

            Assert.Equal(PublishOutcome.Accepted, outcome);
            Assert.Equal(2, probe.MessageIds.Count);
            Assert.Equal(probe.MessageIds[0], probe.MessageIds[1]);
            Assert.Equal(1, probe.StatusChecks);
        }
        finally
        {
            await app.StopAsync();
            await app.DisposeAsync();
        }
    }

    [Fact]
    public async Task HealthCheckDoesNotCreateBrokerState()
    {
        int grpcPort = FreePort();
        string stateFile = NewStateFile();
        using BrokerProcess broker = StartBroker(grpcPort, FreePort(), stateFile);

        await WaitUntilHealthy(grpcPort);

        Assert.False(File.Exists(stateFile));
    }

    [Fact]
    public async Task HttpOrderEndpointResolvesLostGrpcReply()
    {
        int grpcPort = FreePort();
        int producerPort = FreePort();
        string stateFile = NewStateFile();
        using BrokerProcess broker = StartBroker(grpcPort, FreePort(), stateFile);
        await WaitUntilHealthy(grpcPort);
        using BrokerProcess producer = StartProducer(producerPort, grpcPort);
        using HttpClient http = new()
        {
            BaseAddress = new Uri($"http://127.0.0.1:{producerPort}"),
            Timeout = TimeSpan.FromSeconds(2)
        };
        await WaitUntilHttpReady(http, producer);

        using HttpResponseMessage response = await http.PostAsJsonAsync(
            "/api/orders?simulateLostResponse=true",
            new { orderId = "ORDER-HTTP-LOSS", product = "Laptop", description = "Test" });
        string body = await response.Content.ReadAsStringAsync();
        using JsonDocument result = JsonDocument.Parse(body);

        Assert.True(response.IsSuccessStatusCode, body);
        Assert.True(result.RootElement.GetProperty("accepted").GetBoolean());
        Assert.Equal("Accepted", result.RootElement.GetProperty("outcome").GetString());
        Assert.Single(MessageJson.Deserialize<BrokerState>(File.ReadAllText(stateFile)).PublishedMessages);
    }

    private static ProducerSettings Settings(int grpcPort, int timeoutMs) => new()
    {
        ProducerId = "producer-1",
        BrokerAddress = $"http://127.0.0.1:{grpcPort}",
        RequestTimeoutMilliseconds = timeoutMs
    };

    private static Message NewMessage() => new()
    {
        Type = MessageType.Publish,
        SenderId = "producer-1",
        Topic = "producer-1",
        Payload = "test-order"
    };

    private static string NewStateFile() => Path.Combine(
        Path.GetTempPath(), "producer-grpc-integration-" + Guid.NewGuid().ToString("N"), "state.json");

    private static BrokerProcess StartBroker(int grpcPort, int tcpPort, string stateFile)
    {
        ProcessStartInfo start = new("dotnet", $"\"{typeof(BrokerServer).Assembly.Location}\"")
        {
            UseShellExecute = false,
            CreateNoWindow = true
        };
        start.Environment["BROKER_GRPC_PORT"] = grpcPort.ToString();
        start.Environment["BROKER_TCP_PORT"] = tcpPort.ToString();
        start.Environment["BROKER_STATE_FILE"] = stateFile;
        return new BrokerProcess(Process.Start(start)!);
    }

    private static BrokerProcess StartProducer(int producerPort, int grpcPort)
    {
        ProcessStartInfo start = new("dotnet",
            $"\"{typeof(ProducerApplication).Assembly.Location}\" --urls http://127.0.0.1:{producerPort}")
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = Path.GetDirectoryName(typeof(ProducerApplication).Assembly.Location)!
        };
        start.Environment["ASPNETCORE_URLS"] = $"http://127.0.0.1:{producerPort}";
        start.Environment["Broker__GrpcAddress"] = $"http://127.0.0.1:{grpcPort}";
        start.Environment["Broker__RequestTimeoutMilliseconds"] = "500";
        return new BrokerProcess(Process.Start(start)!);
    }

    private static async Task WaitUntilHttpReady(HttpClient http, BrokerProcess producer)
    {
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        while (!timeout.IsCancellationRequested)
        {
            if (producer.HasExited)
                throw new InvalidOperationException($"Producer exited before HTTP became ready. {producer.Diagnostics}");
            try
            {
                // Testăm API-ul, nu pagina statică: în directorul de teste wwwroot nu este copiat.
                using HttpResponseMessage response = await http.GetAsync("/api/status", timeout.Token);
                if (response.IsSuccessStatusCode) return;
            }
            catch (HttpRequestException) { }
            catch (TaskCanceledException) when (timeout.IsCancellationRequested) { break; }
            catch (TaskCanceledException) { }
            try { await Task.Delay(100, timeout.Token); }
            catch (TaskCanceledException) { break; }
        }
        throw new TimeoutException($"Producer HTTP endpoint did not become ready. {producer.Diagnostics}");
    }

    private static async Task WaitUntilHealthy(int grpcPort)
    {
        using GrpcChannel channel = GrpcChannel.ForAddress($"http://127.0.0.1:{grpcPort}");
        BrokerRpcV1.BrokerRpcV1Client client = new(channel);
        for (int attempt = 0; attempt < 50; attempt++)
        {
            try
            {
                if ((await client.CheckHealthAsync(new HealthRequest(),
                        deadline: DateTime.UtcNow.AddSeconds(1))).Status == "Healthy") return;
            }
            catch (RpcException) { }
            await Task.Delay(100);
        }
        throw new TimeoutException("Broker gRPC did not become healthy.");
    }

    private static int FreePort()
    {
        TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class BrokerProcess : IDisposable
    {
        private readonly Process _process;
        private readonly List<string> _output = [];

        public BrokerProcess(Process process)
        {
            _process = process;
            if (!process.StartInfo.RedirectStandardOutput) return;
            process.OutputDataReceived += (_, args) => Add(args.Data);
            process.ErrorDataReceived += (_, args) => Add(args.Data);
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
        }

        private void Add(string? line)
        {
            if (line is null) return;
            lock (_output) _output.Add(line);
        }

        public bool HasExited => _process.HasExited;
        public string Diagnostics
        {
            get { lock (_output) return string.Join(" | ", _output); }
        }

        public void Dispose()
        {
            if (!_process.HasExited)
            {
                _process.Kill(entireProcessTree: true);
                _process.WaitForExit(5000);
            }
            _process.Dispose();
        }
    }
}

public sealed class RetryProbe
{
    public List<string> MessageIds { get; } = [];
    public int StatusChecks { get; set; }
}

public sealed class RetryProbeService(RetryProbe probe) : BrokerRpcV1.BrokerRpcV1Base
{
    public override Task<PublishReply> Publish(PublishRequest request, ServerCallContext context)
    {
        probe.MessageIds.Add(request.MessageId);
        if (probe.MessageIds.Count == 1)
            throw new RpcException(new Status(StatusCode.Unavailable, "Response unavailable before save."));

        return Task.FromResult(new PublishReply
        {
            MessageId = request.MessageId,
            Accepted = true,
            SchemaVersion = "1.0"
        });
    }

    public override Task<PublishStatusReply> GetPublishStatus(
        PublishStatusRequest request, ServerCallContext context)
    {
        probe.StatusChecks++;
        return Task.FromResult(new PublishStatusReply
        {
            MessageId = request.MessageId,
            Found = false,
            Status = "NotFound",
            SchemaVersion = "1.0"
        });
    }
}
