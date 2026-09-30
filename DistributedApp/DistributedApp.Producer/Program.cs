using DistributedApp.Contracts;
using DistributedApp.Producer;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
ProducerSettings settings = new()
{
    ProducerId = builder.Configuration["Producer:Id"] ?? "producer-1",
    BrokerHost = builder.Configuration["Broker:Host"] ?? "127.0.0.1",
    BrokerPort = builder.Configuration.GetValue("Broker:Port", 5000),
    RequestTimeoutMilliseconds = builder.Configuration.GetValue("Broker:RequestTimeoutMilliseconds", 3000)
};

builder.Services.AddSingleton(settings);
builder.Services.AddSingleton<TcpClientService>();
builder.Services.AddSingleton<Producer>();
WebApplication app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles(new StaticFileOptions
{
    OnPrepareResponse = context => context.Context.Response.Headers.CacheControl = "no-store"
});

app.MapPost("/api/orders", async (OrderPayload order, Producer producer, CancellationToken token) =>
{
    try
    {
        PublishResult result = await producer.PublishOrderAsync(order, token);
        return result.Accepted ? Results.Ok(result) : Results.BadRequest(result);
    }
    catch
    {
        return Results.Json(new { accepted = false, reason = "Broker offline." }, statusCode: 503);
    }
});

app.MapGet("/api/status", async (TcpClientService tcp, CancellationToken token) =>
{
    try
    {
        BrokerStatus status = await tcp.GetStatusAsync(token);
        return Results.Ok(new
        {
            brokerConnected = true,
            producerId = settings.ProducerId,
            status.ConsumerConnected,
            status.ConsumerCount,
            status.PendingMessages,
            status.InFlightMessages,
            status.DeadLetterMessages,
            status.AcknowledgedMessages,
            status.ConsumerEndpoints,
            status.RecentAcknowledgements,
            status.DeadLetters,
            status.ObservedAtUtc
        });
    }
    catch
    {
        return Results.Ok(new
        {
            brokerConnected = false,
            producerId = settings.ProducerId,
            consumerConnected = false,
            consumerCount = 0,
            pendingMessages = 0,
            inFlightMessages = 0,
            deadLetterMessages = 0,
            acknowledgedMessages = 0,
            consumerEndpoints = Array.Empty<string>(),
            recentAcknowledgements = Array.Empty<AcknowledgementInfo>(),
            deadLetters = Array.Empty<DeadLetterInfo>(),
            observedAtUtc = DateTimeOffset.UtcNow
        });
    }
});

app.MapPost("/api/dead-letters/{messageId:guid}/redrive", async (
    Guid messageId, TcpClientService tcp, CancellationToken token) =>
{
    try
    {
        Message reply = await tcp.RedriveAsync(messageId, token);
        return reply.Type == MessageType.Ack
            ? Results.Ok(new { redriven = true, messageId })
            : Results.BadRequest(new { redriven = false, messageId, reason = reply.Reason });
    }
    catch
    {
        return Results.Json(new { redriven = false, reason = "Broker offline." }, statusCode: 503);
    }
});

app.MapFallbackToFile("index.html");
app.Run();
