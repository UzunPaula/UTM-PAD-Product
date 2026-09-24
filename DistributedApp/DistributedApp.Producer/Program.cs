using DistributedApp.Contracts;
using DistributedApp.Producer;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

ProducerSettings settings = new()
{
    BrokerHost = builder.Configuration["Broker:Host"] ?? "127.0.0.1",
    BrokerPort = builder.Configuration.GetValue("Broker:Port", 5000),
    RequestTimeoutMilliseconds = builder.Configuration.GetValue(
        "Broker:RequestTimeoutMilliseconds",
        3000)
};
settings.Validate();

builder.Services.AddSingleton(settings);
builder.Services.AddSingleton<TcpClientService>();
builder.Services.AddSingleton<Producer>();

WebApplication app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet(
    "/api/status",
    async (
        TcpClientService tcpClient,
        CancellationToken cancellationToken) =>
    {
        bool brokerConnected = await tcpClient.CanConnectAsync(
            cancellationToken);

        return Results.Ok(new
        {
            brokerConnected,
            // The current protocol does not expose consumer or queue state.
            consumerConnected = (bool?)null,
            pendingMessages = (int?)null,
            deadLetterMessages = (int?)null
        });
    });

app.MapPost(
    "/api/orders",
    async (
        OrderPayload order,
        Producer producer,
        ILogger<Program> logger,
        CancellationToken cancellationToken) =>
    {
        string? validationError = Producer.ValidateOrder(order);
        if (validationError is not null)
        {
            return Results.BadRequest(new
            {
                accepted = false,
                reason = validationError
            });
        }

        try
        {
            PublishResult result = await producer.PublishOrderAsync(
                order,
                cancellationToken);

            object body = new
            {
                result.Accepted,
                result.MessageId,
                result.CorrelationId,
                result.Reason
            };

            return result.Accepted
                ? Results.Ok(body)
                : Results.Json(body, statusCode: StatusCodes.Status502BadGateway);
        }
        catch (Exception exception) when (
            exception is IOException
            or System.Net.Sockets.SocketException
            or System.Text.Json.JsonException
            or InvalidDataException
            or TimeoutException)
        {
            logger.LogWarning(
                exception,
                "Order publishing failed for {OrderId}.",
                order.OrderId);

            return Results.Json(
                new
                {
                    accepted = false,
                    reason = "Brokerul nu este disponibil sau nu a răspuns corect."
                },
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }
    });

app.MapFallbackToFile("index.html");

app.Run();
