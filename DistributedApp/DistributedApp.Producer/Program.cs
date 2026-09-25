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
app.UseStaticFiles(new StaticFileOptions
{
    OnPrepareResponse = context =>
    {
        context.Context.Response.Headers.CacheControl =
            "no-store, no-cache, must-revalidate";
        context.Context.Response.Headers.Pragma = "no-cache";
        context.Context.Response.Headers.Expires = "0";
    }
});

app.MapGet(
    "/api/status",
    async (
        TcpClientService tcpClient,
        ILogger<Program> logger,
        CancellationToken cancellationToken) =>
    {
        try
        {
            BrokerStatusSnapshot status =
                await tcpClient.GetBrokerStatusAsync(
                    "orders",
                    cancellationToken);

            return Results.Ok(new
            {
                brokerConnected = true,
                status.ConsumerConnected,
                status.PendingMessages,
                status.InFlightMessages,
                status.DeadLetterMessages,
                status.AcknowledgedMessages,
                status.RecentAcknowledgements,
                status.DeadLetters,
                status.ObservedAtUtc
            });
        }
        catch (Exception exception) when (
            exception is IOException
            or System.Net.Sockets.SocketException
            or System.Text.Json.JsonException
            or InvalidDataException
            or TimeoutException)
        {
            logger.LogDebug(
                exception,
                "Broker status is unavailable.");

            return Results.Ok(new
            {
                brokerConnected = false,
                consumerConnected = false,
                pendingMessages = 0,
                inFlightMessages = 0,
                deadLetterMessages = 0,
                acknowledgedMessages = 0,
                recentAcknowledgements =
                    Array.Empty<AcknowledgementSummary>(),
                deadLetters = Array.Empty<DeadLetterSummary>(),
                observedAtUtc = DateTimeOffset.UtcNow
            });
        }
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

app.MapPost(
    "/api/dead-letters/{messageId:guid}/redrive",
    async (
        Guid messageId,
        TcpClientService tcpClient,
        ILogger<Program> logger,
        CancellationToken cancellationToken) =>
    {
        try
        {
            Message response = await tcpClient.RedriveDeadLetterAsync(
                "orders",
                messageId,
                cancellationToken);

            if (response.Type == MessageType.Ack)
            {
                return Results.Ok(new
                {
                    redriven = true,
                    messageId
                });
            }

            return Results.NotFound(new
            {
                redriven = false,
                messageId,
                reason = response.Reason
                    ?? "Mesajul nu mai există în dead-letter queue."
            });
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
                "DLQ redrive failed for message {MessageId}.",
                messageId);

            return Results.Json(
                new
                {
                    redriven = false,
                    messageId,
                    reason =
                        "Brokerul nu este disponibil sau nu a răspuns corect."
                },
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }
    });
app.MapFallbackToFile("index.html");

app.Run();
