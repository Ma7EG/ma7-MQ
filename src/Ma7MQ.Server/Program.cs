using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Ma7MQ.Core.Broker;
using Ma7MQ.Core.Storage;
using Ma7MQ.Core.Types;
using Prometheus;
using OpenTelemetry.Trace;
using OpenTelemetry.Resources;

var builder = WebApplication.CreateBuilder(args);

// Configure structured JSON logging for Loki/ELK compatibility
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole(options =>
{
    options.IncludeScopes = true;
    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
    options.JsonWriterOptions = new JsonWriterOptions { Indented = false };
});

// Configure Distributed Tracing (OpenTelemetry)
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddSource("Ma7MQ.Broker")
        .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("ma7-MQ"))
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter()
        .AddConsoleExporter());

// Load Redis connection string
string redisConn = Environment.GetEnvironmentVariable("REDIS_CONNECTION") ?? "localhost:6379";

// Register Core MQ dependencies
builder.Services.AddSingleton<IStorageDriver>(sp => new RedisDriver(redisConn));
builder.Services.AddSingleton<IBrokerEngine, BrokerEngine>();
builder.Services.AddSingleton<Ma7MQ.Server.MetricsExporter>();

// Enable CORS
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
    });
});

var app = builder.Build();

app.UseCors();
app.UseRouting();

// Prometheus Metrics Endpoint
app.UseMetricServer();
app.UseHttpMetrics();

// Start Throughput Calculator
Telemetry.StartThroughputCalculator();

// Configure Graceful Shutdown Lifecycle
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
var storeDriver = app.Services.GetRequiredService<IStorageDriver>();

lifetime.ApplicationStopping.Register(() =>
{
    app.Logger.LogInformation("Graceful shutdown initiated. Terminating Redis driver connection...");
    if (storeDriver is IDisposable disposable)
    {
        disposable.Dispose();
        app.Logger.LogInformation("Redis driver terminated cleanly.");
    }
});

// Health Check Endpoint
app.MapGet("/healthz", async (IStorageDriver store) =>
{
    try
    {
        await store.GetTopicsCountAsync();
        return Results.Ok(new { status = "healthy", redis = "connected" });
    }
    catch (Exception ex)
    {
        return Results.Problem($"Unhealthy: {ex.Message}");
    }
});

// Publish endpoint
app.MapPost("/api/publish", async (HttpContext context, IBrokerEngine engine, Ma7MQ.Server.MetricsExporter metrics) =>
{
    using var document = await JsonDocument.ParseAsync(context.Request.Body);
    var root = document.RootElement;
    
    var msg = new MQMessage
    {
        Topic = root.GetProperty("topic").GetString() ?? "default",
        Payload = System.Text.Encoding.UTF8.GetBytes(root.GetProperty("payload").GetString() ?? "")
    };

    if (root.TryGetProperty("priority", out var priorityProp))
    {
        msg.Priority = (MessagePriority)priorityProp.GetInt32();
    }

    using (metrics.PublishLatency.NewTimer())
    {
        await engine.PublishAsync(msg);
    }
    
    metrics.MessagesPublished.Inc();
    Telemetry.RecordMessage();

    context.Response.StatusCode = 202;
    await context.Response.WriteAsJsonAsync(new { id = msg.ID, status = msg.Status.ToString() });
});

// Topics Endpoints
app.MapGet("/api/topics", async (IStorageDriver store) =>
{
    var topics = await store.GetTopicNamesAsync();
    return Results.Ok(topics);
});

app.MapPost("/api/topics", async (HttpContext context, IBrokerEngine engine) =>
{
    var topic = await JsonSerializer.DeserializeAsync<MQTopic>(context.Request.Body);
    if (topic != null)
    {
        await engine.CreateTopicAsync(topic);
        return Results.Created($"/api/topics/{topic.Name}", topic);
    }
    return Results.BadRequest("Invalid topic body");
});

// Consumers Endpoint
app.MapGet("/api/consumers", async (IStorageDriver store) =>
{
    var consumers = await store.GetActiveConsumersAsync();
    return Results.Ok(consumers);
});

// Worker Consume Endpoint
app.MapPost("/api/consume", async (HttpContext context, IBrokerEngine engine) =>
{
    using var document = await JsonDocument.ParseAsync(context.Request.Body);
    var root = document.RootElement;
    
    var topic = root.GetProperty("topic").GetString() ?? "default";
    var group = root.GetProperty("group").GetString() ?? "default";
    var consumerId = root.GetProperty("consumerId").GetString() ?? "default";

    string filter = "";
    if (root.TryGetProperty("filter", out var filterProp))
    {
        filter = filterProp.GetString() ?? "";
    }

    await engine.RegisterConsumerAsync(group, consumerId);
    await engine.HeartbeatAsync(group, consumerId);

    var messages = await engine.GetMessagesForGroupAsync(topic, group, consumerId, filter, 10);
    return Results.Ok(messages);
});

// Acknowledge Endpoint
app.MapPost("/api/ack", (HttpContext context) =>
{
    return Results.Ok(new { status = "acknowledged" });
});

// SSE Streaming Metrics
app.MapGet("/api/stream/metrics", async (HttpContext context, IStorageDriver store) =>
{
    context.Response.Headers.Append("Content-Type", "text/event-stream");
    context.Response.Headers.Append("Cache-Control", "no-cache");
    context.Response.Headers.Append("Connection", "keep-alive");
    context.Response.Headers.Append("Access-Control-Allow-Origin", "*");

    var writer = new StreamWriter(context.Response.Body);

    while (!context.RequestAborted.IsCancellationRequested)
    {
        long topics = await store.GetTopicsCountAsync();
        long consumers = await store.GetActiveConsumersCountAsync();
        long throughput = Telemetry.Throughput;

        await writer.WriteAsync($"data: {{\"throughput\": {throughput}, \"topics\": {topics}, \"consumers\": {consumers}}}\n\n");
        await writer.FlushAsync();
        await Task.Delay(1000);
    }
});

app.Run();

public static class Telemetry
{
    private static long _publishCount = 0;
    private static long _lastCalculatedThroughput = 0;
    
    public static void RecordMessage()
    {
        Interlocked.Increment(ref _publishCount);
    }
    
    public static void StartThroughputCalculator()
    {
        Task.Run(async () =>
        {
            while (true)
            {
                long current = Interlocked.Exchange(ref _publishCount, 0);
                _lastCalculatedThroughput = current;
                await Task.Delay(1000);
            }
        });
    }
    
    public static long Throughput => _lastCalculatedThroughput;
}
