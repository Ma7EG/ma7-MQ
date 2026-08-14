using System;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Ma7MQ.Core.Broker;
using Ma7MQ.Core.Storage;
using Ma7MQ.Core.Types;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

// Register Core MQ dependencies
builder.Services.AddSingleton<IStorageDriver>(sp => new RedisDriver("localhost:6379"));
builder.Services.AddSingleton<IBrokerEngine, BrokerEngine>();
builder.Services.AddSingleton<Ma7MQ.Server.MetricsExporter>();

var app = builder.Build();

app.UseRouting();

// Prometheus Metrics Endpoint
app.UseMetricServer();
app.UseHttpMetrics();

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

    context.Response.StatusCode = 202;
    await context.Response.WriteAsJsonAsync(new { id = msg.ID, status = msg.Status.ToString() });
});

// SSE Streaming Metrics
app.MapGet("/api/stream/metrics", async (HttpContext context) =>
{
    context.Response.Headers.Append("Content-Type", "text/event-stream");
    context.Response.Headers.Append("Cache-Control", "no-cache");
    context.Response.Headers.Append("Connection", "keep-alive");
    context.Response.Headers.Append("Access-Control-Allow-Origin", "*");

    var writer = new StreamWriter(context.Response.Body);
    var random = new Random();

    while (!context.RequestAborted.IsCancellationRequested)
    {
        int throughput = 15000 + random.Next(200);
        await writer.WriteAsync($"data: {{\"throughput\": {throughput}, \"topics\": 12, \"consumers\": 104}}\n\n");
        await writer.FlushAsync();
        await Task.Delay(2000);
    }
});

app.Run();
// Simplified parsing
