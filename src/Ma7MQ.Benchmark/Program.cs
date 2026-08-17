using System;
using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Ma7MQ.Benchmark
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string url = "http://localhost:5000/api/publish";
            int concurrency = 50;
            int durationSeconds = 10;

            Console.WriteLine("========================================");
            Console.WriteLine("          ma7-MQ Benchmark Tool         ");
            Console.WriteLine("========================================");
            Console.WriteLine($"Target URL:  {url}");
            Console.WriteLine($"Concurrency: {concurrency} workers");
            Console.WriteLine($"Duration:    {durationSeconds} seconds");
            Console.WriteLine("----------------------------------------");
            Console.WriteLine("Benchmarking starting in 3 seconds...");
            await Task.Delay(3000);

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
            var token = cts.Token;

            long totalRequests = 0;
            long successRequests = 0;
            long failureRequests = 0;
            long totalLatencyTicks = 0;

            var stopwatch = Stopwatch.StartNew();

            var tasks = new Task[concurrency];
            for (int i = 0; i < concurrency; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    using var client = new HttpClient();
                    client.DefaultRequestHeaders.ConnectionClose = false;

                    var jsonPayload = JsonSerializer.Serialize(new
                    {
                        topic = "benchmark-test",
                        payload = "test-message-data-payload-12345"
                    });

                    while (!token.IsCancellationRequested)
                    {
                        var reqStopwatch = Stopwatch.StartNew();
                        try
                        {
                            var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                            var response = await client.PostAsync(url, content, token);

                            reqStopwatch.Stop();
                            Interlocked.Add(ref totalLatencyTicks, reqStopwatch.ElapsedTicks);
                            Interlocked.Increment(ref totalRequests);

                            if (response.IsSuccessStatusCode)
                            {
                                Interlocked.Increment(ref successRequests);
                            }
                            else
                            {
                                Interlocked.Increment(ref failureRequests);
                            }
                        }
                        catch
                        {
                            reqStopwatch.Stop();
                            Interlocked.Increment(ref totalRequests);
                            Interlocked.Increment(ref failureRequests);
                        }
                    }
                });
            }

            await Task.WhenAll(tasks);
            stopwatch.Stop();

            double elapsedSeconds = stopwatch.Elapsed.TotalSeconds;
            double rps = totalRequests / elapsedSeconds;
            double avgLatencyMs = totalRequests > 0 
                ? (double)totalLatencyTicks / totalRequests / TimeSpan.TicksPerMillisecond 
                : 0;

            Console.WriteLine("----------------------------------------");
            Console.WriteLine($"Elapsed Time:    {elapsedSeconds:F2} seconds");
            Console.WriteLine($"Total Requests:  {totalRequests}");
            Console.WriteLine($"Successful:      {successRequests}");
            Console.WriteLine($"Failed:          {failureRequests}");
            Console.WriteLine($"Throughput (RPS): {rps:F2} req/sec");
            Console.WriteLine($"Avg Latency:     {avgLatencyMs:F2} ms");
            Console.WriteLine("========================================");
        }
    }
}
