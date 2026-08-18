using System;
using System.Collections.Generic;
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
            string baseUrl = "http://localhost:5000";
            int concurrency = 200;
            int durationSeconds = 10;
            int batchSize = 50;
            bool useBatch = true;

            // Parse optional args
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "--url": baseUrl = args[++i]; break;
                    case "--concurrency": concurrency = int.Parse(args[++i]); break;
                    case "--duration": durationSeconds = int.Parse(args[++i]); break;
                    case "--batch-size": batchSize = int.Parse(args[++i]); break;
                    case "--no-batch": useBatch = false; break;
                }
            }

            string singleUrl = $"{baseUrl}/api/publish";
            string batchUrl = $"{baseUrl}/api/publish/batch";

            Console.WriteLine("╔══════════════════════════════════════════╗");
            Console.WriteLine("║       ma7-MQ Performance Benchmark       ║");
            Console.WriteLine("╠══════════════════════════════════════════╣");
            Console.WriteLine($"║ Target:      {baseUrl,-28}║");
            Console.WriteLine($"║ Concurrency: {concurrency,-28}║");
            Console.WriteLine($"║ Duration:    {durationSeconds}s{new string(' ', 27 - durationSeconds.ToString().Length)}║");
            Console.WriteLine($"║ Batch Size:  {(useBatch ? batchSize.ToString() : "disabled"),-28}║");
            Console.WriteLine("╠══════════════════════════════════════════╣");
            Console.WriteLine("║ Starting in 2 seconds...                 ║");
            Console.WriteLine("╚══════════════════════════════════════════╝");
            await Task.Delay(2000);

            // Use a single SocketsHttpHandler with connection pooling
            var handler = new SocketsHttpHandler
            {
                MaxConnectionsPerServer = concurrency,
                PooledConnectionLifetime = TimeSpan.FromMinutes(5),
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
                EnableMultipleHttp2Connections = true
            };
            using var client = new HttpClient(handler)
            {
                Timeout = TimeSpan.FromSeconds(30)
            };

            // Pre-build payloads
            string singlePayload = JsonSerializer.Serialize(new
            {
                topic = "benchmark-test",
                payload = "perf-test-" + new string('x', 64)
            });

            var batchItems = new List<object>();
            for (int b = 0; b < batchSize; b++)
            {
                batchItems.Add(new
                {
                    topic = "benchmark-test",
                    payload = $"perf-batch-{b}-" + new string('x', 32)
                });
            }
            string batchPayload = JsonSerializer.Serialize(batchItems);

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
            var token = cts.Token;

            long totalMessages = 0;
            long successMessages = 0;
            long failedRequests = 0;
            long totalRequests = 0;
            long totalLatencyTicks = 0;

            // Progress reporting
            var progressTask = Task.Run(async () =>
            {
                var sw = Stopwatch.StartNew();
                while (!token.IsCancellationRequested)
                {
                    await Task.Delay(1000);
                    long msgs = Interlocked.Read(ref totalMessages);
                    double elapsed = sw.Elapsed.TotalSeconds;
                    Console.Write($"\r  ▸ {msgs:N0} messages | {msgs / elapsed:N0} msg/sec | {elapsed:F1}s elapsed");
                }
                Console.WriteLine();
            });

            var stopwatch = Stopwatch.StartNew();

            var tasks = new Task[concurrency];
            for (int i = 0; i < concurrency; i++)
            {
                if (useBatch)
                {
                    tasks[i] = Task.Run(async () =>
                    {
                        while (!token.IsCancellationRequested)
                        {
                            var reqSw = Stopwatch.StartNew();
                            try
                            {
                                var content = new StringContent(batchPayload, Encoding.UTF8, "application/json");
                                var response = await client.PostAsync(batchUrl, content, token);
                                reqSw.Stop();

                                Interlocked.Add(ref totalLatencyTicks, reqSw.ElapsedTicks);
                                Interlocked.Increment(ref totalRequests);

                                if (response.IsSuccessStatusCode)
                                {
                                    Interlocked.Add(ref totalMessages, batchSize);
                                    Interlocked.Add(ref successMessages, batchSize);
                                }
                                else
                                {
                                    Interlocked.Increment(ref failedRequests);
                                }
                            }
                            catch (OperationCanceledException) { break; }
                            catch
                            {
                                reqSw.Stop();
                                Interlocked.Increment(ref totalRequests);
                                Interlocked.Increment(ref failedRequests);
                            }
                        }
                    });
                }
                else
                {
                    tasks[i] = Task.Run(async () =>
                    {
                        while (!token.IsCancellationRequested)
                        {
                            var reqSw = Stopwatch.StartNew();
                            try
                            {
                                var content = new StringContent(singlePayload, Encoding.UTF8, "application/json");
                                var response = await client.PostAsync(singleUrl, content, token);
                                reqSw.Stop();

                                Interlocked.Add(ref totalLatencyTicks, reqSw.ElapsedTicks);
                                Interlocked.Increment(ref totalRequests);

                                if (response.IsSuccessStatusCode)
                                {
                                    Interlocked.Add(ref totalMessages, 1);
                                    Interlocked.Add(ref successMessages, 1);
                                }
                                else
                                {
                                    Interlocked.Increment(ref failedRequests);
                                }
                            }
                            catch (OperationCanceledException) { break; }
                            catch
                            {
                                reqSw.Stop();
                                Interlocked.Increment(ref totalRequests);
                                Interlocked.Increment(ref failedRequests);
                            }
                        }
                    });
                }
            }

            await Task.WhenAll(tasks);
            stopwatch.Stop();
            try { await progressTask; } catch { }

            double elapsedSeconds = stopwatch.Elapsed.TotalSeconds;
            double msgPerSec = totalMessages / elapsedSeconds;
            double reqPerSec = totalRequests / elapsedSeconds;
            double avgLatencyMs = totalRequests > 0
                ? (double)totalLatencyTicks / totalRequests / TimeSpan.TicksPerMillisecond
                : 0;

            Console.WriteLine();
            Console.WriteLine("╔══════════════════════════════════════════╗");
            Console.WriteLine("║              BENCHMARK RESULTS           ║");
            Console.WriteLine("╠══════════════════════════════════════════╣");
            Console.WriteLine($"║ Duration:        {elapsedSeconds:F2}s{new string(' ', 23 - elapsedSeconds.ToString("F2").Length)}║");
            Console.WriteLine($"║ Total Messages:  {totalMessages:N0}{new string(' ', 24 - totalMessages.ToString("N0").Length)}║");
            Console.WriteLine($"║ Total Requests:  {totalRequests:N0}{new string(' ', 24 - totalRequests.ToString("N0").Length)}║");
            Console.WriteLine($"║ Successful:      {successMessages:N0}{new string(' ', 24 - successMessages.ToString("N0").Length)}║");
            Console.WriteLine($"║ Failed:          {failedRequests:N0}{new string(' ', 24 - failedRequests.ToString("N0").Length)}║");
            Console.WriteLine("╠══════════════════════════════════════════╣");
            Console.WriteLine($"║ ▸ Messages/sec:  {msgPerSec:N0}{new string(' ', 24 - msgPerSec.ToString("N0").Length)}║");
            Console.WriteLine($"║ ▸ Requests/sec:  {reqPerSec:N0}{new string(' ', 24 - reqPerSec.ToString("N0").Length)}║");
            Console.WriteLine($"║ ▸ Avg Latency:   {avgLatencyMs:F2}ms{new string(' ', 22 - avgLatencyMs.ToString("F2").Length)}║");
            Console.WriteLine("╚══════════════════════════════════════════╝");
        }
    }
}
