using System.Diagnostics;
using System.Runtime.InteropServices.JavaScript;
using System.Text;
using System.Text.Json;
using XerToCsvConverter;
using XerToCsvConverter.BrowserProcessing;

PerformanceConfig.MaxParallelFiles = 1;
PerformanceConfig.MaxParallelTransformations = 1;

namespace XerToCsvConverter.Web.Worker
{
    internal static partial class WorkerHost
    {
        private static readonly BrowserProcessingSession Session = new(ReadFileAsync, ReadContentHashAsync);
        private static readonly Dictionary<string, string?> ReadHashes = new(StringComparer.Ordinal);
        private static CancellationTokenSource? _active;

        [JSExport]
        internal static void Cancel() => _active?.Cancel();
        [JSExport]
        internal static void Remove(string token) => Session.Remove(token);
        [JSExport]
        internal static void Invalidate() => Session.InvalidateDerived();

        [JSExport]
        internal static async Task<string> Run(string requestJson)
        {
            BrowserJob job = JsonSerializer.Deserialize(requestJson, BrowserJsonContext.Default.BrowserJob)
                ?? throw new ArgumentException("A processing request is required.");
            if (_active is not null) throw new InvalidOperationException("The worker is already processing a request.");
            using var cancellation = new CancellationTokenSource();
            _active = cancellation;
            try
            {
                BrowserResult result = await Session.RunAsync(job, new InlineProgress<ProcessingService.DetailedProgress>(p =>
                    ReportProgress(job.JobId, JsonSerializer.Serialize(p, BrowserJsonContext.Default.DetailedProgress))), cancellation.Token);
                using MemoryStream? zip = result.ZipContent;
                if (zip is not null)
                {
                    const int chunkSize = 256 * 1024;
                    var pending = new Queue<Task>();
                    var transfer = Stopwatch.StartNew();
                    byte[] bytes = zip.GetBuffer();
                    int length = checked((int)zip.Length);
                    int sequence = 0;
                    for (int offset = 0; offset < length; offset += chunkSize)
                    {
                        cancellation.Token.ThrowIfCancellationRequested();
                        SendZipChunk(job.JobId, sequence, bytes.AsSpan(offset, Math.Min(chunkSize, length - offset)));
                        pending.Enqueue(WaitForAcknowledgement(job.JobId, sequence++));
                        if (pending.Count >= 2) await pending.Dequeue();
                    }
                    while (pending.Count != 0) await pending.Dequeue();
                    var stages = new Dictionary<string, double>(result.StageMilliseconds) { ["zipTransfer"] = transfer.Elapsed.TotalMilliseconds };
                    result = result with { DownloadId = job.JobId, ZipContent = null, StageMilliseconds = stages };
                }
                cancellation.Token.ThrowIfCancellationRequested();
                var serialization = Stopwatch.StartNew();
                string json = JsonSerializer.Serialize(result, BrowserJsonContext.Default.BrowserResult);
                serialization.Stop();
                // Report scalar measurements beside the result so its full diagnostic
                // evidence is serialized once, without embedding its own elapsed time.
                ReportResultMetrics(job.JobId, serialization.Elapsed.TotalMilliseconds, Encoding.UTF8.GetByteCount(json));
                return json;
            }
            finally { ReadHashes.Clear(); _active = null; }
        }

        private static async Task<byte[]> ReadFileAsync(BrowserSource source, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            byte[] bytes = GC.AllocateUninitializedArray<byte>(checked((int)source.Size));
            string? hash = await FillFile(source.SourceToken, new ArraySegment<byte>(bytes));
            token.ThrowIfCancellationRequested();
            ReadHashes[source.SourceToken] = hash;
            return bytes;
        }

        private static Task<string?> ReadContentHashAsync(BrowserSource source, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            ReadHashes.Remove(source.SourceToken, out string? hash);
            return Task.FromResult(hash);
        }

        [JSImport("fillFile", "worker-bridge")]
        private static partial Task<string?> FillFile(string sourceToken, [JSMarshalAs<JSType.MemoryView>] ArraySegment<byte> destination);
        [JSImport("progress", "worker-bridge")]
        private static partial void ReportProgress(string jobId, string json);
        [JSImport("resultMetrics", "worker-bridge")]
        private static partial void ReportResultMetrics(string jobId, double elapsedMilliseconds, int jsonBytes);
        [JSImport("chunk", "worker-bridge")]
        private static partial void SendZipChunk(string jobId, int sequence, [JSMarshalAs<JSType.MemoryView>] Span<byte> bytes);
        [JSImport("acknowledgement", "worker-bridge")]
        private static partial Task WaitForAcknowledgement(string jobId, int sequence);
    }
}
