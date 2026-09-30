using System.IO.Compression;
using System.Text;
using XerToCsvConverter.Core;

namespace XerToCsvConverter.Core.Tests;

public sealed class ParserOperationTelemetryTests
{
    [Fact]
    public async Task Nested_async_phases_report_exclusive_time_without_double_counting()
    {
        using var operation = ParserOperationTelemetry.BeginOperation();
        using (ParserOperationTelemetry.Measure(ParserOperationPhase.Transformation))
        {
            await Task.Yield();
            using (ParserOperationTelemetry.Measure(ParserOperationPhase.StandaloneValidation))
                await Task.Delay(2);
        }
        ParserOperationTimingSnapshot result = operation.Snapshot();
        Assert.False(result.HasOverlaps);
        Assert.Equal(2, result.Scopes.Count);
        ParserPhaseTiming transform = Assert.Single(result.Phases, phase => phase.Phase == ParserOperationPhase.Transformation);
        Assert.True(transform.InclusiveMs > transform.ExclusiveMs);
        Assert.True(result.Phases.Sum(phase => phase.ExclusiveMs) <= result.ElapsedMs + 0.1);
        using var disabledLater = ParserOperationTelemetry.Measure(ParserOperationPhase.Metadata);
        disabledLater.Dispose(); // Idempotent disposal must not disturb the enclosing operation.
        disabledLater.Dispose();
    }

    [Fact]
    public async Task Concurrent_children_are_identified_and_parent_excludes_their_union()
    {
        using var operation = ParserOperationTelemetry.BeginOperation();
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int entered = 0;
        using (ParserOperationTelemetry.Measure(ParserOperationPhase.Transformation))
            await Task.WhenAll(Child(ParserOperationPhase.Metadata), Child(ParserOperationPhase.StandaloneValidation));
        ParserOperationTimingSnapshot result = operation.Snapshot();
        Assert.True(result.HasOverlaps);
        ParserScopeTiming parent = Assert.Single(result.Scopes, scope => scope.Phase == ParserOperationPhase.Transformation);
        Assert.True(parent.OverlappingChildren);
        Assert.InRange(parent.ExclusiveMs, 0, parent.InclusiveMs);
        double children = result.Scopes.Where(scope => scope.Phase != ParserOperationPhase.Transformation).Sum(scope => scope.InclusiveMs);
        Assert.True(parent.InclusiveMs - parent.ExclusiveMs < children);

        async Task Child(ParserOperationPhase phase)
        {
            using var scope = ParserOperationTelemetry.Measure(phase);
            if (Interlocked.Increment(ref entered) == 2) ready.SetResult();
            await ready.Task;
            await Task.Delay(2);
        }
    }

    [Fact]
    public async Task Independent_async_operations_do_not_mix_evidence()
    {
        ParserOperationTimingSnapshot[] results = await Task.WhenAll(
            Capture(ParserOperationPhase.Metadata), Capture(ParserOperationPhase.Merging));
        Assert.Equal(ParserOperationPhase.Metadata, Assert.Single(results[0].Phases).Phase);
        Assert.Equal(ParserOperationPhase.Merging, Assert.Single(results[1].Phases).Phase);

        static async Task<ParserOperationTimingSnapshot> Capture(ParserOperationPhase phase)
        {
            using var operation = ParserOperationTelemetry.BeginOperation();
            using (ParserOperationTelemetry.Measure(phase)) await Task.Yield();
            return operation.Snapshot();
        }
    }

    [Fact]
    public void Buffered_zip_and_digest_evidence_preserves_csv_bytes_and_separates_nested_work()
    {
        string content = string.Concat(Enumerable.Repeat("quoted,\"é中😀\"\r\n", 4000));
        byte[] bytes = Encoding.UTF8.GetBytes(content);
        using var expected = new MemoryStream();
        using (var archive = new ZipArchive(expected, ZipArchiveMode.Create, leaveOpen: true))
        using (Stream entry = archive.CreateEntry("table.csv", CompressionLevel.Fastest).Open()) entry.Write(bytes);

        using var operation = ParserOperationTelemetry.BeginOperation();
        using var actual = new MemoryStream();
        string digest;
        using (var archive = ParserOperationTelemetry.CreateZipArchive(actual))
        using (Stream entry = ParserOperationTelemetry.OpenZipEntry(archive.CreateEntry("table.csv", CompressionLevel.Fastest)))
        using (var hashing = new HashingWriteStream(entry))
        using (Stream csv = ParserOperationTelemetry.TrackCsvHashing(hashing, entry, "table.csv"))
        {
            using (ParserOperationTelemetry.Measure(ParserOperationPhase.CsvSerialization, "table.csv"))
            {
                for (int offset = 0; offset < bytes.Length; offset += 4096)
                    csv.Write(bytes.AsSpan(offset, Math.Min(4096, bytes.Length - offset)));
                csv.Flush();
            }
            using (ParserOperationTelemetry.Measure(ParserOperationPhase.CsvHashing, "table.csv:digest"))
                digest = hashing.GetHash();
        }
        Assert.Equal(Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(bytes)).ToLowerInvariant(), digest);
        Assert.Equal(Extract(expected), Extract(actual));
        ParserOperationTimingSnapshot evidence = operation.Snapshot();
        Assert.False(evidence.HasOverlaps);
        Assert.True(Assert.Single(evidence.Phases, phase => phase.Phase == ParserOperationPhase.ZipCompressionFinalization).BufferedCalls > 0);
        ParserPhaseTiming csvTime = Assert.Single(evidence.Phases, phase => phase.Phase == ParserOperationPhase.CsvSerialization);
        ParserPhaseTiming hashTime = Assert.Single(evidence.Phases, phase => phase.Phase == ParserOperationPhase.CsvHashing);
        Assert.True(csvTime.InclusiveMs > csvTime.ExclusiveMs);
        Assert.True(hashTime.InclusiveMs > hashTime.ExclusiveMs);
        Assert.All(evidence.Phases, phase => Assert.InRange(phase.ExclusiveMs, 0, phase.InclusiveMs));

        static byte[] Extract(MemoryStream stream)
        {
            stream.Position = 0;
            using var archive = new ZipArchive(stream, ZipArchiveMode.Read, leaveOpen: true);
            using Stream entry = Assert.Single(archive.Entries).Open();
            using var extracted = new MemoryStream();
            entry.CopyTo(extracted);
            return extracted.ToArray();
        }
    }
}
