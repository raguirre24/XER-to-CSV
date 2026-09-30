using System.Security.Cryptography;
using System.Text;
using XerToCsvConverter.BrowserProcessing;

namespace XerToCsvConverter.Core.Tests;

public sealed class BrowserContentHashTests
{
    [Fact]
    public async Task Trusted_digest_is_consumed_once_per_read_and_sealed_hash_rejects_an_uncached_changed_source()
    {
        byte[] content = Xer("Original");
        string? readHash = null;
        int reads = 0, hashes = 0;
        var source = new BrowserSource("occurrence", "schedule.xer", content.Length, Hash(content));
        var session = new BrowserProcessingSession((_, token) =>
        {
            token.ThrowIfCancellationRequested();
            reads++;
            byte[] owned = content.ToArray();
            readHash = Hash(owned);
            return Task.FromResult(owned);
        }, (_, token) =>
        {
            token.ThrowIfCancellationRequested();
            hashes++;
            string? result = readHash;
            readHash = null;
            return Task.FromResult(result);
        });
        BrowserResult metadata = await session.RunAsync(Job("metadata", source));
        Assert.Equal(source.SourceSha256, metadata.SourceSha256);
        Assert.Null(readHash);
        BrowserJob parse = Job("parse", source) with { CacheBudgetBytes = 0 };
        Assert.Equal(1, (await session.RunAsync(parse)).ParsedSources);
        Assert.Equal(1, reads);
        Assert.Equal(1, hashes);
        Assert.Equal(1, (await session.RunAsync(parse)).ParsedSources);
        Assert.Equal(2, reads);
        Assert.Equal(2, hashes);
        content = Xer("Modified"); // Same length, so the sealed digest detects the change.
        Assert.Equal(source.Size, content.LongLength);
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(() => session.RunAsync(parse));
        Assert.Contains("source_sha256", error.Message);
        Assert.Null(readHash);
        content = Xer("Original");
        BrowserResult retry = await session.RunAsync(parse);
        Assert.Equal(1, retry.ParsedSources);
        Assert.Equal(0, retry.CacheHits);
        Assert.Equal(4, reads);
        Assert.Equal(4, hashes);
    }

    [Fact]
    public async Task An_unavailable_browser_digest_uses_the_existing_managed_hash()
    {
        byte[] bytes = Xer("Activity");
        var source = new BrowserSource("source", "schedule.xer", bytes.Length);
        int hashes = 0;
        var session = new BrowserProcessingSession((_, _) => Task.FromResult(bytes.ToArray()), (_, _) =>
        {
            hashes++;
            return Task.FromResult<string?>(null);
        });
        BrowserResult result = await session.RunAsync(Job("metadata", source));
        Assert.Equal(Hash(bytes), result.SourceSha256);
        Assert.Equal("2026-09-30", result.DataDate);
        Assert.Equal(1, hashes);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-a-digest")]
    [InlineData("gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg")]
    [InlineData("0000000000000000000000000000000000000000000000000000000000000000")]
    public async Task Invalid_or_configured_mismatched_digest_fails_before_retention_and_can_retry(string supplied)
    {
        byte[] bytes = Xer("Activity");
        string actual = Hash(bytes);
        var source = new BrowserSource("source", "schedule.xer", bytes.Length, actual);
        int reads = 0;
        var session = new BrowserProcessingSession((_, _) =>
        {
            reads++;
            return Task.FromResult(bytes.ToArray());
        }, (_, _) => Task.FromResult<string?>(supplied));
        await Assert.ThrowsAsync<InvalidDataException>(() => session.RunAsync(Job("parse", source)));
        supplied = actual.ToUpperInvariant();
        BrowserResult retry = await session.RunAsync(Job("parse", source));
        Assert.Equal(1, retry.ParsedSources);
        Assert.Equal(0, retry.CacheHits);
        Assert.Equal(2, reads);
    }

    [Fact]
    public async Task Cancelled_digest_does_not_seal_partial_state_and_allows_a_fresh_retry()
    {
        byte[] bytes = Xer("Activity");
        var source = new BrowserSource("source", "schedule.xer", bytes.Length);
        using var cancellation = new CancellationTokenSource();
        int hashes = 0, reads = 0;
        var session = new BrowserProcessingSession((_, _) =>
        {
            reads++;
            return Task.FromResult(bytes.ToArray());
        }, (_, _) =>
        {
            if (++hashes == 1) cancellation.Cancel();
            return Task.FromResult<string?>(Hash(bytes));
        });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => session.RunAsync(Job("metadata", source), token: cancellation.Token));
        BrowserResult retry = await session.RunAsync(Job("metadata", source));
        Assert.Equal(Hash(bytes), retry.SourceSha256);
        Assert.Equal(2, hashes);
        Assert.Equal(2, reads);
    }

    private static BrowserJob Job(string kind, BrowserSource source) => new()
    { JobId = Guid.NewGuid().ToString("N"), Generation = 1, Kind = kind, Sources = [source] };

    private static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    private static byte[] Xer(string activity) => Encoding.UTF8.GetBytes(
        "%T\tPROJECT\r\n%F\tproj_id\tlast_recalc_date\r\n%R\tP1\t2026-09-30\r\n" +
        "%T\tTASK\r\n%F\ttask_id\tproj_id\ttask_name\r\n%R\tT1\tP1\t" + activity + "\r\n%E\r\n");
}
