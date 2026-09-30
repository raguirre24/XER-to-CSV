using System.Security.Cryptography;
using XerToCsvConverter.Core;

namespace XerToCsvConverter.Core.Tests;

public sealed class HashingWriteStreamTests
{
    [Fact]
    public async Task Mixed_writes_match_sha256_and_finalization_prevents_further_writes()
    {
        using var destination = new MemoryStream();
        using (var hashing = new HashingWriteStream(destination))
        {
            hashing.Write(new byte[] { 1, 2 }, 0, 2);
            hashing.Write(new byte[] { 3 }.AsSpan());
            await hashing.WriteAsync(new byte[] { 4, 5 }.AsMemory());
            await hashing.WriteAsync(new byte[] { 6, 7 }, 0, 2, CancellationToken.None);
            await hashing.FlushAsync(CancellationToken.None);
            string expected = Hash([1, 2, 3, 4, 5, 6, 7]);
            Assert.Equal(expected, hashing.GetHash());
            Assert.Equal(expected, hashing.GetHash());
            Assert.False(hashing.CanWrite);
            Assert.Throws<InvalidOperationException>(() => hashing.WriteByte(8));
            await Assert.ThrowsAsync<InvalidOperationException>(() => hashing.WriteAsync(new byte[] { 8 }.AsMemory()).AsTask());
        }
        Assert.True(destination.CanWrite);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7 }, destination.ToArray());
    }

    [Fact]
    public async Task Failed_and_cancelled_async_writes_do_not_change_the_digest()
    {
        using var destination = new ControlledStream();
        using var hashing = new HashingWriteStream(destination);
        hashing.WriteByte(1);
        destination.FailWrites = true;
        await Assert.ThrowsAsync<IOException>(() => hashing.WriteAsync(new byte[] { 2 }.AsMemory()).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            hashing.WriteAsync(new byte[] { 3 }, 0, 1, new CancellationToken(true)));
        Assert.Equal(Hash([1]), hashing.GetHash());
        Assert.Equal(new byte[] { 1 }, destination.ToArray());
    }

    [Fact]
    public async Task Digest_cannot_be_finalized_while_an_async_write_is_pending()
    {
        using var destination = new ControlledStream { Pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
        using var hashing = new HashingWriteStream(destination);
        Task write = hashing.WriteAsync(new byte[] { 1 }.AsMemory()).AsTask();
        Assert.False(write.IsCompleted);
        Assert.Throws<InvalidOperationException>(() => hashing.GetHash());
        destination.Pending.SetResult();
        await write;
        Assert.Equal(Hash([1]), hashing.GetHash());
    }

    [Fact]
    public async Task Disposed_wrapper_rejects_operations_and_leaves_the_caller_stream_open()
    {
        using var destination = new MemoryStream();
        var hashing = new HashingWriteStream(destination);
        hashing.Dispose();
        hashing.Dispose();
        Assert.False(hashing.CanWrite);
        Assert.Throws<ObjectDisposedException>(() => hashing.WriteByte(1));
        Assert.Throws<ObjectDisposedException>(() => hashing.GetHash());
        Assert.Throws<ObjectDisposedException>(() => hashing.Flush());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => hashing.WriteAsync(new byte[] { 1 }.AsMemory()).AsTask());
        Assert.True(destination.CanWrite);
    }

    private static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    private sealed class ControlledStream : MemoryStream
    {
        public bool FailWrites { get; set; }
        public TaskCompletionSource? Pending { get; init; }
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (Pending is not null) await Pending.Task.WaitAsync(cancellationToken);
            if (FailWrites) throw new IOException("Synthetic failed write.");
            await base.WriteAsync(buffer, cancellationToken);
        }
    }
}
