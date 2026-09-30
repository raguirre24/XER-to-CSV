using System.Security.Cryptography;

namespace XerToCsvConverter.Core;

/// <summary>
/// A write-only pass-through stream that computes the SHA-256 hash of all written data
/// incrementally without buffering the uncompressed bytes in memory.
/// </summary>
internal sealed class HashingWriteStream : Stream
{
    private readonly Stream _inner;
    private readonly IncrementalHash _hash;
    private string? _computedHash;
    private bool _disposed;
    private int _activeWrite;

    public HashingWriteStream(Stream inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        if (!inner.CanWrite) throw new ArgumentException("The destination stream must be writable.", nameof(inner));
        _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => !_disposed && _computedHash is null && _inner.CanWrite;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _inner.Flush();
    }
    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _inner.FlushAsync(cancellationToken);
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count)
    {
        BeginWrite();
        try
        {
            _inner.Write(buffer, offset, count);
            _hash.AppendData(buffer, offset, count);
        }
        finally { Volatile.Write(ref _activeWrite, 0); }
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        BeginWrite();
        try
        {
            _inner.Write(buffer);
            _hash.AppendData(buffer);
        }
        finally { Volatile.Write(ref _activeWrite, 0); }
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        BeginWrite();
        try
        {
            await _inner.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            _hash.AppendData(buffer.Span);
        }
        finally { Volatile.Write(ref _activeWrite, 0); }
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        return WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    /// <summary>
    /// Gets the lowercase hex-encoded SHA-256 digest of all written data.
    /// </summary>
    public string GetHash()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (Volatile.Read(ref _activeWrite) != 0)
            throw new InvalidOperationException("Wait for the pending write before finalizing the hash.");
        _computedHash ??= Convert.ToHexString(_hash.GetHashAndReset()).ToLowerInvariant();
        return _computedHash;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
        {
            _disposed = true;
            _hash.Dispose();
        }
        base.Dispose(disposing);
    }

    private void BeginWrite()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_computedHash is not null)
            throw new InvalidOperationException("The hash has been finalized; no further writes are allowed.");
        if (Interlocked.CompareExchange(ref _activeWrite, 1, 0) != 0)
            throw new InvalidOperationException("Concurrent writes are not supported.");
    }
}
