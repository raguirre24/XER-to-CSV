using System.Security.Cryptography;

namespace XerToCsvConverter.Core;

/// <summary>
/// A write-only pass-through stream that computes the SHA-256 hash of all written data
/// incrementally without buffering the uncompressed bytes in memory.
/// </summary>
internal sealed class HashingWriteStream : Stream
{
    private readonly Stream _inner;
    private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private string? _computedHash;

    public HashingWriteStream(Stream inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _inner.Length;
    public override long Position
    {
        get => _inner.Position;
        set => throw new NotSupportedException();
    }

    public override void Flush() => _inner.Flush();
    public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count)
    {
        _inner.Write(buffer, offset, count);
        _hash.AppendData(buffer, offset, count);
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        _inner.Write(buffer);
        _hash.AppendData(buffer);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        _hash.AppendData(buffer.Span);
        return _inner.WriteAsync(buffer, cancellationToken);
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        _hash.AppendData(buffer, offset, count);
        return _inner.WriteAsync(buffer, offset, count, cancellationToken);
    }

    /// <summary>
    /// Gets the lowercase hex-encoded SHA-256 digest of all written data.
    /// </summary>
    public string GetHash()
    {
        _computedHash ??= Convert.ToHexString(_hash.GetHashAndReset()).ToLowerInvariant();
        return _computedHash;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _hash.Dispose();
        }
        base.Dispose(disposing);
    }
}
