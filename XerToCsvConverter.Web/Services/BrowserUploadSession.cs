using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Web.Services;

/// <summary>
/// Serializes upload admission and gives each pending selection a cancellable generation.
/// Invalidation never resets source identity, including when an old read finishes later.
/// </summary>
public sealed class BrowserUploadSession : IDisposable
{
    private readonly object _gate = new();
    private Batch? _current;
    private int _nextSourceOrdinal;
    private bool _disposed;

    public bool IsUploading { get { lock (_gate) return _current is not null; } }

    public Batch? TryBegin()
    {
        lock (_gate)
        {
            if (_disposed || _current is not null) return null;
            return _current = new Batch();
        }
    }

    public string NextSourceToken(Batch batch)
    {
        lock (_gate)
        {
            RequireCurrent(batch);
            int ordinal = _nextSourceOrdinal;
            _nextSourceOrdinal = checked(ordinal + 1);
            return TenderReviewNaming.CreateSourceToken(ordinal);
        }
    }

    public bool IsCurrent(Batch batch)
    {
        lock (_gate) return !_disposed && ReferenceEquals(_current, batch) && !batch.Token.IsCancellationRequested;
    }

    /// <summary>The commit and its final validation run synchronously as one generation check.</summary>
    public bool TryCommit(Batch batch, Action commit)
    {
        ArgumentNullException.ThrowIfNull(commit);
        lock (_gate)
        {
            if (!IsCurrent(batch)) return false;
            commit();
            return true;
        }
    }

    public void Complete(Batch batch)
    {
        lock (_gate)
        {
            if (ReferenceEquals(_current, batch)) _current = null;
            batch.Dispose();
        }
    }

    public void Invalidate()
    {
        lock (_gate)
        {
            Batch? pending = _current;
            _current = null;
            pending?.Cancel();
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
            Invalidate();
        }
    }

    private void RequireCurrent(Batch batch)
    {
        if (!IsCurrent(batch)) throw new OperationCanceledException(batch.Token);
    }

    public sealed class Batch : IDisposable
    {
        private readonly CancellationTokenSource _cancellation = new();
        internal Batch() => Token = _cancellation.Token;
        public CancellationToken Token { get; }
        internal void Cancel() => _cancellation.Cancel();
        public void Dispose() => _cancellation.Dispose();
    }

    /// <summary>Use immediately before committing, against the actual current plus pending rows.</summary>
    public static void ValidateCombined(
        IEnumerable<BrowserUploadDescriptor> files,
        int maximumFiles,
        long maximumFileBytes,
        long maximumTotalBytes,
        bool allowRepeatedNames)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        int count = 0;
        long total = 0;
        foreach (BrowserUploadDescriptor file in files)
        {
            if (++count > maximumFiles)
                throw new InvalidOperationException($"This profile accepts at most {maximumFiles} XER files.");
            if (file.Size < 0 || file.Size > maximumFileBytes)
                throw new InvalidOperationException($"{file.Name} exceeds the profile's per-file size limit.");
            total = checked(total + file.Size);
            if (total > maximumTotalBytes)
                throw new InvalidOperationException("The combined files exceed this profile's browser memory limit. Use the Windows app for larger inputs.");
            if (!allowRepeatedNames && !names.Add(file.Name))
                throw new InvalidOperationException("Programme Review does not accept duplicate XER filenames.");
        }
    }
}

public sealed record BrowserUploadDescriptor(string Name, long Size);
