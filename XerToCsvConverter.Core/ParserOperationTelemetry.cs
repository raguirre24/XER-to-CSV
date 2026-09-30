using System.Diagnostics;
using System.IO.Compression;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("ParserStreamingParity")]

namespace XerToCsvConverter;

internal enum ParserOperationPhase
{
    FileReading, Metadata, SourceSha256, Parsing, Merging, Transformation,
    StandaloneValidation, CsvSerialization, CsvHashing, ZipCompressionFinalization
}

internal sealed record ParserPhaseTiming(ParserOperationPhase Phase, double InclusiveMs,
    double ExclusiveMs, int Scopes, long BufferedCalls);

internal sealed record ParserScopeTiming(ParserOperationPhase Phase, string? Detail,
    double StartMs, double EndMs, double InclusiveMs, double ExclusiveMs, bool OverlappingChildren);

internal sealed record ParserOperationTimingSnapshot(double ElapsedMs, bool HasOverlaps,
    IReadOnlyList<ParserPhaseTiming> Phases, IReadOnlyList<ParserScopeTiming> Scopes);

/// <summary>
/// Opt-in wall-clock evidence for one async operation. Disabled scopes do not read
/// the clock. Exclusive time subtracts nested work; inclusive totals must never be
/// summed. Parallel work is identified explicitly and can exceed operation time.
/// Inline cell parsing/normalisation remains transformation work. Buffered ZIP and
/// digest stream timers aggregate calls, never create a scope for each CSV row.
/// </summary>
internal static class ParserOperationTelemetry
{
    private static readonly AsyncLocal<Context?> Current = new();
    internal sealed record Context(Operation Operation, Frame? Frame);

    internal static Operation BeginOperation()
    {
        var operation = new Operation(Current.Value);
        Current.Value = new Context(operation, null);
        return operation;
    }

    internal static Scope Measure(ParserOperationPhase phase, string? detail = null)
    {
        Context? previous = Current.Value;
        if (previous is null) return default;
        var frame = new Frame(previous, phase, detail);
        Current.Value = new Context(previous.Operation, frame);
        return new Scope(frame);
    }

    internal static T Time<T>(ParserOperationPhase phase, string? detail, Func<T> action)
    {
        using var scope = Measure(phase, detail);
        return action();
    }

    internal static TimedZipArchive CreateZipArchive(Stream output) => new(output);

    internal static Stream OpenZipEntry(ZipArchiveEntry entry)
    {
        using var scope = Measure(ParserOperationPhase.ZipCompressionFinalization, entry.Name + ":open");
        Stream inner = entry.Open();
        return Current.Value is { } context
            ? new TimedWriteStream(inner, context.Operation, ParserOperationPhase.ZipCompressionFinalization)
            : inner;
    }

    // The hashing timer surrounds the existing digest stream, subtracting all
    // inner ZIP calls. This includes digest bookkeeping/stream overhead, rather
    // than claiming that both timers independently measured the same write.
    internal static Stream TrackCsvHashing(Stream hashingStream, Stream zipStream, string detail) =>
        Current.Value is { } context
            ? new TimedWriteStream(hashingStream, context.Operation, ParserOperationPhase.CsvHashing,
                zipStream as TimedWriteStream, leaveOpen: true)
            : hashingStream;

    internal sealed class Operation : IDisposable
    {
        private readonly Context? _previous;
        private readonly long _started = Stopwatch.GetTimestamp();
        private readonly object _gate = new();
        private readonly Dictionary<ParserOperationPhase, Totals> _totals = new();
        private readonly List<ParserScopeTiming> _scopes = new();
        private long? _ended;
        internal int _rootActivities;
        private bool _overlaps;

        private sealed class Totals { internal long Inclusive, Exclusive, BufferedCalls; internal int Scopes; }

        internal Operation(Context? previous) => _previous = previous;

        internal ParserOperationTimingSnapshot Snapshot()
        {
            lock (_gate)
                return new(ToMs((_ended ?? Stopwatch.GetTimestamp()) - _started), _overlaps,
                    _totals.OrderBy(pair => pair.Key).Select(pair => new ParserPhaseTiming(pair.Key,
                        ToMs(pair.Value.Inclusive), ToMs(pair.Value.Exclusive), pair.Value.Scopes,
                        pair.Value.BufferedCalls)).ToArray(), _scopes.ToArray());
        }

        internal void EnterRoot()
        {
            if (Interlocked.Increment(ref _rootActivities) > 1) MarkOverlap();
        }

        internal void MarkOverlap() { lock (_gate) _overlaps = true; }

        internal void Record(Frame frame, long end, long exclusive)
        {
            lock (_gate)
            {
                Add(frame.Phase, end - frame.Start, exclusive, 1, 0);
                _scopes.Add(new(frame.Phase, frame.Detail, ToMs(frame.Start - _started),
                    ToMs(end - _started), ToMs(end - frame.Start), ToMs(exclusive), frame.Overlaps));
            }
        }

        internal void RecordBuffered(ParserOperationPhase phase, long inclusive, long exclusive, long calls)
        {
            lock (_gate) Add(phase, inclusive, exclusive, 0, calls);
        }

        private void Add(ParserOperationPhase phase, long inclusive, long exclusive, int scopes, long calls)
        {
            if (!_totals.TryGetValue(phase, out Totals? total)) _totals.Add(phase, total = new());
            total.Inclusive += inclusive; total.Exclusive += exclusive;
            total.Scopes += scopes; total.BufferedCalls += calls;
        }

        public void Dispose()
        {
            if (_ended is not null) return;
            _ended = Stopwatch.GetTimestamp();
            if (ReferenceEquals(Current.Value?.Operation, this)) Current.Value = _previous;
        }
    }

    internal readonly struct Scope : IDisposable
    {
        private readonly Frame? _frame;
        internal Scope(Frame frame) => _frame = frame;
        public void Dispose() => _frame?.End();
    }

    internal sealed class Frame
    {
        private readonly Context _previous;
        private readonly List<(long Start, long End)> _children = new();
        private long _bufferedChildren;
        private int _activeChildren, _ended;
        internal readonly ParserOperationPhase Phase;
        internal readonly string? Detail;
        internal readonly long Start = Stopwatch.GetTimestamp();
        internal bool Overlaps;

        internal Frame(Context previous, ParserOperationPhase phase, string? detail)
        {
            _previous = previous; Phase = phase; Detail = detail;
            if (previous.Frame is { } parent)
            {
                if (Interlocked.Increment(ref parent._activeChildren) > 1)
                { parent.Overlaps = true; previous.Operation.MarkOverlap(); }
            }
            else previous.Operation.EnterRoot();
        }

        internal void AddBuffered(long ticks) => Interlocked.Add(ref _bufferedChildren, ticks);

        internal void End()
        {
            if (Interlocked.Exchange(ref _ended, 1) != 0) return;
            long end = Stopwatch.GetTimestamp();
            long childTicks = 0, intervalStart = 0, intervalEnd = 0;
            lock (_children)
                foreach ((long start, long finish) in _children.OrderBy(interval => interval.Start))
                {
                    if (intervalEnd == 0) { intervalStart = start; intervalEnd = finish; }
                    else if (start <= intervalEnd) intervalEnd = Math.Max(intervalEnd, finish);
                    else { childTicks += intervalEnd - intervalStart; intervalStart = start; intervalEnd = finish; }
                }
            if (intervalEnd != 0) childTicks += intervalEnd - intervalStart;
            long exclusive = Math.Max(0, end - Start - childTicks - Interlocked.Read(ref _bufferedChildren));
            _previous.Operation.Record(this, end, exclusive);
            if (_previous.Frame is { } parent)
            {
                lock (parent._children) parent._children.Add((Start, end));
                Interlocked.Decrement(ref parent._activeChildren);
            }
            else Interlocked.Decrement(ref _previous.Operation._rootActivities);
            Current.Value = _previous;
        }
    }

    internal sealed class TimedZipArchive : IDisposable
    {
        private readonly ZipArchive _archive;
        internal TimedZipArchive(Stream output)
        {
            using var scope = Measure(ParserOperationPhase.ZipCompressionFinalization, "archive:create");
            _archive = new ZipArchive(output, ZipArchiveMode.Create, leaveOpen: true);
        }
        internal ZipArchiveEntry CreateEntry(string name, CompressionLevel compression)
        {
            using var scope = Measure(ParserOperationPhase.ZipCompressionFinalization, name + ":create");
            return _archive.CreateEntry(name, compression);
        }
        public void Dispose()
        {
            using var scope = Measure(ParserOperationPhase.ZipCompressionFinalization, "archive:finalize");
            _archive.Dispose();
        }
    }

    private sealed class TimedWriteStream(Stream inner, Operation operation, ParserOperationPhase phase,
        TimedWriteStream? child = null, bool leaveOpen = false) : Stream
    {
        private long _inclusive, _exclusive, _calls;
        private bool _disposed;
        internal long InclusiveTicks => _inclusive;
        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => inner.CanSeek;
        public override bool CanWrite => inner.CanWrite;
        public override long Length => inner.Length;
        public override long Position { get => inner.Position; set => inner.Position = value; }
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);
        public override void SetLength(long value) => inner.SetLength(value);

        private void Record(long start, long childBefore)
        {
            long elapsed = Stopwatch.GetTimestamp() - start;
            long exclusive = Math.Max(0, elapsed - ((child?.InclusiveTicks ?? 0) - childBefore));
            _inclusive += elapsed; _exclusive += exclusive; _calls++;
            if (Current.Value is { } context && ReferenceEquals(context.Operation, operation))
                context.Frame?.AddBuffered(exclusive);
        }
        public override void Write(byte[] buffer, int offset, int count)
        {
            long before = child?.InclusiveTicks ?? 0, start = Stopwatch.GetTimestamp();
            try { inner.Write(buffer, offset, count); } finally { Record(start, before); }
        }
        public override void Write(ReadOnlySpan<byte> buffer)
        {
            long before = child?.InclusiveTicks ?? 0, start = Stopwatch.GetTimestamp();
            try { inner.Write(buffer); } finally { Record(start, before); }
        }
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            long before = child?.InclusiveTicks ?? 0, start = Stopwatch.GetTimestamp();
            try { await inner.WriteAsync(buffer, cancellationToken).ConfigureAwait(false); }
            finally { Record(start, before); }
        }
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken token) =>
            WriteAsync(buffer.AsMemory(offset, count), token).AsTask();
        public override void Flush()
        {
            long before = child?.InclusiveTicks ?? 0, start = Stopwatch.GetTimestamp();
            try { inner.Flush(); } finally { Record(start, before); }
        }
        public override async Task FlushAsync(CancellationToken token)
        {
            long before = child?.InclusiveTicks ?? 0, start = Stopwatch.GetTimestamp();
            try { await inner.FlushAsync(token).ConfigureAwait(false); } finally { Record(start, before); }
        }
        protected override void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                _disposed = true;
                if (!leaveOpen)
                {
                    long start = Stopwatch.GetTimestamp();
                    try { inner.Dispose(); } finally { Record(start, 0); }
                }
                operation.RecordBuffered(phase, _inclusive, _exclusive, _calls);
            }
            base.Dispose(disposing);
        }
    }

    private static double ToMs(long ticks) => ticks * (1000d / Stopwatch.Frequency);
}
