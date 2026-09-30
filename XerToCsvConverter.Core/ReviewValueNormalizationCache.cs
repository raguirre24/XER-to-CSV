namespace XerToCsvConverter;

/// <summary>
/// Operation-owned memo of successful pure conversions. Required/blank checks and
/// every failed conversion still run in the originating row's diagnostic context.
/// No failures, evidence, source identities or mutable projected rows are cached.
/// </summary>
internal sealed class ReviewValueNormalizationCache
{
    private const int MaximumEntries = 8192;
    private const long MaximumEstimatedBytes = 2 * 1024 * 1024;
    private readonly Dictionary<(int Type, string Raw), string> _values = new();
    private long _estimatedBytes;
    internal int Count => _values.Count;
    internal bool TryGet(int type, string raw, out string value) => _values.TryGetValue((type, raw), out value!);
    internal void Add(int type, string raw, string value)
    {
        long size = 96L + raw.Length * 2L + value.Length * 2L;
        if (_values.Count < MaximumEntries && _estimatedBytes + size <= MaximumEstimatedBytes && _values.TryAdd((type, raw), value))
            _estimatedBytes += size;
    }
}
