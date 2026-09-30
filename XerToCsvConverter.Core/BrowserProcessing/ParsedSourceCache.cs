namespace XerToCsvConverter.BrowserProcessing;

/// <summary>A private, operation-independent cache of raw parsed sources. Views are rebound per job.</summary>
internal sealed class ParsedSourceCache
{
    internal const long DefaultBudgetBytes = 256L * 1024 * 1024;
    private readonly Dictionary<Key, Entry> _entries = new();
    private readonly LinkedList<Key> _lru = new();
    public long BudgetBytes { get; private set; }
    public long RetainedBytes { get; private set; }
    public int Count => _entries.Count;

    public ParsedSourceCache(long budgetBytes = DefaultBudgetBytes) => SetBudget(budgetBytes);

    public void SetBudget(long bytes)
    {
        if (bytes < 0 || bytes > DefaultBudgetBytes) throw new ArgumentOutOfRangeException(nameof(bytes));
        BudgetBytes = bytes;
        EvictToFit(0);
    }

    public bool TryGet(string token, string sha256, out XerDataStore? store)
    {
        var key = new Key(token, sha256, ParserVersion);
        if (!_entries.TryGetValue(key, out Entry? entry)) { store = null; return false; }
        _lru.Remove(entry.Node);
        _lru.AddLast(entry.Node);
        store = entry.Store;
        return true;
    }

    public void Retain(string token, string sha256, XerDataStore store, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        long estimate = EstimateBytes(store, cancellationToken);
        Remove(token);
        if (estimate > BudgetBytes) return;
        EvictToFit(estimate);
        var key = new Key(token, sha256, ParserVersion);
        LinkedListNode<Key> node = _lru.AddLast(key);
        _entries.Add(key, new Entry(store, estimate, node));
        RetainedBytes += estimate;
    }

    public void Remove(string token)
    {
        foreach (Key key in _entries.Keys.Where(key => key.Token == token).ToArray()) Evict(key);
    }

    public void Clear() { _entries.Clear(); _lru.Clear(); RetainedBytes = 0; }

    private void EvictToFit(long additional)
    {
        while (_lru.First is { } first && RetainedBytes + additional > BudgetBytes) Evict(first.Value);
    }

    private void Evict(Key key)
    {
        Entry entry = _entries[key];
        _entries.Remove(key);
        _lru.Remove(entry.Node);
        RetainedBytes -= entry.Bytes;
    }

    // Deliberately conservative: shared strings are charged for each occurrence.
    // Both live/evidence arrays, excess original cells, headers, unused row-list
    // capacity and row/container overhead are included. This
    // bounds retained cache estimates, not active or runtime/browser heap memory.
    internal static long EstimateBytes(XerDataStore store, CancellationToken token = default)
    {
        long bytes = 512;
        foreach (string name in store.TableNames)
        {
            XerTable table = store.GetTable(name)!;
            bytes += 1024 + (table.Headers?.Length ?? 0) * 96L;
            foreach (string? header in table.Headers ?? []) bytes += 24 + (header?.Length ?? 0) * 2L;
            bytes += (table.Capacity - table.RowCount) * 64L;
            foreach (DataRow row in table.Rows)
            {
                token.ThrowIfCancellationRequested();
                bytes += row.EstimatedRetainedBytes;
            }
        }
        return bytes;
    }

    private static string ParserVersion => "raw-v1/" + typeof(XerParser).Assembly.GetName().Version;
    private sealed record Key(string Token, string Sha256, string ParserVersion);
    private sealed record Entry(XerDataStore Store, long Bytes, LinkedListNode<Key> Node);
}
