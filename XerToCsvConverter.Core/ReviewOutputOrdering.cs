namespace XerToCsvConverter;

internal static class ReviewOutputOrdering
{
    private const string Separator = "\u001f";

    // Match the published string.Join ordering without allocating joined keys.
    // Comparing columns individually is different when values contain/prefix the separator.
    internal static int CompareJoined(IReadOnlyDictionary<string, string> left,
        IReadOnlyDictionary<string, string> right, IReadOnlyList<string> columns)
    {
        var x = new JoinedCursor(left, columns);
        var y = new JoinedCursor(right, columns);
        while (true)
        {
            bool hasX = x.TryGetChunk(out ReadOnlySpan<char> spanX);
            bool hasY = y.TryGetChunk(out ReadOnlySpan<char> spanY);
            if (!hasX || !hasY) return hasX.CompareTo(hasY);
            int length = Math.Min(spanX.Length, spanY.Length);
            int comparison = spanX[..length].SequenceCompareTo(spanY[..length]);
            if (comparison != 0) return comparison;
            x.Advance(length);
            y.Advance(length);
        }
    }

    internal static IReadOnlyList<TRow> Sort<TRow>(IReadOnlyList<TRow> rows,
        IReadOnlyList<string> sortColumns, IReadOnlyList<string> allColumns,
        Func<TRow, IReadOnlyDictionary<string, string>> values, Comparison<TRow> finalComparison)
    {
        if (rows.Count <= 1) return rows;
        var order = new int[rows.Count];
        var keys = new string[rows.Count];
        var tieKeys = new string?[rows.Count];
        for (int index = 0; index < order.Length; index++)
        {
            order[index] = index;
            keys[index] = Join(values(rows[index]), sortColumns);
        }
        Array.Sort(order, (left, right) =>
        {
            TRow x = rows[left], y = rows[right];
            // Materialise each key once. Repeated dictionary reads and cursor calls
            // inside O(n log n) comparisons are costly in the WASM interpreter.
            // Joined keys exactly preserve the published ordinal separator rules.
            int comparison = string.CompareOrdinal(keys[left], keys[right]);
            if (comparison == 0)
                comparison = string.CompareOrdinal(tieKeys[left] ??= Join(values(x), allColumns),
                    tieKeys[right] ??= Join(values(y), allColumns));
            if (comparison == 0) comparison = finalComparison(x, y);
            // Enumerable.OrderBy/ThenBy preserved the original order of complete ties.
            return comparison != 0 ? comparison : left.CompareTo(right);
        });
        var result = new TRow[rows.Count];
        for (int index = 0; index < result.Length; index++) result[index] = rows[order[index]];
        return result;

        static string Join(IReadOnlyDictionary<string, string> rowValues, IReadOnlyList<string> columns) =>
            string.Join(Separator, columns.Select(column => rowValues[column]));
    }

    private struct JoinedCursor
    {
        private readonly IReadOnlyDictionary<string, string> _values;
        private readonly IReadOnlyList<string> _columns;
        private int _column;
        private int _offset;
        private bool _separator;
        private string _current;

        internal JoinedCursor(IReadOnlyDictionary<string, string> values, IReadOnlyList<string> columns)
        {
            _values = values;
            _columns = columns;
            _column = 0;
            _offset = 0;
            _separator = false;
            _current = columns.Count == 0 ? string.Empty : values[columns[0]] ?? string.Empty;
        }

        internal bool TryGetChunk(out ReadOnlySpan<char> chunk)
        {
            while (_offset == _current.Length)
            {
                if (!_separator && _column >= _columns.Count - 1)
                {
                    chunk = default;
                    return false;
                }
                if (_separator)
                {
                    _column++;
                    _current = _values[_columns[_column]] ?? string.Empty;
                    _separator = false;
                }
                else
                {
                    _current = Separator;
                    _separator = true;
                }
                _offset = 0;
            }
            chunk = _current.AsSpan(_offset);
            return true;
        }

        internal void Advance(int length) => _offset += length;
    }
}
