namespace XerToCsvConverter;

// A projection binds its fixed contract once, after the enhanced table schema is complete.
// Bindings are deliberately operation-local: another source or export can have different headers.
internal static class ReviewProjectionAccess
{
    internal static int ResolveIndex(XerTable table, string name, IReadOnlyList<string> aliases)
    {
        if (table.FieldIndexes.TryGetValue(name, out int index)) return index;
        foreach (string alias in aliases)
            if (table.FieldIndexes.TryGetValue(alias, out index)) return index;
        return -1;
    }

    internal static IReadOnlyDictionary<string, int> Bind<TColumn>(XerTable table,
        IReadOnlyList<TColumn> columns, Func<TColumn, string> name, Func<TColumn, IReadOnlyList<string>> aliases)
    {
        var indexes = new Dictionary<string, int>(columns.Count, StringComparer.OrdinalIgnoreCase);
        foreach (TColumn column in columns)
            indexes.Add(name(column), ResolveIndex(table, name(column), aliases(column)));
        return indexes;
    }
}
