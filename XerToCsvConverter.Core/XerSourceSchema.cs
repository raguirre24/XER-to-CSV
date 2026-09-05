namespace XerToCsvConverter;

/// <summary>
/// Validate source schemas before a merge or calculation can hide ambiguous fields.
/// Header case and order remain exactly as supplied for valid schemas.
/// </summary>
internal static class XerSourceSchema
{
    internal static void ValidateHeaders(string tableName, IReadOnlyList<string>? headers)
    {
        if (headers is null || headers.Count == 0)
            throw new InvalidDataException($"XER table '{tableName}' has no source header fields.");

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < headers.Count; i++)
        {
            string header = headers[i];
            if (string.IsNullOrWhiteSpace(header))
                throw new InvalidDataException($"XER table '{tableName}' has a blank source header at column {i + 1}.");
            if (!seen.Add(header))
                throw new InvalidDataException($"XER table '{tableName}' has duplicate source header '{header}' (case-insensitive). No fields may be discarded or selected by column order.");
        }
    }
}
