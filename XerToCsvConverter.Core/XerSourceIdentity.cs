using System.Globalization;

namespace XerToCsvConverter;

/// <summary>An input occurrence, never a filename-, path- or content-hash-keyed input.</summary>
internal sealed record XerSourceIdentity(string SourceToken, string OriginalFilename, string PublicNamespace)
{
    internal static XerSourceIdentity[] CreateOrdered(IReadOnlyList<string> names)
    {
        ArgumentNullException.ThrowIfNull(names);
        foreach (string name in names) ArgumentException.ThrowIfNullOrWhiteSpace(name);
        string[] namespaces = names.Select(name => name.Trim()).ToArray();
        var duplicateNames = namespaces.GroupBy(name => name, StringComparer.OrdinalIgnoreCase)
            .Where(group => group.Count() > 1).Select(group => group.Key)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        // Reserve even unusual real filenames so a generated namespace cannot shadow one.
        var reserved = namespaces.ToHashSet(StringComparer.OrdinalIgnoreCase);
        string batchToken = Guid.NewGuid().ToString("N");
        var result = new XerSourceIdentity[names.Count];
        for (int i = 0; i < names.Count; i++)
        {
            string ordinal = "source-" + (i + 1).ToString("D6", CultureInfo.InvariantCulture);
            string token = batchToken + ":" + ordinal;
            string source = names[i];
            string publicNamespace = namespaces[i];
            if (duplicateNames.Contains(publicNamespace))
            {
                string candidate = publicNamespace + "#" + ordinal;
                publicNamespace = candidate;
                int suffix = 0;
                while (!reserved.Add(publicNamespace))
                    publicNamespace = candidate + "-" + (++suffix).ToString(CultureInfo.InvariantCulture);
            }
            result[i] = new XerSourceIdentity(token, source, publicNamespace);
        }
        return result;
    }

    internal XerDataStore ApplyTo(XerDataStore source)
    {
        var result = new XerDataStore();
        foreach (string name in source.TableNames)
        {
            XerTable input = source.GetTable(name)!;
            var table = new XerTable(name, input.RowCount);
            if (input.Headers is not null) table.SetHeaders(input.Headers.ToArray());
            foreach (DataRow row in input.Rows)
                table.AddRow(row with
                {
                    SourceFilename = PublicNamespace,
                    SourceToken = SourceToken,
                    OriginalSourceFilename = OriginalFilename
                });
            result.AddTable(table);
        }
        return result;
    }
}
