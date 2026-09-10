using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter;

// Review schemas are typed projections. A bad source cell must not remove the
// rest of its row, and warning evidence must remain tied to its input occurrence.
internal sealed class ReviewDataQualityCollector
{
    private readonly XerDataStore _store;
    // Array equality is reference equality; source occurrence is still essential
    // because rebinding/cloning can retain the same immutable evidence array.
    private readonly Dictionary<XerTable, Dictionary<(string Source, string[] Fields), ReviewRowEvidence>> _evidence = new();
    private readonly Dictionary<XerTable, Dictionary<(string Source, object Identity), ReviewRowEvidence>> _rawEvidence = new();
    private readonly Dictionary<(string Source, object Identity), HashSet<string>> _invalidColumns = new();
    internal List<DataRow> Rows { get; } = new();
    internal ReviewDataQualityCollector(XerDataStore store) => _store = store;

    internal ReviewRowEvidence ForRow(XerTable table, DataRow row)
    {
        if (!_evidence.TryGetValue(table, out var rows))
        {
            rows = new();
            var ordinals = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (DataRow item in table.Rows)
            {
                int ordinal = ordinals.GetValueOrDefault(item.SourceToken) + 1;
                ordinals[item.SourceToken] = ordinal;
                rows[(item.SourceToken, item.Fields)] = new ReviewRowEvidence(table, item, ordinal);
            }
            _evidence[table] = rows;
        }
        return rows.TryGetValue((row.SourceToken, row.Fields), out var evidence) ? evidence : new(table, row, 0);
    }

    internal string Evaluate(ReviewRowEvidence evidence, string column, string raw, Func<string> calculation)
    {
        try { return calculation(); }
        catch (Exception ex) when (ex is ProgrammeReviewValidationException
            or TenderReviewValidationException or OverflowException)
        {
            var identity = (evidence.Row.SourceToken, evidence.Row.RawEvidenceIdentity);
            if (!_invalidColumns.TryGetValue(identity, out var columns))
                _invalidColumns[identity] = columns = new(StringComparer.OrdinalIgnoreCase);
            columns.Add(column);
            Warn(evidence, "REVIEW_VALUE_INVALID", ex.Message, column, raw);
            return string.Empty;
        }
    }

    internal bool HasInvalidColumn(ReviewRowEvidence evidence, params string[] names) =>
        _invalidColumns.TryGetValue((evidence.Row.SourceToken, evidence.Row.RawEvidenceIdentity), out var columns) && names.Any(columns.Contains);

    internal void Warn(ReviewRowEvidence evidence, string code, string message,
        string column = "", string raw = "", string? outputTableName = null)
    {
        string rawTableName = evidence.Table.Name switch
        {
            "01_XER_TASK" => "TASK", "02_XER_PROJECT" => "PROJECT", "03_XER_PROJWBS" => "PROJWBS",
            "06_XER_PREDECESSOR" => "TASKPRED", "07_XER_ACTVTYPE" => "ACTVTYPE",
            "08_XER_ACTVCODE" => "ACTVCODE", "09_XER_TASKACTV" => "TASKACTV",
            "10_XER_CALENDAR" => "CALENDAR", "12_XER_RSRC" => "RSRC",
            "15_XER_RESOURCE_DISTRIBUTION" => "TASKRSRC", _ => evidence.Table.Name
        };
        XerTable rawTable = _store.GetTable(rawTableName) ?? evidence.Table;
        if (!_rawEvidence.TryGetValue(rawTable, out var sourceEvidence))
        {
            sourceEvidence = new();
            var ambiguous = new HashSet<(string Source, object Identity)>();
            foreach (DataRow row in rawTable.Rows)
            {
                var identity = (row.SourceToken, row.RawEvidenceIdentity);
                if (ambiguous.Contains(identity) || !sourceEvidence.TryAdd(identity, ForRow(rawTable, row)))
                {
                    ambiguous.Add(identity);
                    sourceEvidence.Remove(identity);
                }
            }
            _rawEvidence[rawTable] = sourceEvidence;
        }
        int sourceOrdinal = sourceEvidence.TryGetValue((evidence.Row.SourceToken, evidence.Row.RawEvidenceIdentity), out var originalEvidence)
            ? originalEvidence.Ordinal : 0;
        // WithFields preserves immutable source evidence even when a derived row
        // has more columns. Do not serialize generated Tender token-prefixed keys.
        string[] rawFields = (rawTable.Headers ?? []).Select(name => evidence.Row.GetRawField(name).RawValue).ToArray();
        DataRow rawRow = evidence.Row.WithFields(rawFields);
        string publicSource = evidence.Row.OriginalSourceFilename;
        string Safe(string value) => value.Replace(evidence.Row.SourceToken, publicSource, StringComparison.Ordinal)
            .Replace(evidence.Row.SourceFilename + ".", publicSource + ".", StringComparison.Ordinal);
        Rows.Add(XerDataQuality.CreateWarning(outputTableName ?? evidence.Table.Name, code, Safe(message), evidence.Row,
            sourceOrdinal,
            rawTableName, column, Safe(raw), XerDataQuality.RawRowJson(rawTable, rawRow)));
    }
}

internal sealed record ReviewRowEvidence(XerTable Table, DataRow Row, int Ordinal);
