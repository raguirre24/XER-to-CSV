using System.Collections.Concurrent;
using System.Globalization;

namespace XerToCsvConverter;

public partial class XerTransformer
{
    private readonly ConcurrentDictionary<string, ConcurrentQueue<DataRow>> _exportWarnings =
        new(StringComparer.OrdinalIgnoreCase);

    internal void RecordDataQualityWarning(string tableName, string issueCode, string message,
        DataRow sourceRow, int sourceRowNumber, string sourceTable = "", string columnName = "",
        string rawValue = "", string rawRowJson = "") =>
        _exportWarnings.GetOrAdd(tableName, _ => new()).Enqueue(XerDataQuality.CreateWarning(
            tableName, issueCode, message, sourceRow, sourceRowNumber, sourceTable, columnName, rawValue, rawRowJson));

    private static int[] SourceOrdinals(XerTable table)
    {
        var counts = new Dictionary<string, int>(StringComparer.Ordinal);
        return table.Rows.Select(row =>
        {
            counts.TryGetValue(row.SourceToken, out int ordinal);
            return counts[row.SourceToken] = ordinal + 1;
        }).ToArray();
    }

    private string[] CreateEnhancedHeaders(XerTable source, string tableName, IEnumerable<string> additions)
    {
        string[] derived = additions.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        var headers = StandardExportSchema.CreateHeaders(source.Headers ?? [], derived);
        int[] ordinals = SourceOrdinals(source);
        for (int index = 0; index < (source.Headers?.Length ?? 0); index++)
        {
            string original = source.Headers![index], replacement = headers[index];
            if (original == replacement) continue;
            for (int i = 0; i < source.RowCount; i++)
                RecordDataQualityWarning(tableName, "SOURCE_COLUMN_COLLISION",
                    $"Imported column '{original}' is preserved as '{replacement}' to keep the calculated/provenance column unambiguous.",
                    source.Rows[i], ordinals[i], source.Name, original,
                    index < source.Rows[i].Fields.Length ? source.Rows[i].Fields[index] : "",
                    XerDataQuality.RawRowJson(source, source.Rows[i]));
        }
        return headers;
    }

    private void RecordTaskCalculationWarnings(XerTable source, XerTable output,
        Dictionary<(string Source, string Id), decimal> hours,
        Dictionary<(string Source, string Id), DateTime> dataDates)
    {
        int[] ordinals = SourceOrdinals(source);
        var duplicateIds = source.Rows.GroupBy(row => (row.SourceToken,
            Id: GetFieldValue(row.Fields, source.FieldIndexes, FieldNames.TaskId).Trim()))
            .Where(group => group.Skip(1).Any()).Select(group => group.Key).ToHashSet();
        for (int index = 0; index < source.RowCount; index++)
        {
            DataRow row = source.Rows[index];
            string Read(string field) => GetFieldValue(row.Fields, source.FieldIndexes, field);
            string Value(string field) => GetFieldValue(output.Rows[index].Fields, output.FieldIndexes, field);
            void Warn(string code, string field, string message) => RecordDataQualityWarning(
                EnhancedTableNames.XerTask01, code, message, row, ordinals[index], source.Name, field, Read(field), XerDataQuality.RawRowJson(source, row));
            string id = Read(FieldNames.TaskId).Trim(), status = Read(FieldNames.StatusCode).Trim().ToUpperInvariant();
            if (id.Length == 0 || duplicateIds.Contains((row.SourceToken, id)))
                Warn("TASK_IDENTITY_INVALID", FieldNames.TaskId, "Missing or duplicate activity identity; all imported occurrences are retained. Dependent identity resolution remains unknown.");
            foreach (string field in new[] { FieldNames.CstrDate, FieldNames.TargetStartDate, FieldNames.TargetEndDate,
                FieldNames.ActStartDate, FieldNames.ActEndDate, FieldNames.EarlyStartDate, FieldNames.EarlyEndDate,
                FieldNames.LateStartDate, FieldNames.LateEndDate, FieldNames.RestartDate, FieldNames.ReendDate })
                if (!string.IsNullOrWhiteSpace(Read(field)) && DateParser.TryParse(Read(field)) is null)
                    Warn("ACTIVITY_DATE_INVALID", field, "The imported date cannot be parsed. Its raw value is retained here; dependent date calculations remain blank.");
            foreach (var pair in new[] { (FieldNames.RemainDurationHrCnt, FieldNames.RemainingDuration),
                (FieldNames.TargetDurationHrCnt, FieldNames.OriginalDuration),
                (FieldNames.TotalFloatHrCnt, FieldNames.TotalFloat), (FieldNames.FreeFloatHrCnt, FieldNames.FreeFloat) })
            {
                if (status == "TK_COMPLETE" && pair.Item1 is FieldNames.TotalFloatHrCnt or FieldNames.FreeFloatHrCnt) continue;
                if (!string.IsNullOrWhiteSpace(Read(pair.Item1)) && Value(pair.Item2).Length == 0)
                    Warn("ACTIVITY_CONVERSION_UNAVAILABLE", pair.Item1,
                        $"'{pair.Item2}' requires representable source hours and a unique positive calendar hours-per-day factor. No default factor or zero is substituted.");
            }
            if (status is not ("TK_NOTSTART" or "TK_ACTIVE" or "TK_COMPLETE"))
                Warn("ACTIVITY_STATUS_INVALID", FieldNames.StatusCode, "Unrecognised activity state; imported fields remain available but state-dependent calculations cannot be inferred.");
            else if (status == "TK_ACTIVE" && Value(FieldNames.PercentComplete).Length == 0
                && !string.IsNullOrWhiteSpace(Read("complete_pct_type")))
                Warn("ACTIVITY_PERCENTAGE_UNAVAILABLE", "complete_pct_type", "Completion percentage cannot be established from the exported method and quantities; it is not zero.");
        }
    }

    private void RecordSimpleIdentityWarnings(XerTable source, string tableName)
    {
        string? identity = source.Name.ToUpperInvariant() switch
        {
            "PROJECT" => "proj_id", "ACTVTYPE" => "actv_code_type_id", "ACTVCODE" => "actv_code_id",
            "CALENDAR" => "clndr_id", "RSRC" => "rsrc_id", "TASKRSRC" => "taskrsrc_id", "UMEASURE" => "unit_id",
            _ => null
        };
        if (identity is null) return;
        string Read(DataRow row) => GetFieldValue(row.Fields, source.FieldIndexes, identity).Trim();
        var duplicates = source.Rows.GroupBy(row => (row.SourceToken, Id: Read(row)))
            .Where(group => group.Skip(1).Any()).Select(group => group.Key).ToHashSet();
        int[] ordinals = SourceOrdinals(source);
        for (int i = 0; i < source.RowCount; i++)
        {
            DataRow row = source.Rows[i];
            string id = Read(row);
            if (id.Length == 0 || duplicates.Contains((row.SourceToken, id)))
                RecordDataQualityWarning(tableName, "SOURCE_IDENTITY_INVALID",
                    $"'{source.Name}.{identity}' is blank or duplicated within this input. All raw occurrences are preserved; do not assume these keys are unique.",
                    row, ordinals[i], source.Name, identity, id, XerDataQuality.RawRowJson(source, row));
        }
    }

    internal IEnumerable<DataRow> GeneralDataQualityRows() => _exportWarnings
        .OrderBy(pair => pair.Key, StringComparer.Ordinal)
        .SelectMany(pair => pair.Value.OrderBy(row => row.SourceToken, StringComparer.Ordinal)
            .ThenBy(row => int.TryParse(row.Fields[5], NumberStyles.None, CultureInfo.InvariantCulture, out int n) ? n : 0)
            .ThenBy(row => row.Fields[31], StringComparer.Ordinal).ThenBy(row => row.Fields[2], StringComparer.Ordinal)
            .ThenBy(row => row.Fields[22], StringComparer.Ordinal));

    private void RecordBaselineCalculationWarnings(XerTable baseline)
    {
        if (!_exportWarnings.TryGetValue(EnhancedTableNames.XerTask01, out var warnings)) return;
        // The source evidence handle follows row copying/reordering. Keep the
        // original diagnostic ordinal rather than re-numbering a filtered table.
        var retained = baseline.Rows.Select(row => (row.SourceToken, row.RawEvidenceIdentity)).ToHashSet();
        foreach (DataRow warning in warnings)
        {
            if (!retained.Contains((warning.SourceToken, warning.RawEvidenceIdentity))) continue;
            string[] fields = warning.Fields.ToArray();
            fields[3] = EnhancedTableNames.XerBaseline04;
            _exportWarnings.GetOrAdd(EnhancedTableNames.XerBaseline04, _ => new())
                .Enqueue(warning.WithFields(fields));
        }
    }

    // Unavailable dependencies/failed transformations must not erase imported rows.
    // This is a last-resort recovery only; normal transformations isolate individual
    // calculations and keep all independently computable fields.
    internal XerTable RecoverEnhancedTable(string name, Exception? failure = null)
    {
        XerTable result = StandardExportSchema.CreateEmpty(_dataStore, name);
        string sourceName = StandardExportSchema.SourceTable(name)!;
        var source = _dataStore.GetTable(sourceName);
        string code = source is null ? "SOURCE_TABLE_UNAVAILABLE" : "TABLE_GENERATION_FAILED";
        string message = failure?.Message ?? $"Source table '{sourceName}' is unavailable; no source records can be inferred.";
        if (source is { Headers: not null })
        {
            bool fixedTask = name == EnhancedTableNames.XerTask01;
            if (name is not (EnhancedTableNames.XerTask01 or EnhancedTableNames.XerBaseline04
                or EnhancedTableNames.XerCalendarDetailed11 or EnhancedTableNames.XerResourceDist15))
                result.SetHeaders(CreateEnhancedHeaders(source, name, StandardExportSchema.AddedFields(name)));
            int[] ordinals = SourceOrdinals(source);
            for (int i = 0; i < source.Rows.Count; i++)
            {
                var row = source.Rows[i];
                // Expanded calendar/distribution grains cannot invent a date or month.
                // Their complete raw source occurrence remains in the diagnostic.
                if (name is not (EnhancedTableNames.XerCalendarDetailed11 or EnhancedTableNames.XerResourceDist15
                    or EnhancedTableNames.XerBaseline04))
                {
                    string[] fields = Enumerable.Repeat("", result.Headers!.Length).ToArray();
                    if (fixedTask)
                    {
                        var derived = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                        {
                            FieldNames.Start, FieldNames.Finish, FieldNames.IdName, FieldNames.RemainingDuration,
                            FieldNames.OriginalDuration, FieldNames.TotalFloat, FieldNames.FreeFloat, FieldNames.PercentComplete,
                            FieldNames.DataDate, FieldNames.WbsIdKey, FieldNames.TaskIdKey, FieldNames.CalendarIdKey,
                            FieldNames.ProjIdKey, FieldNames.MonthUpdate
                        };
                        for (int fieldIndex = 0; fieldIndex < result.Headers.Length; fieldIndex++)
                            if (!derived.Contains(result.Headers[fieldIndex])
                                && source.FieldIndexes.TryGetValue(result.Headers[fieldIndex], out int rawIndex)
                                && rawIndex < row.Fields.Length) fields[fieldIndex] = row.Fields[rawIndex];
                    }
                    else Array.Copy(row.Fields, fields, Math.Min(source.Headers.Length, row.Fields.Length));
                    result.AddRow(row.WithFields(fields));
                }
                RecordDataQualityWarning(name, code, message + " Imported evidence is retained; unavailable calculations are not inferred.",
                    row, ordinals[i], sourceName, rawRowJson: XerDataQuality.RawRowJson(source, row));
            }
        }
        if (source is null || source.RowCount == 0)
        {
            // Attach a missing-table warning to each known input occurrence, never a
            // fabricated input token or a dictionary keyed by filename.
            foreach (var row in _dataStore.TableNames.SelectMany(table => _dataStore.GetTable(table)!.Rows)
                .GroupBy(row => row.SourceToken).Select(group => group.First()))
                RecordDataQualityWarning(name, code, message, row, 0, sourceName);
        }
        return result;
    }
}
