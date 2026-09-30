using System.Globalization;
using System.Text;
using System.Text.Json;

namespace XerToCsvConverter;

/// <summary>
/// Supplemental source-row/cell diagnostics. Numbered table schemas are unchanged;
/// unresolved actual or remaining units belong here, never in an invented monthly bucket.
/// The normal CSV writer appends original FileName provenance.
/// </summary>
public static class XerDataQuality
{
    public const string TableName = "XER_DATA_QUALITY";
    public const string FileName = TableName + ".csv";
    public const string SchemaVersion = "1.2";
    public static readonly string[] Columns =
    [
        "diagnostic_schema_version", "severity", "issue_code", "table_name",
        "source_namespace", "source_row_number", "proj_id_key", "task_id_key",
        "rsrc_id_key", "taskrsrc_id_key", "taskrsrc_id", "task_code", "rsrc_name",
        "rsrc_type", "unit", "status_code", "act_start_date", "act_end_date",
        "project_data_date", "act_reg_qty", "act_ot_qty", "unallocated_actual_quantity", "message",
        "allocation_portion", "restart_date", "reend_date", "remain_qty", "curv_id", "remain_crv",
        "unallocated_remaining_quantity", "source_table", "column_name", "raw_value", "raw_row_json"
    ];

    public static DataRow CreateWarning(string tableName, string issueCode, string message,
        DataRow sourceRow, int sourceRowNumber, string sourceTable = "", string columnName = "",
        string rawValue = "", string rawRowJson = "")
    {
        var fields = Enumerable.Repeat("", Columns.Length).ToArray();
        fields[0] = SchemaVersion;
        fields[1] = "Warning";
        fields[2] = issueCode;
        fields[3] = tableName;
        fields[4] = sourceRow.SourceFilename;
        fields[5] = sourceRowNumber > 0 ? sourceRowNumber.ToString(CultureInfo.InvariantCulture) : "";
        fields[22] = message.Replace($" (input occurrence '{sourceRow.SourceToken}')", "", StringComparison.Ordinal)
            .Replace($" (occurrence '{sourceRow.SourceToken}')", "", StringComparison.Ordinal);
        fields[30] = sourceTable;
        fields[31] = columnName;
        fields[32] = rawValue;
        fields[33] = rawRowJson;
        return sourceRow.WithFields(fields);
    }

    /// <summary>Ordered source fields, including omitted cells; no internal occurrence token.</summary>
    public static string RawRowJson(XerTable table, DataRow row) => JsonSerializer.Serialize(
        (table.Headers ?? []).Select((name, index) => new RawRowField(name,
            index < row.Fields.Length ? row.Fields[index] : null,
            row.GetRawField(name).State.ToString())), CoreJsonContext.Default.RawRowFields);

    public static byte[] WriteToBytes(XerTable table, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(table);
        cancellationToken.ThrowIfCancellationRequested();
        if (table.Name != TableName || table.Headers is null || !table.Headers.SequenceEqual(Columns))
            throw new InvalidDataException("Invalid data-quality companion schema.");
        using var stream = new MemoryStream();
        new CsvExporter().WriteTableToStream(table, stream);
        cancellationToken.ThrowIfCancellationRequested();
        return stream.ToArray();
    }

    /// <summary>
    /// Plain-text review diagnostics in source order, including repeated occurrences. Bounds ordinary
    /// warnings but always includes Tender project mappings and unknown State warnings, even beyond the display limit.
    /// Only original filename, issue code and message are rendered; internal correlation tokens are not.
    /// </summary>
    public static IReadOnlyList<string> GetMessages(XerTable? table, int maximumMessages = 100)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maximumMessages);
        if (table is null
            || !table.FieldIndexes.TryGetValue("issue_code", out int codeIndex)
            || !table.FieldIndexes.TryGetValue("message", out int messageIndex))
            return Array.Empty<string>();

        var result = new List<string>();
        int omitted = 0;
        int ordinaryMessages = 0;
        foreach (DataRow row in table.Rows)
        {
            string code = codeIndex < row.Fields.Length ? row.Fields[codeIndex] : string.Empty;
            string message = messageIndex < row.Fields.Length ? row.Fields[messageIndex] : string.Empty;
            if (string.IsNullOrWhiteSpace(message)) continue;
            if (code is not ("TENDER_PROJECT_CODE_MAPPED" or "TENDER_PROJECT_STATE_UNKNOWN")
                && ordinaryMessages++ >= maximumMessages)
            {
                omitted++;
                continue;
            }

            string source = row.OriginalSourceFilename;
            // Filename and issue code are public provenance. Only redact the identity
            // in occurrence annotations or as a complete token in message evidence.
            if (!string.IsNullOrEmpty(row.SourceToken))
            {
                message = message.Replace($" (input occurrence '{row.SourceToken}')", "", StringComparison.Ordinal)
                    .Replace($" (occurrence '{row.SourceToken}')", "", StringComparison.Ordinal);
                message = RedactIdentityReferences(message, row.SourceToken, source);
            }
            result.Add($"{source}: [{code}] {message}");
        }
        if (omitted > 0)
            result.Add($"{omitted.ToString(CultureInfo.InvariantCulture)} additional source diagnostic(s) omitted from this display; all remain available on the Core result's DataQualityTable.");
        return result;

        static string RedactIdentityReferences(string message, string token, string originalFilename)
        {
            StringBuilder? redacted = null;
            int cursor = 0;
            int filenameStart = originalFilename.Length == 0 ? -1 : message.IndexOf(originalFilename, StringComparison.Ordinal);
            for (int index = message.IndexOf(token, StringComparison.Ordinal); index >= 0;
                index = message.IndexOf(token, index + token.Length, StringComparison.Ordinal))
            {
                while (filenameStart >= 0 && filenameStart + originalFilename.Length <= index)
                    filenameStart = message.IndexOf(originalFilename, filenameStart + originalFilename.Length, StringComparison.Ordinal);
                bool inFilename = filenameStart >= 0 && filenameStart <= index && index + token.Length <= filenameStart + originalFilename.Length;
                bool startsToken = index == 0 || !IsIdentityCharacter(message[index - 1]);
                bool endsToken = index + token.Length == message.Length || !IsIdentityCharacter(message[index + token.Length]);
                if (inFilename || !startsToken || !endsToken) continue;
                redacted ??= new StringBuilder(message.Length);
                redacted.Append(message, cursor, index - cursor).Append("[source]");
                cursor = index + token.Length;
            }
            return redacted is null ? message : redacted.Append(message, cursor, message.Length - cursor).ToString();

            static bool IsIdentityCharacter(char value) => char.IsLetterOrDigit(value) || value is '_' or '-';
        }
    }
}

public sealed record StandardMemoryExportResult(Dictionary<string, byte[]> Files, int WarningCount);
public sealed record StandardDiskExportResult(List<string> Files, int WarningCount);
