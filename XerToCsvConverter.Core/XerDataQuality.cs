using System.Globalization;
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
        (table.Headers ?? []).Select((name, index) => new
        {
            column = name,
            value = index < row.Fields.Length ? row.Fields[index] : null,
            presence = row.GetRawField(name).State.ToString()
        }));

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
}

public sealed record StandardMemoryExportResult(Dictionary<string, byte[]> Files, int WarningCount);
public sealed record StandardDiskExportResult(List<string> Files, int WarningCount);
