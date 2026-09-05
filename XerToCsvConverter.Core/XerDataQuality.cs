namespace XerToCsvConverter;

/// <summary>
/// Supplemental, assignment-grain diagnostics. Numbered table schemas are unchanged;
/// unresolved actual units belong here, never in an invented monthly bucket.
/// The normal CSV writer appends original FileName provenance.
/// </summary>
public static class XerDataQuality
{
    public const string TableName = "XER_DATA_QUALITY";
    public const string FileName = TableName + ".csv";
    public const string SchemaVersion = "1.0";
    public static readonly string[] Columns =
    [
        "diagnostic_schema_version", "severity", "issue_code", "table_name",
        "source_namespace", "source_row_number", "proj_id_key", "task_id_key",
        "rsrc_id_key", "taskrsrc_id_key", "taskrsrc_id", "task_code", "rsrc_name",
        "rsrc_type", "unit", "status_code", "act_start_date", "act_end_date",
        "project_data_date", "act_reg_qty", "act_ot_qty", "unallocated_actual_quantity", "message"
    ];

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
