using System.Text;

namespace XerToCsvConverter.Core.Tests;

public sealed class CalendarExportValidationTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("malformed")]
    public async Task Missing_or_malformed_requested_11_preserves_memory_export(string? calendarData)
    {
        var result = await new ProcessingService().ExportTablesToMemoryWithDiagnosticsAsync(Store(calendarData),
            new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerCalendarDetailed11 }, null, CancellationToken.None);
        Assert.Equal(3, result.Files.Count);
        Assert.True(result.WarningCount > 0);
        Assert.Contains("preserved", Encoding.UTF8.GetString(result.Files["AUDIT_SOURCE"]));
        Assert.Contains("work_hours", Lines(result.Files[EnhancedTableNames.XerCalendarDetailed11])[0]);
        Assert.Contains(calendarData ?? "SOURCE_TABLE_UNAVAILABLE", Encoding.UTF8.GetString(result.Files[XerDataQuality.TableName]));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("malformed")]
    public async Task Missing_or_malformed_requested_11_preserves_disk_export(string? calendarData)
    {
        using var output = new TemporaryOutput();
        var result = await new ProcessingService().ExportTablesWithDiagnosticsAsync(Store(calendarData),
            new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerCalendarDetailed11 },
            output.Path, null, CancellationToken.None);
        Assert.Equal(3, result.Files.Count);
        Assert.True(result.WarningCount > 0);
        Assert.Equal(3, Directory.EnumerateFiles(output.Path).Count());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Valid_requested_11_exports_explicit_week_availability(bool toDisk)
    {
        using var output = new TemporaryOutput();
        var bytes = await Export(toDisk, Store(P6TestCalendars.WorkWeek()),
            new List<string> { EnhancedTableNames.XerCalendarDetailed11 }, output.Path);

        Assert.Equal(2, bytes.Count);
        byte[] csv = bytes[EnhancedTableNames.XerCalendarDetailed11];
        string[] lines = Lines(csv);
        Assert.Equal(8, lines.Length); // Header plus seven explicitly defined weekdays.
        Assert.Contains("work_hours", lines[0], StringComparison.Ordinal);
        Assert.Contains(lines.Skip(1), line => line.Contains(",Monday,Y,8,Standard,", StringComparison.Ordinal));
        Assert.Contains(lines.Skip(1), line => line.Contains(",Sunday,N,0,Standard,", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Requesting_only_06_does_not_require_successful_11_generation(bool toDisk)
    {
        using var output = new TemporaryOutput();
        XerDataStore store = Store(P6TestCalendars.WorkWeek());
        store.GetTable("CALENDAR")!.AddRow(new DataRow(
            new[] { "UNUSED", "Malformed unused calendar", "CA_Project", "8", "malformed" }, "source"));
        AddRelationships(store);
        Assert.NotNull(new XerTransformer(store).Create11XerCalendarDetailed());

        var bytes = await Export(toDisk, store,
            new List<string> { EnhancedTableNames.XerPredecessor06 }, output.Path);

        Assert.Equal(2, bytes.Count);
        KeyValuePair<string, byte[]> exported = bytes.Single(pair => pair.Key == EnhancedTableNames.XerPredecessor06);
        Assert.Equal(EnhancedTableNames.XerPredecessor06, exported.Key);
        string[] lines = Lines(exported.Value);
        Assert.Equal(2, lines.Length);
        string[] headers = lines[0].Split(',');
        string[] row = lines[1].Split(',');
        int floatIndex = Array.IndexOf(headers, "free_float");
        Assert.True(floatIndex >= 0);
        Assert.Equal("1", row[floatIndex]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Colliding_raw_06_column_is_preserved_without_blocking_other_tables(bool toDisk)
    {
        using var output = new TemporaryOutput();
        XerDataStore store = Store(P6TestCalendars.WorkWeek());
        AddRelationships(store);
        XerTable original = store.GetTable("TASKPRED")!;
        var conflicting = new XerTable("TASKPRED");
        conflicting.SetHeaders(original.Headers!.Append("free_float").ToArray());
        conflicting.AddRows(original.Rows.Select(row => row.WithFields(row.Fields.Append("99").ToArray())));
        store.AddTable(conflicting);
        var files = await Export(toDisk, store, new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerPredecessor06 }, output.Path);
        Assert.Equal(3, files.Count);
        string[] lines = Lines(files[EnhancedTableNames.XerPredecessor06]);
        string[] headers = lines[0].Split(','), values = lines[1].Split(',');
        Assert.Equal("99", values[Array.IndexOf(headers, "raw_free_float")]);
        Assert.Equal("1", values[Array.IndexOf(headers, "free_float")]);
        Assert.Contains("SOURCE_COLUMN_COLLISION", Encoding.UTF8.GetString(files[XerDataQuality.TableName]));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Explicit_requested_06_without_relationship_source_is_header_only_and_warned(bool toDisk)
    {
        using var output = new TemporaryOutput();
        var files = await Export(toDisk, Store(P6TestCalendars.WorkWeek()),
            new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerPredecessor06 }, output.Path);
        Assert.Equal(3, files.Count);
        Assert.Single(Lines(files[EnhancedTableNames.XerPredecessor06]));
        Assert.Contains("SOURCE_TABLE_UNAVAILABLE", Encoding.UTF8.GetString(files[XerDataQuality.TableName]));
        Assert.Contains("TASKPRED", Encoding.UTF8.GetString(files[XerDataQuality.TableName]));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Invalid_assignment_calendar_preserves_requested_exports_and_unallocated_quantity(bool toDisk)
    {
        using var output = new TemporaryOutput();
        XerDataStore store = Store("malformed");
        AddAssignments(store);
        AddTable(store, "RSRC", new[] { "rsrc_id", "rsrc_name", "rsrc_type" }, new[] { "R1", "Labour", "RT_Labor" });

        Dictionary<string, byte[]> files = await Export(toDisk, store,
            new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerResourceDist15 }, output.Path);

        Assert.Equal(3, files.Count);
        Assert.Contains("preserved", Encoding.UTF8.GetString(files["AUDIT_SOURCE"]), StringComparison.Ordinal);
        Assert.Single(Lines(files[EnhancedTableNames.XerResourceDist15]));
        using var stream = new MemoryStream(files[XerDataQuality.TableName], writable: false);
        using var parser = new Microsoft.VisualBasic.FileIO.TextFieldParser(stream, Encoding.UTF8, detectEncoding: false)
        { TextFieldType = Microsoft.VisualBasic.FileIO.FieldType.Delimited, HasFieldsEnclosedInQuotes = true, TrimWhiteSpace = false };
        parser.SetDelimiters(",");
        string[] headers = parser.ReadFields()!;
        Dictionary<string, string> warning = headers.Zip(parser.ReadFields()!)
            .ToDictionary(pair => pair.First, pair => pair.Second);
        Assert.True(parser.EndOfData);
        Assert.Equal(XerDataQuality.Columns.Append("FileName"), headers);
        Assert.Equal("RESOURCE_CALENDAR_INVALID", warning["issue_code"]);
        Assert.Equal("Remaining", warning["allocation_portion"]);
        Assert.Equal("A1", warning["taskrsrc_id"]);
        Assert.Equal("8.0000", warning["unallocated_remaining_quantity"]);
        Assert.Equal("", warning["unallocated_actual_quantity"]);
        Assert.Equal("malformed", store.GetTable("CALENDAR")!.Rows[0].Fields[store.GetTable("CALENDAR")!.FieldIndexes["clndr_data"]]);
        Assert.Equal(toDisk ? 3 : 0, Directory.EnumerateFiles(output.Path).Count());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Explicit_requested_15_without_assignment_source_is_header_only_and_warned(bool toDisk)
    {
        using var output = new TemporaryOutput();
        var files = await Export(toDisk, Store(P6TestCalendars.WorkWeek()),
            new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerResourceDist15 }, output.Path);
        Assert.Equal(3, files.Count);
        Assert.Single(Lines(files[EnhancedTableNames.XerResourceDist15]));
        Assert.Contains("TASKRSRC", Encoding.UTF8.GetString(files[XerDataQuality.TableName]));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Unrequested_absent_relationships_or_assignments_do_not_block_valid_project_export(bool toDisk)
    {
        using var output = new TemporaryOutput();
        XerDataStore store = Store(P6TestCalendars.WorkWeek());
        AddTasksAndProject(store);
        var bytes = await Export(toDisk, store, new List<string> { EnhancedTableNames.XerProject02 }, output.Path);
        Assert.Equal(2, bytes.Count);
        Assert.Contains(EnhancedTableNames.XerProject02, bytes.Keys);
    }

    private static async Task<Dictionary<string, byte[]>> Export(bool toDisk, XerDataStore store,
        List<string> tables, string outputPath)
    {
        var service = new ProcessingService();
        if (!toDisk)
            return await service.ExportTablesToMemoryAsync(store, tables, null, CancellationToken.None);
        List<string> paths = await service.ExportTablesAsync(store, tables, outputPath, null, CancellationToken.None);
        return paths.ToDictionary(path => System.IO.Path.GetFileNameWithoutExtension(path),
            File.ReadAllBytes, StringComparer.Ordinal);
    }

    private static XerDataStore Store(string? calendarData)
    {
        var store = new XerDataStore();
        AddTable(store, "AUDIT_SOURCE", new[] { "id" }, new[] { "preserved" });
        if (calendarData is not null)
            AddTable(store, "CALENDAR", new[] { "clndr_id", "clndr_name", "clndr_type", "day_hr_cnt", "clndr_data" },
                new[] { "C1", "Calendar", "CA_Project", "8", calendarData });
        return store;
    }

    private static void AddTasksAndProject(XerDataStore store)
    {
        AddTable(store, "PROJECT", new[] { "proj_id", "clndr_id", "last_recalc_date" },
            new[] { "P1", "C1", "2026-08-31 08:00" });
        AddTable(store, "TASK", new[] { "task_id", "proj_id", "clndr_id", "status_code", "task_type", "task_code",
                "early_start_date", "early_end_date", "restart_date", "reend_date" },
            new[] { "T1", "P1", "C1", "TK_NotStart", "TT_Task", "A1", "2026-08-31 08:00", "2026-08-31 17:00",
                "2026-08-31 08:00", "2026-08-31 17:00" },
            new[] { "T2", "P1", "C1", "TK_NotStart", "TT_Task", "A2", "2026-09-02 08:00", "2026-09-02 17:00",
                "2026-09-02 08:00", "2026-09-02 17:00" });
    }

    private static void AddRelationships(XerDataStore store)
    {
        AddTasksAndProject(store);
        AddTable(store, "TASKPRED", new[] { "task_pred_id", "task_id", "pred_task_id", "pred_type", "lag_hr_cnt", "proj_id" },
            new[] { "R1", "T2", "T1", "PR_FS", "0", "P1" });
    }

    private static void AddAssignments(XerDataStore store)
    {
        AddTasksAndProject(store);
        AddTable(store, "TASKRSRC", new[] { "taskrsrc_id", "task_id", "rsrc_id", "proj_id", "remain_qty", "restart_date", "reend_date" },
            new[] { "A1", "T1", "R1", "P1", "8", "2026-08-31 08:00", "2026-08-31 17:00" });
    }

    private static void AddTable(XerDataStore store, string name, string[] headers, params string[][] rows)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        foreach (string[] row in rows) table.AddRow(new DataRow(row, "source"));
        store.AddTable(table);
    }

    private static string[] Lines(byte[] bytes) => Encoding.UTF8.GetString(bytes).TrimStart('\uFEFF')
        .Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);

    private sealed class TemporaryOutput : IDisposable
    {
        private readonly DirectoryInfo _directory = Directory.CreateTempSubdirectory("xer-calendar-export-");
        public string Path => _directory.FullName;
        public void Dispose() => _directory.Delete(true);
    }
}
