using System.Text;

namespace XerToCsvConverter.Core.Tests;

public sealed class CalendarExportValidationTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("malformed")]
    public async Task Missing_or_malformed_requested_11_fails_memory_export(string? calendarData)
    {
        var service = new ProcessingService();
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(() =>
            service.ExportTablesToMemoryAsync(Store(calendarData),
                new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerCalendarDetailed11 },
                null, CancellationToken.None));

        Assert.Contains(EnhancedTableNames.XerCalendarDetailed11, error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("malformed")]
    public async Task Missing_or_malformed_requested_11_fails_before_any_disk_CSV_is_written(string? calendarData)
    {
        using var output = new TemporaryOutput();
        var service = new ProcessingService();
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(() =>
            service.ExportTablesAsync(Store(calendarData),
                new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerCalendarDetailed11 },
                output.Path, null, CancellationToken.None));

        Assert.Contains(EnhancedTableNames.XerCalendarDetailed11, error.Message, StringComparison.Ordinal);
        Assert.Empty(Directory.EnumerateFileSystemEntries(output.Path));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Valid_requested_11_exports_explicit_week_availability(bool toDisk)
    {
        using var output = new TemporaryOutput();
        var bytes = await Export(toDisk, Store(P6TestCalendars.WorkWeek()),
            new List<string> { EnhancedTableNames.XerCalendarDetailed11 }, output.Path);

        byte[] csv = Assert.Single(bytes).Value;
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
        Assert.Null(new XerTransformer(store).Create11XerCalendarDetailed());

        var bytes = await Export(toDisk, store,
            new List<string> { EnhancedTableNames.XerPredecessor06 }, output.Path);

        KeyValuePair<string, byte[]> exported = Assert.Single(bytes);
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
    public async Task Failed_requested_06_with_relationships_rejects_export_before_any_CSV_is_written(bool toDisk)
    {
        using var output = new TemporaryOutput();
        XerDataStore store = Store(P6TestCalendars.WorkWeek());
        AddRelationships(store);
        XerTable original = store.GetTable("TASKPRED")!;
        var conflicting = new XerTable("TASKPRED");
        // A raw field colliding with an added field cannot form a valid enhanced
        // schema. Previously this table disappeared and unrelated CSVs still wrote.
        conflicting.SetHeaders(original.Headers!.Append("free_float").ToArray());
        conflicting.AddRows(original.Rows.Select(row => new DataRow(
            row.Fields.Append("0").ToArray(), row.SourceFilename)));
        store.AddTable(conflicting);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(() =>
            Export(toDisk, store, new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerPredecessor06 }, output.Path));

        Assert.Contains(EnhancedTableNames.XerPredecessor06, error.Message, StringComparison.Ordinal);
        Assert.Empty(Directory.EnumerateFileSystemEntries(output.Path));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Explicit_requested_06_without_relationship_source_fails_the_complete_export(bool toDisk)
    {
        using var output = new TemporaryOutput();
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(() =>
            Export(toDisk, Store(P6TestCalendars.WorkWeek()),
                new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerPredecessor06 }, output.Path));
        Assert.Contains("TASKPRED", error.Message);
        Assert.Empty(Directory.EnumerateFileSystemEntries(output.Path));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Failed_15_with_assignments_rejects_export_and_preserves_empty_disk_output(bool toDisk)
    {
        using var output = new TemporaryOutput();
        XerDataStore store = Store("malformed");
        AddAssignments(store);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(() =>
            Export(toDisk, store, new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerResourceDist15 }, output.Path));

        Assert.Contains(EnhancedTableNames.XerResourceDist15, error.Message, StringComparison.Ordinal);
        Assert.Empty(Directory.EnumerateFileSystemEntries(output.Path));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Explicit_requested_15_without_assignment_source_fails_the_complete_export(bool toDisk)
    {
        using var output = new TemporaryOutput();
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(() =>
            Export(toDisk, Store(P6TestCalendars.WorkWeek()),
                new List<string> { "AUDIT_SOURCE", EnhancedTableNames.XerResourceDist15 }, output.Path));
        Assert.Contains("TASKRSRC", error.Message);
        Assert.Empty(Directory.EnumerateFileSystemEntries(output.Path));
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
        Assert.Equal(EnhancedTableNames.XerProject02, Assert.Single(bytes).Key);
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
