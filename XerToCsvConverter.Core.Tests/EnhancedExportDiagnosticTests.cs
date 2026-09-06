using System.Text;

namespace XerToCsvConverter.Core.Tests;

public sealed class EnhancedExportDiagnosticTests
{
    private const string OriginalFilename = "2607-repeat.xer";
    private const string PublicNamespace = "2607-repeat.xer#source-000002";
    private const string SourceToken = "diagnostic-batch:source-000002";
    private const string AuditTable = "00_AUDIT_SOURCE";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Calendar_warning_reports_filename_calendar_and_reason_while_publishing_valid_tables(bool toDisk)
    {
        using var output = new TemporaryOutput(EnhancedTableNames.XerCalendarDetailed11);
        XerDataStore store = Store(P6TestCalendars.WorkWeek().Replace("f|17:00", "f|25:00", StringComparison.Ordinal));

        Dictionary<string, byte[]> files = await WarnedExport(toDisk, store,
            EnhancedTableNames.XerCalendarDetailed11, output);
        string diagnostics = Encoding.UTF8.GetString(files[XerDataQuality.TableName]);
        Assert.Contains(OriginalFilename, diagnostics, StringComparison.Ordinal);
        Assert.Contains("CALENDAR 'C1'", diagnostics, StringComparison.Ordinal);
        Assert.Contains("Invalid calendar clock '25:00'", diagnostics, StringComparison.Ordinal);
        Assert.Contains("CALENDAR_WEEKDAY_INVALID", diagnostics, StringComparison.Ordinal);
        Assert.DoesNotContain(SourceToken, diagnostics, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Resource_warning_reports_assignment_and_calendar_reason_and_publishes_all_requested_tables(bool toDisk)
    {
        using var output = new TemporaryOutput(EnhancedTableNames.XerResourceDist15);
        XerDataStore store = Store(P6TestCalendars.WorkWeek());
        AddAssignments(store);

        var service = new ProcessingService();
        List<string> requested = [AuditTable, EnhancedTableNames.XerResourceDist15];
        Dictionary<string, byte[]> files;
        if (toDisk)
        {
            var result = await service.ExportTablesWithDiagnosticsAsync(store, requested, output.Path, null, CancellationToken.None);
            Assert.Equal(1, result.WarningCount);
            files = result.Files.ToDictionary(path => Path.GetFileNameWithoutExtension(path), File.ReadAllBytes);
            Assert.Equal("unrelated user file", File.ReadAllText(Path.Combine(output.Path, "unrelated.txt")));
        }
        else
        {
            var result = await service.ExportTablesToMemoryWithDiagnosticsAsync(store, requested, null, CancellationToken.None);
            Assert.Equal(1, result.WarningCount);
            files = result.Files.ToDictionary(pair => pair.Key, pair => pair.Value);
            output.AssertUnchanged();
        }
        Assert.Equal(3, files.Count);
        Assert.Contains("new export content", Encoding.UTF8.GetString(files[AuditTable]), StringComparison.Ordinal);
        Assert.Single(Encoding.UTF8.GetString(files[EnhancedTableNames.XerResourceDist15]).Trim().Split('\n'));
        string diagnostics = Encoding.UTF8.GetString(files[XerDataQuality.TableName]);
        Assert.Contains(OriginalFilename, diagnostics, StringComparison.Ordinal);
        Assert.Contains("REMAINING_NO_WORKING_TIME", diagnostics, StringComparison.Ordinal);
        Assert.Contains("A1", diagnostics, StringComparison.Ordinal);
        Assert.Contains("C1", diagnostics, StringComparison.Ordinal);
        Assert.Contains("no working time", diagnostics, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("8.0000", diagnostics, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Wbs_duplicates_preserve_both_source_rows_and_publish_warning_details(bool toDisk)
    {
        using var output = new TemporaryOutput(EnhancedTableNames.XerProjWbs03);
        XerDataStore store = Store(P6TestCalendars.WorkWeek());
        AddTable(store, "PROJWBS", ["wbs_id", "proj_id", "parent_wbs_id"],
            ["W1", "P1", ""], ["W1", "P1", ""]);

        Dictionary<string, byte[]> files = await WarnedExport(toDisk, store,
            EnhancedTableNames.XerProjWbs03, output);
        string diagnostics = Encoding.UTF8.GetString(files[XerDataQuality.TableName]);
        Assert.Contains(OriginalFilename, diagnostics, StringComparison.Ordinal);
        Assert.Contains("PROJWBS.wbs_id 'W1' is duplicated", diagnostics, StringComparison.Ordinal);
        Assert.Contains("WBS_IDENTITY_INVALID", diagnostics, StringComparison.Ordinal);
        Assert.Equal(3, Encoding.UTF8.GetString(files[EnhancedTableNames.XerProjWbs03]).Trim().Split('\n').Length);
    }

    [Fact]
    public void Public_transformer_preserves_unknown_rows_and_clears_stale_warnings_on_retry()
    {
        XerDataStore store = Store("malformed");
        var transformer = new XerTransformer(store);
        Assert.NotNull(transformer.Create11XerCalendarDetailed());
        Assert.NotEmpty(transformer.CreateDataQualityTable().Rows);
        Assert.Null(transformer.GetGenerationFailure(EnhancedTableNames.XerCalendarDetailed11));
        Assert.Null(transformer.GetGenerationFailure(EnhancedTableNames.XerResourceDist15));

        AddCalendar(store, P6TestCalendars.WorkWeek());

        Assert.NotNull(transformer.Create11XerCalendarDetailed());
        Assert.Null(transformer.GetGenerationFailure(EnhancedTableNames.XerCalendarDetailed11));
        Assert.Empty(transformer.CreateDataQualityTable().Rows);
    }

    private static async Task<Dictionary<string, byte[]>> WarnedExport(bool toDisk, XerDataStore store,
        string tableName, TemporaryOutput output)
    {
        var service = new ProcessingService();
        var progress = new CapturedProgress();
        var requested = new List<string> { AuditTable, tableName };
        Dictionary<string, byte[]> files;
        if (toDisk)
        {
            var result = await service.ExportTablesWithDiagnosticsAsync(store, requested, output.Path, progress, CancellationToken.None);
            Assert.True(result.WarningCount > 0);
            files = result.Files.ToDictionary(path => Path.GetFileNameWithoutExtension(path), File.ReadAllBytes);
            Assert.Equal("unrelated user file", File.ReadAllText(Path.Combine(output.Path, "unrelated.txt")));
        }
        else
        {
            var result = await service.ExportTablesToMemoryWithDiagnosticsAsync(store, requested, progress, CancellationToken.None);
            Assert.True(result.WarningCount > 0);
            files = result.Files;
            output.AssertUnchanged();
        }
        Assert.Equal(3, files.Count);
        Assert.Contains("new export content", Encoding.UTF8.GetString(files[AuditTable]), StringComparison.Ordinal);
        return files;
    }

    private static XerDataStore Store(string calendarData)
    {
        var store = new XerDataStore();
        AddTable(store, AuditTable, ["id"], ["new export content"]);
        AddCalendar(store, calendarData);
        return store;
    }

    private static void AddCalendar(XerDataStore store, string calendarData) =>
        AddTable(store, "CALENDAR", ["clndr_id", "clndr_name", "clndr_type", "day_hr_cnt", "clndr_data"],
            ["C1", "Calendar", "CA_Project", "8", calendarData]);

    private static void AddAssignments(XerDataStore store)
    {
        AddTable(store, "PROJECT", ["proj_id", "clndr_id", "last_recalc_date"],
            ["P1", "C1", "2026-08-31 08:00"]);
        AddTable(store, "TASK", ["task_id", "proj_id", "clndr_id", "status_code", "task_type"],
            ["T1", "P1", "C1", "TK_NotStart", "TT_Task"]);
        AddTable(store, "RSRC", ["rsrc_id", "clndr_id", "rsrc_type"], ["R1", "C1", "RT_Labor"]);
        // A remaining allocation entirely on Sunday is still invalid. Recorded
        // actual work is a different contract and must not relax this safeguard.
        AddTable(store, "TASKRSRC", ["taskrsrc_id", "task_id", "rsrc_id", "proj_id", "remain_qty", "restart_date", "reend_date"],
            ["A1", "T1", "R1", "P1", "8", "2026-08-30 08:00", "2026-08-30 17:00"]);
    }

    private static void AddTable(XerDataStore store, string name, string[] headers, params string[][] rows)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        foreach (string[] row in rows)
            table.AddRow(new DataRow(row, PublicNamespace, SourceToken, OriginalFilename));
        store.AddTable(table);
    }

    private sealed class CapturedProgress : IProgress<(int percent, string message)>
    {
        public List<string> Messages { get; } = [];
        public void Report((int percent, string message) value) => Messages.Add(value.message);
    }

    private sealed class TemporaryOutput : IDisposable
    {
        private readonly DirectoryInfo _directory = Directory.CreateTempSubdirectory("xer-export-diagnostic-");
        private readonly Dictionary<string, byte[]> _before;
        public string Path => _directory.FullName;

        public TemporaryOutput(string tableName)
        {
            _before = new(StringComparer.Ordinal)
            {
                [$"{AuditTable}.csv"] = Encoding.UTF8.GetBytes("prior audit output"),
                [$"{tableName}.csv"] = Encoding.UTF8.GetBytes("prior generated output"),
                ["unrelated.txt"] = Encoding.UTF8.GetBytes("unrelated user file")
            };
            foreach (var pair in _before) File.WriteAllBytes(System.IO.Path.Combine(Path, pair.Key), pair.Value);
        }

        public void AssertUnchanged()
        {
            Assert.Equal(_before.Keys.OrderBy(name => name, StringComparer.Ordinal),
                Directory.EnumerateFileSystemEntries(Path).Select(System.IO.Path.GetFileName)
                    .OrderBy(name => name, StringComparer.Ordinal));
            foreach (var pair in _before)
                Assert.Equal(pair.Value, File.ReadAllBytes(System.IO.Path.Combine(Path, pair.Key)));
        }

        public void Dispose() => _directory.Delete(true);
    }
}
