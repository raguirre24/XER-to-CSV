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
    public async Task Calendar_failure_reports_filename_calendar_and_original_reason_without_publication(bool toDisk)
    {
        using var output = new TemporaryOutput(EnhancedTableNames.XerCalendarDetailed11);
        XerDataStore store = Store(P6TestCalendars.WorkWeek().Replace("f|17:00", "f|25:00", StringComparison.Ordinal));

        InvalidDataException error = await FailedExport(toDisk, store,
            EnhancedTableNames.XerCalendarDetailed11, output);

        Assert.Contains(OriginalFilename, error.Message, StringComparison.Ordinal);
        Assert.Contains("CALENDAR 'C1'", error.Message, StringComparison.Ordinal);
        Assert.Contains("Invalid calendar clock '25:00'", error.Message, StringComparison.Ordinal);
        Assert.IsType<InvalidDataException>(error.InnerException);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Resource_failure_reports_assignment_and_calendar_reason_without_publication(bool toDisk)
    {
        using var output = new TemporaryOutput(EnhancedTableNames.XerResourceDist15);
        XerDataStore store = Store(P6TestCalendars.WorkWeek());
        AddAssignments(store);

        InvalidDataException error = await FailedExport(toDisk, store,
            EnhancedTableNames.XerResourceDist15, output);

        Assert.Contains(OriginalFilename, error.Message, StringComparison.Ordinal);
        Assert.Contains("TASKRSRC 'A1' (task 'T1', resource 'R1')", error.Message, StringComparison.Ordinal);
        Assert.Contains("C1", error.Message, StringComparison.Ordinal);
        Assert.Contains("no working time", error.Message, StringComparison.Ordinal);
        Assert.IsType<InvalidDataException>(error.InnerException);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Other_caught_generated_table_failures_preserve_their_original_details(bool toDisk)
    {
        using var output = new TemporaryOutput(EnhancedTableNames.XerProjWbs03);
        XerDataStore store = Store(P6TestCalendars.WorkWeek());
        AddTable(store, "PROJWBS", ["wbs_id", "proj_id", "parent_wbs_id"],
            ["W1", "P1", ""], ["W1", "P1", ""]);

        InvalidDataException error = await FailedExport(toDisk, store,
            EnhancedTableNames.XerProjWbs03, output);

        Assert.Contains(OriginalFilename, error.Message, StringComparison.Ordinal);
        Assert.Contains("duplicate PROJWBS.wbs_id 'W1'", error.Message, StringComparison.Ordinal);
        Assert.IsType<InvalidDataException>(error.InnerException);
    }

    [Fact]
    public void Public_transformer_retains_null_contract_and_clears_stale_diagnostics_on_retry()
    {
        XerDataStore store = Store("malformed");
        var transformer = new XerTransformer(store);
        Assert.Null(transformer.Create11XerCalendarDetailed());
        Assert.NotNull(transformer.GetGenerationFailure(EnhancedTableNames.XerCalendarDetailed11));
        Assert.Null(transformer.GetGenerationFailure(EnhancedTableNames.XerResourceDist15));

        AddCalendar(store, P6TestCalendars.WorkWeek());

        Assert.NotNull(transformer.Create11XerCalendarDetailed());
        Assert.Null(transformer.GetGenerationFailure(EnhancedTableNames.XerCalendarDetailed11));
    }

    private static async Task<InvalidDataException> FailedExport(bool toDisk, XerDataStore store,
        string tableName, TemporaryOutput output)
    {
        var service = new ProcessingService();
        var progress = new CapturedProgress();
        var requested = new List<string> { AuditTable, tableName };
        InvalidDataException error = toDisk
            ? await Assert.ThrowsAsync<InvalidDataException>(() => service.ExportTablesAsync(
                store, requested, output.Path, progress, CancellationToken.None))
            : await Assert.ThrowsAsync<InvalidDataException>(() => service.ExportTablesToMemoryAsync(
                store, requested, progress, CancellationToken.None));
        Assert.Contains($"Cannot export '{tableName}'", error.Message, StringComparison.Ordinal);
        Assert.Contains("No CSV files have been published by this export.", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("generation failed or no valid output schema", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(progress.Messages, message => message.StartsWith("Exported:", StringComparison.Ordinal));
        output.AssertUnchanged();
        return error;
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
