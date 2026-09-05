using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ReviewExportDiagnosticTests
{
    private const string OriginalFilename = "review diagnostic.xer";

    [Theory]
    [InlineData("programme", false, false)]
    [InlineData("programme", true, false)]
    [InlineData("tender", false, false)]
    [InlineData("tender", true, false)]
    [InlineData("programme", false, true)]
    [InlineData("programme", true, true)]
    [InlineData("tender", false, true)]
    [InlineData("tender", true, true)]
    public async Task Failed_15_preserves_calendar_or_assignment_details_and_publishes_no_bundle(
        string profile, bool toDisk, bool malformedCalendar)
    {
        using var output = new TemporaryOutput();
        Exception error;
        if (profile == "programme")
        {
            var baseline = ProgrammeReviewNamingTests.Snapshot(OriginalFilename,
                ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-08-31", "2026-08-31");
            var request = ProgrammeReviewNamingTests.Request([baseline]);
            var store = Store(baseline.OriginalXerFilename, "J123", malformedCalendar);
            var service = new ProgrammeReviewBundleService();
            error = toDisk
                ? await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
                    service.BuildFromParsedDataAsync(store, request, output.Path))
                : await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
                    service.BuildFromParsedDataToMemoryAsync(store, request));
        }
        else
        {
            var source = TenderReviewNamingTests.Source(0, OriginalFilename, "2026-09-06");
            var request = TenderReviewNamingTests.Request([source]);
            var store = Store(source.SourceToken, "J5001", malformedCalendar);
            var service = new TenderReviewBundleService();
            error = toDisk
                ? await Assert.ThrowsAsync<TenderReviewValidationException>(() =>
                    service.BuildFromParsedDataAsync(store, request, output.Path))
                : await Assert.ThrowsAsync<TenderReviewValidationException>(() =>
                    service.BuildFromParsedDataToMemoryAsync(store, request));
        }

        Assert.Contains(EnhancedTableNames.XerResourceDist15, error.Message, StringComparison.Ordinal);
        Assert.Contains(OriginalFilename, error.Message, StringComparison.Ordinal);
        Assert.Contains("TASKRSRC 'A1' (task 'T1', resource 'R1')", error.Message, StringComparison.Ordinal);
        Assert.Contains("C1", error.Message, StringComparison.Ordinal);
        Assert.Contains(malformedCalendar ? "Invalid calendar clock '25:00'" : "no working time",
            error.Message, StringComparison.Ordinal);
        Assert.IsType<InvalidDataException>(error.InnerException);
        output.AssertUnchanged();
    }

    private static XerDataStore Store(string publicSource, string projectCode, bool malformedCalendar)
    {
        var store = new XerDataStore();
        Add("PROJECT", ["proj_id", "clndr_id", "last_recalc_date", "proj_short_name"],
            ["P1", "C1", "2026-08-31 08:00", projectCode]);
        Add("PROJWBS", ["wbs_id", "proj_id", "parent_wbs_id", "wbs_name"], ["W1", "P1", "", "Root"]);
        Add("TASK", ["task_id", "proj_id", "wbs_id", "clndr_id", "status_code", "task_type", "task_code",
            "restart_date", "reend_date", "early_start_date", "early_end_date", "remain_drtn_hr_cnt"],
            ["T1", "P1", "W1", "C1", "TK_NotStart", "TT_Task", "A100", "2026-08-30 08:00", "2026-08-30 17:00",
                "2026-08-30 08:00", "2026-08-30 17:00", "8"]);
        string calendarData = P6TestCalendars.WorkWeek();
        if (malformedCalendar)
            calendarData = calendarData.Replace("f|17:00", "f|25:00", StringComparison.Ordinal);
        Add("CALENDAR", ["clndr_id", "clndr_name", "clndr_type", "day_hr_cnt", "clndr_data"],
            ["C1", "Calendar", "CA_Project", "8", calendarData]);
        Add("RSRC", ["rsrc_id", "clndr_id", "rsrc_type", "def_qty_per_hr"], ["R1", "C1", "RT_Labor", "1"]);
        Add("TASKRSRC", ["taskrsrc_id", "task_id", "rsrc_id", "proj_id", "remain_qty", "restart_date", "reend_date"],
            ["A1", "T1", "R1", "P1", "8", "2026-08-30 08:00", "2026-08-30 17:00"]);
        return store;

        void Add(string name, string[] headers, string[] fields)
        {
            var table = new XerTable(name);
            table.SetHeaders(headers);
            table.AddRow(new DataRow(fields, publicSource, "review-diagnostic-occurrence", OriginalFilename));
            store.AddTable(table);
        }
    }

    private sealed class TemporaryOutput : IDisposable
    {
        private readonly DirectoryInfo _directory = Directory.CreateTempSubdirectory("xer-review-diagnostic-");
        private readonly string _unrelatedPath;
        public string Path => _directory.FullName;

        public TemporaryOutput()
        {
            _unrelatedPath = System.IO.Path.Combine(Path, "unrelated.txt");
            File.WriteAllText(_unrelatedPath, "preserve me");
        }

        public void AssertUnchanged()
        {
            Assert.Equal(_unrelatedPath, Assert.Single(Directory.EnumerateFileSystemEntries(Path)));
            Assert.Equal("preserve me", File.ReadAllText(_unrelatedPath));
        }

        public void Dispose() => _directory.Delete(true);
    }
}
