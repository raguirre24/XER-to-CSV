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
    public async Task Resource_calendar_or_period_warning_does_not_abort_a_complete_review_bundle(
        string profile, bool toDisk, bool malformedCalendar)
    {
        using var output = new TemporaryOutput();
        // The existing allocation warning remains one; each of the five malformed
        // weekday definitions now also has an independently reported table11 issue.
        int expectedWarnings = malformedCalendar ? 6 : 1;
        IReadOnlyDictionary<string, byte[]> files;
        XerTable? qualityTable;
        if (profile == "programme")
        {
            var baseline = ProgrammeReviewNamingTests.Snapshot(OriginalFilename,
                ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-08-31", "2026-08-31");
            var request = ProgrammeReviewNamingTests.Request([baseline]);
            var store = Store(baseline.OriginalXerFilename, "J123", malformedCalendar);
            var service = new ProgrammeReviewBundleService();
            if (toDisk)
            {
                var result = await service.BuildFromParsedDataAsync(store, request, output.Path);
                Assert.Equal(expectedWarnings, result.WarningCount);
                qualityTable = result.DataQualityTable;
                files = Directory.EnumerateFiles(result.BundlePath).ToDictionary(path => Path.GetFileName(path), File.ReadAllBytes);
            }
            else
            {
                var result = await service.BuildFromParsedDataToMemoryAsync(store, request);
                Assert.Equal(expectedWarnings, result.WarningCount);
                qualityTable = result.DataQualityTable;
                files = result.Files;
            }
        }
        else
        {
            var source = TenderReviewNamingTests.Source(0, OriginalFilename, "2026-09-06");
            var request = TenderReviewNamingTests.Request([source]);
            var store = Store(source.SourceToken, "J5001", malformedCalendar);
            var service = new TenderReviewBundleService();
            if (toDisk)
            {
                var result = await service.BuildFromParsedDataAsync(store, request, output.Path);
                Assert.Equal(expectedWarnings, result.WarningCount);
                qualityTable = result.DataQualityTable;
                files = Directory.EnumerateFiles(result.BundlePath).ToDictionary(path => Path.GetFileName(path), File.ReadAllBytes);
            }
            else
            {
                var result = await service.BuildFromParsedDataToMemoryAsync(store, request);
                Assert.Equal(expectedWarnings, result.WarningCount);
                qualityTable = result.DataQualityTable;
                files = result.Files;
            }
        }
        Assert.Equal(12, files.Count);
        Assert.DoesNotContain(XerDataQuality.FileName, files.Keys);
        string distribution = System.Text.Encoding.UTF8.GetString(files[EnhancedTableNames.XerResourceDist15 + ".csv"]);
        Assert.Single(distribution.Trim().Split('\n'));
        string diagnostics = System.Text.Encoding.UTF8.GetString(XerDataQuality.WriteToBytes(qualityTable!, CancellationToken.None));
        Assert.Contains(malformedCalendar ? "RESOURCE_CALENDAR_INVALID" : "REMAINING_NO_WORKING_TIME", diagnostics, StringComparison.Ordinal);
        if (malformedCalendar) Assert.Contains("Invalid calendar clock '25:00'", diagnostics, StringComparison.Ordinal);
        var calendarRows = ReviewProjectNameBundleTests.ReadCsv(files["11_XER_CALENDAR_DETAILED.csv"]);
        Assert.Equal(7, calendarRows.Length);
        XerTable quality = Assert.IsType<XerTable>(qualityTable);
        Assert.Single(quality.Rows, row => row.Fields[quality.FieldIndexes["table_name"]] == EnhancedTableNames.XerResourceDist15);
        var calendarWarnings = quality.Rows.Where(row => row.Fields[quality.FieldIndexes["table_name"]] == EnhancedTableNames.XerCalendarDetailed11).ToArray();
        Assert.Equal(malformedCalendar ? 5 : 0, calendarWarnings.Length);
        if (malformedCalendar)
            Assert.Contains(calendarRows, row => row["work_hours"] == "" && row["working_day"] == "" && row["working_day_int"] == "");
        Assert.Contains("8.0000", diagnostics, StringComparison.Ordinal);
        Assert.Contains(OriginalFilename, diagnostics, StringComparison.Ordinal);
        Assert.Equal("preserve me", File.ReadAllText(Path.Combine(output.Path, "unrelated.txt")));
    }

    private static XerDataStore Store(string publicSource, string projectCode, bool malformedCalendar)
    {
        var store = new XerDataStore();
        Add("PROJECT", ["proj_id", "clndr_id", "last_recalc_date", "proj_short_name"],
            ["P1", "C1", "2026-08-31 08:00", projectCode]);
        Add("PROJWBS", ["wbs_id", "proj_id", "parent_wbs_id", "wbs_name"], ["W1", "P1", "", "Root"]);
        Add("TASK", ["task_id", "proj_id", "wbs_id", "clndr_id", "status_code", "task_type", "task_code", "task_name",
            "restart_date", "reend_date", "early_start_date", "early_end_date", "remain_drtn_hr_cnt"],
            ["T1", "P1", "W1", "C1", "TK_NotStart", "TT_Task", "A100", "Resource warning task", "2026-08-30 08:00", "2026-08-30 17:00",
                "2026-08-30 08:00", "2026-08-30 17:00", "8"]);
        string calendarData = P6TestCalendars.WorkWeek();
        if (malformedCalendar)
            calendarData = calendarData.Replace("f|17:00", "f|25:00", StringComparison.Ordinal);
        Add("CALENDAR", ["clndr_id", "clndr_name", "clndr_type", "day_hr_cnt", "clndr_data"],
            ["C1", "Calendar", "CA_Project", "8", calendarData]);
        Add("RSRC", ["rsrc_id", "clndr_id", "rsrc_type", "def_qty_per_hr", "rsrc_name"], ["R1", "C1", "RT_Labor", "1", "Labour"]);
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
