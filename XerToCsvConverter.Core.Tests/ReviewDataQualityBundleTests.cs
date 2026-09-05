using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Microsoft.VisualBasic.FileIO;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ReviewDataQualityBundleTests
{
    [Fact]
    public async Task Programme_preserves_bad_actuals_and_valid_remaining_with_manifested_warning()
    {
        ProgrammeReviewSnapshot snapshot = ProgrammeSnapshot("original.xer", "BL01");
        XerDataStore store = CreateStore(new Input(snapshot.OriginalXerFilename,
            "private-parser-occurrence", snapshot.OriginalXerFilename));
        ProgrammeReviewInMemoryBundleResult result = await new ProgrammeReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, ProgrammeReviewNamingTests.Request(new[] { snapshot }));

        Assert.Equal(1, result.WarningCount);
        Assert.Equal(12, result.Files.Count);
        Assert.Equal(10, ProgrammeReviewContract.Tables.Count);
        Assert.Equal(11, result.ManifestRows.Count);
        Dictionary<string, string> warning = Assert.Single(ReadRows(result.Files[XerDataQuality.FileName]));
        AssertWarning(warning, "CSV::J123::C::BL01", snapshot.OriginalXerFilename);
        Assert.DoesNotContain("private-parser-occurrence", Encoding.UTF8.GetString(result.Files[XerDataQuality.FileName]));
        AssertAllocations(result.Files["15_XER_RESOURCE_DISTRIBUTION.csv"], 1);
        ProgrammeReviewManifestRow manifest = Assert.Single(result.ManifestRows, row => row.TableName == XerDataQuality.TableName);
        Assert.Equal(1, manifest.RowCount);
        Assert.Equal("complete", manifest.BundleStatus);
        Assert.Equal(Hash(result.Files[XerDataQuality.FileName]), manifest.CsvSha256);
        Assert.Equal(manifest.CsvSha256, result.CsvSha256ByFile[XerDataQuality.FileName]);

        // Every supplied bad value remains unchanged in the caller's raw data.
        XerTable assignments = store.GetTable("TASKRSRC")!;
        Assert.Equal("2026-02-03 08:00", assignments.Rows[0].Fields[assignments.FieldIndexes["act_start_date"]]);
        Assert.Equal("5.12555", assignments.Rows[0].Fields[assignments.FieldIndexes["act_reg_qty"]]);
    }

    [Fact]
    public async Task Programme_disk_and_memory_publish_identical_companion_and_hashes()
    {
        ProgrammeReviewSnapshot snapshot = ProgrammeSnapshot("original.xer", "BL01");
        ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[] { snapshot });
        XerDataStore store = CreateStore(new Input(snapshot.OriginalXerFilename, "private-occurrence", snapshot.OriginalXerFilename));
        var service = new ProgrammeReviewBundleService();
        ProgrammeReviewInMemoryBundleResult memory = await service.BuildFromParsedDataToMemoryAsync(store, request);
        string root = NewTempDirectory();
        try
        {
            ProgrammeReviewBundleResult disk = await service.BuildFromParsedDataAsync(store, request, root);
            Assert.Equal(memory.WarningCount, disk.WarningCount);
            Assert.Equal(12, Directory.EnumerateFiles(disk.BundlePath).Count());
            foreach ((string name, byte[] bytes) in memory.Files)
                Assert.Equal(bytes, File.ReadAllBytes(Path.Combine(disk.BundlePath, name)));
            Assert.Equal(memory.CsvSha256ByFile[XerDataQuality.FileName], disk.CsvSha256ByFile[XerDataQuality.FileName]);
        }
        finally { Directory.Delete(root, recursive: true); }
    }

    [Fact]
    public async Task Programme_discarded_snapshot_cannot_contribute_warnings()
    {
        ProgrammeReviewSnapshot discarded = ProgrammeSnapshot("discarded.xer", "BL01");
        ProgrammeReviewSnapshot retained = ProgrammeSnapshot("retained.xer", "BL02");
        XerDataStore store = CreateStore(
            new Input(discarded.OriginalXerFilename, "discarded-occurrence", discarded.OriginalXerFilename),
            new Input(retained.OriginalXerFilename, "retained-occurrence", retained.OriginalXerFilename, InvalidActual: false));
        ProgrammeReviewInMemoryBundleResult result = await new ProgrammeReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, ProgrammeReviewNamingTests.Request(new[] { discarded, retained }));

        Assert.Equal(0, result.WarningCount);
        Assert.Empty(ReadRows(result.Files[XerDataQuality.FileName]));
        Assert.Equal(11, result.ManifestRows.Count);
        Assert.All(result.ManifestRows, row => Assert.Equal(retained.OriginalXerFilename, row.OriginalXerFilename));
        Assert.Equal(0, result.ManifestRows.Single(row => row.TableName == XerDataQuality.TableName).RowCount);
    }

    [Fact]
    public async Task Tender_repeated_names_and_content_keep_stage_diagnostics_separate()
    {
        TenderReviewSource first = TenderReviewNamingTests.Source(0, "repeated.xer", "2026-02-10", new string('a', 64));
        TenderReviewSource second = TenderReviewNamingTests.Source(1, "repeated.xer", "2026-02-11", new string('a', 64));
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { first, second });
        XerDataStore store = CreateStore(
            new Input(first.SourceToken, first.SourceToken, first.OriginalXerFilename),
            new Input(second.SourceToken, second.SourceToken, second.OriginalXerFilename));
        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, request);

        Assert.Equal(2, result.WarningCount);
        Assert.Equal(12, result.Files.Count);
        Assert.Equal(10, TenderReviewContract.Tables.Count);
        Assert.Equal(22, result.ManifestRows.Count);
        IReadOnlyList<Dictionary<string, string>> warnings = ReadRows(result.Files[XerDataQuality.FileName]);
        Assert.Equal(2, warnings.Count);
        AssertWarning(warnings[0], "CSV::J5001::TENDER::20260210", "repeated.xer");
        AssertWarning(warnings[1], "CSV::J5001::TENDER::20260211", "repeated.xer");
        Assert.All(warnings, row => Assert.Equal("1", row["source_row_number"]));
        Assert.DoesNotContain(first.SourceToken, Encoding.UTF8.GetString(result.Files[XerDataQuality.FileName]));
        Assert.DoesNotContain(second.SourceToken, Encoding.UTF8.GetString(result.Files[XerDataQuality.FileName]));
        AssertAllocations(result.Files["15_XER_RESOURCE_DISTRIBUTION.csv"], 2);
        TenderReviewManifestRow[] manifest = result.ManifestRows.Where(row => row.TableName == XerDataQuality.TableName).ToArray();
        Assert.Equal(2, manifest.Length);
        Assert.All(manifest, row =>
        {
            Assert.Equal(1, row.RowCount);
            Assert.Equal("COMPLETE", row.BundleStatus);
            Assert.Equal(Hash(result.Files[XerDataQuality.FileName]), row.CsvSha256);
        });
    }

    [Fact]
    public async Task Tender_disk_and_memory_publish_identical_companion_and_hashes()
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "original.xer", "2026-02-10");
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { source });
        XerDataStore store = CreateStore(new Input(source.SourceToken, source.SourceToken, source.OriginalXerFilename));
        var service = new TenderReviewBundleService();
        TenderReviewInMemoryBundleResult memory = await service.BuildFromParsedDataToMemoryAsync(store, request);
        string root = NewTempDirectory();
        try
        {
            TenderReviewBundleResult disk = await service.BuildFromParsedDataAsync(store, request, root);
            Assert.Equal(memory.WarningCount, disk.WarningCount);
            Assert.Equal(12, Directory.EnumerateFiles(disk.BundlePath).Count());
            foreach ((string name, byte[] bytes) in memory.Files)
                Assert.Equal(bytes, File.ReadAllBytes(Path.Combine(disk.BundlePath, name)));
            Assert.Equal(memory.CsvSha256ByFile[XerDataQuality.FileName], disk.CsvSha256ByFile[XerDataQuality.FileName]);
        }
        finally { Directory.Delete(root, recursive: true); }
    }

    [Fact]
    public async Task Browser_byte_entrypoints_preserve_diagnostics_and_ordered_tender_occurrences()
    {
        XerDataStore store = CreateStore(new Input("serialized", "private-serialized", "original.xer"));
        var text = new StringBuilder();
        foreach (string name in store.TableNames)
        {
            XerTable table = store.GetTable(name)!;
            text.Append("%T\t").Append(name).Append("\r\n");
            text.Append("%F\t").AppendJoin('\t', table.Headers!).Append("\r\n");
            foreach (DataRow row in table.Rows)
                text.Append("%R\t").AppendJoin('\t', row.Fields).Append("\r\n");
        }
        text.Append("%E\r\n");
        byte[] bytes = Encoding.UTF8.GetBytes(text.ToString());
        ProgrammeReviewSnapshot snapshot = ProgrammeSnapshot("original.xer", "BL01") with { SourceSha256 = null };
        ProgrammeReviewInMemoryBundleResult programme = await new ProgrammeReviewBundleService().BuildFromXerBytesAsync(
            ProgrammeReviewNamingTests.Request(new[] { snapshot }), new Dictionary<string, byte[]> { [snapshot.OriginalXerFilename] = bytes });
        Assert.Equal(1, programme.WarningCount);
        AssertWarning(Assert.Single(ReadRows(programme.Files[XerDataQuality.FileName])), "CSV::J123::C::BL01", "original.xer");

        TenderReviewSource first = TenderReviewNamingTests.Source(0, "original.xer", "2026-02-10") with { SourceSha256 = null };
        TenderReviewSource second = TenderReviewNamingTests.Source(1, "original.xer", "2026-02-11") with { SourceSha256 = null };
        TenderReviewInMemoryBundleResult tender = await new TenderReviewBundleService().BuildFromXerBytesAsync(
            TenderReviewNamingTests.Request(new[] { first, second }), new[]
            {
                new TenderReviewSourceBytes { SourceToken = first.SourceToken, Content = bytes },
                new TenderReviewSourceBytes { SourceToken = second.SourceToken, Content = bytes }
            });
        Assert.Equal(2, tender.WarningCount);
        Assert.Equal(2, ReadRows(tender.Files[XerDataQuality.FileName]).Select(row => row["source_namespace"]).Distinct().Count());
        Assert.All(tender.ManifestRows.Where(row => row.TableName == XerDataQuality.TableName), row => Assert.Equal(Hash(bytes), row.SourceSha256));
    }

    [Fact]
    public async Task Optional_absent_assignments_still_emit_header_only_diagnostics_in_both_profiles()
    {
        ProgrammeReviewSnapshot snapshot = ProgrammeSnapshot("empty.xer", "BL01");
        XerDataStore programmeStore = CreateStore(new Input(snapshot.OriginalXerFilename, "private-empty", snapshot.OriginalXerFilename), includeAssignments: false);
        ProgrammeReviewInMemoryBundleResult programme = await new ProgrammeReviewBundleService().BuildFromParsedDataToMemoryAsync(
            programmeStore, ProgrammeReviewNamingTests.Request(new[] { snapshot }));
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "empty.xer", "2026-02-10");
        XerDataStore tenderStore = CreateStore(new Input(source.SourceToken, source.SourceToken, source.OriginalXerFilename), includeAssignments: false);
        TenderReviewInMemoryBundleResult tender = await new TenderReviewBundleService().BuildFromParsedDataToMemoryAsync(
            tenderStore, TenderReviewNamingTests.Request(new[] { source }));

        Assert.Equal(0, programme.WarningCount);
        Assert.Equal(0, tender.WarningCount);
        Assert.Equal(programme.Files[XerDataQuality.FileName], tender.Files[XerDataQuality.FileName]);
        Assert.Empty(ReadRows(programme.Files[XerDataQuality.FileName]));
        Assert.Equal(XerDataQuality.Columns.Append("FileName"), ReadHeader(programme.Files[XerDataQuality.FileName]));
        Assert.Equal(0, tender.ManifestRows.Single(row => row.TableName == XerDataQuality.TableName).RowCount);
    }

    [Fact]
    public async Task Remaining_date_failure_does_not_publish_partial_review_bundle_or_warning_file()
    {
        ProgrammeReviewSnapshot snapshot = ProgrammeSnapshot("bad-remaining.xer", "BL01");
        XerDataStore store = CreateStore(new Input(snapshot.OriginalXerFilename, "private-bad-remaining", snapshot.OriginalXerFilename));
        XerTable assignments = store.GetTable("TASKRSRC")!;
        assignments.Rows[0].Fields[assignments.FieldIndexes["reend_date"]] = "2026-02-01 08:00";
        string root = NewTempDirectory();
        try
        {
            await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() => new ProgrammeReviewBundleService()
                .BuildFromParsedDataAsync(store, ProgrammeReviewNamingTests.Request(new[] { snapshot }), root));
            Assert.Empty(Directory.EnumerateFileSystemEntries(root));
        }
        finally { Directory.Delete(root, recursive: true); }
    }

    private static void AssertWarning(IReadOnlyDictionary<string, string> row, string prefix, string original)
    {
        Assert.Equal(prefix, row["source_namespace"]);
        Assert.Equal(prefix + "::P1", row["proj_id_key"]);
        Assert.Equal(prefix + "::T1", row["task_id_key"]);
        Assert.Equal(prefix + "::R1", row["rsrc_id_key"]);
        Assert.Equal(prefix + "::A1", row["taskrsrc_id_key"]);
        Assert.Equal("A1", row["taskrsrc_id"]);
        Assert.Equal(original, row["FileName"]);
        Assert.Equal("2026-02-03 08:00", row["act_start_date"]);
        Assert.Equal("", row["act_end_date"]);
        Assert.Equal("2026-01-30 08:00", row["project_data_date"]);
        Assert.Equal("5.12555", row["act_reg_qty"]);
        Assert.Equal("0.125", row["act_ot_qty"]);
        Assert.Equal(5.2506m, decimal.Parse(row["unallocated_actual_quantity"], CultureInfo.InvariantCulture));
    }

    private static void AssertAllocations(byte[] csv, int sources)
    {
        IReadOnlyList<Dictionary<string, string>> rows = ReadRows(csv);
        Assert.Equal(8m * sources, rows.Where(row => row["is_actual"] == "true")
            .Sum(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
        Assert.Equal(20m * sources, rows.Where(row => row["is_actual"] == "false")
            .Sum(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
        Assert.All(rows, row => Assert.False(string.IsNullOrWhiteSpace(row["distribution_month"])));
    }

    private static ProgrammeReviewSnapshot ProgrammeSnapshot(string name, string tag) =>
        ProgrammeReviewNamingTests.Snapshot(name, ProgrammeReviewSnapshotKind.Baseline, tag, "2026-01-31", "2026-01-30");

    private static XerDataStore CreateStore(Input input, bool includeAssignments = true) => CreateStore(new[] { input }, includeAssignments);
    private static XerDataStore CreateStore(params Input[] inputs) => CreateStore(inputs, true);

    private static XerDataStore CreateStore(Input[] inputs, bool includeAssignments)
    {
        var store = new XerDataStore();
        Add("TASK", new[]
        {
            "task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code", "task_code", "task_name",
            "rsrc_id", "act_start_date", "act_end_date", "early_start_date", "early_end_date", "late_start_date",
            "late_end_date", "restart_date", "reend_date", "target_start_date", "target_end_date", "cstr_type", "cstr_date", "priority_type",
            "float_path", "float_path_order", "driving_path_flag", "remain_drtn_hr_cnt", "target_drtn_hr_cnt",
            "total_float_hr_cnt", "free_float_hr_cnt", "complete_pct_type", "phys_complete_pct", "act_work_qty", "remain_work_qty"
        }, _ => new[] { new[]
        {
            "T1", "P1", "W1", "C1", "TT_Task", "TK_Active", "A100", "Activity", "", "2026-01-01 08:00", "",
            "2026-01-01 08:00", "2026-02-06 17:00", "2026-01-01 08:00", "2026-02-06 17:00",
            "2026-02-03 08:00", "2026-02-06 17:00", "2026-01-01 08:00", "2026-02-06 17:00",
            "", "", "", "", "", "Y", "24", "40", "16", "8", "CP_Phys", "0", "0", "0"
        } });
        Add("PROJECT", new[] { "proj_id", "last_recalc_date", "proj_short_name", "clndr_id" },
            _ => new[] { new[] { "P1", "2026-01-30 08:00", "J5001", "C1" } });
        Add("PROJWBS", new[] { "wbs_id", "parent_wbs_id", "proj_id", "wbs_name" },
            _ => new[] { new[] { "W1", "", "P1", "Root" } });
        Add("CALENDAR", new[] { "clndr_id", "clndr_name", "day_hr_cnt", "clndr_type", "clndr_data" },
            _ => new[] { new[] { "C1", "Calendar", "8", "CA_Project", P6TestCalendars.WorkWeek() } });
        if (!includeAssignments) return store;
        Add("RSRC", new[] { "rsrc_id", "rsrc_short_name", "rsrc_name", "rsrc_type", "unit_id", "clndr_id", "def_qty_per_hr" },
            _ => new[] { new[] { "R1", "LAB", "Labour", "RT_Labor", "", "C1", "1" } });
        Add("TASKRSRC", new[] { "taskrsrc_id", "task_id", "proj_id", "rsrc_id", "act_reg_qty", "act_ot_qty", "remain_qty", "act_start_date", "act_end_date", "restart_date", "reend_date" },
            input => new[]
            {
                new[] { "A1", "T1", "P1", "R1", "5.12555", "0.125", "20", input.InvalidActual ? "2026-02-03 08:00" : "2026-01-08 08:00", "", "2026-02-03 08:00", "2026-02-06 17:00" },
                new[] { "A2", "T1", "P1", "R1", "7.5", "0.5", "0", "2026-01-08 08:00", "2026-01-09 17:00", "", "" }
            });
        return store;

        void Add(string name, string[] headers, Func<Input, string[][]> values)
        {
            var table = new XerTable(name);
            table.SetHeaders(headers);
            foreach (Input input in inputs)
                foreach (string[] fields in values(input))
                    table.AddRow(new DataRow(fields, input.PublicSource, input.Token, input.Original));
            store.AddTable(table);
        }
    }

    private static IReadOnlyList<Dictionary<string, string>> ReadRows(byte[] bytes)
    {
        IReadOnlyList<string[]> rows = ReadCsv(bytes);
        return rows.Skip(1).Select(row => rows[0].Zip(row).ToDictionary(pair => pair.First, pair => pair.Second)).ToArray();
    }

    private static string[] ReadHeader(byte[] bytes) => ReadCsv(bytes)[0];

    private static IReadOnlyList<string[]> ReadCsv(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes, writable: false);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: false)
        { TextFieldType = FieldType.Delimited, HasFieldsEnclosedInQuotes = true, TrimWhiteSpace = false };
        parser.SetDelimiters(",");
        var rows = new List<string[]>();
        while (!parser.EndOfData) rows.Add(parser.ReadFields()!);
        return rows;
    }

    private static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    private static string NewTempDirectory() => Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "xer-review-warning-tests-" + Guid.NewGuid().ToString("N"))).FullName;
    private sealed record Input(string PublicSource, string Token, string Original, bool InvalidActual = true);
}
