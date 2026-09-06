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
    public void Diagnostic_1_2_appends_general_evidence_without_reordering_existing_columns()
    {
        string[] previousColumns =
        [
            "diagnostic_schema_version", "severity", "issue_code", "table_name", "source_namespace", "source_row_number",
            "proj_id_key", "task_id_key", "rsrc_id_key", "taskrsrc_id_key", "taskrsrc_id", "task_code", "rsrc_name",
            "rsrc_type", "unit", "status_code", "act_start_date", "act_end_date", "project_data_date", "act_reg_qty",
            "act_ot_qty", "unallocated_actual_quantity", "message"
        ];
        Assert.Equal("1.2", XerDataQuality.SchemaVersion);
        Assert.Equal(previousColumns, XerDataQuality.Columns.Take(previousColumns.Length));
        Assert.Equal(new[] { "allocation_portion", "restart_date", "reend_date", "remain_qty", "curv_id", "remain_crv",
            "unallocated_remaining_quantity", "source_table", "column_name", "raw_value", "raw_row_json" }, XerDataQuality.Columns.Skip(previousColumns.Length));
        Assert.Equal(35, XerDataQuality.Columns.Append("FileName").Count());
    }

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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Programme_disk_and_memory_publish_identical_companion_and_hashes(bool completedRemaining)
    {
        ProgrammeReviewSnapshot snapshot = ProgrammeSnapshot("original.xer", "BL01");
        ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[] { snapshot });
        XerDataStore store = CreateStore(new Input(snapshot.OriginalXerFilename, "private-occurrence", snapshot.OriginalXerFilename,
            CompletedRemaining: completedRemaining));
        var service = new ProgrammeReviewBundleService();
        ProgrammeReviewInMemoryBundleResult memory = await service.BuildFromParsedDataToMemoryAsync(store, request);
        Assert.Equal(1, memory.WarningCount);
        if (completedRemaining)
            AssertCompletedRemaining(memory.Files, "CSV::J123::C::BL01", snapshot.OriginalXerFilename, 1);
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

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Tender_repeated_names_and_content_keep_stage_diagnostics_separate(bool completedRemaining, bool reverseInputs)
    {
        TenderReviewSource first = TenderReviewNamingTests.Source(0, "repeated.xer", "2026-02-10", new string('a', 64));
        TenderReviewSource second = TenderReviewNamingTests.Source(1, "repeated.xer", "2026-02-11", new string('a', 64));
        if (reverseInputs) (first, second) = (second, first);
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { first, second });
        XerDataStore store = CreateStore(
            new Input(first.SourceToken, first.SourceToken, first.OriginalXerFilename, CompletedRemaining: completedRemaining),
            new Input(second.SourceToken, second.SourceToken, second.OriginalXerFilename, CompletedRemaining: completedRemaining));
        TenderReviewInMemoryBundleResult result = await new TenderReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, request);

        Assert.Equal(2, result.WarningCount);
        Assert.Equal(12, result.Files.Count);
        Assert.Equal(10, TenderReviewContract.Tables.Count);
        Assert.Equal(22, result.ManifestRows.Count);
        IReadOnlyList<Dictionary<string, string>> warnings = ReadRows(result.Files[XerDataQuality.FileName]);
        Assert.Equal(2, warnings.Count);
        string[] prefixes = ["CSV::J5001::TENDER::20260210", "CSV::J5001::TENDER::20260211"];
        Assert.Equal(prefixes, warnings.Select(row => row["source_namespace"]).Order(StringComparer.Ordinal));
        foreach (Dictionary<string, string> warning in warnings)
        {
            if (completedRemaining) AssertRemainingWarning(warning, warning["source_namespace"], "repeated.xer");
            else AssertWarning(warning, warning["source_namespace"], "repeated.xer");
        }
        Assert.All(warnings, row => Assert.Equal("1", row["source_row_number"]));
        Assert.DoesNotContain(first.SourceToken, Encoding.UTF8.GetString(result.Files[XerDataQuality.FileName]));
        Assert.DoesNotContain(second.SourceToken, Encoding.UTF8.GetString(result.Files[XerDataQuality.FileName]));
        if (completedRemaining) AssertCompletedAllocations(result.Files["15_XER_RESOURCE_DISTRIBUTION.csv"], 2);
        else AssertAllocations(result.Files["15_XER_RESOURCE_DISTRIBUTION.csv"], 2);
        TenderReviewManifestRow[] manifest = result.ManifestRows.Where(row => row.TableName == XerDataQuality.TableName).ToArray();
        Assert.Equal(2, manifest.Length);
        Assert.All(manifest, row =>
        {
            Assert.Equal(1, row.RowCount);
            Assert.Equal("COMPLETE", row.BundleStatus);
            Assert.Equal(Hash(result.Files[XerDataQuality.FileName]), row.CsvSha256);
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Tender_disk_and_memory_publish_identical_companion_and_hashes(bool completedRemaining)
    {
        TenderReviewSource source = TenderReviewNamingTests.Source(0, "original.xer", "2026-02-10");
        TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { source });
        XerDataStore store = CreateStore(new Input(source.SourceToken, source.SourceToken, source.OriginalXerFilename,
            CompletedRemaining: completedRemaining));
        var service = new TenderReviewBundleService();
        TenderReviewInMemoryBundleResult memory = await service.BuildFromParsedDataToMemoryAsync(store, request);
        Assert.Equal(1, memory.WarningCount);
        if (completedRemaining)
            AssertCompletedRemaining(memory.Files, "CSV::J5001::TENDER::20260210", source.OriginalXerFilename, 1);
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Browser_byte_entrypoints_preserve_diagnostics_and_ordered_tender_occurrences(bool completedRemaining)
    {
        XerDataStore store = CreateStore(new Input("serialized", "private-serialized", "original.xer", CompletedRemaining: completedRemaining));
        byte[] bytes = WriteXerBytes(store);
        ProgrammeReviewSnapshot snapshot = ProgrammeSnapshot("original.xer", "BL01") with { SourceSha256 = null };
        ProgrammeReviewInMemoryBundleResult programme = await new ProgrammeReviewBundleService().BuildFromXerBytesAsync(
            ProgrammeReviewNamingTests.Request(new[] { snapshot }), new Dictionary<string, byte[]> { [snapshot.OriginalXerFilename] = bytes });
        Assert.Equal(1, programme.WarningCount);
        if (completedRemaining) AssertCompletedRemaining(programme.Files, "CSV::J123::C::BL01", "original.xer", 1);
        else AssertWarning(Assert.Single(ReadRows(programme.Files[XerDataQuality.FileName])), "CSV::J123::C::BL01", "original.xer");

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
        if (completedRemaining)
        {
            AssertCompletedAllocations(tender.Files["15_XER_RESOURCE_DISTRIBUTION.csv"], 2);
            Assert.All(ReadRows(tender.Files[XerDataQuality.FileName]), row =>
                AssertRemainingWarning(row, row["source_namespace"], "original.xer"));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Completed_remaining_real_disk_and_browser_ingestion_publish_identical_review_files(bool tender)
    {
        const string original = "completed-remaining.xer";
        byte[] bytes = WriteXerBytes(CreateStore(new Input("serialized", "private-source", original, CompletedRemaining: true)));
        string root = NewTempDirectory();
        try
        {
            string inputPath = Path.Combine(root, original);
            File.WriteAllBytes(inputPath, bytes);
            string outputRoot = Path.Combine(root, "output");
            IReadOnlyDictionary<string, byte[]> browserFiles;
            string bundlePath;
            if (tender)
            {
                // The identical path, bytes and native identities are intentionally two separate stage inputs.
                TenderReviewSource first = TenderReviewNamingTests.Source(0, original, "2026-02-10") with
                { XerFilePath = inputPath, SourceSha256 = null };
                TenderReviewSource second = TenderReviewNamingTests.Source(1, original, "2026-02-11") with
                { XerFilePath = inputPath, SourceSha256 = null };
                TenderReviewBundleRequest request = TenderReviewNamingTests.Request(new[] { first, second });
                var service = new TenderReviewBundleService();
                TenderReviewInMemoryBundleResult browser = await service.BuildFromXerBytesAsync(request, new[]
                {
                    new TenderReviewSourceBytes { SourceToken = first.SourceToken, Content = bytes },
                    new TenderReviewSourceBytes { SourceToken = second.SourceToken, Content = bytes }
                });
                TenderReviewBundleResult disk = await service.BuildFromXerFilesAsync(request, outputRoot);
                Assert.Equal(2, browser.WarningCount);
                Assert.Equal(browser.WarningCount, disk.WarningCount);
                Assert.All(disk.ManifestRows.Where(row => row.TableName == XerDataQuality.TableName), row => Assert.Equal(1, row.RowCount));
                browserFiles = browser.Files;
                bundlePath = disk.BundlePath;
                AssertCompletedAllocations(browserFiles["15_XER_RESOURCE_DISTRIBUTION.csv"], 2);
                Assert.Equal(2, ReadRows(browserFiles[XerDataQuality.FileName]).Select(row => row["source_namespace"]).Distinct().Count());
            }
            else
            {
                ProgrammeReviewSnapshot snapshot = ProgrammeSnapshot(original, "BL01") with
                { XerFilePath = inputPath, SourceSha256 = null };
                ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[] { snapshot });
                var service = new ProgrammeReviewBundleService();
                ProgrammeReviewInMemoryBundleResult browser = await service.BuildFromXerBytesAsync(request,
                    new Dictionary<string, byte[]> { [original] = bytes });
                ProgrammeReviewBundleResult disk = await service.BuildFromXerFilesAsync(request, outputRoot);
                Assert.Equal(1, browser.WarningCount);
                Assert.Equal(browser.WarningCount, disk.WarningCount);
                browserFiles = browser.Files;
                bundlePath = disk.BundlePath;
                AssertCompletedRemaining(browserFiles, "CSV::J123::C::BL01", original, 1);
            }
            Assert.Equal(12, Directory.EnumerateFiles(bundlePath).Count());
            foreach ((string name, byte[] expected) in browserFiles)
                Assert.Equal(expected, File.ReadAllBytes(Path.Combine(bundlePath, name)));
            Assert.Equal(bytes, File.ReadAllBytes(inputPath));
        }
        finally { Directory.Delete(root, recursive: true); }
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Invalid_actual_and_remaining_periods_publish_complete_review_bundle_with_two_portion_warnings(bool tender)
    {
        ProgrammeReviewSnapshot snapshot = ProgrammeSnapshot("bad-remaining.xer", "BL01");
        TenderReviewSource source = TenderReviewNamingTests.Source(0, snapshot.OriginalXerFilename, "2026-02-10");
        XerDataStore store = CreateStore(tender
            ? new Input(source.SourceToken, source.SourceToken, source.OriginalXerFilename)
            : new Input(snapshot.OriginalXerFilename, "private-bad-remaining", snapshot.OriginalXerFilename));
        XerTable assignments = store.GetTable("TASKRSRC")!;
        assignments.Rows[0].Fields[assignments.FieldIndexes["reend_date"]] = "2026-02-01 08:00";
        string root = NewTempDirectory();
        try
        {
            string path;
            if (tender)
            {
                TenderReviewBundleResult result = await new TenderReviewBundleService().BuildFromParsedDataAsync(
                    store, TenderReviewNamingTests.Request(new[] { source }), root);
                Assert.Equal(2, result.WarningCount);
                path = result.BundlePath;
            }
            else
            {
                ProgrammeReviewBundleResult result = await new ProgrammeReviewBundleService().BuildFromParsedDataAsync(
                    store, ProgrammeReviewNamingTests.Request(new[] { snapshot }), root);
                Assert.Equal(2, result.WarningCount);
                path = result.BundlePath;
            }
            Assert.Equal(12, Directory.EnumerateFiles(path).Count());
            IReadOnlyList<Dictionary<string, string>> warnings = ReadRows(File.ReadAllBytes(Path.Combine(path, XerDataQuality.FileName)));
            Assert.Equal(new[] { "Actual", "Remaining" }, warnings.Select(row => row["allocation_portion"]));
            Assert.All(warnings, row => Assert.Equal("1", row["source_row_number"]));
            Assert.Equal("", warnings[0]["unallocated_remaining_quantity"]);
            Assert.Equal("", warnings[1]["unallocated_actual_quantity"]);
            Assert.Equal("REMAINING_PERIOD_INVALID", warnings[1]["issue_code"]);
            Assert.Equal(20m, decimal.Parse(warnings[1]["unallocated_remaining_quantity"], CultureInfo.InvariantCulture));
            Assert.Equal("2026-02-01 08:00", warnings[1]["reend_date"]);
            IReadOnlyList<Dictionary<string, string>> allocations = ReadRows(File.ReadAllBytes(
                Path.Combine(path, "15_XER_RESOURCE_DISTRIBUTION.csv")));
            Assert.All(allocations, row => Assert.Equal("true", row["is_actual"]));
            Assert.Equal(8m, allocations.Sum(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
        }
        finally { Directory.Delete(root, recursive: true); }
    }

    [Theory]
    [InlineData(false, "-5.12555", "-5.1256")]
    [InlineData(true, "-5.12555", "-5.1256")]
    [InlineData(false, "not-a-quantity", "")]
    [InlineData(true, "1e1000", "")]
    public async Task Review_remaining_quantity_warnings_preserve_signed_known_units_and_do_not_invent_unknown_units(
        bool tender, string rawQuantity, string unallocatedQuantity)
    {
        ProgrammeReviewSnapshot snapshot = ProgrammeSnapshot("bad-quantity.xer", "BL01");
        TenderReviewSource source = TenderReviewNamingTests.Source(0, snapshot.OriginalXerFilename, "2026-02-10");
        XerDataStore store = CreateStore(tender
            ? new Input(source.SourceToken, source.SourceToken, source.OriginalXerFilename, CompletedRemaining: true)
            : new Input(snapshot.OriginalXerFilename, "private-quantity", snapshot.OriginalXerFilename, CompletedRemaining: true));
        XerTable assignments = store.GetTable("TASKRSRC")!;
        assignments.Rows[0].Fields[assignments.FieldIndexes["remain_qty"]] = rawQuantity;
        IReadOnlyDictionary<string, byte[]> files = tender
            ? (await new TenderReviewBundleService().BuildFromParsedDataToMemoryAsync(store,
                TenderReviewNamingTests.Request(new[] { source }))).Files
            : (await new ProgrammeReviewBundleService().BuildFromParsedDataToMemoryAsync(store,
                ProgrammeReviewNamingTests.Request(new[] { snapshot }))).Files;

        Dictionary<string, string> warning = Assert.Single(ReadRows(files[XerDataQuality.FileName]));
        Assert.Equal("Remaining", warning["allocation_portion"]);
        Assert.Equal("REMAINING_QUANTITY_INVALID", warning["issue_code"]);
        Assert.Equal(rawQuantity, warning["remain_qty"]);
        Assert.Equal("", warning["unallocated_actual_quantity"]);
        if (unallocatedQuantity.Length == 0) Assert.Equal("", warning["unallocated_remaining_quantity"]);
        else Assert.Equal(decimal.Parse(unallocatedQuantity, CultureInfo.InvariantCulture),
            decimal.Parse(warning["unallocated_remaining_quantity"], CultureInfo.InvariantCulture));
        AssertCompletedAllocations(files["15_XER_RESOURCE_DISTRIBUTION.csv"], 1);
    }

    private static void AssertWarning(IReadOnlyDictionary<string, string> row, string prefix, string original)
    {
        Assert.Equal("1.2", row["diagnostic_schema_version"]);
        Assert.Equal("Actual", row["allocation_portion"]);
        Assert.Equal("", row["unallocated_remaining_quantity"]);
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

    private static void AssertCompletedRemaining(IReadOnlyDictionary<string, byte[]> files, string prefix, string original, int sources)
    {
        Assert.Equal(12, files.Count);
        Assert.Equal(XerDataQuality.Columns.Append("FileName"), ReadHeader(files[XerDataQuality.FileName]));
        AssertRemainingWarning(Assert.Single(ReadRows(files[XerDataQuality.FileName])), prefix, original);
        AssertCompletedAllocations(files["15_XER_RESOURCE_DISTRIBUTION.csv"], sources);
    }

    private static void AssertRemainingWarning(IReadOnlyDictionary<string, string> row, string prefix, string original)
    {
        Assert.Equal("1.2", row["diagnostic_schema_version"]);
        Assert.Equal("Remaining", row["allocation_portion"]);
        Assert.Equal("REMAINING_ON_COMPLETED", row["issue_code"]);
        Assert.Equal(prefix, row["source_namespace"]);
        Assert.Equal(prefix + "::P1", row["proj_id_key"]);
        Assert.Equal(prefix + "::T1", row["task_id_key"]);
        Assert.Equal(prefix + "::R1", row["rsrc_id_key"]);
        Assert.Equal(prefix + "::A1", row["taskrsrc_id_key"]);
        Assert.Equal("A1", row["taskrsrc_id"]);
        Assert.Equal("1", row["source_row_number"]);
        Assert.Equal("TK_Complete", row["status_code"]);
        Assert.Equal(original, row["FileName"]);
        Assert.Equal("2026-01-08 08:00", row["act_start_date"]);
        Assert.Equal("2026-01-09 17:00", row["act_end_date"]);
        Assert.Equal("5.12555", row["act_reg_qty"]);
        Assert.Equal("0.125", row["act_ot_qty"]);
        Assert.Equal("", row["unallocated_actual_quantity"]);
        Assert.Equal("2026-02-03 08:00", row["restart_date"]);
        Assert.Equal("2026-02-06 17:00", row["reend_date"]);
        Assert.Equal("20", row["remain_qty"]);
        Assert.Equal("", row["curv_id"]);
        Assert.Equal("", row["remain_crv"]);
        Assert.Equal(20m, decimal.Parse(row["unallocated_remaining_quantity"], CultureInfo.InvariantCulture));
    }

    private static void AssertCompletedAllocations(byte[] csv, int sources)
    {
        IReadOnlyList<Dictionary<string, string>> rows = ReadRows(csv);
        Assert.NotEmpty(rows);
        Assert.All(rows, row =>
        {
            Assert.Equal("true", row["is_actual"]);
            Assert.False(string.IsNullOrWhiteSpace(row["distribution_month"]));
        });
        Assert.Equal(13.2506m * sources,
            rows.Sum(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
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
        }, input => new[] { new[]
        {
            "T1", "P1", "W1", "C1", "TT_Task", input.CompletedRemaining ? "TK_Complete" : "TK_Active", "A100", "Activity", "", "2026-01-01 08:00", input.CompletedRemaining ? "2026-01-09 17:00" : "",
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
                new[] { "A1", "T1", "P1", "R1", "5.12555", "0.125", "20", input.InvalidActual && !input.CompletedRemaining ? "2026-02-03 08:00" : "2026-01-08 08:00", input.CompletedRemaining ? "2026-01-09 17:00" : "", "2026-02-03 08:00", "2026-02-06 17:00" },
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

    private static byte[] WriteXerBytes(XerDataStore store)
    {
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
        return Encoding.UTF8.GetBytes(text.ToString());
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
    private sealed record Input(string PublicSource, string Token, string Original, bool InvalidActual = true, bool CompletedRemaining = false);
}
