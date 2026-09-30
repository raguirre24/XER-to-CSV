using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class StreamingZipExportTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Standard_streaming_matches_memory_and_reports_actual_files_without_mutating_parsed_data(bool enhanced)
    {
        using var input = new MemoryStream(Fixture());
        var service = new ProcessingService();
        XerDataStore parsed = await service.ParseXerStreamsAsync([(input, "sample.xer")], null, CancellationToken.None);
        var requested = new List<string> { enhanced ? "02_XER_PROJECT" : "PROJECT" };
        StandardMemoryExportResult expected = await service.ExportTablesToMemoryWithDiagnosticsAsync(parsed, requested, null, CancellationToken.None);
        using var output = new MemoryStream();
        var percentages = new List<int>();
        StandardZipStreamExportResult actual = await service.ExportTablesToZipStreamWithDiagnosticsAsync(parsed,
            requested, output, new InlineProgress<(int percent, string message)>(p => percentages.Add(p.percent)), CancellationToken.None);
        Assert.Equal(expected.WarningCount, actual.WarningCount);
        Assert.Equal(expected.Files.Count, actual.FileCount);
        Assert.Equal(enhanced ? 2 : 1, actual.FileCount);
        AssertArchive(output, expected.Files.ToDictionary(pair => pair.Key + ".csv", pair => pair.Value));
        AssertMonotonic(percentages);
        Assert.True(output.CanWrite);
        StandardMemoryExportResult repeated = await service.ExportTablesToMemoryWithDiagnosticsAsync(parsed, requested, null, CancellationToken.None);
        Assert.Equal(expected.Files.Keys, repeated.Files.Keys);
        foreach (string name in expected.Files.Keys) Assert.Equal(expected.Files[name], repeated.Files[name]);
        using var compatibilityOutput = new MemoryStream();
        Assert.Equal(actual.WarningCount, await service.ExportTablesToZipStreamAsync(parsed, requested,
            compatibilityOutput, null, CancellationToken.None));
    }

    [Fact]
    public async Task Programme_streaming_matches_memory_hashes_manifest_and_internal_parsed_entrypoint()
    {
        byte[] source = Fixture();
        ProgrammeReviewBundleRequest request = ProgrammeRequest();
        var uploads = new Dictionary<string, byte[]> { ["sample.xer"] = source };
        var service = new ProgrammeReviewBundleService();
        ProgrammeReviewInMemoryBundleResult expected = await service.BuildFromXerBytesAsync(request, uploads);
        using var output = new MemoryStream();
        var percentages = new List<int>();
        ProgrammeReviewZipStreamBundleResult actual = await service.BuildFromXerBytesToZipStreamAsync(request, uploads,
            output, new InlineProgress<ProcessingService.DetailedProgress>(p => percentages.Add(p.Percent)));
        Assert.Equal(12, actual.FileCount);
        Assert.Equal(expected.ManifestRows, actual.ManifestRows);
        Assert.Equal(expected.WarningCount, actual.WarningCount);
        AssertArchive(output, BundleFiles(expected.BundleId, expected.Files), actual.CsvSha256ByFile);
        AssertMonotonic(percentages);
        using var input = new MemoryStream(source);
        XerDataStore parsed = await new ProcessingService().ParseXerStreamsAsync([(input, "sample.xer")], null, CancellationToken.None);
        ProgrammeReviewBundleRequest hydrated = request with
        {
            Snapshots = request.Snapshots.Select(snapshot => snapshot with { SourceSha256 = Hash(source) }).ToArray()
        };
        using var parsedOutput = new MemoryStream();
        ProgrammeReviewZipStreamBundleResult workerResult = await service.BuildFromParsedDataToZipStreamAsync(parsed, hydrated, parsedOutput);
        Assert.Equal(actual.ManifestRows, workerResult.ManifestRows);
        AssertArchive(parsedOutput, BundleFiles(expected.BundleId, expected.Files), workerResult.CsvSha256ByFile);
    }

    [Fact]
    public async Task Tender_streaming_preserves_duplicate_stage_identity_hashes_and_deterministic_archive()
    {
        byte[] source = Fixture();
        TenderReviewBundleRequest request = TenderRequest(twoStages: true);
        TenderReviewSourceBytes[] uploads = request.Sources.Select(stage => new TenderReviewSourceBytes
        {
            SourceToken = stage.SourceToken,
            Content = source
        }).ToArray();
        var service = new TenderReviewBundleService();
        TenderReviewInMemoryBundleResult expected = await service.BuildFromXerBytesAsync(request, uploads);
        using var output = new MemoryStream();
        var percentages = new List<int>();
        TenderReviewZipStreamBundleResult actual = await service.BuildFromXerBytesToZipStreamAsync(request, uploads,
            output, new InlineProgress<ProcessingService.DetailedProgress>(p => percentages.Add(p.Percent)));
        Assert.Equal(12, actual.FileCount);
        Assert.Equal(22, actual.ManifestRows.Count);
        Assert.Equal(expected.ManifestRows, actual.ManifestRows);
        Assert.Equal(expected.WarningCount, actual.WarningCount);
        DateTime timestamp = TenderReviewContract.GetDeterministicZipEntryTimestamp(request.ExportedAtUtc!.Value).DateTime;
        AssertArchive(output, BundleFiles(expected.BundleId, expected.Files), actual.CsvSha256ByFile, timestamp);
        AssertMonotonic(percentages);
        using var repeatedOutput = new MemoryStream();
        await service.BuildFromXerBytesToZipStreamAsync(request, uploads, repeatedOutput);
        Assert.Equal(output.ToArray(), repeatedOutput.ToArray());
    }

    [Fact]
    public async Task Tender_internal_parsed_entrypoint_preserves_source_provenance()
    {
        byte[] source = Fixture();
        TenderReviewBundleRequest request = TenderRequest();
        string token = request.Sources[0].SourceToken;
        using var input = new MemoryStream(source);
        XerDataStore parsed = await new ProcessingService().ParseXerStreamsAsync([(input, token)], null, CancellationToken.None);
        request = request with { Sources = request.Sources.Select(stage => stage with { SourceSha256 = Hash(source) }).ToArray() };
        var service = new TenderReviewBundleService();
        TenderReviewInMemoryBundleResult expected = await service.BuildFromParsedDataToMemoryAsync(parsed, request);
        using var output = new MemoryStream();
        TenderReviewZipStreamBundleResult actual = await service.BuildFromParsedDataToZipStreamAsync(parsed, request, output);
        Assert.Equal(expected.ManifestRows, actual.ManifestRows);
        AssertArchive(output, BundleFiles(expected.BundleId, expected.Files), actual.CsvSha256ByFile);
    }

    [Theory]
    [InlineData("standard")]
    [InlineData("programme")]
    [InlineData("tender")]
    public async Task Nonwritable_destination_is_rejected_before_any_input_processing(string mode)
    {
        using var output = new MemoryStream([], writable: false);
        await Assert.ThrowsAsync<ArgumentException>(() => Build(mode, output));
        Assert.True(output.CanRead);
    }

    [Theory]
    [InlineData("standard")]
    [InlineData("programme")]
    [InlineData("tender")]
    public async Task Failed_destination_never_returns_success_and_stays_owned_by_caller(string mode)
    {
        using var output = new FailingOutputStream();
        await Assert.ThrowsAsync<IOException>(() => Build(mode, output));
        Assert.True(output.CanWrite);
    }

    [Theory]
    [InlineData("standard")]
    [InlineData("programme")]
    [InlineData("tender")]
    public async Task Precancelled_export_leaves_caller_output_empty_and_open(string mode)
    {
        using var output = new MemoryStream();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Build(mode, output, new CancellationToken(true)));
        Assert.Equal(0, output.Length);
        Assert.True(output.CanWrite);
    }

    [Theory]
    [InlineData("programme")]
    [InlineData("tender")]
    public async Task Cancellation_at_packaging_never_returns_a_complete_result_and_leaves_stream_owned_by_caller(string mode)
    {
        using var cancellation = new CancellationTokenSource();
        using var output = new MemoryStream();
        var progress = new InlineProgress<ProcessingService.DetailedProgress>(p =>
        {
            if (p.Message?.StartsWith("Packaging", StringComparison.Ordinal) == true) cancellation.Cancel();
        });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Build(mode, output, cancellation.Token, progress));
        Assert.True(output.CanWrite);
    }

    [Theory]
    [InlineData("programme")]
    [InlineData("tender")]
    public async Task Cancellation_after_archive_finalization_is_observed_before_return(string mode)
    {
        using var cancellation = new CancellationTokenSource();
        using var output = new MemoryStream();
        var progress = new InlineProgress<ProcessingService.DetailedProgress>(p =>
        {
            if (p.Message == "Archive ready for browser download.") cancellation.Cancel();
        });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Build(mode, output, cancellation.Token, progress));
        Assert.True(output.Length > 0);
        Assert.True(output.CanWrite);
    }

    [Fact]
    public async Task Standard_observes_cancellation_after_archive_finalization()
    {
        using var cancellation = new CancellationTokenSource();
        using var input = new MemoryStream(Fixture());
        var service = new ProcessingService();
        XerDataStore parsed = await service.ParseXerStreamsAsync([(input, "sample.xer")], null, CancellationToken.None);
        using var output = new MemoryStream();
        var progress = new InlineProgress<(int percent, string message)>(p =>
        {
            if (p.percent == 97) cancellation.Cancel();
        });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.ExportTablesToZipStreamWithDiagnosticsAsync(
            parsed, ["PROJECT"], output, progress, cancellation.Token));
        Assert.True(output.Length > 0);
        Assert.True(output.CanWrite);
    }

    [Fact]
    public async Task Programme_streaming_retains_input_mutation_guard_before_zip_output()
    {
        byte[] source = Fixture();
        using var output = new MemoryStream();
        var progress = new InlineProgress<ProcessingService.DetailedProgress>(p =>
        {
            if (p.Message?.StartsWith("Finished parsing", StringComparison.Ordinal) == true) source[^1] ^= 1;
        });
        await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
            new ProgrammeReviewBundleService().BuildFromXerBytesToZipStreamAsync(ProgrammeRequest(),
                new Dictionary<string, byte[]> { ["sample.xer"] = source }, output, progress));
        Assert.Equal(0, output.Length);
        Assert.True(output.CanWrite);
    }

    private static async Task Build(string mode, Stream output, CancellationToken cancellation = default,
        IProgress<ProcessingService.DetailedProgress>? progress = null)
    {
        byte[] source = Fixture();
        if (mode == "programme")
            await new ProgrammeReviewBundleService().BuildFromXerBytesToZipStreamAsync(ProgrammeRequest(),
                new Dictionary<string, byte[]> { ["sample.xer"] = source }, output, progress, cancellation);
        else if (mode == "tender")
        {
            TenderReviewBundleRequest request = TenderRequest();
            await new TenderReviewBundleService().BuildFromXerBytesToZipStreamAsync(request,
                [new TenderReviewSourceBytes { SourceToken = request.Sources[0].SourceToken, Content = source }], output, progress, cancellation);
        }
        else
        {
            var table = new XerTable("PROJECT");
            table.SetHeaders(["proj_id"]);
            table.AddRow(new DataRow(["P1"], "sample.xer"));
            var store = new XerDataStore();
            store.AddTable(table);
            await new ProcessingService().ExportTablesToZipStreamWithDiagnosticsAsync(store, ["PROJECT"], output, null, cancellation);
        }
    }

    private static void AssertArchive(MemoryStream output, IReadOnlyDictionary<string, byte[]> expected,
        IReadOnlyDictionary<string, string>? hashes = null, DateTime? timestamp = null)
    {
        output.Position = 0;
        using var archive = new ZipArchive(output, ZipArchiveMode.Read, leaveOpen: true);
        Assert.Equal(expected.Keys.Order(StringComparer.Ordinal), archive.Entries.Select(entry => entry.FullName));
        foreach (ZipArchiveEntry entry in archive.Entries)
        {
            using var bytes = new MemoryStream();
            using (Stream content = entry.Open()) content.CopyTo(bytes);
            Assert.Equal(expected[entry.FullName], bytes.ToArray());
            if (hashes?.TryGetValue(Path.GetFileName(entry.FullName), out string? expectedHash) == true)
                Assert.Equal(expectedHash, Hash(bytes.ToArray()));
            if (timestamp is { } time) Assert.Equal(time, entry.LastWriteTime.DateTime);
        }
    }

    private static Dictionary<string, byte[]> BundleFiles(string bundleId, IReadOnlyDictionary<string, byte[]> files) =>
        files.ToDictionary(pair => bundleId + "/" + pair.Key, pair => pair.Value);
    private static void AssertMonotonic(IReadOnlyList<int> values)
    {
        Assert.NotEmpty(values);
        Assert.Equal(97, values[^1]);
        Assert.All(values, value => Assert.InRange(value, 0, 97));
        for (int index = 1; index < values.Count; index++) Assert.True(values[index] >= values[index - 1]);
    }

    private static ProgrammeReviewBundleRequest ProgrammeRequest() => ProgrammeReviewNamingTests.Request(
        [ProgrammeReviewNamingTests.Snapshot("sample.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30") with { SourceSha256 = null }]);
    private static TenderReviewBundleRequest TenderRequest(bool twoStages = false) => TenderReviewNamingTests.Request(
        twoStages
            ? [TenderReviewNamingTests.Source(0, "sample.xer", "2026-09-05") with { SourceSha256 = null }, TenderReviewNamingTests.Source(1, "sample.xer", "2026-09-06") with { SourceSha256 = null }]
            : [TenderReviewNamingTests.Source(0, "sample.xer", "2026-09-05") with { SourceSha256 = null }]);
    private static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    private static byte[] Fixture()
    {
        var builder = new StringBuilder();
        Add("TASK", ["task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code", "task_code", "task_name",
            "rsrc_id", "act_start_date", "act_end_date", "early_start_date", "early_end_date", "late_start_date", "late_end_date",
            "target_start_date", "target_end_date", "cstr_type", "cstr_date", "priority_type", "float_path", "float_path_order",
            "driving_path_flag", "remain_drtn_hr_cnt", "target_drtn_hr_cnt", "total_float_hr_cnt", "free_float_hr_cnt",
            "complete_pct_type", "phys_complete_pct", "act_work_qty", "remain_work_qty"],
            ["T1", "P1", "W1", "C1", "TT_Task", "TK_NotStart", "A100", "Quoted, \"activity\"", "", "", "",
            "2026-02-02", "2026-02-06", "2026-02-02", "2026-02-06", "2026-02-02", "2026-02-06", "", "", "", "", "", "Y",
            "24", "40", "16", "8", "CP_Phys", "0", "0", "0"]);
        Add("PROJECT", ["proj_id", "last_recalc_date", "proj_short_name", "add_date"], ["P1", "2026-01-30", "J5001", "2025-12-01"]);
        Add("PROJWBS", ["wbs_id", "parent_wbs_id", "proj_id", "wbs_name"], ["W1", "", "P1", "Root WBS"]);
        Add("CALENDAR", ["clndr_id", "clndr_name", "day_hr_cnt", "clndr_type", "clndr_data"], ["C1", "Standard 8h", "8", "CA_Project", P6TestCalendars.WorkWeek()]);
        builder.Append("%E\r\n");
        return Encoding.UTF8.GetBytes(builder.ToString());
        void Add(string name, string[] headers, string[] values) => builder.Append("%T\t").Append(name).Append("\r\n%F\t")
            .AppendJoin('\t', headers).Append("\r\n%R\t").AppendJoin('\t', values).Append("\r\n");
    }

    private sealed class InlineProgress<T>(Action<T> report) : IProgress<T>
    {
        public void Report(T value) => report(value);
    }

    private sealed class FailingOutputStream : MemoryStream
    {
        public override void Write(byte[] buffer, int offset, int count) => throw new IOException("Synthetic destination failure.");
        public override void Write(ReadOnlySpan<byte> buffer) => throw new IOException("Synthetic destination failure.");
    }
}
