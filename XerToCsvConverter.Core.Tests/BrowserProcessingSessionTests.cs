using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using XerToCsvConverter.BrowserProcessing;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;
using Xunit;

namespace XerToCsvConverter.Core.Tests;

public sealed class BrowserProcessingSessionTests
{
    private static readonly string[] AllTables =
    [
        EnhancedTableNames.XerTask01, EnhancedTableNames.XerProject02, EnhancedTableNames.XerProjWbs03,
        EnhancedTableNames.XerBaseline04, EnhancedTableNames.XerPredecessor06, EnhancedTableNames.XerActvType07,
        EnhancedTableNames.XerActvCode08, EnhancedTableNames.XerTaskActv09, EnhancedTableNames.XerCalendar10,
        EnhancedTableNames.XerCalendarDetailed11, EnhancedTableNames.XerRsrc12, EnhancedTableNames.XerTaskRsrc13,
        EnhancedTableNames.XerUmeasure14, EnhancedTableNames.XerResourceDist15
    ];

    [Fact]
    public async Task Selected_then_all_standard_exports_reuse_parse_and_match_public_stream_csv()
    {
        var files = new PrivateFiles();
        BrowserSource source = files.Add("source-a", "2608 stage.xer", Xer("First activity", badHours: true));
        var session = new BrowserProcessingSession(files.Read);
        BrowserResult parsed = await session.RunAsync(Job("parse", [source]));
        Assert.Null(parsed.ZipContent);
        Assert.Equal(1, parsed.ParsedSources);
        Assert.Contains("TASK", parsed.TableNames);
        BrowserResult selected = await session.RunAsync(Job("export", [source], [EnhancedTableNames.XerTask01]));
        Assert.Equal(0, selected.ParsedSources);
        var selectedOutput = TakeFiles(selected);
        Assert.Equal(2, selectedOutput.Count);
        AssertFiles(await StandardFiles(files, [source], [EnhancedTableNames.XerTask01]), selectedOutput);
        BrowserResult all = await session.RunAsync(Job("export", [source], AllTables));
        Assert.Equal(0, all.ParsedSources);
        Assert.Equal(1, files.ReadCount(source));
        Assert.Equal(15, all.FileCount);
        Assert.True(all.WarningCount > 0);
        AssertFiles(await StandardFiles(files, [source], AllTables), TakeFiles(all));
        files.AssertUnchangedPrivateBuffers();
    }

    [Fact]
    public async Task Metadata_and_profile_changes_rebind_cached_source_without_changing_bytes_or_request()
    {
        var files = new PrivateFiles();
        string tenderToken = TenderReviewNaming.CreateSourceToken(0);
        BrowserSource source = files.Add(tenderToken, "2608 stage.xer", Xer("Activity, with \"quotes\"", badHours: true));
        var session = new BrowserProcessingSession(files.Read);
        BrowserResult metadata = await session.RunAsync(Job("metadata", [source]));
        Assert.Equal("2026-08-31", metadata.DataDate);
        Assert.Equal(Hash(files.Content(source)), metadata.SourceSha256);
        Assert.Equal(0, metadata.ParsedSources);
        Assert.Null(metadata.ZipContent);
        ProgrammeReviewBundleRequest programme = Programme([Snapshot(source)]);
        TenderReviewBundleRequest tender = Tender([Stage(source, 0)]);
        BrowserResult first = await session.RunAsync(Job("programme", [source]) with { ProgrammeRequest = programme });
        Assert.Equal(1, first.ParsedSources);
        await AssertProgrammeParity(files, [source], programme, first);
        BrowserResult second = await session.RunAsync(Job("tender", [source]) with { TenderRequest = tender });
        Assert.Equal(1, second.CacheHits);
        Assert.Equal(0, second.ParsedSources);
        await AssertTenderParity(files, [source], tender, second);
        ProgrammeReviewBundleRequest changed = programme with { ProjectName = "Revised project metadata" };
        BrowserResult third = await session.RunAsync(Job("programme", [source]) with { ProgrammeRequest = changed });
        Assert.Equal(1, third.CacheHits);
        Assert.Equal(0, third.ParsedSources);
        await AssertProgrammeParity(files, [source], changed, third);
        BrowserResult standard = await session.RunAsync(Job("export", [source], AllTables));
        Assert.Equal(1, standard.CacheHits);
        AssertFiles(await StandardFiles(files, [source], AllTables), TakeFiles(standard));
        Assert.Equal(1, files.ReadCount(source));
        Assert.Null(programme.Snapshots[0].SourceSha256);
        Assert.Null(tender.Sources[0].SourceSha256);
        Assert.Equal("Test Project", programme.ProjectName);
        files.AssertUnchangedPrivateBuffers();
    }

    [Fact]
    public async Task Adding_an_update_parses_only_the_new_occurrence_and_changes_export_rows()
    {
        var files = new PrivateFiles();
        BrowserSource first = files.Add("first", "2608 first.xer", Xer("First activity"));
        BrowserSource update = files.Add("update", "2609 update.xer", Xer("Updated activity", "2026-09-30"));
        var session = new BrowserProcessingSession(files.Read);
        _ = await session.RunAsync(Job("parse", [first]));
        BrowserResult result = await session.RunAsync(Job("export", [first, update], [EnhancedTableNames.XerTask01]));
        Assert.Equal(1, result.CacheHits);
        Assert.Equal(1, result.ParsedSources);
        var output = TakeFiles(result);
        AssertFiles(await StandardFiles(files, [first, update], [EnhancedTableNames.XerTask01]), output);
        string task = Encoding.UTF8.GetString(output["01_XER_TASK.csv"]);
        Assert.Contains("First activity", task);
        Assert.Contains("Updated activity", task);
        Assert.Equal(1, files.ReadCount(first));
        Assert.Equal(1, files.ReadCount(update));
    }

    [Fact]
    public async Task Multi_project_metadata_prefills_latest_date_and_transports_warning_without_parsing_or_mutating_file()
    {
        var files = new PrivateFiles();
        BrowserSource source = files.Add("multi", "combined.xer", Encoding.UTF8.GetBytes(
            "%T\tPROJECT\r\n%F\tproj_id\tlast_recalc_date\r\n"
            + "%R\t1\t2026-07-31 08:00\r\n%R\t2\t2026-08-25 17:00\r\n%E\r\n"));
        var session = new BrowserProcessingSession(files.Read);

        BrowserResult metadata = await session.RunAsync(Job("metadata", [source]));
        string json = System.Text.Json.JsonSerializer.Serialize(metadata, BrowserJsonContext.Default.BrowserResult);
        BrowserResult restored = System.Text.Json.JsonSerializer.Deserialize(json, BrowserJsonContext.Default.BrowserResult)!;

        Assert.Equal("2026-08-25", restored.DataDate);
        Assert.Contains("different data dates", Assert.Single(restored.MetadataWarnings));
        Assert.Equal(Hash(files.Content(source)), restored.SourceSha256);
        Assert.Equal(0, metadata.ParsedSources);
        Assert.Equal(0, metadata.RetainedSourceCount);
        Assert.Null(metadata.ZipContent);
        Assert.Equal(1, files.ReadCount(source));
        files.AssertUnchangedPrivateBuffers();
    }

    [Fact]
    public async Task Identical_tender_bytes_and_filenames_remain_two_independent_stages_on_cache_hit()
    {
        var files = new PrivateFiles();
        byte[] bytes = Xer("Shared activity", badHours: true);
        BrowserSource first = files.Add(TenderReviewNaming.CreateSourceToken(0), "same.xer", bytes);
        BrowserSource second = files.Add(TenderReviewNaming.CreateSourceToken(1), "same.xer", bytes);
        var session = new BrowserProcessingSession(files.Read);
        TenderReviewBundleRequest request = Tender([Stage(first, 0), Stage(second, 1)]);
        BrowserJob job = Job("tender", [first, second]) with { TenderRequest = request };
        BrowserResult cold = await session.RunAsync(job);
        Assert.Equal(2, cold.ParsedSources);
        await AssertTenderParity(files, [first, second], request, cold);
        BrowserResult warm = await session.RunAsync(job);
        Assert.Equal(2, warm.CacheHits);
        Assert.Equal(0, warm.ParsedSources);
        var output = await AssertTenderParity(files, [first, second], request, warm);
        string task = Encoding.UTF8.GetString(output[$"{warm.BundleId}/01_XER_TASK.csv"]);
        Assert.Contains("CSV::J5001::TENDER::20260905::T1", task);
        Assert.Contains("CSV::J5001::TENDER::20260906::T1", task);
        Assert.Equal(3, task.Split("\r\n", StringSplitOptions.RemoveEmptyEntries).Length);
        Assert.Equal(1, files.ReadCount(first));
        Assert.Equal(1, files.ReadCount(second));
        files.AssertUnchangedPrivateBuffers();
    }

    [Fact]
    public async Task Superseded_malformed_programme_sources_are_hashed_but_never_parsed_or_exported()
    {
        var files = new PrivateFiles();
        BrowserSource old = files.Add("old", "old baseline.xer", Encoding.UTF8.GetBytes("%T\tTASK\r\n%F\ttask_id\ttask_id\r\n%R\told\tbroken\r\n%E\r\n"));
        BrowserSource selected = files.Add("selected", "selected baseline.xer", Xer("Selected activity", badHours: true));
        ProgrammeReviewSnapshot older = ProgrammeReviewNamingTests.Snapshot(old.Name, ProgrammeReviewSnapshotKind.Baseline,
            "BL01-A", "2026-07-01", "2026-07-31") with { SourceSha256 = null };
        ProgrammeReviewBundleRequest request = Programme([older, Snapshot(selected) with { SnapshotTag = "BL01-B" }]);
        var session = new BrowserProcessingSession(files.Read);
        BrowserResult result = await session.RunAsync(Job("programme", [old, selected]) with { ProgrammeRequest = request });
        Assert.Equal(1, result.ParsedSources);
        Assert.Equal(new[] { selected.Name }, result.RetainedSourceNames);
        var output = await AssertProgrammeParity(files, [old, selected], request, result);
        Assert.DoesNotContain(old.Name, Encoding.UTF8.GetString(output[$"{result.BundleId}/{ProgrammeReviewContract.ManifestFileName}"]));
        Assert.Equal(1, files.ReadCount(old));
        Assert.Equal(1, files.ReadCount(selected));
    }

    [Fact]
    public void Raw_source_cache_evicts_the_least_recently_used_source_and_obeys_reduced_budget()
    {
        XerDataStore store = CacheStore();
        long bytes = ParsedSourceCache.EstimateBytes(store);
        var cache = new ParsedSourceCache(bytes * 2);
        cache.Retain("a", "hash-a", store, default);
        cache.Retain("b", "hash-b", store, default);
        Assert.True(cache.TryGet("a", "hash-a", out _));
        cache.Retain("c", "hash-c", store, default);
        Assert.False(cache.TryGet("b", "hash-b", out _));
        Assert.True(cache.TryGet("a", "hash-a", out _));
        cache.SetBudget(bytes);
        Assert.False(cache.TryGet("c", "hash-c", out _));
        Assert.True(cache.TryGet("a", "hash-a", out _));
        Assert.False(cache.TryGet("a", "different-content", out _));
        Assert.InRange(cache.RetainedBytes, 0, cache.BudgetBytes);
        cache.SetBudget(bytes - 1);
        cache.Retain("oversized", "hash", store, default);
        Assert.Equal(0, cache.Count);
        Assert.Equal(0, cache.RetainedBytes);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.Throws<OperationCanceledException>(() => cache.Retain("canceled", "hash", store, cancellation.Token));
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public async Task Session_cache_budget_evicts_an_untouched_source_and_reparse_preserves_export_bytes()
    {
        var files = new PrivateFiles();
        BrowserSource first = files.Add("first", "same.xer", Xer("First activity"));
        BrowserSource second = files.Add("second", "same.xer", Xer("Other activity"));
        BrowserSource third = files.Add("third", "same.xer", Xer("Third activity"));
        var session = new BrowserProcessingSession(files.Read);
        BrowserResult initial = await session.RunAsync(Job("parse", [first]));
        long oneSourceBytes = initial.RetainedCacheBytes;
        Assert.True(oneSourceBytes > 0);
        await session.RunAsync(Job("parse", [second]) with { CacheBudgetBytes = oneSourceBytes * 2 });
        Assert.Equal(1, (await session.RunAsync(Job("parse", [first]))).CacheHits);
        BrowserResult added = await session.RunAsync(Job("parse", [third]));
        Assert.Equal(1, added.ParsedSources);
        Assert.InRange(added.RetainedCacheBytes, 1, oneSourceBytes * 2);
        Assert.Equal(1, (await session.RunAsync(Job("parse", [first]))).CacheHits);
        BrowserResult evicted = await session.RunAsync(Job("export", [second], [EnhancedTableNames.XerTask01]));
        Assert.Equal(0, evicted.CacheHits);
        Assert.Equal(1, evicted.ParsedSources);
        Assert.Equal(2, files.ReadCount(second));
        Assert.Equal(1, files.ReadCount(first));
        Assert.Equal(1, files.ReadCount(third));
        AssertFiles(await StandardFiles(files, [second], [EnhancedTableNames.XerTask01]), TakeFiles(evicted));
    }

    [Fact]
    public async Task Oversized_uncached_source_is_reread_and_mutation_is_rejected_without_output()
    {
        var files = new PrivateFiles();
        BrowserSource source = files.Add("source", "2608 source.xer", Xer("Original activity"));
        var session = new BrowserProcessingSession(files.Read);
        BrowserJob parse = Job("parse", [source]) with { CacheBudgetBytes = 0 };
        BrowserResult first = await session.RunAsync(parse);
        Assert.Equal(0, first.RetainedCacheBytes);
        session.InvalidateDerived();
        BrowserResult second = await session.RunAsync(parse);
        Assert.Equal(1, second.ParsedSources);
        Assert.Equal(0, second.CacheHits);
        Assert.Equal(2, files.ReadCount(source));
        files.Replace(source, Xer("Modified activity")); // Equal byte length, different content.
        session.InvalidateDerived();
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(() => session.RunAsync(parse with { Kind = "export", Tables = AllTables }));
        Assert.Contains("source_sha256", error.Message);
        session.Remove(source.SourceToken);
        BrowserResult replacement = await session.RunAsync(Job("export", [source], [EnhancedTableNames.XerTask01]) with { CacheBudgetBytes = 0 });
        Assert.Contains("Modified activity", Encoding.UTF8.GetString(TakeFiles(replacement)["01_XER_TASK.csv"]));
    }

    [Fact]
    public async Task Source_rebinding_size_and_configured_hash_mismatches_fail_before_export()
    {
        var files = new PrivateFiles();
        BrowserSource source = files.Add("source", "2608 source.xer", Xer("Activity"));
        var session = new BrowserProcessingSession(files.Read);
        _ = await session.RunAsync(Job("parse", [source]));
        foreach (BrowserSource changed in new[] { source with { Name = "other.xer" }, source with { Size = source.Size + 1 } })
            await Assert.ThrowsAsync<InvalidOperationException>(() => session.RunAsync(Job("export", [changed], AllTables)));
        await Assert.ThrowsAsync<InvalidDataException>(() => session.RunAsync(Job("export", [source with { SourceSha256 = new string('0', 64) }], AllTables)));
        await Assert.ThrowsAsync<InvalidDataException>(() => session.RunAsync(Job("metadata", [source with { SourceSha256 = new string('0', 64) }])));
        var fresh = new BrowserProcessingSession(files.Read);
        await Assert.ThrowsAsync<InvalidDataException>(() => fresh.RunAsync(Job("parse", [source with { SourceSha256 = new string('0', 64) }])));
        var wrongLength = new BrowserProcessingSession(files.Read);
        InvalidDataException length = await Assert.ThrowsAsync<InvalidDataException>(() => wrongLength.RunAsync(Job("parse", [source with { Size = source.Size + 1 }])));
        Assert.Contains("length", length.Message);
        AssertFiles(await StandardFiles(files, [source], [EnhancedTableNames.XerTask01]),
            TakeFiles(await session.RunAsync(Job("export", [source], [EnhancedTableNames.XerTask01]))));
    }

    [Fact]
    public async Task Cancellation_after_zip_writing_and_failed_export_both_allow_clean_retry()
    {
        var files = new PrivateFiles();
        BrowserSource source = files.Add("source", "2608 source.xer", Xer("Retry activity", badHours: true));
        var session = new BrowserProcessingSession(files.Read);
        using var cancellation = new CancellationTokenSource();
        bool wroteEntry = false;
        var progress = new InlineProgress<ProcessingService.DetailedProgress>(value =>
        {
            if (value.Message?.StartsWith("Exported:", StringComparison.Ordinal) == true)
            {
                wroteEntry = true;
                cancellation.Cancel();
            }
        });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => session.RunAsync(Job("export", [source], AllTables), progress, cancellation.Token));
        Assert.True(wroteEntry);
        await Assert.ThrowsAsync<InvalidDataException>(() => session.RunAsync(Job("export", [source], ["UNSUPPORTED_TABLE"])));
        BrowserResult retry = await session.RunAsync(Job("export", [source], AllTables));
        Assert.Equal(0, retry.ParsedSources);
        Assert.Equal(1, files.ReadCount(source));
        AssertFiles(await StandardFiles(files, [source], AllTables), TakeFiles(retry));
    }

    [Fact]
    public async Task Invalidate_remove_and_clear_discard_derived_outputs_and_do_not_resurrect_removed_rows()
    {
        var files = new PrivateFiles();
        BrowserSource first = files.Add("first", "2608 first.xer", Xer("Removed activity"));
        BrowserSource second = files.Add("second", "2608 second.xer", Xer("Retained activity"));
        var session = new BrowserProcessingSession(files.Read);
        TakeFiles(await session.RunAsync(Job("export", [first, second], [EnhancedTableNames.XerTask01])));
        BrowserResult invalidated = await session.RunAsync(Job("invalidate", []));
        Assert.Null(invalidated.ZipContent);
        BrowserResult reused = await session.RunAsync(Job("parse", [first, second]));
        Assert.Equal(2, reused.CacheHits);
        Assert.Equal(0, reused.ParsedSources);
        BrowserResult removed = await session.RunAsync(Job("remove", [first]));
        Assert.Null(removed.ZipContent);
        BrowserResult remaining = await session.RunAsync(Job("export", [second], [EnhancedTableNames.XerTask01]));
        var output = TakeFiles(remaining);
        Assert.DoesNotContain("Removed activity", Encoding.UTF8.GetString(output["01_XER_TASK.csv"]));
        AssertFiles(await StandardFiles(files, [second], [EnhancedTableNames.XerTask01]), output);
        BrowserResult cleared = await session.RunAsync(Job("clear", []));
        Assert.Null(cleared.ZipContent);
        Assert.Equal(0, cleared.RetainedCacheBytes);
        BrowserResult reparsed = await session.RunAsync(Job("export", [second], [EnhancedTableNames.XerTask01]));
        Assert.Equal(1, reparsed.ParsedSources);
        Assert.Equal(0, reparsed.CacheHits);
        Assert.Equal(2, files.ReadCount(second));
        AssertFiles(await StandardFiles(files, [second], [EnhancedTableNames.XerTask01]), TakeFiles(reparsed));
    }

    private static BrowserJob Job(string kind, IReadOnlyList<BrowserSource> sources, IReadOnlyList<string>? tables = null) =>
        new() { JobId = "test-job", Generation = 7, Kind = kind, Sources = sources, Tables = tables ?? [] };

    private static ProgrammeReviewSnapshot Snapshot(BrowserSource source) => ProgrammeReviewNamingTests.Snapshot(source.Name,
        ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-08-01", "2026-08-31") with { SourceSha256 = null };
    private static ProgrammeReviewBundleRequest Programme(IReadOnlyList<ProgrammeReviewSnapshot> sources) =>
        ProgrammeReviewNamingTests.Request(sources) with { ProjectCode = "J5001" };
    private static TenderReviewSource Stage(BrowserSource source, int index) => TenderReviewNamingTests.Source(index,
        source.Name, index == 0 ? "2026-09-05" : "2026-09-06") with { SourceToken = source.SourceToken, SourceSha256 = null };
    private static TenderReviewBundleRequest Tender(IReadOnlyList<TenderReviewSource> sources) => TenderReviewNamingTests.Request(sources);

    private static async Task<Dictionary<string, byte[]>> AssertProgrammeParity(PrivateFiles files, IReadOnlyList<BrowserSource> sources,
        ProgrammeReviewBundleRequest request, BrowserResult actual)
    {
        using var zip = new MemoryStream();
        ProgrammeReviewZipStreamBundleResult expected = await new ProgrammeReviewBundleService().BuildFromXerBytesToZipStreamAsync(request,
            sources.ToDictionary(source => source.Name, source => files.Content(source).ToArray(), StringComparer.Ordinal), zip);
        Dictionary<string, byte[]> output = TakeFiles(actual);
        AssertFiles(ReadZip(zip), output);
        Assert.Equal(12, actual.FileCount);
        Assert.Equal(actual.FileCount, output.Count);
        Assert.DoesNotContain(output.Keys, name => name.EndsWith("XER_DATA_QUALITY.csv", StringComparison.Ordinal));
        Assert.Equal(expected.BundleId, actual.BundleId);
        Assert.Equal(expected.WarningCount, actual.WarningCount);
        Assert.Equal(XerDataQuality.GetMessages(expected.DataQualityTable), actual.DiagnosticMessages);
        Assert.Equal(Hash(XerDataQuality.WriteToBytes(Assert.IsType<XerTable>(expected.DataQualityTable))), actual.DiagnosticSha256);
        AssertDiagnostics(expected.DataQualityTable!, actual.Diagnostics, sources, tender: false);
        Assert.Equal(expected.CsvSha256ByFile.OrderBy(pair => pair.Key), actual.CsvSha256ByFile.OrderBy(pair => pair.Key));
        foreach (var pair in actual.CsvSha256ByFile) Assert.Equal(pair.Value, Hash(output[$"{actual.BundleId}/{pair.Key}"]));
        return output;
    }

    private static async Task<Dictionary<string, byte[]>> AssertTenderParity(PrivateFiles files, IReadOnlyList<BrowserSource> sources,
        TenderReviewBundleRequest request, BrowserResult actual)
    {
        using var zip = new MemoryStream();
        TenderReviewZipStreamBundleResult expected = await new TenderReviewBundleService().BuildFromXerBytesToZipStreamAsync(request,
            sources.Select(source => new TenderReviewSourceBytes { SourceToken = source.SourceToken,
                Content = files.Content(source).ToArray() }).ToArray(), zip);
        Dictionary<string, byte[]> output = TakeFiles(actual);
        AssertFiles(ReadZip(zip), output);
        Assert.Equal(12, actual.FileCount);
        Assert.Equal(actual.FileCount, output.Count);
        Assert.DoesNotContain(output.Keys, name => name.EndsWith("XER_DATA_QUALITY.csv", StringComparison.Ordinal));
        Assert.Equal(expected.BundleId, actual.BundleId);
        Assert.Equal(expected.WarningCount, actual.WarningCount);
        Assert.Equal(XerDataQuality.GetMessages(expected.DataQualityTable), actual.DiagnosticMessages);
        Assert.Equal(Hash(XerDataQuality.WriteToBytes(Assert.IsType<XerTable>(expected.DataQualityTable))), actual.DiagnosticSha256);
        AssertDiagnostics(expected.DataQualityTable!, actual.Diagnostics, sources, tender: true);
        Assert.Equal(expected.CsvSha256ByFile.OrderBy(pair => pair.Key), actual.CsvSha256ByFile.OrderBy(pair => pair.Key));
        foreach (var pair in actual.CsvSha256ByFile) Assert.Equal(pair.Value, Hash(output[$"{actual.BundleId}/{pair.Key}"]));
        return output;
    }

    private static void AssertDiagnostics(XerTable expected, IReadOnlyList<BrowserDiagnostic> actual,
        IReadOnlyList<BrowserSource> sources, bool tender)
    {
        Assert.Equal(expected.RowCount, actual.Count);
        for (int index = 0; index < expected.RowCount; index++)
        {
            DataRow row = expected.Rows[index];
            BrowserDiagnostic diagnostic = actual[index];
            Assert.Equal(row.Fields, diagnostic.Values);
            Assert.Equal(row.OriginalSourceFilename, diagnostic.OriginalSourceFilename);
            Assert.Equal(row.SourceFilename, diagnostic.SourceFilename);
            Assert.Equal(tender ? row.SourceToken : sources.Single(source => source.Name == row.OriginalSourceFilename).SourceToken, diagnostic.SourceToken);
            foreach (BrowserDiagnosticField field in diagnostic.RawFields)
            {
                XerRawField original = row.GetRawField(field.Name);
                Assert.Equal(original.RawValue, field.RawValue);
                Assert.Equal(original.State, field.State);
            }
        }
    }

    private static async Task<Dictionary<string, byte[]>> StandardFiles(PrivateFiles files, IReadOnlyList<BrowserSource> sources, IReadOnlyList<string> tables)
    {
        var service = new ProcessingService();
        var inputs = sources.Select(source => ((Stream)new MemoryStream(files.Content(source).ToArray(), writable: false), source.Name)).ToArray();
        try
        {
            XerDataStore parsed = await service.ParseXerStreamsAsync(inputs, null, default);
            using var output = new MemoryStream();
            await service.ExportTablesToZipStreamWithDiagnosticsAsync(parsed, tables.ToList(), output, null, default);
            return ReadZip(output);
        }
        finally { foreach (var input in inputs) input.Item1.Dispose(); }
    }

    private static Dictionary<string, byte[]> TakeFiles(BrowserResult result)
    {
        using MemoryStream zip = Assert.IsType<MemoryStream>(result.ZipContent);
        return ReadZip(zip);
    }

    private static Dictionary<string, byte[]> ReadZip(MemoryStream stream)
    {
        stream.Position = 0;
        using var archive = new ZipArchive(stream, ZipArchiveMode.Read, leaveOpen: true);
        return archive.Entries.ToDictionary(entry => entry.FullName, entry =>
        {
            using Stream input = entry.Open();
            using var output = new MemoryStream();
            input.CopyTo(output);
            return output.ToArray();
        }, StringComparer.Ordinal);
    }

    private static void AssertFiles(IReadOnlyDictionary<string, byte[]> expected, IReadOnlyDictionary<string, byte[]> actual)
    {
        Assert.Equal(expected.Keys.Order(StringComparer.Ordinal), actual.Keys.Order(StringComparer.Ordinal));
        foreach (string file in expected.Keys) Assert.Equal(expected[file], actual[file]);
    }

    private static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    private static XerDataStore CacheStore()
    {
        var table = new XerTable("TASK");
        table.SetHeaders(["task_id", "task_name"]);
        table.AddRow(new DataRow(["T1", "Cache activity"], "source"));
        var store = new XerDataStore();
        store.AddTable(table);
        return store;
    }

    // Same complete source contract used by the existing review bundle fixtures,
    // with real calendar slots and an optional invalid duration for diagnostic parity.
    private static byte[] Xer(string activity, string dataDate = "2026-08-31", bool badHours = false)
    {
        var builder = new StringBuilder();
        Table("TASK", ["task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code", "task_code", "task_name",
            "rsrc_id", "act_start_date", "act_end_date", "early_start_date", "early_end_date", "late_start_date", "late_end_date",
            "restart_date", "reend_date", "target_start_date", "target_end_date", "cstr_type", "cstr_date", "priority_type", "float_path",
            "float_path_order", "driving_path_flag", "remain_drtn_hr_cnt", "target_drtn_hr_cnt", "total_float_hr_cnt", "free_float_hr_cnt",
            "complete_pct_type", "phys_complete_pct", "act_work_qty", "remain_work_qty"],
            ["T1", "P1", "W1", "C1", "TT_Task", "TK_NotStart", "A100", activity, "", "", "", "2026-09-07", "2026-09-11",
                "2026-09-07", "2026-09-11", "2026-09-07", "2026-09-11", "2026-09-07", "2026-09-11", "", "", "", "", "", "Y",
                badHours ? "bad" : "16", "40", "8", "4", "CP_Phys", "0", "0", "0"]);
        Table("PROJECT", ["proj_id", "last_recalc_date", "proj_short_name", "add_date"], ["P1", dataDate, "J5001", "2026-06-02 12:00"]);
        Table("PROJWBS", ["wbs_id", "parent_wbs_id", "proj_id", "wbs_name"], ["W1", "", "P1", "Root WBS"]);
        Table("CALENDAR", ["clndr_id", "clndr_name", "day_hr_cnt", "clndr_type", "clndr_data"],
            ["C1", "Standard 8h", "8", "CA_Project", P6TestCalendars.WorkWeek()]);
        builder.Append("%E\r\n");
        return Encoding.UTF8.GetBytes(builder.ToString());

        void Table(string name, string[] headers, string[] fields)
        {
            Assert.Equal(headers.Length, fields.Length);
            builder.Append("%T\t").Append(name).Append("\r\n%F\t").AppendJoin('\t', headers)
                .Append("\r\n%R\t").AppendJoin('\t', fields).Append("\r\n");
        }
    }

    private sealed class PrivateFiles
    {
        private readonly Dictionary<string, byte[]> _contents = new(StringComparer.Ordinal);
        private readonly Dictionary<string, int> _reads = new(StringComparer.Ordinal);
        private readonly List<(byte[] Private, byte[] Original)> _private = new();

        internal BrowserSource Add(string token, string name, byte[] bytes)
        {
            _contents.Add(token, bytes.ToArray());
            return new BrowserSource(token, name, bytes.LongLength);
        }

        internal Task<byte[]> Read(BrowserSource source, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            _reads[source.SourceToken] = ReadCount(source) + 1;
            byte[] owned = _contents[source.SourceToken].ToArray();
            _private.Add((owned, owned.ToArray()));
            return Task.FromResult(owned);
        }

        internal byte[] Content(BrowserSource source) => _contents[source.SourceToken];
        internal void Replace(BrowserSource source, byte[] bytes) => _contents[source.SourceToken] = bytes.ToArray();
        internal int ReadCount(BrowserSource source) => _reads.GetValueOrDefault(source.SourceToken);
        internal void AssertUnchangedPrivateBuffers()
        {
            foreach (var pair in _private) Assert.Equal(pair.Original, pair.Private);
        }
    }
}
