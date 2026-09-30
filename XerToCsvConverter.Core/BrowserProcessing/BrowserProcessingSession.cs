using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.BrowserProcessing;

/// <summary>
/// A single browser session owns all raw inputs/cache entries. Public byte APIs
/// keep their existing mutation checks; this path accepts only private buffers
/// read from immutable browser File handles and verifies every reread.
/// </summary>
internal sealed class BrowserProcessingSession(Func<BrowserSource, CancellationToken, Task<byte[]>> readFile,
    Func<BrowserSource, CancellationToken, Task<string?>>? readContentHash = null)
{
    private readonly Dictionary<string, SourceState> _sources = new(StringComparer.Ordinal);
    private readonly ParsedSourceCache _cache = new();
    private XerDataStore? _standardStore;
    private string _standardSelection = string.Empty;
    private bool _running;

    public void Remove(string token)
    {
        _sources.Remove(token);
        _cache.Remove(token);
        InvalidateDerived();
    }

    public void InvalidateDerived() { _standardStore = null; _standardSelection = string.Empty; }
    public void Clear() { InvalidateDerived(); _sources.Clear(); _cache.Clear(); ClearTemporaryRoots(); }

    public async Task<BrowserResult> RunAsync(BrowserJob job,
        IProgress<ProcessingService.DetailedProgress>? progress = null, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        if (job.Protocol != BrowserJob.ProtocolVersion) throw new InvalidOperationException("The browser processing protocol is out of date. Reload this page.");
        if (_running) throw new InvalidOperationException("A browser processing operation is already running.");
        token.ThrowIfCancellationRequested();
        _running = true;
        using var telemetry = ParserOperationTelemetry.BeginOperation();
        long allocated = GC.GetTotalAllocatedBytes();
        long sampledHeapHighWater = GC.GetTotalMemory(false);
        IProgress<ProcessingService.DetailedProgress>? callerProgress = progress;
        progress = new InlineProgress<ProcessingService.DetailedProgress>(value =>
        {
            sampledHeapHighWater = Math.Max(sampledHeapHighWater, GC.GetTotalMemory(false));
            callerProgress?.Report(value);
        });
        var stages = new Dictionary<string, double>(StringComparer.Ordinal);
        int hits = 0, parsed = 0;
        MemoryStream? output = null;
        try
        {
            if (job.CacheBudgetBytes is long budget) _cache.SetBudget(budget);
            if (job.Kind == "clear") { Clear(); return Result(); }
            if (job.Kind == "invalidate") { InvalidateDerived(); return Result(); }
            if (job.Kind == "remove") { foreach (BrowserSource source in job.Sources) Remove(source.SourceToken); return Result(); }
            ValidateSources(job);
            foreach (BrowserSource source in job.Sources)
            {
                if (!_sources.TryGetValue(source.SourceToken, out SourceState? state))
                    _sources.Add(source.SourceToken, new SourceState(source));
                else if (state.Source.Name != source.Name || state.Source.Size != source.Size)
                    throw new InvalidOperationException("An existing source occurrence cannot be rebound to a different file.");
            }

            if (job.Kind == "metadata")
            {
                if (job.Sources.Count != 1) throw new ArgumentException("One metadata source is required.");
                SourceState state = _sources[job.Sources[0].SourceToken];
                var watch = Stopwatch.StartNew();
                await ReadOwnedAsync(state, token);
                ConfiguredHash(job.Sources[0].SourceSha256, state.Hash!);
                ProgrammeReviewDetectedMetadata metadata;
                using (ParserOperationTelemetry.Measure(ParserOperationPhase.Metadata, state.Source.Name))
                    metadata = await ProgrammeReviewXerMetadataReader.ReadProjectMetadataAsync(state.PendingBytes!, token);
                state.DataDate = metadata.DataDate;
                stages["readHashMetadata"] = watch.Elapsed.TotalMilliseconds;
                return Result() with { DataDate = state.DataDate?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                    MetadataWarnings = metadata.Warnings, SourceSha256 = state.Hash };
            }

            foreach (BrowserSource source in job.Sources)
            {
                SourceState state = _sources[source.SourceToken];
                await EnsureHashAsync(state, token);
                ConfiguredHash(source.SourceSha256, state.Hash!);
            }
            BrowserSource[] selected = job.Sources.ToArray();
            ProgrammeReviewBundleRequest? programme = null;
            TenderReviewBundleRequest? tender = null;
            if (job.Kind == "programme")
            {
                ProgrammeReviewBundleRequest request = job.ProgrammeRequest ?? throw new ArgumentException("Programme metadata is required.");
                RequireExactNames(request.Snapshots.Select(s => s.OriginalXerFilename), selected.Select(s => s.Name));
                var byName = selected.ToDictionary(s => s.Name, StringComparer.Ordinal);
                programme = request with { Snapshots = request.Snapshots.Select(snapshot => snapshot with
                {
                    XerFilePath = null,
                    SourceSha256 = ConfiguredHash(snapshot.SourceSha256, _sources[byName[snapshot.OriginalXerFilename].SourceToken].Hash!)
                }).ToArray() };
                ResolvedProgrammeReviewRequest resolved = ProgrammeReviewNaming.Resolve(programme, s => s.SourceSha256!);
                // Preserve the byte/file path's retention-before-parse policy. A
                // superseded malformed table must never enter the retained union.
                selected = resolved.Snapshots.Select(s => byName[s.OriginalXerFilename]).ToArray();
                InvalidateDerived();
            }
            else if (job.Kind == "tender")
            {
                TenderReviewBundleRequest request = job.TenderRequest ?? throw new ArgumentException("Tender metadata is required.");
                if (!request.Sources.Select(s => s.SourceToken).SequenceEqual(selected.Select(s => s.SourceToken), StringComparer.Ordinal))
                    throw new TenderReviewValidationException("Ordered Tender sources must match their immutable browser input occurrences.");
                for (int i = 0; i < selected.Length; i++)
                    if (request.Sources[i].OriginalXerFilename != selected[i].Name)
                        throw new TenderReviewValidationException("Tender filename provenance does not match the browser source.");
                tender = request with { Sources = request.Sources.Select(source => source with
                {
                    XerFilePath = null,
                    SourceSha256 = ConfiguredHash(source.SourceSha256, _sources[source.SourceToken].Hash!)
                }).ToArray() };
                // Resolve governance before parsing, exactly as the public path.
                TenderReviewNaming.Resolve(tender, s => s.SourceSha256!);
                InvalidateDerived();
            }

            XerDataStore store;
            string selection = string.Join("\n", selected.Select(s => s.SourceToken + ":" + _sources[s.SourceToken].Hash));
            var parseWatch = Stopwatch.StartNew();
            if (job.Kind == "export" && _standardStore is not null && _standardSelection == selection)
                store = _standardStore;
            else
            {
                store = new XerDataStore();
                XerSourceIdentity[] standardIdentities = XerSourceIdentity.CreateOrdered(selected.Select(s => s.Name).ToArray());
                for (int index = 0; index < selected.Length; index++)
                {
                    token.ThrowIfCancellationRequested();
                    BrowserSource source = selected[index];
                    SourceState state = _sources[source.SourceToken];
                    XerDataStore raw;
                    if (_cache.TryGet(source.SourceToken, state.Hash!, out XerDataStore? cached))
                    {
                        hits++;
                        raw = cached!;
                    }
                    else
                    {
                        await ReadOwnedAsync(state, token);
                        int inputIndex = index;
                        using var input = new MemoryStream(state.PendingBytes!, writable: false);
                        raw = await new XerParser(64).ParseXerStreamAsync(input, source.Name, (percent, message) =>
                        {
                            progress?.Report(new ProcessingService.DetailedProgress
                            {
                                Percent = (inputIndex * 70 + Math.Clamp(percent, 0, 100) * 70 / 100) / selected.Length,
                                Message = message, FilePath = tender is null ? source.Name : source.SourceToken,
                                SourceToken = source.SourceToken, InputIndex = inputIndex, FileStatus = "Processing"
                            });
                        }, token);
                        token.ThrowIfCancellationRequested();
                        _cache.Retain(source.SourceToken, state.Hash!, raw, token);
                        state.PendingBytes = null;
                        parsed++;
                    }
                    XerSourceIdentity identity = tender is not null
                        ? new(source.SourceToken, source.Name, source.SourceToken)
                        : standardIdentities[index] with { SourceToken = source.SourceToken };
                    // ApplyTo creates new tables/row wrappers. Cached fields and
                    // immutable raw evidence are never edited by this session.
                    using (ParserOperationTelemetry.Measure(ParserOperationPhase.Merging, source.Name + ":rebind and union"))
                    {
                        XerDataStore view = identity.ApplyTo(raw);
                        if (index == 0)
                        {
                            // This view already owns its table and row lists. An empty
                            // union would copy every wrapper a second time. Standard's
                            // source-header preflight still runs before adopting it.
                            if (tender is null)
                                foreach (string name in view.TableNames)
                                    XerSourceSchema.ValidateHeaders(name, view.GetTable(name)!.Headers);
                            store = view;
                        }
                        else if (tender is not null) TenderReviewBundleService.MergeTenderStoreWithUnionSchema(store, view);
                        else store.MergeStore(view);
                    }
                    progress?.Report(new ProcessingService.DetailedProgress
                    {
                        Percent = (index + 1) * 70 / selected.Length, Message = $"Ready: {source.Name}",
                        FilePath = tender is null ? source.Name : source.SourceToken, SourceToken = source.SourceToken,
                        InputIndex = index, FileStatus = "Success"
                    });
                    await Task.Delay(1, token);
                }
            }
            stages["parseAndMerge"] = parseWatch.Elapsed.TotalMilliseconds;
            if (job.Kind is "parse" or "export") { _standardStore = store; _standardSelection = selection; }
            if (job.Kind == "parse") return Result() with { TableNames = store.TableNames.ToArray() };
            if (job.Kind is not ("export" or "programme" or "tender")) throw new ArgumentException("Unknown browser processing operation.");

            var buildWatch = Stopwatch.StartNew();
            output = new MemoryStream();
            BrowserResult result;
            if (programme is not null)
            {
                ProgrammeReviewZipStreamBundleResult bundle = await new ProgrammeReviewBundleService()
                    .BuildFromParsedDataToZipStreamAsync(store, programme, output, progress, token);
                result = Result() with { BundleId = bundle.BundleId, Filename = bundle.BundleId + ".zip", FileCount = bundle.FileCount,
                    WarningCount = bundle.WarningCount, DiagnosticMessages = XerDataQuality.GetMessages(bundle.DataQualityTable),
                    Diagnostics = Diagnostics(bundle.DataQualityTable, store),
                    DiagnosticSha256 = DiagnosticHash(bundle.DataQualityTable, token), CsvSha256ByFile = bundle.CsvSha256ByFile,
                    RetainedSourceNames = bundle.ManifestRows.Select(row => row.OriginalXerFilename).Distinct(StringComparer.Ordinal).ToArray() };
            }
            else if (tender is not null)
            {
                TenderReviewZipStreamBundleResult bundle = await new TenderReviewBundleService()
                    .BuildFromParsedDataToZipStreamAsync(store, tender, output, progress, token);
                result = Result() with { BundleId = bundle.BundleId, Filename = bundle.BundleId + ".zip", FileCount = bundle.FileCount,
                    WarningCount = bundle.WarningCount, DiagnosticMessages = XerDataQuality.GetMessages(bundle.DataQualityTable),
                    Diagnostics = Diagnostics(bundle.DataQualityTable, store),
                    DiagnosticSha256 = DiagnosticHash(bundle.DataQualityTable, token), CsvSha256ByFile = bundle.CsvSha256ByFile,
                    RetainedSourceNames = selected.Select(source => source.Name).ToArray() };
            }
            else
            {
                StandardZipStreamExportResult exported = await new ProcessingService().ExportTablesToZipStreamWithDiagnosticsAsync(
                    store, job.Tables.ToList(), output, new InlineProgress<(int percent, string message)>(item =>
                        progress?.Report(new ProcessingService.DetailedProgress { Percent = Math.Min(97, item.percent), Message = item.message })), token);
                result = Result() with { FileCount = exported.FileCount, WarningCount = exported.WarningCount, TableNames = store.TableNames.ToArray(),
                    DiagnosticMessages = XerDataQuality.GetMessages(exported.DataQualityTable),
                    Diagnostics = Diagnostics(exported.DataQualityTable, store), DiagnosticSha256 = DiagnosticHash(exported.DataQualityTable, token) };
            }
            stages["buildValidateCsvHashZip"] = buildWatch.Elapsed.TotalMilliseconds;
            token.ThrowIfCancellationRequested();
            output.Position = 0;
            MemoryStream ready = output;
            output = null;
            CapturePhases();
            return result with { ZipContent = ready, StageMilliseconds = stages, AllocatedBytes = GC.GetTotalAllocatedBytes() - allocated,
                ManagedHeapBytes = GC.GetTotalMemory(false), ZipBytes = ready.Length, ZipBufferCapacityBytes = ready.Capacity,
                SampledManagedHeapHighWaterBytes = Math.Max(sampledHeapHighWater, GC.GetTotalMemory(false)) };
        }
        finally
        {
            output?.Dispose();
            if (job.Kind != "metadata")
                foreach (SourceState state in _sources.Values) state.PendingBytes = null;
            _running = false;
            ClearTemporaryRoots();
        }

        void CapturePhases()
        {
            ParserOperationTimingSnapshot snapshot = telemetry.Snapshot();
            foreach (ParserPhaseTiming phase in snapshot.Phases)
                stages[phase.Phase.ToString()] = phase.ExclusiveMs;
            foreach (var detail in snapshot.Scopes.Where(scope => scope.Phase == ParserOperationPhase.Transformation && scope.Detail is not null)
                .GroupBy(scope => scope.Detail!))
                stages["Transformation:" + detail.Key] = detail.Sum(scope => scope.ExclusiveMs);
            stages["operationElapsed"] = snapshot.ElapsedMs;
        }

        BrowserResult Result()
        {
            CapturePhases();
            return new()
            {
            JobId = job.JobId, Generation = job.Generation, RetainedCacheBytes = _cache.RetainedBytes, CacheBudgetBytes = _cache.BudgetBytes,
            CacheHits = hits, ParsedSources = parsed, StageMilliseconds = stages,
            AllocatedBytes = GC.GetTotalAllocatedBytes() - allocated, ManagedHeapBytes = GC.GetTotalMemory(false),
            SampledManagedHeapHighWaterBytes = Math.Max(sampledHeapHighWater, GC.GetTotalMemory(false)), RetainedSourceCount = _cache.Count
            };
        }
    }

    private static void ValidateSources(BrowserJob job)
    {
        if (job.Sources.Count is < 1 or > 24) throw new ArgumentException("Between one and 24 browser sources are required.");
        var tokens = new HashSet<string>(StringComparer.Ordinal);
        long total = 0;
        foreach (BrowserSource source in job.Sources)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(source.SourceToken);
            ArgumentException.ThrowIfNullOrWhiteSpace(source.Name);
            if (!tokens.Add(source.SourceToken)) throw new ArgumentException("Source occurrence tokens must be unique.");
            if (source.Size < 0 || source.Size > 128L * 1024 * 1024) throw new ArgumentException("The browser per-file limit was exceeded.");
            total = checked(total + source.Size);
        }
        if (total > 256L * 1024 * 1024) throw new ArgumentException("The browser total-file limit was exceeded.");
        if (job.Kind is "parse" or "export" && (job.Sources.Count > 20 || job.Sources.Any(s => s.Size > 100L * 1024 * 1024)))
            throw new ArgumentException("The Standard browser input limit was exceeded.");
    }

    private async Task EnsureHashAsync(SourceState state, CancellationToken token)
    {
        if (state.Hash is null) await ReadOwnedAsync(state, token);
        ConfiguredHash(state.Source.SourceSha256, state.Hash!);
    }

    private async Task ReadOwnedAsync(SourceState state, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        if (state.PendingBytes is not null) return;
        byte[] bytes;
        using (ParserOperationTelemetry.Measure(ParserOperationPhase.FileReading, state.Source.Name))
            bytes = await readFile(state.Source, token);
        token.ThrowIfCancellationRequested();
        if (bytes.LongLength != state.Source.Size) throw new InvalidDataException("Browser file length changed during reading.");
        string hash;
        using (ParserOperationTelemetry.Measure(ParserOperationPhase.SourceSha256, state.Source.Name))
        {
            // Only the private immutable-File bridge supplies this digest, computed
            // from the exact buffer it just copied. Other readers retain managed SHA.
            string? browserHash = readContentHash is null ? null : await readContentHash(state.Source, token);
            token.ThrowIfCancellationRequested();
            if (browserHash is null) hash = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
            else
            {
                if (browserHash.Length != 64 || !browserHash.All(Uri.IsHexDigit))
                    throw new InvalidDataException("The browser content digest is not a SHA-256 value.");
                hash = browserHash.ToLowerInvariant();
            }
        }
        ConfiguredHash(state.Hash ?? state.Source.SourceSha256, hash);
        state.Hash = hash;
        state.PendingBytes = bytes;
    }

    private static string ConfiguredHash(string? expected, string actual)
    {
        if (!string.IsNullOrWhiteSpace(expected) && !string.Equals(expected.Trim(), actual, StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("Configured source_sha256 does not match the immutable browser file.");
        return actual;
    }

    private static void RequireExactNames(IEnumerable<string> configured, IEnumerable<string> uploaded)
    {
        string[] left = configured.ToArray(), right = uploaded.ToArray();
        if (left.Distinct(StringComparer.OrdinalIgnoreCase).Count() != left.Length || left.Length != right.Length
            || left.Except(right, StringComparer.Ordinal).Any() || right.Except(left, StringComparer.Ordinal).Any())
            throw new ProgrammeReviewValidationException("Uploaded XER files must exactly match the unique configured snapshot filenames.");
    }

    private static string DiagnosticHash(XerTable? table, CancellationToken token)
        => table is null ? string.Empty : Convert.ToHexString(SHA256.HashData(XerDataQuality.WriteToBytes(table, token))).ToLowerInvariant();

    private static IReadOnlyList<BrowserDiagnostic> Diagnostics(XerTable? table, XerDataStore source)
    {
        if (table is null) return Array.Empty<BrowserDiagnostic>();
        int sourceTableIndex = table.FieldIndexes["source_table"];
        return table.Rows.Select(row => new BrowserDiagnostic(row.SourceToken, row.SourceFilename, row.OriginalSourceFilename,
            row.Fields.ToArray(), (source.GetTable(row.Fields[sourceTableIndex])?.Headers ?? Array.Empty<string>())
                .Select(name => { XerRawField field = row.GetRawField(name); return new BrowserDiagnosticField(name, field.RawValue, field.State); }).ToArray())).ToArray();
    }

    private static void ClearTemporaryRoots() { StringInternPool.Clear(); DateParser.ClearCache(); }
    private sealed class SourceState(BrowserSource source)
    {
        public BrowserSource Source { get; } = source;
        public string? Hash { get; set; }
        public DateOnly? DataDate { get; set; }
        public byte[]? PendingBytes { get; set; }
    }
}

internal sealed class InlineProgress<T>(Action<T> report) : IProgress<T>
{
    public void Report(T value) => report(value);
}
