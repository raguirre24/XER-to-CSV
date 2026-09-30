using System.Diagnostics;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.VisualBasic.FileIO;
using XerToCsvConverter;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

// Native evidence only. CSV and diagnostic bytes are authoritative; ZIP timestamps and
// native elapsed times are not compared, and do not establish browser responsiveness.
if (args.Length % 2 != 0) throw new ArgumentException("Arguments must be --name value pairs.");
var arguments = Enumerable.Range(0, args.Length / 2).ToDictionary(index => args[index * 2].TrimStart('-'),
    index => args[index * 2 + 1], StringComparer.OrdinalIgnoreCase);
if (arguments.TryGetValue("compare", out string? firstReport))
{
    Parity.Compare(firstReport, arguments["against"], arguments["output"]);
    return;
}
string repo = Path.GetFullPath(arguments.GetValueOrDefault("repo", Path.Combine(AppContext.BaseDirectory, "../../../../../../")));
string label = arguments.GetValueOrDefault("label", "candidate");
string output = Path.GetFullPath(arguments.GetValueOrDefault("output", Path.Combine(repo, "artifacts/parser-performance-20260930", label)));
int iterations = int.Parse(arguments.GetValueOrDefault("iterations", "2"));
if (iterations < 1) throw new ArgumentOutOfRangeException(nameof(iterations));
Directory.CreateDirectory(output);
if (arguments.ContainsKey("cache-estimates"))
{
#if PARSER_CACHE_EVIDENCE
    await CacheMeasurements.Run(arguments, repo, output);
    return;
#else
    throw new NotSupportedException("Build the candidate harness with EnableCacheMeasurements=true.");
#endif
}
var cases = new List<CaseInput>();
if (!arguments.ContainsKey("fixtures-only"))
{
    SourceInput august = await SourceInput.Read(arguments.GetValueOrDefault("august", Path.Combine(repo, "XER Sample/2608-EBA_PAA_8.0 (draft).xer")));
    SourceInput july = await SourceInput.Read(arguments.GetValueOrDefault("july", "C:/Users/ricar/Downloads/2607 C5064-C-2607.xer"));
    cases.Add(new("real-legacy-enhanced", "enhanced", [august]));
    cases.Add(new("real-legacy-raw", "raw", [august]));
    cases.Add(new("real-legacy-selected", "selected", [august]));
    cases.Add(new("real-programme-single", "programme", [august]));
    cases.Add(new("real-programme-history", "programme", [july, august]));
    cases.Add(new("real-tender-single", "tender", [august]));
    cases.Add(new("real-tender-repeated", "tender", [august with { Name = "repeated.xer" }, august with { Name = "repeated.xer" }]));
    cases.Add(new("real-tender-two-sources", "tender", [july, august]));
}
byte[] fixture = File.ReadAllBytes(Path.Combine(repo, "review/fixtures/J5001_C_BL01_2026-01-31.xer"));
foreach ((string name, byte[] bytes) in Fixtures.Create(fixture))
{
    string fixturePath = Path.Combine(output, "fixtures", name + ".xer");
    Directory.CreateDirectory(Path.GetDirectoryName(fixturePath)!);
    File.WriteAllBytes(fixturePath, bytes);
    SourceInput source = await SourceInput.Read(fixturePath);
    foreach (string mode in new[] { "enhanced", "programme", "tender" })
        cases.Add(new($"fixture-{name}-{mode}", mode, [source]));
    if (name is "quality-portions" or "truncated-cells")
        cases.Add(new($"fixture-{name}-tender-repeated", "tender", [source with { Name = "repeated.xer" }, source with { Name = "repeated.xer" }]));
}
SourceInput evolvingFirst = await SourceInput.Read(Path.Combine(output, "fixtures/missing-headers.xer"));
SourceInput evolvingSecond = await SourceInput.Read(Path.Combine(output, "fixtures/quality-portions.xer"));
cases.Add(new("fixture-tender-evolving-schema", "tender", [evolvingFirst with { Name = "repeated.xer" }, evolvingSecond with { Name = "repeated.xer" }]));

var report = new RunReport(label, typeof(ProcessingService).Assembly.GetName().Version!.ToString(),
    "2.6.0", iterations, new List<CaseResult>());
foreach (CaseInput input in cases)
{
    Console.WriteLine($"{label}: {input.Name}");
    CaseResult? first = null;
    for (int iteration = 0; iteration < iterations; iteration++)
    {
        CaseResult result = await Runner.Run(input, iteration, Path.Combine(output, input.Name, iteration.ToString()));
        if (first is not null)
        {
            string? difference = Parity.Difference(first, result);
            if (difference is not null) throw new InvalidDataException($"Repeated-run parity failed: {input.Name}: {difference}");
        }
        first ??= result;
        report.Cases.Add(result);
        Console.WriteLine($"  #{iteration}: {result.Status}, {result.ElapsedMs:F1} ms, {result.Files.Count} files, {result.WarningCount} warnings");
        File.WriteAllText(Path.Combine(output, "report.json"), JsonSerializer.Serialize(report, Parity.JsonOptions));
    }
}

internal sealed record SourceInput(string Name, byte[] Bytes, string Sha256, DateOnly DataDate)
{
    internal static async Task<SourceInput> Read(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        DateOnly date = await ProgrammeReviewXerMetadataReader.ReadSingleProjectDataDateAsync(bytes)
            ?? new DateOnly(2026, 1, 30); // Explicit fixture metadata; invalid authoritative dates still fail in Build.
        return new(Path.GetFileName(path), bytes, Parity.Hash(bytes), date);
    }
}
internal sealed record CaseInput(string Name, string Mode, SourceInput[] Sources);
internal sealed record FileEvidence(string Name, long ByteCount, long DataRowCount, string Sha256, bool RowOrderIncludedInSha256);
internal sealed record CaseResult(string Name, string Mode, int Iteration, string Status, string? Error,
    double ElapsedMs, double? ParseMs, double? ExportMs, long ZipByteCount, int WarningCount,
    IReadOnlyList<FileEvidence> Files, FileEvidence? Diagnostics, IReadOnlyList<string> EntryOrder,
    IReadOnlyList<string> SourceSha256, bool ManifestReconciled)
{
    public PhaseEvidence? PhaseTiming { get; init; }
}
internal sealed record PhaseEvidence(double OperationMs, bool HasOverlaps,
    IReadOnlyList<PhaseTotalEvidence> Phases, IReadOnlyList<PhaseScopeEvidence> Scopes);
internal sealed record PhaseTotalEvidence(string Phase, double InclusiveMs, double ExclusiveMs, int Scopes, long BufferedCalls);
internal sealed record PhaseScopeEvidence(string Phase, string? Detail, double StartMs, double EndMs,
    double InclusiveMs, double ExclusiveMs, bool OverlappingChildren);
internal sealed record RunReport(string Label, string CoreAssemblyVersion, string RequestParserVersion, int Iterations, List<CaseResult> Cases);

#if PARSER_CACHE_EVIDENCE
internal static class CacheMeasurements
{
    internal static async Task Run(IReadOnlyDictionary<string, string> arguments, string repo, string output)
    {
        string[] paths = [arguments.GetValueOrDefault("august", Path.Combine(repo, "XER Sample/2608-EBA_PAA_8.0 (draft).xer")),
            arguments.GetValueOrDefault("july", "C:/Users/ricar/Downloads/2607 C5064-C-2607.xer")];
        var cache = new XerToCsvConverter.BrowserProcessing.ParsedSourceCache();
        var evidence = new List<CacheSourceEvidence>();
        for (int index = 0; index < paths.Length; index++)
        {
            byte[] bytes = File.ReadAllBytes(paths[index]);
            string name = Path.GetFileName(paths[index]);
            string hash = Parity.Hash(bytes);
            using var input = new MemoryStream(bytes, writable: false);
            var parse = Stopwatch.StartNew();
            XerDataStore raw = await new XerParser(64).ParseXerStreamAsync(input, name, null, default);
            parse.Stop();
            var estimate = Stopwatch.StartNew();
            long retainedBytes = XerToCsvConverter.BrowserProcessing.ParsedSourceCache.EstimateBytes(raw);
            estimate.Stop();
            XerTable[] tables = raw.TableNames.Select(table => raw.GetTable(table)!).ToArray();
            cache.Retain("source-" + index, hash, raw, default);
            evidence.Add(new(name, bytes.LongLength, hash, tables.Length, tables.Sum(table => (long)table.RowCount),
                tables.Sum(table => (long)table.Capacity), tables.Sum(table => (long)table.Capacity - table.RowCount),
                retainedBytes, retainedBytes <= cache.BudgetBytes, parse.Elapsed.TotalMilliseconds,
                estimate.Elapsed.TotalMilliseconds, cache.Count, cache.RetainedBytes));
        }
        var report = new { Purpose = "Conservative parsed-source cache estimates; not actual or peak heap measurements",
            CoreAssemblyVersion = typeof(XerParser).Assembly.GetName().Version!.ToString(), CacheBudgetBytes = cache.BudgetBytes,
            CombinedSourceEstimateBytes = evidence.Sum(source => source.EstimatedRetainedBytes),
            BothSourcesFitBudget = evidence.Sum(source => source.EstimatedRetainedBytes) <= cache.BudgetBytes, Sources = evidence };
        string text = JsonSerializer.Serialize(report, Parity.JsonOptions);
        File.WriteAllText(Path.Combine(output, "report.json"), text);
        Console.WriteLine(text);
    }
    private sealed record CacheSourceEvidence(string Name, long SourceBytes, string SourceSha256, int Tables, long Rows,
        long RowListCapacity, long UnusedRowSlots, long EstimatedRetainedBytes, bool FitsBudget,
        double NativeParseMs, double NativeEstimateMs, int CacheEntriesAfterRetention, long RetainedCacheEstimateAfterRetention);
}
#endif

internal static class Runner
{
    private static readonly DateTimeOffset ExportTime = new(2026, 9, 30, 0, 0, 0, TimeSpan.Zero);
    private static readonly string[] Enhanced =
    [
        EnhancedTableNames.XerTask01, EnhancedTableNames.XerProject02, EnhancedTableNames.XerProjWbs03,
        EnhancedTableNames.XerBaseline04, EnhancedTableNames.XerPredecessor06, EnhancedTableNames.XerActvType07,
        EnhancedTableNames.XerActvCode08, EnhancedTableNames.XerTaskActv09, EnhancedTableNames.XerCalendar10,
        EnhancedTableNames.XerCalendarDetailed11, EnhancedTableNames.XerRsrc12, EnhancedTableNames.XerTaskRsrc13,
        EnhancedTableNames.XerUmeasure14, EnhancedTableNames.XerResourceDist15
    ];

    internal static async Task<CaseResult> Run(CaseInput input, int iteration, string output)
    {
        Directory.CreateDirectory(output);
        using var zip = new MemoryStream();
#if PARSER_PHASE_TELEMETRY
        using var operation = ParserOperationTelemetry.BeginOperation();
#endif
        var timer = Stopwatch.StartNew();
        PhaseEvidence? phaseTiming = null;
        double? parseMs = null, exportMs = null;
        int warnings = 0;
        XerTable? quality = null;
        string? error = null;
        try
        {
            if (input.Mode == "programme")
            {
                var request = new ProgrammeReviewBundleRequest
                {
                    ProjectCode = "TEST", ProjectName = "Parser performance comparison", ProgrammeType = "C",
                    ParserVersion = "2.6.0", ExportedAtUtc = ExportTime,
                    Snapshots = input.Sources.Select((source, index) => new ProgrammeReviewSnapshot
                    {
                        OriginalXerFilename = source.Name,
                        SnapshotKind = index == 0 ? ProgrammeReviewSnapshotKind.Baseline : ProgrammeReviewSnapshotKind.Update,
                        SnapshotTag = index == 0 ? "BL01" : source.DataDate.ToString("yyMM"),
                        MonthUpdate = MonthEnd(source.DataDate), UpdateDate = MonthEnd(source.DataDate),
                        DataDate = source.DataDate
                    }).ToArray()
                };
                var result = await new ProgrammeReviewBundleService().BuildFromXerBytesToZipStreamAsync(request,
                    input.Sources.ToDictionary(source => source.Name, source => source.Bytes, StringComparer.Ordinal), zip);
                warnings = result.WarningCount;
                quality = result.DataQualityTable;
            }
            else if (input.Mode == "tender")
            {
                var request = new TenderReviewBundleRequest
                {
                    ProjectCode = "TEST", ProjectName = "Parser performance comparison", State = "NSW",
                    ParserVersion = "2.6.0", ExportedAtUtc = ExportTime,
                    Sources = input.Sources.Select((source, index) => new TenderReviewSource
                    {
                        SourceToken = TenderReviewNaming.CreateSourceToken(index), OriginalXerFilename = source.Name,
                        StatusDate = new DateOnly(2026, 9, 1).AddDays(index)
                    }).ToArray()
                };
                var bytes = request.Sources.Select((source, index) => new TenderReviewSourceBytes
                { SourceToken = source.SourceToken, Content = input.Sources[index].Bytes }).ToArray();
                var result = await new TenderReviewBundleService().BuildFromXerBytesToZipStreamAsync(request, bytes, zip);
                warnings = result.WarningCount;
                quality = result.DataQualityTable;
            }
            else
            {
                var service = new ProcessingService();
                using var stream = new MemoryStream(input.Sources[0].Bytes, writable: false);
                var store = await service.ParseXerStreamsAsync([(stream, input.Sources[0].Name)], null, CancellationToken.None);
                parseMs = timer.Elapsed.TotalMilliseconds;
                List<string> tables = input.Mode switch
                {
                    "raw" => store.TableNames.ToList(),
                    "selected" => [EnhancedTableNames.XerTask01, EnhancedTableNames.XerPredecessor06],
                    _ => Enhanced.ToList()
                };
                warnings = await service.ExportTablesToZipStreamAsync(store, tables, zip, null, CancellationToken.None);
                exportMs = timer.Elapsed.TotalMilliseconds - parseMs;
            }
        }
        catch (Exception exception)
        {
            error = exception.GetType().FullName + ": " + exception.Message;
        }
        timer.Stop();
#if PARSER_PHASE_TELEMETRY
        ParserOperationTimingSnapshot snapshot = operation.Snapshot();
        phaseTiming = new(snapshot.ElapsedMs, snapshot.HasOverlaps,
            snapshot.Phases.Select(phase => new PhaseTotalEvidence(phase.Phase.ToString(), phase.InclusiveMs,
                phase.ExclusiveMs, phase.Scopes, phase.BufferedCalls)).ToArray(),
            snapshot.Scopes.Select(scope => new PhaseScopeEvidence(scope.Phase.ToString(), scope.Detail,
                scope.StartMs, scope.EndMs, scope.InclusiveMs, scope.ExclusiveMs, scope.OverlappingChildren)).ToArray());
        operation.Dispose(); // Exclude extraction, output auditing and sidecar diagnostic CSV from operation timings.
#endif
        if (error is not null)
            return new(input.Name, input.Mode, iteration, "rejected", error, timer.Elapsed.TotalMilliseconds,
                parseMs, exportMs, zip.Length, warnings, [], null, [], input.Sources.Select(source => source.Sha256).ToArray(), false)
            { PhaseTiming = phaseTiming };

        zip.Position = 0;
        using var archive = new ZipArchive(zip, ZipArchiveMode.Read, leaveOpen: true);
        var files = new List<FileEvidence>();
        var content = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (ZipArchiveEntry entry in archive.Entries)
        {
            using var entryStream = entry.Open();
            using var extracted = new MemoryStream();
            entryStream.CopyTo(extracted);
            byte[] bytes = extracted.ToArray();
            string relativeName = Path.GetFileName(entry.FullName);
            if (!content.TryAdd(relativeName, bytes)) throw new InvalidDataException("Duplicate ZIP file: " + relativeName);
            File.WriteAllBytes(Path.Combine(output, relativeName), bytes);
            files.Add(Evidence(relativeName, bytes));
        }
        FileEvidence? diagnostics = null;
        if (quality is not null)
        {
            byte[] bytes = XerDataQuality.WriteToBytes(quality);
            File.WriteAllBytes(Path.Combine(output, XerDataQuality.FileName), bytes);
            diagnostics = Evidence(XerDataQuality.FileName, bytes);
        }
        else if (content.TryGetValue(XerDataQuality.FileName, out byte[]? diagnosticBytes))
        {
            diagnostics = Evidence(XerDataQuality.FileName, diagnosticBytes);
        }
        bool reconciled = ReconcileManifest(content);
        return new(input.Name, input.Mode, iteration, "complete", null, timer.Elapsed.TotalMilliseconds,
            parseMs, exportMs, zip.Length, warnings, files.OrderBy(file => file.Name, StringComparer.Ordinal).ToArray(),
            diagnostics, archive.Entries.Select(entry => Path.GetFileName(entry.FullName)).ToArray(),
            input.Sources.Select(source => source.Sha256).ToArray(), reconciled)
        { PhaseTiming = phaseTiming };
    }

    private static DateOnly MonthEnd(DateOnly date) => new(date.Year, date.Month, DateTime.DaysInMonth(date.Year, date.Month));
    private static FileEvidence Evidence(string name, byte[] bytes) => new(name, bytes.Length,
        ReadRows(bytes).LongCount() - 1, Parity.Hash(bytes), true);

    private static bool ReconcileManifest(IReadOnlyDictionary<string, byte[]> files)
    {
        if (!files.TryGetValue("XER_CSV_MANIFEST.csv", out byte[]? manifest)) return true;
        string[][] rows = ReadRows(manifest).ToArray();
        int nameIndex = Array.IndexOf(rows[0], "table_name"), hashIndex = Array.IndexOf(rows[0], "csv_sha256");
        int countIndex = Array.IndexOf(rows[0], "row_count");
        foreach (var table in rows.Skip(1).GroupBy(row => row[nameIndex], StringComparer.Ordinal))
        {
            byte[] csv = files[table.Key + ".csv"];
            if (table.Any(row => row[hashIndex] != Parity.Hash(csv))
                || table.Sum(row => long.Parse(row[countIndex])) != ReadRows(csv).LongCount() - 1)
                throw new InvalidDataException("Manifest does not reconcile: " + table.Key);
        }
        return true;
    }

    private static IEnumerable<string[]> ReadRows(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes, writable: false);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: false)
        { TextFieldType = FieldType.Delimited, HasFieldsEnclosedInQuotes = true, TrimWhiteSpace = false };
        parser.SetDelimiters(",");
        while (!parser.EndOfData) yield return parser.ReadFields()!;
    }
}

internal static class Parity
{
    internal static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };
    internal static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    internal static string? Difference(CaseResult before, CaseResult after)
    {
        if (before.Name != after.Name || before.Mode != after.Mode) return "Case identity differs";
        if (!before.SourceSha256.SequenceEqual(after.SourceSha256)) return "Source bytes differ";
        if (before.Status != after.Status || before.Error != after.Error) return "Acceptance/error differs";
        if (before.Status != "complete") return null;
        if (before.WarningCount != after.WarningCount) return "Warning count differs";
        if (!before.EntryOrder.SequenceEqual(after.EntryOrder)) return "ZIP entry order differs";
        if (!before.Files.SequenceEqual(after.Files)) return "Extracted CSV bytes/row counts/order/file set differ";
        if (before.Diagnostics != after.Diagnostics) return "Full diagnostic CSV differs";
        if (!before.ManifestReconciled || !after.ManifestReconciled) return "Manifest reconciliation failed";
        return null;
    }

    internal static void Compare(string beforePath, string afterPath, string output)
    {
        RunReport before = JsonSerializer.Deserialize<RunReport>(File.ReadAllText(beforePath))!;
        RunReport after = JsonSerializer.Deserialize<RunReport>(File.ReadAllText(afterPath))!;
        if (before.CoreAssemblyVersion != after.CoreAssemblyVersion || before.RequestParserVersion != after.RequestParserVersion)
            throw new InvalidDataException("Frozen/candidate assembly or requested parser versions differ.");
        var results = new List<object>();
        int failures = 0;
        foreach (var group in before.Cases.GroupBy(result => result.Name, StringComparer.Ordinal))
        {
            CaseResult baseline = group.OrderBy(result => result.Iteration).Last();
            CaseResult? candidate = after.Cases.Where(result => result.Name == baseline.Name).OrderBy(result => result.Iteration).LastOrDefault();
            string? difference = candidate is null ? "Candidate case missing" : Difference(baseline, candidate);
            if (difference is not null) failures++;
            results.Add(new { baseline.Name, Passed = difference is null, Difference = difference,
                BaselineMs = baseline.ElapsedMs, CandidateMs = candidate?.ElapsedMs,
                baseline.WarningCount, Files = baseline.Files.Count,
                DiagnosticSha256 = baseline.Diagnostics?.Sha256 });
        }
        if (after.Cases.Select(result => result.Name).Distinct().Count() != results.Count) failures++;
        File.WriteAllText(output, JsonSerializer.Serialize(new { before.Label, Candidate = after.Label,
            Cases = results.Count, Failures = failures, Results = results }, JsonOptions));
        Console.WriteLine($"Parity: {results.Count - failures}/{results.Count}; failures={failures}; report={output}");
        if (failures > 0) Environment.ExitCode = 1;
    }
}

internal static class Fixtures
{
    // Mutations reproduce recovery scenarios from ReviewDataQualityBundleTests,
    // HeaderValidationRegressionTests and TenderReviewBundleServiceTests. They retain
    // source omission/header distinctions and never encode a private real schedule.
    internal static IEnumerable<(string Name, byte[] Bytes)> Create(byte[] valid)
    {
        yield return ("valid", valid);
        var quality = new XerFixture(valid);
        quality.Set("TASK", 0, "status_code", "TK_Active");
        quality.Set("TASK", 0, "act_start_date", "2026-01-01 08:00");
        quality.Set("TASKRSRC", 0, "act_reg_qty", "5.12555");
        quality.Set("TASKRSRC", 0, "act_ot_qty", "0.125");
        quality.Set("TASKRSRC", 0, "act_start_date", "2026-02-03 08:00");
        quality.Set("TASKRSRC", 0, "restart_date", "2026-02-03 08:00");
        quality.Set("TASKRSRC", 0, "reend_date", "2026-02-06 17:00");
        quality.Set("TASKRSRC", 0, "remain_qty", "20");
        yield return ("quality-portions", quality.Bytes());
        var malformed = new XerFixture(valid);
        malformed.Set("TASK", 0, "early_start_date", "bad-date");
        malformed.Set("TASK", 0, "restart_date", "bad-date");
        malformed.Set("TASK", 0, "remain_drtn_hr_cnt", "bad-number");
        malformed.Set("TASK", 1, "wbs_id", "ORPHAN");
        malformed.Set("TASKPRED", 0, "lag_hr_cnt", "bad-number");
        malformed.Set("TASKRSRC", 0, "remain_qty", "-2");
        malformed.Set("TASKRSRC", 0, "act_reg_qty", "unknown");
        yield return ("malformed-values", malformed.Bytes());
        var missing = new XerFixture(valid);
        missing.RemoveColumn("TASK", "phys_complete_pct");
        missing.RemoveColumn("RSRC", "rsrc_type");
        missing.RemoveColumn("TASKRSRC", "remain_qty");
        yield return ("missing-headers", missing.Bytes());
        var truncated = new XerFixture(valid);
        truncated.Truncate("TASK", 0, 25);
        truncated.Truncate("TASKRSRC", 0, 5);
        yield return ("truncated-cells", truncated.Bytes());
        var unknown = new XerFixture(valid);
        unknown.Set("TASK", 0, "status_code", "TK_Unknown");
        unknown.Set("TASK", 0, "task_name", "  Activity, \"quoted\"  ");
        unknown.Set("TASK", 1, "task_code", "A100");
        yield return ("unknown-status-duplicate-code", unknown.Bytes());
        var invalidHeader = new XerFixture(valid);
        invalidHeader.DuplicateHeader("TASKRSRC", "remain_qty");
        yield return ("duplicate-header-rejected", invalidHeader.Bytes());
    }

    private sealed class XerFixture
    {
        private readonly List<string> _order = [];
        private readonly Dictionary<string, (List<string> Headers, List<List<string>> Rows)> _tables = new(StringComparer.OrdinalIgnoreCase);

        internal XerFixture(byte[] bytes)
        {
            string? current = null;
            foreach (string line in Encoding.UTF8.GetString(bytes).Split('\n'))
            {
                string[] fields = line.TrimEnd('\r').Split('\t');
                if (fields[0] == "%T") { current = fields[1]; _order.Add(current); }
                else if (fields[0] == "%F") _tables[current!] = (fields.Skip(1).ToList(), []);
                else if (fields[0] == "%R") _tables[current!].Rows.Add(fields.Skip(1).ToList());
            }
        }

        internal void Set(string table, int row, string column, string value)
        {
            int index = _tables[table].Headers.IndexOf(column);
            if (index < 0) throw new InvalidDataException("Fixture column missing: " + table + "." + column);
            _tables[table].Rows[row][index] = value;
        }
        internal void RemoveColumn(string table, string column)
        {
            int index = _tables[table].Headers.IndexOf(column);
            _tables[table].Headers.RemoveAt(index);
            foreach (var row in _tables[table].Rows) if (row.Count > index) row.RemoveAt(index);
        }
        internal void Truncate(string table, int row, int count) => _tables[table].Rows[row].RemoveRange(count, _tables[table].Rows[row].Count - count);
        internal void DuplicateHeader(string table, string column)
        {
            _tables[table].Headers.Add(column.ToUpperInvariant());
            foreach (var row in _tables[table].Rows) row.Add("conflicting-source-value");
        }
        internal byte[] Bytes()
        {
            var text = new StringBuilder();
            foreach (string name in _order)
            {
                text.Append("%T\t").Append(name).Append("\r\n%F\t").AppendJoin('\t', _tables[name].Headers).Append("\r\n");
                foreach (var row in _tables[name].Rows) text.Append("%R\t").AppendJoin('\t', row).Append("\r\n");
            }
            return Encoding.UTF8.GetBytes(text.Append("%E\r\n").ToString());
        }
    }
}
