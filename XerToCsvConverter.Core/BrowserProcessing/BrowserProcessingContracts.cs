using System.Text.Json.Serialization;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.BrowserProcessing;

// These messages stay inside the browser. Neither parsed tables nor input bytes
// cross the UI/worker boundary.
internal sealed record BrowserSource(string SourceToken, string Name, long Size, string? SourceSha256 = null);

internal sealed record BrowserDiagnosticField(string Name, string RawValue, XerRawFieldState State);
internal sealed record BrowserDiagnostic(string SourceToken, string SourceFilename, string OriginalSourceFilename,
    IReadOnlyList<string> Values, IReadOnlyList<BrowserDiagnosticField> RawFields);

internal sealed record BrowserJob
{
    public const int ProtocolVersion = 1;
    public int Protocol { get; init; } = ProtocolVersion;
    public string JobId { get; init; } = string.Empty;
    public long Generation { get; init; }
    public string Kind { get; init; } = string.Empty;
    public IReadOnlyList<BrowserSource> Sources { get; init; } = Array.Empty<BrowserSource>();
    public IReadOnlyList<string> Tables { get; init; } = Array.Empty<string>();
    public ProgrammeReviewBundleRequest? ProgrammeRequest { get; init; }
    public TenderReviewBundleRequest? TenderRequest { get; init; }
    public long? CacheBudgetBytes { get; init; }
}

internal sealed record BrowserResult
{
    public string JobId { get; init; } = string.Empty;
    public long Generation { get; init; }
    public string? DataDate { get; init; }
    public IReadOnlyList<string> MetadataWarnings { get; init; } = Array.Empty<string>();
    public string? SourceSha256 { get; init; }
    public IReadOnlyList<string> TableNames { get; init; } = Array.Empty<string>();
    public int WarningCount { get; init; }
    public int FileCount { get; init; }
    public string Filename { get; init; } = "XER_Export.zip";
    public string? BundleId { get; init; }
    public string? DownloadId { get; init; }
    public IReadOnlyList<string> RetainedSourceNames { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> DiagnosticMessages { get; init; } = Array.Empty<string>();
    public IReadOnlyList<BrowserDiagnostic> Diagnostics { get; init; } = Array.Empty<BrowserDiagnostic>();
    public string DiagnosticSha256 { get; init; } = string.Empty;
    public IReadOnlyDictionary<string, string> CsvSha256ByFile { get; init; } = new Dictionary<string, string>();
    public IReadOnlyDictionary<string, double> StageMilliseconds { get; init; } = new Dictionary<string, double>();
    public long RetainedCacheBytes { get; init; }
    public long CacheBudgetBytes { get; init; }
    public int CacheHits { get; init; }
    public int ParsedSources { get; init; }
    public long AllocatedBytes { get; init; }
    public long ManagedHeapBytes { get; init; }
    public long SampledManagedHeapHighWaterBytes { get; init; }
    public long ZipBytes { get; init; }
    public long ZipBufferCapacityBytes { get; init; }
    public int RetainedSourceCount { get; init; }
    [JsonIgnore] public MemoryStream? ZipContent { get; init; }
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true, IncludeFields = true)]
[JsonSerializable(typeof(BrowserJob))]
[JsonSerializable(typeof(BrowserResult))]
[JsonSerializable(typeof(ProcessingService.DetailedProgress))]
internal partial class BrowserJsonContext : JsonSerializerContext;
