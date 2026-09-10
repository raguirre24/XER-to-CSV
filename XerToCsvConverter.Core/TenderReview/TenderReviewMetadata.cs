using System.Collections.ObjectModel;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace XerToCsvConverter.TenderReview;

/// <summary>One ordered Tender stage input. SourceToken is internal correlation state, not exported data.</summary>
public sealed record TenderReviewSource
{
    public required string SourceToken { get; init; }
    public required string OriginalXerFilename { get; init; }
    public string? XerFilePath { get; init; }
    public required DateOnly StatusDate { get; init; }
    public string? SourceSha256 { get; init; }
}

/// <summary>Caller-owned browser/CLI bytes correlated to a Tender source by its stable token.</summary>
public sealed record TenderReviewSourceBytes
{
    public required string SourceToken { get; init; }
    public required byte[] Content { get; init; }
}

public sealed record TenderReviewBundleRequest
{
    public required string ProjectCode { get; init; }
    public required string ProjectName { get; init; }
    public string? ParserVersion { get; init; }
    public DateTimeOffset? ExportedAtUtc { get; init; }
    public required IReadOnlyList<TenderReviewSource> Sources { get; init; }
}

public sealed record TenderReviewManifestRow(
    string SchemaVersion,
    string BundleProfile,
    string BundleId,
    string BundleStatus,
    string ParserVersion,
    string ProjectCode,
    string ProjectName,
    string OriginalXerFilename,
    string CanonicalXerFilename,
    DateOnly StatusDate,
    DateOnly UpdateDate,
    DateOnly DataDate,
    string SourceSha256,
    string TableName,
    long RowCount,
    string CsvSha256,
    DateTimeOffset ExportedAtUtc);

public sealed record TenderReviewBundleResult(
    string BundleId,
    string BundlePath,
    IReadOnlyList<TenderReviewManifestRow> ManifestRows,
    IReadOnlyDictionary<string, string> CsvSha256ByFile)
{
    public int WarningCount { get; init; }
    public XerTable? DataQualityTable { get; init; }
}

public sealed record TenderReviewInMemoryBundleResult(
    string BundleId,
    IReadOnlyDictionary<string, byte[]> Files,
    IReadOnlyList<TenderReviewManifestRow> ManifestRows,
    IReadOnlyDictionary<string, string> CsvSha256ByFile)
{
    public int WarningCount { get; init; }
    public XerTable? DataQualityTable { get; init; }
}

public sealed class TenderReviewValidationException : InvalidOperationException
{
    public TenderReviewValidationException(string message) : base(message) { }
    public TenderReviewValidationException(string message, Exception innerException) : base(message, innerException) { }
}

public static class TenderReviewDefaults
{
    /// <summary>Returns today's date on the supplied clock's local calendar without a UTC conversion.</summary>
    public static DateOnly GetLocalStatusDate(TimeProvider? timeProvider = null)
    {
        DateTimeOffset localNow = (timeProvider ?? TimeProvider.System).GetLocalNow();
        return DateOnly.FromDateTime(localNow.DateTime);
    }
}

internal sealed record ResolvedTenderReviewSource(
    TenderReviewSource Source,
    int InputIndex,
    string SourceToken,
    string OriginalXerFilename,
    string CanonicalXerFilename,
    DateOnly StatusDate,
    string SourceSha256,
    DateOnly? DataDate = null,
    string? NativeProjectId = null);

internal sealed record ResolvedTenderReviewRequest(
    string ProjectCode,
    string ProjectName,
    string ParserVersion,
    DateTimeOffset ExportedAtUtc,
    string BundleId,
    IReadOnlyList<ResolvedTenderReviewSource> Sources);

public static partial class TenderReviewNaming
{
    internal const string NamespaceDelimiter = "::";

    [GeneratedRegex("^[A-Za-z0-9-]+$", RegexOptions.CultureInvariant)]
    private static partial Regex SourceTokenRegex();

    [GeneratedRegex("^[a-fA-F0-9]{64}$", RegexOptions.CultureInvariant)]
    private static partial Regex Sha256Regex();

    /// <summary>Creates a deterministic opaque token from a zero-based input ordinal.</summary>
    public static string CreateSourceToken(int inputIndex)
    {
        if (inputIndex < 0) throw new ArgumentOutOfRangeException(nameof(inputIndex));
        return $"tender-source-{inputIndex + 1:D6}";
    }

    public static string CreateCanonicalFilename(string projectCode, DateOnly statusDate) =>
        $"{ReviewProjectIdentity.FileComponent(NormalizeProjectCode(projectCode))}-TENDER-{statusDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture)}.xer";

    public static string NamespaceKey(
        string nativeOrLegacyKey,
        string sourceToken,
        string projectCode,
        DateOnly statusDate)
    {
        if (string.IsNullOrWhiteSpace(nativeOrLegacyKey)) return string.Empty;
        ValidateSourceToken(sourceToken);

        string value = nativeOrLegacyKey.Trim();
        string prefix = sourceToken + ".";
        string nativeId;
        if (value.StartsWith(prefix, StringComparison.Ordinal))
        {
            nativeId = value[prefix.Length..];
        }
        else if (!value.Contains('.', StringComparison.Ordinal))
        {
            nativeId = value;
        }
        else
        {
            throw new TenderReviewValidationException(
                $"Key '{value}' does not belong to Tender source token '{sourceToken}'.");
        }

        nativeId = nativeId.Trim();
        if (nativeId.Length == 0) return string.Empty;
        if (nativeId.Contains(NamespaceDelimiter, StringComparison.Ordinal)
            || nativeId.Contains('|', StringComparison.Ordinal))
            throw new TenderReviewValidationException(
                $"Native key identifier '{nativeId}' contains a reserved Tender Review relationship-key delimiter.");

        return NamespacePrefix(projectCode, statusDate) + nativeId;
    }

    internal static string NamespacePrefix(string projectCode, DateOnly statusDate) =>
        $"CSV{NamespaceDelimiter}{ReviewProjectIdentity.EncodeComponent(NormalizeProjectCode(projectCode))}{NamespaceDelimiter}TENDER{NamespaceDelimiter}{statusDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture)}{NamespaceDelimiter}";

    internal static ResolvedTenderReviewRequest Resolve(
        TenderReviewBundleRequest request,
        Func<TenderReviewSource, string> sourceHashResolver)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(sourceHashResolver);
        if (request.Sources is null || request.Sources.Count == 0)
            throw new TenderReviewValidationException("At least one Tender XER source is required.");

        string projectCode = NormalizeProjectCode(request.ProjectCode);
        string projectName = request.ProjectName?.Trim() ?? string.Empty;
        if (projectName.Length == 0)
            throw new TenderReviewValidationException("Project name is required.");

        var tokens = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var statusDates = new HashSet<DateOnly>();
        var resolved = new List<ResolvedTenderReviewSource>(request.Sources.Count);
        for (int index = 0; index < request.Sources.Count; index++)
        {
            TenderReviewSource source = request.Sources[index]
                ?? throw new TenderReviewValidationException($"Tender source at input position {index + 1} is null.");
            string token = ValidateSourceToken(source.SourceToken);
            if (!tokens.Add(token))
                throw new TenderReviewValidationException($"Duplicate Tender source token '{token}'.");

            ValidateMetadataDate(source.StatusDate, "status_date", source.OriginalXerFilename);
            if (!statusDates.Add(source.StatusDate))
                throw new TenderReviewValidationException(
                    $"Duplicate Tender stage identity ({projectCode}, {source.StatusDate:yyyy-MM-dd}). Each source must have a unique status_date.");

            string original = Path.GetFileName(source.OriginalXerFilename?.Trim()) ?? string.Empty;
            if (string.IsNullOrWhiteSpace(original) || !original.EndsWith(".xer", StringComparison.OrdinalIgnoreCase))
                throw new TenderReviewValidationException("Each original_xer_filename must be a .xer file name, not a directory path.");
            if (!string.Equals(original, source.OriginalXerFilename?.Trim(), StringComparison.Ordinal))
                throw new TenderReviewValidationException(
                    $"Original XER filename '{source.OriginalXerFilename}' must not contain a directory path.");

            string hash = sourceHashResolver(source).Trim().ToLowerInvariant();
            if (!Sha256Regex().IsMatch(hash))
                throw new TenderReviewValidationException(
                    $"Source SHA-256 for Tender input {index + 1} ('{original}') must contain exactly 64 hexadecimal characters.");

            resolved.Add(new ResolvedTenderReviewSource(
                source,
                index,
                token,
                original,
                CreateCanonicalFilename(projectCode, source.StatusDate),
                source.StatusDate,
                hash));
        }

        DateTimeOffset exportedAt = (request.ExportedAtUtc ?? DateTimeOffset.UtcNow).ToUniversalTime();
        exportedAt = new DateTimeOffset(
            exportedAt.Year, exportedAt.Month, exportedAt.Day,
            exportedAt.Hour, exportedAt.Minute, exportedAt.Second, TimeSpan.Zero);
        string parserVersion = string.IsNullOrWhiteSpace(request.ParserVersion)
            ? typeof(TenderReviewContract).Assembly.GetName().Version?.ToString(3) ?? "unknown"
            : request.ParserVersion.Trim();

        string identity = string.Join("\n", resolved.Select(source => string.Join('|',
            source.InputIndex.ToString(CultureInfo.InvariantCulture),
            ReviewProjectIdentity.EncodeComponent(projectCode),
            source.OriginalXerFilename,
            source.CanonicalXerFilename,
            source.StatusDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            source.SourceSha256)));
        string bundleHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(identity)))
            .ToLowerInvariant()[..8];
        string exportedAtId = exportedAt.ToString(
            "yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        string bundleId = $"{ReviewProjectIdentity.FileComponent(projectCode)}_TENDER_{exportedAtId}_{bundleHash}";

        return new ResolvedTenderReviewRequest(
            projectCode,
            projectName,
            parserVersion,
            exportedAt,
            bundleId,
            new ReadOnlyCollection<ResolvedTenderReviewSource>(resolved));
    }

    public static string NormalizeProjectCode(string? value)
    {
        try { return ReviewProjectIdentity.NormalizeCode(value); }
        catch (ArgumentException ex) { throw new TenderReviewValidationException("Project code is required.", ex); }
    }

    /// <summary>
    /// Tender governance treats C&lt;digits&gt; and J&lt;digits&gt; as aliases. A digits-only code is an
    /// independent exact identity, and all other valid codes compare exactly.
    /// </summary>
    public static bool IsSameProjectIdentity(string first, string second)
    {
        string left = NormalizeProjectCode(first);
        string right = NormalizeProjectCode(second);
        if (string.Equals(left, right, StringComparison.Ordinal)) return true;
        return IsPrefixedNumericCode(left, out string leftDigits)
            && IsPrefixedNumericCode(right, out string rightDigits)
            && string.Equals(leftDigits, rightDigits, StringComparison.Ordinal);
    }

    internal static string ValidateSourceToken(string? value)
    {
        string supplied = value ?? string.Empty;
        string token = supplied.Trim();
        if (!string.Equals(supplied, token, StringComparison.Ordinal)
            || !SourceTokenRegex().IsMatch(token))
            throw new TenderReviewValidationException(
                "Tender source token must contain only ASCII letters, digits, or hyphens, with no surrounding whitespace.");
        return token;
    }

    internal static void ValidateMetadataDate(DateOnly value, string field, string? filename)
    {
        if (value.Year is <= 1900 or >= 2200)
            throw new TenderReviewValidationException(
                $"{filename ?? "Tender source"}: {field} '{value:yyyy-MM-dd}' is outside the supported 1901-2199 range.");
    }

    private static bool IsPrefixedNumericCode(string value, out string digits)
    {
        digits = string.Empty;
        if (value.Length < 2 || value[0] is not ('C' or 'J')) return false;
        ReadOnlySpan<char> suffix = value.AsSpan(1);
        if (!suffix.ContainsAnyExceptInRange('0', '9'))
        {
            digits = suffix.ToString();
            return true;
        }
        return false;
    }
}
