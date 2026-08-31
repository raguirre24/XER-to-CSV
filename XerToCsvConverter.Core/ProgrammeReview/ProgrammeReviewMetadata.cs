using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace XerToCsvConverter.ProgrammeReview;

public enum ProgrammeReviewSnapshotKind
{
    Baseline,
    Update
}

public sealed record ProgrammeReviewSnapshot
{
    public required string OriginalXerFilename { get; init; }
    public string? XerFilePath { get; init; }
    public required ProgrammeReviewSnapshotKind SnapshotKind { get; init; }
    public required string SnapshotTag { get; init; }
    public required DateOnly MonthUpdate { get; init; }
    public DateOnly? UpdateDate { get; init; }
    public required DateOnly DataDate { get; init; }
    public string? SourceSha256 { get; init; }
}

public sealed record ProgrammeReviewBundleRequest
{
    public required string ProjectCode { get; init; }
    public required string ProjectName { get; init; }
    public required string ProgrammeType { get; init; }
    public string? ParserVersion { get; init; }
    public DateTimeOffset? ExportedAtUtc { get; init; }
    public required IReadOnlyList<ProgrammeReviewSnapshot> Snapshots { get; init; }
}

public sealed record ProgrammeReviewManifestRow(
    string SchemaVersion,
    string BundleId,
    string BundleStatus,
    string ParserVersion,
    string ProjectCode,
    string ProjectName,
    string ProgrammeType,
    string OriginalXerFilename,
    string CanonicalXerFilename,
    string SnapshotKind,
    string SnapshotTag,
    DateOnly MonthUpdate,
    DateOnly UpdateDate,
    DateOnly DataDate,
    string SourceSha256,
    string TableName,
    long RowCount,
    string CsvSha256,
    DateTimeOffset ExportedAtUtc);

public sealed record ProgrammeReviewBundleResult(
    string BundleId,
    string BundlePath,
    IReadOnlyList<ProgrammeReviewManifestRow> ManifestRows,
    IReadOnlyDictionary<string, string> CsvSha256ByFile);

public sealed record ProgrammeReviewInMemoryBundleResult(
    string BundleId,
    IReadOnlyDictionary<string, byte[]> Files,
    IReadOnlyList<ProgrammeReviewManifestRow> ManifestRows,
    IReadOnlyDictionary<string, string> CsvSha256ByFile);

public sealed class ProgrammeReviewValidationException : InvalidOperationException
{
    public ProgrammeReviewValidationException(string message) : base(message) { }
    public ProgrammeReviewValidationException(string message, Exception innerException) : base(message, innerException) { }
}

internal sealed record ResolvedProgrammeReviewSnapshot(
    ProgrammeReviewSnapshot Source,
    string OriginalXerFilename,
    string CanonicalXerFilename,
    ProgrammeReviewSnapshotKind SnapshotKind,
    string SnapshotTag,
    DateOnly MonthUpdate,
    DateOnly EffectiveUpdateDate,
    DateOnly DataDate,
    string SourceSha256);

internal sealed record ResolvedProgrammeReviewRequest(
    string ProjectCode,
    string ProjectName,
    string ProgrammeType,
    string ParserVersion,
    DateTimeOffset ExportedAtUtc,
    string BundleId,
    IReadOnlyList<ResolvedProgrammeReviewSnapshot> Snapshots);

public static partial class ProgrammeReviewNaming
{
    internal const string NamespaceDelimiter = "::";

    [GeneratedRegex("^[A-Z0-9_]+$", RegexOptions.CultureInvariant)]
    private static partial Regex ProjectCodeRegex();

    [GeneratedRegex("^[0-9]{4}$", RegexOptions.CultureInvariant)]
    private static partial Regex UpdateTagRegex();

    [GeneratedRegex("^BL[0-9]{2}(?:-[A-Z])?$", RegexOptions.CultureInvariant)]
    private static partial Regex BaselineTagRegex();

    [GeneratedRegex("^[a-fA-F0-9]{64}$", RegexOptions.CultureInvariant)]
    private static partial Regex Sha256Regex();

    public static string CreateCanonicalFilename(
        string projectCode,
        string programmeType,
        string snapshotTag,
        DateOnly dataDate)
    {
        string project = NormalizeProjectCode(projectCode);
        string programme = NormalizeProgrammeType(programmeType);
        string tag = snapshotTag?.Trim().ToUpperInvariant()
            ?? throw new ProgrammeReviewValidationException("Snapshot tag is required.");

        return $"{project}-{programme}-{tag}_{dataDate:yyyyMMdd}.xer";
    }

    public static string NamespaceKey(
        string nativeOrLegacyKey,
        string originalXerFilename,
        string projectCode,
        string programmeType,
        string snapshotTag)
    {
        if (string.IsNullOrWhiteSpace(nativeOrLegacyKey)) return string.Empty;

        string namespacePrefix = NamespacePrefix(projectCode, programmeType, snapshotTag);

        string value = nativeOrLegacyKey.Trim();
        string prefix = originalXerFilename + ".";
        string nativeId;

        if (value.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            nativeId = value[prefix.Length..];
        }
        else if (!value.Contains('.', StringComparison.Ordinal))
        {
            nativeId = value;
        }
        else
        {
            throw new ProgrammeReviewValidationException(
                $"Key '{value}' does not belong to source XER '{originalXerFilename}'.");
        }

        nativeId = nativeId.Trim();
        if (nativeId.Length == 0) return string.Empty;
        if (nativeId.Contains(NamespaceDelimiter, StringComparison.Ordinal) || nativeId.Contains('|', StringComparison.Ordinal))
            throw new ProgrammeReviewValidationException(
                $"Native key identifier '{nativeId}' contains a reserved Programme Review relationship-key delimiter.");

        return namespacePrefix + nativeId;
    }

    internal static string NamespacePrefix(
        string projectCode,
        string programmeType,
        string snapshotTag)
    {
        string project = NormalizeProjectCode(projectCode);
        string programme = NormalizeProgrammeType(programmeType);
        string snapshot = snapshotTag?.Trim().ToUpperInvariant() ?? string.Empty;
        if (!UpdateTagRegex().IsMatch(snapshot) && !BaselineTagRegex().IsMatch(snapshot))
            throw new ProgrammeReviewValidationException(
                "Snapshot tag used in a relationship key must use YYMM, BLnn, or BLnn-A.");

        return $"CSV{NamespaceDelimiter}{project}{NamespaceDelimiter}{programme}{NamespaceDelimiter}{snapshot}{NamespaceDelimiter}";
    }

    internal static ResolvedProgrammeReviewRequest Resolve(
        ProgrammeReviewBundleRequest request,
        Func<ProgrammeReviewSnapshot, string> sourceHashResolver)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Snapshots is null || request.Snapshots.Count == 0)
            throw new ProgrammeReviewValidationException("At least one XER snapshot is required.");

        string projectCode = NormalizeProjectCode(request.ProjectCode);
        string programmeType = NormalizeProgrammeType(request.ProgrammeType);
        string projectName = request.ProjectName?.Trim() ?? string.Empty;
        if (projectName.Length == 0)
            throw new ProgrammeReviewValidationException("Project name is required.");

        if (!request.Snapshots.Any(s => s.SnapshotKind == ProgrammeReviewSnapshotKind.Baseline))
            throw new ProgrammeReviewValidationException("A Programme Review bundle must contain at least one baseline candidate.");

        var prelim = new List<(ProgrammeReviewSnapshot Source, string Original, string Canonical, string Tag, string Hash, int BaselineNumber, string? BaselineRevision)>();
        var originals = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var canonicals = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var originalsByHash = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (ProgrammeReviewSnapshot snapshot in request.Snapshots)
        {
            ValidateMetadataDate(snapshot.MonthUpdate, "monthupdate", snapshot.OriginalXerFilename);
            ValidateMetadataDate(snapshot.DataDate, "data_date", snapshot.OriginalXerFilename);
            if (snapshot.UpdateDate is { } suppliedUpdateDate)
                ValidateMetadataDate(suppliedUpdateDate, "update_date", snapshot.OriginalXerFilename);

            string original = Path.GetFileName(snapshot.OriginalXerFilename?.Trim()) ?? string.Empty;
            if (string.IsNullOrWhiteSpace(original) || !original.EndsWith(".xer", StringComparison.OrdinalIgnoreCase))
                throw new ProgrammeReviewValidationException("Each original_xer_filename must be a .xer file name, not a directory path.");
            if (!string.Equals(original, snapshot.OriginalXerFilename?.Trim(), StringComparison.Ordinal))
                throw new ProgrammeReviewValidationException($"Original XER filename '{snapshot.OriginalXerFilename}' must not contain a directory path.");
            if (!originals.Add(original))
                throw new ProgrammeReviewValidationException($"Duplicate original XER filename '{original}'.");

            string tag = snapshot.SnapshotTag?.Trim().ToUpperInvariant() ?? string.Empty;
            ValidateSnapshotTag(snapshot.SnapshotKind, tag, snapshot.MonthUpdate);
            string canonical = CreateCanonicalFilename(projectCode, programmeType, tag, snapshot.DataDate);
            if (!canonicals.Add(canonical))
                throw new ProgrammeReviewValidationException($"Duplicate canonical XER filename '{canonical}'.");

            string hash = sourceHashResolver(snapshot).Trim().ToLowerInvariant();
            if (!Sha256Regex().IsMatch(hash))
                throw new ProgrammeReviewValidationException($"Source SHA-256 for '{original}' must contain exactly 64 hexadecimal characters.");
            if (originalsByHash.TryGetValue(hash, out string? duplicateOriginal))
                throw new ProgrammeReviewValidationException(
                    $"XER files '{duplicateOriginal}' and '{original}' contain identical source content. " +
                    "Each history snapshot must be a distinct XER export.");
            originalsByHash.Add(hash, original);

            (int baselineNumber, string? baselineRevision) = snapshot.SnapshotKind == ProgrammeReviewSnapshotKind.Baseline
                ? ParseBaselineTag(tag)
                : (-1, null);
            prelim.Add((snapshot, original, canonical, tag, hash, baselineNumber, baselineRevision));
        }

        // Mirror AthenaScopeSql: retain the highest ranked baseline, then only updates after its MonthUpdate anchor.
        var selectedBaseline = prelim
            .Where(p => p.Source.SnapshotKind == ProgrammeReviewSnapshotKind.Baseline)
            .OrderByDescending(p => p.BaselineNumber)
            .ThenByDescending(p => p.BaselineRevision, StringComparer.Ordinal)
            .ThenByDescending(p => p.Source.MonthUpdate)
            .ThenByDescending(p => p.Canonical, StringComparer.Ordinal)
            .First();
        DateOnly anchorMonth = selectedBaseline.Source.MonthUpdate;
        prelim = prelim
            .Where(p => string.Equals(p.Original, selectedBaseline.Original, StringComparison.OrdinalIgnoreCase)
                || (p.Source.SnapshotKind == ProgrammeReviewSnapshotKind.Update && p.Source.MonthUpdate > anchorMonth))
            .ToList();
        string? duplicateTag = prelim.GroupBy(p => p.Tag, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault(g => g.Count() > 1)?.Key;
        if (duplicateTag is not null)
            throw new ProgrammeReviewValidationException($"Retained snapshots contain duplicate snapshot tag '{duplicateTag}'.");

        var occupiedUpdateDates = new HashSet<DateOnly>();
        foreach (var item in prelim.Where(p => p.Source.SnapshotKind == ProgrammeReviewSnapshotKind.Update))
        {
            DateOnly expected = MonthEndFromTag(item.Tag);
            if (item.Source.UpdateDate is { } supplied && supplied != expected)
                throw new ProgrammeReviewValidationException(
                    $"Update date for '{item.Original}' must be the tag month-end {expected:yyyy-MM-dd}.");
            if (!occupiedUpdateDates.Add(expected))
                throw new ProgrammeReviewValidationException($"More than one update resolves to {expected:yyyy-MM-dd}.");
        }

        var resolved = new List<ResolvedProgrammeReviewSnapshot>(prelim.Count);
        foreach (var item in prelim)
        {
            DateOnly effective;
            if (item.Source.SnapshotKind == ProgrammeReviewSnapshotKind.Update)
            {
                effective = MonthEndFromTag(item.Tag);
            }
            else
            {
                DateOnly seed = EndOfMonth(item.Source.MonthUpdate);
                if (item.Source.UpdateDate is { } supplied && supplied != seed)
                    throw new ProgrammeReviewValidationException(
                        $"Baseline update_date for '{item.Original}' must be omitted or equal MonthUpdate month-end {seed:yyyy-MM-dd}.");
                effective = seed;
                int shifts = 0;
                while (occupiedUpdateDates.Contains(effective) && shifts++ < 240)
                    effective = EndOfMonth(effective.AddMonths(-1));
                if (occupiedUpdateDates.Contains(effective))
                    throw new ProgrammeReviewValidationException("The baseline could not be assigned a non-conflicting update month within 240 months.");
            }

            resolved.Add(new ResolvedProgrammeReviewSnapshot(
                item.Source, item.Original, item.Canonical, item.Source.SnapshotKind, item.Tag,
                item.Source.MonthUpdate, effective, item.Source.DataDate, item.Hash));
        }

        DateTimeOffset exportedAt = (request.ExportedAtUtc ?? DateTimeOffset.UtcNow).ToUniversalTime();
        exportedAt = new DateTimeOffset(exportedAt.Year, exportedAt.Month, exportedAt.Day,
            exportedAt.Hour, exportedAt.Minute, exportedAt.Second, TimeSpan.Zero);
        string parserVersion = string.IsNullOrWhiteSpace(request.ParserVersion)
            ? typeof(ProgrammeReviewContract).Assembly.GetName().Version?.ToString(3) ?? "unknown"
            : request.ParserVersion.Trim();

        string identity = string.Join("\n", resolved.OrderBy(s => s.CanonicalXerFilename, StringComparer.Ordinal)
            .Select(s => $"{projectCode}|{programmeType}|{s.CanonicalXerFilename}|{s.SourceSha256}"));
        string bundleHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(identity))).ToLowerInvariant()[..8];
        string bundleId = $"{projectCode}_{programmeType}_{exportedAt:yyyyMMdd'T'HHmmss'Z'}_{bundleHash}";

        return new ResolvedProgrammeReviewRequest(projectCode, projectName, programmeType, parserVersion,
            exportedAt, bundleId, resolved.OrderBy(s => s.EffectiveUpdateDate).ThenBy(s => s.CanonicalXerFilename, StringComparer.Ordinal).ToArray());
    }

    internal static string NormalizeProjectCode(string? value)
    {
        string result = value?.Trim().ToUpperInvariant() ?? string.Empty;
        if (!ProjectCodeRegex().IsMatch(result))
            throw new ProgrammeReviewValidationException("Project code must contain only A-Z, 0-9, and underscore.");
        return result;
    }

    internal static string NormalizeProgrammeType(string? value)
    {
        string result = value?.Trim().ToUpperInvariant() ?? string.Empty;
        if (result is not ("C" or "T"))
            throw new ProgrammeReviewValidationException("Programme type must be C or T.");
        return result;
    }

    private static void ValidateSnapshotTag(ProgrammeReviewSnapshotKind kind, string tag, DateOnly monthUpdate)
    {
        if (kind == ProgrammeReviewSnapshotKind.Update)
        {
            if (!UpdateTagRegex().IsMatch(tag))
                throw new ProgrammeReviewValidationException("Update snapshot tags must use YYMM.");
            DateOnly expected = MonthEndFromTag(tag);
            if (monthUpdate.Year != expected.Year || monthUpdate.Month != expected.Month)
                throw new ProgrammeReviewValidationException(
                    $"MonthUpdate {monthUpdate:yyyy-MM-dd} does not match update tag '{tag}'.");
        }
        else if (!BaselineTagRegex().IsMatch(tag))
        {
            throw new ProgrammeReviewValidationException("Baseline snapshot tags must use BLnn or BLnn-A.");
        }
    }

    private static DateOnly MonthEndFromTag(string tag)
    {
        int year = 2000 + int.Parse(tag.AsSpan(0, 2), CultureInfo.InvariantCulture);
        int month = int.Parse(tag.AsSpan(2, 2), CultureInfo.InvariantCulture);
        if (month is < 1 or > 12)
            throw new ProgrammeReviewValidationException($"Update snapshot tag '{tag}' contains an invalid month.");
        return new DateOnly(year, month, DateTime.DaysInMonth(year, month));
    }

    internal static DateOnly EndOfMonth(DateOnly value) =>
        new(value.Year, value.Month, DateTime.DaysInMonth(value.Year, value.Month));

    private static (int Number, string? Revision) ParseBaselineTag(string tag)
    {
        int number = int.Parse(tag.AsSpan(2, 2), CultureInfo.InvariantCulture);
        string? revision = tag.Length == 6 ? tag[5].ToString() : null;
        return (number, revision);
    }

    private static void ValidateMetadataDate(DateOnly value, string field, string? filename)
    {
        if (value.Year is <= 1900 or >= 2200)
            throw new ProgrammeReviewValidationException(
                $"{filename ?? "Snapshot"}: {field} '{value:yyyy-MM-dd}' is outside the supported 1901-2199 range.");
    }
}
