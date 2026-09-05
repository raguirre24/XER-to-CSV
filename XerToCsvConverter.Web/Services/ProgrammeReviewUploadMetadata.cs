using System.Globalization;
using System.Text.RegularExpressions;

namespace XerToCsvConverter.Web.Services;

/// <summary>Filename suggestions only; the detected P6 Data Date is never inferred from a filename.</summary>
public static partial class ProgrammeReviewUploadMetadata
{
    // Canonical PROJECT-C/T-TAG_yyyyMMdd, plus the existing underscore/ISO-date editor convention.
    // Anchoring every field prevents project digits, dates, and arbitrary prose becoming YYMM tags.
    [GeneratedRegex(@"^[A-Z0-9_]+[-_][CT][-_](?<tag>BL[0-9]{2}(?:-[A-Z])?|[0-9]{4})_(?<date>[0-9]{8}|[0-9]{4}-[0-9]{2}-[0-9]{2})\.xer$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex GovernedFilename();

    public static ProgrammeReviewUploadSuggestion Infer(string filename, DateOnly? detectedDataDate)
    {
        Match match = GovernedFilename().Match(filename);
        if (!match.Success || !DateOnly.TryParseExact(
                match.Groups["date"].Value, new[] { "yyyyMMdd", "yyyy-MM-dd" },
                CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
            return new(string.Empty, string.Empty, string.Empty);

        string tag = match.Groups["tag"].Value.ToUpperInvariant();
        if (tag.StartsWith("BL", StringComparison.Ordinal))
            return new("Baseline", tag, detectedDataDate?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? string.Empty);

        int month = int.Parse(tag.AsSpan(2, 2), NumberStyles.None, CultureInfo.InvariantCulture);
        if (month is < 1 or > 12) return new(string.Empty, string.Empty, string.Empty);
        return new("Update", tag, $"20{tag[..2]}-{tag[2..]}-01");
    }
}

public sealed record ProgrammeReviewUploadSuggestion(string SnapshotKind, string SnapshotTag, string MonthUpdate);
