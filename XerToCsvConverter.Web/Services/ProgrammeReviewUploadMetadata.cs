using System.Globalization;
using System.Text.RegularExpressions;

namespace XerToCsvConverter.Web.Services;

/// <summary>Governed filename suggestions and a month default from the detected P6 Data Date.</summary>
public static partial class ProgrammeReviewUploadMetadata
{
    // Canonical PROJECT-C/T-TAG_yyyyMMdd, plus the existing underscore/ISO-date editor convention.
    // Anchoring every field prevents project digits, dates, and arbitrary prose becoming YYMM tags.
    [GeneratedRegex(@"^.+[-_][CT][-_](?<tag>BL[0-9]{2}(?:-[A-Z])?|[0-9]{4})_(?<date>[0-9]{8}|[0-9]{4}-[0-9]{2}-[0-9]{2})\.xer$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex GovernedFilename();

    public static ProgrammeReviewUploadSuggestion Infer(string filename, DateOnly? detectedDataDate)
    {
        string detectedMonth = detectedDataDate?.ToString("yyyy-MM", CultureInfo.InvariantCulture) ?? string.Empty;
        Match match = GovernedFilename().Match(filename);
        if (!match.Success || !DateOnly.TryParseExact(
                match.Groups["date"].Value, new[] { "yyyyMMdd", "yyyy-MM-dd" },
                CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
            return new(string.Empty, string.Empty, detectedMonth);

        string tag = match.Groups["tag"].Value.ToUpperInvariant();
        if (tag.StartsWith("BL", StringComparison.Ordinal))
            return new("Baseline", tag, detectedMonth);

        int month = int.Parse(tag.AsSpan(2, 2), NumberStyles.None, CultureInfo.InvariantCulture);
        if (month is < 1 or > 12) return new(string.Empty, string.Empty, detectedMonth);
        return new("Update", tag, detectedDataDate.HasValue ? detectedMonth : $"20{tag[..2]}-{tag[2..]}");
    }

    public static bool TryParseMonthUpdate(string? value, out DateOnly monthUpdate)
    {
        monthUpdate = default;
        return value is { Length: 7 } && DateOnly.TryParseExact(
            value + "-01", "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out monthUpdate);
    }

    public static bool HasUpdateTagMonthConflict(ProgrammeReviewUploadSuggestion suggestion)
    {
        if (suggestion.SnapshotKind != "Update" || suggestion.SnapshotTag.Length != 4
            || !TryParseMonthUpdate(suggestion.MonthUpdate, out DateOnly monthUpdate)
            || !int.TryParse(suggestion.SnapshotTag.AsSpan(0, 2), NumberStyles.None, CultureInfo.InvariantCulture, out int year)
            || !int.TryParse(suggestion.SnapshotTag.AsSpan(2, 2), NumberStyles.None, CultureInfo.InvariantCulture, out int month))
            return false;

        return monthUpdate.Year != 2000 + year || monthUpdate.Month != month;
    }
}

public sealed record ProgrammeReviewUploadSuggestion(string SnapshotKind, string SnapshotTag, string MonthUpdate);
