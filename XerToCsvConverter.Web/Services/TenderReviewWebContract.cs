using System.Globalization;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Web.Services;

/// <summary>
/// Deterministic, UI-independent mapping for the Tender Review web contract.
/// </summary>
public static class TenderReviewWebContract
{
    public const string BrowserLocalDateJsIdentifier = "xerToCsv.getBrowserLocalIsoDate";
    private static readonly DateTimeOffset MinimumZipTimestamp =
        new(1980, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset MaximumZipTimestamp =
        new(2107, 12, 31, 23, 59, 58, TimeSpan.Zero);

    /// <summary>
    /// Captures the add-time browser value as a canonical ISO date. Callers store this value on the upload;
    /// changing profiles later must not recalculate it.
    /// </summary>
    public static string CaptureAddedLocalIsoDate(string? value) =>
        ParseBrowserLocalIsoDate(value).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    public static DateOnly ParseBrowserLocalIsoDate(string? value)
    {
        if (!DateOnly.TryParseExact(
                value,
                "yyyy-MM-dd",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out DateOnly date))
        {
            throw new ArgumentException(
                "The browser did not return a valid local calendar date in yyyy-MM-dd format.",
                nameof(value));
        }

        return date;
    }

    /// <summary>
    /// Converts a frozen UTC bundle instant to the DOS timestamp range and two-second precision used by ZIP entries.
    /// </summary>
    public static DateTimeOffset GetDeterministicZipEntryTimestamp(DateTimeOffset exportedAtUtc)
    {
        DateTimeOffset utc = exportedAtUtc.ToUniversalTime();
        if (utc <= MinimumZipTimestamp) return MinimumZipTimestamp;
        if (utc >= MaximumZipTimestamp) return MaximumZipTimestamp;

        int evenSecond = utc.Second - (utc.Second % 2);
        return new DateTimeOffset(
            utc.Year,
            utc.Month,
            utc.Day,
            utc.Hour,
            utc.Minute,
            evenSecond,
            TimeSpan.Zero);
    }

    public static TenderReviewSource[] CreateOrderedSources(
        IEnumerable<TenderReviewWebSourceInput> inputs)
    {
        ArgumentNullException.ThrowIfNull(inputs);

        var sources = new List<TenderReviewSource>();
        var sourceTokens = new HashSet<string>(StringComparer.Ordinal);
        var statusDates = new HashSet<DateOnly>();
        int index = 0;
        foreach (TenderReviewWebSourceInput input in inputs)
        {
            ArgumentNullException.ThrowIfNull(input);
            int stageNumber = index + 1;
            if (string.IsNullOrWhiteSpace(input.SourceToken))
                throw new ArgumentException($"Tender stage {stageNumber}: internal source identity is missing.");
            if (!sourceTokens.Add(input.SourceToken))
                throw new ArgumentException($"Tender stage {stageNumber}: internal source identity is duplicated.");
            if (string.IsNullOrWhiteSpace(input.OriginalXerFilename))
                throw new ArgumentException($"Tender stage {stageNumber}: original XER filename is required.");
            if (!DateOnly.TryParseExact(
                    input.StatusDate,
                    "yyyy-MM-dd",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out DateOnly statusDate))
            {
                throw new ArgumentException(
                    $"Tender stage {stageNumber} ({input.OriginalXerFilename}): Status date must use yyyy-MM-dd.");
            }
            if (!statusDates.Add(statusDate))
            {
                throw new ArgumentException(
                    $"Tender stage {stageNumber} ({input.OriginalXerFilename}): Status date {statusDate:yyyy-MM-dd} is already used by another Tender stage.");
            }

            sources.Add(new TenderReviewSource
            {
                SourceToken = input.SourceToken,
                OriginalXerFilename = input.OriginalXerFilename,
                StatusDate = statusDate
            });
            index++;
        }

        return sources.ToArray();
    }
}

public sealed record TenderReviewWebSourceInput(
    string SourceToken,
    string OriginalXerFilename,
    string StatusDate);
