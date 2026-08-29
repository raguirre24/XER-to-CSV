using XerToCsvConverter.ProgrammeReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ProgrammeReviewHistoryTests
{
    [Theory]
    [InlineData("2026-08-28", "2026-08-29", 1)] // Friday to Saturday: Athena counts Friday.
    [InlineData("2026-08-30", "2026-08-31", 0)] // Sunday to Monday: Athena excludes Sunday and end date.
    [InlineData("2026-08-31", "2026-09-01", 1)] // Monday to Tuesday.
    [InlineData("2026-08-29", "2026-08-31", 0)] // Weekend to Monday.
    [InlineData("2026-08-29", "2026-08-28", -1)] // Reverse sign uses the same min/max count.
    public void Weekday_variance_matches_Athena_endpoint_formula(string from, string to, long expected)
    {
        Assert.Equal(expected, ProgrammeReviewTransformer.CalculateWeekdayVariance(DateOnly.Parse(from), DateOnly.Parse(to)));
    }
}
