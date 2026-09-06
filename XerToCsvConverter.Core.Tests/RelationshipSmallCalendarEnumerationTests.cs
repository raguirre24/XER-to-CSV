namespace XerToCsvConverter.Core.Tests;

public sealed class RelationshipSmallCalendarEnumerationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void Exhaustive_small_calendar_candidate_enumeration_agrees_with_the_solver_at_minute_resolution(
        bool predecessorIsStart, bool sparseLagCalendar)
    {
        // Independently enumerate every candidate displacement; the oracle does not call
        // production CountWorkingHours/AddWorkingHours or invert a production result.
        DateTime anchor = new(2026, 1, 5, 9, 1, 0);
        DateTime holiday = new(2026, 1, 6);
        var predecessor = Calculator([(540, 543), (545, 547)], holiday);
        var lag = sparseLagCalendar ? Calculator([(541, 544), (546, 549)], holiday) : Calculator([(0, 1440)], null);
        DateTime[] predecessorMinutes = Minutes([(540, 543), (545, 547)], holiday);
        DateTime[] lagMinutes = sparseLagCalendar ? Minutes([(541, 544), (546, 549)], holiday) : Minutes([(0, 1440)], null);

        foreach (int signedLag in new[] { -3, -2, -1, 0, 1, 2, 3 })
        foreach (int deadlineMinute in new[] { -2, -1, 0, 1, 2, 4, 6, 9 })
        {
            DateTime deadline = anchor.AddMinutes(deadlineMinute);
            int[] feasible = Enumerable.Range(-16, 33).Where(candidate =>
                Add(lagMinutes, Move(predecessorMinutes, anchor, candidate, predecessorIsStart), signedLag) <= deadline).ToArray();
            Assert.NotEmpty(feasible);
            int oracle = feasible.Max();
            Assert.InRange(oracle, -15, 15); // The exhaustive window genuinely brackets the maximum.
            Assert.True(Add(lagMinutes, Move(predecessorMinutes, anchor, oracle + 1, predecessorIsStart), signedLag) > deadline);

            decimal actual = RelationshipFreeFloatCalculator.CalculateHours(predecessor, lag, anchor,
                deadline, signedLag / 60m, predecessorIsStart);
            long ticks = (long)decimal.Round(actual * TimeSpan.TicksPerHour, 0);
            long actualWholeMinutes = (long)decimal.Floor((decimal)ticks / TimeSpan.TicksPerMinute);
            Assert.True(oracle == actualWholeMinutes,
                $"start={predecessorIsStart}, sparseLag={sparseLagCalendar}, lag={signedLag}, deadline={deadline:O}, " +
                $"enumerated={oracle} minutes, solver={actual} hours");
        }
    }

    private static DateTime Move(DateTime[] workingMinutes, DateTime original, int displacement, bool isStart)
    {
        if (displacement == 0) return original;
        DateTime moved = Add(workingMinutes, original, displacement);
        int next = LowerBound(workingMinutes, moved);
        return isStart ? workingMinutes[next] : workingMinutes[next - 1].AddMinutes(1);
    }

    private static DateTime Add(DateTime[] workingMinutes, DateTime original, int minutes)
    {
        if (minutes == 0) return original;
        int next = LowerBound(workingMinutes, original);
        return minutes > 0 ? workingMinutes[next + minutes - 1].AddMinutes(1) : workingMinutes[next + minutes];
    }

    private static int LowerBound(DateTime[] values, DateTime value)
    {
        int found = Array.BinarySearch(values, value);
        return found < 0 ? ~found : found;
    }

    private static DateTime[] Minutes((int Start, int End)[] slots, DateTime? holiday)
    {
        DateTime first = new(2025, 12, 30);
        return Enumerable.Range(0, 14 * 1440).Select(minute => first.AddMinutes(minute))
            .Where(instant => instant.Date != holiday && slots.Any(slot =>
                instant.TimeOfDay.TotalMinutes >= slot.Start && instant.TimeOfDay.TotalMinutes < slot.End)).ToArray();
    }

    private static WorkingDayCalculator Calculator((int Start, int End)[] slots, DateTime? holiday)
    {
        var week = Enumerable.Range(0, 7).Select(_ => slots
            .Select(slot => (TimeSpan.FromMinutes(slot.Start), TimeSpan.FromMinutes(slot.End))).ToList()).ToArray();
        Dictionary<DateTime, List<(TimeSpan, TimeSpan)>> exceptions = [];
        if (holiday.HasValue) exceptions.Add(holiday.Value, []);
        return new WorkingDayCalculator([], exceptions,
            week.Select(day => day.Sum(slot => (decimal)(slot.Item2 - slot.Item1).TotalHours)).ToArray(), week);
    }
}
