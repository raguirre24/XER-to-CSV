namespace XerToCsvConverter.Core.Tests;

public sealed class RelationshipDelayAllowanceTests
{
    private static readonly DateTime Monday = new(2026, 9, 7);
    private const decimal OneTickHours = 1m / TimeSpan.TicksPerHour;

    [Theory]
    [InlineData(17, 17, 1)]
    [InlineData(9, 9, 7)]
    [InlineData(17, 0, -1)]
    public void Elapsed_lag_is_inverted_before_measuring_predecessor_delay(
        int predecessorHour, int successorHour, int expectedHours)
    {
        Assert.Equal(expectedHours, Calculate(Weekdays(), Continuous(),
            Monday.AddHours(predecessorHour), Monday.AddDays(1).AddHours(successorHour), 8));
    }

    [Fact]
    public void Negative_working_lag_includes_the_right_hand_nonworking_plateau()
    {
        Assert.Equal(20m, Calculate(Continuous(), Daily(), Monday.AddHours(12),
            Monday.AddHours(12), -4));
    }

    [Fact]
    public void Positive_working_lag_includes_the_right_hand_nonworking_plateau()
    {
        Assert.Equal(15m, Calculate(Continuous(), Weekdays(), Monday.AddHours(17),
            Monday.AddDays(1).AddHours(17), 8));
    }

    [Fact]
    public void Negative_lag_deadline_in_a_nonworking_gap_stops_before_the_jump()
    {
        Assert.Equal((24m * TimeSpan.TicksPerHour - 1) / TimeSpan.TicksPerHour,
            Calculate(Continuous(), Daily(), Monday.AddHours(12), Monday.AddHours(17), -4));
    }

    [Theory]
    [InlineData(false, 4)]
    [InlineData(true, 4)]
    public void Zero_lag_inside_a_shift_uses_predecessor_working_time(bool isStart, int expected)
    {
        Assert.Equal(expected, Calculate(Weekdays(), Continuous(), Monday.AddHours(8),
            Monday.AddHours(12), 0, isStart: isStart) + (isStart ? OneTickHours : 0));
    }

    [Fact]
    public void Delayed_start_at_shift_end_moves_to_the_next_start_boundary()
    {
        var pred = Weekdays();
        decimal result = Calculate(pred, Continuous(), Monday.AddHours(8), Monday.AddHours(17), 0,
            isStart: true);
        Assert.Equal((8m * TimeSpan.TicksPerHour - 1) / TimeSpan.TicksPerHour, result);
        Assert.True(pred.AddWorkingHours(Monday.AddHours(8), result) < Monday.AddHours(17));
    }

    [Fact]
    public void Finish_can_use_the_entire_shift_before_a_successor_nonworking_deadline()
    {
        Assert.Equal(8m, Calculate(Weekdays(), Continuous(), Monday.AddHours(8),
            Monday.AddHours(23), 0));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Exact_zero_keeps_the_original_event_even_at_an_unusual_boundary(bool isStart)
    {
        DateTime endpoint = Monday.AddHours(isStart ? 17 : 8);
        Assert.Equal(0m, Calculate(Weekdays(), Continuous(), endpoint, endpoint, 0, isStart));
    }

    [Fact]
    public void Violated_event_in_a_nonworking_gap_cannot_be_reported_as_zero()
    {
        Assert.Equal(-OneTickHours, Calculate(Weekdays(), Continuous(), Monday.AddHours(8),
            Monday.AddDays(-1).AddHours(12), 0));
    }

    [Fact]
    public void One_tick_positive_and_negative_allowances_are_not_rounded_to_zero()
    {
        var continuous = Continuous();
        Assert.Equal(OneTickHours, Calculate(continuous, continuous, Monday, Monday.AddTicks(1), 0));
        Assert.Equal(-OneTickHours, Calculate(continuous, continuous, Monday, Monday.AddTicks(-1), 0));
    }

    [Fact]
    public void Sub_tick_lag_is_quantised_once_without_creating_a_calendar_snap()
    {
        Assert.Equal(4m, Calculate(Continuous(), Weekdays(), Monday.AddHours(18),
            Monday.AddHours(22), OneTickHours / 4));
    }

    [Fact]
    public void Nonworking_exception_changes_the_latest_predecessor_endpoint()
    {
        var pred = Weekdays(new Dictionary<DateTime, List<(TimeSpan, TimeSpan)>>
            { [Monday.AddDays(1)] = [] });
        Assert.Equal(1m, Calculate(pred, Continuous(), Monday.AddHours(17),
            Monday.AddDays(2).AddHours(17), 8));
    }

    [Fact]
    public void Overnight_work_is_included_on_both_civil_dates()
    {
        var night = Calendar((day, hour) => day is >= 1 and <= 5 && hour == 22,
            [(22, 26)]);
        Assert.Equal(3m, Calculate(night, Continuous(), Monday.AddHours(23),
            Monday.AddDays(1).AddHours(4), 2));
    }

    [Fact]
    public void Cancellation_is_observed()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.Throws<OperationCanceledException>(() => RelationshipFreeFloatCalculator.CalculateHours(
            Weekdays(), Continuous(), Monday, Monday.AddDays(1), 0, false, cancellation.Token));
    }

    [Fact]
    public void No_work_calendar_does_not_fabricate_a_valid_allowance()
    {
        var noWork = Calendar((_, _) => false, []);
        Assert.Throws<InvalidDataException>(() => Calculate(noWork, Continuous(), Monday,
            Monday.AddDays(1), 0));
    }

    [Fact]
    public void Unprojectable_date_and_duration_extremes_fail_explicitly()
    {
        Assert.Throws<InvalidDataException>(() => Calculate(Continuous(), Continuous(),
            DateTime.MaxValue.AddHours(-1), DateTime.MaxValue, 0));
        Assert.Throws<OverflowException>(() => Calculate(Continuous(), Continuous(),
            Monday, Monday.AddDays(1), decimal.MaxValue));
    }

    [Fact]
    public void Discrete_minute_oracle_checks_both_lag_signs_and_endpoint_types()
    {
        // Independent minute-by-minute simulation: no production Count/Add calls are
        // used by the oracle. Compare the greatest feasible whole-minute movement;
        // production may additionally allow part of the following minute.
        var patterns = new[] { Pattern.Continuous, Pattern.SplitWeekdays, Pattern.Daily };
        foreach (var predPattern in patterns)
        foreach (var lagPattern in patterns)
        foreach (int lagMinutes in new[] { -480, -240, 0, 240, 480 })
        foreach (bool isStart in new[] { false, true })
        foreach (int anchorDay in new[] { 0, 4 })
        foreach (int deadlineHour in new[] { 9, 12, 18 })
        {
            DateTime from = Monday.AddDays(anchorDay).AddHours(10);
            DateTime deadline = Monday.AddDays(anchorDay == 0 ? 0 : 7).AddHours(deadlineHour);
            decimal actual = Calculate(ForPattern(predPattern), ForPattern(lagPattern), from,
                deadline, lagMinutes / 60m, isStart);
            long actualMinute = WholeMinutes(actual);
            DateTime candidate = ReferenceMove(predPattern, from, actualMinute, isStart);
            DateTime next = ReferenceMove(predPattern, from, actualMinute + 1, isStart);
            Assert.True(ReferenceAdd(lagPattern, candidate, lagMinutes) <= deadline,
                $"Not feasible: {predPattern}, {lagPattern}, lag {lagMinutes}, start {isStart}, {actual}");
            Assert.True(ReferenceAdd(lagPattern, next, lagMinutes) > deadline,
                $"Not maximal: {predPattern}, {lagPattern}, lag {lagMinutes}, start {isStart}, {actual}");
        }
    }

    [Fact]
    public void Seeded_random_oracle_checks_work_and_nonwork_anchors_deadlines_and_night_shifts()
    {
        var random = new Random(600613);
        for (int example = 0; example < 300; example++)
        {
            var predPattern = (Pattern)random.Next(5);
            var lagPattern = (Pattern)random.Next(5);
            DateTime from = Monday.AddMinutes(random.Next(7 * 1440));
            DateTime deadline = from.AddMinutes(random.Next(-1440, 4321));
            int lagMinutes = random.Next(-720, 721);
            bool isStart = random.Next(2) == 0;
            decimal actual = Calculate(ForPattern(predPattern), ForPattern(lagPattern), from,
                deadline, lagMinutes / 60m, isStart);
            long wholeMinute = WholeMinutes(actual);
            DateTime candidate = ReferenceMove(predPattern, from, wholeMinute, isStart);
            DateTime next = ReferenceMove(predPattern, from, wholeMinute + 1, isStart);
            string details = $"Example {example}: {predPattern}, {lagPattern}, {from:O}, {deadline:O}, "
                + $"lag {lagMinutes}, start {isStart}, result {actual}";
            Assert.True(ReferenceAdd(lagPattern, candidate, lagMinutes) <= deadline, details);
            Assert.True(ReferenceAdd(lagPattern, next, lagMinutes) > deadline, details);
        }
    }

    private enum Pattern { Continuous, SplitWeekdays, Daily, Night, ShortDay }

    private static long WholeMinutes(decimal hours) => (long)decimal.Floor(
        decimal.Round(hours * TimeSpan.TicksPerHour, 0) / TimeSpan.TicksPerMinute);

    private static DateTime ReferenceMove(Pattern pattern, DateTime endpoint, long minutes, bool isStart)
    {
        if (minutes == 0) return endpoint;
        DateTime moved = ReferenceAdd(pattern, endpoint, minutes);
        if (isStart)
            while (!ReferenceIsWork(pattern, moved)) moved = moved.AddMinutes(1);
        else
            while (!ReferenceIsWork(pattern, moved.AddMinutes(-1))) moved = moved.AddMinutes(-1);
        return moved;
    }

    private static DateTime ReferenceAdd(Pattern pattern, DateTime endpoint, long minutes)
    {
        DateTime cursor = endpoint;
        int direction = Math.Sign(minutes);
        long remaining = Math.Abs(minutes);
        while (remaining > 0)
        {
            DateTime occupiedMinute = direction > 0 ? cursor : cursor.AddMinutes(-1);
            if (ReferenceIsWork(pattern, occupiedMinute)) remaining--;
            cursor = cursor.AddMinutes(direction);
        }
        return cursor;
    }

    private static bool ReferenceIsWork(Pattern pattern, DateTime minute) => pattern switch
    {
        Pattern.Continuous => true,
        Pattern.Daily => minute.Hour is >= 8 and < 16,
        Pattern.Night => minute.Hour is >= 22 or < 2,
        Pattern.ShortDay => minute.Hour is >= 10 and < 12,
        _ => minute.DayOfWeek is >= DayOfWeek.Monday and <= DayOfWeek.Friday
            && (minute.Hour is >= 8 and < 12 or >= 13 and < 17)
    };

    private static WorkingDayCalculator ForPattern(Pattern pattern) => pattern switch
    {
        Pattern.Continuous => Continuous(), Pattern.Daily => Daily(),
        Pattern.Night => Calendar((_, _) => true, [(22, 26)]),
        Pattern.ShortDay => Calendar((_, _) => true, [(10, 12)]), _ => Weekdays()
    };

    private static decimal Calculate(WorkingDayCalculator pred, WorkingDayCalculator lag,
        DateTime from, DateTime to, decimal lagHours, bool isStart = false) =>
        RelationshipFreeFloatCalculator.CalculateHours(pred, lag, from, to, lagHours, isStart);

    private static WorkingDayCalculator Continuous() => Calendar((_, _) => true, [(0, 24)]);
    private static WorkingDayCalculator Daily() => Calendar((_, _) => true, [(8, 16)]);
    private static WorkingDayCalculator Weekdays(
        Dictionary<DateTime, List<(TimeSpan, TimeSpan)>>? exceptions = null) =>
        Calendar((day, _) => day is >= 1 and <= 5, [(8, 12), (13, 17)], exceptions);

    private static WorkingDayCalculator Calendar(Func<int, int, bool> includes,
        (int Start, int End)[] intervals,
        Dictionary<DateTime, List<(TimeSpan, TimeSpan)>>? exceptions = null)
    {
        var slots = Enumerable.Range(0, 7).Select(day => intervals
            .Where(slot => includes(day, slot.Start))
            .Select(slot => (TimeSpan.FromHours(slot.Start), TimeSpan.FromHours(slot.End))).ToList()).ToArray();
        return new WorkingDayCalculator([], exceptions ?? [],
            slots.Select(day => day.Sum(slot => (decimal)(slot.Item2 - slot.Item1).TotalHours)).ToArray(), slots);
    }
}
