using Xunit;

namespace XerToCsvConverter.Core.Tests;

public sealed class IndexedCalendarDifferentialTests
{
    private static readonly DateTime Monday = new(2026, 8, 31);

    [Fact]
    public void Indexed_clock_matches_frozen_daily_engine_for_seeded_calendars_and_signed_requests()
    {
        var random = new Random(930026);
        for (int calendar = 0; calendar < 48; calendar++)
        {
            var week = Enumerable.Range(0, 7).Select(_ => RandomSlots(random)).ToArray();
            var exceptions = new Dictionary<DateTime, IReadOnlyList<P6WorkInterval>>();
            for (int day = -20; day <= 20; day++)
                if (random.Next(4) == 0) exceptions.Add(Monday.AddDays(day), RandomSlots(random));
            var pair = Create(week, exceptions);
            if (calendar % 3 == 0)
                pair = Suspend(pair, Monday.AddDays(-4).AddHours(14), Monday.AddDays(3).AddHours(9));
            for (int example = 0; example < 100; example++)
            {
                DateTime start = Monday.AddMinutes(random.Next(-20 * 1440, 20 * 1440)).AddTicks(random.Next(-3, 4));
                DateTime finish = Monday.AddMinutes(random.Next(-40 * 1440, 40 * 1440)).AddTicks(random.Next(-3, 4));
                Assert.Equal(pair.Old.CountWorkingHours(start, finish), pair.New.CountWorkingHours(start, finish));
                Assert.Equal(pair.Old.IsWorkingDay(start), pair.New.IsWorkingDay(start));
                decimal hours = random.Next(-1600, 1601) / 8m;
                EqualProjection(() => pair.Old.AddWorkingHours(start, hours), () => pair.New.AddWorkingHours(start, hours));
                long ticks = random.NextInt64(-200 * TimeSpan.TicksPerHour, 200 * TimeSpan.TicksPerHour);
                EqualProjection(() => pair.Old.AddWorkingTicks(start, ticks), () => pair.New.AddWorkingTicks(start, ticks));
            }
        }
    }

    [Fact]
    public void Shift_boundary_ties_and_fractional_ticks_match_in_both_directions_and_date_kinds()
    {
        var week = Enumerable.Range(0, 7).Select(day => (IReadOnlyList<P6WorkInterval>)(day is >= 1 and <= 5
            ? new[] { Slot(8, 12), Slot(13, 17) } : Array.Empty<P6WorkInterval>())).ToArray();
        var pair = Create(week, new Dictionary<DateTime, IReadOnlyList<P6WorkInterval>>
        {
            [Monday.AddDays(1)] = [], [Monday.AddDays(2)] = [Slot(10, 11), Slot(14, 16)]
        });
        decimal[] fractionalTicks = [0, 0.000000000000000001m, 0.25m, 0.5m, 0.75m, 1, 1.25m, 1.5m];
        foreach (DateTimeKind kind in Enum.GetValues<DateTimeKind>())
        foreach (int day in new[] { 0, 1, 2, 4, 5 })
        foreach (int hour in new[] { 0, 8, 10, 11, 12, 13, 14, 16, 17, 23 })
        foreach (int tickOffset in new[] { -1, 0, 1 })
        {
            DateTime start = DateTime.SpecifyKind(Monday.AddDays(day).AddHours(hour).AddTicks(tickOffset), kind);
            foreach (decimal ticks in fractionalTicks)
            foreach (int sign in new[] { -1, 1 })
            {
                foreach (decimal wholeHours in new[] { 0, 1, 4, 8 })
                {
                    decimal hours = sign * (wholeHours + ticks / TimeSpan.TicksPerHour);
                    EqualProjection(() => pair.Old.AddWorkingHours(start, hours), () => pair.New.AddWorkingHours(start, hours));
                }
                EqualProjection(() => pair.Old.AddWorkingTicks(start, sign), () => pair.New.AddWorkingTicks(start, sign));
            }
        }
    }

    [Fact]
    public void Exception_only_and_suspended_clocks_preserve_exact_exhaustion_errors()
    {
        IReadOnlyList<P6WorkInterval>[] noWeek = Enumerable.Range(0, 7).Select(_ => (IReadOnlyList<P6WorkInterval>)Array.Empty<P6WorkInterval>()).ToArray();
        var pair = Create(noWeek, new Dictionary<DateTime, IReadOnlyList<P6WorkInterval>>
        {
            [Monday.AddDays(-3)] = [Slot(22, 26)], [Monday] = [Slot(8, 12)],
            [Monday.AddDays(3)] = [Slot(10, 11)]
        });
        foreach (bool suspended in new[] { false, true })
        {
            var current = suspended ? Suspend(pair, Monday.AddDays(-5), Monday.AddDays(5)) : pair;
            foreach (int day in new[] { -7, -3, -2, 0, 3, 4, 7 })
            foreach (int hour in new[] { 0, 8, 12, 23 })
            foreach (decimal amount in new[] { -20m, -5m, -1m, 0, 1, 4, 5, 20 })
            {
                DateTime start = Monday.AddDays(day).AddHours(hour);
                EqualProjection(() => current.Old.AddWorkingHours(start, amount), () => current.New.AddWorkingHours(start, amount));
            }
        }
        EqualProjection(() => pair.Old.WithNonWorkingPeriod(Monday, Monday.AddDays(-1)).AddWorkingHours(Monday, 1),
            () => pair.New.WithNonWorkingPeriod(Monday, Monday.AddDays(-1)).AddWorkingHours(Monday, 1));
    }

    [Fact]
    public void Projection_horizon_and_representable_date_limits_match_daily_engine()
    {
        var fullWeek = Enumerable.Range(0, 7).Select(_ => (IReadOnlyList<P6WorkInterval>)new[] { Slot(0, 24) }).ToArray();
        var pair = Create(fullWeek, new Dictionary<DateTime, IReadOnlyList<P6WorkInterval>>());
        foreach (DateTime start in new[] { DateTime.MinValue, DateTime.MinValue.AddTicks(1), DateTime.MaxValue.AddTicks(-1), DateTime.MaxValue })
        foreach (long ticks in new[] { -TimeSpan.TicksPerDay, -1L, 0, 1, TimeSpan.TicksPerDay, long.MinValue, long.MaxValue })
            EqualProjection(() => pair.Old.AddWorkingTicks(start, ticks), () => pair.New.AddWorkingTicks(start, ticks));
        EqualProjection(() => pair.Old.AddWorkingHours(Monday, decimal.MinValue), () => pair.New.AddWorkingHours(Monday, decimal.MinValue));
        IReadOnlyList<P6WorkInterval>[] emptyWeek = Enumerable.Range(0, 7).Select(_ => (IReadOnlyList<P6WorkInterval>)Array.Empty<P6WorkInterval>()).ToArray();
        var sparseLimits = Create(emptyWeek, new Dictionary<DateTime, IReadOnlyList<P6WorkInterval>>
        {
            [DateTime.MinValue.Date] = [Slot(0, 24)], [DateTime.MaxValue.Date] = [Slot(0, 24)]
        });
        foreach (DateTime start in new[] { DateTime.MinValue, DateTime.MaxValue })
        foreach (decimal amount in new[] { -48m, -24m, -1m, 0, 1, 24, 48 })
            EqualProjection(() => sparseLimits.Old.AddWorkingHours(start, amount), () => sparseLimits.New.AddWorkingHours(start, amount));
        // Includes the original start date and the date exactly 366,000 days away.
        foreach (int sign in new[] { -1, 1 })
        {
            DateTime start = new(5000, 1, 1);
            decimal exactHours = 366_000m * 24;
            foreach (decimal amount in new[] { exactHours, exactHours + 24, exactHours + 24 + 1m / TimeSpan.TicksPerHour })
                EqualProjection(() => pair.Old.AddWorkingHours(start, sign * amount), () => pair.New.AddWorkingHours(start, sign * amount));
            IReadOnlyList<P6WorkInterval>[] noWeek = Enumerable.Range(0, 7).Select(_ => (IReadOnlyList<P6WorkInterval>)Array.Empty<P6WorkInterval>()).ToArray();
            foreach (int distance in new[] { 366_000, 366_001 })
            {
                var sparse = Create(noWeek, new Dictionary<DateTime, IReadOnlyList<P6WorkInterval>>
                {
                    [start.AddDays(sign * distance)] = [Slot(8, 9)]
                });
                EqualProjection(() => sparse.Old.AddWorkingHours(start, sign * 2), () => sparse.New.AddWorkingHours(start, sign * 2));
            }
        }
    }

    [Fact]
    public void Counting_full_date_span_and_same_day_suspension_preserve_ticks_and_identity()
    {
        var week = Enumerable.Range(0, 7).Select(_ => (IReadOnlyList<P6WorkInterval>)new[] { Slot(0, 24) }).ToArray();
        var pair = Create(week, new Dictionary<DateTime, IReadOnlyList<P6WorkInterval>>());
        Assert.Equal((DateTime.MaxValue.Ticks - DateTime.MinValue.Ticks) / (decimal)TimeSpan.TicksPerHour,
            pair.New.CountWorkingHours(DateTime.MinValue, DateTime.MaxValue));
        Assert.Same(pair.New, pair.New.WithNonWorkingPeriod(Monday.AddHours(8), Monday.AddHours(17)));
        var overlay = Suspend(pair, Monday, Monday.AddDays(3));
        Assert.Equal(pair.Old.CountWorkingHours(Monday.AddDays(-1), Monday.AddDays(5)) - 72,
            overlay.New.CountWorkingHours(Monday.AddDays(-1), Monday.AddDays(5)));
        Assert.Equal(144m, pair.New.CountWorkingHours(Monday.AddDays(-1), Monday.AddDays(5)));
    }

    [Fact]
    public void Canceled_requests_are_observed_even_on_warm_index_and_zero_work()
    {
        var pair = Create(Enumerable.Range(0, 7).Select(_ => (IReadOnlyList<P6WorkInterval>)new[] { Slot(8, 17) }).ToArray(),
            new Dictionary<DateTime, IReadOnlyList<P6WorkInterval>>());
        _ = pair.New.CountWorkingHours(Monday, Monday.AddDays(1));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        foreach (int amount in new[] { -1, 0, 1 })
        {
            EqualProjection(() => pair.Old.AddWorkingHours(Monday, amount, cancellation.Token),
                () => pair.New.AddWorkingHours(Monday, amount, cancellation.Token));
            EqualProjection(() => pair.Old.AddWorkingTicks(Monday, amount, cancellation.Token),
                () => pair.New.AddWorkingTicks(Monday, amount, cancellation.Token));
        }
        Assert.Throws<OperationCanceledException>(() => pair.New.CountWorkingHours(Monday, Monday, cancellation.Token));
    }

    private static IReadOnlyList<P6WorkInterval> RandomSlots(Random random) => random.Next(6) switch
    {
        0 => [], 1 => [Slot(0, 24)], 2 => [Slot(8, 12), Slot(13, 17)],
        3 => [Slot(22, 26)], 4 => [Slot(10, 11), Slot(15, 16)],
        _ => [Slot(7, 11), Slot(10, 15)]
    };

    private static P6WorkInterval Slot(int start, int finish) => new(TimeSpan.FromHours(start), TimeSpan.FromHours(finish));

    private static (CalendarDailyScanOracle Old, WorkingDayCalculator New) Create(
        IReadOnlyList<IReadOnlyList<P6WorkInterval>> rawWeek,
        IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> rawExceptions)
    {
        var resolved = P6CalendarNormalization.Resolve(rawWeek, rawExceptions);
        return (new CalendarDailyScanOracle(resolved.Week, resolved.Exceptions), new WorkingDayCalculator(resolved.Week, resolved.Exceptions));
    }

    private static (CalendarDailyScanOracle Old, WorkingDayCalculator New) Suspend(
        (CalendarDailyScanOracle Old, WorkingDayCalculator New) pair, DateTime start, DateTime finish) =>
        (pair.Old.WithNonWorkingPeriod(start, finish), pair.New.WithNonWorkingPeriod(start, finish));

    private static void EqualProjection(Func<DateTime> oldProjection, Func<DateTime> newProjection)
    {
        DateTime expected = default;
        Exception? oldError = Record.Exception(() => { expected = oldProjection(); });
        if (oldError is not null)
        {
            Exception? newError = Record.Exception(() => newProjection());
            Assert.NotNull(newError);
            Assert.Equal(oldError.GetType(), newError.GetType());
            Assert.Equal(oldError.Message, newError.Message);
        }
        else
        {
            DateTime actual = newProjection();
            Assert.Equal(expected.Ticks, actual.Ticks);
            Assert.Equal(expected.Kind, actual.Kind);
            Assert.Equal(expected.ToBinary(), actual.ToBinary());
        }
    }
}
