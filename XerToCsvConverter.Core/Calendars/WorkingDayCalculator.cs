namespace XerToCsvConverter;

/// <summary>Exact working-time arithmetic over normalized half-open civil-day intervals.</summary>
public sealed class WorkingDayCalculator
{
    private const int MaximumProjectionDays = 366_000;
    private readonly IReadOnlyList<IReadOnlyList<P6WorkInterval>> _week;
    private readonly IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> _exceptions;
    private readonly bool _hasWeeklyWork;

    /// <summary>An explicit conventional calendar for callers that deliberately request one.</summary>
    public static WorkingDayCalculator Default { get; } = CreateDefault();

    // Retained for existing callers. Availability always comes from slots, never total-hour hints.
    public WorkingDayCalculator(Dictionary<DateTime, decimal> exceptionHours,
        Dictionary<DateTime, List<(TimeSpan Start, TimeSpan End)>> exceptionTimeSlots,
        decimal[] standardWeekHours, List<(TimeSpan Start, TimeSpan End)>[] standardWeekTimeSlots)
    {
        ArgumentNullException.ThrowIfNull(exceptionHours);
        ArgumentNullException.ThrowIfNull(exceptionTimeSlots);
        ArgumentNullException.ThrowIfNull(standardWeekHours);
        ArgumentNullException.ThrowIfNull(standardWeekTimeSlots);
        if (standardWeekHours.Length != 7 || standardWeekTimeSlots.Length != 7)
            throw new ArgumentException("A calendar must provide seven weekdays.");
        var week = standardWeekTimeSlots.Select(slots => (IReadOnlyList<P6WorkInterval>)
            (slots ?? []).Select(ConvertSlot).ToArray()).ToArray();
        var exceptions = exceptionTimeSlots.ToDictionary(pair => pair.Key.Date,
            pair => (IReadOnlyList<P6WorkInterval>)pair.Value.Select(ConvertSlot).ToArray());
        for (int day = 0; day < 7; day++)
            if (standardWeekHours[day] > 0 && week[day].Count == 0)
                throw new InvalidDataException("Positive calendar hours require explicit working intervals.");
        foreach (var pair in exceptionHours)
        {
            if (!exceptions.ContainsKey(pair.Key.Date))
            {
                if (pair.Value > 0)
                    throw new InvalidDataException("Positive exception hours require explicit working intervals.");
                exceptions.Add(pair.Key.Date, Array.Empty<P6WorkInterval>());
            }
        }
        (_week, _exceptions) = P6CalendarNormalization.Resolve(week, exceptions);
        _hasWeeklyWork = _week.Any(day => day.Count > 0);
    }

    internal WorkingDayCalculator(IReadOnlyList<IReadOnlyList<P6WorkInterval>> week,
        IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> exceptions)
    {
        _week = week;
        _exceptions = exceptions;
        _hasWeeklyWork = week.Any(day => day.Count > 0);
    }

    public bool IsWorkingDay(DateTime date) => GetIntervals(date.Date).Count > 0;

    public decimal CountWorkingHours(DateTime startDate, DateTime endDate,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (startDate == endDate) return 0;
        if (endDate < startDate) return -CountWorkingHours(endDate, startDate, cancellationToken);
        decimal ticks = 0;
        DateTime day = startDate.Date;
        while (day <= endDate.Date)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long lower = day == startDate.Date ? startDate.TimeOfDay.Ticks : 0;
            long upper = day == endDate.Date ? endDate.TimeOfDay.Ticks : TimeSpan.TicksPerDay;
            foreach (var interval in GetIntervals(day))
            {
                long left = Math.Max(lower, interval.Start.Ticks);
                long right = Math.Min(upper, interval.End.Ticks);
                if (right > left) ticks += right - left;
            }
            if (day == endDate.Date) break;
            day = day.AddDays(1);
        }
        return ticks / TimeSpan.TicksPerHour;
    }

    public DateTime AddWorkingHours(DateTime startDate, decimal hours,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (hours == 0) return startDate;
        bool forward = hours > 0;
        decimal ticksRemaining;
        try { ticksRemaining = Math.Abs(hours) * TimeSpan.TicksPerHour; }
        catch (OverflowException exception)
        { throw new ArgumentOutOfRangeException(nameof(hours), hours, exception.Message); }
        return ProjectWorkingTicks(startDate, ticksRemaining, forward, cancellationToken);
    }

    /// <summary>
    /// Integral-tick projection for relationship inverse and endpoint-boundary checks.
    /// Avoids decimal hours round trips changing which side of a work boundary is used.
    /// Existing hour-based callers retain their original arithmetic and behaviour.
    /// </summary>
    internal DateTime AddWorkingTicks(DateTime startDate, long ticks,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (ticks == 0) return startDate;
        return ProjectWorkingTicks(startDate, Math.Abs((decimal)ticks), ticks > 0, cancellationToken);
    }

    private DateTime ProjectWorkingTicks(DateTime startDate, decimal ticksRemaining, bool forward,
        CancellationToken cancellationToken)
    {
        if (!_hasWeeklyWork && !_exceptions.Any(pair => pair.Value.Count > 0
                && (forward ? pair.Key >= startDate.Date : pair.Key <= startDate.Date)))
            throw new InvalidDataException("The calendar has no working time in the requested direction.");

        DateTime day = startDate.Date;
        long cursor = startDate.TimeOfDay.Ticks;
        for (int searched = 0; searched <= MaximumProjectionDays; searched++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var intervals = GetIntervals(day);
            for (int index = forward ? 0 : intervals.Count - 1;
                 forward ? index < intervals.Count : index >= 0;
                 index += forward ? 1 : -1)
            {
                var interval = intervals[index];
                long left = forward ? Math.Max(cursor, interval.Start.Ticks) : interval.Start.Ticks;
                long right = forward ? interval.End.Ticks : Math.Min(cursor, interval.End.Ticks);
                long available = right - left;
                if (available <= 0) continue;
                if (ticksRemaining <= available)
                {
                    long offset = checked((long)decimal.Round(ticksRemaining, 0, MidpointRounding.AwayFromZero));
                    long timeOfDay = forward ? left + offset : right - offset;
                    if (timeOfDay > DateTime.MaxValue.Ticks - day.Ticks)
                        throw new InvalidDataException("Calendar projection exceeds the supported date range.");
                    return day.AddTicks(timeOfDay);
                }
                ticksRemaining -= available;
            }
            if (forward ? day == DateTime.MaxValue.Date : day == DateTime.MinValue.Date)
                throw new InvalidDataException("Calendar projection exceeds the supported date range.");
            day = day.AddDays(forward ? 1 : -1);
            cursor = forward ? 0 : TimeSpan.TicksPerDay;
            if (!_hasWeeklyWork && !_exceptions.Any(pair => pair.Value.Count > 0
                    && (forward ? pair.Key >= day : pair.Key <= day)))
                throw new InvalidDataException("The calendar has insufficient working time in the requested direction.");
        }
        throw new InvalidDataException("Calendar projection exceeds the bounded search horizon (1000 years).");
    }

    private IReadOnlyList<P6WorkInterval> GetIntervals(DateTime date) =>
        _exceptions.TryGetValue(date, out var intervals) ? intervals : _week[(int)date.DayOfWeek];

    private static P6WorkInterval ConvertSlot((TimeSpan Start, TimeSpan End) slot)
    {
        var end = slot.End;
        if (end < slot.Start || (slot.Start == TimeSpan.Zero && end == TimeSpan.Zero))
            end += TimeSpan.FromDays(1);
        return new(slot.Start, end);
    }

    private static WorkingDayCalculator CreateDefault()
    {
        var week = new IReadOnlyList<P6WorkInterval>[7];
        for (int day = 0; day < 7; day++)
            week[day] = day is >= 1 and <= 5
                ? new[] { new P6WorkInterval(TimeSpan.FromHours(8), TimeSpan.FromHours(12)),
                    new P6WorkInterval(TimeSpan.FromHours(13), TimeSpan.FromHours(17)) }
                : Array.Empty<P6WorkInterval>();
        return new WorkingDayCalculator(week,
            new Dictionary<DateTime, IReadOnlyList<P6WorkInterval>>());
    }
}
