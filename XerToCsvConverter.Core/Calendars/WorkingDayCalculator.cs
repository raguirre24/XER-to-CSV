namespace XerToCsvConverter;

/// <summary>Exact working-time arithmetic over normalized half-open civil-day intervals.</summary>
public sealed class WorkingDayCalculator
{
    private const int MaximumProjectionDays = 366_000;
    private readonly CalendarWorkIndex _index;
    private readonly int? _nonWorkingStartDay;
    private readonly int? _nonWorkingFinishDay;

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
        var resolved = P6CalendarNormalization.Resolve(week, exceptions);
        _index = new CalendarWorkIndex(resolved.Week, resolved.Exceptions);
    }

    internal WorkingDayCalculator(IReadOnlyList<IReadOnlyList<P6WorkInterval>> week,
        IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> exceptions,
        DateTime? nonWorkingStart = null, DateTime? nonWorkingFinish = null)
        : this(new CalendarWorkIndex(week, exceptions), nonWorkingStart, nonWorkingFinish) { }

    internal WorkingDayCalculator(CalendarWorkIndex index, DateTime? nonWorkingStart = null,
        DateTime? nonWorkingFinish = null)
    {
        _index = index;
        _nonWorkingStartDay = nonWorkingStart.HasValue ? CalendarWorkIndex.DayNumber(nonWorkingStart.Value) : null;
        _nonWorkingFinishDay = nonWorkingFinish.HasValue ? CalendarWorkIndex.DayNumber(nonWorkingFinish.Value) : null;
    }

    /// <summary>Activity-specific civil-day exclusion; shared calendar definitions and lag clocks stay unchanged.</summary>
    internal WorkingDayCalculator WithNonWorkingPeriod(DateTime start, DateTime finish)
    {
        if (finish.Date < start.Date)
            throw new ArgumentException("A nonworking period cannot finish before it starts.", nameof(finish));
        return finish.Date == start.Date ? this : new WorkingDayCalculator(_index, start.Date, finish.Date);
    }

    public bool IsWorkingDay(DateTime date) => GetIntervals(CalendarWorkIndex.DayNumber(date)).Length > 0;

    public decimal CountWorkingHours(DateTime startDate, DateTime endDate,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return (Prefix(endDate.Ticks) - Prefix(startDate.Ticks)) / (decimal)TimeSpan.TicksPerHour;
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
        int startDay = CalendarWorkIndex.DayNumber(startDate);
        if (!_index.HasWeeklyWork && !_index.HasPositiveException(startDay, forward))
            throw new InvalidDataException("The calendar has no working time in the requested direction.");
        int dateLimit = forward ? CalendarWorkIndex.MaximumDayNumber : 0;
        int limitDay = forward ? Math.Min(startDay + MaximumProjectionDays, dateLimit)
            : Math.Max(startDay - MaximumProjectionDays, dateLimit);
        long startPrefix = Prefix(startDate.Ticks);
        int day = startDay;
        long initialCapacity = forward ? BeforeDay(startDay + 1) - startPrefix : startPrefix - BeforeDay(startDay);
        if (ticksRemaining > initialCapacity)
        {
            long capacity = forward ? BeforeDay(limitDay + 1) - startPrefix : startPrefix - BeforeDay(limitDay);
            if (ticksRemaining > capacity) ThrowProjectionFailure(limitDay, dateLimit, forward);

            // Bracket nearby projections first; one-tick boundary checks should not
            // binary-search the entire thousand-year horizon on every relationship.
            int direction = forward ? 1 : -1;
            int maximumDistance = Math.Abs(limitDay - startDay);
            int previousDistance = 0, distance = 1;
            long Available(int offset)
            {
                int candidate = startDay + direction * offset;
                return forward ? BeforeDay(candidate + 1) - startPrefix : startPrefix - BeforeDay(candidate);
            }
            while (Available(distance) < ticksRemaining)
            {
                cancellationToken.ThrowIfCancellationRequested();
                previousDistance = distance;
                distance = Math.Min(distance * 2, maximumDistance);
            }
            // Compare differences to the unchanged decimal amount; adding it to a
            // large absolute prefix could round a sub-tick request away.
            int low = previousDistance + 1, high = distance;
            while (low < high)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int middle = low + (high - low) / 2;
                if (Available(middle) >= ticksRemaining) high = middle;
                else low = middle + 1;
            }
            day = startDay + direction * low;
        }
        if (day != startDay)
            ticksRemaining -= forward ? BeforeDay(day) - startPrefix : startPrefix - BeforeDay(day + 1);
        long cursor = day == startDay ? startDate.TimeOfDay.Ticks : forward ? 0 : TimeSpan.TicksPerDay;
        var intervals = GetIntervals(day);
        for (int index = forward ? 0 : intervals.Length - 1;
             forward ? index < intervals.Length : index >= 0; index += forward ? 1 : -1)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var interval = intervals[index];
            long left = forward ? Math.Max(cursor, interval.Start.Ticks) : interval.Start.Ticks;
            long right = forward ? interval.End.Ticks : Math.Min(cursor, interval.End.Ticks);
            long available = right - left;
            if (available <= 0) continue;
            if (ticksRemaining <= available)
            {
                // Select the interval before rounding, exactly as the daily engine did.
                long offset = checked((long)decimal.Round(ticksRemaining, 0, MidpointRounding.AwayFromZero));
                long timeOfDay = forward ? left + offset : right - offset;
                long dayTicks = day * TimeSpan.TicksPerDay;
                if (timeOfDay > DateTime.MaxValue.Ticks - dayTicks)
                    throw new InvalidDataException("Calendar projection exceeds the supported date range.");
                return startDate.Date.AddTicks((day - (long)startDay) * TimeSpan.TicksPerDay + timeOfDay);
            }
            ticksRemaining -= available;
        }
        throw new InvalidOperationException("Indexed calendar projection did not resolve its working interval.");
    }

    private void ThrowProjectionFailure(int limitDay, int dateLimit, bool forward)
    {
        if (!_index.HasWeeklyWork)
        {
            int lastException = _index.LastPositiveException(forward);
            if (forward ? lastException <= limitDay : lastException >= limitDay)
            {
                if (lastException != dateLimit)
                    throw new InvalidDataException("The calendar has insufficient working time in the requested direction.");
            }
        }
        if (limitDay == dateLimit)
            throw new InvalidDataException("Calendar projection exceeds the supported date range.");
        throw new InvalidDataException("Calendar projection exceeds the bounded search horizon (1000 years).");
    }

    private ReadOnlySpan<P6WorkInterval> GetIntervals(int day) =>
        _nonWorkingStartDay.HasValue && day >= _nonWorkingStartDay.Value && day < _nonWorkingFinishDay!.Value
            ? ReadOnlySpan<P6WorkInterval>.Empty : _index.Intervals(day);

    private long Prefix(long ticks)
    {
        long total = _index.Prefix(ticks);
        if (!_nonWorkingStartDay.HasValue) return total;
        long start = _nonWorkingStartDay.Value * TimeSpan.TicksPerDay;
        if (ticks <= start) return total;
        long finish = _nonWorkingFinishDay!.Value * TimeSpan.TicksPerDay;
        return total - (_index.Prefix(Math.Min(ticks, finish)) - _index.Prefix(start));
    }

    private long BeforeDay(int day)
    {
        long total = _index.BeforeDay(day);
        if (!_nonWorkingStartDay.HasValue || day <= _nonWorkingStartDay.Value) return total;
        return total - (_index.BeforeDay(Math.Min(day, _nonWorkingFinishDay!.Value))
            - _index.BeforeDay(_nonWorkingStartDay.Value));
    }

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
