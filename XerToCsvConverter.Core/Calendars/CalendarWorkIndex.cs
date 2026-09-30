namespace XerToCsvConverter;

/// <summary>Immutable periodic-week clock with sparse complete daily replacements.</summary>
internal sealed class CalendarWorkIndex
{
    internal static readonly int MaximumDayNumber = (int)(DateTime.MaxValue.Ticks / TimeSpan.TicksPerDay);
    private readonly DailyWork[] _week;
    private readonly long[] _weekPrefix = new long[8];
    private readonly int[] _exceptionDays;
    private readonly DailyWork[] _exceptionWork;
    private readonly long[] _exceptionDeltaPrefix;
    private readonly int[] _positiveExceptionDays;

    internal CalendarWorkIndex(IReadOnlyList<IReadOnlyList<P6WorkInterval>> week,
        IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> exceptions)
    {
        _week = week.Select(intervals => new DailyWork(intervals)).ToArray();
        // DateTime day zero is Monday; the public calendar week starts on Sunday.
        for (int day = 0; day < 7; day++)
            _weekPrefix[day + 1] = checked(_weekPrefix[day] + _week[(day + 1) % 7].TotalTicks);
        HasWeeklyWork = _week.Any(day => day.Intervals.Length > 0);
        var ordered = exceptions.OrderBy(pair => pair.Key).ToArray();
        _exceptionDays = new int[ordered.Length];
        _exceptionWork = new DailyWork[ordered.Length];
        _exceptionDeltaPrefix = new long[ordered.Length + 1];
        var positive = new List<int>();
        for (int index = 0; index < ordered.Length; index++)
        {
            int day = DayNumber(ordered[index].Key);
            var work = new DailyWork(ordered[index].Value);
            _exceptionDays[index] = day;
            _exceptionWork[index] = work;
            _exceptionDeltaPrefix[index + 1] = checked(_exceptionDeltaPrefix[index]
                + work.TotalTicks - _week[(day + 1) % 7].TotalTicks);
            if (work.Intervals.Length > 0) positive.Add(day);
        }
        _positiveExceptionDays = positive.ToArray();
    }

    internal bool HasWeeklyWork { get; }
    internal static int DayNumber(DateTime date) => (int)(date.Ticks / TimeSpan.TicksPerDay);

    internal ReadOnlySpan<P6WorkInterval> Intervals(int day) => Work(day).Intervals;

    internal long Prefix(long timestampTicks)
    {
        int day = (int)(timestampTicks / TimeSpan.TicksPerDay);
        return checked(BeforeDay(day) + Work(day).Prefix(timestampTicks % TimeSpan.TicksPerDay));
    }

    // A virtual midnight after the final supported civil day is valid for capacity
    // comparisons. Projection must still reject an unrepresentable final endpoint.
    internal long BeforeDay(int day)
    {
        int exceptionCount = LowerBound(_exceptionDays, day);
        return checked(day / 7L * _weekPrefix[7] + _weekPrefix[day % 7]
            + _exceptionDeltaPrefix[exceptionCount]);
    }

    internal bool HasPositiveException(int startDay, bool forward)
    {
        if (_positiveExceptionDays.Length == 0) return false;
        return forward ? _positiveExceptionDays[^1] >= startDay : _positiveExceptionDays[0] <= startDay;
    }

    internal int LastPositiveException(bool forward) =>
        forward ? _positiveExceptionDays[^1] : _positiveExceptionDays[0];

    private DailyWork Work(int day)
    {
        int index = Array.BinarySearch(_exceptionDays, day);
        return index >= 0 ? _exceptionWork[index] : _week[(day + 1) % 7];
    }

    private static int LowerBound(int[] values, int target)
    {
        int low = 0, high = values.Length;
        while (low < high)
        {
            int middle = low + (high - low) / 2;
            if (values[middle] < target) low = middle + 1;
            else high = middle;
        }
        return low;
    }

    private sealed class DailyWork
    {
        internal readonly P6WorkInterval[] Intervals;
        private readonly long[] _prefix;

        internal DailyWork(IReadOnlyList<P6WorkInterval> intervals)
        {
            Intervals = intervals.ToArray();
            _prefix = new long[Intervals.Length + 1];
            for (int index = 0; index < Intervals.Length; index++)
                _prefix[index + 1] = checked(_prefix[index] + (Intervals[index].End - Intervals[index].Start).Ticks);
        }

        internal long TotalTicks => _prefix[^1];

        internal long Prefix(long timeOfDay)
        {
            int low = 0, high = Intervals.Length;
            while (low < high)
            {
                int middle = low + (high - low) / 2;
                if (Intervals[middle].Start.Ticks < timeOfDay) low = middle + 1;
                else high = middle;
            }
            if (low == 0) return 0;
            int index = low - 1;
            return checked(_prefix[index] + Math.Min(timeOfDay, Intervals[index].End.Ticks) - Intervals[index].Start.Ticks);
        }
    }
}
