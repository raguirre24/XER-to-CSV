using System.Collections.ObjectModel;
using System.Globalization;

namespace XerToCsvConverter;

/// <summary>A half-open working interval. Resolved intervals stay inside one civil day.</summary>
public readonly record struct P6WorkInterval(TimeSpan Start, TimeSpan End)
{
    public decimal WorkHours => (End - Start).Ticks / (decimal)TimeSpan.TicksPerHour;
}

/// <summary>One source-qualified calendar, resolved to civil-day availability.</summary>
public sealed class P6CalendarDefinition
{
    public string SourceToken { get; }
    public string CalendarId { get; }
    public string Name { get; }
    public string CalendarType { get; }
    public string BaseCalendarId { get; }
    public string RawHoursPerDay { get; }
    public decimal? HoursPerDay { get; }
    /// <summary>Sunday is index zero. These are actual shifts, not hours-per-period settings.</summary>
    public IReadOnlyList<IReadOnlyList<P6WorkInterval>> StandardWeek { get; }
    /// <summary>Complete daily replacements, including days affected by an overnight exception.</summary>
    public IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> Exceptions { get; }

    internal P6CalendarDefinition(string sourceToken, string calendarId, string name,
        string calendarType, string baseCalendarId, string rawHoursPerDay,
        IReadOnlyList<IReadOnlyList<P6WorkInterval>> rawWeek,
        IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> rawExceptions)
    {
        SourceToken = sourceToken;
        CalendarId = calendarId;
        Name = name;
        CalendarType = calendarType;
        BaseCalendarId = baseCalendarId;
        RawHoursPerDay = rawHoursPerDay;
        HoursPerDay = decimal.TryParse(rawHoursPerDay, NumberStyles.Float,
            CultureInfo.InvariantCulture, out decimal hours) && hours > 0 ? hours : null;
        (StandardWeek, Exceptions) = P6CalendarNormalization.Resolve(rawWeek, rawExceptions);
    }

    public WorkingDayCalculator CreateCalculator() => new(StandardWeek, Exceptions);
}

internal static class P6CalendarNormalization
{
    private static readonly TimeSpan Midnight = TimeSpan.FromDays(1);

    internal static (IReadOnlyList<IReadOnlyList<P6WorkInterval>> Week,
        IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> Exceptions) Resolve(
        IReadOnlyList<IReadOnlyList<P6WorkInterval>> rawWeek,
        IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> rawExceptions)
    {
        if (rawWeek.Count != 7)
            throw new InvalidDataException("A P6 calendar must define all seven weekdays.");
        foreach (var intervals in rawWeek) Validate(intervals);
        foreach (var intervals in rawExceptions.Values) Validate(intervals);

        var week = new IReadOnlyList<P6WorkInterval>[7];
        for (int day = 0; day < 7; day++)
            week[day] = Merge(OwnDay(rawWeek[day]).Concat(Spill(rawWeek[(day + 6) % 7])));

        // A changed day can also replace/remove the preceding day's normal overnight spill.
        var affectedDates = new SortedSet<DateTime>(rawExceptions.Keys.Select(date => date.Date));
        foreach (DateTime date in rawExceptions.Keys)
        {
            if (date.Date == DateTime.MaxValue.Date)
            {
                if (rawExceptions[date].Any(slot => slot.End > Midnight))
                    throw new InvalidDataException("A calendar exception spills beyond the supported date range.");
            }
            else affectedDates.Add(date.Date.AddDays(1));
        }

        var exceptions = new SortedDictionary<DateTime, IReadOnlyList<P6WorkInterval>>();
        foreach (DateTime date in affectedDates)
        {
            IReadOnlyList<P6WorkInterval> effective;
            if (rawExceptions.TryGetValue(date, out var explicitIntervals))
            {
                // Explicit exceptions replace the entire date, including incoming night work.
                effective = Merge(OwnDay(explicitIntervals));
            }
            else
            {
                var previous = date == DateTime.MinValue.Date
                    ? Array.Empty<P6WorkInterval>()
                    : rawExceptions.TryGetValue(date.AddDays(-1), out var previousException)
                        ? previousException : rawWeek[((int)date.DayOfWeek + 6) % 7];
                effective = Merge(OwnDay(rawWeek[(int)date.DayOfWeek]).Concat(Spill(previous)));
            }
            if (rawExceptions.ContainsKey(date) || !effective.SequenceEqual(week[(int)date.DayOfWeek]))
                exceptions.Add(date, effective);
        }

        return (Array.AsReadOnly(week),
            new ReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>>(exceptions));
    }

    private static IEnumerable<P6WorkInterval> OwnDay(IEnumerable<P6WorkInterval> intervals) =>
        intervals.Where(slot => slot.Start < Midnight && slot.End > slot.Start)
            .Select(slot => new P6WorkInterval(slot.Start, slot.End > Midnight ? Midnight : slot.End));

    private static IEnumerable<P6WorkInterval> Spill(IEnumerable<P6WorkInterval> intervals) =>
        intervals.Where(slot => slot.End > Midnight)
            .Select(slot => new P6WorkInterval(TimeSpan.Zero, slot.End - Midnight));

    internal static IReadOnlyList<P6WorkInterval> Merge(IEnumerable<P6WorkInterval> intervals)
    {
        var result = new List<P6WorkInterval>();
        foreach (var interval in intervals.Where(slot => slot.End > slot.Start)
                     .OrderBy(slot => slot.Start).ThenBy(slot => slot.End))
        {
            if (result.Count == 0 || interval.Start > result[^1].End) result.Add(interval);
            else if (interval.End > result[^1].End)
                result[^1] = result[^1] with { End = interval.End };
        }
        return result.AsReadOnly();
    }

    private static void Validate(IEnumerable<P6WorkInterval> intervals)
    {
        foreach (var interval in intervals)
            if (interval.Start < TimeSpan.Zero || interval.Start >= Midnight
                || interval.End < interval.Start || interval.End > interval.Start + Midnight)
                throw new InvalidDataException("Calendar shifts must start within a day and last at most 24 hours.");
    }
}
