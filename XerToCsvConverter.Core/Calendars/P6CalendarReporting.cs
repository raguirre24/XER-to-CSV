using System.Globalization;

namespace XerToCsvConverter;

// Reporting intentionally keeps unknown rules nullable. This must never be used
// as a fallback working calendar by relationship or resource calculations.
internal sealed record P6CalendarReportIssue(string Code, string Message, string RawValue);
internal sealed record P6ReportedCalendar(
    IReadOnlyList<IReadOnlyList<P6WorkInterval>?> Week,
    IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>?> Exceptions,
    IReadOnlyList<P6CalendarReportIssue> Issues,
    bool UnknownExceptionDates = false);

internal static partial class P6CalendarParser
{
    internal static P6ReportedCalendar ParseForReporting(string text)
    {
        var week = new IReadOnlyList<P6WorkInterval>?[7];
        var exceptions = new SortedDictionary<DateTime, IReadOnlyList<P6WorkInterval>?>();
        var issues = new List<P6CalendarReportIssue>();
        bool unknownDates = false;
        Node[] allNodes;
        try
        {
            if (string.IsNullOrWhiteSpace(text))
                throw new InvalidDataException("clndr_data is blank; working availability cannot be inferred from hours per day.");
            allNodes = Descendants(new TreeReader(text, CancellationToken.None).Read()).ToArray();
        }
        catch (InvalidDataException error)
        {
            issues.Add(new("CALENDAR_STRUCTURE_INVALID", error.Message, text));
            return new(week, exceptions, issues, true);
        }

        Node[] weekSections = allNodes.Where(node => node.Name == "DaysOfWeek").ToArray();
        if (weekSections.Length != 1)
            issues.Add(new("CALENDAR_WEEK_INVALID", "clndr_data must contain exactly one DaysOfWeek section; weekday availability is unknown.", text));
        else
        {
            var seen = new HashSet<int>();
            foreach (Node node in weekSections[0].Children)
            {
                if (!int.TryParse(node.Name, NumberStyles.None, CultureInfo.InvariantCulture, out int day)
                    || day < 1 || day > 7)
                {
                    issues.Add(new("CALENDAR_WEEKDAY_INVALID", "DaysOfWeek contains an unidentified weekday record.", node.Name));
                    continue;
                }
                if (!seen.Add(day))
                {
                    week[day - 1] = null;
                    issues.Add(new("CALENDAR_WEEKDAY_DUPLICATE", $"DaysOfWeek repeats day {day}; none of its competing definitions is selected.", node.Name));
                    continue;
                }
                try
                {
                    if (node.Attributes.Length != 0)
                        throw new InvalidDataException($"Day {day} has unexpected attributes '{node.Attributes}'.");
                    week[day - 1] = ReadShifts(node);
                }
                catch (InvalidDataException error)
                { issues.Add(new("CALENDAR_WEEKDAY_INVALID", $"Day {day}: {error.Message}", text)); }
            }
            foreach (int day in Enumerable.Range(1, 7).Where(day => !seen.Contains(day)))
                issues.Add(new("CALENDAR_WEEKDAY_MISSING", $"DaysOfWeek does not explicitly define day {day}; its availability is unknown.", text));
        }

        foreach (Node section in allNodes.Where(node => node.Name is "Exceptions" or "HolidayOrExceptions" or "HolidayOrException"))
        foreach (Node node in section.Children)
        {
            DateTime date;
            try
            {
                string[] parts = node.Attributes.Split('|', StringSplitOptions.TrimEntries);
                if (parts.Length is not (2 or 3) || parts[0] != "d"
                    || (parts.Length == 3 && parts[2] != "0")
                    || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out int serial))
                    throw new InvalidDataException($"Invalid calendar exception date record '{node.Attributes}'.");
                date = new DateTime(1899, 12, 30).AddDays(serial);
            }
            catch (Exception error) when (error is InvalidDataException or ArgumentOutOfRangeException)
            {
                unknownDates = true;
                issues.Add(new("CALENDAR_EXCEPTION_DATE_INVALID", error.Message, node.Attributes));
                continue;
            }
            if (exceptions.ContainsKey(date))
            {
                exceptions[date] = null;
                issues.Add(new("CALENDAR_EXCEPTION_DUPLICATE", $"Calendar repeats exception date {date:yyyy-MM-dd}; its availability is unknown.", node.Attributes));
                continue;
            }
            try { exceptions.Add(date, ReadShifts(node)); }
            catch (InvalidDataException error)
            {
                exceptions.Add(date, null);
                issues.Add(new("CALENDAR_EXCEPTION_INVALID", $"Exception {date:yyyy-MM-dd}: {error.Message}", text));
            }
        }
        return new(week, exceptions, issues, unknownDates);
    }
}

internal static class P6CalendarReportingNormalization
{
    private static readonly TimeSpan Midnight = TimeSpan.FromDays(1);

    internal static P6ReportedCalendar Resolve(P6ReportedCalendar raw)
    {
        var week = new IReadOnlyList<P6WorkInterval>?[7];
        for (int day = 0; day < 7; day++)
            week[day] = Combine(raw.Week[day], raw.Week[(day + 6) % 7]);
        var affected = new SortedSet<DateTime>(raw.Exceptions.Keys);
        var issues = raw.Issues.ToList();
        foreach (var pair in raw.Exceptions)
        {
            if (pair.Key < DateTime.MaxValue.Date) affected.Add(pair.Key.AddDays(1));
            else if (pair.Value?.Any(interval => interval.End > Midnight) == true)
                issues.Add(new("CALENDAR_EXCEPTION_RANGE_INVALID", "An overnight exception extends beyond the supported date range; only its representable civil-day hours are reported.", pair.Key.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)));
        }
        var exceptions = new SortedDictionary<DateTime, IReadOnlyList<P6WorkInterval>?>();
        foreach (DateTime date in affected)
        {
            bool explicitDate = raw.Exceptions.TryGetValue(date, out var own);
            IReadOnlyList<P6WorkInterval>? effective;
            if (explicitDate) effective = own is null ? null : P6CalendarNormalization.Merge(Own(own));
            else
            {
                var previous = date == DateTime.MinValue.Date ? Array.Empty<P6WorkInterval>()
                    : raw.Exceptions.TryGetValue(date.AddDays(-1), out var previousException)
                        ? previousException : raw.Week[((int)date.DayOfWeek + 6) % 7];
                effective = Combine(raw.Week[(int)date.DayOfWeek], previous);
            }
            var standard = week[(int)date.DayOfWeek];
            if (explicitDate || effective is null || standard is null || !effective.SequenceEqual(standard))
                exceptions.Add(date, effective);
        }
        return new(week, exceptions, issues, raw.UnknownExceptionDates);
    }

    private static IReadOnlyList<P6WorkInterval>? Combine(IReadOnlyList<P6WorkInterval>? own,
        IReadOnlyList<P6WorkInterval>? previous) => own is null || previous is null ? null
            : P6CalendarNormalization.Merge(Own(own).Concat(previous.Where(slot => slot.End > Midnight)
                .Select(slot => new P6WorkInterval(TimeSpan.Zero, slot.End - Midnight))));

    private static IEnumerable<P6WorkInterval> Own(IEnumerable<P6WorkInterval> intervals) =>
        intervals.Where(slot => slot.Start < Midnight && slot.End > slot.Start)
            .Select(slot => new P6WorkInterval(slot.Start, slot.End > Midnight ? Midnight : slot.End));
}
