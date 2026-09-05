using System.Globalization;

namespace XerToCsvConverter.Core.Tests;

internal static class P6TestCalendars
{
    // Availability is explicitly encoded in test inputs, never inferred by production code.
    internal static string WorkWeek(string hoursPerDay = "8")
    {
        if (!decimal.TryParse(hoursPerDay, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal hours)
            || hours <= 0 || hours > 24) hours = 8;
        string start = hours > 16 ? "00:00" : "08:00";
        int startMinutes = hours > 16 ? 0 : 480;
        int endMinutes = startMinutes + (int)(hours * 60);
        string finish = $"{endMinutes / 60:00}:{endMinutes % 60:00}";
        string shifts = hours == 8 ? "(0||0(s|08:00|f|12:00)())(0||1(s|13:00|f|17:00)())"
            : $"(0||0(s|{start}|f|{finish})())";
        return "(0||CalendarData()((0||DaysOfWeek()(" + string.Concat(Enumerable.Range(1, 7)
            .Select(day => $"(0||{day}()({(hours == 24 || day is >= 2 and <= 6 ? shifts : "")}))")) + "))))";
    }
}
