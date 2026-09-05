namespace XerToCsvConverter;

// Scheduling availability is separate from the hours-per-day used to display lag or float.
internal enum RelationshipLagCalendar
{
    Predecessor,
    Successor,
    TwentyFourHour,
    ProjectDefault
}

internal static class RelationshipLagCalendarPolicy
{
    internal static bool TryParse(string? raw, out RelationshipLagCalendar calendar)
    {
        switch (raw?.Trim().ToUpperInvariant())
        {
            case "RCAL_PREDECESSOR": calendar = RelationshipLagCalendar.Predecessor; return true;
            case "RCAL_SUCCESSOR": calendar = RelationshipLagCalendar.Successor; return true;
            case "RCAL_24HOUR": calendar = RelationshipLagCalendar.TwentyFourHour; return true;
            case "RCAL_PROJDEFAULT":
            case "RCAL_PROJECT": // Compatibility alias accepted by earlier parser versions.
                calendar = RelationshipLagCalendar.ProjectDefault; return true;
            default: calendar = default; return false;
        }
    }
}
