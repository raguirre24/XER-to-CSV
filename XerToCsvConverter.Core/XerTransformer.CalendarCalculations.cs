using System.Globalization;

namespace XerToCsvConverter;

public partial class XerTransformer
{
    private sealed record RelationshipScheduleOptions(
        string LagCalendar = "",
        string DefaultCalendarId = "",
        bool IsAmbiguous = false);

    private static readonly WorkingDayCalculator ContinuousRelationshipCalendar = new(
        new(), new(), Enumerable.Repeat(24m, 7).ToArray(),
        Enumerable.Range(0, 7).Select(_ => new List<(TimeSpan, TimeSpan)>
        {
            (TimeSpan.Zero, TimeSpan.FromDays(1))
        }).ToArray());

    public XerTable? Create11XerCalendarDetailed()
    {
        if (!IsTableValid(_dataStore.GetTable(TableNames.Calendar))) return null;
        try
        {
            var result = new XerTable(EnhancedTableNames.XerCalendarDetailed11);
            result.SetHeaders(new[]
            {
                FieldNames.ClndrId, FieldNames.CalendarName, FieldNames.CalendarType,
                FieldNames.Date, FieldNames.DayOfWeek, FieldNames.WorkingDay, FieldNames.WorkHours,
                FieldNames.ExceptionType, FieldNames.ClndrIdKey, FieldNames.MonthUpdate,
                FieldNames.DayOfWeekNum, FieldNames.WorkingDayInt
            });
            var sourceRows = _dataStore.GetTable(TableNames.Calendar)!.Rows
                .GroupBy(row => (row.SourceToken,
                    GetFieldValue(row.Fields, _dataStore.GetTable(TableNames.Calendar)!.FieldIndexes, FieldNames.ClndrId).Trim()))
                .ToDictionary(group => group.Key, group => group.ToArray());
            foreach (P6CalendarDefinition calendar in new P6CalendarRepository(_dataStore).GetAll())
            {
                for (int day = 0; day < 7; day++)
                    AddCalendarRow(calendar, (DayOfWeek)day, null, calendar.StandardWeek[day]);
                foreach (var exception in calendar.Exceptions.OrderBy(pair => pair.Key))
                    AddCalendarRow(calendar, exception.Key.DayOfWeek, exception.Key, exception.Value);
            }
            return result;

            void AddCalendarRow(P6CalendarDefinition calendar, DayOfWeek day, DateTime? date,
                IReadOnlyList<P6WorkInterval> intervals)
            {
                decimal hours = intervals.Sum(interval => interval.WorkHours);
                bool working = hours > 0;
                DataRow sourceRow = sourceRows[(calendar.SourceToken, calendar.CalendarId)].Single();
                result.AddRow(sourceRow.WithFields(new[]
                {
                    calendar.CalendarId, calendar.Name, calendar.CalendarType,
                    date?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? "",
                    day.ToString(), working ? "Y" : "N",
                    hours.ToString("0.############################", CultureInfo.InvariantCulture),
                    date is null ? "Standard" : working ? "Exception - Working" : "Exception - Non-Working",
                    CreateKey(sourceRow.SourceFilename, calendar.CalendarId),
                    ParseMonthUpdateFromFilename(sourceRow.OriginalSourceFilename),
                    (day == DayOfWeek.Sunday ? 7 : (int)day).ToString(CultureInfo.InvariantCulture),
                    working ? "1" : "0"
                }));
            }
        }
        catch (InvalidDataException ex)
        {
            Console.WriteLine($"Error creating {EnhancedTableNames.XerCalendarDetailed11}: {ex.Message}");
            return null;
        }
    }

    private Dictionary<(string Source, string Project), RelationshipScheduleOptions> BuildProjectScheduleOptionsLookup(XerTable? options)
    {
        var result = new Dictionary<(string Source, string Project), RelationshipScheduleOptions>();
        var projectKeys = new HashSet<(string Source, string Project)>();
        var optionKeys = new HashSet<(string Source, string Project)>();
        XerTable? projects = _dataStore.GetTable(TableNames.Project);
        if (IsTableValid(projects))
        {
            foreach (DataRow row in projects.Rows)
            {
                string id = GetFieldValue(row.Fields, projects.FieldIndexes, FieldNames.ProjectId);
                string calendarId = GetFieldValue(row.Fields, projects.FieldIndexes, FieldNames.ClndrId);
                if (string.IsNullOrWhiteSpace(id)) continue;
                var key = (row.SourceToken, id.Trim());
                result[key] = new RelationshipScheduleOptions(
                    DefaultCalendarId: calendarId.Trim(),
                    IsAmbiguous: !projectKeys.Add(key));
            }
        }
        if (!IsTableValid(options)) return result;
        // An omitted export column does not tell us the configured scheduling option.
        if (!options.FieldIndexes.TryGetValue("sched_calendar_on_relationship_lag", out int lagIndex)) return result;
        foreach (DataRow row in options.Rows)
        {
            string id = GetFieldValue(row.Fields, options.FieldIndexes, FieldNames.ProjectId);
            if (string.IsNullOrWhiteSpace(id)) continue;
            var key = (row.SourceToken, id.Trim());
            result.TryGetValue(key, out RelationshipScheduleOptions? prior);
            string selected = GetFieldValue(row.Fields, options.FieldIndexes, "sched_calendar_on_relationship_lag").Trim();
            result[key] = (prior ?? new()) with
            {
                // P6 documents Successor as the default when the selection is empty.
                LagCalendar = lagIndex >= row.Fields.Length ? ""
                    : selected.Length == 0 ? "rcal_Successor" : selected,
                IsAmbiguous = prior?.IsAmbiguous == true || !optionKeys.Add(key)
            };
        }
        return result;
    }

    private string CalculateFreeFloat(
        DataRow relationship, IReadOnlyDictionary<string, int> predIndexes,
        Dictionary<(string Source, string Task), RelationshipTask[]> tasks,
        Dictionary<(string Source, string Calendar), RelationshipCalendar> calendars,
        Dictionary<(string Source, string Project), RelationshipScheduleOptions> scheduleOptions)
    {
        string Read(string field) => GetFieldValue(relationship.Fields, predIndexes, field);
        // The display lag column retains its legacy default, but an absent or malformed
        // relationship lag cannot establish a numerical delay allowance.
        if (!decimal.TryParse(Read(FieldNames.LagHrCnt), NumberStyles.Float,
                CultureInfo.InvariantCulture, out decimal lagHours)) return "";
        RelationshipTask? successor = ResolveRelationshipTask(tasks, relationship.SourceToken,
            Read(FieldNames.TaskId), Read(FieldNames.ProjectId));
        RelationshipTask? predecessor = ResolveRelationshipTask(tasks, relationship.SourceToken,
            Read(FieldNames.PredTaskId), Read("pred_proj_id"));
        if (predecessor is null || successor is null
            || !IsFloatActivity(predecessor) || !IsFloatActivity(successor)
            || successor.Status == "TK_ACTIVE"
            || !calendars.TryGetValue((relationship.SourceToken, successor.CalendarId), out var successorCalendar)
            || !calendars.TryGetValue((relationship.SourceToken, predecessor.CalendarId), out var predecessorDefinition)
            || predecessorDefinition.HoursPerDay is not > 0)
            return "";
        decimal predecessorHoursPerDay = predecessorDefinition.HoursPerDay.Value;
        WorkingDayCalculator predecessorCalendar = predecessorDefinition.Calculator;

        scheduleOptions.TryGetValue((relationship.SourceToken, successor.ProjectId), out RelationshipScheduleOptions? options);
        string type = Read(FieldNames.PredType).Trim().ToUpperInvariant();
        DateTime? from;
        DateTime? to;
        switch (type)
        {
            case "PR_FS": from = predecessor.Finish; to = successor.Start; break;
            case "PR_FF": from = predecessor.Finish; to = successor.Finish; break;
            case "PR_SS":
            case "PR_SF":
                // Once a start relationship has progressed, original lag/actual start alone do
                // not establish remaining relationship lag or the retained-logic constraint.
                // Do not silently invent a number when those scheduling semantics are absent.
                if (predecessor.Status == "TK_ACTIVE") return "";
                from = predecessor.Start;
                to = type == "PR_SS" ? successor.Start : successor.Finish;
                break;
            default: return "";
        }
        if (from is null || to is null) return "";
        if (predecessor.Status == "TK_ACTIVE" && !predecessor.HasRemainingFinish) return "";

        WorkingDayCalculator lagCalendar = predecessorCalendar;
        if (lagHours != 0)
        {
            if (options?.IsAmbiguous == true
                || !RelationshipLagCalendarPolicy.TryParse(options?.LagCalendar, out var kind)) return "";
            if (predecessor.ProjectId != successor.ProjectId)
            {
                // A cross-project scheduling context is not universally the successor's.
                // Compute only when both exported settings resolve the same lag operation.
                if (!scheduleOptions.TryGetValue((relationship.SourceToken, predecessor.ProjectId), out var other)
                    || other.IsAmbiguous
                    || !RelationshipLagCalendarPolicy.TryParse(other.LagCalendar, out var otherKind)
                    || otherKind != kind
                    || (kind == RelationshipLagCalendar.ProjectDefault
                        && other.DefaultCalendarId != options!.DefaultCalendarId)) return "";
            }
            switch (kind)
            {
                case RelationshipLagCalendar.Predecessor: break;
                case RelationshipLagCalendar.Successor:
                    lagCalendar = successorCalendar.Calculator;
                    break;
                case RelationshipLagCalendar.TwentyFourHour: lagCalendar = ContinuousRelationshipCalendar; break;
                case RelationshipLagCalendar.ProjectDefault:
                    if (!calendars.TryGetValue((relationship.SourceToken, options!.DefaultCalendarId), out var projectCalendar)) return "";
                    lagCalendar = projectCalendar.Calculator;
                    break;
            }
        }
        try
        {
            decimal hours = RelationshipFreeFloatCalculator.CalculateHours(predecessorCalendar,
                lagCalendar, from.Value, to.Value, lagHours,
                predecessorIsStart: type is "PR_SS" or "PR_SF");
            // Maximum signed predecessor working-time movement, not the working gap
            // after applying lag. Preserve sub-day precision and negative allowances.
            return (hours / predecessorHoursPerDay).ToString("G29", CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is InvalidOperationException or InvalidDataException or ArgumentOutOfRangeException or OverflowException)
        {
            Console.WriteLine($"Relationship free_float unavailable: {ex.Message}");
            return "";
        }
    }

    private static string FormatRelationshipDays(decimal hours, decimal hoursPerDay, string format)
    {
        if (hoursPerDay <= 0) return "";
        try { return (hours / hoursPerDay).ToString(format, CultureInfo.InvariantCulture); }
        catch (OverflowException) { return ""; }
    }

    private static bool IsFloatActivity(RelationshipTask task) => task.ValidState
        && (task.Status is "TK_ACTIVE" or "TK_NOTSTART")
        // Resource-dependent dates cannot be moved safely using TASK.clndr_id alone.
        && (task.Type is "TT_TASK" or "TT_MILE" or "TT_FINMILE");
}
