using System.Globalization;

namespace XerToCsvConverter;

public partial class XerTransformer
{
    private static readonly WorkingDayCalculator ContinuousRelationshipCalendar = new(
        new(), new(), Enumerable.Repeat(24m, 7).ToArray(),
        Enumerable.Range(0, 7).Select(_ => new List<(TimeSpan, TimeSpan)>
        {
            (TimeSpan.Zero, TimeSpan.FromDays(1))
        }).ToArray());

    public XerTable? Create11XerCalendarDetailed()
    {
        ClearGenerationFailure(EnhancedTableNames.XerCalendarDetailed11);
        XerTable? source = _dataStore.GetTable(TableNames.Calendar);
        if (!IsTableValid(source)) return null;
        var result = new XerTable(EnhancedTableNames.XerCalendarDetailed11);
        result.SetHeaders(new[]
        {
            FieldNames.ClndrId, FieldNames.CalendarName, FieldNames.CalendarType,
            FieldNames.Date, FieldNames.DayOfWeek, FieldNames.WorkingDay, FieldNames.WorkHours,
            FieldNames.ExceptionType, FieldNames.ClndrIdKey, FieldNames.MonthUpdate,
            FieldNames.DayOfWeekNum, FieldNames.WorkingDayInt
        });
        string Read(DataRow row, string field) => GetFieldValue(row.Fields, source.FieldIndexes, field);
        var rowsByKey = source.Rows.GroupBy(row => (row.SourceToken, Id: Read(row, FieldNames.ClndrId).Trim()))
            .ToDictionary(group => group.Key, group => group.ToArray());
        var ordinals = new Dictionary<string, int>(StringComparer.Ordinal);
        var rows = source.Rows.Select(row => (Row: row,
            Ordinal: ordinals[row.SourceToken] = ordinals.GetValueOrDefault(row.SourceToken) + 1)).ToArray();
        var parsed = new Dictionary<(string SourceToken, string Id), P6ReportedCalendar>();

        foreach (var entry in rows.OrderBy(entry => entry.Row.SourceToken, StringComparer.Ordinal)
                     .ThenBy(entry => Read(entry.Row, FieldNames.ClndrId).Trim(), StringComparer.Ordinal))
        {
            DataRow sourceRow = entry.Row;
            string id = Read(sourceRow, FieldNames.ClndrId).Trim();
            bool validIdentity = id.Length > 0 && rowsByKey[(sourceRow.SourceToken, id)].Length == 1;
            P6ReportedCalendar calendar = validIdentity
                ? ResolveRaw(sourceRow, new HashSet<(string, string)>())
                : new(new IReadOnlyList<P6WorkInterval>?[7],
                    new Dictionary<DateTime, IReadOnlyList<P6WorkInterval>?>(),
                    new[] { new P6CalendarReportIssue("CALENDAR_IDENTITY_INVALID",
                        id.Length == 0 ? "CALENDAR.clndr_id is blank; no calendar identity has been invented."
                            : $"CALENDAR.clndr_id '{id}' is duplicated within this input; no competing calendar definition has been selected.", id) });
            calendar = P6CalendarReportingNormalization.Resolve(calendar);
            foreach (var issue in calendar.Issues)
                RecordDataQualityWarning(EnhancedTableNames.XerCalendarDetailed11, issue.Code,
                    $"CALENDAR '{id}': {issue.Message}", sourceRow, entry.Ordinal, TableNames.Calendar,
                    issue.Code == "CALENDAR_IDENTITY_INVALID" ? FieldNames.ClndrId
                        : issue.Code.StartsWith("CALENDAR_INHERITANCE", StringComparison.Ordinal) ? "base_clndr_id" : "clndr_data",
                    issue.RawValue, XerDataQuality.RawRowJson(source, sourceRow));
            for (int day = 0; day < 7; day++)
                AddCalendarRow((DayOfWeek)day, null, calendar.Week[day]);
            foreach (var exception in calendar.Exceptions.OrderBy(pair => pair.Key))
                AddCalendarRow(exception.Key.DayOfWeek, exception.Key, exception.Value);
            if (calendar.UnknownExceptionDates)
                AddCalendarRow(null, null, null, "Exception - Invalid");

            void AddCalendarRow(DayOfWeek? day, DateTime? date,
                IReadOnlyList<P6WorkInterval>? intervals, string? type = null)
            {
                decimal? hours = intervals?.Sum(interval => interval.WorkHours);
                bool? working = hours.HasValue ? hours > 0 : null;
                result.AddRow(sourceRow.WithFields(new[]
                {
                    id, Read(sourceRow, FieldNames.CalendarName), Read(sourceRow, FieldNames.CalendarType),
                    date?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? "",
                    day?.ToString() ?? "", working.HasValue ? working.Value ? "Y" : "N" : "",
                    hours?.ToString("0.############################", CultureInfo.InvariantCulture) ?? "",
                    type ?? (date is null ? "Standard" : !working.HasValue ? "Exception - Invalid"
                        : working.Value ? "Exception - Working" : "Exception - Non-Working"),
                    validIdentity ? CreateKey(sourceRow.SourceFilename, id) : "",
                    ParseMonthUpdateFromFilename(sourceRow.OriginalSourceFilename),
                    day.HasValue ? (day == DayOfWeek.Sunday ? 7 : (int)day.Value).ToString(CultureInfo.InvariantCulture) : "",
                    working.HasValue ? working.Value ? "1" : "0" : ""
                }));
            }
        }
        return result;

        P6ReportedCalendar ResolveRaw(DataRow row, HashSet<(string, string)> visiting)
        {
            var key = (row.SourceToken, Read(row, FieldNames.ClndrId).Trim());
            if (parsed.TryGetValue(key, out var cached)) return cached;
            if (visiting.Count >= 256 || !visiting.Add(key))
                throw new InvalidDataException("Cyclic or excessively deep base-calendar inheritance; inherited exceptions are unresolved.");
            P6ReportedCalendar own = P6CalendarParser.ParseForReporting(Read(row, "clndr_data"));
            string parentId = Read(row, "base_clndr_id").Trim();
            try
            {
                if (parentId.Length > 0 && parentId is not ("0" or "-1"))
                {
                    if (!rowsByKey.TryGetValue((row.SourceToken, parentId), out var parents) || parents.Length != 1)
                        throw new InvalidDataException($"Base calendar '{parentId}' is absent or ambiguous; inherited exceptions are unresolved.");
                    P6ReportedCalendar parent = ResolveRaw(parents[0], visiting);
                    var inherited = new SortedDictionary<DateTime, IReadOnlyList<P6WorkInterval>?>();
                    foreach (var pair in parent.Exceptions) inherited.Add(pair.Key, pair.Value);
                    foreach (var pair in own.Exceptions) inherited[pair.Key] = pair.Value;
                    var issues = own.Issues.ToList();
                    if (parent.UnknownExceptionDates || parent.Exceptions.Values.Any(value => value is null))
                        issues.Add(new("CALENDAR_INHERITANCE_INCOMPLETE", $"Base calendar '{parentId}' has unresolved exception evidence; known local overrides are retained.", parentId));
                    own = own with { Exceptions = inherited, Issues = issues,
                        UnknownExceptionDates = own.UnknownExceptionDates || parent.UnknownExceptionDates };
                }
            }
            catch (InvalidDataException error)
            {
                own = own with { UnknownExceptionDates = true, Issues = own.Issues.Append(
                    new P6CalendarReportIssue("CALENDAR_INHERITANCE_INVALID", error.Message, parentId)).ToArray() };
            }
            finally { visiting.Remove(key); }
            // Do not cache incomplete inheritance: a cycle must not make its
            // apparent resolution depend on which calendar was exported first.
            if (!own.UnknownExceptionDates) parsed[key] = own;
            return own;
        }
    }

    private static string FormatRelationshipDays(decimal hours, decimal hoursPerDay, string format)
    {
        if (hoursPerDay <= 0) return "";
        try { return (hours / hoursPerDay).ToString(format, CultureInfo.InvariantCulture); }
        catch (OverflowException) { return ""; }
    }

}
