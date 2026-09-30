namespace XerToCsvConverter;

public partial class XerTransformer
{
    // A lookup is rebuilt for each assessment/export call. Its rows remain the
    // authoritative evidence while frequently used evaluation values are read once.
    private sealed class RelationshipTask
    {
        public DataRow Row { get; }
        public string ProjectId { get; }
        public string CalendarId { get; }
        public string Status { get; }
        public string Type { get; }
        public string ActualStartText { get; }
        public string ActualEndText { get; }
        public DateTime? ActualStart { get; }
        public string SuspendText { get; }
        public string ResumeText { get; }
        public DateTime? Suspend { get; }
        public DateTime? Resume { get; }
        public (DateTime? Date, string Field) RemainingStart { get; }
        public (DateTime? Date, string Field) RemainingFinish { get; }

        public RelationshipTask(DataRow row)
        {
            Row = row;
            ProjectId = RawText(row, "proj_id");
            CalendarId = RawText(row, "clndr_id");
            Status = RawText(row, "status_code").ToUpperInvariant();
            Type = RawText(row, "task_type").ToUpperInvariant();
            ActualStartText = RawText(row, "act_start_date");
            ActualEndText = RawText(row, "act_end_date");
            ActualStart = DateParser.TryParse(ActualStartText);
            SuspendText = RawText(row, "suspend_date");
            ResumeText = RawText(row, "resume_date");
            Suspend = DateParser.TryParse(SuspendText);
            Resume = DateParser.TryParse(ResumeText);
            RemainingStart = ReadEndpoint(true);
            RemainingFinish = ReadEndpoint(false);
        }

        private (DateTime? Date, string Field) ReadEndpoint(bool start)
        {
            string field = start ? "restart_date" : "reend_date";
            string text = RawText(Row, field);
            // Unstarted early-date fallback is allowed only for absent/blank
            // preferred values. A malformed nonblank preferred value stays invalid.
            if (Status == "TK_NOTSTART" && text.Length == 0)
            {
                field = start ? "early_start_date" : "early_end_date";
                text = RawText(Row, field);
            }
            return (DateParser.TryParse(text), field);
        }
    }

    private static string RawText(DataRow? row, string field) => row?.GetEvaluationField(field).RawValue.Trim() ?? "";

    private Dictionary<(string Source, string Task), RelationshipTask[]> BuildRelationshipTaskLookup(XerTable? tasks)
    {
        var result = new Dictionary<(string Source, string Task), RelationshipTask[]>();
        if (!IsTableValid(tasks)) return result;
        foreach (var group in tasks.Rows.GroupBy(row => (Source: row.SourceToken, Task: RawText(row, "task_id"))))
            if (group.Key.Task.Length > 0)
                result.Add(group.Key, group.Select(row => new RelationshipTask(row)).ToArray());
        return result;
    }

    private static RelationshipTask? ResolveRelationshipTask(
        Dictionary<(string Source, string Task), RelationshipTask[]> tasks, string source, string id, string project)
    {
        if (!tasks.TryGetValue((source, id.Trim()), out var candidates) || candidates.Length != 1) return null;
        // Project filtering cannot legitimise duplicate source-local native IDs.
        var task = candidates[0];
        return string.IsNullOrWhiteSpace(project) || task.ProjectId == project.Trim() ? task : null;
    }

    private sealed record RelationshipCalendar(WorkingDayCalculator? Calculator, decimal? HoursPerDay,
        string? Failure = null, XerRawField HoursPerDayInput = default);

    private Dictionary<(string Source, string Calendar), RelationshipCalendar> BuildRelationshipCalendars()
    {
        var result = new Dictionary<(string Source, string Calendar), RelationshipCalendar>();
        XerTable? raw = _dataStore.GetTable(TableNames.Calendar);
        if (!IsTableValid(raw)) return result;
        P6CalendarRepository repository;
        try { repository = GetResilientCalendarRepository(); }
        catch (InvalidDataException ex)
        {
            Console.WriteLine($"Relationship calendars unavailable: {ex.Message}");
            return result;
        }
        foreach (var key in raw.Rows.Select(row => (Source: row.SourceToken,
                     Calendar: GetFieldValue(row.Fields, raw.FieldIndexes, FieldNames.ClndrId).Trim())).Distinct())
        {
            if (key.Calendar.Length == 0) continue;
            try
            {
                var calendar = repository.Get(key.Source, key.Calendar);
                var sourceRow = raw.Rows.Single(row => row.SourceToken == key.Source
                    && GetFieldValue(row.Fields, raw.FieldIndexes, FieldNames.ClndrId).Trim() == key.Calendar);
                result.Add(key, new(calendar.CreateCalculator(), calendar.HoursPerDay,
                    HoursPerDayInput: sourceRow.GetEvaluationField("day_hr_cnt")));
            }
            catch (InvalidDataException ex)
            {
                // Keep the actual failure distinct from an absent identity. Remove only
                // the internal occurrence annotation before publishing an audit message.
                string failure = ex.Message.Replace($" (input occurrence '{key.Source}')", "", StringComparison.Ordinal);
                Console.WriteLine($"Relationship calendar unavailable: {failure}");
                result.Add(key, new(null, null, failure));
            }
        }
        return result;
    }
}
