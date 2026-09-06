namespace XerToCsvConverter;

public partial class XerTransformer
{
    private sealed record RelationshipTask(DataRow Row)
    {
        public string ProjectId => RawText(Row, "proj_id");
        public string CalendarId => RawText(Row, "clndr_id");
        public string Status => RawText(Row, "status_code").ToUpperInvariant();
        public string Type => RawText(Row, "task_type").ToUpperInvariant();
    }

    private static string RawText(DataRow? row, string field) => row?.GetEvaluationField(field).RawValue.Trim() ?? "";
    private static DateTime? RawDate(DataRow? row, string field) => DateParser.TryParse(RawText(row, field));

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
        try { repository = new P6CalendarRepository(_dataStore, isolateInvalidIdentities: true); }
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
