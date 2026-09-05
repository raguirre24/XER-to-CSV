namespace XerToCsvConverter;

public partial class XerTransformer
{
    // Relationship movement uses remaining endpoints and stricter progress guards
    // than the status-aware activity display dates.
    private sealed record RelationshipTask(
        string ProjectId, string CalendarId, string Status, string Type,
        DateTime? Start, DateTime? Finish, bool HasRemainingFinish, bool ValidState);

    private Dictionary<(string Source, string Task), RelationshipTask[]> BuildRelationshipTaskLookup(
        XerTable? tasks)
    {
        var result = new Dictionary<(string Source, string Task), RelationshipTask[]>();
        if (!IsTableValid(tasks)) return result;
        var indexes = tasks.FieldIndexes;
        foreach (var group in tasks.Rows.GroupBy(row => (Source: row.SourceToken,
                     Task: GetFieldValue(row.Fields, indexes, FieldNames.TaskId).Trim())))
        {
            if (string.IsNullOrWhiteSpace(group.Key.Task)) continue;
            result.Add(group.Key, group.Select(row =>
            {
                string Read(string field) => GetFieldValue(row.Fields, indexes, field);
                string project = Read(FieldNames.ProjectId).Trim();
                string status = Read(FieldNames.StatusCode).Trim().ToUpperInvariant();
                DateTime? start = ReadEndpoint(FieldNames.RestartDate, FieldNames.EarlyStartDate);
                DateTime? finish = ReadEndpoint(FieldNames.ReendDate, FieldNames.EarlyEndDate);
                string actualStart = Read(FieldNames.ActStartDate);
                string actualFinish = Read(FieldNames.ActEndDate);
                bool validState = !string.IsNullOrWhiteSpace(project)
                    && !(start.HasValue && finish.HasValue && finish < start)
                    && string.IsNullOrWhiteSpace(actualFinish)
                    && (status != "TK_NOTSTART" || string.IsNullOrWhiteSpace(actualStart));
                return new RelationshipTask(project, Read(FieldNames.CalendarId).Trim(), status,
                    Read(FieldNames.TaskType).Trim().ToUpperInvariant(), start, finish,
                    !string.IsNullOrWhiteSpace(Read(FieldNames.ReendDate))
                        && DateParser.TryParse(Read(FieldNames.ReendDate)).HasValue,
                    validState);

                DateTime? ReadEndpoint(string remaining, string early)
                {
                    string raw = Read(remaining);
                    // A malformed nonblank preferred date is NOT a missing date.
                    return DateParser.TryParse(string.IsNullOrWhiteSpace(raw) ? Read(early) : raw);
                }
            }).ToArray());
        }
        return result;
    }

    private static RelationshipTask? ResolveRelationshipTask(
        Dictionary<(string Source, string Task), RelationshipTask[]> tasks,
        string source, string id, string project)
    {
        if (!tasks.TryGetValue((source, id.Trim()), out var candidates)) return null;
        // Public task keys are source/native-ID qualified, not project/native-ID
        // qualified. Duplicate source-local task IDs cannot identify a safe edge,
        // even if filtering the malformed duplicates by project selects one row.
        if (candidates.Length != 1) return null;
        // Older exports can omit the project field; only a unique source-local endpoint
        // is then usable. Neither native IDs nor filenames are cross-source identities.
        var matches = string.IsNullOrWhiteSpace(project) ? candidates
            : candidates.Where(task => task.ProjectId == project.Trim()).ToArray();
        return matches.Length == 1 ? matches[0] : null;
    }

    private sealed record RelationshipCalendar(WorkingDayCalculator Calculator, decimal? HoursPerDay);

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
        // Tuple identities cannot collide because of punctuation or case in source tokens.
        foreach (var key in raw.Rows.Select(row => (Source: row.SourceToken,
                     Calendar: GetFieldValue(row.Fields, raw.FieldIndexes, FieldNames.ClndrId).Trim())).Distinct())
        {
            if (key.Calendar.Length == 0) continue;
            try
            {
                var calendar = repository.Get(key.Source, key.Calendar);
                result.Add(key, new(calendar.CreateCalculator(), calendar.HoursPerDay));
            }
            catch (InvalidDataException ex)
            {
                // Missing, malformed or duplicate calendars affect only their dependants.
                Console.WriteLine($"Relationship calendar unavailable: {ex.Message}");
            }
        }
        return result;
    }
}
