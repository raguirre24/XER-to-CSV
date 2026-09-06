namespace XerToCsvConverter;

/// <summary>Describes legitimate empty Standard results, without inventing missing raw schemas.</summary>
internal static class StandardExportSchema
{
    internal static string? SourceTable(string name) => name.ToUpperInvariant() switch
    {
        EnhancedTableNames.XerTask01 or EnhancedTableNames.XerBaseline04 => TableNames.Task,
        EnhancedTableNames.XerProject02 => TableNames.Project,
        EnhancedTableNames.XerProjWbs03 => TableNames.ProjWbs,
        EnhancedTableNames.XerPredecessor06 => TableNames.TaskPred,
        EnhancedTableNames.XerActvType07 => TableNames.ActvType,
        EnhancedTableNames.XerActvCode08 => TableNames.ActvCode,
        EnhancedTableNames.XerTaskActv09 => TableNames.TaskActv,
        EnhancedTableNames.XerCalendar10 or EnhancedTableNames.XerCalendarDetailed11 => TableNames.Calendar,
        EnhancedTableNames.XerRsrc12 => TableNames.Rsrc,
        EnhancedTableNames.XerTaskRsrc13 or EnhancedTableNames.XerResourceDist15 => TableNames.TaskRsrc,
        EnhancedTableNames.XerUmeasure14 => TableNames.Umeasure,
        _ => null
    };

    internal static XerTable? CreateIfSourceEmpty(XerDataStore store, string name)
    {
        string? sourceName = SourceTable(name);
        if (sourceName is null || store.GetTable(sourceName) is not { Headers: not null, IsEmpty: true } source)
            return null;
        return CreateEmpty(store, name);
    }

    internal static XerTable CreateEmpty(XerDataStore store, string name)
    {
        string sourceName = SourceTable(name) ?? throw new InvalidDataException($"Unknown Enhanced table '{name}'.");
        var source = store.GetTable(sourceName);
        string[] headers = name.ToUpperInvariant() switch
        {
            EnhancedTableNames.XerTask01 or EnhancedTableNames.XerBaseline04 => XerTransformer.TaskColumns01,
            EnhancedTableNames.XerCalendarDetailed11 =>
            [
                FieldNames.ClndrId, FieldNames.CalendarName, FieldNames.CalendarType,
                FieldNames.Date, FieldNames.DayOfWeek, FieldNames.WorkingDay, FieldNames.WorkHours,
                FieldNames.ExceptionType, FieldNames.ClndrIdKey, FieldNames.MonthUpdate,
                FieldNames.DayOfWeekNum, FieldNames.WorkingDayInt
            ],
            EnhancedTableNames.XerResourceDist15 => XerTransformer.ResourceDistributionColumns,
            _ => CreateHeaders(source?.Headers ?? [], AddedFields(name))
        };
        var result = new XerTable(name);
        result.SetHeaders(headers.ToArray());
        return result;
    }

    internal static string[] CreateHeaders(string[] sourceHeaders, string[] derived)
    {
        derived = derived.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        var reserved = new HashSet<string>(derived.Append(FieldNames.FileName), StringComparer.OrdinalIgnoreCase);
        var occupied = new HashSet<string>(sourceHeaders.Concat(reserved), StringComparer.OrdinalIgnoreCase);
        string[] headers = sourceHeaders.ToArray();
        for (int index = 0; index < headers.Length; index++)
        {
            if (!reserved.Contains(headers[index])) continue;
            string original = headers[index], replacement = "raw_" + original;
            int suffix = 2;
            while (!occupied.Add(replacement)) replacement = "raw_" + original + "_" + suffix++;
            headers[index] = replacement;
        }
        return headers.Concat(derived).ToArray();
    }

    internal static string[] AddedFields(string name) => name.ToUpperInvariant() switch
    {
        EnhancedTableNames.XerProject02 => [FieldNames.ProjIdKey, FieldNames.MonthUpdate],
        EnhancedTableNames.XerProjWbs03 => [FieldNames.WbsIdKey, FieldNames.ParentWbsIdKey, FieldNames.MonthUpdate],
        EnhancedTableNames.XerPredecessor06 =>
        [
            FieldNames.TaskIdKey, FieldNames.PredTaskIdKey, FieldNames.CalendarIdKey,
            FieldNames.PredecessorClndrIdKey, FieldNames.StatusCode, FieldNames.PredecessorStatusCode,
            FieldNames.TaskType, FieldNames.PredecessorTaskType, FieldNames.Lag,
            FieldNames.TimePeriodHoursPerDay, FieldNames.Start, FieldNames.Finish,
            FieldNames.PredecessorStart, FieldNames.PredecessorFinish, FieldNames.PredecessorFreeFloat,
            FieldNames.TotalFloat, FieldNames.MonthUpdate
        ],
        EnhancedTableNames.XerActvType07 => [FieldNames.ActvCodeTypeIdKey, FieldNames.MonthUpdate],
        EnhancedTableNames.XerActvCode08 => [FieldNames.ActvCodeIdKey, FieldNames.ActvCodeTypeIdKey, FieldNames.MonthUpdate],
        EnhancedTableNames.XerTaskActv09 => [FieldNames.ActvCodeIdKey, FieldNames.TaskIdKey, FieldNames.MonthUpdate],
        EnhancedTableNames.XerCalendar10 => [FieldNames.ClndrIdKey, FieldNames.MonthUpdate],
        EnhancedTableNames.XerRsrc12 => [FieldNames.RsrcIdKey, FieldNames.ClndrIdKey, FieldNames.UnitIdKey, FieldNames.MonthUpdate],
        EnhancedTableNames.XerTaskRsrc13 => [FieldNames.RsrcIdKey, FieldNames.TaskIdKey, FieldNames.MonthUpdate],
        EnhancedTableNames.XerUmeasure14 => [FieldNames.UnitIdKey, FieldNames.MonthUpdate],
        _ => throw new InvalidDataException($"Unknown Standard table '{name}'.")
    };
}
