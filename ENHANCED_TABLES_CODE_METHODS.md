# Enhanced Tables Implementation Code

## Table of Contents
0. [Foundation & Shared Helpers](#0-foundation--shared-helpers)
1. [01_XER_TASK](#1-01_xer_task)
2. [02_XER_PROJECT](#2-02_xer_project)
3. [03_XER_PROJWBS](#3-03_xer_projwbs)
4. [04_XER_BASELINE](#4-04_xer_baseline)
5. [06_XER_PREDECESSOR](#5-06_xer_predecessor)
6. [07_XER_ACTVTYPE, 08_XER_ACTVCODE, 09_XER_TASKACTV](#6-07_xer_actvtype-08_xer_actvcode-09_xer_taskactv)
7. [10_XER_CALENDAR](#7-10_xer_calendar)
8. [11_XER_CALENDAR_DETAILED](#8-11_xer_calendar_detailed)
9. [12_XER_RSRC, 13_XER_TASKRSRC, 14_XER_UMEASURE](#9-12_xer_rsrc-13_xer_taskrsrc-14_xer_umeasure)
10. [15_XER_RESOURCE_DISTRIBUTION](#10-15_xer_resource_distribution)

---

## 0. Foundation & Shared Helpers

### 0.1 Key Generation (`CreateKey`)
```csharp
private static string CreateKey(string filename, string value)
{
    if (string.IsNullOrEmpty(filename) || string.IsNullOrEmpty(value)) return string.Empty;
    return StringInternPool.Intern($"{filename.Trim()}.{value.Trim()}");
}
```

### 0.2 MonthUpdate Extraction (`ParseMonthUpdateFromFilename`)
```csharp
[GeneratedRegex("""^(\d{4})""", RegexOptions.Singleline)]
private static partial Regex MonthUpdateRegex();

private string ParseMonthUpdateFromFilename(string filename)
{
    if (string.IsNullOrWhiteSpace(filename)) return string.Empty;

    var match = MonthUpdateRegex().Match(filename.Trim());
    if (match.Success)
    {
        ReadOnlySpan<char> yymm = match.Groups[1].Value.AsSpan();
        if (yymm.Length == 4 &&
            int.TryParse(yymm[..2], out int year) &&
            int.TryParse(yymm.Slice(2, 2), out int month))
        {
            year += 2000;
            if (month >= 1 && month <= 12)
            {
                try
                {
                    return new DateTime(year, month, 1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                }
                catch (ArgumentOutOfRangeException)
                {
                    return string.Empty;
                }
            }
        }
    }
    return string.Empty;
}
```

### 0.3 Date Parser (`DateParser`)
```csharp
public static class DateParser
{
    private static readonly ConcurrentDictionary<string, DateTime?> DateCache = new();

    private static readonly string[] P6Formats = {
        "d/M/yyyy", "dd/MM/yyyy", "M/d/yyyy", "MM/dd/yyyy",
        "yyyy-MM-dd", "dd-MMM-yy", "dd-MMM-yyyy",
        "d/M/yyyy H:mm:ss", "dd/MM/yyyy HH:mm:ss", "M/d/yyyy h:mm:ss tt",
        "yyyy-MM-dd HH:mm:ss"
    };

    public const string OutputFormat = "yyyy-MM-dd HH:mm:ss";

    public static DateTime? TryParse(string? dateStr)
    {
        if (string.IsNullOrWhiteSpace(dateStr)) return null;

        return DateCache.GetOrAdd(dateStr, str =>
        {
            if (DateTime.TryParseExact(str, P6Formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))
            {
                if (result.Year > 1900 && result.Year < 2200) return result;
            }
            if (DateTime.TryParse(str, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
            {
                if (result.Year > 1900 && result.Year < 2200) return result;
            }
            return null;
        });
    }

    public static string Format(DateTime? date) =>
        date.HasValue && date.Value != DateTime.MinValue
            ? date.Value.ToString(OutputFormat, CultureInfo.InvariantCulture)
            : "";

    public static string Format(DateTime date) =>
        date != DateTime.MinValue
            ? date.ToString(OutputFormat, CultureInfo.InvariantCulture)
            : "";

    public static void ClearCache() => DateCache.Clear();
}
```

### 0.4 Field Access Helpers
```csharp
private static string GetFieldValue(string[] row, IReadOnlyDictionary<string, int> indexes, string fieldName)
{
    if (row == null || indexes == null) return "";
    if (indexes.TryGetValue(fieldName, out int index) && index >= 0 && index < row.Length)
    {
        return row[index] ?? "";
    }
    return "";
}

private void SetTransformedField(string[] transformedRow, IReadOnlyDictionary<string, int> finalIndexes, string fieldName, string value)
{
    if (finalIndexes.TryGetValue(fieldName, out int index))
    {
        transformedRow[index] = value ?? string.Empty;
    }
}
```

### 0.5 Header Collision Handling (`CreateEnhancedHeaders`)
```csharp
internal static class StandardExportSchema
{
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
}

private string[] CreateEnhancedHeaders(XerTable source, string tableName, IEnumerable<string> additions)
{
    string[] derived = additions.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    return StandardExportSchema.CreateHeaders(source.Headers ?? [], derived);
}
```

### 0.6 Generic Keyed Table Pipeline (`CreateSimpleKeyedTable`)
```csharp
private XerTable? CreateSimpleKeyedTable(string sourceTableName, string newTableName, List<Tuple<string, string>> keyMappings)
{
    ClearGenerationFailure(newTableName);
    var sourceTable = _dataStore.GetTable(sourceTableName);
    if (!IsTableValid(sourceTable)) return null;

    try
    {
        if (sourceTable.Headers is not { } sourceHeaders) return null;

        var sourceIndexes = sourceTable.FieldIndexes;
        var finalHeadersList = new List<string>();
        foreach (var mapping in keyMappings)
        {
            finalHeadersList.Add(mapping.Item1);
        }
        finalHeadersList.Add(FieldNames.MonthUpdate);

        string[] finalHeaders = CreateEnhancedHeaders(sourceTable, newTableName, finalHeadersList);
        var finalIndexes = finalHeaders
            .Select((name, index) => new { name, index })
            .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);

        var resultTable = new XerTable(newTableName, sourceTable.RowCount);
        resultTable.SetHeaders(finalHeaders);

        var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
        var transformedRows = new DataRow[sourceTable.RowCount];

        Parallel.For(0, sourceTable.RowCount, parallelOptions, rowIndex =>
        {
            DataRow sourceRow = sourceTable.Rows[rowIndex];
            var row = sourceRow.Fields;
            string[] transformed = new string[finalHeaders.Length];
            string originalFilename = sourceRow.SourceFilename;

            int copyLength = Math.Min(row.Length, sourceHeaders.Length);
            Array.Copy(row, transformed, copyLength);

            foreach (var mapping in keyMappings)
            {
                if (sourceIndexes.ContainsKey(mapping.Item2))
                {
                    string sourceValue = GetFieldValue(row, sourceIndexes, mapping.Item2);
                    string key = CreateKey(originalFilename, sourceValue);
                    SetTransformedField(transformed, finalIndexes, mapping.Item1, key);
                }
            }

            SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(sourceRow.OriginalSourceFilename));

            for (int k = 0; k < transformed.Length; k++)
            {
                transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);
            }

            transformedRows[rowIndex] = sourceRow.WithFields(transformed);
        });

        resultTable.AddRows(transformedRows);
        RecordSimpleIdentityWarnings(sourceTable, newTableName);
        return resultTable;
    }
    catch (Exception ex)
    {
        RecordGenerationFailure(newTableName, ex);
        return null;
    }
}
```

---

## 1. 01_XER_TASK

### 1.1 Schema & Table Generation
```csharp
internal static readonly string[] TaskColumns01 = {
    FieldNames.TaskId, FieldNames.ProjectId, FieldNames.WbsId, FieldNames.CalendarId,
    FieldNames.TaskType, FieldNames.StatusCode, FieldNames.TaskCode, FieldNames.TaskName,
    FieldNames.RsrcId, FieldNames.ActStartDate, FieldNames.ActEndDate,
    FieldNames.EarlyStartDate, FieldNames.EarlyEndDate, FieldNames.LateStartDate, FieldNames.LateEndDate,
    FieldNames.TargetStartDate, FieldNames.TargetEndDate,
    FieldNames.CstrType, FieldNames.CstrDate, FieldNames.PriorityType, FieldNames.FloatPath,
    FieldNames.FloatPathOrder, FieldNames.DrivingPathFlag,
    FieldNames.RemainDurationHrCnt, FieldNames.PhysCompletePct,
    FieldNames.Start, FieldNames.Finish, FieldNames.IdName,
    FieldNames.RemainingDuration, FieldNames.OriginalDuration, FieldNames.TotalFloat, FieldNames.FreeFloat,
    FieldNames.PercentComplete, FieldNames.DataDate,
    FieldNames.WbsIdKey, FieldNames.TaskIdKey, FieldNames.CalendarIdKey, FieldNames.ProjIdKey,
    FieldNames.MonthUpdate
};

public XerTable? Create01XerTaskTable()
{
    ClearGenerationFailure(EnhancedTableNames.XerTask01);

    var taskTable = _dataStore.GetTable(TableNames.Task);
    var calendarTable = _dataStore.GetTable(TableNames.Calendar);
    var projectTable = _dataStore.GetTable(TableNames.Project);

    if (!IsTableValid(taskTable)) return null;

    try
    {
        var taskIndexes = taskTable.FieldIndexes;
        var calendarHours = BuildCalendarHoursLookup(calendarTable);
        var projectDataDates = BuildProjectDataDatesLookup(projectTable);

        string[] finalColumns = TaskColumns01;
        var finalIndexes = finalColumns
            .Select((name, index) => new { name, index })
            .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);

        var finalTable = new XerTable(EnhancedTableNames.XerTask01, taskTable.RowCount);
        finalTable.SetHeaders(finalColumns.Select(s => StringInternPool.Intern(s) ?? string.Empty).ToArray());

        const string TK_Complete = "TK_Complete";
        const string TK_NotStart = "TK_NotStart";
        const string TK_Active = "TK_Active";

        var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
        var transformedRows = new DataRow[taskTable.RowCount];

        Parallel.For(0, taskTable.RowCount, parallelOptions, rowIndex =>
        {
            DataRow sourceRowData = taskTable.Rows[rowIndex];
            string[] row = sourceRowData.Fields;
            string[] transformed = new string[finalColumns.Length];
            string originalFilename = sourceRowData.SourceFilename;

            string projId = GetFieldValue(row, taskIndexes, FieldNames.ProjectId);
            string taskId = GetFieldValue(row, taskIndexes, FieldNames.TaskId);
            string wbsId = GetFieldValue(row, taskIndexes, FieldNames.WbsId);
            string clndrId = GetFieldValue(row, taskIndexes, FieldNames.CalendarId);
            string statusCode = GetFieldValue(row, taskIndexes, FieldNames.StatusCode);
            DateTime? actEndDate = DateParser.TryParse(GetFieldValue(row, taskIndexes, FieldNames.ActEndDate));

            CopyDirectFields(row, taskIndexes, transformed, finalIndexes);
            FormatDateFields(row, taskIndexes, transformed, finalIndexes, actEndDate);

            SetTransformedField(transformed, finalIndexes, FieldNames.StatusCode,
                StringInternPool.Intern(
                    statusCode == TK_Complete ? "Complete" :
                    statusCode == TK_NotStart ? "Not Started" :
                    statusCode == TK_Active ? "In Progress" : statusCode)
                );

            DateTime startDate = CalculateStartDate(row, taskIndexes, statusCode);
            DateTime finishDate = CalculateFinishDate(row, taskIndexes, statusCode);
            SetTransformedField(transformed, finalIndexes, FieldNames.Start, DateParser.Format(startDate));
            SetTransformedField(transformed, finalIndexes, FieldNames.Finish, DateParser.Format(finishDate));

            string taskCode = GetFieldValue(row, taskIndexes, FieldNames.TaskCode);
            string taskName = GetFieldValue(row, taskIndexes, FieldNames.TaskName);
            SetTransformedField(transformed, finalIndexes, FieldNames.IdName, $"{taskCode} - {taskName}");

            string calendarKey = CreateKey(originalFilename, clndrId);
            if (!string.IsNullOrEmpty(clndrId) && calendarHours.TryGetValue((sourceRowData.SourceToken, clndrId.Trim()), out decimal dayHrCnt) && dayHrCnt > 0)
            {
                SetTransformedField(transformed, finalIndexes, FieldNames.RemainingDuration,
                    CalculateDaysFromHours(row, taskIndexes, FieldNames.RemainDurationHrCnt, dayHrCnt, 2));
                SetTransformedField(transformed, finalIndexes, FieldNames.OriginalDuration,
                    CalculateDaysFromHours(row, taskIndexes, FieldNames.TargetDurationHrCnt, dayHrCnt, 2));

                if (statusCode != TK_Complete)
                {
                    SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat,
                        CalculateDaysFromHours(row, taskIndexes, FieldNames.TotalFloatHrCnt, dayHrCnt, 2));
                    SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloat,
                        CalculateDaysFromHours(row, taskIndexes, FieldNames.FreeFloatHrCnt, dayHrCnt, 2));
                }
                else
                {
                    SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat, "");
                    SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloat, "");
                }
            }
            else
            {
                SetTransformedField(transformed, finalIndexes, FieldNames.RemainingDuration, "");
                SetTransformedField(transformed, finalIndexes, FieldNames.OriginalDuration, "");
                SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat, "");
                SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloat, "");
            }

            decimal? pct = CalculateCompletionPercentage(row, taskIndexes, statusCode);
            SetTransformedField(transformed, finalIndexes, FieldNames.PercentComplete,
                pct?.ToString("F2", CultureInfo.InvariantCulture) ?? "");

            string projectKey = CreateKey(originalFilename, projId);
            if (!string.IsNullOrEmpty(projId) && projectDataDates.TryGetValue((sourceRowData.SourceToken, projId.Trim()), out DateTime dataDateValue))
            {
                SetTransformedField(transformed, finalIndexes, FieldNames.DataDate, DateParser.Format(dataDateValue));
            }
            else
            {
                SetTransformedField(transformed, finalIndexes, FieldNames.DataDate, "");
            }

            SetTransformedField(transformed, finalIndexes, FieldNames.WbsIdKey, CreateKey(originalFilename, wbsId));
            SetTransformedField(transformed, finalIndexes, FieldNames.TaskIdKey, CreateKey(originalFilename, taskId));
            SetTransformedField(transformed, finalIndexes, FieldNames.CalendarIdKey, CreateKey(originalFilename, clndrId));
            SetTransformedField(transformed, finalIndexes, FieldNames.ProjIdKey, CreateKey(originalFilename, projId));

            SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(sourceRowData.OriginalSourceFilename));

            for (int k = 0; k < transformed.Length; k++)
            {
                transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);
            }

            transformedRows[rowIndex] = sourceRowData.WithFields(transformed);
        });

        finalTable.AddRows(transformedRows);
        RecordTaskCalculationWarnings(taskTable, finalTable, calendarHours, projectDataDates);
        return finalTable;
    }
    catch (Exception ex)
    {
        RecordGenerationFailure(EnhancedTableNames.XerTask01, ex);
        return null;
    }
}
```

### 1.2 Lookups & Formatters
```csharp
private Dictionary<(string Source, string Id), decimal> BuildCalendarHoursLookup(XerTable? table)
{
    var result = new Dictionary<(string Source, string Id), decimal>();
    if (!IsTableValid(table)) return result;
    foreach (var group in table.Rows.GroupBy(row => (row.SourceToken,
                 GetFieldValue(row.Fields, table.FieldIndexes, FieldNames.ClndrId).Trim())))
    {
        if (group.Key.Item2.Length == 0 || group.Count() != 1) continue;
        string raw = GetFieldValue(group.Single().Fields, table.FieldIndexes, FieldNames.DayHourCount);
        if (decimal.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal hours) && hours > 0)
            result.Add(group.Key, hours);
    }
    return result;
}

private Dictionary<(string Source, string Id), DateTime> BuildProjectDataDatesLookup(XerTable? table)
{
    var result = new Dictionary<(string Source, string Id), DateTime>();
    if (!IsTableValid(table)) return result;
    foreach (var group in table.Rows.GroupBy(row => (row.SourceToken,
                 GetFieldValue(row.Fields, table.FieldIndexes, FieldNames.ProjectId).Trim())))
    {
        if (group.Key.Item2.Length == 0 || group.Count() != 1) continue;
        DateTime? date = DateParser.TryParse(GetFieldValue(group.Single().Fields,
            table.FieldIndexes, FieldNames.LastRecalcDate));
        if (date.HasValue) result.Add(group.Key, date.Value);
    }
    return result;
}

private void CopyDirectFields(string[] sourceRow, IReadOnlyDictionary<string, int> sourceIndexes, string[] targetRow, IReadOnlyDictionary<string, int> targetIndexes)
{
    foreach (var kvp in targetIndexes)
    {
        string colName = kvp.Key;
        int targetIndex = kvp.Value;
        if (sourceIndexes.TryGetValue(colName, out int srcIndex) && !HandledTaskFields.Contains(colName))
        {
            if (srcIndex < sourceRow.Length)
            {
                targetRow[targetIndex] = sourceRow[srcIndex];
            }
        }
    }
}

private void FormatDateFields(string[] sourceRow, IReadOnlyDictionary<string, int> sourceIndexes, string[] transformedRow, IReadOnlyDictionary<string, int> targetIndexes, DateTime? actEndDate)
{
    string[] directFormatCols = {
        FieldNames.CstrDate, FieldNames.TargetStartDate, FieldNames.TargetEndDate,
        FieldNames.ActStartDate, FieldNames.EarlyStartDate, FieldNames.EarlyEndDate,
        FieldNames.LateStartDate, FieldNames.LateEndDate
    };

    foreach (string colName in directFormatCols)
    {
        if (targetIndexes.TryGetValue(colName, out int targetIndex))
        {
            string originalValue = GetFieldValue(sourceRow, sourceIndexes, colName);
            transformedRow[targetIndex] = DateParser.Format(DateParser.TryParse(originalValue));
        }
    }

    if (targetIndexes.TryGetValue(FieldNames.ActEndDate, out int actEndIdxTarget))
    {
        transformedRow[actEndIdxTarget] = DateParser.Format(actEndDate);
    }
}
```

### 1.3 Dates, Durations & Completion Percentages
```csharp
private static DateTime? ReadPreferredDate(string[] row, IReadOnlyDictionary<string, int> indexes,
    string preferred, string fallback)
{
    string raw = GetFieldValue(row, indexes, preferred);
    return DateParser.TryParse(string.IsNullOrWhiteSpace(raw) ? GetFieldValue(row, indexes, fallback) : raw);
}

private DateTime CalculateStartDate(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode) =>
    (statusCode.Trim().ToUpperInvariant() switch
    {
        "TK_NOTSTART" => ReadPreferredDate(row, indexes, FieldNames.RestartDate, FieldNames.EarlyStartDate),
        "TK_ACTIVE" or "TK_COMPLETE" => DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActStartDate)),
        _ => null
    }) ?? DateTime.MinValue;

private DateTime CalculateFinishDate(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode) =>
    (statusCode.Trim().ToUpperInvariant() switch
    {
        "TK_COMPLETE" => DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActEndDate)),
        "TK_NOTSTART" or "TK_ACTIVE" => ReadPreferredDate(row, indexes, FieldNames.ReendDate, FieldNames.EarlyEndDate),
        _ => null
    }) ?? DateTime.MinValue;

private string CalculateDaysFromHours(string[] row, IReadOnlyDictionary<string, int> indexes, string hourFieldName, decimal hoursPerDay, int decimalPlaces = 1)
{
    if (hoursPerDay <= 0) return "";
    string hourStr = GetFieldValue(row, indexes, hourFieldName);
    if (decimal.TryParse(hourStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal hours))
    {
        decimal days;
        try { days = hours / hoursPerDay; }
        catch (OverflowException) { return ""; }
        return days.ToString($"F{decimalPlaces}", CultureInfo.InvariantCulture);
    }
    return "";
}

private decimal? CalculateCompletionPercentage(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode)
{
    if (string.Equals(statusCode, "TK_Complete", StringComparison.OrdinalIgnoreCase)) return 100m;
    if (string.Equals(statusCode, "TK_NotStart", StringComparison.OrdinalIgnoreCase)) return 0m;
    if (!string.Equals(statusCode, "TK_Active", StringComparison.OrdinalIgnoreCase)) return null;

    decimal? ReadNumber(string field, bool optional = false)
    {
        string raw = GetFieldValue(row, indexes, field);
        if (optional && string.IsNullOrWhiteSpace(raw)) return 0m;
        return decimal.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal number)
            && number >= 0 ? number : null;
    }
    try
    {
        switch (GetFieldValue(row, indexes, FieldNames.CompletePctType).Trim().ToUpperInvariant())
        {
            case "CP_PHYS":
                decimal? physical = ReadNumber(FieldNames.PhysCompletePct);
                return physical.HasValue ? Math.Clamp(physical.Value, 0m, 100m) : null;
            case "CP_UNITS":
                decimal? actual = ReadNumber(FieldNames.ActWorkQty, true) + ReadNumber("act_equip_qty", true);
                decimal? remaining = ReadNumber(FieldNames.RemainWorkQty, true) + ReadNumber("remain_equip_qty", true);
                if (!actual.HasValue || !remaining.HasValue) return null;
                decimal total = actual.Value + remaining.Value;
                return total == 0 ? 0m : Math.Clamp(actual.Value / total * 100m, 0m, 100m);
            case "CP_DRTN":
                decimal? target = ReadNumber(FieldNames.TargetDurationHrCnt);
                decimal? remainingDuration = ReadNumber(FieldNames.RemainDurationHrCnt);
                if (!target.HasValue || !remainingDuration.HasValue || target.Value == 0) return null;
                return Math.Clamp((1m - remainingDuration.Value / target.Value) * 100m, 0m, 100m);
            default: return null;
        }
    }
    catch (OverflowException) { return null; }
}
```

---

## 2. 02_XER_PROJECT

```csharp
public XerTable? Create02XerProject() => CreateSimpleKeyedTable(
    TableNames.Project,
    EnhancedTableNames.XerProject02,
    new List<Tuple<string, string>> { Tuple.Create(FieldNames.ProjIdKey, FieldNames.ProjectId) }
);
```

---

## 3. 03_XER_PROJWBS

```csharp
public XerTable? Create03XerProjWbsTable() => Create03XerProjWbsTableCore();

private XerTable? Create03XerProjWbsTableCore()
{
    ClearGenerationFailure(EnhancedTableNames.XerProjWbs03);
    XerTable? source = _dataStore.GetTable(TableNames.ProjWbs);
    if (!IsTableValid(source)) return null;

    string Read(DataRow row, string field) => GetFieldValue(row.Fields, source.FieldIndexes, field).Trim();

    var nodes = source.Rows.GroupBy(row => (Source: row.SourceToken, Id: Read(row, FieldNames.WbsId)))
        .ToDictionary(group => group.Key, group => group.ToArray());

    var parents = new Dictionary<(string Source, string Id), (string Source, string Id)>();
    foreach (var pair in nodes.Where(pair => pair.Key.Id.Length > 0 && pair.Value.Length == 1))
    {
        DataRow row = pair.Value[0];
        string parentId = Read(row, FieldNames.ParentWbsId);
        var parentKey = (pair.Key.Source, parentId);
        if (parentId.Length == 0 || !nodes.TryGetValue(parentKey, out var parentRows) || parentRows.Length != 1) continue;
        if (Read(row, FieldNames.ProjectId) != Read(parentRows[0], FieldNames.ProjectId)) continue;
        parents.Add(pair.Key, parentKey);
    }

    var cyclic = new HashSet<(string Source, string Id)>();
    var complete = new HashSet<(string Source, string Id)>();
    foreach (var key in parents.Keys)
    {
        var path = new List<(string Source, string Id)>();
        var positions = new Dictionary<(string Source, string Id), int>();
        var current = key;
        while (!complete.Contains(current))
        {
            if (positions.TryGetValue(current, out int cycleStart))
            {
                cyclic.UnionWith(path.Skip(cycleStart));
                break;
            }
            positions.Add(current, path.Count);
            path.Add(current);
            if (!parents.TryGetValue(current, out current)) break;
        }
        complete.UnionWith(path);
    }

    string[] headers = CreateEnhancedHeaders(source, EnhancedTableNames.XerProjWbs03,
        new[] { FieldNames.WbsIdKey, FieldNames.ParentWbsIdKey, FieldNames.MonthUpdate });
    var result = new XerTable(EnhancedTableNames.XerProjWbs03, source.RowCount);
    result.SetHeaders(headers);

    var ordinals = new Dictionary<string, int>(StringComparer.Ordinal);
    foreach (DataRow row in source.Rows)
    {
        int ordinal = ordinals[row.SourceToken] = ordinals.GetValueOrDefault(row.SourceToken) + 1;
        string id = Read(row, FieldNames.WbsId);
        string parentId = Read(row, FieldNames.ParentWbsId);
        var key = (Source: row.SourceToken, Id: id);
        string? issue = null;
        string? message = null;
        bool usableId = id.Length > 0 && nodes[key].Length == 1;
        bool usableParent = parents.TryGetValue(key, out var parent);

        if (!usableId)
        {
            issue = "WBS_IDENTITY_INVALID";
            message = id.Length == 0
                ? "PROJWBS.wbs_id is blank; its derived identity and parent key are unknown."
                : $"PROJWBS.wbs_id '{id}' is duplicated within this input; competing rows are retained without selecting an identity.";
            usableParent = false;
        }
        else if (cyclic.Contains(key))
        {
            issue = "WBS_PARENT_CYCLE";
            message = $"WBS '{id}' participates in a parent cycle; its parent key is blank.";
            usableParent = false;
        }
        else if (parentId.Length > 0 && !usableParent)
        {
            issue = "WBS_PARENT_UNRESOLVED";
            message = $"WBS '{id}' parent '{parentId}' is absent, ambiguous or belongs to another project; its parent key is blank.";
        }

        string[] values = new string[headers.Length];
        Array.Copy(row.Fields, values, Math.Min(row.Fields.Length, source.Headers!.Length));
        values[^3] = usableId ? CreateKey(row.SourceFilename, id) : "";
        values[^2] = usableParent ? CreateKey(row.SourceFilename, parent.Id) : "";
        values[^1] = ParseMonthUpdateFromFilename(row.OriginalSourceFilename);
        result.AddRow(row.WithFields(values));

        if (issue is not null)
            RecordDataQualityWarning(EnhancedTableNames.XerProjWbs03, issue, message!, row,
                ordinal, TableNames.ProjWbs, usableId ? FieldNames.ParentWbsId : FieldNames.WbsId,
                usableId ? parentId : id, XerDataQuality.RawRowJson(source, row));
    }
    return result;
}
```

---

## 4. 04_XER_BASELINE

```csharp
public XerTable? Create04XerBaselineTable(XerTable task01Table)
{
    ClearGenerationFailure(EnhancedTableNames.XerBaseline04);

    if (!IsTableValid(task01Table)) return null;

    try
    {
        var baselineTable = new XerTable(EnhancedTableNames.XerBaseline04, task01Table.RowCount);
        if (task01Table.Headers is null) return null;
        baselineTable.SetHeaders(task01Table.Headers);

        if (!task01Table.FieldIndexes.TryGetValue(FieldNames.MonthUpdate, out int monthUpdateIndex)) return null;

        DateTime? minMonthUpdate = null;
        foreach (var rowData in task01Table.Rows)
        {
            var currentDate = DateParser.TryParse(XerTable.GetFieldValueSafe(rowData, monthUpdateIndex));
            if (currentDate.HasValue)
            {
                if (!minMonthUpdate.HasValue || currentDate.Value.Date < minMonthUpdate.Value.Date)
                {
                    minMonthUpdate = currentDate.Value.Date;
                }
            }
        }

        if (!minMonthUpdate.HasValue)
        {
            var empty = new XerTable(EnhancedTableNames.XerBaseline04);
            empty.SetHeaders(task01Table.Headers!.ToArray());
            return empty;
        }

        foreach (var sourceRow in task01Table.Rows)
        {
            var rowDate = DateParser.TryParse(XerTable.GetFieldValueSafe(sourceRow, monthUpdateIndex));
            if (rowDate.HasValue && rowDate.Value.Date == minMonthUpdate.Value)
            {
                baselineTable.AddRow(sourceRow);
            }
        }

        RecordBaselineCalculationWarnings(baselineTable);
        return baselineTable;
    }
    catch (Exception ex)
    {
        RecordGenerationFailure(EnhancedTableNames.XerBaseline04, ex);
        return null;
    }
}
```

---

## 5. 06_XER_PREDECESSOR

### 5.1 Main Pipeline & Lookups
```csharp
private struct TaskData
{
    public string? ProjectId;
    public string? CalendarId;
    public string? ClndrIdKey;
    public string? StatusCode;
    public string? TaskType;
    public DateTime? DisplayStartDate;
    public DateTime? DisplayFinishDate;
    public string? TotalFloatHrCnt;
}

private Dictionary<(string Source, string Task), TaskData[]> BuildTaskLookupDictionary(XerTable? table)
{
    var result = new Dictionary<(string Source, string Task), TaskData[]>();
    if (!IsTableValid(table)) return result;
    foreach (var group in table.Rows.GroupBy(row => (row.SourceToken,
                 GetFieldValue(row.Fields, table.FieldIndexes, FieldNames.TaskId).Trim())))
    {
        if (group.Key.Item2.Length == 0) continue;
        result.Add(group.Key, group.Select(row =>
        {
            string Read(string field) => GetFieldValue(row.Fields, table.FieldIndexes, field);
            string status = Read(FieldNames.StatusCode).Trim();
            DateTime start = CalculateStartDate(row.Fields, table.FieldIndexes, status);
            DateTime finish = CalculateFinishDate(row.Fields, table.FieldIndexes, status);
            return new TaskData
            {
                ProjectId = Read(FieldNames.ProjectId).Trim(),
                CalendarId = Read(FieldNames.ClndrId).Trim(),
                ClndrIdKey = CreateKey(row.SourceFilename, Read(FieldNames.ClndrId)),
                StatusCode = status,
                TaskType = Read(FieldNames.TaskType),
                DisplayStartDate = start == DateTime.MinValue ? null : start,
                DisplayFinishDate = finish == DateTime.MinValue ? null : finish,
                TotalFloatHrCnt = Read(FieldNames.TotalFloatHrCnt)
            };
        }).ToArray());
    }
    return result;
}

private static TaskData ResolveDisplayTask(Dictionary<(string Source, string Task), TaskData[]> lookup,
    string source, string taskId, string projectId)
{
    if (!lookup.TryGetValue((source, taskId.Trim()), out var rows) || rows.Length != 1) return new();
    TaskData task = rows[0];
    if (string.IsNullOrWhiteSpace(task.ProjectId)
        || (!string.IsNullOrWhiteSpace(projectId) && task.ProjectId != projectId.Trim())) return new();
    return task;
}

public XerTable? Create06XerPredecessor(ConcurrentDictionary<string, XerTable> cache)
{
    ClearGenerationFailure(EnhancedTableNames.XerPredecessor06);

    var taskPredTable = _dataStore.GetTable(TableNames.TaskPred);
    var taskTable = _dataStore.GetTable(TableNames.Task);
    var calendarTable = _dataStore.GetTable(TableNames.Calendar);
    var schedOptionsTable = _dataStore.GetTable("SCHEDOPTIONS");

    if (!IsTableValid(taskPredTable)) return null;

    try
    {
        if (taskPredTable.Headers is not { } sourceHeaders) return null;

        var sourceIndexes = taskPredTable.FieldIndexes;
        var finalHeadersList = new List<string>
        {
            FieldNames.TaskIdKey,
            FieldNames.PredTaskIdKey,
            FieldNames.CalendarIdKey,
            FieldNames.PredecessorClndrIdKey,
            FieldNames.StatusCode,
            FieldNames.PredecessorStatusCode,
            FieldNames.TaskType,
            FieldNames.PredecessorTaskType,
            FieldNames.Lag,
            FieldNames.TimePeriodHoursPerDay,
            FieldNames.Start,
            FieldNames.Finish,
            FieldNames.PredecessorStart,
            FieldNames.PredecessorFinish,
            FieldNames.PredecessorFreeFloat,
            FieldNames.FreeFloatStatus,
            FieldNames.FreeFloatBasis,
            FieldNames.FreeFloatReason,
            FieldNames.TotalFloat,
            FieldNames.MonthUpdate
        };

        string[] finalHeaders = CreateEnhancedHeaders(taskPredTable, EnhancedTableNames.XerPredecessor06, finalHeadersList);
        var finalIndexes = finalHeaders
            .Select((name, index) => new { name, index })
            .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);

        var resultTable = new XerTable(EnhancedTableNames.XerPredecessor06, taskPredTable.RowCount);
        resultTable.SetHeaders(finalHeaders);

        var taskLookup = BuildTaskLookupDictionary(taskTable);
        var calendarHoursLookup = BuildCalendarHoursLookup(calendarTable);
        var relationshipCalendars = BuildRelationshipCalendars();
        var relationshipTasks = BuildRelationshipTaskLookup(taskTable);
        var schedOptionsLookup = BuildProjectScheduleOptionsLookup(schedOptionsTable);

        var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
        var transformedRows = new DataRow[taskPredTable.RowCount];
        int[] relationshipOrdinals = SourceOrdinals(taskPredTable);

        Parallel.For(0, taskPredTable.RowCount, parallelOptions, rowIndex =>
        {
            DataRow sourceRow = taskPredTable.Rows[rowIndex];
            var row = sourceRow.Fields;
            string[] transformed = new string[finalHeaders.Length];
            string originalFilename = sourceRow.SourceFilename;

            int copyLength = Math.Min(row.Length, sourceHeaders.Length);
            Array.Copy(row, transformed, copyLength);

            string taskId = GetFieldValue(row, sourceIndexes, FieldNames.TaskId);
            string predTaskId = GetFieldValue(row, sourceIndexes, FieldNames.PredTaskId);

            TaskData succTask = ResolveDisplayTask(taskLookup, sourceRow.SourceToken, taskId,
                GetFieldValue(row, sourceIndexes, FieldNames.ProjectId));
            TaskData predTask = ResolveDisplayTask(taskLookup, sourceRow.SourceToken, predTaskId,
                GetFieldValue(row, sourceIndexes, "pred_proj_id"));

            string taskIdKey = succTask.ProjectId is null ? "" : CreateKey(originalFilename, taskId);
            string predTaskIdKey = predTask.ProjectId is null ? "" : CreateKey(originalFilename, predTaskId);

            SetTransformedField(transformed, finalIndexes, FieldNames.TaskIdKey, taskIdKey);
            SetTransformedField(transformed, finalIndexes, FieldNames.PredTaskIdKey, predTaskIdKey);

            string succClndrIdKey = succTask.ClndrIdKey ?? string.Empty;
            string predClndrIdKey = predTask.ClndrIdKey ?? string.Empty;

            SetTransformedField(transformed, finalIndexes, FieldNames.CalendarIdKey, succClndrIdKey);
            SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorClndrIdKey, predClndrIdKey);
            SetTransformedField(transformed, finalIndexes, FieldNames.StatusCode, succTask.StatusCode ?? string.Empty);
            SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorStatusCode, predTask.StatusCode ?? string.Empty);
            SetTransformedField(transformed, finalIndexes, FieldNames.TaskType, succTask.TaskType ?? string.Empty);
            SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorTaskType, predTask.TaskType ?? string.Empty);
            SetTransformedField(transformed, finalIndexes, FieldNames.Start, DateParser.Format(succTask.DisplayStartDate));
            SetTransformedField(transformed, finalIndexes, FieldNames.Finish, DateParser.Format(succTask.DisplayFinishDate));
            SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorStart, DateParser.Format(predTask.DisplayStartDate));
            SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorFinish, DateParser.Format(predTask.DisplayFinishDate));

            decimal hoursPerDay = 0;
            if (!string.IsNullOrEmpty(predClndrIdKey) && calendarHoursLookup.TryGetValue((sourceRow.SourceToken, predTask.CalendarId ?? ""), out decimal hpd))
            {
                hoursPerDay = hpd;
            }
            SetTransformedField(transformed, finalIndexes, FieldNames.TimePeriodHoursPerDay, hoursPerDay > 0 ? hoursPerDay.ToString("F2", CultureInfo.InvariantCulture) : "");

            string lagHrCntStr = GetFieldValue(row, sourceIndexes, FieldNames.LagHrCnt);
            decimal lagHours = 0m;
            bool validLag = string.IsNullOrWhiteSpace(lagHrCntStr)
                || decimal.TryParse(lagHrCntStr, NumberStyles.Float, CultureInfo.InvariantCulture, out lagHours);

            string lagDays = !validLag ? "" : lagHours == 0 ? "0"
                : FormatRelationshipDays(lagHours, hoursPerDay, "F2");
            SetTransformedField(transformed, finalIndexes, FieldNames.Lag, lagDays);

            decimal hoursPerDayForSuccessor = 0;
            if (calendarHoursLookup.TryGetValue((sourceRow.SourceToken, succTask.CalendarId ?? ""), out decimal succHpd))
            {
                hoursPerDayForSuccessor = succHpd;
            }

            var assessment = AssessRelationship(sourceRow, relationshipTasks, relationshipCalendars,
                schedOptionsLookup, relationshipOrdinals[rowIndex], false, default);
            string freeFloatInDays = assessment.FormattedDays;
            if (assessment.Classification is RelationshipFloatClassification.InvalidData
                or RelationshipFloatClassification.MissingData or RelationshipFloatClassification.Unsupported)
                RecordDataQualityWarning(EnhancedTableNames.XerPredecessor06,
                    "RELATIONSHIP_" + assessment.ReasonCode, assessment.Message, sourceRow,
                    relationshipOrdinals[rowIndex], TableNames.TaskPred, "free_float", "",
                    XerDataQuality.RawRowJson(taskPredTable, sourceRow));

            SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorFreeFloat, freeFloatInDays);
            SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloatStatus, assessment.AllowanceStatus.ToString());
            SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloatBasis, assessment.CalculationBasis);
            SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloatReason, assessment.ReasonCode);

            string totalFloatVal = "";
            if (!string.Equals(succTask.StatusCode, "TK_Complete", StringComparison.OrdinalIgnoreCase) &&
                !string.IsNullOrEmpty(succTask.TotalFloatHrCnt) && hoursPerDayForSuccessor > 0)
            {
                if (decimal.TryParse(succTask.TotalFloatHrCnt, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal tfHours))
                {
                    totalFloatVal = FormatRelationshipDays(tfHours, hoursPerDayForSuccessor, "F1");
                }
            }
            SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat, totalFloatVal);

            SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(sourceRow.OriginalSourceFilename));

            for (int k = 0; k < transformed.Length; k++)
            {
                transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);
            }

            transformedRows[rowIndex] = sourceRow.WithFields(transformed);
        });

        resultTable.AddRows(transformedRows);
        return resultTable;
    }
    catch (Exception ex)
    {
        RecordGenerationFailure(EnhancedTableNames.XerPredecessor06, ex);
        return null;
    }
}
```

### 5.2 Relationship Assessment Logic
```csharp
public enum RelationshipFloatClassification
{
    Calculated, Ignored, Historical, Unsupported, MissingData, InvalidData
}

/// <summary>Meaning of the relationship allowance, independent of the legacy audit classification.</summary>
public enum RelationshipAllowanceStatus
{
    Finite, Estimated, NoFiniteBound, Historical, FixedEvent, RequiresContext, MissingData, InvalidData
}

/// <summary>Original input evidence; State also distinguishes malformed typed values.</summary>
public sealed record RelationshipFieldEvidence(string RawValue, string State)
{
    // Parsed-data callers can edit existing cells. Keep original evidence distinct
    // from the supplied evaluation value without repeating identical evidence.
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? EvaluationValue { get; init; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? EvaluationState { get; init; }
}

/// <summary>
/// A relationship-only remaining predecessor allowance, evaluated under exported settings.
/// This is neither a reschedule nor a native P6 driving classification. No internal token
/// is exposed; public namespace and the source-local row ordinal identify an occurrence.
/// </summary>
public sealed record RelationshipFloatAssessment
{
    public string FileName { get; init; } = "";
    public string SourceNamespace { get; init; } = "";
    public int SourceRowNumber { get; init; }
    public string RelationshipId { get; init; } = "";
    public string RelationshipIdKey { get; init; } = "";
    public string ProjectIdKey { get; init; } = "";
    public string PredecessorProjectIdKey { get; init; } = "";
    public string SuccessorIdKey { get; init; } = "";
    public string PredecessorIdKey { get; init; } = "";
    public string RelationshipType { get; init; } = "";
    public string PredecessorStatus { get; init; } = "";
    public string SuccessorStatus { get; init; } = "";
    public string SchedulingMode { get; init; } = "Unresolved";
    public string SsLagBasis { get; init; } = "Unresolved";
    public string LagCalendarSetting { get; init; } = "";
    public string LagCalendarKey { get; init; } = "";
    public string PredecessorCalendarKey { get; init; } = "";
    public string RawLag { get; init; } = "";
    public decimal? EffectiveLagHours { get; init; }
    public DateTime? ProjectDataDate { get; init; }
    public DateTime? PredecessorEndpoint { get; init; }
    public DateTime? SuccessorEndpoint { get; init; }
    public string PredecessorEndpointField { get; init; } = "";
    public string SuccessorEndpointField { get; init; } = "";
    public decimal? PredecessorHoursPerDay { get; init; }
    public decimal? FloatHours { get; init; }
    public decimal? FloatDays { get; init; }
    public RelationshipFloatClassification Classification { get; init; }
    public RelationshipAllowanceStatus AllowanceStatus { get; init; } = RelationshipAllowanceStatus.RequiresContext;
    public string CalculationBasis { get; init; } = "None";
    public string ReasonCode { get; init; } = "";
    public string Message { get; init; } = "";
    public IReadOnlyDictionary<string, RelationshipFieldEvidence> InputEvidence { get; init; } =
        new ReadOnlyDictionary<string, RelationshipFieldEvidence>(new Dictionary<string, RelationshipFieldEvidence>());

    public string FormattedDays => Classification == RelationshipFloatClassification.Calculated
        && AllowanceStatus is RelationshipAllowanceStatus.Finite or RelationshipAllowanceStatus.Estimated
        ? FloatDays?.ToString("G29", CultureInfo.InvariantCulture) ?? "" : "";
}

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

private sealed record RelationshipScheduleOptions(
    DataRow? ProjectRow = null, DataRow? OptionRow = null,
    bool ProjectAmbiguous = false, bool OptionsAmbiguous = false)
{
    public bool IsAmbiguous => ProjectAmbiguous || OptionsAmbiguous;
    public string DefaultCalendarId => RawText(ProjectRow, "clndr_id");
    public DateTime? DataDate => RawDate(ProjectRow, "last_recalc_date");
    public string LagCalendar
    {
        get
        {
            var raw = OptionRow?.GetEvaluationField("sched_calendar_on_relationship_lag");
            return raw?.State == XerRawFieldState.Blank ? "rcal_Successor"
                : raw?.State == XerRawFieldState.Present ? raw.Value.RawValue.Trim() : "";
        }
    }
    public string Mode => IsAmbiguous ? "Unresolved" :
        (RawText(OptionRow, "sched_retained_logic").ToUpperInvariant(),
         RawText(OptionRow, "sched_progress_override").ToUpperInvariant()) switch
        {
            ("Y", "N") => "RetainedLogic", ("N", "Y") => "ProgressOverride",
            ("N", "N") => "ActualDates", _ => "Unresolved"
        };
    public string SsBasis => IsAmbiguous ? "Unresolved" : Mode == "ActualDates" ? "NotApplicable" :
        RawText(OptionRow, "sched_lag_early_start_flag").ToUpperInvariant() switch
        { "Y" => "EarlyStart", "N" => "ActualStart", _ => "Unresolved" };
}

private Dictionary<(string Source, string Project), RelationshipScheduleOptions> BuildProjectScheduleOptionsLookup(XerTable? options)
{
    var result = new Dictionary<(string Source, string Project), RelationshipScheduleOptions>();
    var projects = _dataStore.GetTable(TableNames.Project);
    if (IsTableValid(projects))
        foreach (var group in projects.Rows.GroupBy(row => (Source: row.SourceToken, Project: RawText(row, "proj_id"))))
            if (group.Key.Project.Length > 0)
                result[group.Key] = new(ProjectRow: group.First(), ProjectAmbiguous: group.Skip(1).Any());
    if (IsTableValid(options))
        foreach (var group in options.Rows.GroupBy(row => (Source: row.SourceToken, Project: RawText(row, "proj_id"))))
            if (group.Key.Project.Length > 0)
            {
                result.TryGetValue(group.Key, out var prior);
                result[group.Key] = (prior ?? new()) with { OptionRow = group.First(), OptionsAmbiguous = group.Skip(1).Any() };
            }
    return result;
}

/// <summary>One assessment per TASKPRED row in input order. Does not mutate the store or export any files.</summary>
public IReadOnlyList<RelationshipFloatAssessment> AssessRelationships(CancellationToken cancellationToken = default)
{
    cancellationToken.ThrowIfCancellationRequested();
    var relationships = _dataStore.GetTable(TableNames.TaskPred);
    if (!IsTableValid(relationships)) return Array.Empty<RelationshipFloatAssessment>();
    var tasks = BuildRelationshipTaskLookup(_dataStore.GetTable(TableNames.Task));
    var calendars = BuildRelationshipCalendars();
    var options = BuildProjectScheduleOptionsLookup(_dataStore.GetTable("SCHEDOPTIONS"));
    var ordinals = new Dictionary<string, int>(StringComparer.Ordinal);
    var results = new List<RelationshipFloatAssessment>(relationships.RowCount);
    foreach (var row in relationships.Rows)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ordinals.TryGetValue(row.SourceToken, out int ordinal);
        ordinals[row.SourceToken] = ++ordinal;
        results.Add(AssessRelationship(row, tasks, calendars, options, ordinal, true, cancellationToken));
    }
    return results.AsReadOnly();
}

// Existing export entry point uses exactly the same evaluator, without materialising
// the potentially large optional audit evidence for every edge in a browser export.
private string CalculateFreeFloat(DataRow relationship, IReadOnlyDictionary<string, int> predIndexes,
    Dictionary<(string Source, string Task), RelationshipTask[]> tasks,
    Dictionary<(string Source, string Calendar), RelationshipCalendar> calendars,
    Dictionary<(string Source, string Project), RelationshipScheduleOptions> options) =>
    AssessRelationship(relationship, tasks, calendars, options, 0, false, default).FormattedDays;

private RelationshipFloatAssessment AssessRelationship(DataRow row,
    Dictionary<(string Source, string Task), RelationshipTask[]> tasks,
    Dictionary<(string Source, string Calendar), RelationshipCalendar> calendars,
    Dictionary<(string Source, string Project), RelationshipScheduleOptions> scheduleOptions,
    int ordinal, bool includeEvidence, CancellationToken cancellationToken)
{
    cancellationToken.ThrowIfCancellationRequested();
    string Read(string field) => RawText(row, field);
    string Key(string id) => id.Length == 0 ? "" : CreateKey(row.SourceFilename, id);
    string type = Read("pred_type").ToUpperInvariant();
    var successor = ResolveRelationshipTask(tasks, row.SourceToken, Read("task_id"), Read("proj_id"));
    var predecessor = ResolveRelationshipTask(tasks, row.SourceToken, Read("pred_task_id"), Read("pred_proj_id"));
    string successorProject = successor?.ProjectId ?? Read("proj_id");
    string predecessorProject = predecessor?.ProjectId ?? Read("pred_proj_id");
    scheduleOptions.TryGetValue((row.SourceToken, successorProject), out var options);
    scheduleOptions.TryGetValue((row.SourceToken, predecessorProject), out var otherOptions);
    bool external = successorProject.Length > 0 && predecessorProject.Length > 0 && successorProject != predecessorProject;
    var assessment = new RelationshipFloatAssessment
    {
        FileName = row.OriginalSourceFilename, SourceNamespace = row.SourceFilename, SourceRowNumber = ordinal,
        RelationshipId = Read("task_pred_id"), RelationshipIdKey = Key(Read("task_pred_id")),
        ProjectIdKey = Key(successorProject), PredecessorProjectIdKey = Key(predecessorProject),
        SuccessorIdKey = successor is null ? "" : Key(Read("task_id")),
        PredecessorIdKey = predecessor is null ? "" : Key(Read("pred_task_id")),
        RelationshipType = type, PredecessorStatus = predecessor?.Status ?? "", SuccessorStatus = successor?.Status ?? "",
        SchedulingMode = options?.Mode ?? "Unresolved", SsLagBasis = options?.SsBasis ?? "Unresolved",
        LagCalendarSetting = options?.LagCalendar ?? "", RawLag = row.GetRawField("lag_hr_cnt").RawValue,
        ProjectDataDate = options?.DataDate,
        PredecessorCalendarKey = predecessor is null ? "" : Key(predecessor.CalendarId)
    };
    if (includeEvidence)
        assessment = assessment with { InputEvidence = BuildRelationshipEvidence(row, predecessor, successor, otherOptions, options) };
    RelationshipFloatAssessment Result(RelationshipFloatClassification classification, string reason, string message) =>
        assessment with
        {
            Classification = classification, ReasonCode = reason, Message = message,
            AllowanceStatus = classification switch
            {
                RelationshipFloatClassification.Ignored => RelationshipAllowanceStatus.NoFiniteBound,
                RelationshipFloatClassification.Historical => RelationshipAllowanceStatus.Historical,
                RelationshipFloatClassification.MissingData => RelationshipAllowanceStatus.MissingData,
                RelationshipFloatClassification.InvalidData => RelationshipAllowanceStatus.InvalidData,
                _ => RelationshipAllowanceStatus.RequiresContext
            },
            CalculationBasis = classification switch
            {
                RelationshipFloatClassification.Ignored => "IgnoredRelationship",
                RelationshipFloatClassification.Historical => "HistoricalActualEvent",
                RelationshipFloatClassification.Unsupported => "UnresolvedContext",
                _ => "None"
            }
        };
    RelationshipFloatAssessment Missing(string reason, string message) => Result(RelationshipFloatClassification.MissingData, reason, message);
    RelationshipFloatAssessment Invalid(string reason, string message) => Result(RelationshipFloatClassification.InvalidData, reason, message);
    RelationshipFloatAssessment Unsupported(string reason, string message) => Result(RelationshipFloatClassification.Unsupported, reason, message);

    if (external && options is { IsAmbiguous: false } &&
        string.Equals(RawText(options.OptionRow, "sched_outer_depend_type"), "SD_None", StringComparison.OrdinalIgnoreCase))
        return Result(RelationshipFloatClassification.Ignored, "IgnoredExternalRelationship",
            "External relationship is ignored under the successor project's exported settings; no governing scheduling run is inferred.");

    if (predecessor is null || successor is null)
    {
        foreach (string field in new[] { "pred_task_id", "task_id" })
            if (tasks.TryGetValue((row.SourceToken, Read(field)), out var candidates) && candidates.Length != 1)
                return Invalid("AmbiguousEndpointIdentity", "Duplicate source-local task IDs cannot establish a relationship endpoint.");
        return Missing("UnresolvedEndpointIdentity", "An endpoint is missing or its exported project identity does not match; no cross-source matching is performed.");
    }
    if (predecessor.ProjectId.Length == 0 || successor.ProjectId.Length == 0)
        return Missing("MissingProjectIdentity", "Both endpoints need an exported owning project identity.");
    if (predecessor.Status is not ("TK_NOTSTART" or "TK_ACTIVE" or "TK_COMPLETE") ||
        successor.Status is not ("TK_NOTSTART" or "TK_ACTIVE" or "TK_COMPLETE"))
        return Invalid("UnknownActivityStatus", "An endpoint has an unrecognised progress status.");
    if (type is not ("PR_FS" or "PR_SS" or "PR_FF" or "PR_SF"))
        return Unsupported("UnsupportedRelationshipType", "The relationship type is not FS, SS, FF or SF.");
    if (successor.Status == "TK_COMPLETE")
        return Result(RelationshipFloatClassification.Historical, "HistoricalSuccessor", "The successor is complete; there is no remaining successor event for this metric.");
    if (predecessor.Status == "TK_COMPLETE")
        return Result(RelationshipFloatClassification.Historical, "HistoricalFixedPredecessor", "There is no movable remaining predecessor event. A fixed historical release or unexpired lag may still affect the successor.");
    if (predecessor.Type == "TT_WBS" || successor.Type == "TT_WBS")
        return Result(RelationshipFloatClassification.Ignored, "IgnoredSummaryRelationship",
            "Direct WBS-summary relationships are ignored during scheduling; summary rollups are a separate dependency mechanism.");
    if (predecessor.Type is not ("TT_TASK" or "TT_RSRC" or "TT_MILE" or "TT_FINMILE") ||
        successor.Type is not ("TT_TASK" or "TT_RSRC" or "TT_MILE" or "TT_FINMILE"))
        return Unsupported("UnsupportedActivityType", predecessor.Type == "TT_LOE" || successor.Type == "TT_LOE"
            ? "LOE movement depends on its linked activity network; an independent remaining endpoint allowance cannot be established."
            : "The exported activity type has no supported remaining endpoint movement model.");
    foreach (var task in new[] { predecessor, successor })
    {
        if (RawText(task.Row, "act_end_date").Length > 0 ||
            (task.Status == "TK_NOTSTART" && RawText(task.Row, "act_start_date").Length > 0))
            return Invalid("InconsistentActivityState", "Actual dates conflict with the exported unfinished activity status; source dates are not repaired.");
        var start = RemainingEndpoint(task, true);
        var finish = RemainingEndpoint(task, false);
        if (start.Date.HasValue && finish.Date.HasValue && finish.Date < start.Date)
            return Invalid("ReversedRemainingPeriod", "An exported remaining finish precedes its remaining start.");
    }

    // An active predecessor needs the same evidence as an active successor.
    // Validate chronology before scheduling-mode exclusions so contradictory
    // actuals cannot receive a numeric finish allowance or a fixed-event label.
    foreach (var task in new[] { predecessor, successor }.Where(task => task.Status == "TK_ACTIVE"))
    {
        DateTime? actual = RawDate(task.Row, "act_start_date");
        if (!actual.HasValue)
            return Result(RawText(task.Row, "act_start_date").Length > 0
                    ? RelationshipFloatClassification.InvalidData : RelationshipFloatClassification.MissingData,
                "UnresolvedActualStart", "Every active endpoint requires an explicit valid actual start; no source dates are repaired.");
        foreach (bool startEndpoint in new[] { true, false })
        {
            var remaining = RemainingEndpoint(task, startEndpoint);
            if (remaining.Date.HasValue && actual > remaining.Date)
                return Invalid("ActualStartAfterRemainingEndpoint", "An active endpoint's actual start is after its exported remaining start or finish.");
        }
        var owningOptions = ReferenceEquals(task, predecessor) ? otherOptions : options;
        if (owningOptions?.ProjectAmbiguous == true || owningOptions?.DataDate is null)
            return Result(owningOptions?.ProjectAmbiguous == true || RawText(owningOptions?.ProjectRow, "last_recalc_date").Length > 0
                    ? RelationshipFloatClassification.InvalidData : RelationshipFloatClassification.MissingData,
                "UnresolvedProjectDataDate", "Every active endpoint requires a valid unambiguous owning project Data Date.");
        if (actual > owningOptions.DataDate)
            return Unsupported("ActualStartAfterDataDate", "The recorded actual start is after its owning project's Data Date; remaining scheduling context is not established.");
    }
    if (external && (predecessor.Status == "TK_ACTIVE" || successor.Status == "TK_ACTIVE"))
        return Unsupported("UnverifiedMultiProjectProgressContext", "A progressed cross-project relationship needs the governing scheduling project and progress context; matching flags alone is insufficient.");

    if (predecessor.Status == "TK_ACTIVE" && (type == "PR_SF" ||
        (type == "PR_SS" && options is { Mode: "RetainedLogic" or "ProgressOverride", SsBasis: "ActualStart" })))
        return Result(RelationshipFloatClassification.Unsupported, "FixedActualPredecessorStart",
            "The relationship uses the predecessor's fixed actual start; remaining work delay cannot move this event. Any fixed release or unexpired lag is separate from remaining allowance.") with
        {
            AllowanceStatus = RelationshipAllowanceStatus.FixedEvent,
            CalculationBasis = "FixedActualStart",
            PredecessorEndpoint = RawDate(predecessor.Row, "act_start_date"),
            PredecessorEndpointField = "act_start_date"
        };

    var rawLag = row.GetEvaluationField("lag_hr_cnt");
    if (rawLag.State != XerRawFieldState.Present)
        return Missing("MissingRelationshipLag", "An absent, truncated or blank lag cannot establish zero lag.");
    if (!decimal.TryParse(rawLag.RawValue, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal lagHours))
        return Invalid("InvalidRelationshipLag", "Relationship lag is not a finite representable decimal hour value.");
    assessment = assessment with { EffectiveLagHours = lagHours };

    bool activeSuccessor = successor.Status == "TK_ACTIVE";
    if (activeSuccessor)
    {
        if (options?.Mode is null or "Unresolved")
        {
            bool invalid = options?.IsAmbiguous == true ||
                (RawText(options?.OptionRow, "sched_retained_logic").Length > 0 &&
                 RawText(options?.OptionRow, "sched_progress_override").Length > 0);
            return Result(invalid ? RelationshipFloatClassification.InvalidData : RelationshipFloatClassification.MissingData,
                "UnresolvedProgressMode", "Progress-sensitive evaluation requires explicit unambiguous Retained Logic/Progress Override flags; absent flags do not imply Actual Dates.");
        }
        if (options.Mode == "ActualDates")
            return Unsupported("UnsupportedActualDatesProgressCase", "This progressed Actual Dates case is not verified; actual events are never moved or replaced by invented remaining events.");
        if (options.Mode == "ProgressOverride")
            return type == "PR_FS" && lagHours == 0
                ? Result(RelationshipFloatClassification.Ignored, "IgnoredUnderExportedProgressOverride",
                    "Zero-lag FS has an unfinished predecessor and a successor started by the Data Date; ignored under the exported Progress Override setting.")
                : Unsupported("UnsupportedProgressOverrideCase", "This progressed relationship is not the verified zero-lag FS override case; it is not assumed to be ignored.");
        if (type == "PR_SS")
            return Unsupported("UnsupportedProgressedRelationship", "The successor's actual start is fixed; substituting a remaining restart for SS requires an unverified progressed relationship rule.");
    }
    if (predecessor.Status == "TK_ACTIVE" && type is "PR_SS" or "PR_SF")
        return Unsupported("UnsupportedProgressedStartEndpoint", "An actual predecessor start cannot be substituted into the movable remaining-start solver; remaining SS/SF lag semantics are not verified.");

    var from = RemainingEndpoint(predecessor, type is "PR_SS" or "PR_SF");
    var to = RemainingEndpoint(successor, type is "PR_FS" or "PR_SS");
    assessment = assessment with { PredecessorEndpoint = from.Date, PredecessorEndpointField = from.Field,
        SuccessorEndpoint = to.Date, SuccessorEndpointField = to.Field };
    if (from.Date is null || to.Date is null)
    {
        bool malformed = (from.Date is null && predecessor.Row.GetEvaluationField(from.Field).State == XerRawFieldState.Present) ||
            (to.Date is null && successor.Row.GetEvaluationField(to.Field).State == XerRawFieldState.Present);
        return malformed ? Invalid("InvalidRemainingEndpoint", "A selected remaining endpoint is malformed; no actual/early-date repair is made.")
            : Missing("MissingRemainingEndpoint", "A required remaining endpoint is unavailable; active endpoints require explicit remaining dates.");
    }
    if (!calendars.TryGetValue((row.SourceToken, predecessor.CalendarId), out var predecessorCalendar))
        return Missing("UnresolvedPredecessorCalendar", "The required predecessor calendar identity is absent.");
    if (predecessorCalendar.Calculator is null)
        return Invalid("InvalidPredecessorCalendar", predecessorCalendar.Failure ?? "The predecessor calendar cannot be resolved.");
    assessment = assessment with { PredecessorHoursPerDay = predecessorCalendar.HoursPerDay };
    if (predecessorCalendar.HoursPerDay is not > 0)
        return Result(predecessorCalendar.HoursPerDayInput.State == XerRawFieldState.Present
                ? RelationshipFloatClassification.InvalidData : RelationshipFloatClassification.MissingData,
            "UnresolvedPredecessorHoursPerDay", $"A positive predecessor hours-per-day factor is required; supplied day_hr_cnt='{predecessorCalendar.HoursPerDayInput.RawValue}'. Eight hours is not assumed.");
    WorkingDayCalculator lagCalendar = predecessorCalendar.Calculator;
    string lagKey = Key(predecessor.CalendarId);
    if (lagHours != 0)
    {
        if (options?.IsAmbiguous == true || !RelationshipLagCalendarPolicy.TryParse(options?.LagCalendar, out var kind))
            return Result(options?.IsAmbiguous == true || RawText(options?.OptionRow, "sched_calendar_on_relationship_lag").Length > 0
                    ? RelationshipFloatClassification.InvalidData : RelationshipFloatClassification.MissingData,
                "UnresolvedLagCalendarSetting", "Nonzero lag requires a recognised unambiguous lag-calendar setting; missing is not an explicit blank default.");
        if (external && (otherOptions is null || otherOptions.IsAmbiguous ||
            !RelationshipLagCalendarPolicy.TryParse(otherOptions.LagCalendar, out var otherKind) || otherKind != kind ||
            (kind == RelationshipLagCalendar.ProjectDefault && otherOptions.DefaultCalendarId != options!.DefaultCalendarId)))
            return Missing("ConflictingMultiProjectLagContext", "Both projects must resolve the same lag operation; no governing scheduling run is guessed.");
        string calendarId = kind == RelationshipLagCalendar.Successor ? successor.CalendarId : options!.DefaultCalendarId;
        if (kind is RelationshipLagCalendar.Successor or RelationshipLagCalendar.ProjectDefault)
        {
            if (!calendars.TryGetValue((row.SourceToken, calendarId), out var definition))
                return Missing("UnresolvedLagCalendar", "The selected successor/project lag calendar identity is absent.");
            if (definition.Calculator is null)
                return Invalid("InvalidLagCalendar", definition.Failure ?? "The selected lag calendar cannot be resolved.");
            lagCalendar = definition.Calculator;
            lagKey = Key(calendarId);
        }
        else if (kind == RelationshipLagCalendar.TwentyFourHour)
        {
            lagCalendar = ContinuousRelationshipCalendar;
            lagKey = "24Hour";
        }
    }
    assessment = assessment with { LagCalendarKey = lagHours == 0 ? "NotRequired" : lagKey };
    var suspension = ResolveRelationshipSuspension(predecessor);
    if (suspension.Failure.HasValue)
        return Result(suspension.Failure.Value, suspension.Reason!,
            "Predecessor suspension requires active task/resource work, a coherent actual start and valid closed suspend/resume dates. Open or absent bounds are not invented.");
    try
    {
        bool suspensionAdjusted = suspension.Start.HasValue && suspension.Finish > suspension.Start;
        WorkingDayCalculator movementCalendar = suspensionAdjusted
            ? predecessorCalendar.Calculator.WithNonWorkingPeriod(suspension.Start!.Value, suspension.Finish!.Value)
            : predecessorCalendar.Calculator;
        decimal hours = RelationshipFreeFloatCalculator.CalculateHours(movementCalendar, lagCalendar,
            from.Date.Value, to.Date.Value, lagHours, type is "PR_SS" or "PR_SF", cancellationToken);
        decimal days = hours / predecessorCalendar.HoursPerDay.Value;
        bool resourceEstimate = predecessor.Type == "TT_RSRC" || successor.Type == "TT_RSRC";
        return assessment with { Classification = RelationshipFloatClassification.Calculated,
            AllowanceStatus = resourceEstimate ? RelationshipAllowanceStatus.Estimated : RelationshipAllowanceStatus.Finite,
            CalculationBasis = (resourceEstimate, suspensionAdjusted) switch
            {
                (true, true) => "TaskCalendarResourceEstimateSuspensionAdjusted",
                (true, false) => "TaskCalendarResourceEstimate",
                (false, true) => "TaskCalendarSuspensionAdjusted",
                _ => "TaskCalendarRemainingEndpoint"
            },
            ReasonCode = activeSuccessor ? type == "PR_SF" ? "CalculatedRetainedStartToFinish"
                : "CalculatedRetainedRemainingRelationship" : "CalculatedRemainingRelationship",
            FloatHours = hours, FloatDays = days,
            Message = resourceEstimate
                ? "Task-calendar estimate with a fixed successor endpoint; assigned-resource calendars, date rollups and leveling are not simulated."
                : "Signed predecessor-working-time allowance with a fixed successor endpoint and any valid closed suspension removed from movement availability; not a reschedule or native P6 driving flag." };
    }
    catch (Exception ex) when (ex is InvalidDataException or ArgumentOutOfRangeException or OverflowException)
    {
        return Unsupported("UnprojectableCalendarBoundary", $"Calendar boundary could not be verified: {ex.Message}");
    }
}

private static (DateTime? Date, string Field) RemainingEndpoint(RelationshipTask task, bool start)
{
    string field = start ? "restart_date" : "reend_date";
    // Only unstarted activities may use an early date when a preferred value is absent/blank.
    // Malformed nonblank values are authoritative invalid evidence, never repaired.
    if (task.Status == "TK_NOTSTART" && RawText(task.Row, field).Length == 0)
        field = start ? "early_start_date" : "early_end_date";
    return (RawDate(task.Row, field), field);
}

private static (DateTime? Start, DateTime? Finish, RelationshipFloatClassification? Failure, string? Reason)
    ResolveRelationshipSuspension(RelationshipTask task)
{
    string suspendText = RawText(task.Row, "suspend_date"), resumeText = RawText(task.Row, "resume_date");
    if (suspendText.Length == 0 && resumeText.Length == 0) return default;
    if (task.Status != "TK_ACTIVE" || task.Type is not ("TT_TASK" or "TT_RSRC"))
        return (null, null, RelationshipFloatClassification.InvalidData, "InconsistentSuspensionState");
    DateTime? suspend = RawDate(task.Row, "suspend_date"), resume = RawDate(task.Row, "resume_date");
    if ((suspendText.Length > 0 && !suspend.HasValue) || (resumeText.Length > 0 && !resume.HasValue))
        return (null, null, RelationshipFloatClassification.InvalidData, "InvalidSuspensionBounds");
    if (!suspend.HasValue || !resume.HasValue)
        return (null, null, RelationshipFloatClassification.Unsupported, "UnresolvedSuspensionBounds");
    // P6 suspension/resumption takes effect at the beginning of each recorded day.
    // A same-day pair excludes no time; raw intraday clock differences do not reverse it.
    if (resume.Value.Date < suspend.Value.Date)
        return (null, null, RelationshipFloatClassification.InvalidData, "InvalidSuspensionBounds");
    if (suspend.Value.Date < RawDate(task.Row, "act_start_date")!.Value.Date)
        return (null, null, RelationshipFloatClassification.InvalidData, "InconsistentSuspensionState");
    return (suspend.Value.Date, resume.Value.Date, null, null);
}

private static IReadOnlyDictionary<string, RelationshipFieldEvidence> BuildRelationshipEvidence(DataRow relationship,
    RelationshipTask? predecessor, RelationshipTask? successor,
    RelationshipScheduleOptions? predecessorOptions, RelationshipScheduleOptions? successorOptions)
{
    var evidence = new Dictionary<string, RelationshipFieldEvidence>(StringComparer.Ordinal);
    Add("relationship", relationship, ["task_pred_id", "task_id", "pred_task_id", "proj_id", "pred_proj_id", "pred_type", "lag_hr_cnt", "aref", "arls"]);
    foreach (var (prefix, task, options) in new[] { ("predecessor", predecessor, predecessorOptions), ("successor", successor, successorOptions) })
    {
        Add(prefix, task?.Row, ["task_id", "proj_id", "clndr_id", "task_type", "status_code", "act_start_date", "act_end_date",
            "restart_date", "reend_date", "early_start_date", "early_end_date", "remain_drtn_hr_cnt", "suspend_date", "resume_date",
            "expect_end_date", "cstr_type", "cstr_date", "cstr_type2", "cstr_date2"]);
        Add(prefix + "_project", options?.ProjectRow, ["proj_id", "clndr_id", "last_recalc_date"]);
        Add(prefix + "_options", options?.OptionRow, ["sched_retained_logic", "sched_progress_override", "sched_lag_early_start_flag",
            "sched_calendar_on_relationship_lag", "sched_outer_depend_type", "sched_use_expect_end_flag", "level_all_rsrc_flag",
            "level_keep_sched_date_flag", "level_within_float_flag", "sched_float_type", "sched_use_project_end_date_for_float"]);
        evidence[prefix + "_project.identity"] = new("", options?.ProjectAmbiguous == true ? "Ambiguous" : options?.ProjectRow is null ? "Missing" : "Unique");
        evidence[prefix + "_options.identity"] = new("", options?.OptionsAmbiguous == true ? "Ambiguous" : options?.OptionRow is null ? "Missing" : "Unique");
    }
    return new ReadOnlyDictionary<string, RelationshipFieldEvidence>(evidence);

    void Add(string prefix, DataRow? row, string[] fields)
    {
        foreach (string field in fields)
        {
            var raw = row?.GetRawField(field) ?? new XerRawField("", XerRawFieldState.AbsentHeader);
            var evaluation = row?.GetEvaluationField(field) ?? raw;
            evidence[prefix + "." + field] = new(raw.RawValue, EvidenceState(field, raw))
            {
                EvaluationValue = evaluation == raw ? null : evaluation.RawValue,
                EvaluationState = evaluation == raw ? null : EvidenceState(field, evaluation)
            };
        }
    }
}

private static string EvidenceState(string field, XerRawField raw)
{
    if (raw.State != XerRawFieldState.Present) return raw.State.ToString();
    bool invalid = field.EndsWith("_date", StringComparison.Ordinal) || field is "aref" or "arls" or "cstr_date2"
        ? !DateParser.TryParse(raw.RawValue).HasValue
        : field is "lag_hr_cnt" or "remain_drtn_hr_cnt"
            ? !decimal.TryParse(raw.RawValue, NumberStyles.Float, CultureInfo.InvariantCulture, out _)
            : field == "sched_calendar_on_relationship_lag"
                ? !RelationshipLagCalendarPolicy.TryParse(raw.RawValue, out _)
                : field.EndsWith("_flag", StringComparison.Ordinal) || field is "sched_retained_logic" or "sched_progress_override" or "sched_use_project_end_date_for_float"
                    ? raw.RawValue.Trim().ToUpperInvariant() is not ("Y" or "N") : false;
    return invalid ? "Malformed" : "Valid";
}
}
```

### 5.3 Float Engine (`RelationshipFreeFloatCalculator`)
```csharp
/// not exceed a fixed successor endpoint. This is a relationship delay allowance,
/// not an activity float or a classification of P6 driving relationships.
/// </summary>
public static class RelationshipFreeFloatCalculator
{
    public static decimal CalculateHours(WorkingDayCalculator predecessorCalendar,
        WorkingDayCalculator lagCalendar, DateTime predecessorEndpoint,
        DateTime successorEndpoint, decimal lagHours, bool predecessorIsStart,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(predecessorCalendar);
        ArgumentNullException.ThrowIfNull(lagCalendar);
        cancellationToken.ThrowIfCancellationRequested();
        // Match the calendar engine's DateTime precision once, before inversion.
        // Sub-tick input must not accidentally turn into a nonworking-time snap.
        long lagTicks = checked((long)decimal.Round(lagHours * TimeSpan.TicksPerHour, 0,
            MidpointRounding.AwayFromZero));

        DateTime latestEndpoint = FindLatestLagInput(lagCalendar, successorEndpoint,
            lagTicks, cancellationToken);
        // CountWorkingHours is an integral number of time ticks divided by ticks/hour.
        // Restore that integer before adding/subtracting one tick; do not round days.
        long movementTicks = checked((long)decimal.Round(
            predecessorCalendar.CountWorkingHours(predecessorEndpoint, latestEndpoint,
                cancellationToken) * TimeSpan.TicksPerHour, 0));

        DateTime Move(long ticks)
        {
            // An unchanged event retains its exact exported timestamp, including a
            // milestone or finish at a shift start. Canonicalise only moved events.
            if (ticks == 0) return predecessorEndpoint;
            DateTime moved = predecessorCalendar.AddWorkingTicks(predecessorEndpoint,
                ticks, cancellationToken);
            return predecessorIsStart
                ? NextWorkStart(predecessorCalendar, moved, cancellationToken)
                : PreviousWorkFinish(predecessorCalendar, moved, cancellationToken);
        }

        bool IsFeasible(long ticks) => lagCalendar.AddWorkingTicks(Move(ticks),
            lagTicks, cancellationToken) <= successorEndpoint;

        // Working-time integration has no width across nonworking gaps. Its inverse
        // may therefore choose the wrong side of a Start/Finish boundary. At most one
        // work tick must be removed to obtain the greatest representable allowance.
        if (!IsFeasible(movementTicks)) movementTicks = checked(movementTicks - 1);
        if (!IsFeasible(movementTicks) || IsFeasible(checked(movementTicks + 1)))
            throw new InvalidDataException(
                "The relationship delay allowance could not be verified at calendar precision.");

        return movementTicks / (decimal)TimeSpan.TicksPerHour;
    }

    private static DateTime FindLatestLagInput(WorkingDayCalculator calendar,
        DateTime deadline, long lagTicks, CancellationToken cancellationToken)
    {
        if (lagTicks == 0) return deadline;

        // Subtracting lag gives a candidate, not generally the inverse: addition is
        // monotone but flat over nonworking gaps, and jumps at work boundaries.
        DateTime candidate = calendar.AddWorkingTicks(deadline, checked(-lagTicks), cancellationToken);
        if (calendar.AddWorkingTicks(candidate, lagTicks, cancellationToken) > deadline)
        {
            // A negative lag can put the candidate exactly at the beginning of the
            // next feasible work segment, whose backwards projection jumps past a
            // deadline in a nonworking gap. The preceding tick is the last input.
            candidate = candidate.AddTicks(-1);
        }
        else
        {
            // Both signs of lag can produce a plateau from one shift's finish to
            // the next shift's start. The rightmost input, not its first occurrence,
            // is the latest predecessor endpoint permitted by the relationship.
            candidate = NextWorkStart(calendar, candidate, cancellationToken);
        }

        if (calendar.AddWorkingTicks(candidate, lagTicks, cancellationToken) > deadline
            || calendar.AddWorkingTicks(candidate.AddTicks(1), lagTicks, cancellationToken) <= deadline)
            throw new InvalidDataException(
                "The relationship lag inverse could not be verified at calendar precision.");
        return candidate;
    }

    private static DateTime NextWorkStart(WorkingDayCalculator calendar, DateTime endpoint,
        CancellationToken cancellationToken) =>
        calendar.AddWorkingTicks(endpoint, 1, cancellationToken).AddTicks(-1);

    private static DateTime PreviousWorkFinish(WorkingDayCalculator calendar, DateTime endpoint,
        CancellationToken cancellationToken) =>
        calendar.AddWorkingTicks(endpoint, -1, cancellationToken).AddTicks(1);
}
```

### 5.4 Calendar Calculations (`WorkingDayCalculator`)
```csharp
/// <summary>Exact working-time arithmetic over normalized half-open civil-day intervals.</summary>
public sealed class WorkingDayCalculator
{
    private const int MaximumProjectionDays = 366_000;
    private readonly IReadOnlyList<IReadOnlyList<P6WorkInterval>> _week;
    private readonly IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> _exceptions;
    private readonly bool _hasWeeklyWork;
    private readonly DateTime? _nonWorkingStart;
    private readonly DateTime? _nonWorkingFinish;

    /// <summary>An explicit conventional calendar for callers that deliberately request one.</summary>
    public static WorkingDayCalculator Default { get; } = CreateDefault();

    // Retained for existing callers. Availability always comes from slots, never total-hour hints.
    public WorkingDayCalculator(Dictionary<DateTime, decimal> exceptionHours,
        Dictionary<DateTime, List<(TimeSpan Start, TimeSpan End)>> exceptionTimeSlots,
        decimal[] standardWeekHours, List<(TimeSpan Start, TimeSpan End)>[] standardWeekTimeSlots)
    {
        ArgumentNullException.ThrowIfNull(exceptionHours);
        ArgumentNullException.ThrowIfNull(exceptionTimeSlots);
        ArgumentNullException.ThrowIfNull(standardWeekHours);
        ArgumentNullException.ThrowIfNull(standardWeekTimeSlots);
        if (standardWeekHours.Length != 7 || standardWeekTimeSlots.Length != 7)
            throw new ArgumentException("A calendar must provide seven weekdays.");
        var week = standardWeekTimeSlots.Select(slots => (IReadOnlyList<P6WorkInterval>)
            (slots ?? []).Select(ConvertSlot).ToArray()).ToArray();
        var exceptions = exceptionTimeSlots.ToDictionary(pair => pair.Key.Date,
            pair => (IReadOnlyList<P6WorkInterval>)pair.Value.Select(ConvertSlot).ToArray());
        for (int day = 0; day < 7; day++)
            if (standardWeekHours[day] > 0 && week[day].Count == 0)
                throw new InvalidDataException("Positive calendar hours require explicit working intervals.");
        foreach (var pair in exceptionHours)
        {
            if (!exceptions.ContainsKey(pair.Key.Date))
            {
                if (pair.Value > 0)
                    throw new InvalidDataException("Positive exception hours require explicit working intervals.");
                exceptions.Add(pair.Key.Date, Array.Empty<P6WorkInterval>());
            }
        }
        (_week, _exceptions) = P6CalendarNormalization.Resolve(week, exceptions);
        _hasWeeklyWork = _week.Any(day => day.Count > 0);
    }

    internal WorkingDayCalculator(IReadOnlyList<IReadOnlyList<P6WorkInterval>> week,
        IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> exceptions,
        DateTime? nonWorkingStart = null, DateTime? nonWorkingFinish = null)
    {
        _week = week;
        _exceptions = exceptions;
        _hasWeeklyWork = week.Any(day => day.Count > 0);
        _nonWorkingStart = nonWorkingStart;
        _nonWorkingFinish = nonWorkingFinish;
    }

    /// <summary>Activity-specific civil-day exclusion; shared calendar definitions and lag clocks stay unchanged.</summary>
    internal WorkingDayCalculator WithNonWorkingPeriod(DateTime start, DateTime finish)
    {
        if (finish.Date < start.Date)
            throw new ArgumentException("A nonworking period cannot finish before it starts.", nameof(finish));
        return finish.Date == start.Date ? this : new WorkingDayCalculator(_week, _exceptions, start.Date, finish.Date);
    }

    public bool IsWorkingDay(DateTime date) => GetIntervals(date.Date).Count > 0;

    public decimal CountWorkingHours(DateTime startDate, DateTime endDate,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (startDate == endDate) return 0;
        if (endDate < startDate) return -CountWorkingHours(endDate, startDate, cancellationToken);
        decimal ticks = 0;
        DateTime day = startDate.Date;
        while (day <= endDate.Date)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long lower = day == startDate.Date ? startDate.TimeOfDay.Ticks : 0;
            long upper = day == endDate.Date ? endDate.TimeOfDay.Ticks : TimeSpan.TicksPerDay;
            foreach (var interval in GetIntervals(day))
            {
                long left = Math.Max(lower, interval.Start.Ticks);
                long right = Math.Min(upper, interval.End.Ticks);
                if (right > left) ticks += right - left;
            }
            if (day == endDate.Date) break;
            day = day.AddDays(1);
        }
        return ticks / TimeSpan.TicksPerHour;
    }

    public DateTime AddWorkingHours(DateTime startDate, decimal hours,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (hours == 0) return startDate;
        bool forward = hours > 0;
        decimal ticksRemaining;
        try { ticksRemaining = Math.Abs(hours) * TimeSpan.TicksPerHour; }
        catch (OverflowException exception)
        { throw new ArgumentOutOfRangeException(nameof(hours), hours, exception.Message); }
        return ProjectWorkingTicks(startDate, ticksRemaining, forward, cancellationToken);
    }

    /// <summary>
    /// Integral-tick projection for relationship inverse and endpoint-boundary checks.
    /// Avoids decimal hours round trips changing which side of a work boundary is used.
    /// Existing hour-based callers retain their original arithmetic and behaviour.
    /// </summary>
    internal DateTime AddWorkingTicks(DateTime startDate, long ticks,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (ticks == 0) return startDate;
        return ProjectWorkingTicks(startDate, Math.Abs((decimal)ticks), ticks > 0, cancellationToken);
    }

    private DateTime ProjectWorkingTicks(DateTime startDate, decimal ticksRemaining, bool forward,
        CancellationToken cancellationToken)
    {
        if (!_hasWeeklyWork && !_exceptions.Any(pair => pair.Value.Count > 0
                && (forward ? pair.Key >= startDate.Date : pair.Key <= startDate.Date)))
            throw new InvalidDataException("The calendar has no working time in the requested direction.");

        DateTime day = startDate.Date;
        long cursor = startDate.TimeOfDay.Ticks;
        for (int searched = 0; searched <= MaximumProjectionDays; searched++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var intervals = GetIntervals(day);
            for (int index = forward ? 0 : intervals.Count - 1;
                 forward ? index < intervals.Count : index >= 0;
                 index += forward ? 1 : -1)
            {
                var interval = intervals[index];
                long left = forward ? Math.Max(cursor, interval.Start.Ticks) : interval.Start.Ticks;
                long right = forward ? interval.End.Ticks : Math.Min(cursor, interval.End.Ticks);
                long available = right - left;
                if (available <= 0) continue;
                if (ticksRemaining <= available)
                {
                    long offset = checked((long)decimal.Round(ticksRemaining, 0, MidpointRounding.AwayFromZero));
                    long timeOfDay = forward ? left + offset : right - offset;
                    if (timeOfDay > DateTime.MaxValue.Ticks - day.Ticks)
                        throw new InvalidDataException("Calendar projection exceeds the supported date range.");
                    return day.AddTicks(timeOfDay);
                }
                ticksRemaining -= available;
            }
            if (forward ? day == DateTime.MaxValue.Date : day == DateTime.MinValue.Date)
                throw new InvalidDataException("Calendar projection exceeds the supported date range.");
            day = day.AddDays(forward ? 1 : -1);
            cursor = forward ? 0 : TimeSpan.TicksPerDay;
            if (!_hasWeeklyWork && !_exceptions.Any(pair => pair.Value.Count > 0
                    && (forward ? pair.Key >= day : pair.Key <= day)))
                throw new InvalidDataException("The calendar has insufficient working time in the requested direction.");
        }
        throw new InvalidDataException("Calendar projection exceeds the bounded search horizon (1000 years).");
    }

    private IReadOnlyList<P6WorkInterval> GetIntervals(DateTime date) =>
        _nonWorkingStart.HasValue && date >= _nonWorkingStart.Value && date < _nonWorkingFinish!.Value
            ? Array.Empty<P6WorkInterval>()
            : _exceptions.TryGetValue(date, out var intervals) ? intervals : _week[(int)date.DayOfWeek];

    private static P6WorkInterval ConvertSlot((TimeSpan Start, TimeSpan End) slot)
    {
        var end = slot.End;
        if (end < slot.Start || (slot.Start == TimeSpan.Zero && end == TimeSpan.Zero))
            end += TimeSpan.FromDays(1);
        return new(slot.Start, end);
    }

    private static WorkingDayCalculator CreateDefault()
    {
        var week = new IReadOnlyList<P6WorkInterval>[7];
        for (int day = 0; day < 7; day++)
            week[day] = day is >= 1 and <= 5
                ? new[] { new P6WorkInterval(TimeSpan.FromHours(8), TimeSpan.FromHours(12)),
                    new P6WorkInterval(TimeSpan.FromHours(13), TimeSpan.FromHours(17)) }
                : Array.Empty<P6WorkInterval>();
        return new WorkingDayCalculator(week,
            new Dictionary<DateTime, IReadOnlyList<P6WorkInterval>>());
    }
}
```
---

## 6. 07_XER_ACTVTYPE, 08_XER_ACTVCODE, 09_XER_TASKACTV

```csharp
public XerTable? Create07XerActvType() => CreateSimpleKeyedTable(
    TableNames.ActvType,
    EnhancedTableNames.XerActvType07,
    new List<Tuple<string, string>> { Tuple.Create(FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId) }
);

public XerTable? Create08XerActvCode() => CreateSimpleKeyedTable(
    TableNames.ActvCode,
    EnhancedTableNames.XerActvCode08,
    new List<Tuple<string, string>>
    {
        Tuple.Create(FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),
        Tuple.Create(FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId)
    }
);

public XerTable? Create09XerTaskActv() => CreateSimpleKeyedTable(
    TableNames.TaskActv,
    EnhancedTableNames.XerTaskActv09,
    new List<Tuple<string, string>>
    {
        Tuple.Create(FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),
        Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId)
    }
);
```

---

## 7. 10_XER_CALENDAR

```csharp
public XerTable? Create10XerCalendar() => CreateSimpleKeyedTable(
    TableNames.Calendar,
    EnhancedTableNames.XerCalendar10,
    new List<Tuple<string, string>> { Tuple.Create(FieldNames.ClndrIdKey, FieldNames.ClndrId) }
);
```

---

## 8. 11_XER_CALENDAR_DETAILED

### 8.1 Table Generation & Inheritance
```csharp
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
        if (!own.UnknownExceptionDates) parsed[key] = own;
        return own;
    }
}
```

### 8.2 Calendar Parser
```csharp
public readonly record struct P6WorkInterval(TimeSpan Start, TimeSpan End)
{
    public decimal WorkHours => (End - Start).Ticks / (decimal)TimeSpan.TicksPerHour;
}

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

    private static IReadOnlyList<P6WorkInterval> ReadShifts(Node day)
    {
        var intervals = new List<P6WorkInterval>();
        foreach (var node in day.Children)
        {
            if (node.Children.Count != 0)
                throw new InvalidDataException("A calendar shift unexpectedly contains nested records.");
            var fields = node.Attributes.Split('|', StringSplitOptions.TrimEntries);
            if (fields.Length != 4 || !((fields[0] == "s" && fields[2] == "f")
                    || (fields[0] == "f" && fields[2] == "s")))
                throw new InvalidDataException($"Invalid calendar shift '{node.Attributes}'.");
            TimeSpan start = ParseClock(fields[0] == "s" ? fields[1] : fields[3], false);
            TimeSpan end = ParseClock(fields[0] == "f" ? fields[1] : fields[3], true);
            if (end < start || (start == TimeSpan.Zero && end == TimeSpan.Zero))
                end += TimeSpan.FromDays(1);
            if (end > start) intervals.Add(new(start, end));
        }
        return P6CalendarNormalization.Merge(intervals);
    }

    private static TimeSpan ParseClock(string clock, bool allowMidnightEnd)
    {
        var fields = clock.Split(':');
        if (fields.Length != 2 || fields[0].Length is < 1 or > 2 || fields[1].Length != 2
            || !int.TryParse(fields[0], NumberStyles.None, CultureInfo.InvariantCulture, out int hours)
            || !int.TryParse(fields[1], NumberStyles.None, CultureInfo.InvariantCulture, out int minutes)
            || hours > 24 || minutes >= 60 || (hours == 24 && (!allowMidnightEnd || minutes != 0)))
            throw new InvalidDataException($"Invalid calendar clock '{clock}'.");
        return TimeSpan.FromMinutes(hours * 60 + minutes);
    }

    private static IEnumerable<Node> Descendants(IEnumerable<Node> nodes)
    {
        foreach (var node in nodes)
        {
            yield return node;
            foreach (var child in Descendants(node.Children)) yield return child;
        }
    }

    private sealed record Node(string Name, string Attributes, List<Node> Children);

    private sealed class TreeReader(string text, CancellationToken cancellationToken)
    {
        private int _position;
        private int _nodes;

        internal List<Node> Read()
        {
            var nodes = new List<Node>();
            SkipSpace();
            while (_position < text.Length)
            {
                nodes.Add(ReadNode(0));
                SkipSpace();
            }
            return nodes;
        }

        private Node ReadNode(int depth)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (depth > 64 || ++_nodes > 1_000_000)
                throw new InvalidDataException("Calendar structure exceeds the supported nesting or record limit.");
            Expect('(');
            int headerStart = _position;
            while (_position < text.Length && text[_position] != '(')
            {
                if (text[_position] == ')') Fail();
                _position++;
            }
            string header = TrimStructuralSpace(text[headerStart.._position]);
            int separator = header.IndexOf("||", StringComparison.Ordinal);
            if (separator < 1 || !int.TryParse(TrimStructuralSpace(header[..separator]), NumberStyles.None,
                    CultureInfo.InvariantCulture, out _))
                throw new InvalidDataException($"Invalid calendar record header '{header}'.");

            string name = TrimStructuralSpace(header[(separator + 2)..]);
            Expect('(');
            int attributesStart = _position;
            while (_position < text.Length && text[_position] != ')')
            {
                if (text[_position] == '(') Fail();
                _position++;
            }
            string attributes = TrimStructuralSpace(text[attributesStart.._position]);
            Expect(')');
            Expect('(');
            var children = new List<Node>();
            SkipSpace();
            while (_position < text.Length && text[_position] == '(')
            {
                children.Add(ReadNode(depth + 1));
                SkipSpace();
            }
            Expect(')');
            Expect(')');
            return new(name, attributes, children);
        }

        private void Expect(char expected)
        {
            SkipSpace();
            if (_position >= text.Length || text[_position] != expected) Fail();
            _position++;
        }

        private void SkipSpace()
        {
            while (_position < text.Length && IsStructuralSpace(text[_position])) _position++;
        }

        private static bool IsStructuralSpace(char value) => char.IsWhiteSpace(value) || char.IsControl(value);

        private static string TrimStructuralSpace(string value)
        {
            int start = 0;
            int end = value.Length;
            while (start < end && IsStructuralSpace(value[start])) start++;
            while (end > start && IsStructuralSpace(value[end - 1])) end--;
            return value[start..end];
        }

        private void Fail() => throw new InvalidDataException($"Malformed calendar parentheses near character {_position}.");
    }
}
```

### 8.3 Calendar Normalization
```csharp
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

internal static class P6CalendarNormalization
{
    private static readonly TimeSpan Midnight = TimeSpan.FromDays(1);

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
}
```

---

## 9. 12_XER_RSRC, 13_XER_TASKRSRC, 14_XER_UMEASURE

```csharp
public XerTable? Create12XerRsrc() => CreateSimpleKeyedTable(
    TableNames.Rsrc,
    EnhancedTableNames.XerRsrc12,
    new List<Tuple<string, string>>
    {
        Tuple.Create(FieldNames.RsrcIdKey, FieldNames.RsrcId),
        Tuple.Create(FieldNames.ClndrIdKey, FieldNames.ClndrId),
        Tuple.Create(FieldNames.UnitIdKey, FieldNames.UnitId)
    }
);

public XerTable? Create13XerTaskRsrc() => CreateSimpleKeyedTable(
    TableNames.TaskRsrc,
    EnhancedTableNames.XerTaskRsrc13,
    new List<Tuple<string, string>>
    {
        Tuple.Create(FieldNames.RsrcIdKey, FieldNames.RsrcId),
        Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId)
    }
);

public XerTable? Create14XerUmeasure() => CreateSimpleKeyedTable(
    TableNames.Umeasure,
    EnhancedTableNames.XerUmeasure14,
    new List<Tuple<string, string>> { Tuple.Create(FieldNames.UnitIdKey, FieldNames.UnitId) }
);
```

---

## 10. 15_XER_RESOURCE_DISTRIBUTION

### 10.1 Schema & Distribution Pipeline
```csharp
internal static readonly string[] ResourceDistributionColumns =
[
    FieldNames.TaskIdKey, FieldNames.RsrcIdKey, FieldNames.ClndrIdKey, FieldNames.ProjIdKey,
    FieldNames.DistributionMonth, FieldNames.MonthStartDate, FieldNames.MonthEndDate,
    FieldNames.MonthlyQuantity, FieldNames.DistributionType,
    FieldNames.MonthWorkingHours, FieldNames.TotalWorkingHours, FieldNames.CalendarHoursPerDay,
    FieldNames.MonthWorkingDays, FieldNames.TotalWorkingDays,
    FieldNames.MonthCalendarDays, FieldNames.TotalCalendarDays,
    FieldNames.Start, FieldNames.Finish, FieldNames.IsActual, FieldNames.StatusCode, FieldNames.Unit,
    FieldNames.TaskCode, FieldNames.RsrcShortName, FieldNames.RsrcName, FieldNames.RsrcType,
    FieldNames.MonthUpdate
];

public XerTable? Create15XerResourceDistribution()
{
    ClearGenerationFailure(EnhancedTableNames.XerResourceDist15);
    _resourceDataQualityRows = Array.Empty<DataRow>();
    XerTable? assignments = _dataStore.GetTable(TableNames.TaskRsrc);
    if (!IsTableValid(assignments)) return null;
    var tasks = new DistributionInputIndex(_dataStore.GetTable(TableNames.Task), FieldNames.TaskId);
    var projects = new DistributionInputIndex(_dataStore.GetTable(TableNames.Project), FieldNames.ProjectId);
    var resources = new DistributionInputIndex(_dataStore.GetTable(TableNames.Rsrc), FieldNames.RsrcId);
    var units = new DistributionInputIndex(_dataStore.GetTable(TableNames.Umeasure), FieldNames.UnitId);
    P6CalendarRepository? calendars = null;
    ResourceCurveRepository? curves = null;
    var calendarCache = new Dictionary<(string Source, string Id), (P6CalendarDefinition Definition, WorkingDayCalculator Calculator)>();
    var calendarFailures = new Dictionary<(string Source, string Id), InvalidDataException>();
    // All occurrences of a duplicate assignment are ambiguous, not just the later
    // occurrence. Never distribute one arbitrarily by input order.
    var duplicateAssignments = assignments.Rows
        .GroupBy(row => (row.SourceToken, Id: GetFieldValue(row.Fields, assignments.FieldIndexes, "taskrsrc_id").Trim()))
        .Where(group => group.Key.Id.Length > 0 && group.Skip(1).Any()).Select(group => group.Key).ToHashSet();
    var sourceRowNumbers = new Dictionary<string, int>(StringComparer.Ordinal);
    var issues = new List<DataRow>();
    var result = new XerTable(EnhancedTableNames.XerResourceDist15);
    result.SetHeaders(ResourceDistributionColumns.ToArray());

    foreach (DataRow assignment in assignments.Rows)
    {
        string source = assignment.SourceToken;
        sourceRowNumbers.TryGetValue(source, out int sourceRowNumber);
        sourceRowNumbers[source] = ++sourceRowNumber;
        string Read(string field) => GetFieldValue(assignment.Fields, assignments.FieldIndexes, field);
        string assignmentId = Read("taskrsrc_id").Trim();
        string taskId = Read(FieldNames.TaskId).Trim(), resourceId = Read(FieldNames.RsrcId).Trim();
        DistributionQuantity actual = DistributionQuantity.Sum(
            ReadDistributionQuantity(Read(FieldNames.ActRegQty), FieldNames.ActRegQty),
            ReadDistributionQuantity(Read(FieldNames.ActOtQty), FieldNames.ActOtQty));
        DistributionQuantity remaining = ReadDistributionQuantity(Read(FieldNames.RemainQty), FieldNames.RemainQty);
        if (actual.IsZero && remaining.IsZero) continue;

        // Resolve descriptive evidence only when unique. Raw assignment identities
        // remain visible even when they cannot safely resolve a distribution row.
        DataRow? task = tasks.Optional(source, taskId), resource = resources.Optional(source, resourceId);
        string TaskRead(string field) => task.HasValue ? tasks.Read(task.Value, field) : "";
        string ResourceRead(string field) => resource.HasValue ? resources.Read(resource.Value, field) : "";
        string projectId = TaskRead(FieldNames.ProjectId).Trim();
        if (projectId.Length == 0) projectId = Read(FieldNames.ProjectId).Trim();
        DataRow? project = projects.Optional(source, projectId);
        string ProjectRead(string field) => project.HasValue ? projects.Read(project.Value, field) : "";
        string status = TaskRead(FieldNames.StatusCode).Trim(), normalizedStatus = status.ToUpperInvariant();
        string type = TaskRead(FieldNames.TaskType).Trim().ToUpperInvariant();
        string calendarId = (type == "TT_RSRC" ? ResourceRead(FieldNames.ClndrId) : TaskRead(FieldNames.ClndrId)).Trim();
        string resourceType = ResourceRead(FieldNames.RsrcType);
        string unit = resource.HasValue ? "unit/time" : "";
        if (string.Equals(resourceType, "RT_Mat", StringComparison.OrdinalIgnoreCase))
        {
            DataRow? unitRow = units.Optional(source, ResourceRead(FieldNames.UnitId).Trim());
            unit = unitRow.HasValue ? units.Read(unitRow.Value, FieldNames.UnitAbbr) : "";
            if (unit.Length == 0 && unitRow.HasValue) unit = units.Read(unitRow.Value, FieldNames.UnitName);
        }
        var metadata = new DistributionMetadata(assignment, CreateKey(assignment.SourceFilename, taskId),
            CreateKey(assignment.SourceFilename, resourceId), CreateKey(assignment.SourceFilename, calendarId),
            CreateKey(assignment.SourceFilename, projectId), status, TaskRead(FieldNames.TaskCode),
            ResourceRead(FieldNames.RsrcShortName), ResourceRead(FieldNames.RsrcName), resourceType, unit);
        (P6CalendarDefinition Definition, WorkingDayCalculator Calculator) calendar = default;
        bool contextResolved = false;
        string? contextIssue = null, contextMessage = null;

        AddPortion(actual, isActual: true);
        AddPortion(remaining, isActual: false);

        void ResolveContext()
        {
            if (contextResolved) return;
            contextResolved = true;
            string issueCode = "ASSIGNMENT_CONTEXT_INVALID";
            try
            {
                if (assignmentId.Length == 0)
                    throw new InvalidDataException("Assignment identity is missing; its source occurrence is preserved as unallocated.");
                if (duplicateAssignments.Contains((source, assignmentId)))
                    throw new InvalidDataException("Duplicate assignment identity; every occurrence is preserved as unallocated.");
                task = tasks.Require(source, taskId, Read(FieldNames.ProjectId));
                project = projects.Require(source, projectId);
                resource = resources.Require(source, resourceId);
                if (normalizedStatus is not ("TK_NOTSTART" or "TK_ACTIVE" or "TK_COMPLETE"))
                    throw new InvalidDataException($"Unknown activity status '{status}'.");
                if (type is not ("TT_TASK" or "TT_RSRC" or "TT_LOE" or "TT_WBS" or "TT_MILE" or "TT_FINMILE"))
                    throw new InvalidDataException($"Unknown activity type '{type}'.");
                issueCode = "RESOURCE_CALENDAR_INVALID";
                if (!calendarCache.TryGetValue((source, calendarId), out calendar))
                {
                    if (calendarFailures.TryGetValue((source, calendarId), out var priorFailure)) throw priorFailure;
                    try
                    {
                        calendars ??= new P6CalendarRepository(_dataStore, isolateInvalidIdentities: true);
                        var definition = calendars.Get(source, calendarId);
                        calendar = (definition, definition.CreateCalculator());
                        calendarCache.Add((source, calendarId), calendar);
                    }
                    catch (InvalidDataException ex) { calendarFailures[(source, calendarId)] = ex; throw; }
                }
            }
            catch (InvalidDataException ex) { contextIssue = issueCode; contextMessage = ex.Message; }
        }

        void AddPortion(DistributionQuantity quantity, bool isActual)
        {
            if (quantity.IsZero) return;
            if (quantity.Error is not null)
            {
                AddIssue(isActual ? "ACTUAL_QUANTITY_INVALID" : "REMAINING_QUANTITY_INVALID", quantity, isActual, quantity.Error);
                return;
            }
            ResolveContext();
            if (contextIssue is not null)
            {
                AddIssue(contextIssue, quantity, isActual, contextMessage!);
                return;
            }
            var definition = calendar.Definition
                ?? throw new InvalidOperationException("Resolved assignment context has no calendar definition.");
            var calculator = calendar.Calculator
                ?? throw new InvalidOperationException("Resolved assignment context has no working-time calculator.");
            string issueCode = isActual ? "ACTUAL_PERIOD_INVALID" : "REMAINING_PERIOD_INVALID";
            try
            {
                DateTime start, finish;
                RemainingResourceProfile? profile = null;
                RemainingDistributionPlan? remainingPlan = null;
                if (isActual)
                {
                    if (normalizedStatus == "TK_NOTSTART")
                    {
                        issueCode = "ACTUAL_ON_UNSTARTED";
                        throw new InvalidDataException("Actual quantity exists on an unstarted activity; its source state is preserved.");
                    }
                    start = ParseDistributionDate(Read(FieldNames.ActStartDate), FieldNames.ActStartDate);
                    string rawFinish = Read(FieldNames.ActEndDate);
                    finish = !string.IsNullOrWhiteSpace(rawFinish)
                        ? ParseDistributionDate(rawFinish, FieldNames.ActEndDate)
                        : normalizedStatus == "TK_ACTIVE"
                            ? ParseDistributionDate(ProjectRead(FieldNames.LastRecalcDate), "PROJECT.last_recalc_date")
                            : throw new InvalidDataException("Completed actual allocation requires TASKRSRC.act_end_date.");
                    if (finish < start)
                    {
                        issueCode = "ACTUAL_FINISH_BEFORE_START";
                        string finishField = string.IsNullOrWhiteSpace(rawFinish) ? "PROJECT.last_recalc_date" : "TASKRSRC.act_end_date";
                        throw new InvalidDataException($"Actual allocation finish {finishField} '{DateParser.Format(finish)}' precedes TASKRSRC.act_start_date '{DateParser.Format(start)}'. Actual quantity is unallocated; original dates are preserved.");
                    }
                }
                else
                {
                    if (normalizedStatus == "TK_COMPLETE")
                    {
                        issueCode = "REMAINING_ON_COMPLETED";
                        throw new InvalidDataException("Remaining quantity exists on a completed activity; no remaining period or completion month is invented.");
                    }
                    start = ParseDistributionDate(Read(FieldNames.RestartDate), FieldNames.RestartDate);
                    finish = ParseDistributionDate(Read(FieldNames.ReendDate), FieldNames.ReendDate);
                    if (finish <= start)
                        throw new InvalidDataException("Positive remaining quantity requires a finish after its remaining start; original dates are preserved.");
                    issueCode = "REMAINING_NO_WORKING_TIME";
                    if (calculator.CountWorkingHours(start, finish) <= 0)
                        throw new InvalidDataException($"Calendar '{metadata.CalendarKey}' has no working time in the positive-quantity period.");
                    issueCode = "REMAINING_PROFILE_INVALID";
                    remainingPlan = ResolveRemainingProfile(start, finish, quantity.Value!.Value);
                    profile = remainingPlan.Profile;
                }
                issueCode = isActual ? "ACTUAL_DISTRIBUTION_INVALID" : "REMAINING_DISTRIBUTION_INVALID";
                // Commit only a completely reconciled portion. A recoverable source
                // failure cannot leave early-month rows plus its full unallocated units.
                var portionRows = new List<DataRow>();
                AddResourceDistributionRows(portionRows, metadata, definition, calculator,
                    start, finish, quantity.Value!.Value, isActual, profile, remainingPlan?.Clock);
                DataRow? methodNote = remainingPlan?.MethodCode is { } methodCode
                    ? CreateDiagnostic(methodCode, remainingPlan.MethodMessage!, null, "") : null;
                result.AddRows(portionRows);
                if (methodNote.HasValue) issues.Add(methodNote.Value);
            }
            catch (Exception ex) when (ex is InvalidDataException or OverflowException or ArgumentOutOfRangeException)
            {
                AddIssue(issueCode, quantity, isActual, ex.Message);
            }
        }

        RemainingDistributionPlan ResolveRemainingProfile(DateTime start, DateTime finish, decimal quantity)
        {
            string manual = Read("remain_crv");
            if (!string.IsNullOrWhiteSpace(manual))
                return new(RemainingResourceProfile.FromManual(manual, quantity, calendar.Calculator.CountWorkingHours(start, finish)));
            string curveId = Read("curv_id").Trim();
            if (curveId.Length == 0) return new(null);
            if (curveId == "9")
                throw new InvalidDataException("Manual curve '9' requires an exported remain_crv profile.");
            string durationType = TaskRead("duration_type").Trim().ToUpperInvariant();
            if (durationType is not ("DT_FIXEDDRTN" or "DT_FIXEDDUR2"))
                throw new InvalidDataException($"Curve '{curveId}' requires Fixed Duration & Units/Time or Fixed Duration & Units; got duration_type '{durationType}'.");
            curves ??= new ResourceCurveRepository(_dataStore);
            var named = curves.Get(source, curveId);
            if (normalizedStatus == "TK_ACTIVE")
            {
                var clock = new ResourceForecastClock(calendar.Calculator,
                    TaskRead("suspend_date"), TaskRead("resume_date"), finish);
                return ResourceCurveForecast.Resolve(named, clock, start, finish,
                    assignment.GetEvaluationField, ProjectRead(FieldNames.LastRecalcDate), actual.IsZero);
            }
            // Preserve the existing unstarted-activity contract when contradictory
            // assignment actual dates exist. The new forecast applies to active tasks.
            if (!named.IsUniform && (!string.IsNullOrWhiteSpace(Read(FieldNames.ActStartDate))
                || !string.IsNullOrWhiteSpace(Read(FieldNames.ActEndDate)))
                && !ResourceForecastClock.HasSingleWorkingMonth(start, finish,
                    (left, right) => calendar.Calculator.CountWorkingHours(left, right)))
                throw new InvalidDataException($"Curve '{curveId}' on a progressed assignment requires an exported remain_crv profile; its remaining curve phase cannot be established from this XER.");
            return new(named);
        }

        void AddIssue(string code, DistributionQuantity quantity, bool isActual, string message)
        {
            string amount = quantity.Value.HasValue
                ? decimal.Round(quantity.Value.Value, 4, MidpointRounding.ToEven).ToString("F4", CultureInfo.InvariantCulture) : "";
            issues.Add(CreateDiagnostic(code, message, isActual, amount));
        }

        // A null portion is a successful method notice, not an unallocated amount.
        DataRow CreateDiagnostic(string code, string message, bool? isActual, string amount)
        {
            // Repository diagnostics can include private occurrence annotations.
            // Public namespace plus assignment ordinal already supplies provenance.
            message = message.Replace($" (input occurrence '{source}')", "", StringComparison.Ordinal);
            string[] values =
            [
                XerDataQuality.SchemaVersion, "Warning", code, EnhancedTableNames.XerResourceDist15,
                assignment.SourceFilename, sourceRowNumber.ToString(CultureInfo.InvariantCulture), metadata.ProjectKey,
                metadata.TaskKey, metadata.ResourceKey, CreateKey(assignment.SourceFilename, assignmentId), assignmentId,
                metadata.TaskCode, metadata.ResourceName, metadata.ResourceType, metadata.Unit, metadata.Status,
                Read(FieldNames.ActStartDate), Read(FieldNames.ActEndDate), ProjectRead(FieldNames.LastRecalcDate),
                Read(FieldNames.ActRegQty), Read(FieldNames.ActOtQty), isActual == true ? amount : "", message,
                isActual.HasValue ? (isActual.Value ? "Actual" : "Remaining") : "", Read(FieldNames.RestartDate), Read(FieldNames.ReendDate),
                Read(FieldNames.RemainQty), Read("curv_id"), Read("remain_crv"), isActual == false ? amount : "",
                TableNames.TaskRsrc, "", "", XerDataQuality.RawRowJson(assignments, assignment)
            ];
            return assignment.WithFields(values);
        }
    }
    _resourceDataQualityRows = issues.AsReadOnly();
    return result;
}
```

### 10.2 Monthly Bucket Spreading
```csharp

private readonly record struct DistributionQuantity(decimal? Value, string? Error = null)
{
    internal bool IsZero => Error is null && Value == 0;
    internal static DistributionQuantity Sum(DistributionQuantity first, DistributionQuantity second)
    {
        decimal? total;
        try { total = first.Value + second.Value; }
        catch (OverflowException) { return new(null, "Actual regular plus overtime quantity exceeds the supported numeric range; raw components are preserved."); }
        string[] errors = new[] { first.Error, second.Error }.OfType<string>().ToArray();
        return new(total, errors.Length == 0 ? null : string.Join(" ", errors));
    }
}

private static DistributionQuantity ReadDistributionQuantity(string raw, string field)
{
    if (string.IsNullOrWhiteSpace(raw)) return new(0);
    if (!decimal.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal value))
        return new(null, $"{field} must be a finite nonnegative invariant number; got '{raw}'. Raw data is preserved, not treated as zero.");
    return new(value, value < 0 ? $"{field} must be nonnegative; got '{raw}'. The signed source quantity is preserved as unallocated." : null);
}

private static DateTime ParseDistributionDate(string raw, string field) =>
    DateParser.TryParse(raw) ?? throw new InvalidDataException($"{field} requires a valid assignment-period date; got '{raw}'.");

private sealed record DistributionMetadata(DataRow SourceRow, string TaskKey, string ResourceKey,
    string CalendarKey, string ProjectKey, string Status, string TaskCode, string ResourceShortName,
    string ResourceName, string ResourceType, string Unit);

private void AddResourceDistributionRows(List<DataRow> result, DistributionMetadata metadata,
    P6CalendarDefinition definition, WorkingDayCalculator calculator, DateTime start, DateTime finish,
    decimal quantity, bool isActual, RemainingResourceProfile? profile = null, ResourceForecastClock? forecastClock = null)
{
    if (finish < start || (!isActual && finish == start))
        throw new InvalidDataException("Positive quantity requires finish after start, except recorded actuals at a single instant.");
    decimal totalHours = finish == start ? 0 : calculator.CountWorkingHours(start, finish);
    decimal allocationHours = forecastClock?.CountHours(start, finish) ?? totalHours;
    long totalTicks = checked((long)decimal.Round(allocationHours * TimeSpan.TicksPerHour, 0));
    if (totalTicks <= 0 && !isActual)
        throw new InvalidDataException($"Calendar '{metadata.CalendarKey}' has no working time in the positive-quantity period.");
    // Actual units are historical observations, not calendar capacity. A valid
    // calendar can have no scheduled work in their recorded period (overtime,
    // weekends, milestones). Preserve these actuals in their recorded month, or
    // estimate multi-month shares by elapsed time only when ALL work hours are zero.
    // Never apply this fallback to forecast remaining units or malformed calendars.
    bool pointActual = isActual && finish == start;
    bool elapsedActual = isActual && totalTicks == 0 && !pointActual;
    string distributionType = pointActual ? "Actual Recorded Date"
        : elapsedActual ? "Actual Elapsed Time" : profile?.DistributionType ?? "Working Hours";
    decimal target = decimal.Round(quantity, 4, MidpointRounding.ToEven);
    decimal allocated = 0;
    long cumulativeTicks = 0;
    DateTime month = new(start.Year, start.Month, 1);
    while (month < finish || pointActual)
    {
        bool lastMonth = month.Year == finish.Year && month.Month == finish.Month;
        DateTime periodEnd = lastMonth ? finish : month.AddMonths(1);
        DateTime periodStart = start > month ? start : month;
        decimal hours = periodStart == periodEnd ? 0 : calculator.CountWorkingHours(periodStart, periodEnd);
        decimal allocationMonthHours = forecastClock?.CountHours(periodStart, periodEnd) ?? hours;
        long ticks = checked((long)decimal.Round(allocationMonthHours * TimeSpan.TicksPerHour, 0));
        cumulativeTicks = checked(cumulativeTicks + ticks);
        decimal roundedCumulative = periodEnd == finish ? target
            : decimal.Round(quantity * (elapsedActual
                ? (periodEnd - start).Ticks / (decimal)(finish - start).Ticks
                : profile?.CumulativeShare(cumulativeTicks, totalTicks)
                    ?? cumulativeTicks / (decimal)totalTicks), 4, MidpointRounding.ToEven);
        decimal monthlyQuantity = roundedCumulative - allocated;
        allocated = roundedCumulative;

        // Preserve zero actual slices and working remaining slices. In working-
        // time mode, a closed month cannot receive another month's rounding residue.
        if (ticks > 0 || isActual)
        {
            string Days(decimal workingHours) => definition.HoursPerDay is > 0
                ? FormatRelationshipDays(workingHours, definition.HoursPerDay.Value, "F2") : "";
            string[] values =
            [
                metadata.TaskKey, metadata.ResourceKey, metadata.CalendarKey, metadata.ProjectKey,
                month.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), DateParser.Format(periodStart),
                DateParser.Format(periodEnd), monthlyQuantity.ToString("F4", CultureInfo.InvariantCulture), distributionType,
                hours.ToString("F2", CultureInfo.InvariantCulture), totalHours.ToString("F2", CultureInfo.InvariantCulture),
                definition.HoursPerDay?.ToString("F2", CultureInfo.InvariantCulture) ?? "", Days(hours), Days(totalHours),
                OccupiedCalendarDays(periodStart, periodEnd), OccupiedCalendarDays(start, finish),
                DateParser.Format(start), DateParser.Format(finish), isActual ? "1" : "0", metadata.Status,
                metadata.Unit, metadata.TaskCode, metadata.ResourceShortName, metadata.ResourceName,
                metadata.ResourceType, ParseMonthUpdateFromFilename(metadata.SourceRow.OriginalSourceFilename)
            ];
            result.Add(metadata.SourceRow.WithFields(values.Select(value => StringInternPool.Intern(value)).ToArray()));
        }
        if (periodEnd == finish) break; // Also supports December 9999 without AddMonths overflow.
        month = periodEnd;
    }
    if (cumulativeTicks != totalTicks || allocated != target)
        throw new InvalidOperationException("Monthly distribution did not reconcile to the assignment period and rounded quantity.");
}

private static string OccupiedCalendarDays(DateTime start, DateTime exclusiveFinish) =>
    exclusiveFinish == start ? "0"
        : ((exclusiveFinish.AddTicks(-1).Date - start.Date).Days + 1).ToString(CultureInfo.InvariantCulture);

private sealed class DistributionInputIndex
{
    private readonly XerTable? _table;
    private readonly Dictionary<(string Source, string Id), DataRow[]> _rows;

    internal DistributionInputIndex(XerTable? table, string identityField)
    {
        _table = table;
        _rows = table?.Rows.GroupBy(row => (row.SourceToken, Read(row, identityField).Trim()))
            .ToDictionary(group => group.Key, group => group.ToArray()) ?? new();
    }

    internal string Read(DataRow row, string field) => _table is null ? ""
        : GetFieldValue(row.Fields, _table.FieldIndexes, field);

    internal DataRow Require(string source, string id, string? projectId = null)
    {
        if (id.Length == 0 || !_rows.TryGetValue((source, id), out var candidates))
            throw new InvalidDataException($"Missing {_table?.Name ?? "required source table"} identity '{id}'.");
        // Public keys do not include project ID: project filtering cannot make
        // duplicate source-local identities safe to join to exported dimensions.
        if (candidates.Length != 1)
            throw new InvalidDataException($"Ambiguous {_table?.Name} identity '{id}'.");
        if (!string.IsNullOrWhiteSpace(projectId))
            candidates = candidates.Where(row => Read(row, FieldNames.ProjectId).Trim() == projectId.Trim()).ToArray();
        if (candidates.Length != 1)
            throw new InvalidDataException($"Ambiguous or mismatched {_table?.Name} identity '{id}' for project '{projectId}'.");
        return candidates[0];
    }

    internal DataRow? Optional(string source, string id) =>
        id.Length > 0 && _rows.TryGetValue((source, id), out var candidates) && candidates.Length == 1
            ? candidates[0] : null;
}
}
```

### 10.3 Curve Repository & Profiles
```csharp
internal sealed class RemainingResourceProfile
{
    private readonly decimal[] _ends;
    private readonly decimal[] _quantities;
    private readonly decimal _duration;
    private readonly decimal _quantity;
    private readonly bool _usesTicks;

    internal string DistributionType { get; }
    internal bool IsUniform { get; }

    private RemainingResourceProfile(decimal[] ends, decimal[] quantities, string type, bool uniform = false, bool usesTicks = false)
    {
        _ends = ends;
        _quantities = quantities;
        _duration = ends[^1];
        _quantity = quantities[^1];
        DistributionType = type;
        IsUniform = uniform;
        _usesTicks = usesTicks;
    }

    internal decimal CumulativeShare(long workingTicks, long totalTicks)
    {
        if (workingTicks == 0) return 0;
        if (workingTicks == totalTicks) return 1;
        decimal progress = workingTicks / (decimal)totalTicks;
        // Preserve the exact existing arithmetic for an assigned linear curve.
        if (IsUniform) return progress;
        decimal position = _usesTicks ? workingTicks : progress * _duration;
        int band = Array.BinarySearch(_ends, position);
        if (band >= 0) return _quantities[band] / _quantity;
        band = ~band;
        decimal start = band == 0 ? 0 : _ends[band - 1];
        decimal before = band == 0 ? 0 : _quantities[band - 1];
        decimal share = (before + (_quantities[band] - before)
            * ((position - start) / (_ends[band] - start))) / _quantity;
        return Math.Clamp(share, 0, 1);
    }

    internal static RemainingResourceProfile Uniform(string type) => new([1m], [1m], type, uniform: true);

    // Crop each duration band at the estimated phase, then normalize the surviving
    // band quantities. This is equivalent to (F(p + x*(1-p))-F(p))/(1-F(p)),
    // without subtracting two nearly equal cumulative shares. Never mutate the
    // source-cached definition: different assignments can have different phases.
    internal RemainingResourceProfile? RemainingTail(decimal phase)
    {
        if (_usesTicks || phase < 0 || phase >= 1)
            throw new ArgumentOutOfRangeException(nameof(phase));
        decimal position = phase * _duration;
        var ends = new List<decimal>();
        var quantities = new List<decimal>();
        decimal quantity = 0;
        for (int i = 0; i < _ends.Length; i++)
        {
            decimal bandStart = i == 0 ? 0 : _ends[i - 1];
            if (_ends[i] <= position) continue;
            decimal bandQuantity = _quantities[i] - (i == 0 ? 0 : _quantities[i - 1]);
            quantity += bandQuantity * ((_ends[i] - Math.Max(position, bandStart)) / (_ends[i] - bandStart));
            ends.Add(_ends[i] - position);
            quantities.Add(quantity);
        }
        return quantity <= 0 ? null : new RemainingResourceProfile(ends.ToArray(), quantities.ToArray(), "Resource Curve Forecast");
    }

    internal static RemainingResourceProfile FromCurve(Func<string, string> read, string id)
    {
        decimal[] percentages = Enumerable.Range(0, 21)
            .Select(index => Number(read($"pct_usage_{index}"), $"curve '{id}' pct_usage_{index}")).ToArray();
        if (percentages.Any(value => value > 100))
            throw new InvalidDataException($"Curve '{id}' percentages must not exceed 100.");
        // The 0% entry describes already-used resource under P6's actuals rules.
        // Never turn it into another 5% duration band or invent an instant allocation.
        if (percentages[0] != 0)
            throw new InvalidDataException($"Curve '{id}' has a nonzero 0% entry. Its P6 actuals semantics require an exported remain_crv profile.");
        decimal sum = percentages.Sum();
        // Permit only small decimal serialization/proration noise, not arbitrary
        // rescaling of an invalid curve. Normalize accepted values by their sum.
        if (Math.Abs(sum - 100) > 0.001m)
            throw new InvalidDataException($"Curve '{id}' percentages must total 100 (tolerance 0.001); got {sum.ToString(CultureInfo.InvariantCulture)}.");
        var cumulative = new decimal[20];
        decimal allocated = 0;
        for (int i = 0; i < 20; i++) cumulative[i] = allocated += percentages[i + 1];
        return new RemainingResourceProfile(Enumerable.Range(1, 20).Select(value => (decimal)value).ToArray(),
            cumulative, "Resource Curve", percentages.Skip(1).All(value => value == percentages[1]));
    }

    internal static RemainingResourceProfile FromManual(string raw, decimal remainingQuantity, decimal workingHours)
    {
        // XER remain_crv uses quantity:period-working-hours pairs. Each pair is
        // anchored after the previous pair in assignment-calendar working time.
        // Zero-quantity pairs are intentional gaps and must not be discarded.
        // A trailing delimiter is harmless; internal empty bands are still invalid.
        string[] bands = raw.Trim().TrimEnd(';').Split(';');
        var ends = new decimal[bands.Length];
        var quantities = new decimal[bands.Length];
        decimal durationTicks = 0;
        decimal quantity = 0;
        for (int i = 0; i < bands.Length; i++)
        {
            string[] pair = bands[i].Split(':');
            if (pair.Length != 2)
                throw new InvalidDataException($"remain_crv band {i + 1} requires quantity:working-hours.");
            decimal units = Number(pair[0], $"remain_crv band {i + 1} quantity");
            decimal hours = Number(pair[1], $"remain_crv band {i + 1} working-hours");
            decimal ticks = hours * TimeSpan.TicksPerHour;
            if (hours == 0 || ticks != decimal.Truncate(ticks))
                throw new InvalidDataException($"remain_crv band {i + 1} requires positive working-hours representable as whole ticks.");
            ends[i] = durationTicks += ticks;
            quantities[i] = quantity += units;
        }
        if (durationTicks != decimal.Round(workingHours * TimeSpan.TicksPerHour, 0))
            throw new InvalidDataException("remain_crv duration does not reconcile to the assignment's remaining calendar working time.");
        if (quantity <= 0 || decimal.Round(quantity, 4, MidpointRounding.ToEven)
            != decimal.Round(remainingQuantity, 4, MidpointRounding.ToEven))
            throw new InvalidDataException("remain_crv quantities do not reconcile to remain_qty at four-decimal export precision.");
        return new RemainingResourceProfile(ends, quantities, "Remaining Units Profile", usesTicks: true);
    }

    private static decimal Number(string raw, string field)
    {
        if (!decimal.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal value) || value < 0)
            throw new InvalidDataException($"{field} requires a finite nonnegative invariant number; got '{raw}'.");
        return value;
    }
}

internal sealed record RemainingDistributionPlan(RemainingResourceProfile? Profile,
    ResourceForecastClock? Clock = null, string? MethodCode = null, string? MethodMessage = null);

// Forecast availability is separate from the raw calendar diagnostics and from
// the historical actuals allocator. Only active named-curve allocations use it.
internal sealed class ResourceForecastClock
{
    private readonly WorkingDayCalculator _calendar;
    private readonly DateTime? _suspend;
    private readonly DateTime? _resume;

    internal ResourceForecastClock(WorkingDayCalculator calendar, string suspend, string resume, DateTime remainingFinish)
    {
        _calendar = calendar;
        if (string.IsNullOrWhiteSpace(suspend) && string.IsNullOrWhiteSpace(resume)) return;
        _suspend = DateParser.TryParse(suspend)?.Date;
        _resume = DateParser.TryParse(resume)?.Date;
        if (!_suspend.HasValue || (!string.IsNullOrWhiteSpace(resume) && (!_resume.HasValue || _resume < _suspend)))
            throw new InvalidDataException("Malformed suspend/resume dates prevent establishing remaining working availability.");
        if (!_resume.HasValue && _suspend < remainingFinish)
            throw new InvalidDataException("An open suspension overlaps the remaining period; no resume date or working availability is invented.");
    }

    internal decimal CountHours(DateTime start, DateTime finish)
    {
        decimal hours = _calendar.CountWorkingHours(start, finish);
        if (_suspend.HasValue && _resume.HasValue)
        {
            DateTime left = start > _suspend.Value ? start : _suspend.Value;
            DateTime right = finish < _resume.Value ? finish : _resume.Value;
            if (right > left) hours -= _calendar.CountWorkingHours(left, right);
        }
        return hours;
    }

    internal string Description => _suspend.HasValue
        ? $"suspend_date={DateParser.Format(_suspend.Value)}; resume_date={(_resume.HasValue ? DateParser.Format(_resume.Value) : "blank")}; "
        : "";

    internal static bool HasSingleWorkingMonth(DateTime start, DateTime finish, Func<DateTime, DateTime, decimal> hours)
    {
        int workingMonths = 0;
        DateTime cursor = start;
        while (cursor < finish)
        {
            DateTime end = cursor.Year == finish.Year && cursor.Month == finish.Month
                ? finish : new DateTime(cursor.Year, cursor.Month, 1).AddMonths(1);
            if (hours(cursor, end) > 0 && ++workingMonths > 1) return false;
            cursor = end;
        }
        return workingMonths == 1;
    }
}

internal static class ResourceCurveForecast
{
    internal static RemainingDistributionPlan Resolve(RemainingResourceProfile named, ResourceForecastClock clock,
        DateTime remainingStart, DateTime remainingFinish, Func<string, XerRawField> assignment,
        string dataDateText, bool actualQuantityIsZero)
    {
        decimal remainingHours = clock.CountHours(remainingStart, remainingFinish);
        if (remainingHours <= 0)
            throw new InvalidDataException("The remaining period has no working availability after recorded suspensions.");
        // These allocations need no estimated phase, even if actual dates are missing.
        if (named.IsUniform || ResourceForecastClock.HasSingleWorkingMonth(remainingStart, remainingFinish, clock.CountHours))
            return new(named, clock);

        decimal? elapsedHours = null, phase = null;
        string? reason = null;
        XerRawField actualStart = assignment("act_start_date"), actualFinish = assignment("act_end_date");
        bool Known(string field) => assignment(field).State is XerRawFieldState.Blank or XerRawFieldState.Present;
        if (actualStart.State == XerRawFieldState.Blank && actualFinish.State == XerRawFieldState.Blank
            && actualQuantityIsZero && Known("act_reg_qty") && Known("act_ot_qty"))
        {
            elapsedHours = 0;
        }
        else
        {
            DateTime? start = DateParser.TryParse(actualStart.RawValue);
            DateTime? dataDate = DateParser.TryParse(dataDateText);
            if (!start.HasValue || !dataDate.HasValue || !Known("act_end_date"))
                reason = "Assignment actual-start, actual-finish presence or project Data Date evidence is missing/invalid.";
            else if (actualFinish.State != XerRawFieldState.Blank || start > dataDate || dataDate > remainingStart)
                reason = "Assignment progress dates conflict with its Data Date or remaining period.";
            else elapsedHours = clock.CountHours(start.Value, dataDate.Value);
        }

        RemainingResourceProfile? profile = null;
        if (elapsedHours.HasValue)
        {
            phase = elapsedHours.Value / (elapsedHours.Value + remainingHours);
            profile = named.RemainingTail(phase.Value);
            if (profile is null) reason = "The named curve has no weight remaining after the calculated progress point.";
        }
        bool fallback = profile is null;
        string Number(decimal? value) => value?.ToString("G29", CultureInfo.InvariantCulture) ?? "unavailable";
        string method = fallback ? "Working Hours Fallback" : "Resource Curve Forecast";
        string message = $"Allocated remaining units using {method}; this is an exporter forecast, not native P6 timephased output. "
            + $"A={Number(elapsedHours)} working hours; R={Number(remainingHours)} working hours; p={Number(phase)}; "
            + clock.Description + (reason is null ? "Retained and normalized the remaining curve bands." : $"Reason: {reason}");
        return new(profile ?? RemainingResourceProfile.Uniform(method), clock,
            fallback ? "REMAINING_CURVE_UNIFORM_FALLBACK" : "REMAINING_CURVE_ESTIMATED", message);
    }
}

internal sealed class ResourceCurveRepository
{
    private readonly XerTable? _table;
    private readonly Dictionary<(string Source, string Id), DataRow[]> _rows;
    private readonly Dictionary<(string Source, string Id), RemainingResourceProfile> _cache = new();

    internal ResourceCurveRepository(XerDataStore store)
    {
        _table = store.GetTable("RSRCCURVDATA");
        _rows = _table?.Rows.GroupBy(row => (row.SourceToken, Read(row, "curv_id").Trim()))
            .ToDictionary(group => group.Key, group => group.ToArray()) ?? new();
    }

    internal RemainingResourceProfile Get(string source, string id)
    {
        if (_cache.TryGetValue((source, id), out var profile)) return profile;
        if (!_rows.TryGetValue((source, id), out var rows) || rows.Length != 1)
            throw new InvalidDataException($"Curve '{id}' requires exactly one RSRCCURVDATA definition in its source occurrence. "
                + "Missing/ambiguous definitions and opaque RSRCCURV.curv_data are not replaced by a uniform spread; export remain_crv for manual profiles.");
        profile = RemainingResourceProfile.FromCurve(field => Read(rows[0], field), id);
        _cache.Add((source, id), profile);
        return profile;
    }

    private string Read(DataRow row, string field) => _table is not null
        && _table.FieldIndexes.TryGetValue(field, out int index) && index < row.Fields.Length
            ? row.Fields[index] : "";
}
```
