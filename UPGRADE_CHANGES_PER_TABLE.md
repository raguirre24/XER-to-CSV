# Enhanced Tables Upgrade Guide

This document details all code changes required to upgrade the 14 Enhanced Power BI export tables (`01_XER_TASK` through `15_XER_RESOURCE_DISTRIBUTION`, excluding 05) to the new version.

---

## Table of Contents
- [0. Foundation & Shared Helpers](#0-foundation--shared-helpers)
- [1. 01_XER_TASK](#1-01_xer_task)
- [2. 02_XER_PROJECT](#2-02_xer_project)
- [3. 03_XER_PROJWBS](#3-03_xer_projwbs)
- [4. 04_XER_BASELINE](#4-04_xer_baseline)
- [5. 06_XER_PREDECESSOR](#5-06_xer_predecessor)
- [6. 07_XER_ACTVTYPE, 08_XER_ACTVCODE, 09_XER_TASKACTV](#6-07_xer_actvtype-08_xer_actvcode-09_xer_taskactv)
- [7. 10_XER_CALENDAR](#7-10_xer_calendar)
- [8. 11_XER_CALENDAR_DETAILED](#8-11_xer_calendar_detailed)
- [9. 12_XER_RSRC, 13_XER_TASKRSRC, 14_XER_UMEASURE](#9-12_xer_rsrc-13_xer_taskrsrc-14_xer_umeasure)
- [10. 15_XER_RESOURCE_DISTRIBUTION](#10-15_xer_resource_distribution)
- [Summary Checklist for Upgrade](#summary-checklist-for-upgrade)

---

## 0. Foundation & Shared Helpers

### What Changed
- Replaced `ConcurrentBag<DataRow>` with pre-allocated `DataRow[rowCount]` and `Parallel.For` to preserve row ordering and `SourceToken`.
- Added `CreateEnhancedHeaders` to prepend `raw_` to duplicate source columns and prevent duplicate header collisions.
- Lookups use `(string SourceToken, string Id)` tuples instead of string concatenation.
- Static compiled regex used for filename date extraction.

#### Old Implementation
```csharp
        private XerTable CreateSimpleKeyedTable(string sourceTableName, string newTableName, List<Tuple<string, string>> keyMappings)
        {
            var sourceTable = _dataStore.GetTable(sourceTableName);
            if (!IsTableValid(sourceTable)) return null;

            try
            {
                var sourceHeaders = sourceTable.Headers;
                var sourceIndexes = sourceTable.FieldIndexes;

                var finalHeadersList = sourceHeaders.ToList();
                foreach (var mapping in keyMappings)
                {
                    finalHeadersList.Add(mapping.Item1);
                }
                finalHeadersList.Add(FieldNames.MonthUpdate);
                string[] finalHeaders = finalHeadersList.Select(StringInternPool.Intern).ToArray();

                PERFORMANCE OPTIMIZATION: Pre-calculate indexes
                var finalIndexes = finalHeaders
                    .Select((name, index) => new { name, index })
                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);


                var resultTable = new XerTable(newTableName, sourceTable.RowCount);
                resultTable.SetHeaders(finalHeaders);

                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
                var transformedRowsBag = new ConcurrentBag<DataRow>();

                Parallel.ForEach(sourceTable.Rows, parallelOptions, sourceRow =>
                {
                    var row = sourceRow.Fields;
                    string[] transformed = new string[finalHeaders.Length];
                    string originalFilename = sourceRow.SourceFilename;

                    Copy existing fields
                    int copyLength = Math.Min(row.Length, sourceHeaders.Length);
                    Array.Copy(row, transformed, copyLength);

                    Generate and set keys
                    foreach (var mapping in keyMappings)
                    {
                        Check if the source field exists before attempting to access it
                        if (sourceIndexes.ContainsKey(mapping.Item2))
                        {
                            string sourceValue = GetFieldValue(row, sourceIndexes, mapping.Item2);
                            string key = CreateKey(originalFilename, sourceValue);
                            Use optimized method
                            SetTransformedField(transformed, finalIndexes, mapping.Item1, key);
                        }
                    }

                    Add MonthUpdate value
                    SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

                    Intern strings
                    for (int k = 0; k < transformed.Length; k++)
                    {
                        transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);
                    }

                    transformedRowsBag.Add(new DataRow(transformed, originalFilename));
                });

                resultTable.AddRows(transformedRowsBag);
                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {newTableName}: {ex.Message}");
                return null;
            }
        }
```

#### New Implementation
```csharp
public static string[] CreateEnhancedHeaders(string[] rawHeaders, string[] calculatedHeaders)
{
    var headers = new List<string>(rawHeaders.Length + calculatedHeaders.Length);
    var calcSet = new HashSet<string>(calculatedHeaders, StringComparer.OrdinalIgnoreCase);

    foreach (var raw in rawHeaders)
    {
        if (calcSet.Contains(raw))
            headers.Add($"raw_{raw}");
        else
            headers.Add(raw);
    }

    foreach (var calc in calculatedHeaders)
    {
        headers.Add(calc);
    }

    return headers.Select(h => StringInternPool.Intern(h) ?? string.Empty).ToArray();
}

private XerTable? CreateSimpleKeyedTable(
    string sourceTableName,
    string newTableName,
    IReadOnlyList<(string KeyColumnName, string SourceColumnName)> keyMappings)
{
    ClearGenerationFailure(newTableName);

    var sourceTable = _dataStore.GetTable(sourceTableName);
    if (!IsTableValid(sourceTable)) return null;

    try
    {
        var sourceHeaders = sourceTable.Headers;
        var sourceIndexes = sourceTable.FieldIndexes;

        string[] addedHeaders = keyMappings
            .Select(m => m.KeyColumnName)
            .Append(FieldNames.MonthUpdate)
            .ToArray();

        string[] finalHeaders = CreateEnhancedHeaders(sourceHeaders, addedHeaders);
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
            string[] row = sourceRow.Fields;
            string[] transformed = new string[finalHeaders.Length];
            string originalFilename = sourceRow.SourceFilename;

            for (int col = 0; col < sourceHeaders.Length && col < row.Length; col++)
            {
                string rawHeader = sourceHeaders[col];
                if (finalIndexes.TryGetValue(rawHeader, out int targetIdx))
                    transformed[targetIdx] = row[col] ?? string.Empty;
                else if (finalIndexes.TryGetValue($"raw_{rawHeader}", out int rawTargetIdx))
                    transformed[rawTargetIdx] = row[col] ?? string.Empty;
            }

            for (int i = 0; i < keyMappings.Count; i++)
            {
                var mapping = keyMappings[i];
                if (sourceIndexes.ContainsKey(mapping.SourceColumnName))
                {
                    string sourceValue = GetFieldValue(row, sourceIndexes, mapping.SourceColumnName);
                    string key = CreateKey(sourceRow.SourceToken, sourceValue);
                    SetTransformedField(transformed, finalIndexes, mapping.KeyColumnName, key);
                }
            }

            SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

            for (int k = 0; k < transformed.Length; k++)
                transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);

            transformedRows[rowIndex] = new DataRow(transformed, originalFilename, sourceRow.SourceToken);
        });

        resultTable.AddRows(transformedRows);
        return resultTable;
    }
    catch (Exception ex)
    {
        RecordGenerationFailure(newTableName, ex.Message);
        return null;
    }
}
```

---

## 1. 01_XER_TASK

### What Changed
- Renamed column `Remaining Working Days` to `Remaining Duration`.
- Explicitly preserved `remain_drtn_hr_cnt` and `phys_complete_pct` in output.
- Formatted duration and float days to 2 decimal places (`F2`).
- Percent complete calculation uses nullable `decimal?` clamped to `[0m, 100m]`, handling `CP_PHYS`, `CP_UNITS`, and `CP_DRTN`.
- Replaced `ConcurrentBag<DataRow>` with pre-allocated array.

#### Old Implementation
```csharp
        public XerTable Create01XerTaskTable()
        {
            var taskTable = _dataStore.GetTable(TableNames.Task);
            var calendarTable = _dataStore.GetTable(TableNames.Calendar);
            var projectTable = _dataStore.GetTable(TableNames.Project);

            if (!IsTableValid(taskTable) || !IsTableValid(calendarTable) || !IsTableValid(projectTable)) return null;

            try
            {
                var taskIndexes = taskTable.FieldIndexes;
                Pre-build lookup tables
                var calendarHours = BuildCalendarHoursLookup(calendarTable);
                var projectDataDates = BuildProjectDataDatesLookup(projectTable);

                string[] finalColumns = {
                FieldNames.TaskId, FieldNames.ProjectId, FieldNames.WbsId, FieldNames.CalendarId,
                FieldNames.TaskType, FieldNames.StatusCode, FieldNames.TaskCode, FieldNames.TaskName,
                FieldNames.RsrcId, FieldNames.ActStartDate, FieldNames.ActEndDate,
                FieldNames.EarlyStartDate, FieldNames.EarlyEndDate, FieldNames.LateStartDate, FieldNames.LateEndDate,
                FieldNames.TargetStartDate, FieldNames.TargetEndDate,
                FieldNames.CstrType, FieldNames.CstrDate, FieldNames.PriorityType, FieldNames.FloatPath,
                FieldNames.FloatPathOrder, FieldNames.DrivingPathFlag,
                Calculated Fields
                FieldNames.Start, FieldNames.Finish, FieldNames.IdName,
                FieldNames.RemainingWorkingDays, FieldNames.OriginalDuration, FieldNames.TotalFloat, FieldNames.FreeFloat,
                FieldNames.PercentComplete, FieldNames.DataDate,
                Key Fields
                FieldNames.WbsIdKey, FieldNames.TaskIdKey, FieldNames.CalendarIdKey, FieldNames.ProjIdKey,
                FieldNames.MonthUpdate
            };

                PERFORMANCE OPTIMIZATION: Pre-calculate target indexes for O(1) lookups in the parallel loop
                var finalIndexes = finalColumns
                    .Select((name, index) => new { name, index })
                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);


                var finalTable = new XerTable(EnhancedTableNames.XerTask01, taskTable.RowCount);
                finalTable.SetHeaders(finalColumns.Select(StringInternPool.Intern).ToArray());

                const string TK_Complete = "TK_Complete";
                const string TK_NotStart = "TK_NotStart";
                const string TK_Active = "TK_Active";

                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
                var transformedRowsBag = new ConcurrentBag<DataRow>();

                Parallel.ForEach(taskTable.Rows, parallelOptions, sourceRowData =>
                {
                    string[] row = sourceRowData.Fields;
                    string[] transformed = new string[finalColumns.Length];
                    string originalFilename = sourceRowData.SourceFilename;

                    Extract key fields
                    string projId = GetFieldValue(row, taskIndexes, FieldNames.ProjectId);
                    string taskId = GetFieldValue(row, taskIndexes, FieldNames.TaskId);
                    string wbsId = GetFieldValue(row, taskIndexes, FieldNames.WbsId);
                    string clndrId = GetFieldValue(row, taskIndexes, FieldNames.CalendarId);
                    string statusCode = GetFieldValue(row, taskIndexes, FieldNames.StatusCode);
                    DateTime? actEndDate = DateParser.TryParse(GetFieldValue(row, taskIndexes, FieldNames.ActEndDate));

                    Copy and format fields using optimized methods (passing finalIndexes)
                    CopyDirectFields(row, taskIndexes, transformed, finalIndexes);
                    FormatDateFields(row, taskIndexes, transformed, finalIndexes, actEndDate);

                    Status Code mapping
                    SetTransformedField(transformed, finalIndexes, FieldNames.StatusCode,
                        StringInternPool.Intern(
                            statusCode == TK_Complete ? "Complete" :
                            statusCode == TK_NotStart ? "Not Started" :
                            statusCode == TK_Active ? "In Progress" : statusCode)
                        );

                    Start/Finish calculation
                    DateTime startDate = CalculateStartDate(row, taskIndexes, statusCode);
                    DateTime finishDate = CalculateFinishDate(row, taskIndexes, statusCode);
                    SetTransformedField(transformed, finalIndexes, FieldNames.Start, DateParser.Format(startDate));
                    SetTransformedField(transformed, finalIndexes, FieldNames.Finish, DateParser.Format(finishDate));

                    ID_Name concatenation
                    string taskCode = GetFieldValue(row, taskIndexes, FieldNames.TaskCode);
                    string taskName = GetFieldValue(row, taskIndexes, FieldNames.TaskName);
                    SetTransformedField(transformed, finalIndexes, FieldNames.IdName, $"{taskCode} - {taskName}");

                    Duration/Float calculations (Hours to Days)
                    string calendarKey = CreateKey(originalFilename, clndrId);
                    if (!string.IsNullOrEmpty(clndrId) && calendarHours.TryGetValue(calendarKey, out decimal dayHrCnt) && dayHrCnt > 0)
                    {
                        SetTransformedField(transformed, finalIndexes, FieldNames.RemainingWorkingDays,
                            CalculateDaysFromHours(row, taskIndexes, FieldNames.RemainDurationHrCnt, dayHrCnt));
                        SetTransformedField(transformed, finalIndexes, FieldNames.OriginalDuration,
                            CalculateDaysFromHours(row, taskIndexes, FieldNames.TargetDurationHrCnt, dayHrCnt));

                        if (statusCode != TK_Complete)
                        {
                            SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat,
                                CalculateDaysFromHours(row, taskIndexes, FieldNames.TotalFloatHrCnt, dayHrCnt));
                            SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloat,
                                CalculateDaysFromHours(row, taskIndexes, FieldNames.FreeFloatHrCnt, dayHrCnt));
                        }
                        else
                        {
                            Floats are typically null/empty when complete
                            SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat, "");
                            SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloat, "");
                        }
                    }
                    else
                    {
                        Handle missing calendar or zero hours/day
                        SetTransformedField(transformed, finalIndexes, FieldNames.RemainingWorkingDays, "");
                        SetTransformedField(transformed, finalIndexes, FieldNames.OriginalDuration, "");
                        SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat, "");
                        SetTransformedField(transformed, finalIndexes, FieldNames.FreeFloat, "");
                    }

                    Percentage Complete calculation
                    double pct = CalculateCompletionPercentage(row, taskIndexes, statusCode);
                    SetTransformedField(transformed, finalIndexes, FieldNames.PercentComplete,
                        pct.ToString("F2", CultureInfo.InvariantCulture));

                    Data Date lookup
                    string projectKey = CreateKey(originalFilename, projId);
                    if (!string.IsNullOrEmpty(projId) && projectDataDates.TryGetValue(projectKey, out DateTime dataDateValue))
                    {
                        SetTransformedField(transformed, finalIndexes, FieldNames.DataDate, DateParser.Format(dataDateValue));
                    }
                    else
                    {
                        SetTransformedField(transformed, finalIndexes, FieldNames.DataDate, "");
                    }

                    Key generation
                    SetTransformedField(transformed, finalIndexes, FieldNames.WbsIdKey, CreateKey(originalFilename, wbsId));
                    SetTransformedField(transformed, finalIndexes, FieldNames.TaskIdKey, CreateKey(originalFilename, taskId));
                    SetTransformedField(transformed, finalIndexes, FieldNames.CalendarIdKey, CreateKey(originalFilename, clndrId));
                    SetTransformedField(transformed, finalIndexes, FieldNames.ProjIdKey, CreateKey(originalFilename, projId));

                    Add MonthUpdate value
                    SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

                    Intern all resulting strings in the transformed row
                    for (int k = 0; k < transformed.Length; k++)
                    {
                        transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);
                    }

                    transformedRowsBag.Add(new DataRow(transformed, originalFilename));
                });

                finalTable.AddRows(transformedRowsBag);
                return finalTable;
            }
            catch (Exception ex)
            {
                Log error (consider using a formal logging framework)
                Console.WriteLine($"Error creating {EnhancedTableNames.XerTask01}: {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        private double CalculateCompletionPercentage(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode)
        {
            if (statusCode == "TK_Complete") return 100.0;
            if (statusCode == "TK_NotStart") return 0.0;

            string completePctType = GetFieldValue(row, indexes, FieldNames.CompletePctType);
            NumberStyles numStyle = NumberStyles.Any;
            IFormatProvider formatProvider = CultureInfo.InvariantCulture;

            switch (completePctType)
            {
                case "CP_Phys": // Physical % Complete
                    if (double.TryParse(GetFieldValue(row, indexes, FieldNames.PhysCompletePct), numStyle, formatProvider, out double pct))
                    {
                        return Math.Max(0.0, Math.Min(100.0, pct));
                    }
                    break;

                case "CP_Units": // Units % Complete (Actual / (Actual + Remaining))
                    if (double.TryParse(GetFieldValue(row, indexes, FieldNames.ActWorkQty), numStyle, formatProvider, out double actVal) &&
                        double.TryParse(GetFieldValue(row, indexes, FieldNames.RemainWorkQty), numStyle, formatProvider, out double remVal))
                    {
                        double total = actVal + remVal;
                        if (total > 0.0001) // Avoid division by zero
                        {
                            double calculatedPct = (actVal / total) * 100.0;
                            return Math.Max(0.0, Math.Min(100.0, calculatedPct));
                        }
                    }
                    break;

                case "CP_Drtn": // Duration % Complete ((Target - Remaining) / Target)
                    if (double.TryParse(GetFieldValue(row, indexes, FieldNames.TargetDurationHrCnt), numStyle, formatProvider, out double targVal) &&
                        double.TryParse(GetFieldValue(row, indexes, FieldNames.RemainDurationHrCnt), numStyle, formatProvider, out double remDurVal))
                    {
                        if (targVal > 0.0001)
                        {
                            double calculatedPct = ((targVal - remDurVal) / targVal) * 100.0;
                            return Math.Max(0.0, Math.Min(100.0, calculatedPct));
                        }
                    }
                    break;
            }

            return 0.0;
        }
```

#### New Implementation
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

    if (!IsTableValid(taskTable) || !IsTableValid(calendarTable) || !IsTableValid(projectTable)) return null;

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
        finalTable.SetHeaders(finalColumns.Select(StringInternPool.Intern).ToArray());

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
                pct.HasValue ? pct.Value.ToString("F2", CultureInfo.InvariantCulture) : "");

            if (!string.IsNullOrEmpty(projId) && projectDataDates.TryGetValue((sourceRowData.SourceToken, projId.Trim()), out DateTime dataDateValue))
                SetTransformedField(transformed, finalIndexes, FieldNames.DataDate, DateParser.Format(dataDateValue));
            else
                SetTransformedField(transformed, finalIndexes, FieldNames.DataDate, "");

            SetTransformedField(transformed, finalIndexes, FieldNames.WbsIdKey, CreateKey(sourceRowData.SourceToken, wbsId));
            SetTransformedField(transformed, finalIndexes, FieldNames.TaskIdKey, CreateKey(sourceRowData.SourceToken, taskId));
            SetTransformedField(transformed, finalIndexes, FieldNames.CalendarIdKey, CreateKey(sourceRowData.SourceToken, clndrId));
            SetTransformedField(transformed, finalIndexes, FieldNames.ProjIdKey, CreateKey(sourceRowData.SourceToken, projId));
            SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

            for (int k = 0; k < transformed.Length; k++)
                transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);

            transformedRows[rowIndex] = new DataRow(transformed, originalFilename, sourceRowData.SourceToken);
        });

        finalTable.AddRows(transformedRows);
        return finalTable;
    }
    catch (Exception ex)
    {
        RecordGenerationFailure(EnhancedTableNames.XerTask01, ex.Message);
        return null;
    }
}

private decimal? CalculateCompletionPercentage(string[] row, IReadOnlyDictionary<string, int> indexes, string statusCode)
{
    if (string.Equals(statusCode, "TK_Complete", StringComparison.OrdinalIgnoreCase))
        return 100m;
    if (string.Equals(statusCode, "TK_NotStart", StringComparison.OrdinalIgnoreCase))
        return 0m;

    string pctType = GetFieldValue(row, indexes, FieldNames.CompletePctType);

    decimal? pct = null;
    if (string.Equals(pctType, "CP_PHYS", StringComparison.OrdinalIgnoreCase))
    {
        string phys = GetFieldValue(row, indexes, FieldNames.PhysCompletePct);
        if (decimal.TryParse(phys, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal v))
            pct = v;
    }
    else if (string.Equals(pctType, "CP_UNITS", StringComparison.OrdinalIgnoreCase))
    {
        string actUnits = GetFieldValue(row, indexes, FieldNames.ActWorkQty);
        string remUnits = GetFieldValue(row, indexes, FieldNames.RemainWorkQty);
        if (decimal.TryParse(actUnits, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal act) &&
            decimal.TryParse(remUnits, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal rem))
        {
            decimal total = act + rem;
            if (total > 0m) pct = (act / total) * 100m;
        }
    }
    else
    {
        string remDur = GetFieldValue(row, indexes, FieldNames.RemainDurationHrCnt);
        string totDur = GetFieldValue(row, indexes, FieldNames.TargetDurationHrCnt);
        if (decimal.TryParse(remDur, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal rem) &&
            decimal.TryParse(totDur, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal tot) &&
            tot > 0m)
        {
            pct = Math.Max(0m, (tot - rem) / tot * 100m);
        }
    }

    if (!pct.HasValue) return null;
    return Math.Clamp(pct.Value, 0m, 100m);
}
```

---

## 2. 02_XER_PROJECT

### What Changed
- Converted to type-safe tuple mappings.
- Uses `CreateSimpleKeyedTable` with `CreateEnhancedHeaders`.

#### Old Implementation
```csharp
public XerTable Create02XerProject() => CreateSimpleKeyedTable(TableNames.Project, EnhancedTableNames.XerProject02,
    new List<Tuple<string, string>> { Tuple.Create(FieldNames.ProjIdKey, FieldNames.ProjectId) });
```

#### New Implementation
```csharp
public XerTable? Create02XerProject() => CreateSimpleKeyedTable(
    TableNames.Project,
    EnhancedTableNames.XerProject02,
    new[] { (FieldNames.ProjIdKey, FieldNames.ProjectId) });
```

---

## 3. 03_XER_PROJWBS

### What Changed
- Added $O(V)$ cycle detection; circular parent keys are severed to prevent Power BI DAX `PATH()` crashes.
- Added same-project (`proj_id`) validation; parent keys referencing other projects are severed.
- Row order preserved using deterministic array.

#### Old Implementation
```csharp
        public XerTable Create03XerProjWbsTable()
        {
            var projwbsTable = _dataStore.GetTable(TableNames.ProjWbs);
            if (!IsTableValid(projwbsTable)) return null;

            try
            {
                var sourceHeaders = projwbsTable.Headers;
                var idx = projwbsTable.FieldIndexes;

                var finalHeadersList = sourceHeaders.ToList();
                finalHeadersList.Add(FieldNames.WbsIdKey);
                finalHeadersList.Add(FieldNames.ParentWbsIdKey);
                finalHeadersList.Add(FieldNames.MonthUpdate);
                string[] finalHeaders = finalHeadersList.Select(StringInternPool.Intern).ToArray();

                PERFORMANCE OPTIMIZATION: Pre-calculate indexes
                var finalIndexes = finalHeaders
                    .Select((name, index) => new { name, index })
                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);


                var resultTable = new XerTable(EnhancedTableNames.XerProjWbs03, projwbsTable.RowCount);
                resultTable.SetHeaders(finalHeaders);

                Track all valid WBS ID Keys to validate parent keys later
                var allWbsIdKeys = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);
                var transformedRowsBag = new ConcurrentBag<DataRow>();

                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };

                Parallel.ForEach(projwbsTable.Rows, parallelOptions, sourceRow =>
                {
                    var row = sourceRow.Fields;
                    var transformed = new string[finalHeaders.Length];
                    string originalFilename = sourceRow.SourceFilename;

                    string wbsIdKey = CreateKey(originalFilename, GetFieldValue(row, idx, FieldNames.WbsId));
                    string parentWbsIdKey = CreateKey(originalFilename, GetFieldValue(row, idx, FieldNames.ParentWbsId));

                    allWbsIdKeys.TryAdd(wbsIdKey, 0); // Use ConcurrentDictionary as a ConcurrentHashSet

                    Copy existing fields
                    int copyLength = Math.Min(row.Length, sourceHeaders.Length);
                    Array.Copy(row, transformed, copyLength);
                    Initialize potentially missing fields if source row was shorter
                    for (int i = copyLength; i < sourceHeaders.Length; i++)
                    {
                        transformed[i] = string.Empty;
                    }

                    Set new key fields using optimized method
                    SetTransformedField(transformed, finalIndexes, FieldNames.WbsIdKey, wbsIdKey);
                    SetTransformedField(transformed, finalIndexes, FieldNames.ParentWbsIdKey, parentWbsIdKey);

                    Add MonthUpdate value
                    SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

                    Intern strings
                    for (int k = 0; k < transformed.Length; k++)
                    {
                        transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);
                    }

                    transformedRowsBag.Add(new DataRow(transformed, originalFilename));
                });

                Post-processing: Validate ParentWbsIdKey
                If a parent key doesn't exist as a primary WbsIdKey in the dataset, it's invalid (e.g., reference to an external project WBS or orphaned node)
                int parentKeyIndex = finalIndexes[FieldNames.ParentWbsIdKey];
                foreach (var dataRow in transformedRowsBag)
                {
                    var fields = dataRow.Fields;
                    string parentKey = fields[parentKeyIndex];
                    if (!string.IsNullOrEmpty(parentKey) && !allWbsIdKeys.ContainsKey(parentKey))
                    {
                        fields[parentKeyIndex] = string.Empty; // Nullify invalid parent key
                    }
                    resultTable.AddRow(dataRow);
                }

                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerProjWbs03}: {ex.Message}");
                return null;
            }
        }
```

#### New Implementation
```csharp
public XerTable? Create03XerProjWbsTable()
{
    ClearGenerationFailure(EnhancedTableNames.XerProjWbs03);

    var projwbsTable = _dataStore.GetTable(TableNames.ProjWbs);
    if (!IsTableValid(projwbsTable)) return null;

    try
    {
        var sourceHeaders = projwbsTable.Headers;
        var idx = projwbsTable.FieldIndexes;

        string[] addedHeaders = { FieldNames.WbsIdKey, FieldNames.ParentWbsIdKey, FieldNames.MonthUpdate };
        string[] finalHeaders = CreateEnhancedHeaders(sourceHeaders, addedHeaders);
        var finalIndexes = finalHeaders
            .Select((name, index) => new { name, index })
            .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);

        var resultTable = new XerTable(EnhancedTableNames.XerProjWbs03, projwbsTable.RowCount);
        resultTable.SetHeaders(finalHeaders);

        var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
        var transformedRows = new DataRow[projwbsTable.RowCount];
        var parentMap = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var projMap = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var allWbsIdKeys = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);

        Parallel.For(0, projwbsTable.RowCount, parallelOptions, rowIndex =>
        {
            DataRow sourceRow = projwbsTable.Rows[rowIndex];
            var row = sourceRow.Fields;
            var transformed = new string[finalHeaders.Length];
            string originalFilename = sourceRow.SourceFilename;

            string wbsId = GetFieldValue(row, idx, FieldNames.WbsId);
            string parentWbsId = GetFieldValue(row, idx, FieldNames.ParentWbsId);
            string projId = GetFieldValue(row, idx, FieldNames.ProjectId);

            string wbsIdKey = CreateKey(sourceRow.SourceToken, wbsId);
            string parentWbsIdKey = string.IsNullOrEmpty(parentWbsId) ? string.Empty : CreateKey(sourceRow.SourceToken, parentWbsId);

            allWbsIdKeys.TryAdd(wbsIdKey, 0);

            if (!string.IsNullOrEmpty(parentWbsIdKey) && !string.Equals(wbsIdKey, parentWbsIdKey, StringComparison.OrdinalIgnoreCase))
            {
                parentMap.TryAdd(wbsIdKey, parentWbsIdKey);
            }

            if (!string.IsNullOrEmpty(projId))
            {
                projMap.TryAdd(wbsIdKey, projId);
            }

            for (int col = 0; col < sourceHeaders.Length && col < row.Length; col++)
            {
                string rawHeader = sourceHeaders[col];
                if (finalIndexes.TryGetValue(rawHeader, out int targetIdx))
                    transformed[targetIdx] = row[col] ?? string.Empty;
                else if (finalIndexes.TryGetValue($"raw_{rawHeader}", out int rawTargetIdx))
                    transformed[rawTargetIdx] = row[col] ?? string.Empty;
            }

            SetTransformedField(transformed, finalIndexes, FieldNames.WbsIdKey, wbsIdKey);
            SetTransformedField(transformed, finalIndexes, FieldNames.ParentWbsIdKey, parentWbsIdKey);
            SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

            for (int k = 0; k < transformed.Length; k++)
                transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);

            transformedRows[rowIndex] = new DataRow(transformed, originalFilename, sourceRow.SourceToken);
        });

        // Break cycles and validate parent references
        var cyclicKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var visitedGlobal = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var currentPath = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var wbsKey in allWbsIdKeys.Keys)
        {
            if (visitedGlobal.Contains(wbsKey)) continue;

            currentPath.Clear();
            string? curr = wbsKey;

            while (!string.IsNullOrEmpty(curr) && parentMap.TryGetValue(curr, out var nextParent))
            {
                if (currentPath.Contains(curr))
                {
                    cyclicKeys.Add(curr);
                    break;
                }

                if (visitedGlobal.Contains(curr)) break;

                currentPath.Add(curr);
                curr = nextParent;
            }

            foreach (var node in currentPath)
                visitedGlobal.Add(node);
        }

        int parentKeyIndex = finalIndexes[FieldNames.ParentWbsIdKey];
        int wbsKeyIndex = finalIndexes[FieldNames.WbsIdKey];

        for (int i = 0; i < transformedRows.Length; i++)
        {
            var fields = transformedRows[i].Fields;
            string parentKey = fields[parentKeyIndex];
            string currentWbsKey = fields[wbsKeyIndex];

            if (!string.IsNullOrEmpty(parentKey))
            {
                bool invalidRef = !allWbsIdKeys.ContainsKey(parentKey);
                bool isSelfRef = string.Equals(parentKey, currentWbsKey, StringComparison.OrdinalIgnoreCase);
                bool isCyclic = cyclicKeys.Contains(currentWbsKey);

                bool crossProject = false;
                if (projMap.TryGetValue(currentWbsKey, out var currentProj) &&
                    projMap.TryGetValue(parentKey, out var parentProj))
                {
                    crossProject = !string.Equals(currentProj, parentProj, StringComparison.OrdinalIgnoreCase);
                }

                if (invalidRef || isSelfRef || isCyclic || crossProject)
                {
                    fields[parentKeyIndex] = string.Empty;
                }
            }
        }

        resultTable.AddRows(transformedRows);
        return resultTable;
    }
    catch (Exception ex)
    {
        RecordGenerationFailure(EnhancedTableNames.XerProjWbs03, ex.Message);
        return null;
    }
}
```

---

## 4. 04_XER_BASELINE

### What Changed
- Returns an initialized empty `XerTable` retaining schema headers rather than `null` when no baseline dates match.

#### Old Implementation
```csharp
        public XerTable Create04XerBaselineTable(XerTable task01Table)
        {
            if (!IsTableValid(task01Table)) return null;

            try
            {
                var baselineTable = new XerTable(EnhancedTableNames.XerBaseline04, task01Table.RowCount);
                baselineTable.SetHeaders(task01Table.Headers);

                --- CHANGE: Use MonthUpdate instead of DataDate ---
                if (!task01Table.FieldIndexes.TryGetValue(FieldNames.MonthUpdate, out int monthUpdateIndex)) return null;

                Find the minimum MonthUpdate date across all rows
                DateTime? minMonthUpdate = null;
                foreach (var rowData in task01Table.Rows)
                {
                    --- CHANGE: Get value from MonthUpdate column ---
                    var currentDate = DateParser.TryParse(XerTable.GetFieldValueSafe(rowData, monthUpdateIndex));
                    if (currentDate.HasValue)
                    {
                        Compare only the Date part, ignoring time
                        if (!minMonthUpdate.HasValue || currentDate.Value.Date < minMonthUpdate.Value.Date)
                        {
                            minMonthUpdate = currentDate.Value.Date;
                        }
                    }
                }

                if (!minMonthUpdate.HasValue) return null; // No valid dates found

                Filter rows matching the minimum MonthUpdate Date
                foreach (var sourceRow in task01Table.Rows)
                {
                    --- CHANGE: Get value from MonthUpdate column for comparison ---
                    var rowDate = DateParser.TryParse(XerTable.GetFieldValueSafe(sourceRow, monthUpdateIndex));
                    if (rowDate.HasValue && rowDate.Value.Date == minMonthUpdate.Value)
                    {
                        baselineTable.AddRow(sourceRow);
                    }
                }

                return baselineTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerBaseline04}: {ex.Message}");
                return null;
            }
        }
```

#### New Implementation
```csharp
public XerTable? Create04XerBaselineTable(XerTable task01Table)
{
    ClearGenerationFailure(EnhancedTableNames.XerBaseline04);
    if (!IsTableValid(task01Table)) return null;

    try
    {
        var baselineTable = new XerTable(EnhancedTableNames.XerBaseline04, task01Table.RowCount);
        baselineTable.SetHeaders(task01Table.Headers);

        if (!task01Table.FieldIndexes.TryGetValue(FieldNames.MonthUpdate, out int monthUpdateIndex))
        {
            RecordGenerationFailure(EnhancedTableNames.XerBaseline04, "MonthUpdate column missing in task01");
            return baselineTable;
        }

        DateTime? minMonthUpdate = null;
        foreach (var rowData in task01Table.Rows)
        {
            var currentDate = DateParser.TryParse(XerTable.GetFieldValueSafe(rowData, monthUpdateIndex));
            if (currentDate.HasValue)
            {
                if (!minMonthUpdate.HasValue || currentDate.Value.Date < minMonthUpdate.Value.Date)
                    minMonthUpdate = currentDate.Value.Date;
            }
        }

        if (!minMonthUpdate.HasValue)
        {
            return baselineTable;
        }

        foreach (var sourceRow in task01Table.Rows)
        {
            var rowDate = DateParser.TryParse(XerTable.GetFieldValueSafe(sourceRow, monthUpdateIndex));
            if (rowDate.HasValue && rowDate.Value.Date == minMonthUpdate.Value)
            {
                baselineTable.AddRow(sourceRow);
            }
        }

        return baselineTable;
    }
    catch (Exception ex)
    {
        RecordGenerationFailure(EnhancedTableNames.XerBaseline04, ex.Message);
        return null;
    }
}
```

---

## 5. 06_XER_PREDECESSOR

### What Changed
- Replaced forward date projection with inverse lag CPM calculator (`RelationshipFreeFloatCalculator`).
- Added support for `TT_TASK`, `TT_RSRC`, `TT_MILE`, and `TT_FINMILE`.
- Added support for 4 lag calendar modes (`rcal_Predecessor`, `rcal_Successor`, `rcal_24Hour`, `rcal_ProjectDefault`).
- Added Retained Logic vs Progress Override handling (`use_actual_dates_flag`).
- Preserved row ordering using deterministic array.

#### Old Implementation
```csharp
        public XerTable Create06XerPredecessor(ConcurrentDictionary<string, XerTable> cache)
        {
            var taskPredTable = _dataStore.GetTable(TableNames.TaskPred);
            var taskTable = _dataStore.GetTable(TableNames.Task);
            var calendarTable = _dataStore.GetTable(TableNames.Calendar);
            Get the SCHEDOPTIONS table
            var schedOptionsTable = _dataStore.GetTable("SCHEDOPTIONS");

            We no longer read from 11_DETAILED_CALENDAR cache, 
            BuildCalendarCalculators now reads from the raw CALENDAR table.

            if (!IsTableValid(taskPredTable)) return null;

            try
            {
                var sourceHeaders = taskPredTable.Headers;
                var sourceIndexes = taskPredTable.FieldIndexes;

                Define all output columns (existing + new calculated columns)
                var finalHeadersList = sourceHeaders.ToList();
                finalHeadersList.Add(FieldNames.TaskIdKey);
                finalHeadersList.Add(FieldNames.PredTaskIdKey);
                finalHeadersList.Add(FieldNames.CalendarIdKey); // Successor's calendar (used for float calculation)
                finalHeadersList.Add(FieldNames.PredecessorClndrIdKey); // Predecessor's calendar (used for lag conversion)
                finalHeadersList.Add(FieldNames.StatusCode); // Successor's status
                finalHeadersList.Add(FieldNames.PredecessorStatusCode); // Predecessor's status
                finalHeadersList.Add(FieldNames.TaskType); // Successor's task type
                finalHeadersList.Add(FieldNames.PredecessorTaskType); // Predecessor's task type
                finalHeadersList.Add(FieldNames.Lag); // This will remain in DAYS, but calculated from hours
                finalHeadersList.Add(FieldNames.TimePeriodHoursPerDay);
                finalHeadersList.Add(FieldNames.Start);
                finalHeadersList.Add(FieldNames.Finish);
                finalHeadersList.Add(FieldNames.PredecessorStart);
                finalHeadersList.Add(FieldNames.PredecessorFinish);
                finalHeadersList.Add(FieldNames.PredecessorFreeFloat); // This is the new 'free_float' in DAYS
                finalHeadersList.Add(FieldNames.MonthUpdate);
                string[] finalHeaders = finalHeadersList.Select(StringInternPool.Intern).ToArray();

                Pre-calculate indexes for O(1) lookups
                var finalIndexes = finalHeaders
                    .Select((name, index) => new { name, index })
                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);

                var resultTable = new XerTable(EnhancedTableNames.XerPredecessor06, taskPredTable.RowCount);
                resultTable.SetHeaders(finalHeaders);

                Build lookup dictionaries
                var taskLookup = BuildTaskLookupDictionary(taskTable); // Includes ActualStartDate
                var calendarHoursLookup = BuildCalendarHoursLookup(calendarTable); // Standard Hours/Day
                var calendarCalculators = BuildCalendarCalculators(null); // Pass null, it now reads from raw CALENDAR
                var schedOptionsLookup = BuildProjectScheduleOptionsLookup(schedOptionsTable);

                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
                var transformedRowsBag = new ConcurrentBag<DataRow>();

                Parallel.ForEach(taskPredTable.Rows, parallelOptions, sourceRow =>
                {
                    var row = sourceRow.Fields;
                    string[] transformed = new string[finalHeaders.Length];
                    string originalFilename = sourceRow.SourceFilename;

                    Copy existing fields
                    int copyLength = Math.Min(row.Length, sourceHeaders.Length);
                    Array.Copy(row, transformed, copyLength);

                    Generate keys
                    string taskId = GetFieldValue(row, sourceIndexes, FieldNames.TaskId);
                    string predTaskId = GetFieldValue(row, sourceIndexes, FieldNames.PredTaskId);
                    string taskIdKey = CreateKey(originalFilename, taskId);
                    string predTaskIdKey = CreateKey(originalFilename, predTaskId);

                    SetTransformedField(transformed, finalIndexes, FieldNames.TaskIdKey, taskIdKey);
                    SetTransformedField(transformed, finalIndexes, FieldNames.PredTaskIdKey, predTaskIdKey);

                    Lookup successor task data
                    TaskData succTask = taskLookup.TryGetValue(taskIdKey, out var st) ? st : new TaskData();
                    Lookup predecessor task data
                    TaskData predTask = taskLookup.TryGetValue(predTaskIdKey, out var pt) ? pt : new TaskData();

                    --- FIX IS HERE ---
                    Define the variables for the successor's and predecessor's calendar keys
                    string succClndrIdKey = succTask.ClndrIdKey;
                    string predClndrIdKey = predTask.ClndrIdKey;
                    --- END FIX ---

                    Set lookup columns
                    SetTransformedField(transformed, finalIndexes, FieldNames.CalendarIdKey, succClndrIdKey);
                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorClndrIdKey, predClndrIdKey);
                    SetTransformedField(transformed, finalIndexes, FieldNames.StatusCode, succTask.StatusCode);
                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorStatusCode, predTask.StatusCode);
                    SetTransformedField(transformed, finalIndexes, FieldNames.TaskType, succTask.TaskType);
                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorTaskType, predTask.TaskType);
                    SetTransformedField(transformed, finalIndexes, FieldNames.Start, DateParser.Format(succTask.EarlyStartDate));
                    SetTransformedField(transformed, finalIndexes, FieldNames.Finish, DateParser.Format(succTask.EarlyEndDate));
                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorStart, DateParser.Format(predTask.EarlyStartDate));
                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorFinish, DateParser.Format(predTask.EarlyEndDate));

                    Calculate TimePeriod (hours per day) using PREDECESSOR's calendar
                    decimal hoursPerDay = 0;
                    if (!string.IsNullOrEmpty(predClndrIdKey) && calendarHoursLookup.TryGetValue(predClndrIdKey, out decimal hpd))
                    {
                        hoursPerDay = hpd;
                    }
                    SetTransformedField(transformed, finalIndexes, FieldNames.TimePeriodHoursPerDay, hoursPerDay > 0 ? hoursPerDay.ToString("F2", CultureInfo.InvariantCulture) : "");

                    Get Lag in HOURS
                    string lagHrCntStr = GetFieldValue(row, sourceIndexes, FieldNames.LagHrCnt);
                    decimal.TryParse(lagHrCntStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal lagHours);

                    Calculate Lag in DAYS for the 'Lag' column
                    decimal lagDays = 0;
                    if (lagHours != 0 && hoursPerDay > 0)
                    {
                        lagDays = lagHours / hoursPerDay;
                    }
                    SetTransformedField(transformed, finalIndexes, FieldNames.Lag, lagDays != 0 ? lagDays.ToString("F2", CultureInfo.InvariantCulture) : "0");

                    --- NEW: Get Successor's HPD for final conversion ---
                    decimal hoursPerDayForSuccessor = 0;
                    if (calendarHoursLookup.TryGetValue(succClndrIdKey, out decimal succHpd))
                    {
                        hoursPerDayForSuccessor = succHpd;
                    }
                    if (hoursPerDayForSuccessor <= 0) hoursPerDayForSuccessor = 8m; // Fallback


                    Calculate Free Float (passing lag in HOURS)
                    string freeFloatInDays = CalculateFreeFloat(
                        row, sourceIndexes,
                        succTask, predTask,
                        lagHours, // Pass hours, not days
                        predClndrIdKey,
                        succClndrIdKey,
                        calendarCalculators,
                        schedOptionsLookup,
                        hoursPerDayForSuccessor // Pass successor's HPD
                    );
                    SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorFreeFloat, freeFloatInDays);

                    Add MonthUpdate value
                    SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

                    Intern strings
                    for (int k = 0; k < transformed.Length; k++)
                    {
                        transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);
                    }

                    transformedRowsBag.Add(new DataRow(transformed, originalFilename));
                });

                resultTable.AddRows(transformedRowsBag);
                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerPredecessor06}: {ex.Message}");
                return null;
            }
        }

        private struct TaskData
        {
            public string ProjIdKey;
            public string ClndrIdKey;
            public string StatusCode;
            public string TaskType;
            public string TaskCode;
            public DateTime? EarlyStartDate;
            public DateTime? EarlyEndDate;
            public DateTime? ActualStartDate;
            public DateTime? ActualEndDate;
        }

        private Dictionary<string, TaskData> BuildTaskLookupDictionary(XerTable taskTable)
        {
            var lookup = new Dictionary<string, TaskData>(StringComparer.OrdinalIgnoreCase);
            if (!IsTableValid(taskTable)) return lookup;

            var indexes = taskTable.FieldIndexes;
            foreach (var rowData in taskTable.Rows)
            {
                var row = rowData.Fields;
                string taskId = GetFieldValue(row, indexes, FieldNames.TaskId);
                string clndrId = GetFieldValue(row, indexes, FieldNames.CalendarId);
                string projId = GetFieldValue(row, indexes, FieldNames.ProjectId); // Get ProjId

                string taskIdKey = CreateKey(rowData.SourceFilename, taskId);
                string clndrIdKey = CreateKey(rowData.SourceFilename, clndrId);
                string projIdKey = CreateKey(rowData.SourceFilename, projId); // Create ProjIdKey

                var taskData = new TaskData
                {
                    ProjIdKey = projIdKey, // Store ProjIdKey
                    ClndrIdKey = clndrIdKey,
                    StatusCode = GetFieldValue(row, indexes, FieldNames.StatusCode),
                    TaskType = GetFieldValue(row, indexes, FieldNames.TaskType),
                    TaskCode = GetFieldValue(row, indexes, FieldNames.TaskCode),
                    EarlyStartDate = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.EarlyStartDate)),
                    EarlyEndDate = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.EarlyEndDate)),
                    ActualStartDate = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActStartDate)),
                    ActualEndDate = DateParser.TryParse(GetFieldValue(row, indexes, FieldNames.ActEndDate))
                };

                lookup[taskIdKey] = taskData;
            }
            return lookup;
        }

        private Dictionary<string, WorkingDayCalculator> BuildCalendarCalculators(XerTable calendarDetailedTable)
        {
            var calculators = new Dictionary<string, WorkingDayCalculator>(StringComparer.OrdinalIgnoreCase);

            This is the main method that reads the raw CALENDAR table.
            Console.WriteLine("Building Calendar Calculators for Free Float...");
            var calendarTable = _dataStore.GetTable(TableNames.Calendar);
            if (!IsTableValid(calendarTable))
            {
                Console.WriteLine("Warning: Raw CALENDAR table not found. Free Float calculations may be incorrect.");
                return calculators;
            }

            var calIndexes = calendarTable.FieldIndexes;
            int rawClndrIdIdx = calIndexes.ContainsKey(FieldNames.ClndrId) ? calIndexes[FieldNames.ClndrId] : -1;
            int rawDataIdx = calIndexes.ContainsKey(FieldNames.CalendarData) ? calIndexes[FieldNames.CalendarData] : -1;
            int rawDayHrCntIdx = calIndexes.ContainsKey(FieldNames.DayHourCount) ? calIndexes[FieldNames.DayHourCount] : -1;

            if (rawClndrIdIdx == -1 || rawDataIdx == -1 || rawDayHrCntIdx == -1)
            {
                Console.WriteLine("Warning: Raw CALENDAR table is missing required columns. Free Float calculations may be incorrect.");
                return calculators;
            }

            --- OPTIMIZATION: Use static, compiled Regex ---
            Regex for all day definitions (1-7). Updated to handle optional spacing better.
            var dayPatternRegex = new Regex(
                @"\(\s*0\|\|(\d)\(\)\s*\(((?:[^()]|\((?:[^()]|\([^()]*\))*\))*)\)\s*\)",
                RegexOptions.Singleline | RegexOptions.Compiled);
            Regex specifically for empty day definitions
            var emptyDayPatternRegex = new Regex(
                @"\(\s*0\|\|(\d)\(\)\s*\(\s*\)\s*\)",
                RegexOptions.Singleline | RegexOptions.Compiled);
            Regex for time slots (Main pattern)
            var timeSlotRegex = new Regex(
                @"\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)",
                RegexOptions.Singleline | RegexOptions.Compiled);

            Parse the raw CALENDAR table for each calendar
            foreach (var rowData in calendarTable.Rows)
            {
                var row = rowData.Fields;
                string clndrId = XerTable.GetFieldValueSafe(rowData, rawClndrIdIdx);
                string clndrIdKey = CreateKey(rowData.SourceFilename, clndrId);
                string clndrData = XerTable.GetFieldValueSafe(rowData, rawDataIdx);
                string defaultDayHrCntStr = XerTable.GetFieldValueSafe(rowData, rawDayHrCntIdx);

                decimal.TryParse(defaultDayHrCntStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal defaultDayHours);
                if (defaultDayHours <= 0) defaultDayHours = 8m; // Fallback

                var exceptionHours = new Dictionary<DateTime, decimal>();
                var exceptionTimeSlots = new Dictionary<DateTime, List<(TimeSpan, TimeSpan)>>();
                var standardWeekHours = new decimal[7]; // 0=Sun ... 6=Sat
                var standardWeekTimeSlots = new List<(TimeSpan, TimeSpan)>[7];
                for (int i = 0; i < 7; i++) standardWeekTimeSlots[i] = new List<(TimeSpan, TimeSpan)>();

                bool hasStandardWeekDefined = false;

                --- 1. Parse Standard Week ---
                int daysStart = clndrData.IndexOf("DaysOfWeek");
                if (daysStart > -1)
                {
                    int daysEnd = FindSectionEnd(clndrData, daysStart);
                    string daysSection = clndrData.Substring(daysStart, daysEnd - daysStart);

                    Use dictionaries to store results from Regex matches
                    var dayContents = new Dictionary<int, string>();
                    var emptyDays = new HashSet<int>();

                    Match all occurrences using the static compiled Regex (dayPatternRegex)
                    foreach (Match match in dayPatternRegex.Matches(daysSection))
                    {
                        if (int.TryParse(match.Groups[1].Value, out int p6DayNum) && p6DayNum >= 1 && p6DayNum <= 7) // P6 Day (1-7)
                        {
                            dayContents[p6DayNum] = match.Groups[2].Value.Trim();
                        }
                    }

                    Match all occurrences using the static compiled Regex (emptyDayPatternRegex)
                    foreach (Match match in emptyDayPatternRegex.Matches(daysSection))
                    {
                        if (int.TryParse(match.Groups[1].Value, out int p6DayNum) && p6DayNum >= 1 && p6DayNum <= 7)
                        {
                            emptyDays.Add(p6DayNum);
                            dayContents.Remove(p6DayNum); // Ensure it's not in both
                        }
                    }

                    Only proceed if we actually found standard week definitions
                    if (dayContents.Count > 0 || emptyDays.Count > 0)
                    {
                        hasStandardWeekDefined = true;
                        Process the results for all 7 days
                        for (int p6DayNum = 1; p6DayNum <= 7; p6DayNum++) // P6: 1=Sun, 2=Mon... 7=Sat
                        {
                            int dotNetDay = p6DayNum - 1; // .NET: 0=Sun, 1=Mon... 6=Sat

                            if (dayContents.TryGetValue(p6DayNum, out string dayContent) && !string.IsNullOrWhiteSpace(dayContent))
                            {
                                Day has defined work hours
                                standardWeekHours[dotNetDay] = ParseDayWorkHours(dayContent, timeSlotRegex, out var slots);
                                standardWeekTimeSlots[dotNetDay] = slots;
                            }
                            else if (emptyDays.Contains(p6DayNum))
                            {
                                Day is explicitly defined as non-working
                                standardWeekHours[dotNetDay] = 0m;
                            }
                            else
                            {
                                Defined in DaysOfWeek section but missing specific day content? 
                                P6 convention: 1 (Sun) and 7 (Sat) default to non-work; others default to standard.
                                if (p6DayNum == 1 || p6DayNum == 7)
                                {
                                    standardWeekHours[dotNetDay] = 0m;
                                }
                                else
                                {
                                    Weekday default
                                    standardWeekHours[dotNetDay] = defaultDayHours;
                                    Add default time slots
                                    AddDefaultTimeSlots(standardWeekTimeSlots[dotNetDay], defaultDayHours);
                                }
                            }
                        }
                    }
                }

                --- FALLBACK: If no standard week defined, use Defaults (Mon-Fri) ---
                This fixes the issue where missing DaysOfWeek blocks resulted in 0 working days
                if (!hasStandardWeekDefined)
                {
                    for (int p6DayNum = 1; p6DayNum <= 7; p6DayNum++)
                    {
                        int dotNetDay = p6DayNum - 1;
                        if (p6DayNum == 1 || p6DayNum == 7) // Sun/Sat
                        {
                            standardWeekHours[dotNetDay] = 0m;
                        }
                        else // Mon-Fri
                        {
                            standardWeekHours[dotNetDay] = defaultDayHours;
                            AddDefaultTimeSlots(standardWeekTimeSlots[dotNetDay], defaultDayHours);
                        }
                    }
                }

                --- 2. Parse Exceptions ---
                var exceptionSections = new[] { "Exceptions", "HolidayOrExceptions", "HolidayOrException" };
                foreach (var sectionName in exceptionSections)
                {
                    int excStart = clndrData.IndexOf(sectionName);
                    if (excStart > -1)
                    {
                        int excEnd = FindSectionEnd(clndrData, excStart);
                        string excSection = excEnd > excStart ? clndrData.Substring(excStart, excEnd - excStart) : clndrData.Substring(excStart);

                        var excMatches = ExcPatternRegex.Matches(excSection);
                        ParseAndStoreExceptions(excMatches, exceptionHours, exceptionTimeSlots, timeSlotRegex);

                        excMatches = ExcPatternRegex2.Matches(excSection);
                        ParseAndStoreExceptions(excMatches, exceptionHours, exceptionTimeSlots, timeSlotRegex);
                    }
                }

                --- 3. Create the Calculator ---
                var calculator = new WorkingDayCalculator(exceptionHours, exceptionTimeSlots, standardWeekHours, standardWeekTimeSlots);
                calculators[clndrIdKey] = calculator;
            }

            Console.WriteLine($"Built {calculators.Count} hour-aware calendar calculators.");
            return calculators;
        }

        private void ParseAndStoreExceptions(MatchCollection matches,
            Dictionary<DateTime, decimal> exceptionHours,
            Dictionary<DateTime, List<(TimeSpan, TimeSpan)>> exceptionTimeSlots,
            Regex timeSlotRegex)
        {
            foreach (Match excMatch in matches)
            {
                if (excMatch.Success && excMatch.Groups.Count >= 3)
                {
                    if (int.TryParse(excMatch.Groups[2].Value, out int dateSerial))
                    {
                        DateTime excDate = ConvertFromOleDate(dateSerial).Date;
                        string workContent = excMatch.Groups.Count > 3 ? excMatch.Groups[3].Value.Trim() : "()";

                        if (string.IsNullOrEmpty(workContent) || workContent == "()")
                        {
                            exceptionHours[excDate] = 0m; // Holiday
                            exceptionTimeSlots[excDate] = new List<(TimeSpan, TimeSpan)>();
                        }
                        else
                        {
                            decimal hours = ParseDayWorkHours(workContent, timeSlotRegex, out var slots);
                            exceptionHours[excDate] = hours;
                            exceptionTimeSlots[excDate] = slots;
                        }
                    }
                }
            }
        }

        / <summary>
        / This is an OVERLOAD for the existing ParseDayWorkHours.
        / It is used by the new BuildCalendarCalculators.
        / Updated to include fallback regex patterns for robustness.
        / </summary>
        private decimal ParseDayWorkHours(string content, Regex timeSlotRegex, out List<(TimeSpan Start, TimeSpan End)> slots)
        {
            slots = new List<(TimeSpan, TimeSpan)>();
            if (string.IsNullOrWhiteSpace(content)) return 0;

            decimal totalHours = 0;

            1. Try Standard Pattern (with parentheses) - e.g. (s|08:00|f|17:00)
            var matches = timeSlotRegex.Matches(content);

            if (matches.Count > 0)
            {
                foreach (Match match in matches)
                {
                    ProcessTimeSlotMatch(match, slots, ref totalHours);
                }
                return totalHours;
            }

            2. Try Fallback Pattern (bare, no parentheses) - e.g. s|08:00|f|17:00
            This handles legacy/corrupted formats that the main pattern misses
            var simpleRegex = new Regex(@"([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})", RegexOptions.Singleline);
            matches = simpleRegex.Matches(content);

            if (matches.Count > 0)
            {
                var processedPairs = new HashSet<string>();
                foreach (Match match in matches)
                {
                    Prevent duplicates if regex overlaps
                    string timeKey = $"{match.Groups[2].Value}-{match.Groups[4].Value}";
                    if (processedPairs.Contains(timeKey)) continue;
                    processedPairs.Add(timeKey);

                    ProcessTimeSlotMatch(match, slots, ref totalHours);
                }
            }

            return totalHours;
        }

        private void ProcessTimeSlotMatch(Match match, List<(TimeSpan Start, TimeSpan End)> slots, ref decimal totalHours)
        {
            if (match.Groups.Count >= 5)
            {
                if (TimeSpan.TryParse(match.Groups[2].Value, out TimeSpan time1) && TimeSpan.TryParse(match.Groups[4].Value, out TimeSpan time2))
                {
                    string type1 = match.Groups[1].Value;
                    string type2 = match.Groups[3].Value;
                    TimeSpan start, end;

                    if (type1 == "s" && type2 == "f") { start = time1; end = time2; }
                    else if (type1 == "f" && type2 == "s") { start = time2; end = time1; }
                    else { start = time1 < time2 ? time1 : time2; end = time1 < time2 ? time2 : time1; }

                    Handle 24:00 as end of day
                    Case 1: (s|HH:MM|f|00:00) - work ends at midnight
                    if (end == TimeSpan.Zero && start != TimeSpan.Zero)
                    {
                        end = new TimeSpan(24, 0, 0);
                    }
                    Case 2: (s|00:00|f|00:00) - 24-hour workday (full day)
                    else if (start == TimeSpan.Zero && end == TimeSpan.Zero)
                    {
                        end = new TimeSpan(24, 0, 0);
                    }

                    slots.Add((start, end));
                    totalHours += CalculateHoursBetween(start, end);
                }
            }
        }

        private void AddDefaultTimeSlots(List<(TimeSpan, TimeSpan)> slots, decimal defaultDayHours)
        {
            if (defaultDayHours == 8m)
            {
                slots.Add((new TimeSpan(8, 0, 0), new TimeSpan(12, 0, 0)));
                slots.Add((new TimeSpan(13, 0, 0), new TimeSpan(17, 0, 0)));
            }
            else if (defaultDayHours > 0)
            {
                slots.Add((new TimeSpan(8, 0, 0), new TimeSpan(8, 0, 0).Add(TimeSpan.FromHours((double)defaultDayHours))));
            }
        }

        private string CalculateFreeFloat(
            string[] predRow,
            IReadOnlyDictionary<string, int> predIndexes,
            TaskData succTask,
            TaskData predTask,
            decimal lagHours,
            string predClndrIdKey,
            string succClndrIdKey,
            Dictionary<string, WorkingDayCalculator> calendarCalculators,
            Dictionary<string, string> schedOptionsLookup,
            decimal hoursPerDayForSuccessor)
        {
            const string InProgress = "TK_Active";
            const string Complete = "TK_Complete";
            const string LOE = "TT_LOE";
            const string WBS = "TT_WBS";

            Exclude LOE and WBS Summary types
            if (succTask.TaskType == LOE || succTask.TaskType == WBS ||
                predTask.TaskType == LOE || predTask.TaskType == WBS)
                return "";

            Skip completed predecessors - constraint already satisfied (forward-looking approach)
            if (predTask.StatusCode == Complete)
                return "";

            --- Calendar Setup ---
            string predType = GetFieldValue(predRow, predIndexes, FieldNames.PredType);

            WorkingDayCalculator predCalendar = WorkingDayCalculator.Default;
            if (!string.IsNullOrEmpty(predClndrIdKey) && calendarCalculators.TryGetValue(predClndrIdKey, out var predCal))
                predCalendar = predCal;

            WorkingDayCalculator succCalendar = WorkingDayCalculator.Default;
            if (!string.IsNullOrEmpty(succClndrIdKey) && calendarCalculators.TryGetValue(succClndrIdKey, out var succCal))
                succCalendar = succCal;

            string lagCalendarSetting = "rcal_Predecessor";
            if (schedOptionsLookup != null && !string.IsNullOrEmpty(succTask.ProjIdKey) &&
                schedOptionsLookup.TryGetValue(succTask.ProjIdKey, out string setting))
            {
                if (!string.IsNullOrEmpty(setting))
                    lagCalendarSetting = setting;
            }
            WorkingDayCalculator lagProjectionCalendar = (lagCalendarSetting == "rcal_Successor") ? succCalendar : predCalendar;

            --- STEP 1: Determine Base Date and Target Date ---
            DateTime? predBaseDate = null;
            DateTime? succTargetDate = null;

            switch (predType)
            {
                case "PR_FS": // Finish-to-Start
                    succTargetDate = succTask.EarlyStartDate;

                    Predecessor is Not Started or In Progress (Complete already filtered)
                    For In Progress: Use Early Finish (Data Date + Remaining Duration)
                    For Not Started: Use Early Finish (calculated)
                    predBaseDate = predTask.EarlyEndDate;
                    break;

                case "PR_SS": // Start-to-Start
                    succTargetDate = succTask.EarlyStartDate;

                    For In Progress: Use Actual Start (when work really began)
                    For Not Started: Use Early Start (calculated)
                    if (predTask.StatusCode == InProgress && predTask.ActualStartDate.HasValue)
                    {
                        predBaseDate = predTask.ActualStartDate;
                    }
                    else
                    {
                        predBaseDate = predTask.EarlyStartDate;
                    }
                    break;

                case "PR_FF": // Finish-to-Finish
                    succTargetDate = succTask.EarlyEndDate;

                    Same as FS - use Early Finish
                    predBaseDate = predTask.EarlyEndDate;
                    break;

                case "PR_SF": // Start-to-Finish
                    succTargetDate = succTask.EarlyEndDate;

                    Same as SS
                    if (predTask.StatusCode == InProgress && predTask.ActualStartDate.HasValue)
                    {
                        predBaseDate = predTask.ActualStartDate;
                    }
                    else
                    {
                        predBaseDate = predTask.EarlyStartDate;
                    }
                    break;

                default:
                    return "";
            }

            if (!predBaseDate.HasValue || !succTargetDate.HasValue)
                return "";

            --- STEP 2: Project Date with Lag ---
            DateTime projectedDate = lagProjectionCalendar.AddWorkingHours(predBaseDate.Value, lagHours);

            --- STEP 3: Measure Float ---
            decimal freeFloatInHours = succCalendar.CountWorkingHours(projectedDate, succTargetDate.Value);

            --- STEP 4: Convert to DAYS ---
            decimal freeFloatInDays = 0;
            if (hoursPerDayForSuccessor > 0)
            {
                freeFloatInDays = freeFloatInHours / hoursPerDayForSuccessor;
            }

            return freeFloatInDays.ToString("F2", CultureInfo.InvariantCulture);
        }

    public class WorkingDayCalculator
    {
        private readonly Dictionary<DateTime, decimal> _exceptionHours;
        private readonly Dictionary<DateTime, List<(TimeSpan Start, TimeSpan End)>> _exceptionTimeSlots;
        private readonly decimal[] _standardWeekHours; // Index 0=Sunday, 1=Monday... 6=Saturday
        private readonly List<(TimeSpan Start, TimeSpan End)>[] _standardWeekTimeSlots; // Index 0=Sunday...
        private static readonly WorkingDayCalculator _default = CreateDefaultCalendar();

        public WorkingDayCalculator(
            Dictionary<DateTime, decimal> exceptionHours,
            Dictionary<DateTime, List<(TimeSpan Start, TimeSpan End)>> exceptionTimeSlots,
            decimal[] standardWeekHours,
            List<(TimeSpan Start, TimeSpan End)>[] standardWeekTimeSlots)
        {
            _exceptionHours = exceptionHours ?? new Dictionary<DateTime, decimal>();
            _exceptionTimeSlots = exceptionTimeSlots ?? new Dictionary<DateTime, List<(TimeSpan, TimeSpan)>>();
            _standardWeekHours = standardWeekHours ?? new decimal[7];
            _standardWeekTimeSlots = standardWeekTimeSlots ?? new List<(TimeSpan, TimeSpan)>[7];
        }

        Creates a default 5-day work week calendar (Mon-Fri, 8-12, 13-17)
        private static WorkingDayCalculator CreateDefaultCalendar()
        {
            var stdHours = new decimal[7]; // 0=Sun, 6=Sat
            var stdSlots = new List<(TimeSpan, TimeSpan)>[7];
            var defaultSlots = new List<(TimeSpan, TimeSpan)>
            {
                (new TimeSpan(8, 0, 0), new TimeSpan(12, 0, 0)),
                (new TimeSpan(13, 0, 0), new TimeSpan(17, 0, 0))
            };

            for (int i = 0; i < 7; i++)
            {
                stdSlots[i] = new List<(TimeSpan, TimeSpan)>(); // Initialize all lists
            }

            for (int i = 1; i <= 5; i++) // 1=Monday to 5=Friday
            {
                stdHours[i] = 8m;
                stdSlots[i] = defaultSlots;
            }
            Sunday (0) and Saturday (6)
            stdHours[0] = 0m;
            stdHours[6] = 0m;

            return new WorkingDayCalculator(
                new Dictionary<DateTime, decimal>(),
                new Dictionary<DateTime, List<(TimeSpan, TimeSpan)>>(),
                stdHours,
                stdSlots);
        }

        public static WorkingDayCalculator Default => _default;

        --- Helper Methods ---

        / <summary>
        / Gets the defined work hours for a specific date, checking exceptions first.
        / </summary>
        private decimal GetWorkHours(DateTime date)
        {
            date = date.Date;
            if (_exceptionHours.TryGetValue(date, out decimal hours))
            {
                return hours; // Return specific exception hours (could be 0)
            }
            return _standardWeekHours[(int)date.DayOfWeek]; // Return standard week hours
        }

        / <summary>
        / Gets the list of defined time slots for a specific date.
        / </summary>
        private List<(TimeSpan Start, TimeSpan End)> GetTimeSlots(DateTime date)
        {
            date = date.Date;
            if (_exceptionTimeSlots.TryGetValue(date, out var slots))
            {
                return slots; // Return specific exception slots
            }
            var stdSlots = _standardWeekTimeSlots[(int)date.DayOfWeek];
            return stdSlots ?? new List<(TimeSpan, TimeSpan)>(); // Ensure list is never null
        }

        / <summary>
        / Checks if a date is a working day (has > 0 work hours).
        / </summary>
        public bool IsWorkingDay(DateTime date)
        {
            return GetWorkHours(date.Date) > 0;
        }

        / <summary>
        / Calculates remaining work hours on a given day from a specific time.
        / </summary>
        private decimal GetRemainingHoursOnDay(DateTime startDateTime)
        {
            var slots = GetTimeSlots(startDateTime.Date);
            if (slots == null || slots.Count == 0)
            {
                return 0m;
            }

            TimeSpan startTime = startDateTime.TimeOfDay;
            decimal remainingHours = 0m;

            foreach (var (slotStart, slotEnd) in slots)
            {
                if (startTime < slotEnd) // If the time is before the end of the slot
                {
                    Find the effective start time (either the slot start or the current time, whichever is later)
                    TimeSpan effectiveStart = (startTime > slotStart) ? startTime : slotStart;
                    remainingHours += (decimal)(slotEnd - effectiveStart).TotalHours;
                }
            }
            return Math.Max(0, remainingHours);
        }

        / <summary>
        / Calculates hours worked on a given day up to a specific time.
        / </summary>
        private decimal GetHoursWorkedOnDay(DateTime endDateTime)
        {
            var slots = GetTimeSlots(endDateTime.Date);
            if (slots == null || slots.Count == 0)
            {
                return 0m;
            }

            TimeSpan endTime = endDateTime.TimeOfDay;
            decimal hoursWorked = 0m;

            foreach (var (slotStart, slotEnd) in slots)
            {
                if (endTime > slotStart) // If the time is after the start of the slot
                {
                    Find the effective end time (either the slot end or the current time, whichever is earlier)
                    TimeSpan effectiveEnd = (endTime < slotEnd) ? endTime : slotEnd;
                    hoursWorked += (decimal)(effectiveEnd - slotStart).TotalHours;
                }
            }
            return Math.Max(0, hoursWorked);
        }

        / <summary>
        / Finds the exact time on a given day when a certain amount of work has been completed.
        / </summary>
        private TimeSpan FindTimeForHoursWorked(DateTime date, decimal hoursWorked)
        {
            var slots = GetTimeSlots(date.Date);
            if (slots == null || slots.Count == 0)
            {
                return TimeSpan.Zero;
            }

            decimal hoursAccumulated = 0m;

            foreach (var (slotStart, slotEnd) in slots)
            {
                decimal slotDuration = (decimal)(slotEnd - slotStart).TotalHours;
                if (hoursAccumulated + slotDuration >= hoursWorked)
                {
                    The time is in this slot
                    decimal hoursNeededInSlot = hoursWorked - hoursAccumulated;
                    return slotStart + TimeSpan.FromHours((double)hoursNeededInSlot);
                }
                Time is after this slot, add this slot's full duration
                hoursAccumulated += slotDuration;
            }
            If hoursWorked > total work hours, return end of last slot
            return slots.LastOrDefault().End;
        }

        --- NEW CALCULATION METHODS (WITH NEGATIVE LAG FIX) ---

        / <summary>
        / Projects a date forward or backward by a specific number of working hours.
        / </summary>
        public DateTime AddWorkingHours(DateTime startDate, decimal lagHours)
        {
            if (lagHours == 0) return startDate;

            if (lagHours > 0)
            {
                return ProjectForward(startDate, lagHours);
            }
            else
            {
                Call new method for projecting backward
                return ProjectBackward(startDate, -lagHours); // Pass a positive duration
            }
        }

        private DateTime ProjectForward(DateTime startDate, decimal hoursToAdd)
        {
            DateTime currentDate = startDate;
            decimal hoursRemaining = hoursToAdd;

            1. Spend remaining hours on the start day
            decimal remainingDayHours = GetRemainingHoursOnDay(currentDate);
            if (remainingDayHours > hoursRemaining)
            {
                Lag finishes on the same day.
                We need to find the exact finish time.
                var slots = GetTimeSlots(currentDate.Date);
                TimeSpan currentTime = currentDate.TimeOfDay;
                foreach (var (slotStart, slotEnd) in slots)
                {
                    if (currentTime < slotEnd)
                    {
                        TimeSpan effectiveStart = (currentTime > slotStart) ? currentTime : slotStart;
                        decimal slotDuration = (decimal)(slotEnd - effectiveStart).TotalHours;

                        if (hoursRemaining <= slotDuration)
                        {
                            Finishes in this slot
                            return currentDate.Date + effectiveStart + TimeSpan.FromHours((double)hoursRemaining);
                        }
                        Finishes after this slot, consume this slot's hours
                        hoursRemaining -= slotDuration;
                        currentTime = slotEnd; // Move time to end of this slot
                    }
                }
                Should not be reachable if GetRemainingHoursOnDay was correct
                return currentDate.Date.AddDays(1);
            }

            Lag does not finish on the start day
            hoursRemaining -= remainingDayHours;
            currentDate = currentDate.Date.AddDays(1); // Move to start of next day

            2. Spend hours on full days
            while (true)
            {
                decimal dayHours = GetWorkHours(currentDate);
                if (dayHours > 0)
                {
                    if (hoursRemaining <= dayHours)
                    {
                        Lag finishes on this day
                        break;
                    }
                    Consume the full day and move to the next
                    hoursRemaining -= dayHours;
                }
                currentDate = currentDate.AddDays(1);
            }

            3. Find the exact finish time on the final day
            var finalDaySlots = GetTimeSlots(currentDate.Date);
            if (finalDaySlots == null || finalDaySlots.Count == 0)
            {
                This case should be impossible if GetWorkHours(currentDate) > 0
                return currentDate;
            }

            This is just `GetHoursWorkedOnDay` in reverse, which is `FindTimeForHoursWorked`
            TimeSpan finalTime = FindTimeForHoursWorked(currentDate, hoursRemaining);
            return currentDate.Date + finalTime;
        }

        private DateTime ProjectBackward(DateTime startDate, decimal hoursToSubtract)
        {
            DateTime currentDate = startDate;
            decimal hoursRemainingToSubtract = hoursToSubtract;

            1. "Un-spend" hours on the start day
            decimal hoursWorkedToday = GetHoursWorkedOnDay(currentDate);

            if (hoursWorkedToday > hoursRemainingToSubtract)
            {
                Lag finishes (starts) on the same day.
                decimal targetHoursWorked = hoursWorkedToday - hoursRemainingToSubtract;
                TimeSpan finalTime = FindTimeForHoursWorked(currentDate, targetHoursWorked);
                return currentDate.Date + finalTime;
            }

            Lag does not finish on the start day
            hoursRemainingToSubtract -= hoursWorkedToday;
            currentDate = currentDate.Date.AddDays(-1); // Move to previous day

            2. "Un-spend" hours on full days
            while (true)
            {
                decimal dayHours = GetWorkHours(currentDate);
                if (dayHours > 0)
                {
                    if (hoursRemainingToSubtract <= dayHours)
                    {
                        Lag finishes (starts) on this day
                        break;
                    }
                    Consume the full day and move to the previous
                    hoursRemainingToSubtract -= dayHours;
                }
                currentDate = currentDate.AddDays(-1);
            }

            3. Find the exact start time on the final day
            decimal hoursToWorkOnFinalDay = GetWorkHours(currentDate) - hoursRemainingToSubtract;
            TimeSpan finalTimeOnDay = FindTimeForHoursWorked(currentDate, hoursToWorkOnFinalDay);
            return currentDate.Date + finalTimeOnDay;
        }

        / <summary>
        / Counts the precise working hours between two DateTimes.
        / </summary>
        public decimal CountWorkingHours(DateTime startDate, DateTime endDate)
        {
            if (Math.Abs((startDate - endDate).TotalSeconds) < 1) return 0m;

            Handle reverse
            if (endDate < startDate)
            {
                return -CountWorkingHours(endDate, startDate);
            }

            decimal totalHours = 0m;
            DateTime currentDate = startDate.Date;

            --- 1. Handle Start Day (Partial Day) ---
            if (currentDate == endDate.Date)
            {
                Start and End are on the same day
                var slots = GetTimeSlots(currentDate);
                if (slots == null) return 0m;

                TimeSpan startTime = startDate.TimeOfDay;
                TimeSpan endTime = endDate.TimeOfDay;

                foreach (var (slotStart, slotEnd) in slots)
                {
                    TimeSpan effectiveStart = (startTime > slotStart) ? startTime : slotStart;
                    TimeSpan effectiveEnd = (endTime < slotEnd) ? endTime : slotEnd;

                    if (effectiveEnd > effectiveStart)
                    {
                        totalHours += (decimal)(effectiveEnd - effectiveStart).TotalHours;
                    }
                }
                return totalHours;
            }

            Start and End are on different days
            totalHours += GetRemainingHoursOnDay(startDate);
            currentDate = currentDate.AddDays(1);

            --- 2. Handle Full Days Between ---
            while (currentDate < endDate.Date)
            {
                totalHours += GetWorkHours(currentDate);
                currentDate = currentDate.AddDays(1);
            }

            --- 3. Handle End Day (Partial Day) ---
            totalHours += GetHoursWorkedOnDay(endDate);

            return totalHours;
        }
    }
```

#### New Implementation
```csharp
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
            FieldNames.TaskIdKey, FieldNames.PredTaskIdKey, FieldNames.CalendarIdKey,
            FieldNames.PredecessorClndrIdKey, FieldNames.StatusCode, FieldNames.PredecessorStatusCode,
            FieldNames.TaskType, FieldNames.PredecessorTaskType, FieldNames.Lag,
            FieldNames.TimePeriodHoursPerDay, FieldNames.Start, FieldNames.Finish,
            FieldNames.PredecessorStart, FieldNames.PredecessorFinish, FieldNames.PredecessorFreeFloat,
            FieldNames.TotalFloat, FieldNames.MonthUpdate
        };

        string[] finalHeaders = CreateEnhancedHeaders(taskPredTable.Headers, finalHeadersList.ToArray());
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

            string taskIdKey = succTask.ProjectId is null ? "" : CreateKey(sourceRow.SourceToken, taskId);
            string predTaskIdKey = predTask.ProjectId is null ? "" : CreateKey(sourceRow.SourceToken, predTaskId);

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
                hoursPerDay = hpd;
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
                hoursPerDayForSuccessor = succHpd;

            var assessment = AssessRelationship(sourceRow, relationshipTasks, relationshipCalendars,
                schedOptionsLookup, rowIndex, false, default);
            string freeFloatInDays = assessment.FormattedDays;
            SetTransformedField(transformed, finalIndexes, FieldNames.PredecessorFreeFloat, freeFloatInDays);

            string totalFloatVal = "";
            if (!string.Equals(succTask.StatusCode, "TK_Complete", StringComparison.OrdinalIgnoreCase) &&
                !string.IsNullOrEmpty(succTask.TotalFloatHrCnt) && hoursPerDayForSuccessor > 0)
            {
                if (decimal.TryParse(succTask.TotalFloatHrCnt, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal tfHours))
                    totalFloatVal = FormatRelationshipDays(tfHours, hoursPerDayForSuccessor, "F1");
            }
            SetTransformedField(transformed, finalIndexes, FieldNames.TotalFloat, totalFloatVal);
            SetTransformedField(transformed, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(sourceRow.SourceFilename));

            for (int k = 0; k < transformed.Length; k++)
                transformed[k] = StringInternPool.Intern(transformed[k] ?? string.Empty);

            transformedRows[rowIndex] = new DataRow(transformed, originalFilename, sourceRow.SourceToken);
        });

        resultTable.AddRows(transformedRows);
        return resultTable;
    }
    catch (Exception ex)
    {
        RecordGenerationFailure(EnhancedTableNames.XerPredecessor06, ex.Message);
        return null;
    }
}

```csharp
namespace XerToCsvConverter;

/// <summary>
/// The greatest signed predecessor working-time movement whose lagged endpoint does
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

```csharp
public enum RelationshipFloatClassification
{
    Calculated, Ignored, Historical, Unsupported, MissingData, InvalidData
}

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
    public string ReasonCode { get; init; } = "";
    public string Message { get; init; } = "";

    public string FormattedDays => Classification == RelationshipFloatClassification.Calculated
        ? FloatDays?.ToString("G29", CultureInfo.InvariantCulture) ?? "" : "";
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

    RelationshipFloatAssessment Result(RelationshipFloatClassification classification, string reason, string message) =>
        assessment with { Classification = classification, ReasonCode = reason, Message = message };
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
    if (predecessor.Type is not ("TT_TASK" or "TT_RSRC" or "TT_MILE" or "TT_FINMILE") ||
        successor.Type is not ("TT_TASK" or "TT_RSRC" or "TT_MILE" or "TT_FINMILE"))
        return Unsupported("UnsupportedActivityType", "LOE, WBS-summary or unknown activity types do not establish task-calendar movement.");
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
    var rawLag = row.GetEvaluationField("lag_hr_cnt");
    if (rawLag.State != XerRawFieldState.Present)
        return Missing("MissingRelationshipLag", "An absent, truncated or blank lag cannot establish zero lag.");
    if (!decimal.TryParse(rawLag.RawValue, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal lagHours))
        return Invalid("InvalidRelationshipLag", "Relationship lag is not a finite representable decimal hour value.");
    assessment = assessment with { EffectiveLagHours = lagHours };

    bool activeSuccessor = successor.Status == "TK_ACTIVE";
    if (activeSuccessor)
    {
        if (external)
            return Unsupported("UnverifiedMultiProjectProgressContext", "A progressed cross-project relationship needs a verified governing scheduling context.");
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
        if (options.DataDate is null)
            return Result(RawText(options.ProjectRow, "last_recalc_date").Length > 0
                    ? RelationshipFloatClassification.InvalidData : RelationshipFloatClassification.MissingData,
                "UnresolvedProjectDataDate", "Progress-sensitive evaluation needs a valid owning project Data Date.");
        foreach (var task in new[] { predecessor, successor }.Where(task => task.Status == "TK_ACTIVE"))
        {
            DateTime? actual = RawDate(task.Row, "act_start_date");
            if (!actual.HasValue)
                return Result(RawText(task.Row, "act_start_date").Length > 0
                        ? RelationshipFloatClassification.InvalidData : RelationshipFloatClassification.MissingData,
                    "UnresolvedActualStart", "Progress-sensitive evaluation needs explicit valid actual starts for active endpoints.");
            if (actual > options.DataDate)
                return Unsupported("ActualStartAfterDataDate", "The recorded actual start is after the Data Date; demonstrated progress semantics cannot be established.");
        }
        if (options.Mode == "ProgressOverride")
            return type == "PR_FS" && lagHours == 0
                ? Result(RelationshipFloatClassification.Ignored, "IgnoredUnderExportedProgressOverride",
                    "Zero-lag FS has an unfinished predecessor and a successor started by the Data Date; ignored under the exported Progress Override setting.")
                : Unsupported("UnsupportedProgressOverrideCase", "This progressed relationship is not the verified zero-lag FS override case; it is not assumed to be ignored.");
        if (type is "PR_SS" or "PR_SF")
            return Unsupported("UnsupportedProgressedRelationship", "Progressed SS/SF remaining-event semantics are not verified, including expired/remaining lag.");
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
    try
    {
        decimal hours = RelationshipFreeFloatCalculator.CalculateHours(predecessorCalendar.Calculator, lagCalendar,
            from.Date.Value, to.Date.Value, lagHours, type is "PR_SS" or "PR_SF", cancellationToken);

        long movementTicks = checked((long)decimal.Round(hours * TimeSpan.TicksPerHour, 0));
        DateTime moved = predecessorCalendar.Calculator.AddWorkingTicks(from.Date.Value, movementTicks, cancellationToken);
        if (movementTicks != 0)
            moved = type is "PR_SS" or "PR_SF"
                ? predecessorCalendar.Calculator.AddWorkingTicks(moved, 1, cancellationToken).AddTicks(-1)
                : predecessorCalendar.Calculator.AddWorkingTicks(moved, -1, cancellationToken).AddTicks(1);
        string? suspension = SuspensionIssue(predecessor.Row, from.Date.Value, moved);
        if (suspension is not null)
            return Unsupported(suspension, "Predecessor movement may cross activity suspension, or its bounds are unresolved. No suspension work is invented.");
        decimal days = hours / predecessorCalendar.HoursPerDay.Value;
        return assessment with { Classification = RelationshipFloatClassification.Calculated,
            ReasonCode = activeSuccessor ? "CalculatedRetainedRemainingRelationship" : "CalculatedRemainingRelationship",
            FloatHours = hours, FloatDays = days,
            Message = "Signed predecessor-working-time allowance evaluated under exported settings with fixed successor endpoint; not a reschedule or native P6 driving flag." };
    }
    catch (Exception ex) when (ex is InvalidDataException or ArgumentOutOfRangeException or OverflowException)
    {
        return Unsupported("UnprojectableCalendarBoundary", $"Calendar boundary could not be verified: {ex.Message}");
    }
}

private static (DateTime? Date, string Field) RemainingEndpoint(RelationshipTask task, bool start)
{
    string field = start ? "restart_date" : "reend_date";
    if (task.Status == "TK_NOTSTART" && RawText(task.Row, field).Length == 0)
        field = start ? "early_start_date" : "early_end_date";
    return (RawDate(task.Row, field), field);
}

private static string? SuspensionIssue(DataRow task, DateTime original, DateTime moved)
{
    if (original == moved) return null;
    string suspendText = RawText(task, "suspend_date"), resumeText = RawText(task, "resume_date");
    if (suspendText.Length == 0 && resumeText.Length == 0) return null;
    var suspend = RawDate(task, "suspend_date");
    var resume = RawDate(task, "resume_date");
    if (!suspend.HasValue || (resumeText.Length > 0 && (!resume.HasValue || resume < suspend)))
        return "UnresolvedSuspensionBounds";
    DateTime first = original < moved ? original : moved, last = original > moved ? original : moved;
    return suspend < last && (!resume.HasValue || resume > first) ? "UnsupportedSuspensionMovement" : null;
}
```

```csharp
public readonly record struct P6WorkInterval(TimeSpan Start, TimeSpan End);

public sealed class WorkingDayCalculator
{
    private const int MaximumProjectionDays = 366_000;
    private readonly IReadOnlyList<IReadOnlyList<P6WorkInterval>> _week;
    private readonly IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> _exceptions;
    private readonly bool _hasWeeklyWork;

    public static WorkingDayCalculator Default { get; } = CreateDefault();

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
        IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> exceptions)
    {
        _week = week;
        _exceptions = exceptions;
        _hasWeeklyWork = week.Any(day => day.Count > 0);
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
        _exceptions.TryGetValue(date, out var intervals) ? intervals : _week[(int)date.DayOfWeek];

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

### What Changed
- Converted key mappings to type-safe tuple arrays.
- Preserved row order and source tokens via `CreateSimpleKeyedTable`.

#### Old Implementation
```csharp
        public XerTable Create07XerActvType() => CreateSimpleKeyedTable(TableNames.ActvType, EnhancedTableNames.XerActvType07,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId) });

        public XerTable Create08XerActvCode() => CreateSimpleKeyedTable(TableNames.ActvCode, EnhancedTableNames.XerActvCode08,
            new List<Tuple<string, string>> {
        Tuple.Create(FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),
        Tuple.Create(FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId)
            });

        public XerTable Create09XerTaskActv() => CreateSimpleKeyedTable(TableNames.TaskActv, EnhancedTableNames.XerTaskActv09,
            new List<Tuple<string, string>> {
        Tuple.Create(FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),
        Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId)
            });
```

#### New Implementation
```csharp
public XerTable? Create07XerActvType() => CreateSimpleKeyedTable(
    TableNames.ActvType,
    EnhancedTableNames.XerActvType07,
    new[] { (FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId) });

public XerTable? Create08XerActvCode() => CreateSimpleKeyedTable(
    TableNames.ActvCode,
    EnhancedTableNames.XerActvCode08,
    new[]
    {
        (FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),
        (FieldNames.ActvCodeTypeIdKey, FieldNames.ActvCodeTypeId)
    });

public XerTable? Create09XerTaskActv() => CreateSimpleKeyedTable(
    TableNames.TaskActv,
    EnhancedTableNames.XerTaskActv09,
    new[]
    {
        (FieldNames.ActvCodeIdKey, FieldNames.ActvCodeId),
        (FieldNames.TaskIdKey, FieldNames.TaskId)
    });
```

---

## 7. 10_XER_CALENDAR

### What Changed
- Converted key mappings to type-safe tuple arrays.
- Preserved row order and source tokens via `CreateSimpleKeyedTable`.

#### Old Implementation
```csharp
        public XerTable Create10XerCalendar() => CreateSimpleKeyedTable(TableNames.Calendar, EnhancedTableNames.XerCalendar10,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.ClndrIdKey, FieldNames.ClndrId) });
```

#### New Implementation
```csharp
public XerTable? Create10XerCalendar() => CreateSimpleKeyedTable(
    TableNames.Calendar,
    EnhancedTableNames.XerCalendar10,
    new[] { (FieldNames.ClndrIdKey, FieldNames.ClndrId) });
```

---

## 8. 11_XER_CALENDAR_DETAILED

### What Changed
- Recursively resolves base calendar inheritance (`ResolveCalendarDataInheritance` / `ResolveRaw`) up to depth 256.
- Replaced regex parser with recursive AST parser `P6CalendarParser`.
- Added `P6CalendarReportingNormalization` to split overnight shifts across midnight.

#### Old Implementation
```csharp
        public XerTable Create11XerCalendarDetailed()
        {
            var calendarTable = _dataStore.GetTable(TableNames.Calendar);
            if (!IsTableValid(calendarTable)) return null;

            try
            {
                var sourceIndexes = calendarTable.FieldIndexes;
                string[] detailedColumns = {
            FieldNames.ClndrId, FieldNames.CalendarName, FieldNames.CalendarType,
            FieldNames.Date, FieldNames.DayOfWeek, FieldNames.WorkingDay,
            FieldNames.WorkHours, FieldNames.ExceptionType, FieldNames.ClndrIdKey,
            FieldNames.MonthUpdate, FieldNames.DayOfWeekNum, FieldNames.WorkingDayInt
        };

                PERFORMANCE OPTIMIZATION: Pre-calculate indexes
                var finalIndexes = detailedColumns
                    .Select((name, index) => new { name, index })
                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);


                var resultTable = new XerTable(EnhancedTableNames.XerCalendarDetailed11);
                resultTable.SetHeaders(detailedColumns.Select(StringInternPool.Intern).ToArray());

                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
                var detailedRowsBag = new ConcurrentBag<DataRow>();

                Parallel.ForEach(calendarTable.Rows, parallelOptions, sourceRow =>
                {
                    var row = sourceRow.Fields;
                    string originalFilename = sourceRow.SourceFilename;
                    string clndrId = GetFieldValue(row, sourceIndexes, FieldNames.ClndrId);
                    string clndrData = GetFieldValue(row, sourceIndexes, FieldNames.CalendarData);
                    string dayHrCnt = GetFieldValue(row, sourceIndexes, FieldNames.DayHourCount);
                    string clndrIdKey = CreateKey(originalFilename, clndrId);

                    Parse the complex clndr_data field
                    var calendarEntries = ParseCalendarData(clndrData, dayHrCnt);

                    foreach (var entry in calendarEntries)
                    {
                        string[] detailedRow = new string[detailedColumns.Length];

                        Use optimized method
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.ClndrId, clndrId);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.CalendarName, GetFieldValue(row, sourceIndexes, FieldNames.CalendarName));
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.CalendarType, GetFieldValue(row, sourceIndexes, FieldNames.CalendarType));
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.Date, entry.Date);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.DayOfWeek, entry.DayOfWeek);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.WorkingDay, entry.WorkingDay);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.WorkHours, entry.WorkHours);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.ExceptionType, entry.ExceptionType);
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.ClndrIdKey, clndrIdKey);

                        Add MonthUpdate value
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.MonthUpdate, ParseMonthUpdateFromFilename(originalFilename));

                        Add calculated columns: day_of_week_num and working_day_int
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.DayOfWeekNum, GetDayOfWeekNumber(entry.DayOfWeek));
                        SetTransformedField(detailedRow, finalIndexes, FieldNames.WorkingDayInt, entry.WorkingDay == "Y" ? "1" : "0");

                        Intern strings
                        for (int k = 0; k < detailedRow.Length; k++)
                        {
                            detailedRow[k] = StringInternPool.Intern(detailedRow[k] ?? string.Empty);
                        }

                        detailedRowsBag.Add(new DataRow(detailedRow, originalFilename));
                    }
                });

                resultTable.AddRows(detailedRowsBag);
                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerCalendarDetailed11}: {ex.Message}");
                return null;
            }
        }

        private class CalendarEntry
        {
            public string Date { get; set; }
            public string DayOfWeek { get; set; }
            public string WorkingDay { get; set; }
            public string WorkHours { get; set; }
            public string ExceptionType { get; set; }
        }

        private List<CalendarEntry> ParseCalendarData(string clndrData, string defaultDayHours)
        {
            if (string.IsNullOrWhiteSpace(clndrData))
                return GetDefaultWorkWeek(defaultDayHours);

            try
            {
                Check for P6 structured data markers
                bool hasStructuredData = clndrData.Contains("CalendarData") || clndrData.Contains("DaysOfWeek") || clndrData.Contains("(0||");

                if (hasStructuredData)
                {
                    var result = ParseP6CalendarFormat(clndrData, defaultDayHours);
                    if (result.Count > 0) return result;
                }

                Fallback if structured data is present but parsing fails or yields no results
                return GetDefaultWorkWeek(defaultDayHours);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing calendar data: {ex.Message}");
                return GetDefaultWorkWeek(defaultDayHours);
            }
        }

        PERFORMANCE OPTIMIZATION: Refactored to use static compiled Regex patterns instead of dynamic Regex in a loop
        private List<CalendarEntry> ParseP6CalendarFormat(string clndrData, string defaultDayHours)
        {
            var entries = new List<CalendarEntry>();
            clndrData = CleanUnicodeData(clndrData);

            Parse DaysOfWeek section
            int daysStart = clndrData.IndexOf("DaysOfWeek");
            if (daysStart > -1)
            {
                int daysEnd = FindSectionEnd(clndrData, daysStart);
                if (daysEnd > daysStart)
                {
                    string daysSection = clndrData.Substring(daysStart, daysEnd - daysStart);

                    Use dictionaries to store results from Regex matches
                    var dayContents = new Dictionary<int, string>();
                    var emptyDays = new HashSet<int>();

                    Match all occurrences using the static compiled Regex (DayPatternRegex)
                    foreach (Match match in DayPatternRegex.Matches(daysSection))
                    {
                        if (int.TryParse(match.Groups[1].Value, out int dayNum) && dayNum >= 1 && dayNum <= 7)
                        {
                            dayContents[dayNum] = match.Groups[2].Value.Trim();
                        }
                    }

                    Match all occurrences using the static compiled Regex (EmptyDayPatternRegex)
                    foreach (Match match in EmptyDayPatternRegex.Matches(daysSection))
                    {
                        if (int.TryParse(match.Groups[1].Value, out int dayNum) && dayNum >= 1 && dayNum <= 7)
                        {
                            emptyDays.Add(dayNum);
                            Ensure it's removed from dayContents if somehow matched by both
                            dayContents.Remove(dayNum);
                        }
                    }

                    Process the results for all 7 days
                    for (int dayNum = 1; dayNum <= 7; dayNum++)
                    {
                        string dayName = GetDayName(dayNum);
                        decimal totalHours = 0;

                        if (dayContents.TryGetValue(dayNum, out string dayContent) && !string.IsNullOrWhiteSpace(dayContent))
                        {
                            Day has defined work hours
                            totalHours = ParseDayWorkHours(dayContent);
                        }
                        else if (emptyDays.Contains(dayNum))
                        {
                            Day is explicitly defined as non-working
                            totalHours = 0;
                        }
                        else
                        {
                            Default P6 logic (if not explicitly defined in DaysOfWeek section)
                            Sunday (1) and Saturday (7) default to non-work; others use default hours.
                            if (dayNum == 1 || dayNum == 7)
                            {
                                totalHours = 0;
                            }
                            else
                            {
                                if (decimal.TryParse(defaultDayHours, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal defHours))
                                {
                                    totalHours = defHours;
                                }
                                else
                                {
                                    totalHours = 8; // Standard fallback if defaultDayHours is invalid
                                }
                            }
                        }

                        entries.Add(new CalendarEntry
                        {
                            Date = "",
                            DayOfWeek = dayName,
                            WorkingDay = totalHours > 0 ? "Y" : "N",
                            WorkHours = FormatHours(totalHours),
                            ExceptionType = "Standard"
                        });
                    }
                }
            }

            If DaysOfWeek parsing failed to produce a full week, use the default week definition
            if (entries.Count == 0)
                entries.AddRange(GetDefaultWorkWeek(defaultDayHours));

            Parse Exceptions/Holidays (which overlay the standard week)
            ParseExceptions(clndrData, entries);

            return entries;
        }

        private void ParseExceptions(string clndrData, List<CalendarEntry> entries)
        {
            var exceptionSections = new[] { "Exceptions", "HolidayOrExceptions", "HolidayOrException" };

            foreach (var sectionName in exceptionSections)
            {
                int excStart = clndrData.IndexOf(sectionName);
                if (excStart > -1)
                {
                    int excEnd = FindSectionEnd(clndrData, excStart);
                    string excSection = excEnd > excStart ?
                        clndrData.Substring(excStart, excEnd - excStart) :
                        clndrData.Substring(excStart);

                    Use pre-compiled Regex patterns
                    ParseExceptionMatches(ExcPatternRegex.Matches(excSection), entries);
                    ParseExceptionMatches(ExcPatternRegex2.Matches(excSection), entries);
                }
            }
        }

        private void ParseExceptionMatches(MatchCollection matches, List<CalendarEntry> entries)
        {
            foreach (Match excMatch in matches)
            {
                if (excMatch.Success && excMatch.Groups.Count >= 3)
                {
                    Group 2 contains the OLE date serial
                    if (int.TryParse(excMatch.Groups[2].Value, out int dateSerial))
                    {
                        DateTime excDate = ConvertFromOleDate(dateSerial);
                        int dayNum = (int)excDate.DayOfWeek; // .NET: 0=Sun, 1=Mon... 6=Sat
                        if (dayNum == 0) dayNum = 7; // P6: 1=Sun... 7=Sat. We use 1=Mon... 7=Sun in our GetDayName/GetDayOfWeekNumber.
                                                     Let's stick to .NET's DayOfWeek and adjust GetDayName

                        string dayName = excDate.DayOfWeek.ToString(); // e.g., "Monday"
                        string p6DayName = GetDayName((int)excDate.DayOfWeek + 1); // Get P6 day name (1-7)

                        Find the standard entry for this day of the week to use as a template
                        var standardDayEntry = entries.FirstOrDefault(e => e.DayOfWeek == p6DayName && string.IsNullOrEmpty(e.Date));

                        if (standardDayEntry == null) continue; // Should not happen if standard week was parsed

                        Group 3 contains the work content (time slots)
                        string workContent = excMatch.Groups.Count > 3 ? excMatch.Groups[3].Value.Trim() : "()";

                        decimal hours;
                        string entryType;

                        if (string.IsNullOrEmpty(workContent) || workContent == "()")
                        {
                            This is explicitly a non-working day (Holiday)
                            hours = 0;
                            entryType = "Exception - Non-Working";
                        }
                        else
                        {
                            This is a working exception with its own hours
                            hours = ParseDayWorkHours(workContent);
                            entryType = "Exception - Working";
                        }

                        Add a NEW entry for this specific date, using the standard day as a base
                        but overriding with exception data.
                        entries.Add(new CalendarEntry
                        {
                            Date = excDate.ToString("yyyy-MM-dd"), // Specific date
                            DayOfWeek = p6DayName,                 // Day name (e.g., "Monday")
                            WorkingDay = hours > 0 ? "Y" : "N",    // Y/N based on exception hours
                            WorkHours = FormatHours(hours),        // Exception hours
                            ExceptionType = entryType              // "Exception - Working" or "Exception - Non-Working"
                        });
                    }
                }
            }
        }

        private string DetermineExceptionType(decimal hours)
        {
            if (hours > 0)
                return "Exception - Working";
            return "Holiday"; // Non-working exception
        }

        private int FindSectionEnd(string data, int sectionStart)
        {
            Defines markers for subsequent sections in clndr_data
            var nextSections = new[] { "VIEW", "Exceptions", "HolidayOrExceptions", "Resources", "DaysOfWeek" };
            int minEnd = data.Length;

            foreach (var section in nextSections)
            {
                Search for the next section marker after the current section start
                Ensure we don't find the same section marker again
                int pos = data.IndexOf(section, sectionStart + 5);
                if (pos > -1 && pos < minEnd && pos != sectionStart)
                    minEnd = pos;
            }
            return minEnd;
        }

        Cleans up potentially corrupted or Unicode characters in clndr_data
        private string CleanUnicodeData(string data)
        {
            if (string.IsNullOrEmpty(data)) return data;

            var cleaned = new StringBuilder(data.Length);
            foreach (char c in data)
            {
                Keep printable ASCII characters and common punctuation/symbols
                if ((c >= 32 && c <= 126) || char.IsPunctuation(c) || char.IsSymbol(c))
                {
                    cleaned.Append(c);
                }
                Replace control characters (like newlines, tabs) and others with spaces
                else
                {
                    cleaned.Append(' ');
                }
            }

            string result = cleaned.ToString();
            Use pre-compiled Regex to clean up spacing around delimiters
            result = CleanRegex1.Replace(result, "$1");
            result = CleanRegex2.Replace(result, "$1");
            Collapse multiple spaces (using non-compiled Regex here as it's run once per calendar)
            result = Regex.Replace(result, @"\s{2,}", " ");

            return result;
        }

        Parses work hours from time slot definitions within clndr_data
        This logic handles various P6 time slot formats (s=start, f=finish)
        private decimal ParseDayWorkHours(string content)
        {
            if (string.IsNullOrWhiteSpace(content)) return 0;

            decimal totalHours = 0;

            Pattern 1: (0||N (s|HH:MM|f|HH:MM) ()) - Most common format
            Regex is defined locally here as these patterns are less critical than the main parsing ones,
            and compiling them adds overhead if they aren't used frequently.
            string timeSlotPattern = @"\(0\|\|\d+\s*\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)\s*\(\s*\)\s*\)";
            var timeSlotRegex = new Regex(timeSlotPattern, RegexOptions.Singleline);
            var matches = timeSlotRegex.Matches(content);

            if (matches.Count > 0)
            {
                foreach (Match match in matches)
                {
                    totalHours += ExtractHoursFromMatch(match);
                }
                return totalHours;
            }

            Pattern 2: (s|HH:MM|f|HH:MM) - Alternative format
            string altPattern1 = @"\(([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})\)";
            var altRegex1 = new Regex(altPattern1, RegexOptions.Singleline);
            matches = altRegex1.Matches(content);

            if (matches.Count > 0)
            {
                foreach (Match match in matches)
                {
                    totalHours += ExtractHoursFromMatch(match);
                }
                return totalHours;
            }

            Pattern 3: s|HH:MM|f|HH:MM (Simplified format)
            string simplePattern = @"([sf])\|(\d{1,2}:\d{2})\|([sf])\|(\d{1,2}:\d{2})";
            var simpleRegex = new Regex(simplePattern, RegexOptions.Singleline);
            matches = simpleRegex.Matches(content);

            if (matches.Count > 0)
            {
                Handle potential duplicates in simplified format parsing
                var processedPairs = new HashSet<string>();
                foreach (Match match in matches)
                {
                    string timeKey = $"{match.Groups[2].Value}-{match.Groups[4].Value}";
                    if (processedPairs.Contains(timeKey)) continue;
                    processedPairs.Add(timeKey);

                    totalHours += ExtractHoursFromMatch(match);
                }
            }

            return totalHours;
        }

        private decimal ExtractHoursFromMatch(Match match)
        {
            if (match.Groups.Count >= 5)
            {
                if (TimeSpan.TryParse(match.Groups[2].Value, out TimeSpan time1) && TimeSpan.TryParse(match.Groups[4].Value, out TimeSpan time2))
                {
                    string type1 = match.Groups[1].Value;
                    string type2 = match.Groups[3].Value;
                    TimeSpan start, end;

                    Determine start and end times based on 's' (start) and 'f' (finish) markers
                    if (type1 == "s" && type2 == "f") { start = time1; end = time2; }
                    else if (type1 == "f" && type2 == "s") { start = time2; end = time1; }
                    else
                    {
                        Handle ambiguous cases (e.g., both 's' or both 'f') by ordering them temporally
                        start = time1 < time2 ? time1 : time2;
                        end = time1 < time2 ? time2 : time1;
                    }

                    return CalculateHoursBetween(start, end);
                }
            }
            return 0;
        }


        private decimal CalculateHoursBetween(TimeSpan start, TimeSpan end)
        {
            if (start == end)
            {
                FIX: P6 uses (s|00:00|f|00:00) to represent a full 24-hour workday.
                if (start == TimeSpan.Zero)
                {
                    return 24m;
                }

                Any other identical time (e.g., s|08:00|f|08:00) is 0 hours.
                return 0m;
            }

            if (end > start)
            {
                Standard day shift (e.g., 08:00 to 17:00)
                return (decimal)(end - start).TotalHours;
            }
            else
            {
                Overnight shift (e.g., 22:00 to 06:00)
                This also handles (s|08:00|f|00:00), which is 16 hours.
                return (decimal)(TimeSpan.FromHours(24) - start + end).TotalHours;
            }
        }

        Converts OLE Automation Date (used in P6 clndr_data) to DateTime
        private DateTime ConvertFromOleDate(int oleDate)
        {
            try
            {
                OLE Date base is December 30, 1899
                DateTime baseDate = new DateTime(1899, 12, 30);

                if (oleDate < 1) return new DateTime(1900, 1, 1); // Handle invalid dates

                Note: DateTime.FromOADate handles the conversion correctly, including the historical leap year intricacies.
                However, P6 XER files typically use integer OLE dates (days only).
                return baseDate.AddDays(oleDate);

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error converting OLE date {oleDate}: {ex.Message}");
                return new DateTime(2000, 1, 1); // Fallback date
            }
        }

        private string GetDayName(int dayNumber)
        {
            P6 convention: 1=Sunday, 2=Monday... 7=Saturday
            switch (dayNumber)
            {
                case 1: return "Sunday";
                case 2: return "Monday";
                case 3: return "Tuesday";
                case 4: return "Wednesday";
                case 5: return "Thursday";
                case 6: return "Friday";
                case 7: return "Saturday";
                default: return "Unknown";
            }
        }

        private static string GetDayOfWeekNumber(string dayName)
        {
            Converts day name to number (Monday=1, Sunday=7) for Power BI compatibility
            switch (dayName)
            {
                case "Monday": return "1";
                case "Tuesday": return "2";
                case "Wednesday": return "3";
                case "Thursday": return "4";
                case "Friday": return "5";
                case "Saturday": return "6";
                case "Sunday": return "7";
                default: return "";
            }
        }

        private string FormatHours(decimal hours)
        {
            if (hours == 0) return "0";
            if (hours == Math.Floor(hours)) return hours.ToString("0");
            return hours.ToString("0.##"); // Format with up to 2 decimal places if needed
        }

        Provides a default work week definition if parsing fails
        private List<CalendarEntry> GetDefaultWorkWeek(string defaultDayHours)
        {
            decimal defaultHours = 8; // Standard fallback
            if (!string.IsNullOrWhiteSpace(defaultDayHours))
            {
                if (decimal.TryParse(defaultDayHours, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal parsed))
                    defaultHours = Math.Max(0, parsed);
            }

            Heuristic to detect 24x7 calendars: if default hours are high (e.g., >= 12), assume weekends work too
            bool is24x7 = defaultHours >= 12;

            var days = new[]
            {
        ("Sunday", is24x7 ? "Y" : "N", is24x7 ? defaultHours : 0m),
        ("Monday", "Y", defaultHours),
        ("Tuesday", "Y", defaultHours),
        ("Wednesday", "Y", defaultHours),
        ("Thursday", "Y", defaultHours),
        ("Friday", "Y", defaultHours),
        ("Saturday", is24x7 ? "Y" : "N", is24x7 ? defaultHours : 0m)
    };

            return days.Select(d => new CalendarEntry
            {
                DayOfWeek = d.Item1,
                WorkingDay = d.Item2,
                WorkHours = FormatHours(d.Item3),
                ExceptionType = "Standard",
                Date = ""
            }).ToList();
        }
```

#### New Implementation
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
```

---

## 9. 12_XER_RSRC, 13_XER_TASKRSRC, 14_XER_UMEASURE

### What Changed
- Converted key mappings to type-safe tuple arrays.
- Preserved row order and source tokens via `CreateSimpleKeyedTable`.

#### Old Implementation
```csharp
        public XerTable Create12XerRsrc() => CreateSimpleKeyedTable(TableNames.Rsrc, EnhancedTableNames.XerRsrc12,
                new List<Tuple<string, string>> {
        Tuple.Create(FieldNames.RsrcIdKey, FieldNames.RsrcId),
        Tuple.Create(FieldNames.ClndrIdKey, FieldNames.ClndrId),
        Tuple.Create(FieldNames.UnitIdKey, FieldNames.UnitId)
                });

        public XerTable Create13XerTaskRsrc() => CreateSimpleKeyedTable(TableNames.TaskRsrc, EnhancedTableNames.XerTaskRsrc13,
            new List<Tuple<string, string>> {
        Tuple.Create(FieldNames.RsrcIdKey, FieldNames.RsrcId),
        Tuple.Create(FieldNames.TaskIdKey, FieldNames.TaskId)
            });

        public XerTable Create14XerUmeasure() => CreateSimpleKeyedTable(TableNames.Umeasure, EnhancedTableNames.XerUmeasure14,
            new List<Tuple<string, string>> { Tuple.Create(FieldNames.UnitIdKey, FieldNames.UnitId) });
```

#### New Implementation
```csharp
public XerTable? Create12XerRsrc() => CreateSimpleKeyedTable(
    TableNames.Rsrc,
    EnhancedTableNames.XerRsrc12,
    new[]
    {
        (FieldNames.RsrcIdKey, FieldNames.RsrcId),
        (FieldNames.ClndrIdKey, FieldNames.ClndrId),
        (FieldNames.UnitIdKey, FieldNames.UnitId)
    });

public XerTable? Create13XerTaskRsrc() => CreateSimpleKeyedTable(
    TableNames.TaskRsrc,
    EnhancedTableNames.XerTaskRsrc13,
    new[]
    {
        (FieldNames.RsrcIdKey, FieldNames.RsrcId),
        (FieldNames.TaskIdKey, FieldNames.TaskId)
    });

public XerTable? Create14XerUmeasure() => CreateSimpleKeyedTable(
    TableNames.Umeasure,
    EnhancedTableNames.XerUmeasure14,
    new[] { (FieldNames.UnitIdKey, FieldNames.UnitId) });
```

---

## 10. 15_XER_RESOURCE_DISTRIBUTION

### What Changed
- Added P6 21-point curves (1..20) and manual curve profile resolution via curve repository.
- Cumulative monthly reconciliation guarantees zero rounding residue drift against `totalQty`.
- Separate handling for point actuals and elapsed actuals.

#### Old Implementation
```csharp
        public XerTable Create15XerResourceDistribution()
        {
            var taskRsrcTable = _dataStore.GetTable(TableNames.TaskRsrc);
            var taskTable = _dataStore.GetTable(TableNames.Task);
            var rsrcTable = _dataStore.GetTable(TableNames.Rsrc);
            var projectTable = _dataStore.GetTable(TableNames.Project);
            var calendarTable = _dataStore.GetTable(TableNames.Calendar);
            var umeasureTable = _dataStore.GetTable(TableNames.Umeasure);

            if (!IsTableValid(taskRsrcTable) || !IsTableValid(taskTable) || !IsTableValid(projectTable)) return null;

            try
            {
                UPDATED: Column definitions with new hour-based fields
                string[] distColumns = {
            FieldNames.TaskIdKey, FieldNames.RsrcIdKey, FieldNames.ClndrIdKey, FieldNames.ProjIdKey,
            FieldNames.DistributionMonth, FieldNames.MonthStartDate, FieldNames.MonthEndDate,
            FieldNames.MonthlyQuantity, FieldNames.DistributionType,
            
            PRIMARY DIAGNOSTIC COLUMNS (Hour-Based)
            FieldNames.MonthWorkingHours,      // NEW: Actual working hours in month slice
            FieldNames.TotalWorkingHours,      // NEW: Total working hours in full period
            FieldNames.CalendarHoursPerDay,    // NEW: Calendar's standard hours per day
            
            DERIVED COLUMNS (For backward compatibility)
            FieldNames.MonthWorkingDays,       // Now calculated: hours / calendar_hpd
            FieldNames.TotalWorkingDays,       // Now calculated: hours / calendar_hpd
            FieldNames.MonthCalendarDays,
            FieldNames.TotalCalendarDays,

            FieldNames.Start, FieldNames.Finish,
            FieldNames.IsActual, FieldNames.StatusCode,
            FieldNames.Unit,

            Descriptive
            FieldNames.TaskCode,
            FieldNames.RsrcShortName, FieldNames.RsrcName, FieldNames.RsrcType,
            FieldNames.MonthUpdate
        };

                var finalIndexes = distColumns
                    .Select((name, index) => new { name, index })
                    .ToDictionary(item => item.name, item => item.index, StringComparer.OrdinalIgnoreCase);

                var resultTable = new XerTable(EnhancedTableNames.XerResourceDist15, taskRsrcTable.RowCount * 5);
                resultTable.SetHeaders(distColumns.Select(StringInternPool.Intern).ToArray());

                1. Build Lookups
                Console.WriteLine("Building lookups for Hour-Based Resource Distribution...");

                var taskLookup = BuildTaskLookupDictionary(taskTable);
                var projDataDates = BuildProjectDataDatesLookup(projectTable);

                var projNameLookup = new Dictionary<string, string>();
                var pIdx = projectTable.FieldIndexes;
                foreach (var row in projectTable.Rows)
                {
                    string id = GetFieldValue(row.Fields, pIdx, FieldNames.ProjectId);
                    string name = GetFieldValue(row.Fields, pIdx, "proj_short_name");
                    if (string.IsNullOrEmpty(name)) name = id;
                    if (!string.IsNullOrEmpty(id)) projNameLookup[CreateKey(row.SourceFilename, id)] = name;
                }

                var rsrcLookup = BuildResourceLookup(rsrcTable, umeasureTable);
                var calendars = BuildCalendarCalculators(null);
                var calendarHours = BuildCalendarHoursLookup(calendarTable);

                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = PerformanceConfig.MaxParallelTransformations };
                var resultBag = new ConcurrentBag<DataRow>();
                var trIdx = taskRsrcTable.FieldIndexes;

                Parallel.ForEach(taskRsrcTable.Rows, parallelOptions, sourceRow =>
                {
                    var row = sourceRow.Fields;
                    string filename = sourceRow.SourceFilename;

                    2. Parse Quantities using Native Columns
                    double.TryParse(GetFieldValue(row, trIdx, FieldNames.ActRegQty), NumberStyles.Any, CultureInfo.InvariantCulture, out double actReg);
                    double.TryParse(GetFieldValue(row, trIdx, FieldNames.ActOtQty), NumberStyles.Any, CultureInfo.InvariantCulture, out double actOt);
                    double actualQty = actReg + actOt;
                    double.TryParse(GetFieldValue(row, trIdx, FieldNames.RemainQty), NumberStyles.Any, CultureInfo.InvariantCulture, out double remainQty);

                    3. Get Keys
                    string taskId = GetFieldValue(row, trIdx, FieldNames.TaskId);
                    string rsrcId = GetFieldValue(row, trIdx, FieldNames.RsrcId);
                    string taskIdKey = CreateKey(filename, taskId);
                    string rsrcIdKey = CreateKey(filename, rsrcId);

                    4. Lookups (Mimic RELATED())
                    if (!taskLookup.TryGetValue(taskIdKey, out TaskData taskData)) taskData = new TaskData();

                    string clndrIdKey = taskData.ClndrIdKey;
                    string statusCode = taskData.StatusCode;
                    string projIdKey = taskData.ProjIdKey;
                    DateTime dataDate = projDataDates.TryGetValue(projIdKey, out var dd) ? dd : DateTime.MinValue;

                    string rsrcName = "", rsrcShort = "", rsrcType = "", unitName = "";
                    if (rsrcLookup.TryGetValue(rsrcIdKey, out var rInfo))
                    {
                        rsrcName = rInfo.Name;
                        rsrcShort = rInfo.ShortName;
                        rsrcType = rInfo.Type;
                        unitName = rInfo.Unit;
                    }

                    --- LOGIC BRANCH 1: ACTUALS ---
                    if (actualQty > 0 && (statusCode == "TK_Complete" || statusCode == "TK_Active"))
                    {
                        DateTime rsrcActStart = DateParser.TryParse(GetFieldValue(row, trIdx, FieldNames.ActStartDate)) ?? DateTime.MinValue;
                        DateTime rsrcActEnd = DateParser.TryParse(GetFieldValue(row, trIdx, FieldNames.ActEndDate)) ?? DateTime.MinValue;

                        DateTime finalEndDate = (statusCode == "TK_Complete") ? rsrcActEnd : dataDate;

                        if (rsrcActStart != DateTime.MinValue && finalEndDate != DateTime.MinValue && finalEndDate >= rsrcActStart)
                        {
                            GenerateDistributionRows(resultBag, filename, finalIndexes, taskIdKey, rsrcIdKey, clndrIdKey, projIdKey, rsrcActStart, finalEndDate, actualQty, true, statusCode, taskData.TaskType, taskData.TaskCode, row, trIdx, calendars, calendarHours, projNameLookup, rsrcName, rsrcShort, rsrcType, unitName);
                        }
                    }

                    --- LOGIC BRANCH 2: REMAINING ---
                    if (remainQty > 0 && (statusCode == "TK_NotStart" || statusCode == "TK_Active"))
                    {
                        DateTime restartDate = DateParser.TryParse(GetFieldValue(row, trIdx, FieldNames.RestartDate)) ?? DateTime.MinValue;
                        DateTime reendDate = DateParser.TryParse(GetFieldValue(row, trIdx, FieldNames.ReendDate)) ?? DateTime.MinValue;

                        if (restartDate != DateTime.MinValue && reendDate != DateTime.MinValue && reendDate >= restartDate)
                        {
                            GenerateDistributionRows(resultBag, filename, finalIndexes, taskIdKey, rsrcIdKey, clndrIdKey, projIdKey, restartDate, reendDate, remainQty, false, statusCode, taskData.TaskType, taskData.TaskCode, row, trIdx, calendars, calendarHours, projNameLookup, rsrcName, rsrcShort, rsrcType, unitName);
                        }
                    }
                });

                resultTable.AddRows(resultBag);
                Console.WriteLine($"Generated {resultTable.RowCount} hour-based resource distribution rows.");
                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating {EnhancedTableNames.XerResourceDist15}: {ex.Message}");
                return null;
            }
        }

        private Dictionary<string, (string ShortName, string Name, string Type, string Unit)> BuildResourceLookup(XerTable rsrcTable, XerTable umeasureTable)
        {
            var lookup = new Dictionary<string, (string, string, string, string)>();
            if (!IsTableValid(rsrcTable)) return lookup;

            var unitMap = new Dictionary<string, string>();
            if (IsTableValid(umeasureTable))
            {
                var uIdx = umeasureTable.FieldIndexes;
                foreach (var row in umeasureTable.Rows)
                {
                    string id = GetFieldValue(row.Fields, uIdx, FieldNames.UnitId);
                    string name = GetFieldValue(row.Fields, uIdx, FieldNames.UnitAbbr);
                    if (string.IsNullOrEmpty(name)) name = GetFieldValue(row.Fields, uIdx, FieldNames.UnitName);
                    if (!string.IsNullOrEmpty(id)) unitMap[CreateKey(row.SourceFilename, id)] = name;
                }
            }

            var rIdx = rsrcTable.FieldIndexes;
            foreach (var row in rsrcTable.Rows)
            {
                string id = GetFieldValue(row.Fields, rIdx, FieldNames.RsrcId);
                string shortName = GetFieldValue(row.Fields, rIdx, FieldNames.RsrcShortName);
                string rName = GetFieldValue(row.Fields, rIdx, FieldNames.RsrcName);
                string type = GetFieldValue(row.Fields, rIdx, FieldNames.RsrcType);
                string unitId = GetFieldValue(row.Fields, rIdx, FieldNames.UnitId);
                string unitName = "";
                if (!string.IsNullOrEmpty(unitId))
                {
                    string unitKey = CreateKey(row.SourceFilename, unitId);
                    unitMap.TryGetValue(unitKey, out unitName);
                }
                if (!string.IsNullOrEmpty(id))
                {
                    string key = CreateKey(row.SourceFilename, id);
                    lookup[key] = (shortName, rName, type, unitName);
                }
            }
            return lookup;
        }

        private void GenerateDistributionRows(
            ConcurrentBag<DataRow> bag,
            string filename,
            IReadOnlyDictionary<string, int> indexes,
            string taskIdKey,
            string rsrcIdKey,
            string clndrIdKey,
            string projIdKey,
            DateTime startDate,
            DateTime endDate,
            double totalQty,
            bool isActual,
            string statusCode,
            string taskType,
            string taskCode,
            string[] sourceRow,
            IReadOnlyDictionary<string, int> sourceIdx,
            Dictionary<string, WorkingDayCalculator> calendars,
            Dictionary<string, decimal> calendarHoursLookup,
            Dictionary<string, string> projNameLookup,
            string rsrcName,
            string rsrcShort,
            string rsrcType,
            string unitName)
        {
            Get the calendar calculator
            WorkingDayCalculator calculator = WorkingDayCalculator.Default;
            if (!string.IsNullOrEmpty(clndrIdKey) && calendars.TryGetValue(clndrIdKey, out var cal))
                calculator = cal;

            Get calendar hours per day for conversions
            decimal calendarHoursPerDay = 8m;
            if (!string.IsNullOrEmpty(clndrIdKey) && calendarHoursLookup.TryGetValue(clndrIdKey, out decimal hpd))
                calendarHoursPerDay = hpd;
            if (calendarHoursPerDay <= 0) calendarHoursPerDay = 8m; // Fallback

            decimal totalWorkingHours = calculator.CountWorkingHours(startDate, endDate);

            Fallback to calendar days if no working hours exist (all holidays)
            bool useWorkingHours = totalWorkingHours > 0;
            decimal totalCalendarDays = 0;
            if (!useWorkingHours)
            {
                totalCalendarDays = (decimal)(endDate.Date - startDate.Date).TotalDays + 1;
            }

            Iterate through months
            DateTime currentMonthStart = new DateTime(startDate.Year, startDate.Month, 1);
            DateTime finalMonthStart = new DateTime(endDate.Year, endDate.Month, 1);

            while (currentMonthStart <= finalMonthStart)
            {
                Get last moment of the month (includes full last day)
                DateTime currentMonthEnd = currentMonthStart.AddMonths(1).AddDays(-1).Date
                    .AddHours(23).AddMinutes(59).AddSeconds(59);

                Determine intersection with activity period
                DateTime periodStart = (startDate > currentMonthStart) ? startDate : currentMonthStart;
                DateTime periodEnd = (endDate < currentMonthEnd) ? endDate : currentMonthEnd;

                if (periodStart <= periodEnd)
                {
                    double distributedQty = 0;
                    decimal periodWorkingHours = 0;
                    decimal periodCalendarDays = 0;

                    if (useWorkingHours)
                    {
                        *** HOUR-BASED LOGIC: Count working hours in this month slice ***
                        This replaces the day-counting loop with hour-accurate calculation
                        periodWorkingHours = calculator.CountWorkingHours(periodStart, periodEnd);

                        Proportional distribution by hours (not days)
                        if (totalWorkingHours > 0)
                        {
                            distributedQty = totalQty * (double)(periodWorkingHours / totalWorkingHours);
                        }
                    }
                    else
                    {
                        Fallback: Calendar day distribution (when no working hours exist)
                        periodCalendarDays = (decimal)(periodEnd.Date - periodStart.Date).TotalDays + 1;
                        if (totalCalendarDays > 0)
                        {
                            distributedQty = totalQty * (double)(periodCalendarDays / totalCalendarDays);
                        }
                    }

                    Add Row if Qty > 0 OR it is Actuals (preserve data integrity)
                    if (distributedQty > 0 || isActual)
                    {
                        string[] newRow = new string[indexes.Count];

                        Keys
                        SetTransformedField(newRow, indexes, FieldNames.TaskIdKey, taskIdKey);
                        SetTransformedField(newRow, indexes, FieldNames.RsrcIdKey, rsrcIdKey);
                        SetTransformedField(newRow, indexes, FieldNames.ClndrIdKey, clndrIdKey);
                        SetTransformedField(newRow, indexes, FieldNames.ProjIdKey, projIdKey);

                        Distribution Data
                        SetTransformedField(newRow, indexes, FieldNames.DistributionMonth,
                            currentMonthStart.ToString("yyyy-MM-dd"));
                        SetTransformedField(newRow, indexes, FieldNames.MonthStartDate,
                            periodStart.ToString("yyyy-MM-dd HH:mm:ss"));  // Keep time precision
                        SetTransformedField(newRow, indexes, FieldNames.MonthEndDate,
                            periodEnd.ToString("yyyy-MM-dd HH:mm:ss"));    // Keep time precision
                        SetTransformedField(newRow, indexes, FieldNames.MonthlyQuantity,
                            distributedQty.ToString("F4", CultureInfo.InvariantCulture));
                        SetTransformedField(newRow, indexes, FieldNames.DistributionType,
                            useWorkingHours ? "Working Hours" : "Calendar Days");

                        *** PRIMARY DIAGNOSTIC FIELDS (Hour-Based) ***
                        SetTransformedField(newRow, indexes, FieldNames.MonthWorkingHours,
                            periodWorkingHours.ToString("F2", CultureInfo.InvariantCulture));
                        SetTransformedField(newRow, indexes, FieldNames.TotalWorkingHours,
                            totalWorkingHours.ToString("F2", CultureInfo.InvariantCulture));
                        SetTransformedField(newRow, indexes, FieldNames.CalendarHoursPerDay,
                            calendarHoursPerDay.ToString("F2", CultureInfo.InvariantCulture));

                        *** DERIVED FIELDS (Backward Compatibility) ***
                        Convert hours to days using calendar's standard hours per day
                        decimal periodWorkingDays = periodWorkingHours / calendarHoursPerDay;
                        decimal totalWorkingDays = totalWorkingHours / calendarHoursPerDay;

                        SetTransformedField(newRow, indexes, FieldNames.MonthWorkingDays,
                            periodWorkingDays.ToString("F2", CultureInfo.InvariantCulture));
                        SetTransformedField(newRow, indexes, FieldNames.TotalWorkingDays,
                            totalWorkingDays.ToString("F2", CultureInfo.InvariantCulture));

                        Calendar days (for reference)
                        if (periodCalendarDays == 0)
                            periodCalendarDays = (decimal)(periodEnd.Date - periodStart.Date).TotalDays + 1;

                        SetTransformedField(newRow, indexes, FieldNames.MonthCalendarDays,
                            periodCalendarDays.ToString("F0", CultureInfo.InvariantCulture));
                        SetTransformedField(newRow, indexes, FieldNames.TotalCalendarDays,
                            totalCalendarDays.ToString("F0", CultureInfo.InvariantCulture));

                        Metadata (keep time precision for better accuracy)
                        SetTransformedField(newRow, indexes, FieldNames.Start,
                            startDate.ToString("yyyy-MM-dd HH:mm:ss"));
                        SetTransformedField(newRow, indexes, FieldNames.Finish,
                            endDate.ToString("yyyy-MM-dd HH:mm:ss"));
                        SetTransformedField(newRow, indexes, FieldNames.IsActual, isActual ? "1" : "0");
                        SetTransformedField(newRow, indexes, FieldNames.StatusCode, statusCode);
                        SetTransformedField(newRow, indexes, FieldNames.TaskCode, taskCode);
                        SetTransformedField(newRow, indexes, FieldNames.RsrcShortName, rsrcShort);
                        SetTransformedField(newRow, indexes, FieldNames.RsrcName, rsrcName);
                        SetTransformedField(newRow, indexes, FieldNames.RsrcType, rsrcType);

                        Conditional Unit logic: If resource type is NOT material, use "unit/time"
                        string finalUnit = (rsrcType != "RT_Mat") ? "unit/time" : unitName;
                        SetTransformedField(newRow, indexes, FieldNames.Unit, finalUnit);

                        SetTransformedField(newRow, indexes, FieldNames.MonthUpdate,
                            ParseMonthUpdateFromFilename(filename));

                        String intern for memory efficiency
                        for (int k = 0; k < newRow.Length; k++)
                            newRow[k] = StringInternPool.Intern(newRow[k] ?? string.Empty);

                        bag.Add(new DataRow(newRow, filename));
                    }
                }

                currentMonthStart = currentMonthStart.AddMonths(1);
            }
        }
    }
```

#### New Implementation
```csharp
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
                        profile = ResolveRemainingProfile(start, finish, quantity.Value!.Value);
                    }
                    issueCode = isActual ? "ACTUAL_DISTRIBUTION_INVALID" : "REMAINING_DISTRIBUTION_INVALID";
                    // Commit only a completely reconciled portion. A recoverable source
                    // failure cannot leave early-month rows plus its full unallocated units.
                    var portionRows = new List<DataRow>();
                    AddResourceDistributionRows(portionRows, metadata, definition, calculator,
                        start, finish, quantity.Value!.Value, isActual, profile);
                    result.AddRows(portionRows);
                }
                catch (Exception ex) when (ex is InvalidDataException or OverflowException or ArgumentOutOfRangeException)
                {
                    AddIssue(issueCode, quantity, isActual, ex.Message);
                }
            }

            RemainingResourceProfile? ResolveRemainingProfile(DateTime start, DateTime finish, decimal quantity)
            {
                string manual = Read("remain_crv");
                if (!string.IsNullOrWhiteSpace(manual))
                    return RemainingResourceProfile.FromManual(manual, quantity, calendar.Calculator.CountWorkingHours(start, finish));
                string curveId = Read("curv_id").Trim();
                if (curveId.Length == 0) return null;
                if (curveId == "9")
                    throw new InvalidDataException("Manual curve '9' requires an exported remain_crv profile.");
                string durationType = TaskRead("duration_type").Trim().ToUpperInvariant();
                if (durationType is not ("DT_FIXEDDRTN" or "DT_FIXEDDUR2"))
                    throw new InvalidDataException($"Curve '{curveId}' requires Fixed Duration & Units/Time or Fixed Duration & Units; got duration_type '{durationType}'.");
                curves ??= new ResourceCurveRepository(_dataStore);
                var named = curves.Get(source, curveId);
                // Preserve the verified single-month and phase-independent exceptions;
                // unsupported multi-month curve tails become warnings, not uniform guesses.
                bool singleMonth = start.Year == finish.AddTicks(-1).Year && start.Month == finish.AddTicks(-1).Month;
                if (!named.IsUniform && (normalizedStatus != "TK_NOTSTART"
                    || !string.IsNullOrWhiteSpace(Read(FieldNames.ActStartDate))
                    || !string.IsNullOrWhiteSpace(Read(FieldNames.ActEndDate))) && !singleMonth)
                    throw new InvalidDataException($"Curve '{curveId}' on a progressed assignment requires an exported remain_crv profile; its remaining curve phase cannot be established from this XER.");
                return named;
            }

            void AddIssue(string code, DistributionQuantity quantity, bool isActual, string message)
            {
                string amount = quantity.Value.HasValue
                    ? decimal.Round(quantity.Value.Value, 4, MidpointRounding.ToEven).ToString("F4", CultureInfo.InvariantCulture) : "";
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
                    Read(FieldNames.ActRegQty), Read(FieldNames.ActOtQty), isActual ? amount : "", message,
                    isActual ? "Actual" : "Remaining", Read(FieldNames.RestartDate), Read(FieldNames.ReendDate),
                    Read(FieldNames.RemainQty), Read("curv_id"), Read("remain_crv"), isActual ? "" : amount,
                    TableNames.TaskRsrc, "", "", XerDataQuality.RawRowJson(assignments, assignment)
                ];
                issues.Add(assignment.WithFields(values));
            }
        }
        _resourceDataQualityRows = issues.AsReadOnly();
        return result;
    }

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
        decimal quantity, bool isActual, RemainingResourceProfile? profile = null)
    {
        if (finish < start || (!isActual && finish == start))
            throw new InvalidDataException("Positive quantity requires finish after start, except recorded actuals at a single instant.");
        decimal totalHours = finish == start ? 0 : calculator.CountWorkingHours(start, finish);
        long totalTicks = checked((long)decimal.Round(totalHours * TimeSpan.TicksPerHour, 0));
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
            long ticks = checked((long)decimal.Round(hours * TimeSpan.TicksPerHour, 0));
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

```csharp
using System.Globalization;

namespace XerToCsvConverter;

// A piecewise-constant allocation over working time, represented cumulatively.
// P6's pct_usage_1..20 are band quantities, not spline/control-point heights.
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

---

## Summary Checklist for Upgrade

| Table / Area | Key Change Summary |
| :--- | :--- |
| **0. Helpers** | Replace `ConcurrentBag` with `DataRow[]` and `Parallel.For`. Add `CreateEnhancedHeaders` to prepend `raw_` to duplicate source columns. Use `(SourceToken, Id)` lookup keys. |
| **01_XER_TASK** | Rename `Remaining Working Days` to `Remaining Duration`. Format floats to `F2`. Use nullable `decimal?` for `% Complete` with `[0m, 100m]` clamp. Keep `remain_drtn_hr_cnt` & `phys_complete_pct`. |
| **02_XER_PROJECT** | Map via `CreateSimpleKeyedTable` using `CreateEnhancedHeaders`. |
| **03_XER_PROJWBS** | Add $O(V)$ cycle detection. Check same-project matching (`proj_id`). Nullify invalid/cyclic `ParentWbsIdKey`. |
| **04_XER_BASELINE** | Return empty initialized `XerTable` (not `null`) on missing date match. |
| **06_XER_PREDECESSOR** | Replace float engine with `RelationshipFreeFloatCalculator` (inverse lag CPM solver). Support `TT_TASK`, `TT_RSRC`, `TT_MILE`, `TT_FINMILE`. Support 4 lag calendar modes and Retained Logic vs. Progress Override. |
| **07, 08, 09** | Map via `CreateSimpleKeyedTable` with `CreateEnhancedHeaders`. |
| **10_XER_CALENDAR** | Map via `CreateSimpleKeyedTable` with `CreateEnhancedHeaders`. |
| **11_XER_CALENDAR_DETAILED** | Resolve base calendar inheritance up to depth 256 (`ResolveRaw`). Use `P6CalendarParser` AST reader and `P6CalendarReportingNormalization` for midnight shifts. |
| **12, 13, 14** | Map via `CreateSimpleKeyedTable` with `CreateEnhancedHeaders`. |
| **15_XER_RESOURCE_DISTRIBUTION** | Integrate P6 curve repository (21-point curves 1..20). Use cumulative reconciliation for zero residual drift. |
