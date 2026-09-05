using System.Globalization;

namespace XerToCsvConverter;

public partial class XerTransformer
{
    private IReadOnlyList<DataRow> _resourceDataQualityRows = Array.Empty<DataRow>();

    /// <summary>Issues from the last successful table 15 generation; never a partial failed result.</summary>
    public XerTable CreateDataQualityTable()
    {
        var table = new XerTable(XerDataQuality.TableName);
        table.SetHeaders(XerDataQuality.Columns.ToArray());
        table.AddRows(_resourceDataQualityRows.Select(row => row.WithFields(row.Fields.ToArray())));
        return table;
    }

    // The schema is unchanged. Remaining allocations can use exported profiles or
    // supported named curves. Actuals normally use working-time estimates; recorded
    // off-calendar/instant actuals retain their quantities without inventing work hours.
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
        try
        {
            var tasks = new DistributionInputIndex(_dataStore.GetTable(TableNames.Task), FieldNames.TaskId);
            var projects = new DistributionInputIndex(_dataStore.GetTable(TableNames.Project), FieldNames.ProjectId);
            var resources = new DistributionInputIndex(_dataStore.GetTable(TableNames.Rsrc), FieldNames.RsrcId);
            var units = new DistributionInputIndex(_dataStore.GetTable(TableNames.Umeasure), FieldNames.UnitId);
            // Resolve only calendars actually used by a positive allocation. Malformed
            // referenced calendars fail the export; unused calendars cannot change it.
            P6CalendarRepository? calendars = null;
            ResourceCurveRepository? curves = null;
            var calendarCache = new Dictionary<(string Source, string Id), (P6CalendarDefinition Definition, WorkingDayCalculator Calculator)>();
            var assignmentKeys = new HashSet<(string Source, string Id)>();
            var sourceRowNumbers = new Dictionary<string, int>(StringComparer.Ordinal);
            var issues = new List<DataRow>();
            var result = new XerTable(EnhancedTableNames.XerResourceDist15);
            result.SetHeaders(ResourceDistributionColumns.ToArray());

            foreach (DataRow assignment in assignments.Rows)
            {
                string source = assignment.SourceToken; // Stable input occurrence, independent of public filenames.
                sourceRowNumbers.TryGetValue(source, out int sourceRowNumber);
                sourceRowNumbers[source] = ++sourceRowNumber;
                string Read(string field) => GetFieldValue(assignment.Fields, assignments.FieldIndexes, field);
                string assignmentId = Read("taskrsrc_id").Trim();
                string context = $"Source '{assignment.OriginalSourceFilename}' (occurrence '{source}'), TASKRSRC '{assignmentId}' (task '{Read(FieldNames.TaskId)}', resource '{Read(FieldNames.RsrcId)}')";
                try
                {
                    if (assignmentId.Length > 0 && !assignmentKeys.Add((source, assignmentId)))
                        throw new InvalidDataException("Duplicate assignment identity.");
                    decimal actual = ReadQuantity(FieldNames.ActRegQty) + ReadQuantity(FieldNames.ActOtQty);
                    decimal remaining = ReadQuantity(FieldNames.RemainQty);
                    if (actual == 0 && remaining == 0) continue;

                    string taskId = Read(FieldNames.TaskId).Trim();
                    string resourceId = Read(FieldNames.RsrcId).Trim();
                    DataRow task = tasks.Require(source, taskId, Read(FieldNames.ProjectId));
                    string projectId = tasks.Read(task, FieldNames.ProjectId).Trim();
                    DataRow project = projects.Require(source, projectId);
                    DataRow resource = resources.Require(source, resourceId);
                    string status = tasks.Read(task, FieldNames.StatusCode).Trim();
                    string normalizedStatus = status.ToUpperInvariant();
                    string type = tasks.Read(task, FieldNames.TaskType).Trim().ToUpperInvariant();
                    if (normalizedStatus is not ("TK_NOTSTART" or "TK_ACTIVE" or "TK_COMPLETE"))
                        throw new InvalidDataException($"Unknown activity status '{status}'.");
                    if (type is not ("TT_TASK" or "TT_RSRC" or "TT_LOE" or "TT_WBS" or "TT_MILE" or "TT_FINMILE"))
                        throw new InvalidDataException($"Unknown activity type '{type}'.");
                    if (actual > 0 && normalizedStatus == "TK_NOTSTART")
                        throw new InvalidDataException("Actual quantity exists on an unstarted activity.");
                    if (remaining > 0 && normalizedStatus == "TK_COMPLETE")
                        throw new InvalidDataException("Remaining quantity exists on a completed activity.");

                    string calendarId = (type == "TT_RSRC"
                        ? resources.Read(resource, FieldNames.ClndrId)
                        : tasks.Read(task, FieldNames.ClndrId)).Trim();
                    if (!calendarCache.TryGetValue((source, calendarId), out var calendar))
                    {
                        calendars ??= new P6CalendarRepository(_dataStore, isolateInvalidIdentities: true);
                        var definition = calendars.Get(source, calendarId);
                        calendar = (definition, definition.CreateCalculator());
                        calendarCache.Add((source, calendarId), calendar);
                    }
                    string resourceType = resources.Read(resource, FieldNames.RsrcType);
                    string unit = "unit/time";
                    if (string.Equals(resourceType, "RT_Mat", StringComparison.OrdinalIgnoreCase))
                    {
                        string unitId = resources.Read(resource, FieldNames.UnitId).Trim();
                        unit = "";
                        // Unit labels are optional descriptive metadata, not calendar
                        // inputs: preserve a blank label when UMEASURE is unavailable.
                        DataRow? unitRow = units.Optional(source, unitId);
                        if (unitRow.HasValue)
                        {
                            unit = units.Read(unitRow.Value, FieldNames.UnitAbbr);
                            if (unit.Length == 0) unit = units.Read(unitRow.Value, FieldNames.UnitName);
                        }
                    }
                    var metadata = new DistributionMetadata(assignment, CreateKey(assignment.SourceFilename, taskId),
                        CreateKey(assignment.SourceFilename, resourceId), CreateKey(assignment.SourceFilename, calendarId), CreateKey(assignment.SourceFilename, projectId),
                        status, tasks.Read(task, FieldNames.TaskCode), resources.Read(resource, FieldNames.RsrcShortName),
                        resources.Read(resource, FieldNames.RsrcName), resourceType, unit);

                    if (actual > 0)
                    {
                        DateTime? start = null;
                        DateTime? finish = null;
                        string? issueMessage = null;
                        string issueCode = "ACTUAL_PERIOD_INVALID";
                        // Only source actual-period validation is recoverable. Calendar,
                        // identity, quantity, curve and calculation failures are not caught
                        // here. Keep the valid remaining portion independent of the actuals.
                        try
                        {
                            start = RequireDate(FieldNames.ActStartDate);
                            string rawFinish = Read(FieldNames.ActEndDate);
                            if (!string.IsNullOrWhiteSpace(rawFinish))
                                finish = ParseDistributionDate(rawFinish, FieldNames.ActEndDate);
                            else if (normalizedStatus == "TK_ACTIVE")
                                finish = ParseDistributionDate(projects.Read(project, FieldNames.LastRecalcDate),
                                    "PROJECT.last_recalc_date");
                            else
                                throw new InvalidDataException("Completed actual allocation requires TASKRSRC.act_end_date.");
                            if (finish < start)
                            {
                                issueCode = "ACTUAL_FINISH_BEFORE_START";
                                string finishField = string.IsNullOrWhiteSpace(rawFinish)
                                    ? "PROJECT.last_recalc_date" : "TASKRSRC.act_end_date";
                                throw new InvalidDataException($"Actual allocation finish {finishField} '{DateParser.Format(finish)}' precedes TASKRSRC.act_start_date '{DateParser.Format(start)}'. Actual quantity is unallocated; original dates are preserved.");
                            }
                        }
                        catch (InvalidDataException ex) { issueMessage = ex.Message; }

                        if (issueMessage is null)
                            AddResourceDistributionRows(result, metadata, calendar.Definition, calendar.Calculator,
                                start!.Value, finish!.Value, actual, isActual: true);
                        else
                        {
                            string[] values =
                            [
                                XerDataQuality.SchemaVersion, "Warning", issueCode,
                                EnhancedTableNames.XerResourceDist15, assignment.SourceFilename,
                                sourceRowNumber.ToString(CultureInfo.InvariantCulture), metadata.ProjectKey,
                                metadata.TaskKey, metadata.ResourceKey, CreateKey(assignment.SourceFilename, assignmentId),
                                assignmentId, metadata.TaskCode, metadata.ResourceName, metadata.ResourceType,
                                metadata.Unit, metadata.Status, Read(FieldNames.ActStartDate), Read(FieldNames.ActEndDate),
                                projects.Read(project, FieldNames.LastRecalcDate), Read(FieldNames.ActRegQty),
                                Read(FieldNames.ActOtQty), decimal.Round(actual, 4, MidpointRounding.ToEven)
                                    .ToString("F4", CultureInfo.InvariantCulture), issueMessage
                            ];
                            issues.Add(assignment.WithFields(values));
                        }
                    }
                    if (remaining > 0)
                    {
                        DateTime start = RequireDate(FieldNames.RestartDate);
                        DateTime finish = RequireDate(FieldNames.ReendDate);
                        RemainingResourceProfile? profile = ResolveRemainingProfile();
                        AddResourceDistributionRows(result, metadata, calendar.Definition, calendar.Calculator,
                            start, finish, remaining, isActual: false, profile);

                        RemainingResourceProfile? ResolveRemainingProfile()
                        {
                            try
                            {
                                // Explicit assignment period allocations take precedence
                                // over a named curve, including on progressed assignments.
                                string manual = Read("remain_crv");
                                if (!string.IsNullOrWhiteSpace(manual))
                                    return RemainingResourceProfile.FromManual(manual, remaining,
                                        calendar.Calculator.CountWorkingHours(start, finish));
                                string curveId = Read("curv_id").Trim();
                                if (curveId.Length == 0) return null;
                                if (curveId == "9")
                                    throw new InvalidDataException("Manual curve '9' requires an exported remain_crv profile.");
                                string durationType = tasks.Read(task, "duration_type").Trim().ToUpperInvariant();
                                if (durationType is not ("DT_FIXEDDRTN" or "DT_FIXEDDUR2"))
                                    throw new InvalidDataException($"Curve '{curveId}' requires Fixed Duration & Units/Time or Fixed Duration & Units; got duration_type '{durationType}'.");
                                curves ??= new ResourceCurveRepository(_dataStore);
                                var named = curves.Get(source, curveId);
                                // A linear curve is independent of its progress phase.
                                // A single monthly bucket also has an exact invariant:
                                // every remaining unit belongs to that month regardless
                                // of the unknown intramonth shape. The allocation loop
                                // uses the total directly and never evaluates that shape.
                                // Across months, do not guess P6's progressed tail from
                                // activity % complete or restart the full named curve.
                                bool singleMonth = finish > start
                                    && start.Year == finish.AddTicks(-1).Year
                                    && start.Month == finish.AddTicks(-1).Month;
                                if (!named.IsUniform && (normalizedStatus != "TK_NOTSTART"
                                    || !string.IsNullOrWhiteSpace(Read(FieldNames.ActStartDate))
                                    || !string.IsNullOrWhiteSpace(Read(FieldNames.ActEndDate))) && !singleMonth)
                                    throw new InvalidDataException($"Curve '{curveId}' on a progressed assignment requires an exported remain_crv profile; its remaining curve phase cannot be established from this XER.");
                                return named;
                            }
                            catch (Exception ex) when (ex is InvalidDataException or OverflowException or ArgumentOutOfRangeException)
                            {
                                // Preserve curve diagnostics through every surface rather
                                // than converting unsupported data to a partial/empty table.
                                throw new InvalidOperationException($"{EnhancedTableNames.XerResourceDist15}: {context}: {ex.Message}", ex);
                            }
                        }
                    }

                    decimal ReadQuantity(string field)
                    {
                        string raw = Read(field);
                        // Absent/blank optional actual/remaining quantities retain the
                        // existing zero convention. Nonblank invalid data is not zero.
                        if (string.IsNullOrWhiteSpace(raw)) return 0;
                        if (!decimal.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal value)
                            || value < 0)
                            throw new InvalidDataException($"{field} must be a finite nonnegative invariant number; got '{raw}'.");
                        return value;
                    }
                    DateTime RequireDate(string field) => ParseDistributionDate(Read(field), field);
                }
                catch (Exception ex) when (ex is InvalidDataException or ArgumentOutOfRangeException or OverflowException)
                {
                    throw new InvalidDataException($"{context}: {ex.Message}", ex);
                }
            }
            _resourceDataQualityRows = issues.AsReadOnly();
            return result;
        }
        catch (InvalidDataException ex)
        {
            // Existing export-service/profile validation rejects a failed requested 15
            // before writing CSVs. Never return the partially accumulated table.
            RecordGenerationFailure(EnhancedTableNames.XerResourceDist15, ex);
            Console.WriteLine($"Error creating {EnhancedTableNames.XerResourceDist15}: {ex.Message}");
            return null;
        }
    }

    private static DateTime ParseDistributionDate(string raw, string field) =>
        DateParser.TryParse(raw) ?? throw new InvalidDataException($"{field} requires a valid assignment-period date; got '{raw}'.");

    private sealed record DistributionMetadata(DataRow SourceRow, string TaskKey, string ResourceKey,
        string CalendarKey, string ProjectKey, string Status, string TaskCode, string ResourceShortName,
        string ResourceName, string ResourceType, string Unit);

    private void AddResourceDistributionRows(XerTable result, DistributionMetadata metadata,
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
                result.AddRow(metadata.SourceRow.WithFields(values.Select(value => StringInternPool.Intern(value)).ToArray()));
            }
            if (periodEnd == finish) break; // Also supports December 9999 without AddMonths overflow.
            month = periodEnd;
        }
        if (cumulativeTicks != totalTicks || allocated != target)
            throw new InvalidDataException("Monthly distribution did not reconcile to the assignment period and rounded quantity.");
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
