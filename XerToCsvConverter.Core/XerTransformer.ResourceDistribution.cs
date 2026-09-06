using System.Globalization;

namespace XerToCsvConverter;

public partial class XerTransformer
{
    private IReadOnlyList<DataRow> _resourceDataQualityRows = Array.Empty<DataRow>();

    /// <summary>Collected table warnings plus the last table 15 portion diagnostics.</summary>
    public XerTable CreateDataQualityTable() => CreateDataQualityTable(null);

    internal XerTable CreateDataQualityTable(IEnumerable<string>? selectedTableNames)
    {
        HashSet<string>? selected = selectedTableNames?.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var table = new XerTable(XerDataQuality.TableName);
        table.SetHeaders(XerDataQuality.Columns.ToArray());
        table.AddRows(GeneralDataQualityRows()
            .Where(row => selected is null || selected.Contains(row.Fields[3]))
            .Select(row => row.WithFields(row.Fields.ToArray())));
        if (selected is null || selected.Contains(EnhancedTableNames.XerResourceDist15))
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
