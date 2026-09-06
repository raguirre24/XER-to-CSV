using System.Collections.ObjectModel;
using System.Globalization;

namespace XerToCsvConverter;

public partial class XerTransformer
{
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
        if (predecessor.Type is not ("TT_TASK" or "TT_MILE" or "TT_FINMILE") ||
            successor.Type is not ("TT_TASK" or "TT_MILE" or "TT_FINMILE"))
            return Unsupported("UnsupportedActivityType", "Resource-dependent, LOE, WBS-summary or unknown activity types do not establish task-calendar movement.");
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
            // Suspension is activity-specific, not an automatic change to CALENDAR or lag work.
            // Conservatively refuse movement that may cross an exported suspension boundary.
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
        // Only unstarted activities may use an early date when a preferred value is absent/blank.
        // Malformed nonblank values are authoritative invalid evidence, never repaired.
        if (task.Status == "TK_NOTSTART" && RawText(task.Row, field).Length == 0)
            field = start ? "early_start_date" : "early_end_date";
        return (RawDate(task.Row, field), field);
    }

    private static string? SuspensionIssue(DataRow task, DateTime original, DateTime moved)
    {
        if (original == moved) return null; // No movement interval consumes suspension time.
        string suspendText = RawText(task, "suspend_date"), resumeText = RawText(task, "resume_date");
        if (suspendText.Length == 0 && resumeText.Length == 0) return null;
        var suspend = RawDate(task, "suspend_date");
        var resume = RawDate(task, "resume_date");
        if (!suspend.HasValue || (resumeText.Length > 0 && (!resume.HasValue || resume < suspend)))
            return "UnresolvedSuspensionBounds";
        DateTime first = original < moved ? original : moved, last = original > moved ? original : moved;
        return suspend < last && (!resume.HasValue || resume > first) ? "UnsupportedSuspensionMovement" : null;
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
