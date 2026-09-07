using System.Globalization;

namespace XerToCsvConverter.Core.Tests;

public sealed partial class RelationshipAssessmentTests
{
    public static IEnumerable<object[]> RetainedSfCases()
    {
        foreach (string calendar in new[] { "rcal_Predecessor", "rcal_Successor", "rcal_24Hour", "rcal_ProjDefault" })
        foreach (int lag in new[] { -8, 0, 8 })
        foreach (int finishDay in new[] { 5, 6, 9 })
            yield return [calendar, lag, finishDay];
    }

    [Theory]
    [MemberData(nameof(RetainedSfCases))]
    public void Retained_SF_from_unstarted_predecessor_uses_original_lag_and_remaining_finish(
        string calendar, int lag, int finishDay)
    {
        var fixture = ActiveFixture();
        fixture.Relationship["pred_type"] = "PR_SF";
        fixture.Relationship["lag_hr_cnt"] = lag.ToString(CultureInfo.InvariantCulture);
        fixture.Options["sched_calendar_on_relationship_lag"] = calendar;
        fixture.Successor["restart_date"] = "2026-01-05 08:00";
        fixture.Successor["reend_date"] = $"2026-01-{finishDay:00} 12:00";
        fixture.Calendars[1]["clndr_data"] = P6TestCalendars.WorkWeek("10");
        fixture.Calendars[1]["day_hr_cnt"] = "10";

        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRetainedStartToFinish");
        Assert.Equal(RelationshipAllowanceStatus.Finite, result.AllowanceStatus);
        Assert.Equal("TaskCalendarRemainingEndpoint", result.CalculationBasis);
        Assert.Equal("restart_date", result.PredecessorEndpointField);
        Assert.Equal("reend_date", result.SuccessorEndpointField);
        Assert.Equal(lag, result.EffectiveLagHours);

        var predecessor = OracleIntervals(OraclePattern.SplitWeekdays);
        var lagIntervals = OracleIntervals(calendar switch
        {
            "rcal_Predecessor" => OraclePattern.SplitWeekdays,
            "rcal_Successor" => OraclePattern.TenHourWeekdays,
            _ => OraclePattern.Continuous
        });
        AssertForwardAllowance(result, predecessor, lagIntervals, predecessorIsStart: true);
    }

    [Fact]
    public void Retained_SF_is_independent_of_the_successors_already_recorded_start()
    {
        var fixture = ActiveFixture();
        fixture.Relationship["pred_type"] = "PR_SF";
        fixture.Relationship["lag_hr_cnt"] = "4";
        RelationshipFloatAssessment first = Assess(fixture, "CalculatedRetainedStartToFinish");
        fixture.Successor["act_start_date"] = "2026-01-02 12:00";
        RelationshipFloatAssessment second = Assess(fixture, "CalculatedRetainedStartToFinish");
        AssertEquivalentCalculation(first, second);
        Assert.Equal(new DateTime(2026, 1, 6, 17, 0, 0), second.SuccessorEndpoint);
    }

    [Theory]
    [InlineData("", RelationshipAllowanceStatus.MissingData, "MissingRemainingEndpoint")]
    [InlineData("invalid", RelationshipAllowanceStatus.InvalidData, "InvalidRemainingEndpoint")]
    public void Retained_SF_never_substitutes_early_finish_for_an_active_successor(
        string remainingFinish, RelationshipAllowanceStatus status, string reason)
    {
        var fixture = ActiveFixture();
        fixture.Relationship["pred_type"] = "PR_SF";
        fixture.Successor["reend_date"] = remainingFinish;
        RelationshipFloatAssessment result = Assess(fixture, reason);
        Assert.Equal(status, result.AllowanceStatus);
        Assert.Equal("", result.FormattedDays);
    }

    [Fact]
    public void Retained_SS_does_not_reinterpret_a_fixed_successor_start_as_remaining_restart()
    {
        var fixture = ActiveFixture();
        fixture.Relationship["pred_type"] = "PR_SS";
        RelationshipFloatAssessment result = Assess(fixture, "UnsupportedProgressedRelationship");
        Assert.Equal(RelationshipAllowanceStatus.RequiresContext, result.AllowanceStatus);
        Assert.Null(result.FloatHours);
    }

    [Theory]
    [InlineData("RetainedLogic", "PR_SF")]
    [InlineData("ProgressOverride", "PR_SF")]
    [InlineData("ActualDates", "PR_SF")]
    [InlineData("Unresolved", "PR_SF")]
    [InlineData("RetainedLogic", "PR_SS")]
    [InlineData("ProgressOverride", "PR_SS")]
    public void Fixed_predecessor_start_is_classified_without_a_fabricated_lag_or_allowance(string mode, string type)
    {
        var fixture = ActiveFixture();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.SetMode(mode);
        fixture.Options["sched_lag_early_start_flag"] = "N";
        fixture.Relationship["pred_type"] = type;
        fixture.Relationship.Remove("lag_hr_cnt");
        RelationshipFloatAssessment result = Assess(fixture, "FixedActualPredecessorStart");
        Assert.Equal(RelationshipAllowanceStatus.FixedEvent, result.AllowanceStatus);
        Assert.Equal("FixedActualStart", result.CalculationBasis);
        Assert.Null(result.FloatHours);
        Assert.Null(result.FloatDays);
        Assert.Null(result.EffectiveLagHours);
    }

    [Theory]
    [InlineData("PR_FS")]
    [InlineData("PR_FF")]
    [InlineData("PR_SS")]
    [InlineData("PR_SF")]
    public void Every_active_predecessor_requires_a_valid_actual_start_even_with_an_unstarted_successor(string type)
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Relationship["pred_type"] = type;
        fixture.Predecessor["act_start_date"] = "";
        RelationshipFloatAssessment result = Assess(fixture, "UnresolvedActualStart");
        Assert.Equal(RelationshipAllowanceStatus.MissingData, result.AllowanceStatus);
        fixture.Predecessor["act_start_date"] = "not a date";
        result = Assess(fixture, "UnresolvedActualStart");
        Assert.Equal(RelationshipAllowanceStatus.InvalidData, result.AllowanceStatus);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Active_actual_start_after_remaining_work_is_invalid_for_either_endpoint(bool predecessor)
    {
        var fixture = new Schedule();
        var task = predecessor ? fixture.Predecessor : fixture.Successor;
        SetStatus(task, "TK_Active");
        task["act_start_date"] = "2026-01-07 08:00";
        fixture.Project["last_recalc_date"] = "2026-01-08 08:00";
        RelationshipFloatAssessment result = Assess(fixture, "ActualStartAfterRemainingEndpoint");
        Assert.Equal(RelationshipAllowanceStatus.InvalidData, result.AllowanceStatus);
        Assert.Null(result.FloatDays);
    }

    [Theory]
    [InlineData("", RelationshipAllowanceStatus.MissingData, "UnresolvedProjectDataDate")]
    [InlineData("not a date", RelationshipAllowanceStatus.InvalidData, "UnresolvedProjectDataDate")]
    [InlineData("2025-12-31 17:00", RelationshipAllowanceStatus.RequiresContext, "ActualStartAfterDataDate")]
    public void Active_predecessor_needs_its_own_data_date_before_calculation_or_fixed_event_classification(
        string dataDate, RelationshipAllowanceStatus status, string reason)
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Project["last_recalc_date"] = dataDate;
        foreach (string type in new[] { "PR_FS", "PR_SF" })
        {
            fixture.Relationship["pred_type"] = type;
            RelationshipFloatAssessment result = Assess(fixture, reason);
            Assert.Equal(status, result.AllowanceStatus);
        }
    }

    [Fact]
    public void Active_cross_project_predecessor_validates_its_own_data_date_before_requiring_run_context()
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Predecessor["proj_id"] = fixture.Relationship["pred_proj_id"] = "P2";
        fixture.ExtraProject = new(fixture.Project) { ["proj_id"] = "P2", ["last_recalc_date"] = "" };
        fixture.ExtraOptions = new(fixture.Options) { ["proj_id"] = "P2" };
        Assert.Equal(RelationshipAllowanceStatus.MissingData,
            Assess(fixture, "UnresolvedProjectDataDate").AllowanceStatus);
        fixture.ExtraProject["last_recalc_date"] = "2026-01-02 17:00";
        Assert.Equal(RelationshipAllowanceStatus.RequiresContext,
            Assess(fixture, "UnverifiedMultiProjectProgressContext").AllowanceStatus);
    }

    [Theory]
    [InlineData(false, false, RelationshipAllowanceStatus.Finite, "TaskCalendarRemainingEndpoint")]
    [InlineData(true, false, RelationshipAllowanceStatus.Estimated, "TaskCalendarResourceEstimate")]
    [InlineData(false, true, RelationshipAllowanceStatus.Finite, "TaskCalendarSuspensionAdjusted")]
    [InlineData(true, true, RelationshipAllowanceStatus.Estimated, "TaskCalendarResourceEstimateSuspensionAdjusted")]
    public void Numeric_status_and_basis_distinguish_resource_estimates_and_suspension_adjustment(
        bool resource, bool suspension, RelationshipAllowanceStatus expectedStatus, string expectedBasis)
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        if (resource) fixture.Successor["task_type"] = "TT_Rsrc";
        if (suspension)
        {
            // Even an interval outside the movement must identify the supplied clock.
            fixture.Predecessor["suspend_date"] = "2026-01-01 08:00";
            fixture.Predecessor["resume_date"] = "2026-01-02 08:00";
        }
        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRemainingRelationship");
        Assert.Equal(expectedStatus, result.AllowanceStatus);
        Assert.Equal(expectedBasis, result.CalculationBasis);
        Assert.Equal(RelationshipFloatClassification.Calculated, result.Classification);
        Assert.Equal("0", result.FormattedDays);
    }

    [Fact]
    public void Nonfinite_public_states_preserve_the_distinction_between_history_fixed_events_and_ignored_edges()
    {
        var historical = new Schedule();
        SetStatus(historical.Predecessor, "TK_Complete");
        var ignored = ActiveFixture();
        ignored.SetMode("ProgressOverride");
        var fixedEvent = new Schedule();
        SetStatus(fixedEvent.Predecessor, "TK_Active");
        fixedEvent.Relationship["pred_type"] = "PR_SF";
        var context = ActiveFixture();
        context.SetMode("ActualDates");
        foreach (var (fixture, reason, status, basis) in new[]
        {
            (historical, "HistoricalFixedPredecessor", RelationshipAllowanceStatus.Historical, "HistoricalActualEvent"),
            (ignored, "IgnoredUnderExportedProgressOverride", RelationshipAllowanceStatus.NoFiniteBound, "IgnoredRelationship"),
            (fixedEvent, "FixedActualPredecessorStart", RelationshipAllowanceStatus.FixedEvent, "FixedActualStart"),
            (context, "UnsupportedActualDatesProgressCase", RelationshipAllowanceStatus.RequiresContext, "UnresolvedContext")
        })
        {
            RelationshipFloatAssessment result = Assess(fixture, reason);
            Assert.Equal(status, result.AllowanceStatus);
            Assert.Equal(basis, result.CalculationBasis);
            Assert.Null(result.FloatHours);
            Assert.Null(result.FloatDays);
            Assert.Equal("", result.FormattedDays);
        }
    }

    public static IEnumerable<object[]> SuspensionOracleCases()
    {
        foreach (string calendar in new[] { "rcal_Predecessor", "rcal_Successor", "rcal_24Hour", "rcal_ProjDefault" })
        foreach (int lag in new[] { -8, 0, 8 })
        foreach (bool negative in new[] { false, true })
            yield return [calendar, lag, negative];
    }

    [Theory]
    [MemberData(nameof(SuspensionOracleCases))]
    public void Closed_suspension_changes_only_predecessor_movement_and_passes_an_independent_interval_oracle(
        string calendar, int lag, bool negative)
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Predecessor["restart_date"] = "2026-01-05 08:00";
        fixture.Predecessor["reend_date"] = negative ? "2026-01-08 17:00" : "2026-01-05 17:00";
        fixture.Successor["restart_date"] = negative ? "2026-01-05 17:00" : "2026-01-08 17:00";
        fixture.Successor["reend_date"] = "2026-01-09 17:00";
        fixture.Predecessor["suspend_date"] = "2026-01-06 10:00";
        fixture.Predecessor["resume_date"] = "2026-01-07 15:00";
        fixture.Relationship["lag_hr_cnt"] = lag.ToString(CultureInfo.InvariantCulture);
        fixture.Options["sched_calendar_on_relationship_lag"] = calendar;
        fixture.Calendars[1]["clndr_data"] = P6TestCalendars.WorkWeek("10");
        fixture.Calendars[1]["day_hr_cnt"] = "10";
        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRemainingRelationship");
        Assert.Equal("TaskCalendarSuspensionAdjusted", result.CalculationBasis);
        Assert.Equal(RelationshipAllowanceStatus.Finite, result.AllowanceStatus);
        Assert.Equal(Math.Sign(result.FloatHours!.Value), negative ? -1 : 1);
        var movement = OracleIntervals(OraclePattern.SplitWeekdays, new DateTime(2026, 1, 6), new DateTime(2026, 1, 7));
        var lagIntervals = OracleIntervals(calendar switch
        {
            "rcal_Predecessor" => OraclePattern.SplitWeekdays,
            "rcal_Successor" => OraclePattern.TenHourWeekdays,
            _ => OraclePattern.Continuous
        });
        AssertForwardAllowance(result, movement, lagIntervals, predecessorIsStart: false);
    }

    [Theory]
    [InlineData("2026-01-06 00:00", "2026-01-07 00:00", 0)]
    [InlineData("2026-01-07 00:00", "2026-01-08 00:00", 8)]
    [InlineData("2026-01-06 10:00", "2026-01-06 11:00", 8)]
    public void Suspension_excludes_whole_civil_days_and_treats_same_day_bounds_as_empty(
        string suspend, string resume, int expectedHours)
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Predecessor["suspend_date"] = suspend;
        fixture.Predecessor["resume_date"] = resume;
        fixture.Successor["restart_date"] = "2026-01-06 17:00";
        fixture.Successor["reend_date"] = "2026-01-07 17:00";
        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRemainingRelationship");
        Assert.Equal(expectedHours, result.FloatHours);
    }

    [Theory]
    [InlineData("2025-12-31 08:00", "2026-01-02 08:00", "InconsistentSuspensionState", RelationshipAllowanceStatus.InvalidData)]
    [InlineData("2026-01-06 10:00", "2026-01-05 10:00", "InvalidSuspensionBounds", RelationshipAllowanceStatus.InvalidData)]
    [InlineData("2026-01-06 10:00", "", "UnresolvedSuspensionBounds", RelationshipAllowanceStatus.RequiresContext)]
    [InlineData("", "2026-01-07 10:00", "UnresolvedSuspensionBounds", RelationshipAllowanceStatus.RequiresContext)]
    public void Suspension_invalid_state_and_missing_bounds_never_become_zero_allowance(
        string suspend, string resume, string reason, RelationshipAllowanceStatus status)
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Predecessor["suspend_date"] = suspend;
        fixture.Predecessor["resume_date"] = resume;
        RelationshipFloatAssessment result = Assess(fixture, reason);
        Assert.Equal(status, result.AllowanceStatus);
        Assert.Null(result.FloatHours);
        Assert.Null(result.FloatDays);
    }

    [Fact]
    public void Suspension_overlay_does_not_mutate_shared_calendars_between_assessments()
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Successor["clndr_id"] = "C1";
        fixture.Successor["restart_date"] = "2026-01-08 17:00";
        fixture.Successor["reend_date"] = "2026-01-09 17:00";
        RelationshipFloatAssessment original = Assess(fixture, "CalculatedRemainingRelationship");
        Assert.Equal(24m, original.FloatHours);
        fixture.Predecessor["suspend_date"] = "2026-01-06 08:00";
        fixture.Predecessor["resume_date"] = "2026-01-07 08:00";
        XerDataStore store = fixture.Store();
        var transformer = new XerTransformer(store);
        Assert.Equal(16m, Assert.Single(transformer.AssessRelationships()).FloatHours);
        var tasks = store.GetTable("TASK")!;
        tasks.Rows[0].Fields[tasks.FieldIndexes["suspend_date"]] = "";
        tasks.Rows[0].Fields[tasks.FieldIndexes["resume_date"]] = "";
        Assert.Equal(24m, Assert.Single(transformer.AssessRelationships()).FloatHours);
    }

    [Fact]
    public void Suspension_overlay_preserves_an_inherited_calendar_holiday()
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Predecessor["suspend_date"] = "2026-01-07 12:00";
        fixture.Predecessor["resume_date"] = "2026-01-08 12:00";
        fixture.Successor["restart_date"] = "2026-01-08 17:00";
        fixture.Successor["reend_date"] = "2026-01-09 17:00";
        // Inheritance supplies exceptions; the child still needs its own valid workweek.
        fixture.Calendars[0]["base_clndr_id"] = "C2";
        string holiday = new DateTime(2026, 1, 6).ToOADate().ToString("0", CultureInfo.InvariantCulture);
        string shifts = "(0||0(s|08:00|f|12:00)())(0||1(s|13:00|f|17:00)())";
        string days = string.Concat(Enumerable.Range(1, 7).Select(day =>
            $"(0||{day}()({(day is >= 2 and <= 6 ? shifts : "")}))"));
        string exceptions = $"(0||0(d|{holiday})())";
        fixture.Calendars[1]["clndr_data"] =
            $"(0||CalendarData()((0||DaysOfWeek()({days}))(0||Exceptions()({exceptions}))))";

        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRemainingRelationship");
        // Tuesday is the inherited holiday, Wednesday is suspended, Thursday works.
        Assert.Equal(8m, result.FloatHours);
        Assert.Equal("TaskCalendarSuspensionAdjusted", result.CalculationBasis);
        var intervals = OracleIntervals(OraclePattern.SplitWeekdays,
            new DateTime(2026, 1, 7), new DateTime(2026, 1, 8));
        intervals.RemoveAll(interval => interval.Start.Date == new DateTime(2026, 1, 6));
        AssertForwardAllowance(result, intervals, OracleIntervals(OraclePattern.Continuous), predecessorIsStart: false);
    }

    [Fact]
    public void Suspension_overlay_removes_both_civil_date_portions_of_normalized_overnight_work()
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Predecessor["reend_date"] = "2026-01-05 23:00";
        fixture.Predecessor["suspend_date"] = "2026-01-06 10:00";
        fixture.Predecessor["resume_date"] = "2026-01-07 10:00";
        fixture.Successor["restart_date"] = "2026-01-08 01:00";
        fixture.Successor["reend_date"] = "2026-01-08 17:00";
        fixture.Calendars[0]["day_hr_cnt"] = "4";
        string days = string.Concat(Enumerable.Range(1, 7).Select(day =>
            $"(0||{day}()((0||0(s|22:00|f|02:00)())))"));
        fixture.Calendars[0]["clndr_data"] = $"(0||CalendarData()((0||DaysOfWeek()({days}))))";

        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRemainingRelationship");
        // Mon 23-24, Wed 00-02 and 22-24, Thu 00-01: six hours.
        // The suspended Tuesday includes both its incoming and outgoing night portions.
        Assert.Equal(6m, result.FloatHours);
        Assert.Equal(1.5m, result.FloatDays);
        AssertForwardAllowance(result, OracleIntervals(OraclePattern.Night,
                new DateTime(2026, 1, 6), new DateTime(2026, 1, 7)),
            OracleIntervals(OraclePattern.Continuous), predecessorIsStart: false);
    }

    private enum OraclePattern { SplitWeekdays, TenHourWeekdays, Continuous, Night }

    private static List<(DateTime Start, DateTime End)> OracleIntervals(OraclePattern pattern,
        DateTime? suspend = null, DateTime? resume = null)
    {
        var intervals = new List<(DateTime, DateTime)>();
        for (DateTime day = new(2025, 12, 1); day < new DateTime(2026, 3, 1); day = day.AddDays(1))
        {
            if (suspend.HasValue && day >= suspend.Value && day < resume!.Value) continue;
            if (pattern == OraclePattern.Continuous) intervals.Add((day, day.AddDays(1)));
            else if (pattern == OraclePattern.Night)
            {
                intervals.Add((day, day.AddHours(2)));
                intervals.Add((day.AddHours(22), day.AddDays(1)));
            }
            else if (day.DayOfWeek is >= DayOfWeek.Monday and <= DayOfWeek.Friday)
            {
                if (pattern == OraclePattern.TenHourWeekdays) intervals.Add((day.AddHours(8), day.AddHours(18)));
                else
                {
                    intervals.Add((day.AddHours(8), day.AddHours(12)));
                    intervals.Add((day.AddHours(13), day.AddHours(17)));
                }
            }
        }
        return intervals;
    }

    private static void AssertForwardAllowance(RelationshipFloatAssessment result,
        List<(DateTime Start, DateTime End)> movement, List<(DateTime Start, DateTime End)> lag,
        bool predecessorIsStart)
    {
        long ticks = checked((long)decimal.Round(result.FloatHours!.Value * TimeSpan.TicksPerHour));
        long lagTicks = checked((long)decimal.Round(result.EffectiveLagHours!.Value * TimeSpan.TicksPerHour));
        DateTime from = result.PredecessorEndpoint!.Value, deadline = result.SuccessorEndpoint!.Value;
        DateTime Bound(long displacement)
        {
            DateTime moved = OracleAdvance(movement, from, displacement);
            if (displacement != 0)
                moved = predecessorIsStart
                    ? movement.Where(interval => interval.End > moved).Select(interval => interval.Start > moved ? interval.Start : moved).First()
                    : movement.Where(interval => interval.Start < moved).Select(interval => interval.End < moved ? interval.End : moved).Last();
            return OracleAdvance(lag, moved, lagTicks);
        }
        Assert.True(Bound(ticks) <= deadline, $"Allowance is infeasible: {result.FloatHours}, lag {result.EffectiveLagHours}.");
        Assert.True(Bound(ticks + 1) > deadline, $"Allowance is not maximal: {result.FloatHours}, lag {result.EffectiveLagHours}.");
    }

    // This forward oracle walks explicit intervals. It never calls production
    // calendar addition, working-time integration, overlay, or inverse methods.
    private static DateTime OracleAdvance(List<(DateTime Start, DateTime End)> intervals, DateTime anchor, long ticks)
    {
        if (ticks == 0) return anchor;
        long remaining = Math.Abs(ticks);
        if (ticks > 0)
        {
            foreach (var interval in intervals)
            {
                DateTime start = anchor > interval.Start ? anchor : interval.Start;
                if (start >= interval.End) continue;
                long width = (interval.End - start).Ticks;
                if (remaining <= width) return start.AddTicks(remaining);
                remaining -= width;
            }
        }
        else
        {
            for (int index = intervals.Count - 1; index >= 0; index--)
            {
                var interval = intervals[index];
                DateTime end = anchor < interval.End ? anchor : interval.End;
                if (end <= interval.Start) continue;
                long width = (end - interval.Start).Ticks;
                if (remaining <= width) return end.AddTicks(-remaining);
                remaining -= width;
            }
        }
        throw new InvalidOperationException("The independent test oracle exhausted its deliberately bounded calendar.");
    }
}
