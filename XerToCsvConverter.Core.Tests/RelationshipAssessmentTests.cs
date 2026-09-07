using System.Collections.Concurrent;
using System.Globalization;
using System.Text;

namespace XerToCsvConverter.Core.Tests;

public sealed class RelationshipAssessmentTests
{
    public static IEnumerable<object[]> ReleaseMatrix()
    {
        foreach (string type in new[] { "PR_FS", "PR_FF", "PR_SS", "PR_SF" })
        foreach (string predecessor in new[] { "TK_NotStart", "TK_Active", "TK_Complete" })
        foreach (string successor in new[] { "TK_NotStart", "TK_Active", "TK_Complete" })
        foreach (string mode in new[] { "RetainedLogic", "ProgressOverride", "ActualDates", "Unresolved" })
            yield return [type, predecessor, successor, mode];
    }

    [Theory]
    [MemberData(nameof(ReleaseMatrix))]
    public void Release_matrix_assesses_every_endpoint_state_type_and_mode_and_agrees_with_export(
        string type, string predecessor, string successor, string mode)
    {
        var fixture = new Schedule();
        fixture.Relationship["pred_type"] = type;
        SetStatus(fixture.Predecessor, predecessor);
        SetStatus(fixture.Successor, successor);
        fixture.SetMode(mode);
        string expected = successor == "TK_Complete" ? "HistoricalSuccessor"
            : predecessor == "TK_Complete" ? "HistoricalFixedPredecessor"
            : successor == "TK_Active" ? mode switch
            {
                "Unresolved" => "UnresolvedProgressMode",
                "ActualDates" => "UnsupportedActualDatesProgressCase",
                "ProgressOverride" => type == "PR_FS" ? "IgnoredUnderExportedProgressOverride" : "UnsupportedProgressOverrideCase",
                _ => type is "PR_SS" or "PR_SF" ? "UnsupportedProgressedRelationship" : "CalculatedRetainedRemainingRelationship"
            }
            : predecessor == "TK_Active" && type is "PR_SS" or "PR_SF" ? "UnsupportedProgressedStartEndpoint"
            : "CalculatedRemainingRelationship";

        RelationshipFloatAssessment result = Assess(fixture, expected);
        Assert.Equal(mode, result.SchedulingMode);
        if (expected.StartsWith("Calculated", StringComparison.Ordinal))
        {
            Assert.Equal(RelationshipFloatClassification.Calculated, result.Classification);
            Assert.NotNull(result.FloatHours);
            Assert.NotNull(result.FloatDays);
        }
        else
        {
            Assert.Null(result.FloatHours);
            Assert.Null(result.FloatDays);
            Assert.Equal("", result.FormattedDays);
        }
    }

    [Theory]
    [InlineData("PR_FS", true)]
    [InlineData("PR_FS", false)]
    [InlineData("PR_SS", true)]
    [InlineData("PR_SS", false)]
    [InlineData("PR_FF", true)]
    [InlineData("PR_FF", false)]
    [InlineData("PR_SF", true)]
    [InlineData("PR_SF", false)]
    public void Resource_dependent_endpoint_uses_the_same_unstarted_calculation_as_a_task(
        string type, bool resourceIsPredecessor)
    {
        var taskFixture = new Schedule();
        taskFixture.Relationship["pred_type"] = type;
        RelationshipFloatAssessment expected = Assess(taskFixture, "CalculatedRemainingRelationship");

        var resourceFixture = new Schedule();
        resourceFixture.Relationship["pred_type"] = type;
        (resourceIsPredecessor ? resourceFixture.Predecessor : resourceFixture.Successor)["task_type"] = "TT_Rsrc";
        RelationshipFloatAssessment actual = Assess(resourceFixture, "CalculatedRemainingRelationship");

        AssertEquivalentCalculation(expected, actual);
    }

    [Fact]
    public void Resource_dependent_relationship_uses_task_calendar_not_assigned_resource_calendar()
    {
        var taskFixture = new Schedule();
        taskFixture.Successor["restart_date"] = taskFixture.Successor["early_start_date"] = "2026-01-06 12:00";
        RelationshipFloatAssessment expected = Assess(taskFixture, "CalculatedRemainingRelationship");
        Assert.Equal(4m, expected.FloatHours);
        Assert.Equal(0.5m, expected.FloatDays);

        var resourceFixture = new Schedule();
        resourceFixture.Predecessor["task_type"] = "TT_Rsrc";
        resourceFixture.Successor["restart_date"] = resourceFixture.Successor["early_start_date"] = "2026-01-06 12:00";
        resourceFixture.Resource = new()
        {
            ["rsrc_id"] = "R1", ["rsrc_short_name"] = "LAB", ["rsrc_name"] = "Labour",
            ["rsrc_type"] = "RT_Labor", ["clndr_id"] = "C3"
        };
        resourceFixture.Assignment = new()
        {
            ["taskrsrc_id"] = "A1", ["task_id"] = "T1", ["proj_id"] = "P1", ["rsrc_id"] = "R1",
            ["restart_date"] = "2026-01-05 08:00", ["reend_date"] = "2026-01-05 17:00",
            ["remain_qty"] = "8"
        };

        var transformer = new XerTransformer(resourceFixture.Store());
        IReadOnlyList<RelationshipFloatAssessment> assessments = transformer.AssessRelationships();
        RelationshipFloatAssessment actual = Assert.Single(assessments);
        Assert.Equal("CalculatedRemainingRelationship", actual.ReasonCode);
        AssertEquivalentCalculation(expected, actual);
        Assert.Equal("2601.xer.C1", actual.PredecessorCalendarKey);

        XerTable output = Assert.IsType<XerTable>(transformer.Create06XerPredecessor(new()));
        DataRow row = Assert.Single(output.Rows);
        Assert.Equal("0.5", row.Fields[output.FieldIndexes["free_float"]]);
        Assert.Equal("2601.xer.C1", row.Fields[output.FieldIndexes["predecessor_clndr_id_key"]]);
    }

    [Theory]
    [InlineData("PR_FS", "CalculatedRemainingRelationship")]
    [InlineData("PR_FF", "CalculatedRemainingRelationship")]
    [InlineData("PR_SS", "UnsupportedProgressedStartEndpoint")]
    [InlineData("PR_SF", "UnsupportedProgressedStartEndpoint")]
    public void Active_resource_dependent_predecessor_follows_task_progress_policy(string type, string reason)
    {
        var taskFixture = new Schedule();
        taskFixture.Relationship["pred_type"] = type;
        SetStatus(taskFixture.Predecessor, "TK_Active");
        RelationshipFloatAssessment expected = Assess(taskFixture, reason);

        var resourceFixture = new Schedule();
        resourceFixture.Relationship["pred_type"] = type;
        resourceFixture.Predecessor["task_type"] = "TT_Rsrc";
        SetStatus(resourceFixture.Predecessor, "TK_Active");
        RelationshipFloatAssessment actual = Assess(resourceFixture, reason);

        AssertEquivalentCalculation(expected, actual);
    }

    [Theory]
    [InlineData("PR_FS", "CalculatedRetainedRemainingRelationship")]
    [InlineData("PR_FF", "CalculatedRetainedRemainingRelationship")]
    [InlineData("PR_SS", "UnsupportedProgressedRelationship")]
    [InlineData("PR_SF", "UnsupportedProgressedRelationship")]
    public void Active_resource_dependent_successor_follows_task_retained_logic_policy(string type, string reason)
    {
        var taskFixture = ActiveFixture();
        taskFixture.Relationship["pred_type"] = type;
        RelationshipFloatAssessment expected = Assess(taskFixture, reason);

        var resourceFixture = ActiveFixture();
        resourceFixture.Relationship["pred_type"] = type;
        resourceFixture.Successor["task_type"] = "TT_Rsrc";
        RelationshipFloatAssessment actual = Assess(resourceFixture, reason);

        AssertEquivalentCalculation(expected, actual);
    }

    [Theory]
    [InlineData("RetainedLogic", "CalculatedRetainedRemainingRelationship")]
    [InlineData("ProgressOverride", "IgnoredUnderExportedProgressOverride")]
    [InlineData("ActualDates", "UnsupportedActualDatesProgressCase")]
    [InlineData("Unresolved", "UnresolvedProgressMode")]
    public void Resource_dependent_endpoints_follow_task_progress_mode_policy(string mode, string reason)
    {
        var taskFixture = ActiveFixture();
        taskFixture.SetMode(mode);
        RelationshipFloatAssessment expected = Assess(taskFixture, reason);

        var resourceFixture = ActiveFixture();
        resourceFixture.Predecessor["task_type"] = "TT_Rsrc";
        resourceFixture.Successor["task_type"] = "TT_Rsrc";
        resourceFixture.SetMode(mode);
        RelationshipFloatAssessment actual = Assess(resourceFixture, reason);

        AssertEquivalentCalculation(expected, actual);
    }

    [Theory]
    [InlineData(true, "HistoricalFixedPredecessor")]
    [InlineData(false, "HistoricalSuccessor")]
    public void Completed_resource_dependent_endpoint_is_historical_like_a_task(
        bool resourceIsPredecessor, string reason)
    {
        var taskFixture = new Schedule();
        SetStatus(resourceIsPredecessor ? taskFixture.Predecessor : taskFixture.Successor, "TK_Complete");
        RelationshipFloatAssessment expected = Assess(taskFixture, reason);

        var resourceFixture = new Schedule();
        Dictionary<string, string> resource = resourceIsPredecessor
            ? resourceFixture.Predecessor : resourceFixture.Successor;
        resource["task_type"] = "TT_Rsrc";
        SetStatus(resource, "TK_Complete");
        RelationshipFloatAssessment actual = Assess(resourceFixture, reason);

        AssertEquivalentCalculation(expected, actual);
    }

    [Fact]
    public void July_relationship_8639741_is_zero_with_retained_remaining_dates_and_unchanged_actual_display()
    {
        var fixture = new Schedule { Filename = "2607-EBA_PAA_8.0.xer" };
        fixture.Project["proj_id"] = "3651";
        fixture.Project["last_recalc_date"] = "2026-07-25 17:00";
        fixture.Options["proj_id"] = "3651";
        fixture.Options["sched_use_expect_end_flag"] = "Y";
        fixture.Predecessor["task_id"] = "4806388";
        fixture.Successor["task_id"] = "4447632";
        foreach (var task in new[] { fixture.Predecessor, fixture.Successor })
        {
            task["proj_id"] = "3651";
            task["clndr_id"] = "2392";
            SetStatus(task, "TK_Active");
            task["act_start_date"] = "2026-07-20 08:00";
        }
        fixture.Predecessor["restart_date"] = fixture.Predecessor["early_start_date"] = "2026-07-27 08:00";
        fixture.Predecessor["reend_date"] = fixture.Predecessor["early_end_date"] = "2026-08-04 17:00";
        fixture.Predecessor["remain_drtn_hr_cnt"] = "48";
        fixture.Successor["restart_date"] = fixture.Successor["early_start_date"] = "2026-08-05 08:00";
        fixture.Successor["reend_date"] = fixture.Successor["early_end_date"] = "2026-08-20 17:00";
        fixture.Successor["remain_drtn_hr_cnt"] = "80";
        fixture.Relationship["task_pred_id"] = "8639741";
        fixture.Relationship["task_id"] = "4447632";
        fixture.Relationship["pred_task_id"] = "4806388";
        fixture.Relationship["proj_id"] = fixture.Relationship["pred_proj_id"] = "3651";
        fixture.Relationship["aref"] = "2026-08-04 17:00";
        fixture.Relationship["arls"] = "2026-08-05 08:00";
        fixture.Calendars[0]["clndr_id"] = "2392";

        var transformer = new XerTransformer(fixture.Store());
        RelationshipFloatAssessment result = Assert.Single(transformer.AssessRelationships());
        Assert.Equal("CalculatedRetainedRemainingRelationship", result.ReasonCode);
        Assert.Equal(0m, result.FloatDays);
        Assert.Equal(0m, result.FloatHours);
        Assert.Equal("reend_date", result.PredecessorEndpointField);
        Assert.Equal("restart_date", result.SuccessorEndpointField);
        Assert.Equal(new DateTime(2026, 8, 4, 17, 0, 0), result.PredecessorEndpoint);
        Assert.Equal(new DateTime(2026, 8, 5, 8, 0, 0), result.SuccessorEndpoint);
        Assert.Equal("2607-EBA_PAA_8.0.xer.4806388", result.PredecessorIdKey);
        XerTable output = Assert.IsType<XerTable>(transformer.Create06XerPredecessor(new()));
        DataRow row = Assert.Single(output.Rows);
        Assert.Equal("0", row.Fields[output.FieldIndexes["free_float"]]);
        Assert.Equal("2026-07-20 08:00:00", row.Fields[output.FieldIndexes["Start"]]);
        Assert.Equal("2026-07-20 08:00:00", row.Fields[output.FieldIndexes["predecessor_start"]]);
    }

    [Theory]
    [InlineData("Y", "N", "RetainedLogic", "CalculatedRetainedRemainingRelationship")]
    [InlineData("y", "n", "RetainedLogic", "CalculatedRetainedRemainingRelationship")]
    [InlineData("N", "Y", "ProgressOverride", "IgnoredUnderExportedProgressOverride")]
    [InlineData("N", "N", "ActualDates", "UnsupportedActualDatesProgressCase")]
    [InlineData("Y", "Y", "Unresolved", "UnresolvedProgressMode")]
    [InlineData(null, "N", "Unresolved", "UnresolvedProgressMode")]
    [InlineData("N", null, "Unresolved", "UnresolvedProgressMode")]
    [InlineData("", "N", "Unresolved", "UnresolvedProgressMode")]
    [InlineData("N", "", "Unresolved", "UnresolvedProgressMode")]
    [InlineData("perhaps", "N", "Unresolved", "UnresolvedProgressMode")]
    [InlineData("N", "1", "Unresolved", "UnresolvedProgressMode")]
    public void Only_explicit_valid_progress_flags_establish_a_mode(string? retained, string? progressOverride,
        string mode, string reason)
    {
        var fixture = ActiveFixture();
        SetOptional(fixture.Options, "sched_retained_logic", retained);
        SetOptional(fixture.Options, "sched_progress_override", progressOverride);
        RelationshipFloatAssessment result = Assess(fixture, reason);
        Assert.Equal(mode, result.SchedulingMode);
        Assert.Equal(retained is null ? "AbsentHeader" : retained == "" ? "Blank" : retained == "perhaps" ? "Malformed" : "Valid",
            result.InputEvidence["successor_options.sched_retained_logic"].State);
    }

    [Theory]
    [InlineData("last_recalc_date", null, "UnresolvedProjectDataDate")]
    [InlineData("last_recalc_date", "", "UnresolvedProjectDataDate")]
    [InlineData("last_recalc_date", "invalid", "UnresolvedProjectDataDate")]
    [InlineData("act_start_date", null, "UnresolvedActualStart")]
    [InlineData("act_start_date", "invalid", "UnresolvedActualStart")]
    [InlineData("act_start_date", "2026-01-03 08:00", "ActualStartAfterDataDate")]
    [InlineData("restart_date", null, "MissingRemainingEndpoint")]
    [InlineData("restart_date", "", "MissingRemainingEndpoint")]
    [InlineData("restart_date", "invalid", "InvalidRemainingEndpoint")]
    public void Retained_active_successor_requires_valid_explicit_progress_and_remaining_dates(
        string field, string? value, string reason)
    {
        var fixture = ActiveFixture();
        SetOptional(field == "last_recalc_date" ? fixture.Project : fixture.Successor, field, value);
        Assess(fixture, reason);
    }

    [Theory]
    [InlineData("act_start_date", "invalid", "UnresolvedActualStart")]
    [InlineData("act_start_date", "2026-01-03 08:00", "ActualStartAfterDataDate")]
    [InlineData("reend_date", "", "MissingRemainingEndpoint")]
    [InlineData("reend_date", "invalid", "InvalidRemainingEndpoint")]
    [InlineData("act_end_date", "2026-01-01 10:00", "InconsistentActivityState")]
    public void Retained_active_predecessor_does_not_repair_required_dates(string field, string value, string reason)
    {
        var fixture = ActiveFixture();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Predecessor[field] = value;
        Assess(fixture, reason);
    }

    [Theory]
    [InlineData("Y", "EarlyStart")]
    [InlineData("N", "ActualStart")]
    [InlineData("invalid", "Unresolved")]
    public void Progressed_SS_records_lag_basis_but_does_not_invent_expired_lag(string flag, string basis)
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Relationship["pred_type"] = "PR_SS";
        fixture.Relationship["lag_hr_cnt"] = "48";
        fixture.Options["sched_lag_early_start_flag"] = flag;
        RelationshipFloatAssessment result = Assess(fixture, "UnsupportedProgressedStartEndpoint");
        Assert.Equal(basis, result.SsLagBasis);
        Assert.Equal(48m, result.EffectiveLagHours);
        Assert.Equal(flag, result.InputEvidence["successor_options.sched_lag_early_start_flag"].RawValue);
    }

    [Fact]
    public void Actual_dates_marks_SS_lag_basis_inapplicable_without_moving_actual_events()
    {
        var fixture = ActiveFixture();
        fixture.SetMode("ActualDates");
        fixture.Relationship["pred_type"] = "PR_SS";
        fixture.Options["sched_lag_early_start_flag"] = "Y";
        Assert.Equal("NotApplicable", Assess(fixture, "UnsupportedActualDatesProgressCase").SsLagBasis);
    }

    [Theory]
    [InlineData("PR_FS", "8", "UnsupportedProgressOverrideCase")]
    [InlineData("PR_FS", "-8", "UnsupportedProgressOverrideCase")]
    [InlineData("PR_FF", "0", "UnsupportedProgressOverrideCase")]
    [InlineData("PR_SS", "0", "UnsupportedProgressOverrideCase")]
    [InlineData("PR_SF", "0", "UnsupportedProgressOverrideCase")]
    public void Progress_override_does_not_ignore_unverified_relationships(string type, string lag, string reason)
    {
        var fixture = ActiveFixture();
        fixture.SetMode("ProgressOverride");
        fixture.Relationship["pred_type"] = type;
        fixture.Relationship["lag_hr_cnt"] = lag;
        Assert.Equal(RelationshipFloatClassification.Unsupported, Assess(fixture, reason).Classification);
    }

    [Theory]
    [InlineData("rcal_Predecessor", "8", -8)]
    [InlineData("rcal_Predecessor", "-8", 8)]
    [InlineData("rcal_Successor", "8", 0)]
    [InlineData("rcal_Successor", "-8", 7)]
    [InlineData("rcal_24Hour", "8", 0)]
    [InlineData("rcal_24Hour", "-8", 7)]
    [InlineData("rcal_ProjDefault", "8", 0)]
    [InlineData("rcal_ProjDefault", "-8", 7)]
    public void Retained_relationship_preserves_signed_full_lag_on_the_selected_calendar(
        string setting, string lag, int expectedHours)
    {
        var fixture = ActiveFixture();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Options["sched_calendar_on_relationship_lag"] = setting;
        fixture.Relationship["lag_hr_cnt"] = lag;
        fixture.Calendars[0]["day_hr_cnt"] = "10";
        fixture.Calendars[1]["clndr_data"] = P6TestCalendars.WorkWeek("24");
        fixture.Calendars[1]["day_hr_cnt"] = "24";
        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRetainedRemainingRelationship");
        Assert.Equal(expectedHours, result.FloatHours);
        Assert.Equal(expectedHours / 10m, result.FloatDays);
        Assert.Equal(decimal.Parse(lag, CultureInfo.InvariantCulture), result.EffectiveLagHours);
    }

    [Theory]
    [InlineData("0", "unknown", "CalculatedRemainingRelationship")]
    [InlineData("8", "rcal_Predecessor", "CalculatedRemainingRelationship")]
    [InlineData("8", "rcal_24Hour", "CalculatedRemainingRelationship")]
    [InlineData("8", "rcal_Successor", "UnresolvedLagCalendar")]
    public void Missing_successor_calendar_only_blocks_operations_that_require_it(string lag, string setting, string reason)
    {
        var fixture = new Schedule();
        fixture.Successor["clndr_id"] = "missing";
        fixture.Relationship["lag_hr_cnt"] = lag;
        fixture.Options["sched_calendar_on_relationship_lag"] = setting;
        Assess(fixture, reason);
    }

    [Fact]
    public void Mode_independent_active_finish_uses_full_lag_even_when_progress_options_are_missing()
    {
        var fixture = new Schedule();
        SetStatus(fixture.Predecessor, "TK_Active");
        fixture.Options.Remove("sched_retained_logic");
        fixture.Options.Remove("sched_progress_override");
        fixture.Options.Remove("sched_lag_early_start_flag");
        fixture.Relationship["lag_hr_cnt"] = "8";
        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRemainingRelationship");
        Assert.Equal(-8m, result.FloatHours);
        Assert.Equal(8m, result.EffectiveLagHours);
    }

    [Fact]
    public void Explicitly_ignored_external_relationship_is_assessed_without_cross_source_matching()
    {
        var fixture = new Schedule();
        fixture.Relationship["pred_proj_id"] = "external-project";
        fixture.Options["sched_outer_depend_type"] = "SD_None";
        RelationshipFloatAssessment result = Assess(fixture, "IgnoredExternalRelationship");
        Assert.Equal(RelationshipFloatClassification.Ignored, result.Classification);
        Assert.Equal("", result.PredecessorIdKey);
    }

    [Theory]
    [InlineData(false, "ConflictingMultiProjectLagContext")]
    [InlineData(true, "UnverifiedMultiProjectProgressContext")]
    public void Conflicting_or_progressed_multi_project_context_is_not_inferred(bool progressed, string reason)
    {
        var fixture = new Schedule();
        fixture.Predecessor["proj_id"] = "P2";
        fixture.Relationship["pred_proj_id"] = "P2";
        fixture.Relationship["lag_hr_cnt"] = "8";
        if (progressed) SetStatus(fixture.Successor, "TK_Active");
        fixture.ExtraProject = new(fixture.Project) { ["proj_id"] = "P2" };
        fixture.ExtraOptions = new(fixture.Options) { ["proj_id"] = "P2", ["sched_calendar_on_relationship_lag"] = "rcal_24Hour" };
        Assess(fixture, reason);
    }

    [Theory]
    [InlineData("PROJECT")]
    [InlineData("SCHEDOPTIONS")]
    public void Duplicate_progress_context_does_not_select_the_first_row(string duplicateTable)
    {
        var fixture = ActiveFixture();
        XerDataStore store = fixture.Store();
        XerTable table = store.GetTable(duplicateTable)!;
        table.AddRow(table.Rows[0]);
        RelationshipFloatAssessment result = Assert.Single(new XerTransformer(store).AssessRelationships());
        Assert.Equal("UnresolvedProgressMode", result.ReasonCode);
        Assert.Equal("Ambiguous", result.InputEvidence[duplicateTable == "PROJECT"
            ? "successor_project.identity" : "successor_options.identity"].State);
    }

    [Theory]
    [InlineData("2026-01-01 08:00", "2026-01-02 08:00", "CalculatedRemainingRelationship")]
    [InlineData("2026-01-07 08:00", "2026-01-08 08:00", "CalculatedRemainingRelationship")]
    [InlineData("2026-01-06 10:00", "2026-01-06 11:00", "UnsupportedSuspensionMovement")]
    [InlineData("2026-01-06 10:00", "", "UnsupportedSuspensionMovement")]
    [InlineData("2026-01-06 10:00", "2026-01-06 09:00", "UnresolvedSuspensionBounds")]
    [InlineData("invalid", "2026-01-06 11:00", "UnresolvedSuspensionBounds")]
    [InlineData("", "2026-01-06 11:00", "UnresolvedSuspensionBounds")]
    public void Suspension_only_blocks_potentially_affected_or_unresolved_movement(string suspend, string resume, string reason)
    {
        var fixture = new Schedule();
        fixture.Successor["restart_date"] = "2026-01-06 17:00";
        fixture.Successor["reend_date"] = "2026-01-07 17:00";
        fixture.Predecessor["suspend_date"] = suspend;
        fixture.Predecessor["resume_date"] = resume;
        Assess(fixture, reason);
    }

    [Fact]
    public void Suspension_check_includes_the_start_boundary_snap_not_just_working_hour_addition()
    {
        var fixture = new Schedule();
        fixture.Relationship["pred_type"] = "PR_SS";
        // Eight working hours moves a start from Monday 08:00 to Tuesday 08:00,
        // not Monday 17:00. Suspension in that nonworking gap still crosses the
        // projected event movement and cannot be silently omitted from its bounds.
        fixture.Predecessor["suspend_date"] = "2026-01-05 20:00";
        fixture.Predecessor["resume_date"] = "2026-01-06 07:00";
        Assess(fixture, "UnsupportedSuspensionMovement");
    }

    [Theory]
    [InlineData("2026-01-05 08:00", "2026-01-05 17:00")]
    [InlineData("2026-01-06 17:00", "2026-01-07 08:00")]
    public void Suspension_boundary_touch_does_not_block_movement_outside_the_suspended_interval(
        string suspend, string resume)
    {
        var fixture = new Schedule();
        fixture.Successor["restart_date"] = "2026-01-06 17:00";
        fixture.Successor["reend_date"] = "2026-01-07 17:00";
        fixture.Predecessor["suspend_date"] = suspend;
        fixture.Predecessor["resume_date"] = resume;

        // The finish moves Jan 5 17:00 -> Jan 6 17:00. Work either starts exactly
        // at resumption, or ends exactly at suspension; neither crosses the pause.
        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRemainingRelationship");
        Assert.Equal(RelationshipFloatClassification.Calculated, result.Classification);
        Assert.Equal(8m, result.FloatHours);
        Assert.Equal(1m, result.FloatDays);
    }

    [Theory]
    [InlineData("2026-01-05 08:00", "2026-01-05 17:00")]
    [InlineData("2026-01-06 17:00", "2026-01-07 08:00")]
    public void Negative_movement_uses_the_same_half_open_suspension_boundaries(string suspend, string resume)
    {
        var fixture = new Schedule();
        fixture.Predecessor["restart_date"] = "2026-01-06 08:00";
        fixture.Predecessor["reend_date"] = "2026-01-06 17:00";
        fixture.Successor["restart_date"] = "2026-01-05 17:00";
        fixture.Successor["reend_date"] = "2026-01-06 17:00";
        fixture.Predecessor["suspend_date"] = suspend;
        fixture.Predecessor["resume_date"] = resume;

        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRemainingRelationship");
        Assert.Equal(-8m, result.FloatHours);
        Assert.Equal(-1m, result.FloatDays);
    }

    [Theory]
    [InlineData("2026-01-05 08:00", "")]
    [InlineData("2026-01-05 08:00", "2026-01-06 17:00")]
    [InlineData("invalid suspend", "invalid resume")]
    public void Zero_movement_does_not_consume_or_cross_suspension(string suspend, string resume)
    {
        var fixture = new Schedule();
        fixture.Predecessor["suspend_date"] = suspend;
        fixture.Predecessor["resume_date"] = resume;
        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRemainingRelationship");
        Assert.Equal(0m, result.FloatHours);
        Assert.Equal(0m, result.FloatDays);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void Present_invalid_or_duplicate_required_calendars_are_invalid_with_sanitized_causes(
        bool lagCalendar, bool duplicate)
    {
        var fixture = new Schedule();
        int index = lagCalendar ? 1 : 0;
        if (lagCalendar)
        {
            fixture.Relationship["lag_hr_cnt"] = "8";
            fixture.Options["sched_calendar_on_relationship_lag"] = "rcal_Successor";
        }
        if (!duplicate)
            fixture.Calendars[index]["clndr_data"] = fixture.Calendars[index]["clndr_data"]
                .Replace("08:00", "99:00", StringComparison.Ordinal);
        XerDataStore store = fixture.Store();
        XerTable calendars = store.GetTable("CALENDAR")!;
        string sourceToken = calendars.Rows[index].SourceToken;
        if (duplicate) calendars.AddRow(calendars.Rows[index]);
        var transformer = new XerTransformer(store);
        var assessments = transformer.AssessRelationships();
        RelationshipFloatAssessment result = Assert.Single(assessments);
        Assert.Equal(RelationshipFloatClassification.InvalidData, result.Classification);
        Assert.Equal(lagCalendar ? "InvalidLagCalendar" : "InvalidPredecessorCalendar", result.ReasonCode);
        Assert.Contains(duplicate ? "duplicate CALENDAR.clndr_id" : "Invalid calendar clock", result.Message, StringComparison.Ordinal);
        Assert.Contains(fixture.Filename, result.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(sourceToken, result.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("input occurrence", result.Message, StringComparison.Ordinal);
        Assert.Null(result.FloatHours);
        Assert.Null(result.FloatDays);
        AssertExportAgreement(transformer, assessments);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Absent_required_calendars_are_missing_not_invalid(bool lagCalendar)
    {
        var fixture = new Schedule();
        if (lagCalendar)
        {
            fixture.Relationship["lag_hr_cnt"] = "8";
            fixture.Options["sched_calendar_on_relationship_lag"] = "rcal_Successor";
            fixture.Successor["clndr_id"] = "AbsentCalendar";
        }
        else fixture.Predecessor["clndr_id"] = "AbsentCalendar";
        RelationshipFloatAssessment result = Assess(fixture, lagCalendar ? "UnresolvedLagCalendar" : "UnresolvedPredecessorCalendar");
        Assert.Equal(RelationshipFloatClassification.MissingData, result.Classification);
        Assert.Contains("absent", result.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Unknown_lag_calendar_token_is_invalid_and_the_evidence_is_malformed()
    {
        var fixture = new Schedule();
        fixture.Relationship["lag_hr_cnt"] = "8";
        fixture.Options["sched_calendar_on_relationship_lag"] = "rcal_Unknown";
        RelationshipFloatAssessment result = Assess(fixture, "UnresolvedLagCalendarSetting");
        Assert.Equal(RelationshipFloatClassification.InvalidData, result.Classification);
        RelationshipFieldEvidence evidence = result.InputEvidence["successor_options.sched_calendar_on_relationship_lag"];
        Assert.Equal("rcal_Unknown", evidence.RawValue);
        Assert.Equal("Malformed", evidence.State);
    }

    [Fact]
    public void Missing_lag_calendar_token_is_missing_and_not_malformed()
    {
        var fixture = new Schedule();
        fixture.Relationship["lag_hr_cnt"] = "8";
        fixture.Options.Remove("sched_calendar_on_relationship_lag");
        RelationshipFloatAssessment result = Assess(fixture, "UnresolvedLagCalendarSetting");
        Assert.Equal(RelationshipFloatClassification.MissingData, result.Classification);
        Assert.Equal("AbsentHeader", result.InputEvidence["successor_options.sched_calendar_on_relationship_lag"].State);
    }

    [Theory]
    [InlineData("not hours")]
    [InlineData("0")]
    [InlineData("-8")]
    [InlineData("NaN")]
    public void Malformed_or_nonpositive_predecessor_hpd_is_invalid_and_preserves_supplied_value(string hours)
    {
        var fixture = new Schedule();
        fixture.Calendars[0]["day_hr_cnt"] = hours;
        RelationshipFloatAssessment result = Assess(fixture, "UnresolvedPredecessorHoursPerDay");
        Assert.Equal(RelationshipFloatClassification.InvalidData, result.Classification);
        Assert.Contains("day_hr_cnt='" + hours + "'", result.Message, StringComparison.Ordinal);
        Assert.Null(result.FloatHours);
        Assert.Null(result.FloatDays);
    }

    [Fact]
    public void Blank_predecessor_hpd_remains_missing_not_invalid()
    {
        var fixture = new Schedule();
        fixture.Calendars[0]["day_hr_cnt"] = "";
        RelationshipFloatAssessment result = Assess(fixture, "UnresolvedPredecessorHoursPerDay");
        Assert.Equal(RelationshipFloatClassification.MissingData, result.Classification);
    }

    [Fact]
    public void Audit_boolean_settings_containing_date_are_valid_flags_not_malformed_timestamps()
    {
        var fixture = new Schedule();
        fixture.Options["level_keep_sched_date_flag"] = "Y";
        fixture.Options["sched_use_project_end_date_for_float"] = "N";
        fixture.Options["sched_use_expect_end_flag"] = "Y";
        fixture.Predecessor["expect_end_date"] = "not a date";
        RelationshipFloatAssessment result = Assess(fixture, "CalculatedRemainingRelationship");
        Assert.Equal("Valid", result.InputEvidence["successor_options.level_keep_sched_date_flag"].State);
        Assert.Equal("Valid", result.InputEvidence["successor_options.sched_use_project_end_date_for_float"].State);
        Assert.Equal("Valid", result.InputEvidence["successor_options.sched_use_expect_end_flag"].State);
        Assert.Equal("Malformed", result.InputEvidence["predecessor.expect_end_date"].State);
    }

    [Fact]
    public void Parsed_data_caller_date_edits_are_evaluated_and_original_evidence_remains_visible()
    {
        XerDataStore store = new Schedule().Store();
        XerTable tasks = store.GetTable("TASK")!;
        DataRow predecessor = tasks.Rows[0];
        predecessor.Fields[tasks.FieldIndexes["reend_date"]] = "2026-01-05 16:00";
        var transformer = new XerTransformer(store);
        var results = transformer.AssessRelationships();
        RelationshipFloatAssessment result = Assert.Single(results);
        Assert.Equal(1m, result.FloatHours);
        Assert.Equal(0.125m, result.FloatDays);
        Assert.Equal(new DateTime(2026, 1, 5, 16, 0, 0), result.PredecessorEndpoint);
        var evidence = result.InputEvidence["predecessor.reend_date"];
        Assert.Equal("2026-01-05 17:00", evidence.RawValue);
        Assert.Equal("2026-01-05 16:00", evidence.EvaluationValue);
        AssertExportAgreement(transformer, results);
    }

    [Fact]
    public void Parsed_data_caller_mode_edits_do_not_disguise_the_original_exported_settings()
    {
        XerDataStore store = ActiveFixture().Store();
        XerTable options = store.GetTable("SCHEDOPTIONS")!;
        options.Rows[0].Fields[options.FieldIndexes["sched_retained_logic"]] = "N";
        options.Rows[0].Fields[options.FieldIndexes["sched_progress_override"]] = "Y";
        var transformer = new XerTransformer(store);
        var results = transformer.AssessRelationships();
        RelationshipFloatAssessment result = Assert.Single(results);
        Assert.Equal("IgnoredUnderExportedProgressOverride", result.ReasonCode);
        Assert.Equal("ProgressOverride", result.SchedulingMode);
        Assert.Equal("Y", result.InputEvidence["successor_options.sched_retained_logic"].RawValue);
        Assert.Equal("N", result.InputEvidence["successor_options.sched_retained_logic"].EvaluationValue);
        Assert.Equal("N", result.InputEvidence["successor_options.sched_progress_override"].RawValue);
        Assert.Equal("Y", result.InputEvidence["successor_options.sched_progress_override"].EvaluationValue);
        AssertExportAgreement(transformer, results);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Real_ingestion_does_not_default_truncated_or_absent_lag_options_after_schema_union(bool disk, bool reverse)
    {
        var fixture = new Schedule();
        fixture.Relationship["lag_hr_cnt"] = "8";
        string prefix = fixture.Xer(includeOptions: false);
        string[] content =
        [
            prefix + "%T\tSCHEDOPTIONS\n%F\tproj_id\tsched_calendar_on_relationship_lag\n%R\tP1\n",
            prefix + "%T\tSCHEDOPTIONS\n%F\tproj_id\n%R\tP1\n",
            prefix + "%T\tSCHEDOPTIONS\n%F\tproj_id\tsched_calendar_on_relationship_lag\n%R\tP1\t\n"
        ];
        if (reverse) Array.Reverse(content);
        XerDataStore store = await ParseOrdered(content, disk);
        var transformer = new XerTransformer(store);
        var audit = transformer.AssessRelationships();
        Assert.Equal(3, audit.Count);
        Assert.Equal(3, audit.Select(result => result.SourceNamespace).Distinct().Count());
        string[] states = reverse ? ["Blank", "AbsentHeader", "OmittedCell"] : ["OmittedCell", "AbsentHeader", "Blank"];
        for (int i = 0; i < audit.Count; i++)
        {
            Assert.Equal(states[i], audit[i].InputEvidence["successor_options.sched_calendar_on_relationship_lag"].State);
            Assert.Equal(states[i] == "Blank" ? "CalculatedRemainingRelationship" : "UnresolvedLagCalendarSetting", audit[i].ReasonCode);
            Assert.Equal(1, audit[i].SourceRowNumber);
        }
        AssertExportAgreement(transformer, audit);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Real_truncated_progress_flags_do_not_acquire_a_mode_from_another_input(bool disk)
    {
        var fixture = ActiveFixture();
        string prefix = fixture.Xer(includeOptions: false);
        XerDataStore store = await ParseOrdered([
            prefix + "%T\tSCHEDOPTIONS\n%F\tproj_id\tsched_retained_logic\tsched_progress_override\n%R\tP1\tY\n",
            prefix + "%T\tSCHEDOPTIONS\n%F\tsched_progress_override\tproj_id\tsched_retained_logic\n%R\tN\tP1\tY\n"
        ], disk);
        var transformer = new XerTransformer(store);
        var results = transformer.AssessRelationships();
        Assert.Equal("UnresolvedProgressMode", results[0].ReasonCode);
        Assert.Equal("OmittedCell", results[0].InputEvidence["successor_options.sched_progress_override"].State);
        Assert.Equal("CalculatedRetainedRemainingRelationship", results[1].ReasonCode);
        AssertExportAgreement(transformer, results);
    }

    private static RelationshipFloatAssessment Assess(Schedule fixture, string reason)
    {
        var transformer = new XerTransformer(fixture.Store());
        var results = transformer.AssessRelationships();
        RelationshipFloatAssessment result = Assert.Single(results);
        Assert.Equal(reason, result.ReasonCode);
        Assert.Equal(1, result.SourceRowNumber);
        Assert.NotEmpty(result.Message);
        AssertExportAgreement(transformer, results);
        return result;
    }

    private static void AssertExportAgreement(XerTransformer transformer, IReadOnlyList<RelationshipFloatAssessment> assessments)
    {
        XerTable table = Assert.IsType<XerTable>(transformer.Create06XerPredecessor(new ConcurrentDictionary<string, XerTable>()));
        Assert.Equal(assessments.Count, table.RowCount);
        for (int i = 0; i < table.RowCount; i++)
            Assert.Equal(assessments[i].FormattedDays, table.Rows[i].Fields[table.FieldIndexes["free_float"]]);
    }

    private static void AssertEquivalentCalculation(
        RelationshipFloatAssessment expected, RelationshipFloatAssessment actual)
    {
        Assert.Equal(expected.Classification, actual.Classification);
        Assert.Equal(expected.ReasonCode, actual.ReasonCode);
        Assert.Equal(expected.FloatHours, actual.FloatHours);
        Assert.Equal(expected.FloatDays, actual.FloatDays);
        Assert.Equal(expected.FormattedDays, actual.FormattedDays);
        Assert.Equal(expected.PredecessorEndpoint, actual.PredecessorEndpoint);
        Assert.Equal(expected.PredecessorEndpointField, actual.PredecessorEndpointField);
        Assert.Equal(expected.SuccessorEndpoint, actual.SuccessorEndpoint);
        Assert.Equal(expected.SuccessorEndpointField, actual.SuccessorEndpointField);
        Assert.Equal(expected.EffectiveLagHours, actual.EffectiveLagHours);
        Assert.Equal(expected.PredecessorHoursPerDay, actual.PredecessorHoursPerDay);
        Assert.Equal(expected.LagCalendarKey, actual.LagCalendarKey);
    }

    private static Schedule ActiveFixture()
    {
        var fixture = new Schedule();
        SetStatus(fixture.Successor, "TK_Active");
        return fixture;
    }

    private static void SetStatus(Dictionary<string, string> task, string status)
    {
        task["status_code"] = status;
        task["act_start_date"] = status == "TK_NotStart" ? "" : "2026-01-01 08:00";
        task["act_end_date"] = status == "TK_Complete" ? "2026-01-01 17:00" : "";
    }

    private static void SetOptional(Dictionary<string, string> row, string field, string? value)
    {
        if (value is null) row.Remove(field); else row[field] = value;
    }

    private static async Task<XerDataStore> ParseOrdered(string[] contents, bool disk)
    {
        if (!disk)
        {
            MemoryStream[] streams = contents.Select(content => new MemoryStream(Encoding.UTF8.GetBytes(content))).ToArray();
            try
            {
                return await new ProcessingService().ParseXerStreamsAsync(
                    streams.Select(stream => ((Stream)stream, "same.xer")).ToArray(), null, CancellationToken.None);
            }
            finally { foreach (var stream in streams) stream.Dispose(); }
        }
        string directory = Path.Combine(Path.GetTempPath(), "xer-assessment-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var paths = new List<string>();
            for (int i = 0; i < contents.Length; i++)
            {
                string folder = Path.Combine(directory, i.ToString(CultureInfo.InvariantCulture));
                Directory.CreateDirectory(folder);
                string path = Path.Combine(folder, "same.xer");
                await File.WriteAllTextAsync(path, contents[i]);
                paths.Add(path);
            }
            return await new ProcessingService().ParseMultipleXerFilesAsync(paths, null, CancellationToken.None);
        }
        finally { Directory.Delete(directory, true); }
    }

    private sealed class Schedule
    {
        internal string Filename { get; set; } = "2601.xer";
        internal Dictionary<string, string> Project { get; } = new()
        { ["proj_id"] = "P1", ["clndr_id"] = "C3", ["last_recalc_date"] = "2026-01-02 17:00" };
        internal Dictionary<string, string> Options { get; } = new()
        {
            ["proj_id"] = "P1", ["sched_retained_logic"] = "Y", ["sched_progress_override"] = "N",
            ["sched_lag_early_start_flag"] = "Y", ["sched_calendar_on_relationship_lag"] = "rcal_Predecessor"
        };
        internal Dictionary<string, string> Predecessor { get; } = Task("T1", "C1", "2026-01-05");
        internal Dictionary<string, string> Successor { get; } = Task("T2", "C2", "2026-01-06");
        internal Dictionary<string, string> Relationship { get; } = new()
        {
            ["task_pred_id"] = "R1", ["task_id"] = "T2", ["pred_task_id"] = "T1", ["pred_type"] = "PR_FS",
            ["lag_hr_cnt"] = "0", ["proj_id"] = "P1", ["pred_proj_id"] = "P1"
        };
        internal Dictionary<string, string>[] Calendars { get; } = [Calendar("C1", "8"), Calendar("C2", "8"), Calendar("C3", "24")];
        internal Dictionary<string, string>? ExtraProject { get; set; }
        internal Dictionary<string, string>? ExtraOptions { get; set; }
        internal Dictionary<string, string>? Resource { get; set; }
        internal Dictionary<string, string>? Assignment { get; set; }

        internal void SetMode(string mode)
        {
            Options["sched_retained_logic"] = mode is "RetainedLogic" or "Unresolved" ? "Y" : "N";
            Options["sched_progress_override"] = mode is "ProgressOverride" or "Unresolved" ? "Y" : "N";
        }

        internal XerDataStore Store()
        {
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(Xer()));
            return new XerParser().ParseXerStream(stream, Filename, null, CancellationToken.None);
        }

        internal string Xer(bool includeOptions = true)
        {
            var text = new StringBuilder();
            Add("PROJECT", ExtraProject is null ? [Project] : [Project, ExtraProject]);
            Add("CALENDAR", Calendars);
            Add("TASK", [Predecessor, Successor]);
            Add("TASKPRED", [Relationship]);
            if (Resource is not null) Add("RSRC", [Resource]);
            if (Assignment is not null) Add("TASKRSRC", [Assignment]);
            if (includeOptions) Add("SCHEDOPTIONS", ExtraOptions is null ? [Options] : [Options, ExtraOptions]);
            return text.ToString();

            void Add(string name, Dictionary<string, string>[] rows)
            {
                string[] headers = rows.SelectMany(row => row.Keys).Distinct(StringComparer.Ordinal).ToArray();
                text.Append("%T\t").AppendLine(name).Append("%F\t").AppendLine(string.Join('\t', headers));
                foreach (var row in rows)
                    text.Append("%R\t").AppendLine(string.Join('\t', headers.Select(header => row.GetValueOrDefault(header, ""))));
            }
        }

        private static Dictionary<string, string> Task(string id, string calendar, string date) => new()
        {
            ["task_id"] = id, ["proj_id"] = "P1", ["clndr_id"] = calendar, ["status_code"] = "TK_NotStart",
            ["task_type"] = "TT_Task", ["task_code"] = id, ["restart_date"] = date + " 08:00",
            ["reend_date"] = date + " 17:00", ["early_start_date"] = date + " 08:00", ["early_end_date"] = date + " 17:00",
            ["act_start_date"] = "", ["act_end_date"] = "", ["remain_drtn_hr_cnt"] = "8", ["total_float_hr_cnt"] = "0"
        };

        private static Dictionary<string, string> Calendar(string id, string hours) => new()
        {
            ["clndr_id"] = id, ["clndr_name"] = id, ["clndr_type"] = "CA_Project", ["base_clndr_id"] = "",
            ["day_hr_cnt"] = hours, ["clndr_data"] = P6TestCalendars.WorkWeek(hours)
        };
    }
}
