using System.Collections.Concurrent;
using System.Globalization;

namespace XerToCsvConverter.Core.Tests;

/// <summary>
/// Hand-calculated relationship examples. Every usable calendar has an actual P6
/// workweek blob, so these tests cannot pass by relying on a fabricated workweek.
/// </summary>
public sealed class RelationshipFreeFloatTests
{
    public static TheoryData<string, int> RelationshipTypesAndHours => new()
    {
        { "PR_FS", 8 }, { "PR_FS", 10 }, { "PR_FS", 24 },
        { "PR_SS", 8 }, { "PR_SS", 10 }, { "PR_SS", 24 },
        { "PR_FF", 8 }, { "PR_FF", 10 }, { "PR_FF", 24 },
        { "PR_SF", 8 }, { "PR_SF", 10 }, { "PR_SF", 24 }
    };

    [Theory]
    [MemberData(nameof(RelationshipTypesAndHours))]
    public void All_relationship_types_use_the_correct_endpoints_and_predecessor_days(
        string relationshipType, int predecessorHours)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", predecessorHours, 24);
        fixture.SetEndpoints("source-A", relationshipType, "2026-09-07 12:00", "2026-09-09 12:00");

        AssertFloat(fixture, 2m);
    }

    [Fact]
    public void Successor_work_during_a_predecessor_weekend_is_not_predecessor_free_float()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24, predecessorWeekdaysOnly: true);
        fixture.SetEndpoints("source-A", "PR_FS", "2026-09-04 16:00", "2026-09-07 08:00");

        // There are 64 successor work hours but no predecessor work hours.
        AssertFloat(fixture, 0m);
    }

    [Theory]
    [InlineData("rcal_Predecessor", "0")]
    [InlineData("rcal_Successor", "0.5")]
    [InlineData("rcal_24Hour", "0.5")]
    [InlineData("rcal_ProjDefault", "0.5")]
    [InlineData("rcal_Project", "0.5")]
    public void Scheduling_lag_calendar_is_separate_from_free_float_calendar(
        string lagCalendar, string expected)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 10, predecessorWeekdaysOnly: true);
        fixture.SetEndpoints("source-A", "PR_FS", "2026-09-04 12:00", "2026-09-07 12:00");
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "8");
        fixture.Set("SCHEDOPTIONS", "source-A", "P1", "sched_calendar_on_relationship_lag", lagCalendar);

        // With predecessor lag, moving Fri 12 later immediately pushes Mon 12 later.
        // With successor/elapsed lag, the predecessor can instead move to Fri 16:
        // its four Friday working hours fit before the latest permitted lag start.
        AssertFloat(fixture, decimal.Parse(expected, CultureInfo.InvariantCulture));
        Assert.Equal(1m, fixture.Number("lag")); // 8h / predecessor 8h/day, for every mode.
    }

    [Theory]
    [InlineData("2026-09-07 17:00", "2026-09-08 17:00", "0.125")]
    [InlineData("2026-09-07 09:00", "2026-09-08 09:00", "0.875")]
    [InlineData("2026-09-07 17:00", "2026-09-08 00:00", "-0.125")]
    public void Elapsed_lag_measures_predecessor_movement_not_the_gap_after_lag(
        string predecessorFinish, string successorStart, string expected)
    {
        var fixture = SplitShiftElapsedLagFixture();
        fixture.SetEndpoints("source-A", "PR_FS", predecessorFinish, successorStart);
        fixture.SetTaskDates("source-A", "T1", "2026-09-07 08:00", predecessorFinish);
        fixture.SetTaskDates("source-A", "T2", successorStart, "2026-09-09 17:00");

        // The latest permitted predecessor finishes are Tue 09, Tue 01 and Mon 16.
        // Count from the ORIGINAL predecessor finish, not from finish + eight hours:
        // respectively +1, +7 and -1 predecessor working hours.
        AssertFloat(fixture, decimal.Parse(expected, CultureInfo.InvariantCulture));
        Assert.Equal(1m, fixture.Number("lag")); // The existing display column is unchanged.
    }

    [Theory]
    [InlineData("PR_FS")]
    [InlineData("PR_SS")]
    [InlineData("PR_FF")]
    [InlineData("PR_SF")]
    public void Mixed_calendar_lag_uses_the_correct_endpoint_for_every_relationship_type(string type)
    {
        var fixture = SplitShiftElapsedLagFixture();
        bool predecessorStart = type is "PR_SS" or "PR_SF";
        bool successorStart = type is "PR_FS" or "PR_SS";
        fixture.Set("TASKPRED", "source-A", "R1", "pred_type", type);
        fixture.SetTaskDates("source-A", "T1",
            predecessorStart ? "2026-09-07 10:00" : "2026-09-07 08:00",
            predecessorStart ? "2026-09-07 17:00" : "2026-09-07 10:00");
        fixture.SetTaskDates("source-A", "T2",
            successorStart ? "2026-09-08 17:00" : "2026-09-08 08:00",
            successorStart ? "2026-09-09 01:00" : "2026-09-08 17:00");

        // Mon 10 moves by seven predecessor work hours to Tue 09; +8 elapsed is
        // Tue 17 exactly. One more minute reaches Tue 17:01 and violates the link.
        AssertFloat(fixture, 0.875m);
    }

    [Theory]
    [InlineData("rcal_Successor")]
    [InlineData("rcal_ProjDefault")]
    public void Negative_lag_includes_the_nonworking_plateau_in_predecessor_allowance(string lagCalendar)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 24, 8);
        fixture.SetEndpoints("source-A", "PR_FS", "2026-09-07 12:00", "2026-09-07 12:00");
        fixture.SetTaskDates("source-A", "T1", "2026-09-07 08:00", "2026-09-07 12:00");
        fixture.SetTaskDates("source-A", "T2", "2026-09-07 12:00", "2026-09-07 16:00");
        fixture.Set("PROJECT", "source-A", "P1", "clndr_id", "C2");
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "-4");
        fixture.Set("SCHEDOPTIONS", "source-A", "P1", "sched_calendar_on_relationship_lag", lagCalendar);

        // Tue 08 minus four 08-16 lag-calendar work hours is Mon 12. The entire
        // Mon 16 -> Tue 08 nonworking lag-calendar plateau remains permissible.
        // Tue 08:01 minus four hours is Mon 12:01, so 20/24 days is the boundary.
        AssertFloat(fixture, 20m / 24m);
    }

    [Theory]
    [InlineData("0", "1")]
    [InlineData("4", "0.5")]
    [InlineData("-4", "1.5")]
    public void Positive_zero_and_negative_lag_preserve_work_time_signs(string lagHours, string expected)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24, predecessorWeekdaysOnly: true);
        fixture.SetEndpoints("source-A", "PR_FS", "2026-09-07 12:00", "2026-09-08 12:00");
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", lagHours);

        AssertFloat(fixture, decimal.Parse(expected, CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData("PR_FS")]
    [InlineData("PR_SS")]
    [InlineData("PR_FF")]
    [InlineData("PR_SF")]
    public void Violated_relationship_keeps_negative_free_float(string relationshipType)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.SetEndpoints("source-A", relationshipType, "2026-09-08 12:00", "2026-09-07 12:00");

        AssertFloat(fixture, -1m);
    }

    [Theory]
    [InlineData("2026-09-07 08:00", "2026-09-07 08:01", 1)]
    [InlineData("2026-09-07 08:01", "2026-09-07 08:00", -1)]
    public void One_minute_signed_gap_is_not_rounded_to_a_driving_zero(string from, string to, int sign)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.SetEndpoints("source-A", "PR_FS", from, to);

        decimal actual = fixture.Number("free_float");
        Assert.Equal(sign / 480m, actual); // One minute / (60 minutes/hour * 8 hours/day).
        Assert.NotEqual(0m, actual);
    }

    [Fact]
    public void Split_shift_lunch_break_is_excluded()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("CALENDAR", "source-A", "C1", "clndr_data",
            CalendarBlob("08:00", "12:00", secondStart: "13:00", secondFinish: "17:00"));
        fixture.SetEndpoints("source-A", "PR_FS", "2026-09-07 11:00", "2026-09-07 14:00");

        AssertFloat(fixture, 0.25m); // 11-12 and 13-14, not three elapsed hours.
    }

    [Fact]
    public void Base_calendar_holiday_is_inherited_by_relationship_arithmetic()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.AddCalendar("source-A", "BASE", 8,
            CalendarBlob("08:00", "16:00", holiday: new DateTime(2026, 9, 8)));
        fixture.Set("CALENDAR", "source-A", "C1", "base_clndr_id", "BASE");
        fixture.SetEndpoints("source-A", "PR_FS", "2026-09-07 08:00", "2026-09-09 08:00");

        AssertFloat(fixture, 1m); // Monday works; inherited Tuesday holiday does not.
    }

    [Fact]
    public void Source_tokens_keep_repeated_task_calendar_project_and_relationship_ids_distinct()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.AddSource("source-B", 10, 24);
        fixture.SetEndpoints("source-A", "PR_FS", "2026-09-07 08:00", "2026-09-07 12:00");
        fixture.SetEndpoints("source-B", "PR_FS", "2026-09-07 08:00", "2026-09-07 12:00");

        XerTable output = fixture.Transform();
        Assert.Equal(2, output.RowCount);
        Assert.Equal(0.5m, Fixture.Number(output, "source-A", "free_float"));
        Assert.Equal(0.4m, Fixture.Number(output, "source-B", "free_float"));
    }

    [Fact]
    public void Source_tokens_that_differ_only_by_case_keep_their_own_calendars_and_day_units()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.AddSource("source-a", 10, 24);
        fixture.SetEndpoints("source-A", "PR_FS", "2026-09-07 08:00", "2026-09-07 12:00");
        fixture.SetEndpoints("source-a", "PR_FS", "2026-09-07 08:00", "2026-09-07 12:00");

        XerTable output = fixture.Transform();
        Assert.Equal(2, output.RowCount);
        Assert.Equal(0.5m, Fixture.Number(output, "source-A", "free_float"));
        Assert.Equal(0.4m, Fixture.Number(output, "source-a", "free_float"));
    }

    [Fact]
    public void Separator_collisions_between_source_and_calendar_id_do_not_merge_calendars()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.AddSource("source-A.C", 10, 24);
        // These distinct pairs both concatenate to source-A.C.C1 under the old
        // dot-separated key. Calculation identity must retain the pair itself.
        fixture.Set("CALENDAR", "source-A", "C1", "clndr_id", "C.C1");
        fixture.Set("TASK", "source-A", "T1", "clndr_id", "C.C1");
        fixture.SetEndpoints("source-A", "PR_FS", "2026-09-07 08:00", "2026-09-07 12:00");
        fixture.SetEndpoints("source-A.C", "PR_FS", "2026-09-07 08:00", "2026-09-07 12:00");

        XerTable output = fixture.Transform();
        Assert.Equal(2, output.RowCount);
        Assert.Equal(0.5m, Fixture.Number(output, "source-A", "free_float"));
        Assert.Equal(0.4m, Fixture.Number(output, "source-A.C", "free_float"));
    }

    [Theory]
    [InlineData("C1", "day_hr_cnt", "")]
    [InlineData("C1", "day_hr_cnt", "0")]
    [InlineData("C1", "day_hr_cnt", "invalid")]
    [InlineData("C1", "day_hr_cnt", "1,0")]
    [InlineData("C1", "clndr_data", "")]
    [InlineData("C1", "clndr_data", "not a calendar")]
    [InlineData("C2", "clndr_data", "")]
    [InlineData("C2", "clndr_data", "not a calendar")]
    public void Unknown_relationship_calendar_does_not_invent_a_float(
        string calendarId, string field, string value)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("CALENDAR", "source-A", calendarId, field, value);

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("T1")]
    [InlineData("T2")]
    public void Missing_relationship_activity_or_calendar_does_not_invent_a_float(string taskId)
    {
        var missingTask = new Fixture();
        missingTask.AddSource("source-A", 8, 24);
        missingTask.Remove("TASK", "source-A", taskId);
        Assert.Equal(string.Empty, missingTask.Value("free_float"));

        var missingCalendar = new Fixture();
        missingCalendar.AddSource("source-A", 8, 24);
        missingCalendar.Set("TASK", "source-A", taskId, "clndr_id", "MISSING");
        Assert.Equal(string.Empty, missingCalendar.Value("free_float"));
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
    public void Missing_relevant_relationship_endpoint_stays_blank(string relationshipType, bool predecessor)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.SetEndpoints("source-A", relationshipType, "2026-09-07 12:00", "2026-09-08 12:00");
        string taskId = predecessor ? "T1" : "T2";
        bool start = predecessor
            ? relationshipType is "PR_SS" or "PR_SF"
            : relationshipType is "PR_FS" or "PR_SS";
        fixture.Set("TASK", "source-A", taskId, start ? "early_start_date" : "early_end_date", "");
        fixture.Set("TASK", "source-A", taskId, start ? "restart_date" : "reend_date", "");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
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
    public void Malformed_preferred_remaining_endpoint_is_not_replaced_by_a_stale_early_date(
        string relationshipType, bool predecessor)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.SetEndpoints("source-A", relationshipType, "2026-09-07 12:00", "2026-09-08 12:00");
        bool start = predecessor
            ? relationshipType is "PR_SS" or "PR_SF"
            : relationshipType is "PR_FS" or "PR_SS";
        fixture.Set("TASK", "source-A", predecessor ? "T1" : "T2",
            start ? "restart_date" : "reend_date", "not-a-date");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("PR_FS")]
    [InlineData("PR_SS")]
    [InlineData("PR_FF")]
    [InlineData("PR_SF")]
    public void Absent_remaining_date_still_allows_a_valid_early_endpoint(string relationshipType)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.SetEndpoints("source-A", relationshipType, "2026-09-07 12:00", "2026-09-08 12:00");
        foreach (string taskId in new[] { "T1", "T2" })
        {
            fixture.Set("TASK", "source-A", taskId, "restart_date", "");
            fixture.Set("TASK", "source-A", taskId, "reend_date", "");
        }

        AssertFloat(fixture, 1m);
    }

    [Theory]
    [InlineData("T1", "status_code", "TK_Complete")]
    [InlineData("T2", "status_code", "TK_Complete")]
    [InlineData("T1", "task_type", "TT_LOE")]
    [InlineData("T2", "task_type", "TT_LOE")]
    [InlineData("T1", "task_type", "TT_WBS")]
    [InlineData("T2", "task_type", "TT_WBS")]
    [InlineData("T1", "task_type", "TT_Rsrc")]
    [InlineData("T2", "task_type", "TT_Rsrc")]
    public void Completed_derived_or_resource_calendar_dependent_activities_have_no_assumed_float(
        string taskId, string field, string value)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("TASK", "source-A", taskId, field, value);

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("PR_FS")]
    [InlineData("PR_SS")]
    [InlineData("PR_FF")]
    [InlineData("PR_SF")]
    public void Remaining_dates_override_different_early_dates_for_unstarted_activities(string relationshipType)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.SetEndpoints("source-A", relationshipType, "2026-09-07 12:00", "2026-09-08 12:00");
        foreach (string field in new[] { "early_start_date", "early_end_date" })
        {
            fixture.Set("TASK", "source-A", "T1", field, "2026-09-01 08:00");
            fixture.Set("TASK", "source-A", "T2", field, "2026-09-30 16:00");
        }

        AssertFloat(fixture, 1m);
    }

    [Theory]
    [InlineData("PR_FS")]
    [InlineData("PR_SS")]
    [InlineData("PR_FF")]
    [InlineData("PR_SF")]
    public void Internal_relationships_do_not_treat_aref_as_an_unadjusted_finish(string relationshipType)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.SetEndpoints("source-A", relationshipType, "2026-09-07 12:00", "2026-09-08 12:00");
        fixture.Set("TASKPRED", "source-A", "R1", "aref", "2099-01-01 00:00");
        fixture.Set("TASKPRED", "source-A", "R1", "arls", "1999-01-01 00:00");

        AssertFloat(fixture, 1m);
    }

    [Fact]
    public void Nonzero_lag_with_unknown_scheduling_calendar_does_not_invent_a_float()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "4");
        fixture.Set("SCHEDOPTIONS", "source-A", "P1", "sched_calendar_on_relationship_lag", "unknown");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("table")]
    [InlineData("column")]
    [InlineData("project row")]
    public void Absent_scheduling_metadata_is_not_confused_with_an_explicit_default(string missing)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "4");
        if (missing == "table") fixture.RemoveTable("SCHEDOPTIONS");
        else if (missing == "column") fixture.RemoveColumn("SCHEDOPTIONS", "sched_calendar_on_relationship_lag");
        else fixture.Remove("SCHEDOPTIONS", "source-A", "P1");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Fact]
    public void Truncated_scheduling_record_is_not_confused_with_an_explicit_blank_default()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "4");
        fixture.TruncateRow("SCHEDOPTIONS", "source-A", "P1", 1);

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("PROJECT")]
    [InlineData("SCHEDOPTIONS")]
    public void Duplicate_scheduling_context_does_not_choose_an_arbitrary_lag_calendar(string tableName)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "4");
        fixture.Set("SCHEDOPTIONS", "source-A", "P1", "sched_calendar_on_relationship_lag", "rcal_ProjDefault");
        fixture.DuplicateRow(tableName, "source-A", "P1");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Present_blank_scheduling_selection_uses_the_documented_successor_default(string setting)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 10, predecessorWeekdaysOnly: true);
        fixture.SetEndpoints("source-A", "PR_FS", "2026-09-04 12:00", "2026-09-07 12:00");
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "8");
        fixture.Set("SCHEDOPTIONS", "source-A", "P1", "sched_calendar_on_relationship_lag", setting);

        AssertFloat(fixture, 0.5m);
    }

    [Theory]
    [InlineData("PR_SS")]
    [InlineData("PR_SF")]
    public void Active_start_relationship_without_progress_metadata_stays_unknown(string relationshipType)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.SetEndpoints("source-A", relationshipType, "2026-09-07 08:00", "2026-09-08 08:00");
        fixture.Set("TASK", "source-A", "T1", "status_code", "TK_Active");
        fixture.Set("TASK", "source-A", "T1", "act_start_date", "2026-09-03 08:00");
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "16");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("PR_FS")]
    [InlineData("PR_FF")]
    public void Active_finish_relationship_uses_remaining_finish_not_stale_early_finish(string relationshipType)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.SetEndpoints("source-A", relationshipType, "2026-09-07 12:00", "2026-09-08 12:00");
        fixture.Set("TASK", "source-A", "T1", "status_code", "TK_Active");
        fixture.Set("TASK", "source-A", "T1", "act_start_date", "2026-09-01 08:00");
        fixture.Set("TASK", "source-A", "T1", "early_end_date", "2026-09-03 16:00");

        AssertFloat(fixture, 1m);
    }

    [Theory]
    [InlineData("PR_FS")]
    [InlineData("PR_FF")]
    public void Active_finish_relationship_requires_remaining_finish_even_when_early_finish_exists(string type)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.SetEndpoints("source-A", type, "2026-09-07 12:00", "2026-09-08 12:00");
        fixture.Set("TASK", "source-A", "T1", "status_code", "TK_Active");
        fixture.Set("TASK", "source-A", "T1", "act_start_date", "2026-09-01 08:00");
        fixture.Set("TASK", "source-A", "T1", "reend_date", "");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("not a number")]
    [InlineData("NaN")]
    [InlineData("Infinity")]
    [InlineData("1,000")]
    [InlineData("")]
    [InlineData("   ")]
    public void Malformed_lag_is_not_reinterpreted_as_zero(string lagHours)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", lagHours);

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Fact]
    public void Missing_lag_column_is_not_reinterpreted_as_an_explicit_zero()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.RemoveColumn("TASKPRED", "lag_hr_cnt");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Fact]
    public void Unknown_blank_lag_changes_only_free_float_not_legacy_display_columns()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        XerTable known = fixture.Transform();
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "");
        XerTable unknown = fixture.Transform();

        Assert.Equal(known.Headers, unknown.Headers);
        DataRow knownRow = Assert.Single(known.Rows);
        DataRow unknownRow = Assert.Single(unknown.Rows);
        foreach (string field in known.Headers!)
        {
            // The copied raw input lag is also blank, intentionally, but derived lag
            // retains its pre-existing display behaviour while free_float is unknown.
            if (field is "free_float" or "lag_hr_cnt") continue;
            Assert.Equal(knownRow.Fields[known.FieldIndexes[field]], unknownRow.Fields[unknown.FieldIndexes[field]]);
        }
        Assert.Equal(string.Empty, unknownRow.Fields[unknown.FieldIndexes["free_float"]]);
        Assert.Equal(0m, fixture.Number("lag"));
    }

    [Fact]
    public void Truncated_relationship_record_without_lag_does_not_invent_zero()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.TruncateRow("TASKPRED", "source-A", "R1", 4);

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("MISSING")]
    public void Project_default_lag_calendar_requires_a_resolvable_project_calendar(string calendarId)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("PROJECT", "source-A", "P1", "clndr_id", calendarId);
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "4");
        fixture.Set("SCHEDOPTIONS", "source-A", "P1", "sched_calendar_on_relationship_lag", "rcal_ProjDefault");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Fact]
    public void Project_default_lag_calendar_requires_owning_project_metadata()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Remove("PROJECT", "source-A", "P1");
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "4");
        fixture.Set("SCHEDOPTIONS", "source-A", "P1", "sched_calendar_on_relationship_lag", "rcal_ProjDefault");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData(" RCAL_PREDECESSOR ", "0")]
    [InlineData(" rCaL_sUcCeSsOr ", "0.5")]
    [InlineData(" rCaL_pRoJdEfAuLt ", "0.5")]
    public void Scheduling_calendar_tokens_are_normalized_before_projection(string setting, string expected)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 10, predecessorWeekdaysOnly: true);
        fixture.SetEndpoints("source-A", "PR_FS", "2026-09-04 12:00", "2026-09-07 12:00");
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "8");
        fixture.Set("SCHEDOPTIONS", "source-A", "P1", "sched_calendar_on_relationship_lag", setting);

        AssertFloat(fixture, decimal.Parse(expected, CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData("PR_FS")]
    [InlineData("PR_SS")]
    [InlineData("PR_FF")]
    [InlineData("PR_SF")]
    public void Active_successor_does_not_invent_remaining_relationship_or_out_of_sequence_logic(string relationshipType)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.SetEndpoints("source-A", relationshipType, "2026-09-07 12:00", "2026-09-08 12:00");
        fixture.Set("TASK", "source-A", "T2", "status_code", "TK_Active");
        fixture.Set("TASK", "source-A", "T2", "act_start_date", "2026-09-01 08:00");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Fact]
    public void Calendar_without_future_work_keeps_relationship_row_with_unknown_float()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("CALENDAR", "source-A", "C1", "clndr_data",
            CalendarBlob("08:00", "16:00", noShifts: true));
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "1");

        // An unavailable calculation must not turn a present TASKPRED into a missing table.
        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("T1")]
    [InlineData("T2")]
    public void Duplicate_task_identity_keeps_relationship_but_does_not_choose_an_arbitrary_endpoint(string taskId)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.DuplicateRow("TASK", "source-A", taskId);

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Fact]
    public void Successor_project_mismatch_keeps_relationship_with_unknown_float()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("TASK", "source-A", "T2", "proj_id", "DIFFERENT_PROJECT");

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("C1")]
    [InlineData("C2")]
    public void Duplicate_used_calendar_identity_does_not_invent_a_float(string calendarId)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.DuplicateRow("CALENDAR", "source-A", calendarId);

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Unrelated_invalid_calendar_does_not_remove_valid_relationships(bool duplicate)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.AddCalendar("source-A", duplicate ? "UNUSED" : "", 8, CalendarBlob("08:00", "16:00"));
        if (duplicate) fixture.DuplicateRow("CALENDAR", "source-A", "UNUSED");

        AssertFloat(fixture, 1m);
    }

    [Fact]
    public void Unprojectable_numeric_lag_keeps_relationship_row_with_unknown_float()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", decimal.MaxValue.ToString(CultureInfo.InvariantCulture));

        Assert.Equal(string.Empty, fixture.Value("free_float"));
    }

    [Theory]
    [InlineData("lag")]
    [InlineData("total_float")]
    public void Overflowing_day_conversion_does_not_remove_other_relationships(string column)
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24);
        fixture.AddSource("source-B", 8, 24);
        bool lag = column == "lag";
        fixture.Set("CALENDAR", "source-A", lag ? "C1" : "C2", "day_hr_cnt", "0.5");
        fixture.Set(lag ? "TASKPRED" : "TASK", "source-A", lag ? "R1" : "T2",
            lag ? "lag_hr_cnt" : "total_float_hr_cnt", decimal.MaxValue.ToString(CultureInfo.InvariantCulture));

        XerTable table = fixture.Transform();
        Assert.Equal(2, table.RowCount);
        DataRow affected = Assert.Single(table.Rows, row => row.SourceFilename == "source-A");
        Assert.Equal(string.Empty, affected.Fields[table.FieldIndexes[column]]);
        if (lag) Assert.Equal(string.Empty, affected.Fields[table.FieldIndexes["free_float"]]);
        Assert.Equal(1m, Fixture.Number(table, "source-B", "free_float"));
    }

    private static void AssertFloat(Fixture fixture, decimal expected) =>
        Assert.Equal(expected, fixture.Number("free_float"));

    private static Fixture SplitShiftElapsedLagFixture()
    {
        var fixture = new Fixture();
        fixture.AddSource("source-A", 8, 24, predecessorWeekdaysOnly: true);
        fixture.Set("CALENDAR", "source-A", "C1", "clndr_data",
            CalendarBlob("08:00", "12:00", weekdaysOnly: true, secondStart: "13:00", secondFinish: "17:00"));
        fixture.Set("TASKPRED", "source-A", "R1", "lag_hr_cnt", "8");
        fixture.Set("SCHEDOPTIONS", "source-A", "P1", "sched_calendar_on_relationship_lag", "rcal_24Hour");
        return fixture;
    }

    private static string CalendarBlob(
        string start, string finish, bool weekdaysOnly = false,
        string? secondStart = null, string? secondFinish = null, DateTime? holiday = null,
        bool noShifts = false)
    {
        string shift = noShifts ? string.Empty : $"(0||0(s|{start}|f|{finish})())";
        if (secondStart is not null)
            shift += $"(0||1(s|{secondStart}|f|{secondFinish})())";
        string days = string.Concat(Enumerable.Range(1, 7).Select(day =>
            $"(0||{day}()({(weekdaysOnly && day is 1 or 7 ? string.Empty : shift)}))"));
        string exceptions = holiday is null ? string.Empty
            : $"(0||0(d|{holiday.Value.ToOADate().ToString("0", CultureInfo.InvariantCulture)})())";
        return $"(0||CalendarData()((0||DaysOfWeek()({days}))(0||Exceptions()({exceptions}))))";
    }

    private sealed class Fixture
    {
        private XerDataStore _store = new();

        internal Fixture()
        {
            AddTable("PROJECT", "proj_id", "clndr_id", "last_recalc_date");
            AddTable("CALENDAR", "clndr_id", "clndr_name", "day_hr_cnt", "clndr_data", "base_clndr_id", "clndr_type");
            AddTable("TASK", "task_id", "proj_id", "clndr_id", "status_code", "task_type", "task_code",
                "early_start_date", "early_end_date", "restart_date", "reend_date", "act_start_date", "act_end_date",
                "total_float_hr_cnt", "remain_drtn_hr_cnt");
            AddTable("TASKPRED", "task_pred_id", "task_id", "pred_task_id", "pred_type", "lag_hr_cnt", "proj_id", "aref", "arls");
            AddTable("SCHEDOPTIONS", "proj_id", "sched_calendar_on_relationship_lag");
        }

        internal void AddSource(string source, int predecessorHours, int successorHours, bool predecessorWeekdaysOnly = false)
        {
            Row("PROJECT", source, "P1", "PROJECT_CAL", "2026-09-04 08:00");
            AddCalendar(source, "C1", predecessorHours, HoursBlob(predecessorHours, predecessorWeekdaysOnly));
            AddCalendar(source, "C2", successorHours, HoursBlob(successorHours));
            AddCalendar(source, "PROJECT_CAL", 24, HoursBlob(24));
            foreach (string id in new[] { "T1", "T2" })
                Row("TASK", source, id, "P1", id == "T1" ? "C1" : "C2", "TK_NotStart", "TT_Task", id,
                    "2026-09-07 08:00", "2026-09-07 16:00", "2026-09-07 08:00", "2026-09-07 16:00", "", "", "16", "8");
            Row("TASKPRED", source, "R1", "T2", "T1", "PR_FS", "0", "P1", "", "");
            Row("SCHEDOPTIONS", source, "P1", "rcal_Predecessor");
            SetEndpoints(source, "PR_FS", "2026-09-07 12:00", "2026-09-08 12:00");
        }

        internal void AddCalendar(string source, string id, int hours, string blob) =>
            Row("CALENDAR", source, id, id, hours.ToString(CultureInfo.InvariantCulture), blob, "", "CA_Project");

        internal void SetEndpoints(string source, string type, string predecessorDate, string successorDate)
        {
            Set("TASKPRED", source, "R1", "pred_type", type);
            bool predecessorStart = type is "PR_SS" or "PR_SF";
            bool successorStart = type is "PR_FS" or "PR_SS";
            SetEndpoint("T1", predecessorStart, predecessorDate);
            SetEndpoint("T2", successorStart, successorDate);

            void SetEndpoint(string taskId, bool start, string value)
            {
                XerTable tasks = _store.GetTable("TASK")!;
                DataRow task = tasks.Rows.Single(row => row.SourceFilename == source && row.Fields[0] == taskId);
                string otherField = start ? "reend_date" : "restart_date";
                DateTime endpoint = DateTime.ParseExact(value, "yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture);
                DateTime other = DateTime.ParseExact(task.Fields[tasks.FieldIndexes[otherField]],
                    "yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture);
                // Keep complete task ranges coherent when changing only a link's
                // selected endpoint. The opposite endpoint is not part of the oracle.
                if ((start && other <= endpoint) || (!start && other >= endpoint))
                    other = endpoint.AddDays(start ? 1 : -1);
                SetTaskDates(source, taskId,
                    start ? value : other.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture),
                    start ? other.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) : value);
            }
        }

        internal void Set(string tableName, string source, string id, string field, string value)
        {
            XerTable table = _store.GetTable(tableName)!;
            DataRow row = table.Rows.Single(row => row.SourceFilename == source && row.Fields[0] == id);
            row.Fields[table.FieldIndexes[field]] = value;
        }

        internal void SetTaskDates(string source, string id, string start, string finish)
        {
            Set("TASK", source, id, "early_start_date", start);
            Set("TASK", source, id, "restart_date", start);
            Set("TASK", source, id, "early_end_date", finish);
            Set("TASK", source, id, "reend_date", finish);
        }

        internal void TruncateRow(string tableName, string source, string id, int length)
        {
            XerTable existing = _store.GetTable(tableName)!;
            var replacement = new XerTable(tableName);
            replacement.SetHeaders(existing.Headers!);
            replacement.AddRows(existing.Rows.Select(row => row.SourceFilename == source && row.Fields[0] == id
                ? new DataRow(row.Fields.Take(length).ToArray(), source)
                : row));
            _store.AddTable(replacement);
        }

        internal void DuplicateRow(string tableName, string source, string id)
        {
            XerTable table = _store.GetTable(tableName)!;
            DataRow existing = table.Rows.Single(row => row.SourceFilename == source && row.Fields[0] == id);
            table.AddRow(new DataRow(existing.Fields.ToArray(), source));
        }

        internal void Remove(string tableName, string source, string id)
        {
            XerTable existing = _store.GetTable(tableName)!;
            var replacement = new XerTable(tableName);
            replacement.SetHeaders(existing.Headers!);
            replacement.AddRows(existing.Rows.Where(row => row.SourceFilename != source || row.Fields[0] != id));
            _store.AddTable(replacement);
        }

        internal void RemoveTable(string tableName)
        {
            var replacement = new XerDataStore();
            foreach (string name in _store.TableNames.Where(name => name != tableName))
                replacement.AddTable(_store.GetTable(name)!);
            _store = replacement;
        }

        internal void RemoveColumn(string tableName, string field)
        {
            XerTable existing = _store.GetTable(tableName)!;
            int removedIndex = existing.FieldIndexes[field];
            var replacement = new XerTable(tableName);
            replacement.SetHeaders(existing.Headers!.Where((_, index) => index != removedIndex).ToArray());
            replacement.AddRows(existing.Rows.Select(row => new DataRow(
                row.Fields.Where((_, index) => index != removedIndex).ToArray(), row.SourceFilename)));
            _store.AddTable(replacement);
        }

        internal XerTable Transform() => Assert.IsType<XerTable>(
            new XerTransformer(_store).Create06XerPredecessor(new ConcurrentDictionary<string, XerTable>()));

        internal string Value(string field)
        {
            XerTable output = Transform();
            DataRow row = Assert.Single(output.Rows);
            return row.Fields[output.FieldIndexes[field]];
        }

        internal decimal Number(string field) => decimal.Parse(Value(field), CultureInfo.InvariantCulture);

        internal static decimal Number(XerTable table, string source, string field) =>
            decimal.Parse(table.Rows.Single(row => row.SourceFilename == source).Fields[table.FieldIndexes[field]],
                CultureInfo.InvariantCulture);

        private void AddTable(string name, params string[] headers)
        {
            var table = new XerTable(name);
            table.SetHeaders(headers);
            _store.AddTable(table);
        }

        private void Row(string tableName, string source, params string[] fields) =>
            _store.GetTable(tableName)!.AddRow(new DataRow(fields, source));

        private static string HoursBlob(int hours, bool weekdaysOnly = false) => hours == 24
            ? CalendarBlob("00:00", "00:00", weekdaysOnly)
            : CalendarBlob("08:00", hours == 10 ? "18:00" : "16:00", weekdaysOnly);
    }
}
