using System.Globalization;

namespace XerToCsvConverter.Core.Tests;

public sealed class ResourceCalendarCalculationTests
{
    private const string SourceToken = "resource-calendar-source";

    [Fact]
    public void Resource_dependent_task_uses_resource_calendar_for_monthly_distribution()
    {
        XerDataStore store = BuildStore(
            taskType: "TT_Rsrc",
            taskCalendarId: "C1",
            resourceCalendarId: "C2",
            quantity: "90",
            start: "2026-01-31 08:00:00",
            finish: "2026-02-02 18:00:00",
            new CalendarInput("C1", "8", P6TestCalendars.WorkWeek()),
            new CalendarInput("C2", "10", AllDays("08:00", "18:00")));

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());
        IReadOnlyList<Dictionary<string, string>> rows = Records(table);

        Assert.Collection(rows,
            january =>
            {
                Assert.Equal($"{SourceToken}.C2", january["clndr_id_key"]);
                Assert.Equal("2026-01-01", january["distribution_month"]);
                Assert.Equal("30.0000", january["monthly_quantity"]);
                Assert.Equal("10.00", january["month_working_hours"]);
                Assert.Equal("10.00", january["calendar_hours_per_day"]);
                Assert.Equal("1.00", january["month_working_days"]);
            },
            february =>
            {
                Assert.Equal($"{SourceToken}.C2", february["clndr_id_key"]);
                Assert.Equal("2026-02-01", february["distribution_month"]);
                Assert.Equal("60.0000", february["monthly_quantity"]);
                Assert.Equal("20.00", february["month_working_hours"]);
                Assert.Equal("10.00", february["calendar_hours_per_day"]);
                Assert.Equal("2.00", february["month_working_days"]);
            });
    }

    [Theory]
    [InlineData("fr-FR")]
    [InlineData("th-TH")]
    public void Twenty_four_hour_month_boundaries_conserve_quantity_and_dates_are_invariant(
        string cultureName)
    {
        CultureInfo previousCulture = CultureInfo.CurrentCulture;
        CultureInfo previousUiCulture = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo(cultureName);
            CultureInfo.CurrentUICulture = new CultureInfo(cultureName);
            XerDataStore store = BuildStore(
                taskType: "TT_Task",
                taskCalendarId: "C1",
                resourceCalendarId: "C2",
                quantity: "48",
                start: "2026-01-31 00:00:00",
                finish: "2026-02-02 00:00:00",
                new CalendarInput("C1", "24", AllDays("00:00", "24:00")),
                new CalendarInput("C2", "10", AllDays("08:00", "18:00")));

            XerTable table = Assert.IsType<XerTable>(
                new XerTransformer(store).Create15XerResourceDistribution());
            IReadOnlyList<Dictionary<string, string>> rows = Records(table);

            Assert.Equal(48m, rows.Sum(row =>
                decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
            Assert.Collection(rows,
                january =>
                {
                    Assert.Equal($"{SourceToken}.C1", january["clndr_id_key"]);
                    Assert.Equal("2026-01-01", january["distribution_month"]);
                    Assert.Equal("2026-01-31 00:00:00", january["month_start_date"]);
                    Assert.Equal("2026-02-01 00:00:00", january["month_end_date"]);
                    Assert.Equal("24.0000", january["monthly_quantity"]);
                    Assert.Equal("24.00", january["month_working_hours"]);
                    Assert.Equal("1", january["month_calendar_days"]);
                    Assert.Equal("2", january["total_calendar_days"]);
                    Assert.Equal("2026-01-31 00:00:00", january["Start"]);
                    Assert.Equal("2026-02-02 00:00:00", january["Finish"]);
                },
                february =>
                {
                    Assert.Equal("2026-02-01", february["distribution_month"]);
                    Assert.Equal("2026-02-01 00:00:00", february["month_start_date"]);
                    Assert.Equal("2026-02-02 00:00:00", february["month_end_date"]);
                    Assert.Equal("24.0000", february["monthly_quantity"]);
                    Assert.Equal("24.00", february["month_working_hours"]);
                    Assert.Equal("1", february["month_calendar_days"]);
                    Assert.Equal("2", february["total_calendar_days"]);
                });
        }
        finally
        {
            CultureInfo.CurrentCulture = previousCulture;
            CultureInfo.CurrentUICulture = previousUiCulture;
        }
    }

    [Fact]
    public void Valid_working_intervals_do_not_invent_hours_per_day_diagnostics()
    {
        XerDataStore store = BuildStore(
            taskType: "TT_Task",
            taskCalendarId: "C1",
            resourceCalendarId: "C2",
            quantity: "16",
            start: "2026-01-05 08:00:00",
            finish: "2026-01-06 17:00:00",
            new CalendarInput("C1", "", P6TestCalendars.WorkWeek()),
            new CalendarInput("C2", "10", AllDays("08:00", "18:00")));

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());
        Dictionary<string, string> row = Assert.Single(Records(table));

        Assert.Equal("16.0000", row["monthly_quantity"]);
        Assert.Equal("16.00", row["month_working_hours"]);
        Assert.Equal("16.00", row["total_working_hours"]);
        Assert.Equal(string.Empty, row["calendar_hours_per_day"]);
        Assert.Equal(string.Empty, row["month_working_days"]);
        Assert.Equal(string.Empty, row["total_working_days"]);
    }

    [Fact]
    public void Invalid_selected_resource_calendar_does_not_fall_back_to_task_calendar()
    {
        XerDataStore store = BuildStore(
            taskType: "TT_Rsrc",
            taskCalendarId: "C1",
            resourceCalendarId: "C2",
            quantity: "8",
            start: "2026-01-05 08:00:00",
            finish: "2026-01-05 17:00:00",
            new CalendarInput("C1", "8", P6TestCalendars.WorkWeek()),
            new CalendarInput("C2", "8", ""));

        AssertUnallocated(store, "RESOURCE_CALENDAR_INVALID", "8.0000");
    }

    [Theory]
    [InlineData("1", "1.0000")]
    [InlineData("0.0001", "0.0001")]
    [InlineData("0.00011", "0.0001")]
    public void Monthly_rounding_conserves_the_assignment_quantity_at_export_precision(
        string quantity, string expectedTotal)
    {
        // There are 1, 28 and 1 days in the three slices. Independently rounding
        // 1/30, 28/30 and 1/30 to four places would lose 0.0001 units.
        XerDataStore store = BuildStore("TT_Task", "C1", "C1", quantity,
            "2026-01-31 00:00:00", "2026-03-02 00:00:00",
            new CalendarInput("C1", "24", AllDays("00:00", "24:00")));

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());
        IReadOnlyList<Dictionary<string, string>> rows = Records(table);

        Assert.Equal(decimal.Parse(expectedTotal, CultureInfo.InvariantCulture),
            rows.Sum(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
        Assert.All(rows, row => Assert.True(
            decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture) >= 0));
        if (quantity == "1")
        {
            Assert.Equal(new[] { "0.0333", "0.9334", "0.0333" },
                rows.Select(row => row["monthly_quantity"]).ToArray());
        }
    }

    [Fact]
    public void Overnight_month_boundary_counts_both_sides_without_losing_or_double_counting_hours()
    {
        XerDataStore store = BuildStore("TT_Task", "C1", "C1", "80",
            "2026-01-31 22:00:00", "2026-02-02 06:00:00",
            new CalendarInput("C1", "8", AllDays("22:00", "06:00")));

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());
        IReadOnlyList<Dictionary<string, string>> rows = Records(table);

        Assert.Collection(rows,
            january =>
            {
                Assert.Equal("2.00", january["month_working_hours"]);
                Assert.Equal("10.0000", january["monthly_quantity"]);
                Assert.Equal("2026-02-01 00:00:00", january["month_end_date"]);
            },
            february =>
            {
                Assert.Equal("14.00", february["month_working_hours"]);
                Assert.Equal("70.0000", february["monthly_quantity"]);
                Assert.Equal("2026-02-01 00:00:00", february["month_start_date"]);
            });
        Assert.All(rows, row => Assert.Equal("16.00", row["total_working_hours"]));
    }

    [Fact]
    public void Partial_split_shifts_allocate_by_working_hours_not_elapsed_hours_or_hours_per_day()
    {
        XerDataStore store = BuildStore("TT_Task", "C1", "C1", "18",
            "2026-01-30 11:00:00", "2026-02-02 14:00:00",
            new CalendarInput("C1", "10", P6TestCalendars.WorkWeek()));

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());
        IReadOnlyList<Dictionary<string, string>> rows = Records(table);

        Assert.Collection(rows,
            january =>
            {
                Assert.Equal("5.00", january["month_working_hours"]);
                Assert.Equal("9.0000", january["monthly_quantity"]);
                Assert.Equal("0.50", january["month_working_days"]);
            },
            february =>
            {
                Assert.Equal("5.00", february["month_working_hours"]);
                Assert.Equal("9.0000", february["monthly_quantity"]);
                Assert.Equal("0.50", february["month_working_days"]);
            });
        Assert.All(rows, row => Assert.Equal("10.00", row["calendar_hours_per_day"]));
    }

    [Fact]
    public void Finished_assignment_on_active_activity_stops_actual_spread_at_its_own_finish()
    {
        XerDataStore store = ActualStore("TK_Active", "0.1", "0.2",
            "2026-01-30 08:00:00", "2026-01-31 16:00:00");
        SetField(store, "PROJECT", "last_recalc_date", "2026-02-02 16:00:00");

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());
        Dictionary<string, string> row = Assert.Single(Records(table));

        Assert.Equal("2026-01-01", row["distribution_month"]);
        Assert.Equal("2026-01-30 08:00:00", row["Start"]);
        Assert.Equal("2026-01-31 16:00:00", row["Finish"]);
        Assert.Equal("16.00", row["total_working_hours"]);
        Assert.Equal("0.3000", row["monthly_quantity"]);
        Assert.Equal("1", row["is_actual"]);
    }

    [Fact]
    public void Unfinished_assignment_spreads_actuals_only_to_its_own_project_data_date()
    {
        XerDataStore store = ActualStore("TK_Active", "24", "8",
            "2026-01-30 08:00:00", "");
        SetField(store, "PROJECT", "last_recalc_date", "2026-02-02 16:00:00");

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());
        IReadOnlyList<Dictionary<string, string>> rows = Records(table);

        Assert.Collection(rows,
            january => Assert.Equal("16.0000", january["monthly_quantity"]),
            february => Assert.Equal("16.0000", february["monthly_quantity"]));
        Assert.All(rows, row =>
        {
            Assert.Equal("2026-02-02 16:00:00", row["Finish"]);
            Assert.Equal("32.00", row["total_working_hours"]);
        });
    }

    [Theory]
    [InlineData("act_reg_qty", "NaN")]
    [InlineData("act_reg_qty", "Infinity")]
    [InlineData("act_reg_qty", "invalid")]
    [InlineData("act_reg_qty", "-1")]
    [InlineData("act_ot_qty", "NaN")]
    [InlineData("act_ot_qty", "-Infinity")]
    [InlineData("act_ot_qty", "invalid")]
    [InlineData("act_ot_qty", "-1")]
    [InlineData("remain_qty", "NaN")]
    [InlineData("remain_qty", "Infinity")]
    [InlineData("remain_qty", "invalid")]
    [InlineData("remain_qty", "-1")]
    public void Invalid_quantity_cannot_be_silently_dropped_or_exported_as_nonfinite(
        string field, string value)
    {
        XerDataStore store = ActualStore("TK_Active", "8", "0",
            "2026-01-01 08:00:00", "");
        SetField(store, "PROJECT", "last_recalc_date", "2026-01-02 16:00:00");
        SetField(store, "TASKRSRC", field, value);

        var transformer = new XerTransformer(store);
        XerTable distribution = Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution());
        XerTable diagnostics = transformer.CreateDataQualityTable();
        DataRow issue = Assert.Single(diagnostics.Rows);
        bool remaining = field == "remain_qty";
        Assert.Equal(remaining ? "REMAINING_QUANTITY_INVALID" : "ACTUAL_QUANTITY_INVALID",
            issue.Fields[diagnostics.FieldIndexes["issue_code"]]);
        Assert.Equal(value, issue.Fields[diagnostics.FieldIndexes[field]]);
        Assert.Equal(remaining ? "Remaining" : "Actual", issue.Fields[diagnostics.FieldIndexes["allocation_portion"]]);
        string expectedUnallocated = value == "-1" ? field == "act_ot_qty" ? "7.0000" : "-1.0000" : "";
        Assert.Equal(expectedUnallocated, issue.Fields[diagnostics.FieldIndexes[
            remaining ? "unallocated_remaining_quantity" : "unallocated_actual_quantity"]]);
        if (remaining)
            Assert.Equal(8m, Records(distribution).Sum(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
        else Assert.Empty(distribution.Rows);
        Assert.Null(transformer.GetGenerationFailure(EnhancedTableNames.XerResourceDist15));
    }

    [Theory]
    [InlineData("restart_date", "")]
    [InlineData("restart_date", "not-a-date")]
    [InlineData("reend_date", "")]
    [InlineData("reend_date", "not-a-date")]
    [InlineData("reend_date", "2025-12-31 08:00:00")]
    public void Positive_remaining_quantity_with_invalid_assignment_period_is_preserved_as_unallocated(
        string field, string value)
    {
        XerDataStore store = BuildStore("TT_Task", "C1", "C1", "8",
            "2026-01-01 08:00:00", "2026-01-01 16:00:00",
            new CalendarInput("C1", "8", AllDays("08:00", "16:00")));
        SetField(store, "TASKRSRC", field, value);

        AssertUnallocated(store, "REMAINING_PERIOD_INVALID", "8.0000", field: field, raw: value);
    }

    [Theory]
    [InlineData("TK_Active", "act_start_date", "")]
    [InlineData("TK_Active", "act_start_date", "not-a-date")]
    [InlineData("TK_Active", "act_end_date", "not-a-date")]
    [InlineData("TK_Active", "act_end_date", "2025-12-31 08:00:00")]
    [InlineData("TK_Complete", "act_end_date", "")]
    [InlineData("TK_Complete", "act_end_date", "not-a-date")]
    public void Positive_actual_quantity_with_invalid_assignment_period_is_preserved_as_unallocated(
        string status, string field, string value)
    {
        XerDataStore store = ActualStore(status, "8", "0",
            "2026-01-01 08:00:00", "2026-01-01 16:00:00");
        SetField(store, "PROJECT", "last_recalc_date", "2026-01-02 16:00:00");
        SetField(store, "TASKRSRC", field, value);

        var transformer = new XerTransformer(store);
        XerTable table = Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution());
        Assert.Empty(table.Rows);
        XerTable diagnostics = transformer.CreateDataQualityTable();
        DataRow issue = Assert.Single(diagnostics.Rows);
        Assert.Equal("8.0000", issue.Fields[diagnostics.FieldIndexes["unallocated_actual_quantity"]]);
        Assert.Equal(value, issue.Fields[diagnostics.FieldIndexes[field]]);
    }

    [Fact]
    public void Missing_project_data_date_does_not_silently_drop_unfinished_actual_assignment()
    {
        XerDataStore store = ActualStore("TK_Active", "8", "0",
            "2026-01-01 08:00:00", "");
        SetField(store, "PROJECT", "last_recalc_date", "");

        var transformer = new XerTransformer(store);
        XerTable table = Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution());
        Assert.Empty(table.Rows);
        XerTable diagnostics = transformer.CreateDataQualityTable();
        DataRow issue = Assert.Single(diagnostics.Rows);
        Assert.Equal("8.0000", issue.Fields[diagnostics.FieldIndexes["unallocated_actual_quantity"]]);
        Assert.Equal("", issue.Fields[diagnostics.FieldIndexes["project_data_date"]]);
    }

    [Fact]
    public void Zero_quantities_do_not_require_assignment_dates_or_calendar_availability()
    {
        XerDataStore store = BuildStore("TT_Task", "C1", "C1", "0", "", "",
            new CalendarInput("C1", "8", P6TestCalendars.WorkWeek()));

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());

        Assert.Empty(table.Rows);
    }

    [Fact]
    public void Remaining_assignment_period_is_not_replaced_by_activity_dates()
    {
        XerDataStore store = BuildStore("TT_Task", "C1", "C1", "8",
            "2026-02-02 08:00:00", "2026-02-02 16:00:00",
            new CalendarInput("C1", "8", AllDays("08:00", "16:00")));
        XerTable task = Assert.IsType<XerTable>(store.GetTable("TASK"));
        // The activity dates are deliberately a wider interval than its assignment.
        XerTable replacement = Table("TASK",
            task.Headers!.Concat(new[] { "restart_date", "reend_date" }).ToArray());
        replacement.AddRow(new DataRow(new[]
        {
            "T1", "P1", "C1", "TT_Task", "TK_NotStart", "A100",
            "2026-01-01 08:00:00", "2026-03-31 16:00:00"
        }, SourceToken));
        store.AddTable(replacement);

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());
        Dictionary<string, string> row = Assert.Single(Records(table));

        Assert.Equal("2026-02-01", row["distribution_month"]);
        Assert.Equal("8.00", row["total_working_hours"]);
        Assert.Equal("8.0000", row["monthly_quantity"]);
    }

    [Fact]
    public void Repeated_native_ids_from_separate_source_occurrences_keep_their_own_calendars()
    {
        XerDataStore first = BuildStore("TT_Task", "C1", "C1", "24",
            "2026-01-31 00:00:00", "2026-02-02 00:00:00",
            new CalendarInput("C1", "24", AllDays("00:00", "24:00")));
        XerDataStore second = BuildStore("TT_Task", "C1", "C1", "8",
            "2026-01-31 00:00:00", "2026-02-02 00:00:00",
            new CalendarInput("C1", "8", AllDays("08:00", "16:00")));
        const string secondToken = "resource-calendar-source-repeat";
        foreach (string name in new[] { "TASK", "PROJECT", "RSRC", "TASKRSRC", "CALENDAR" })
        {
            XerTable destination = Assert.IsType<XerTable>(first.GetTable(name));
            XerTable source = Assert.IsType<XerTable>(second.GetTable(name));
            foreach (DataRow row in source.Rows)
                destination.AddRow(new DataRow(row.Fields.ToArray(), secondToken));
        }

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(first).Create15XerResourceDistribution());
        IReadOnlyList<Dictionary<string, string>> rows = Records(table);

        Assert.Equal(4, rows.Count);
        Dictionary<string, string>[] firstRows = rows
            .Where(row => row["task_id_key"] == $"{SourceToken}.T1").ToArray();
        Dictionary<string, string>[] secondRows = rows
            .Where(row => row["task_id_key"] == $"{secondToken}.T1").ToArray();
        Assert.Equal(2, firstRows.Length);
        Assert.Equal(2, secondRows.Length);
        Assert.All(firstRows, row =>
        {
            Assert.Equal("12.0000", row["monthly_quantity"]);
            Assert.Equal("24.00", row["month_working_hours"]);
            Assert.Equal($"{SourceToken}.C1", row["clndr_id_key"]);
        });
        Assert.All(secondRows, row =>
        {
            Assert.Equal("4.0000", row["monthly_quantity"]);
            Assert.Equal("8.00", row["month_working_hours"]);
            Assert.Equal($"{secondToken}.C1", row["clndr_id_key"]);
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Assignment_project_cannot_disambiguate_a_repeated_public_task_identity(
        bool reverseTaskOrder)
    {
        XerDataStore store = MultiProjectStore(reverseTaskOrder);

        // Project context cannot repair the duplicate public task_id_key shared
        // by these two TASK rows. Preserve each affected assignment as an
        // unallocated warning in either order, without selecting a task.
        AssertUnallocated(store, "ASSIGNMENT_CONTEXT_INVALID", "32.0000", expectedCount: 2);
    }

    [Fact]
    public void Assignment_without_project_cannot_select_between_ambiguous_task_ids()
    {
        XerDataStore store = MultiProjectStore(false);
        XerTable assignments = Assert.IsType<XerTable>(store.GetTable("TASKRSRC"));
        assignments.Rows[0].Fields[assignments.FieldIndexes["proj_id"]] = "";

        AssertUnallocated(store, "ASSIGNMENT_CONTEXT_INVALID", "32.0000", expectedCount: 2);
    }

    [Fact]
    public void Conflicting_task_identity_cannot_select_an_arbitrary_calendar()
    {
        XerDataStore store = BuildStore("TT_Task", "C1", "C2", "24",
            "2026-01-31 00:00:00", "2026-02-02 00:00:00",
            new CalendarInput("C1", "24", AllDays("00:00", "24:00")),
            new CalendarInput("C2", "8", AllDays("08:00", "16:00")));
        XerTable task = Assert.IsType<XerTable>(store.GetTable("TASK"));
        string[] conflicting = Assert.Single(task.Rows).Fields.ToArray();
        conflicting[task.FieldIndexes["clndr_id"]] = "C2";
        task.AddRow(new DataRow(conflicting, SourceToken));

        AssertUnallocated(store, "ASSIGNMENT_CONTEXT_INVALID", "24.0000");
    }

    [Fact]
    public void Conflicting_resource_identity_cannot_select_an_arbitrary_calendar()
    {
        XerDataStore store = BuildStore("TT_Rsrc", "C1", "C1", "24",
            "2026-01-31 00:00:00", "2026-02-02 00:00:00",
            new CalendarInput("C1", "24", AllDays("00:00", "24:00")),
            new CalendarInput("C2", "8", AllDays("08:00", "16:00")));
        XerTable resource = Assert.IsType<XerTable>(store.GetTable("RSRC"));
        string[] conflicting = Assert.Single(resource.Rows).Fields.ToArray();
        conflicting[resource.FieldIndexes["clndr_id"]] = "C2";
        resource.AddRow(new DataRow(conflicting, SourceToken));

        AssertUnallocated(store, "ASSIGNMENT_CONTEXT_INVALID", "24.0000");
    }

    [Fact]
    public void Inherited_holiday_and_partial_exception_control_monthly_resource_weights()
    {
        string holiday = Exception(new DateTime(2026, 1, 30));
        string shortDay = Exception(new DateTime(2026, 2, 2),
            Node("0", "s|08:00|f|10:00"));
        string week = string.Concat(Enumerable.Range(1, 7).Select(day =>
            Node(day.ToString(CultureInfo.InvariantCulture), "",
                day is >= 2 and <= 6
                    ? Node("0", "s|08:00|f|12:00") + Node("1", "s|13:00|f|17:00")
                    : "")));
        string baseData = Node("CalendarData", "", Node("DaysOfWeek", "", week),
            Node("Exceptions", "", holiday, shortDay));
        XerDataStore store = BuildStore("TT_Task", "C1", "C1", "100",
            "2026-01-29 08:00:00", "2026-02-02 17:00:00",
            new CalendarInput("C1", "8", P6TestCalendars.WorkWeek()),
            new CalendarInput("C0", "8", baseData));
        XerTable calendar = Assert.IsType<XerTable>(store.GetTable("CALENDAR"));
        calendar.Rows[0].Fields[calendar.FieldIndexes["base_clndr_id"]] = "C0";
        calendar.Rows[1].Fields[calendar.FieldIndexes["clndr_type"]] = "CA_Base";

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());
        IReadOnlyList<Dictionary<string, string>> rows = Records(table);

        Assert.Collection(rows,
            january =>
            {
                Assert.Equal("8.00", january["month_working_hours"]);
                Assert.Equal("80.0000", january["monthly_quantity"]);
            },
            february =>
            {
                Assert.Equal("2.00", february["month_working_hours"]);
                Assert.Equal("20.0000", february["monthly_quantity"]);
            });
        Assert.All(rows, row => Assert.Equal("10.00", row["total_working_hours"]));
    }

    [Fact]
    public void Entire_nonworking_month_cannot_receive_rounding_residue()
    {
        XerDataStore store = ActualStore("TK_Complete", "0.0001", "0",
            "2026-01-31 08:00:00", "2026-03-01 16:00:00");
        string closedFebruary = string.Concat(Enumerable.Range(1, 28)
            .Select(day => Exception(new DateTime(2026, 2, day))));
        string shift = Node("0", "s|08:00|f|16:00");
        string week = string.Concat(Enumerable.Range(1, 7).Select(day =>
            Node(day.ToString(CultureInfo.InvariantCulture), "", shift)));
        SetField(store, "CALENDAR", "clndr_data", Node("CalendarData", "",
            Node("DaysOfWeek", "", week), Node("Exceptions", "", closedFebruary)));

        XerTable table = Assert.IsType<XerTable>(
            new XerTransformer(store).Create15XerResourceDistribution());
        IReadOnlyList<Dictionary<string, string>> rows = Records(table);

        Assert.Equal(0.0001m, rows.Sum(row =>
            decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
        Dictionary<string, string> february = Assert.Single(rows,
            row => row["distribution_month"] == "2026-02-01");
        Assert.Equal("0.00", february["month_working_hours"]);
        Assert.Equal("0.0000", february["monthly_quantity"]);
    }

    [Fact]
    public void Recorded_actuals_on_a_nonworking_Sunday_retain_units_without_inventing_hours()
    {
        XerDataStore store = ActualStore("TK_Complete", "1", "0.25",
            "2024-06-30 08:00:00", "2024-06-30 17:00:00");
        SetField(store, "CALENDAR", "clndr_data", P6TestCalendars.WorkWeek());

        var table = Assert.IsType<XerTable>(new XerTransformer(store).Create15XerResourceDistribution());
        var row = Assert.Single(Records(table));
        Assert.Equal("2024-06-01", row["distribution_month"]);
        Assert.Equal("1.2500", row["monthly_quantity"]);
        Assert.Equal("Actual Elapsed Time", row["distribution_type"]);
        Assert.Equal("0.00", row["month_working_hours"]);
        Assert.Equal("0.00", row["total_working_hours"]);
        Assert.Equal("0.00", row["month_working_days"]);
        Assert.Equal("0.00", row["total_working_days"]);
        Assert.Equal("1", row["total_calendar_days"]);
    }

    [Theory]
    [InlineData("2026-02-01 00:00:00")]
    [InlineData("2026-02-01 08:00:00")]
    [InlineData("2199-12-31 23:59:59")]
    public void Instant_recorded_actuals_belong_to_their_recorded_month_with_zero_duration(string instant)
    {
        XerDataStore store = ActualStore("TK_Complete", "0.00011", "0", instant, instant);
        var table = Assert.IsType<XerTable>(new XerTransformer(store).Create15XerResourceDistribution());
        var row = Assert.Single(Records(table));
        Assert.Equal(instant[..7] + "-01", row["distribution_month"]);
        Assert.Equal("0.0001", row["monthly_quantity"]);
        Assert.Equal("Actual Recorded Date", row["distribution_type"]);
        Assert.Equal(instant, row["Start"]);
        Assert.Equal(instant, row["Finish"]);
        Assert.Equal(instant, row["month_start_date"]);
        Assert.Equal(instant, row["month_end_date"]);
        Assert.Equal("0.00", row["total_working_hours"]);
        Assert.Equal("0", row["month_calendar_days"]);
        Assert.Equal("0", row["total_calendar_days"]);
    }

    [Theory]
    [InlineData("1", "0.3333", "0.6667")]
    [InlineData("0.0001", "0.0000", "0.0001")]
    public void Entirely_nonworking_actual_period_uses_elapsed_weights_and_conserves_rounded_units(
        string quantity, string januaryUnits, string februaryUnits)
    {
        // Friday 18:00 to Saturday noon: 6 elapsed hours in January, 12 in February,
        // but no scheduled work anywhere in the complete actual period.
        XerDataStore store = ActualStore("TK_Complete", quantity, "0",
            "2025-01-31 18:00:00", "2025-02-01 12:00:00");
        SetField(store, "CALENDAR", "clndr_data", P6TestCalendars.WorkWeek());
        var table = Assert.IsType<XerTable>(new XerTransformer(store).Create15XerResourceDistribution());
        var rows = Records(table);
        Assert.Equal(new[] { januaryUnits, februaryUnits }, rows.Select(row => row["monthly_quantity"]));
        Assert.All(rows, row =>
        {
            Assert.Equal("Actual Elapsed Time", row["distribution_type"]);
            Assert.Equal("0.00", row["month_working_hours"]);
            Assert.Equal("0.00", row["total_working_hours"]);
            Assert.Equal("2", row["total_calendar_days"]);
        });
    }

    [Fact]
    public void Nonworking_actual_period_ending_at_month_boundary_does_not_allocate_next_month()
    {
        XerDataStore store = ActualStore("TK_Complete", "7", "0",
            "2025-01-31 18:00:00", "2025-02-01 00:00:00");
        SetField(store, "CALENDAR", "clndr_data", P6TestCalendars.WorkWeek());
        var table = Assert.IsType<XerTable>(new XerTransformer(store).Create15XerResourceDistribution());
        var row = Assert.Single(Records(table));
        Assert.Equal("2025-01-01", row["distribution_month"]);
        Assert.Equal("7.0000", row["monthly_quantity"]);
        Assert.Equal("1", row["total_calendar_days"]);
    }

    [Theory]
    [InlineData("2024-06-30 08:00:00", "2024-06-30 17:00:00")]
    [InlineData("2024-06-30 08:00:00", "2024-06-30 08:00:00")]
    public void Actual_fallback_never_allows_remaining_units_without_working_time(string start, string finish)
    {
        XerDataStore store = BuildStore("TT_Task", "C1", "C1", "1", start, finish,
            new CalendarInput("C1", "8", P6TestCalendars.WorkWeek()));
        AssertUnallocated(store, start == finish ? "REMAINING_PERIOD_INVALID" : "REMAINING_NO_WORKING_TIME", "1.0000");
    }

    [Fact]
    public void Actual_fallback_does_not_hide_a_malformed_calendar_even_for_an_instant()
    {
        XerDataStore store = ActualStore("TK_Complete", "1", "0",
            "2026-01-01 08:00:00", "2026-01-01 08:00:00");
        SetField(store, "CALENDAR", "clndr_data", "malformed");
        AssertUnallocated(store, "RESOURCE_CALENDAR_INVALID", "1.0000", actual: true);
    }

    private static void AssertUnallocated(XerDataStore store, string code, string expectedQuantity,
        bool actual = false, int expectedCount = 1, string? field = null, string? raw = null)
    {
        var transformer = new XerTransformer(store);
        Assert.Empty(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()).Rows);
        XerTable diagnostics = transformer.CreateDataQualityTable();
        Assert.Equal(expectedCount, diagnostics.Rows.Count);
        Assert.All(diagnostics.Rows, row =>
        {
            Assert.Equal("Warning", row.Fields[diagnostics.FieldIndexes["severity"]]);
            Assert.Equal(code, row.Fields[diagnostics.FieldIndexes["issue_code"]]);
            Assert.Equal(actual ? "Actual" : "Remaining", row.Fields[diagnostics.FieldIndexes["allocation_portion"]]);
            Assert.NotEmpty(row.Fields[diagnostics.FieldIndexes["message"]]);
            if (field is not null) Assert.Equal(raw, row.Fields[diagnostics.FieldIndexes[field]]);
        });
        Assert.Equal(decimal.Parse(expectedQuantity, CultureInfo.InvariantCulture), diagnostics.Rows.Sum(row =>
            decimal.Parse(row.Fields[diagnostics.FieldIndexes[actual ? "unallocated_actual_quantity" : "unallocated_remaining_quantity"]], CultureInfo.InvariantCulture)));
        Assert.Null(transformer.GetGenerationFailure(EnhancedTableNames.XerResourceDist15));
    }

    private static XerDataStore MultiProjectStore(bool reverseTaskOrder)
    {
        XerDataStore store = BuildStore("TT_Task", "C1", "C1", "24",
            "2026-01-31 00:00:00", "2026-02-02 00:00:00",
            new CalendarInput("C1", "24", AllDays("00:00", "24:00")),
            new CalendarInput("C2", "8", AllDays("08:00", "16:00")));
        XerTable task = Assert.IsType<XerTable>(store.GetTable("TASK"));
        DataRow originalTask = Assert.Single(task.Rows);
        var secondTask = new DataRow(new[]
        {
            "T1", "P2", "C2", "TT_Task", "TK_NotStart", "B100"
        }, SourceToken);
        XerTable tasks = Table("TASK", task.Headers!);
        tasks.AddRows(reverseTaskOrder
            ? new[] { secondTask, originalTask }
            : new[] { originalTask, secondTask });
        store.AddTable(tasks);
        Assert.IsType<XerTable>(store.GetTable("PROJECT")).AddRow(new DataRow(
            new[] { "P2", "2026-02-01", "SECOND" }, SourceToken));
        Assert.IsType<XerTable>(store.GetTable("TASKRSRC")).AddRow(new DataRow(new[]
        {
            "T1", "R1", "", "", "8", "", "", "2026-01-31 00:00:00",
            "2026-02-02 00:00:00", "P2", "A2"
        }, SourceToken));
        return store;
    }

    private static string Exception(DateTime date, params string[] shifts) =>
        Node("0", "d|" + date.ToOADate().ToString(CultureInfo.InvariantCulture), shifts);

    private static XerDataStore ActualStore(string status, string regular, string overtime,
        string start, string finish)
    {
        XerDataStore store = BuildStore("TT_Task", "C1", "C1", "0", "", "",
            new CalendarInput("C1", "8", AllDays("08:00", "16:00")));
        SetField(store, "TASK", "status_code", status);
        SetField(store, "TASKRSRC", "act_reg_qty", regular);
        SetField(store, "TASKRSRC", "act_ot_qty", overtime);
        SetField(store, "TASKRSRC", "act_start_date", start);
        SetField(store, "TASKRSRC", "act_end_date", finish);
        return store;
    }

    private static void SetField(XerDataStore store, string tableName, string field, string value)
    {
        XerTable table = Assert.IsType<XerTable>(store.GetTable(tableName));
        Assert.Single(table.Rows).Fields[table.FieldIndexes[field]] = value;
    }

    private static XerDataStore BuildStore(
        string taskType,
        string taskCalendarId,
        string resourceCalendarId,
        string quantity,
        string start,
        string finish,
        params CalendarInput[] calendars)
    {
        var store = new XerDataStore();

        XerTable task = Table("TASK",
            "task_id", "proj_id", "clndr_id", "task_type", "status_code", "task_code");
        task.AddRow(new DataRow(new[]
        {
            "T1", "P1", taskCalendarId, taskType, "TK_NotStart", "A100"
        }, SourceToken));
        store.AddTable(task);

        XerTable project = Table("PROJECT", "proj_id", "last_recalc_date", "proj_short_name");
        project.AddRow(new DataRow(new[] { "P1", "2026-01-01", "PROJECT" }, SourceToken));
        store.AddTable(project);

        XerTable resource = Table("RSRC",
            "rsrc_id", "rsrc_short_name", "rsrc_name", "rsrc_type", "unit_id", "clndr_id");
        resource.AddRow(new DataRow(new[]
        {
            "R1", "LAB", "Labour", "RT_Labor", "", resourceCalendarId
        }, SourceToken));
        store.AddTable(resource);

        XerTable assignments = Table("TASKRSRC",
            "task_id", "rsrc_id", "act_reg_qty", "act_ot_qty", "remain_qty",
            "act_start_date", "act_end_date", "restart_date", "reend_date",
            "proj_id", "taskrsrc_id");
        assignments.AddRow(new DataRow(new[]
        {
            "T1", "R1", "", "", quantity, "", "", start, finish, "P1", "A1"
        }, SourceToken));
        store.AddTable(assignments);

        XerTable calendar = Table("CALENDAR",
            "clndr_id", "clndr_name", "clndr_type", "base_clndr_id", "day_hr_cnt", "clndr_data");
        foreach (CalendarInput input in calendars)
        {
            calendar.AddRow(new DataRow(new[]
            {
                input.Id, input.Id, "CA_Project", "", input.HoursPerDay, input.Data
            }, SourceToken));
        }
        store.AddTable(calendar);

        return store;
    }

    private static XerTable Table(string name, params string[] headers)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        return table;
    }

    private static IReadOnlyList<Dictionary<string, string>> Records(XerTable table)
    {
        string[] headers = table.Headers
            ?? throw new InvalidOperationException($"Table '{table.Name}' has no headers.");
        return table.Rows
            .Select(row => headers.Select((header, index) => (header, row.Fields[index]))
                .ToDictionary(pair => pair.header, pair => pair.Item2,
                    StringComparer.OrdinalIgnoreCase))
            .OrderBy(row => row["distribution_month"], StringComparer.Ordinal)
            .ToArray();
    }

    private static string AllDays(string start, string finish)
    {
        string shift = Node("0", $"s|{start}|f|{finish}");
        string days = string.Concat(Enumerable.Range(1, 7)
            .Select(day => Node(day.ToString(CultureInfo.InvariantCulture), "", shift)));
        return Node("CalendarData", "", Node("DaysOfWeek", "", days), Node("Exceptions", ""));
    }

    private static string Node(string name, string attributes, params string[] children) =>
        "(0||" + name + "(" + attributes + ")(" + string.Concat(children) + "))";

    private readonly record struct CalendarInput(string Id, string HoursPerDay, string Data);
}
