using System.Globalization;

namespace XerToCsvConverter.Core.Tests;

public sealed class ResourceCurveCalculationTests
{
    private const string Source = "curve-source-occurrence-1";
    private const string CurveId = "CURVE1";

    [Theory]
    [InlineData("DT_FixedDrtn")]
    [InlineData("DT_FixedDUR2")]
    public void Linear_curve_preserves_uniform_calendar_spread_and_diagnostics(string durationType)
    {
        XerDataStore uniform = Store(curveId: "", quantity: "1",
            start: "2026-01-31 00:00:00", finish: "2026-03-02 00:00:00");
        XerDataStore curved = Store(Linear(), quantity: "1",
            start: "2026-01-31 00:00:00", finish: "2026-03-02 00:00:00");
        Set(curved, "TASK", "duration_type", durationType);

        AssertEquivalentExceptMethod(Transform(uniform), Transform(curved));
    }

    [Fact]
    public void Unassigned_curve_keeps_existing_uniform_method_and_values()
    {
        Dictionary<string, string>[] rows = Transform(Store(curveId: ""));

        AssertQuantities(rows, "50.0000", "50.0000");
        Assert.All(rows, row => Assert.Equal("Working Hours", row["distribution_type"]));
    }

    [Theory]
    [InlineData(false, "80.0000", "20.0000")]
    [InlineData(true, "20.0000", "80.0000")]
    public void Period_percentages_are_allocations_not_cumulative_values_or_curve_heights(
        bool backLoaded, string january, string february)
    {
        Dictionary<string, string>[] rows = Transform(Store(Loaded(backLoaded)));

        AssertQuantities(rows, january, february);
        Assert.All(rows, row =>
        {
            Assert.Equal("24.00", row["month_working_hours"]);
            Assert.Equal("48.00", row["total_working_hours"]);
        });
    }

    [Theory]
    [InlineData(false, "0.0000", "100.0000")]
    [InlineData(true, "100.0000", "0.0000")]
    public void Zero_usage_bands_remain_zero_despite_available_working_hours(
        bool frontLoaded, string january, string february)
    {
        decimal[] points = Points(index => frontLoaded == (index <= 10) ? 10m : 0m);

        Dictionary<string, string>[] rows = Transform(Store(points));

        AssertQuantities(rows, january, february);
        Assert.All(rows, row => Assert.Equal("24.00", row["month_working_hours"]));
    }

    [Fact]
    public void Month_boundary_inside_a_five_percent_band_gets_only_its_work_time_fraction()
    {
        // January contains 2/16 = 12.5% of working duration: two full 20% usage
        // bands and half of the third 20% band, hence 50% of the quantity.
        decimal[] points = Points(index => index is 1 or 2 or 3 ? 20m : index == 20 ? 40m : 0m);
        Dictionary<string, string>[] rows = Transform(Store(points,
            start: "2026-01-31 22:00:00", finish: "2026-02-01 14:00:00"));

        AssertQuantities(rows, "50.0000", "50.0000");
        Assert.Equal("2.00", rows[0]["month_working_hours"]);
        Assert.Equal("14.00", rows[1]["month_working_hours"]);
    }

    [Fact]
    public void Split_shifts_and_weekends_advance_the_curve_only_during_working_time()
    {
        XerDataStore store = Store(Loaded(), start: "2026-01-30 11:00:00",
            finish: "2026-02-02 14:00:00", calendar: P6TestCalendars.WorkWeek(), hoursPerDay: "10");

        Dictionary<string, string>[] rows = Transform(store);

        AssertQuantities(rows, "80.0000", "20.0000");
        Assert.All(rows, row =>
        {
            Assert.Equal("5.00", row["month_working_hours"]);
            Assert.Equal("10.00", row["total_working_hours"]);
            Assert.Equal("0.50", row["month_working_days"]);
        });
    }

    [Fact]
    public void Overnight_month_boundary_allocates_partial_curve_bands_without_duplicate_hours()
    {
        decimal[] points = Points(index => index is 1 or 2 or 3 ? 20m : index == 20 ? 40m : 0m);
        XerDataStore store = Store(points, start: "2026-01-31 22:00:00",
            finish: "2026-02-02 06:00:00", calendar: AllDays("22:00", "06:00"), hoursPerDay: "8");

        Dictionary<string, string>[] rows = Transform(store);

        AssertQuantities(rows, "50.0000", "50.0000");
        Assert.Equal("2.00", rows[0]["month_working_hours"]);
        Assert.Equal("14.00", rows[1]["month_working_hours"]);
        Assert.All(rows, row => Assert.Equal("16.00", row["total_working_hours"]));
    }

    [Fact]
    public void Inherited_holiday_and_short_day_control_curve_progress_not_elapsed_duration()
    {
        string week = string.Concat(Enumerable.Range(1, 7).Select(day => Node(
            day.ToString(CultureInfo.InvariantCulture), "", day is >= 2 and <= 6
                ? Node("0", "s|08:00|f|12:00") + Node("1", "s|13:00|f|17:00") : "")));
        string exceptions = Exception(new DateTime(2026, 1, 30))
            + Exception(new DateTime(2026, 2, 2), Node("0", "s|08:00|f|10:00"));
        XerDataStore store = Store(Loaded(), start: "2026-01-29 08:00:00",
            finish: "2026-02-02 17:00:00", calendar: P6TestCalendars.WorkWeek(), hoursPerDay: "8");
        Set(store, "CALENDAR", "base_clndr_id", "BASE");
        Required(store, "CALENDAR").AddRow(new DataRow(new[]
        {
            "BASE", "Base", "CA_Base", "", "8",
            Node("CalendarData", "", Node("DaysOfWeek", "", week), Node("Exceptions", "", exceptions))
        }, Source));

        Dictionary<string, string>[] rows = Transform(store);

        // 8/10 of the working duration consumes ten 8% and six 2% usage bands.
        AssertQuantities(rows, "92.0000", "8.0000");
        Assert.Equal("8.00", rows[0]["month_working_hours"]);
        Assert.Equal("2.00", rows[1]["month_working_hours"]);
    }

    [Fact]
    public void Resource_dependent_assignment_uses_its_resource_calendar_for_curve_progress()
    {
        XerDataStore store = Store(Loaded(), start: "2026-01-31 12:00:00",
            finish: "2026-02-01 16:00:00");
        Set(store, "TASK", "task_type", "TT_Rsrc");
        Set(store, "RSRC", "clndr_id", "RESOURCE");
        Required(store, "CALENDAR").AddRow(new DataRow(new[]
        {
            "RESOURCE", "Resource", "CA_Rsrc", "", "8", AllDays("08:00", "16:00")
        }, Source));

        Dictionary<string, string>[] rows = Transform(store);

        AssertQuantities(rows, "53.3333", "46.6667");
        Assert.Equal("4.00", rows[0]["month_working_hours"]);
        Assert.Equal("8.00", rows[1]["month_working_hours"]);
        Assert.All(rows, row => Assert.Equal($"{Source}.RESOURCE", row["clndr_id_key"]));
    }

    [Theory]
    [InlineData("8")]
    [InlineData("10")]
    [InlineData("24")]
    public void Hours_per_day_conversion_does_not_change_curve_quantities(string hoursPerDay)
    {
        Dictionary<string, string>[] rows = Transform(Store(Loaded(), hoursPerDay: hoursPerDay));

        AssertQuantities(rows, "80.0000", "20.0000");
    }

    [Theory]
    [InlineData("1", "1.0000")]
    [InlineData("0.0001", "0.0001")]
    [InlineData("0.00015", "0.0002")]
    public void Cumulative_curve_rounding_conserves_quantity_at_four_decimal_places(
        string quantity, string expectedTotal)
    {
        Dictionary<string, string>[] rows = Transform(Store(Loaded(), quantity: quantity,
            start: "2026-01-31 00:00:00", finish: "2026-03-02 00:00:00"));

        Assert.Equal(decimal.Parse(expectedTotal, CultureInfo.InvariantCulture), rows.Sum(Quantity));
        Assert.All(rows, row => Assert.True(Quantity(row) >= 0));
        if (quantity == "1") AssertQuantities(rows, "0.0533", "0.9334", "0.0133");
    }

    [Theory]
    [InlineData("fr-FR")]
    [InlineData("th-TH")]
    public void Curve_percentages_quantities_and_month_dates_use_invariant_culture(string culture)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo previousUi = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo(culture);
            CultureInfo.CurrentUICulture = new CultureInfo(culture);
            decimal[] points = Points(index => index <= 10 ? 7.5m : 2.5m);

            Dictionary<string, string>[] rows = Transform(Store(points, quantity: "10.5"));

            AssertQuantities(rows, "7.8750", "2.6250");
            Assert.Equal(new[] { "2026-01-01", "2026-02-01" }, rows.Select(row => row["distribution_month"]));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
            CultureInfo.CurrentUICulture = previousUi;
        }
    }

    [Theory]
    [InlineData("TK_Active")]
    [InlineData("TK_Complete")]
    public void Assignment_curve_does_not_rewrite_actual_only_history_estimate(string status)
    {
        XerDataStore uniform = ActualStore(status, "");
        XerDataStore curved = ActualStore(status, CurveId);

        Dictionary<string, string>[] expected = Transform(uniform);
        Dictionary<string, string>[] actual = Transform(curved);

        AssertEquivalentExceptMethod(expected, actual);
        AssertQuantities(actual, "50.0000", "50.0000");
        Assert.All(actual, row =>
        {
            Assert.Equal("1", row["is_actual"]);
            Assert.Equal("Working Hours", row["distribution_type"]);
        });
    }

    [Fact]
    public void Repeated_native_curve_assignment_and_task_ids_are_isolated_by_source_occurrence()
    {
        const string secondSource = "curve-source-occurrence-2";
        XerDataStore store = Store(Loaded());
        XerDataStore second = Store(Loaded(backLoaded: true));
        foreach (string tableName in second.TableNames)
        {
            XerTable destination = Required(store, tableName);
            foreach (DataRow row in Required(second, tableName).Rows)
                destination.AddRow(new DataRow(row.Fields.ToArray(), secondSource));
        }

        Dictionary<string, string>[] rows = Transform(store);

        Assert.Equal(4, rows.Length);
        AssertQuantities(rows.Where(row => row["task_id_key"] == $"{Source}.T1").ToArray(), "80.0000", "20.0000");
        AssertQuantities(rows.Where(row => row["task_id_key"] == $"{secondSource}.T1").ToArray(), "20.0000", "80.0000");
    }

    [Fact]
    public void A_curve_in_another_source_cannot_satisfy_a_missing_curve_reference()
    {
        XerDataStore store = Store();
        AddCurve(store, Loaded(), source: "different-source-occurrence");

        AssertRejected(store);
    }

    [Fact]
    public void Assigned_curve_missing_its_data_table_fails_instead_of_spreading_uniformly()
    {
        AssertRejected(Store());
    }

    [Fact]
    public void Unknown_curve_id_fails_instead_of_using_the_only_available_curve()
    {
        XerDataStore store = Store(Loaded());
        Set(store, "TASKRSRC", "curv_id", "UNKNOWN");

        AssertRejected(store);
    }

    [Fact]
    public void Duplicate_curve_identity_fails_instead_of_using_first_or_last_row()
    {
        XerDataStore store = Store(Loaded());
        XerTable curves = Required(store, "RSRCCURVDATA");
        curves.AddRow(new DataRow(Assert.Single(curves.Rows).Fields.ToArray(), Source));

        AssertRejected(store);
    }

    [Theory]
    [InlineData("")]
    [InlineData("NaN")]
    [InlineData("Infinity")]
    [InlineData("-Infinity")]
    [InlineData("-1")]
    [InlineData("101")]
    [InlineData("5,0")]
    [InlineData("not-a-number")]
    [InlineData("1e100")]
    public void Invalid_curve_usage_percentage_fails_instead_of_becoming_zero(string value)
    {
        XerDataStore store = Store(Linear());
        Set(store, "RSRCCURVDATA", "pct_usage_7", value);

        AssertRejected(store);
    }

    [Theory]
    [InlineData("4")]
    [InlineData("6")]
    public void Curve_usage_total_other_than_one_hundred_is_not_silently_normalized(string firstPoint)
    {
        XerDataStore store = Store(Linear());
        Set(store, "RSRCCURVDATA", "pct_usage_1", firstPoint);

        AssertRejected(store);
    }

    [Fact]
    public void All_zero_curve_cannot_receive_an_arbitrary_final_month_remainder()
    {
        AssertRejected(Store(new decimal[21]));
    }

    [Fact]
    public void Nonzero_zero_percent_actual_usage_point_is_not_redistributed_as_remaining_work()
    {
        decimal[] points = Linear();
        points[0] = 1;
        points[20] = 4; // Still totals 100: fail for the unsupported actual-usage point.

        AssertRejected(Store(points));
    }

    [Fact]
    public void Missing_curve_band_is_not_assumed_to_have_zero_usage()
    {
        XerDataStore store = Store(Linear());
        XerTable original = Required(store, "RSRCCURVDATA");
        int omitted = original.FieldIndexes["pct_usage_20"];
        XerTable replacement = Table("RSRCCURVDATA", original.Headers!
            .Where((_, index) => index != omitted).ToArray());
        replacement.AddRow(new DataRow(Assert.Single(original.Rows).Fields
            .Where((_, index) => index != omitted).ToArray(), Source));
        store.AddTable(replacement);

        AssertRejected(store);
    }

    [Theory]
    [InlineData("DT_FixedQty")]
    [InlineData("DT_FixedRate")]
    [InlineData("UNKNOWN")]
    [InlineData("")]
    public void Assigned_curve_on_unsupported_duration_type_does_not_produce_plausible_estimates(string durationType)
    {
        XerDataStore store = Store(Loaded());
        Set(store, "TASK", "duration_type", durationType);

        AssertRejected(store);
    }

    [Theory]
    [InlineData("")]
    [InlineData(CurveId)]
    public void Malformed_manual_remaining_profile_is_not_silently_overwritten_by_uniform_or_named_curve(string curveId)
    {
        XerDataStore store = Store(Loaded(), curveId: curveId);
        Set(store, "TASKRSRC", "remain_crv", "opaque-manual-future-period-profile");

        AssertRejected(store);
    }

    [Theory]
    [InlineData("TK_NotStart")]
    [InlineData("TK_Active")]
    public void Manual_remaining_profile_overrides_missing_named_curve_and_retains_its_own_phase(string status)
    {
        XerDataStore store = Store(curveId: "MISSING_NAMED_CURVE");
        Set(store, "TASK", "status_code", status);
        Set(store, "TASKRSRC", "remain_crv", "25:24;75:24");

        Dictionary<string, string>[] rows = Transform(store);

        AssertQuantities(rows, "25.0000", "75.0000");
        Assert.All(rows, row => Assert.Equal("Remaining Units Profile", row["distribution_type"]));
    }

    [Fact]
    public void Manual_zero_quantity_band_preserves_a_working_time_gap()
    {
        XerDataStore store = Store(curveId: "");
        Set(store, "TASKRSRC", "remain_crv", "0:24;100:24");

        Dictionary<string, string>[] rows = Transform(store);

        AssertQuantities(rows, "0.0000", "100.0000");
        Assert.All(rows, row => Assert.Equal("24.00", row["month_working_hours"]));
    }

    [Fact]
    public void Manual_profile_band_crossing_month_boundary_is_prorated_in_working_time()
    {
        XerDataStore store = Store(curveId: "", start: "2026-01-31 22:00:00", finish: "2026-02-01 14:00:00");
        Set(store, "TASKRSRC", "remain_crv", "20:0.8;20:0.8;20:0.8;40:13.6");

        AssertQuantities(Transform(store), "50.0000", "50.0000");
    }

    [Theory]
    [InlineData("not-a-profile")]
    [InlineData("1:")]
    [InlineData("-1:48")]
    [InlineData("100:0")]
    [InlineData("NaN:48")]
    [InlineData("100:Infinity")]
    [InlineData("100:24")]
    [InlineData("99:48")]
    [InlineData("50:24;50:23.5")]
    [InlineData("100,48")]
    public void Invalid_or_unreconciled_manual_profile_fails_instead_of_falling_back(string profile)
    {
        XerDataStore store = Store(Loaded());
        Set(store, "TASKRSRC", "remain_crv", profile);

        AssertRejected(store);
    }

    [Fact]
    public void Active_assignment_spanning_months_without_manual_profile_does_not_restart_a_nonlinear_curve()
    {
        XerDataStore store = Store(Loaded());
        Set(store, "TASK", "status_code", "TK_Active");

        AssertRejected(store);
    }

    [Theory]
    [InlineData("2026-08-26 08:00:00", "2026-08-26 17:00:00", "2026-08-01", "8.00")]
    [InlineData("2026-08-26 08:00:00", "2026-09-01 00:00:00", "2026-08-01", "32.00")]
    [InlineData("2026-12-31 08:00:00", "2027-01-01 00:00:00", "2026-12-01", "8.00")]
    public void Active_nonlinear_curve_contained_in_one_half_open_month_has_phase_independent_monthly_total(
        string start, string finish, string month, string workingHours)
    {
        // The intramonth distribution is unknown, but its entire remaining
        // quantity belongs to this one month for every possible curve phase.
        // An exclusive finish at next month's midnight does not add a bucket.
        XerDataStore store = Store(Loaded(), quantity: "20", start: start, finish: finish,
            calendar: P6TestCalendars.WorkWeek(), hoursPerDay: "8");
        Set(store, "TASK", "status_code", "TK_Active");
        Set(store, "TASKRSRC", "act_start_date", "2026-01-01 08:00:00");

        Dictionary<string, string> row = Assert.Single(Transform(store));

        Assert.Equal("20.0000", row["monthly_quantity"]);
        Assert.Equal(month, row["distribution_month"]);
        Assert.Equal("Resource Curve", row["distribution_type"]);
        Assert.Equal("0", row["is_actual"]);
        Assert.Equal(workingHours, row["month_working_hours"]);
        Assert.Equal(workingHours, row["total_working_hours"]);
        Assert.Equal(start, row["Start"]);
        Assert.Equal(finish, row["Finish"]);
    }

    [Fact]
    public void Assignment_actual_start_also_allows_only_a_phase_independent_single_month_total()
    {
        XerDataStore store = Store(Loaded(backLoaded: true), quantity: "20",
            start: "2026-08-26 08:00:00", finish: "2026-08-26 17:00:00");
        // The exported assignment can carry progress independently of its task.
        Set(store, "TASKRSRC", "act_start_date", "2026-08-01 08:00:00");

        Assert.Equal("20.0000", Assert.Single(Transform(store))["monthly_quantity"]);

        Set(store, "TASKRSRC", "reend_date", "2026-09-02 17:00:00");
        AssertRejected(store);
    }

    [Theory]
    [InlineData("2026-08-31 23:00:00", "2026-09-01 00:00:01")]
    [InlineData("2026-12-31 23:00:00", "2027-01-01 00:00:01")]
    [InlineData("2026-01-01 00:00:00", "2027-01-02 00:00:00")]
    public void Active_nonlinear_curve_with_work_in_more_than_one_month_still_requires_remaining_profile(
        string start, string finish)
    {
        XerDataStore store = Store(Loaded(), quantity: "20", start: start, finish: finish);
        Set(store, "TASK", "status_code", "TK_Active");

        AssertRejected(store);
    }

    [Theory]
    [InlineData("invalid percentage")]
    [InlineData("invalid total")]
    [InlineData("missing curve")]
    [InlineData("unsupported duration")]
    [InlineData("manual curve without profile")]
    [InlineData("malformed manual profile")]
    [InlineData("invalid calendar")]
    [InlineData("no working time")]
    [InlineData("zero duration")]
    public void Single_month_phase_independence_does_not_bypass_required_curve_calendar_and_period_validation(
        string defect)
    {
        XerDataStore store = Store(Loaded(), quantity: "20",
            start: "2026-08-26 08:00:00", finish: "2026-08-26 17:00:00");
        Set(store, "TASK", "status_code", "TK_Active");
        switch (defect)
        {
            case "invalid percentage": Set(store, "RSRCCURVDATA", "pct_usage_7", "NaN"); break;
            case "invalid total": Set(store, "RSRCCURVDATA", "pct_usage_7", "1"); break;
            case "missing curve": Set(store, "TASKRSRC", "curv_id", "MISSING"); break;
            case "unsupported duration": Set(store, "TASK", "duration_type", "DT_FixedQty"); break;
            case "manual curve without profile":
                Set(store, "TASKRSRC", "curv_id", "9");
                Set(store, "RSRCCURVDATA", "curv_id", "9");
                break;
            case "malformed manual profile": Set(store, "TASKRSRC", "remain_crv", "not-a-profile"); break;
            case "invalid calendar": Set(store, "CALENDAR", "clndr_data", "malformed"); break;
            case "no working time": Set(store, "CALENDAR", "clndr_data", AllDays("18:00", "19:00")); break;
            case "zero duration": Set(store, "TASKRSRC", "reend_date", "2026-08-26 08:00:00"); break;
            default: throw new InvalidOperationException(defect);
        }

        if (defect is "invalid calendar" or "no working time")
            Assert.Null(new XerTransformer(store).Create15XerResourceDistribution());
        else
            AssertRejected(store);
    }

    [Fact]
    public void Active_assignment_can_use_phase_independent_linear_curve()
    {
        XerDataStore store = Store(Linear());
        Set(store, "TASK", "status_code", "TK_Active");

        AssertQuantities(Transform(store), "50.0000", "50.0000");
    }

    [Fact]
    public void Small_export_precision_residue_in_percentage_sum_is_normalized_without_quantity_loss()
    {
        decimal[] points = Linear();
        points[1] = 5.0004m;

        Dictionary<string, string>[] rows = Transform(Store(points));

        AssertQuantities(rows, "50.0002", "49.9998");
        Assert.Equal(100m, rows.Sum(Quantity));
    }

    [Fact]
    public void Manual_profile_accepts_an_optional_trailing_semicolon()
    {
        XerDataStore store = Store(curveId: "");
        Set(store, "TASKRSRC", "remain_crv", "25:24;75:24;");

        AssertQuantities(Transform(store), "25.0000", "75.0000");
    }

    [Fact]
    public void Empty_internal_manual_band_is_invalid_not_an_optional_delimiter()
    {
        XerDataStore store = Store(curveId: "");
        Set(store, "TASKRSRC", "remain_crv", "25:24;;75:24");

        AssertRejected(store);
    }

    [Fact]
    public void Manual_curve_id_nine_requires_remaining_profile_even_when_numeric_definition_exists()
    {
        XerDataStore store = Store(Loaded(), curveId: "9");
        Set(store, "RSRCCURVDATA", "curv_id", "9");

        AssertRejected(store);
    }

    [Fact]
    public void Manual_curve_id_nine_uses_remaining_profile_not_its_numeric_definition()
    {
        XerDataStore store = Store(Loaded(), curveId: "9");
        Set(store, "RSRCCURVDATA", "curv_id", "9");
        Set(store, "TASKRSRC", "remain_crv", "25:24;75:24");

        Dictionary<string, string>[] rows = Transform(store);

        AssertQuantities(rows, "25.0000", "75.0000");
        Assert.All(rows, row => Assert.Equal("Remaining Units Profile", row["distribution_type"]));
    }

    [Fact]
    public void Unassigned_malformed_curve_data_does_not_change_uniform_spread()
    {
        XerDataStore store = Store(Linear(), curveId: "");
        Set(store, "RSRCCURVDATA", "pct_usage_7", "malformed-unused-value");

        AssertQuantities(Transform(store), "50.0000", "50.0000");
    }

    [Fact]
    public void Unused_malformed_curve_does_not_block_a_different_valid_assigned_curve()
    {
        XerDataStore store = Store(Loaded());
        XerTable curves = Required(store, "RSRCCURVDATA");
        string[] unused = Assert.Single(curves.Rows).Fields.ToArray();
        unused[curves.FieldIndexes["curv_id"]] = "UNUSED";
        unused[curves.FieldIndexes["pct_usage_7"]] = "malformed-unused-value";
        curves.AddRow(new DataRow(unused, Source));

        AssertQuantities(Transform(store), "80.0000", "20.0000");
    }

    [Theory]
    [InlineData("0.0000000000000000000000000001", "0.0000")]
    [InlineData("0.0001", "0.0001")]
    [InlineData("0.00015", "0.0002")]
    public void Tiny_named_curve_quantity_does_not_leak_rounding_into_zero_usage_trailing_months(
        string quantity, string expectedTotal)
    {
        // All usage is in the first 5% of 31 working days, entirely in January.
        decimal[] points = Points(index => index == 1 ? 100m : 0m);
        Dictionary<string, string>[] rows = Transform(Store(points, quantity: quantity,
            start: "2026-01-30 00:00:00", finish: "2026-03-02 00:00:00"));

        AssertQuantities(rows, expectedTotal, "0.0000", "0.0000");
        Assert.Equal(decimal.Parse(expectedTotal, CultureInfo.InvariantCulture), rows.Sum(Quantity));
    }

    [Theory]
    [InlineData("0.0000000000000000000000000001", "0.0000")]
    [InlineData("0.0001", "0.0001")]
    [InlineData("0.00015", "0.0002")]
    public void Tiny_manual_profile_quantity_does_not_leak_rounding_into_zero_usage_trailing_months(
        string quantity, string expectedTotal)
    {
        XerDataStore store = Store(curveId: "", quantity: quantity,
            start: "2026-01-30 00:00:00", finish: "2026-03-02 00:00:00");
        Set(store, "TASKRSRC", "remain_crv", quantity + ":24;0:720");

        Dictionary<string, string>[] rows = Transform(store);

        AssertQuantities(rows, expectedTotal, "0.0000", "0.0000");
        Assert.Equal(decimal.Parse(expectedTotal, CultureInfo.InvariantCulture), rows.Sum(Quantity));
    }

    [Fact]
    public void Active_assignment_manual_remaining_profile_keeps_actual_period_uniform_and_independent()
    {
        XerDataStore store = Store(curveId: "9", start: "2026-02-28 00:00:00",
            finish: "2026-03-02 00:00:00");
        Set(store, "TASK", "status_code", "TK_Active");
        Set(store, "TASKRSRC", "act_reg_qty", "75");
        Set(store, "TASKRSRC", "act_ot_qty", "25");
        Set(store, "TASKRSRC", "act_start_date", "2026-01-31 00:00:00");
        Set(store, "TASKRSRC", "act_end_date", "2026-02-02 00:00:00");
        Set(store, "TASKRSRC", "remain_crv", "25:24;75:24");

        Dictionary<string, string>[] rows = Transform(store);
        Dictionary<string, string>[] actual = rows.Where(row => row["is_actual"] == "1").ToArray();
        Dictionary<string, string>[] remaining = rows.Where(row => row["is_actual"] == "0").ToArray();

        Assert.Equal(4, rows.Length);
        AssertQuantities(actual, "50.0000", "50.0000");
        AssertQuantities(remaining, "25.0000", "75.0000");
        Assert.All(actual, row =>
        {
            Assert.Equal("Working Hours", row["distribution_type"]);
            Assert.Equal("2026-01-31 00:00:00", row["Start"]);
            Assert.Equal("2026-02-02 00:00:00", row["Finish"]);
        });
        Assert.All(remaining, row =>
        {
            Assert.Equal("Remaining Units Profile", row["distribution_type"]);
            Assert.Equal("2026-02-28 00:00:00", row["Start"]);
            Assert.Equal("2026-03-02 00:00:00", row["Finish"]);
        });
    }

    private static XerDataStore ActualStore(string status, string curveId)
    {
        XerDataStore store = Store(Loaded(), curveId: curveId, quantity: "0");
        Set(store, "TASK", "status_code", status);
        Set(store, "TASKRSRC", "act_reg_qty", "75");
        Set(store, "TASKRSRC", "act_ot_qty", "25");
        Set(store, "TASKRSRC", "act_start_date", "2026-01-31 00:00:00");
        Set(store, "TASKRSRC", "act_end_date", "2026-02-02 00:00:00");
        return store;
    }

    private static XerDataStore Store(decimal[]? points = null, string curveId = CurveId,
        string quantity = "100", string start = "2026-01-31 00:00:00",
        string finish = "2026-02-02 00:00:00", string? calendar = null, string hoursPerDay = "24")
    {
        var store = new XerDataStore();
        Add("TASK", new[] { "task_id", "proj_id", "clndr_id", "task_type", "status_code", "task_code", "duration_type" },
            new[] { "T1", "P1", "C1", "TT_Task", "TK_NotStart", "ACT100", "DT_FixedDrtn" });
        Add("PROJECT", new[] { "proj_id", "last_recalc_date", "proj_short_name" },
            new[] { "P1", "2026-02-02 00:00:00", "PROJECT" });
        Add("RSRC", new[] { "rsrc_id", "rsrc_short_name", "rsrc_name", "rsrc_type", "unit_id", "clndr_id" },
            new[] { "R1", "LAB", "Labour", "RT_Labor", "", "C1" });
        Add("TASKRSRC", new[]
            {
                "task_id", "rsrc_id", "proj_id", "taskrsrc_id", "act_reg_qty", "act_ot_qty", "remain_qty",
                "act_start_date", "act_end_date", "restart_date", "reend_date", "curv_id", "remain_crv"
            }, new[] { "T1", "R1", "P1", "A1", "", "", quantity, "", "", start, finish, curveId, "" });
        Add("CALENDAR", new[] { "clndr_id", "clndr_name", "clndr_type", "base_clndr_id", "day_hr_cnt", "clndr_data" },
            new[] { "C1", "Calendar", "CA_Project", "", hoursPerDay, calendar ?? AllDays("00:00", "24:00") });
        if (points is not null) AddCurve(store, points);
        return store;

        void Add(string name, string[] headers, string[] values)
        {
            XerTable table = Table(name, headers);
            table.AddRow(new DataRow(values, Source));
            store.AddTable(table);
        }
    }

    private static void AddCurve(XerDataStore store, decimal[] points, string source = Source)
    {
        Assert.Equal(21, points.Length);
        XerTable table = Table("RSRCCURVDATA", new[] { "curv_id" }.Concat(
            Enumerable.Range(0, 21).Select(index => "pct_usage_" + index.ToString(CultureInfo.InvariantCulture))).ToArray());
        table.AddRow(new DataRow(new[] { CurveId }.Concat(points.Select(
            point => point.ToString(CultureInfo.InvariantCulture))).ToArray(), source));
        store.AddTable(table);
    }

    private static decimal[] Linear() => Points(_ => 5m);
    private static decimal[] Loaded(bool backLoaded = false) => Points(index =>
        (index <= 10) != backLoaded ? 8m : 2m);
    private static decimal[] Points(Func<int, decimal> usage) =>
        Enumerable.Range(0, 21).Select(index => index == 0 ? 0m : usage(index)).ToArray();
    private static XerTable Required(XerDataStore store, string name) => Assert.IsType<XerTable>(store.GetTable(name));
    private static void Set(XerDataStore store, string tableName, string field, string value)
    {
        XerTable table = Required(store, tableName);
        Assert.Single(table.Rows).Fields[table.FieldIndexes[field]] = value;
    }

    private static Dictionary<string, string>[] Transform(XerDataStore store)
    {
        XerTable table = Assert.IsType<XerTable>(new XerTransformer(store).Create15XerResourceDistribution());
        return table.Rows.Select(row => table.Headers!.Select((header, index) => (header, row.Fields[index]))
            .ToDictionary(pair => pair.header, pair => pair.Item2, StringComparer.OrdinalIgnoreCase))
            .OrderBy(row => row["distribution_month"], StringComparer.Ordinal).ToArray();
    }

    private static void AssertQuantities(Dictionary<string, string>[] rows, params string[] quantities) =>
        Assert.Equal(quantities, rows.Select(row => row["monthly_quantity"]).ToArray());
    private static decimal Quantity(Dictionary<string, string> row) =>
        decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture);
    private static void AssertRejected(XerDataStore store)
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() =>
            new XerTransformer(store).Create15XerResourceDistribution());
        Assert.Contains(Source, exception.Message, StringComparison.Ordinal);
        Assert.Contains("A1", exception.Message, StringComparison.Ordinal);
    }
    private static void AssertEquivalentExceptMethod(Dictionary<string, string>[] expected, Dictionary<string, string>[] actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (int index = 0; index < expected.Length; index++)
        {
            Assert.Equal(expected[index].Keys.OrderBy(key => key), actual[index].Keys.OrderBy(key => key));
            foreach (string key in expected[index].Keys.Where(key => key != "distribution_type"))
                Assert.Equal(expected[index][key], actual[index][key]);
        }
    }

    private static XerTable Table(string name, params string[] headers)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        return table;
    }
    private static string AllDays(string start, string finish) => Node("CalendarData", "",
        Node("DaysOfWeek", "", string.Concat(Enumerable.Range(1, 7).Select(day =>
            Node(day.ToString(CultureInfo.InvariantCulture), "", Node("0", $"s|{start}|f|{finish}"))))),
        Node("Exceptions", ""));
    private static string Exception(DateTime date, params string[] shifts) =>
        Node("0", "d|" + date.ToOADate().ToString(CultureInfo.InvariantCulture), shifts);
    private static string Node(string name, string attributes, params string[] children) =>
        "(0||" + name + "(" + attributes + ")(" + string.Concat(children) + "))";
}
