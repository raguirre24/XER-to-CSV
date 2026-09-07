using System.Globalization;
using System.Text.Json;

namespace XerToCsvConverter.Core.Tests;

public sealed partial class ResourceCurveCalculationTests
{
    [Theory]
    [InlineData("front", 8, "75.0000", "25.0000")]
    [InlineData("back", 8, "33.3333", "66.6667")]
    [InlineData("bell", 8, "66.6667", "33.3333")]
    [InlineData("custom", 8, "50.0000", "50.0000")]
    [InlineData("front", 9, "74.1935", "25.8065")]
    [InlineData("front", 24, "50.0000", "50.0000")]
    public void Forecast_retains_the_tail_with_independently_calculated_monthly_totals(
        string shape, int elapsedHours, string january, string february)
    {
        // Quarter quantities: bell=10/40/40/10, custom=20/20/40/20.
        // For p=1/4 and halfway through the remainder, evaluate at x=5/8.
        decimal[] curve = shape switch
        {
            "back" => Loaded(true),
            "bell" => Points(i => i <= 5 || i > 15 ? 2m : 8m),
            "custom" => Points(i => i > 10 && i <= 15 ? 8m : 4m),
            _ => Loaded()
        };
        XerDataStore store = ForecastStore(curve, elapsedHours);
        var (rows, notes) = ForecastResult(store);
        AssertQuantities(rows, january, february);
        Assert.All(rows, row => Assert.Equal("Resource Curve Forecast", row["distribution_type"]));
        AssertMethodNote(Assert.Single(notes), "REMAINING_CURVE_ESTIMATED");
        Assert.Contains($"A={elapsedHours} working hours; R=24 working hours", notes[0]["message"]);
    }

    [Fact]
    public void Forecast_uses_assignment_dates_and_ignores_the_future_restart_gap_and_frozen_plan()
    {
        XerDataStore store = ForecastStore();
        Set(store, "PROJECT", "last_recalc_date", "2026-01-30 08:00");
        Set(store, "TASKRSRC", "act_start_date", "2026-01-30 00:00");
        ForecastFields(store, "TASK", ("act_start_date", "2025-01-01"), ("target_drtn_hr_cnt", "10000"),
            ("remain_drtn_hr_cnt", "1"), ("phys_complete_pct", "99"));
        ForecastFields(store, "TASKRSRC", ("target_start_date", "2027-01-01"), ("target_end_date", "2027-12-31"));
        var (rows, notes) = ForecastResult(store);
        AssertQuantities(rows, "75.0000", "25.0000");
        Assert.Contains("p=0.25", Assert.Single(notes)["message"]);
    }

    [Theory]
    [InlineData("act_start_date", "")]
    [InlineData("act_start_date", "invalid")]
    [InlineData("act_start_date", "2026-02-01")]
    [InlineData("act_end_date", "2026-01-31 08:00")]
    [InlineData("act_end_date", "invalid")]
    [InlineData("last_recalc_date", "")]
    [InlineData("last_recalc_date", "invalid")]
    [InlineData("last_recalc_date", "2026-02-02")]
    public void Missing_or_conflicting_phase_uses_uniform_fallback(string field, string value)
    {
        XerDataStore store = ForecastStore();
        Set(store, field == "last_recalc_date" ? "PROJECT" : "TASKRSRC", field, value);
        // A blank start with positive actual units is missing evidence, not unstarted.
        if (field == "act_start_date" && value.Length == 0) Set(store, "TASKRSRC", "act_reg_qty", "1");
        var (rows, notes) = ForecastResult(store);
        AssertQuantities(rows.Where(row => row["is_actual"] == "0").ToArray(), "50.0000", "50.0000");
        Dictionary<string, string> note = Assert.Single(notes, row => row["allocation_portion"] == "");
        AssertMethodNote(note, "REMAINING_CURVE_UNIFORM_FALLBACK");
        Assert.Contains("Reason:", note["message"]);
        Assert.All(rows.Where(row => row["is_actual"] == "0"), row => Assert.Equal("Working Hours Fallback", row["distribution_type"]));
        Assert.DoesNotContain(notes, row => row["allocation_portion"] == "Remaining");
    }

    [Theory]
    [InlineData("act_start_date")]
    [InlineData("act_end_date")]
    [InlineData("act_reg_qty")]
    [InlineData("act_ot_qty")]
    public void Absent_phase_cells_do_not_become_explicit_blank_or_zero_evidence(string omittedColumn)
    {
        XerDataStore store = ForecastStore();
        Set(store, "TASKRSRC", "act_start_date", "");
        XerTable original = Required(store, "TASKRSRC");
        string[] headers = original.Headers!.Where(header => header != omittedColumn).ToArray();
        XerTable reduced = Table("TASKRSRC", headers);
        reduced.AddRow(new DataRow(headers.Select(header => original.Rows[0].Fields[original.FieldIndexes[header]]).ToArray(), Source));
        store.AddTable(reduced);
        var (rows, notes) = ForecastResult(store);
        AssertQuantities(rows, "50.0000", "50.0000");
        AssertMethodNote(Assert.Single(notes), "REMAINING_CURVE_UNIFORM_FALLBACK");
    }

    [Fact]
    public void Zero_weight_tail_uses_uniform_fallback_without_losing_remaining_units()
    {
        XerDataStore store = ForecastStore(Points(i => i <= 5 ? 20m : 0m));
        var (rows, notes) = ForecastResult(store);
        AssertQuantities(rows, "50.0000", "50.0000");
        AssertMethodNote(Assert.Single(notes), "REMAINING_CURVE_UNIFORM_FALLBACK");
        Assert.Contains("no weight remaining", notes[0]["message"]);
        Assert.Contains("p=0.25", notes[0]["message"]);
    }

    [Fact]
    public void Closed_historical_suspension_is_removed_from_elapsed_time_at_day_boundaries()
    {
        XerDataStore store = ForecastStore();
        Set(store, "TASKRSRC", "act_start_date", "2026-01-30 00:00");
        Set(store, "PROJECT", "last_recalc_date", "2026-01-31 08:00");
        ForecastFields(store, "TASK", ("suspend_date", "2026-01-30 23:00"), ("resume_date", "2026-01-31 18:00"));
        var (rows, notes) = ForecastResult(store);
        AssertQuantities(rows, "75.0000", "25.0000");
        Assert.Contains("A=8 working hours", Assert.Single(notes)["message"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Suspended_month_receives_no_units_and_raw_calendar_diagnostics_are_preserved(bool fallback)
    {
        XerDataStore store = ForecastStore();
        Set(store, "TASKRSRC", "act_start_date", fallback ? "invalid" : "2026-01-30 08:00");
        Set(store, "PROJECT", "last_recalc_date", "2026-01-31 00:00");
        Set(store, "TASKRSRC", "restart_date", "2026-01-31 00:00");
        Set(store, "TASKRSRC", "reend_date", "2026-03-02 00:00");
        ForecastFields(store, "TASK", ("suspend_date", "2026-02-01"), ("resume_date", "2026-03-01"));
        var (rows, notes) = ForecastResult(store);
        Assert.Equal(new[] { "2026-01-01", "2026-03-01" }, rows.Select(row => row["distribution_month"]));
        AssertQuantities(rows, fallback ? "50.0000" : "75.0000", fallback ? "50.0000" : "25.0000");
        Assert.All(rows, row => Assert.Equal("720.00", row["total_working_hours"]));
        Assert.Contains("R=48 working hours", Assert.Single(notes)["message"]);
    }

    [Theory]
    [InlineData("2026-01-31", "")]
    [InlineData("invalid", "2026-02-01")]
    [InlineData("", "2026-02-01")]
    [InlineData("2026-02-01", "2026-01-31")]
    [InlineData("2026-01-31", "2026-02-03")]
    public void Unresolved_or_fully_suspended_availability_is_not_bypassed_by_fallback(string suspend, string resume)
    {
        XerDataStore store = ForecastStore();
        ForecastFields(store, "TASK", ("suspend_date", suspend), ("resume_date", resume));
        AssertRejected(store);
    }

    [Fact]
    public void One_working_month_with_nonworking_calendar_endpoints_needs_no_phase()
    {
        XerDataStore store = Store(Loaded(), quantity: "20", start: "2026-01-31 00:00", finish: "2026-03-01 12:00",
            calendar: P6TestCalendars.WorkWeek(), hoursPerDay: "8");
        Set(store, "TASK", "status_code", "TK_Active");
        Set(store, "TASKRSRC", "act_start_date", "invalid");
        var (rows, notes) = ForecastResult(store);
        Assert.Empty(notes);
        Dictionary<string, string> row = Assert.Single(rows);
        Assert.Equal("2026-02-01", row["distribution_month"]);
        Assert.Equal("20.0000", row["monthly_quantity"]);
        Assert.Equal("Resource Curve", row["distribution_type"]);
    }

    [Fact]
    public void Resource_calendar_controls_forecast_progress_and_remaining_weights()
    {
        // Jan28/29 actual = 16h, Feb2/3 remaining = 16h, Jan30 remaining = 8h.
        // A=16,R=24,p=.4. At Jan end x=.6: (.84-.64)/(.36)=5/9.
        XerDataStore store = Store(Loaded(), start: "2026-01-30 08:00", finish: "2026-02-04 08:00");
        Set(store, "TASK", "status_code", "TK_Active");
        Set(store, "TASK", "task_type", "TT_Rsrc");
        Set(store, "RSRC", "clndr_id", "RESOURCE");
        Set(store, "TASKRSRC", "act_start_date", "2026-01-28 08:00");
        Set(store, "PROJECT", "last_recalc_date", "2026-01-29 17:00");
        XerTable calendars = Required(store, "CALENDAR");
        string[] resource = calendars.Rows[0].Fields.ToArray();
        resource[calendars.FieldIndexes["clndr_id"]] = "RESOURCE";
        resource[calendars.FieldIndexes["day_hr_cnt"]] = "8";
        resource[calendars.FieldIndexes["clndr_data"]] = P6TestCalendars.WorkWeek();
        calendars.AddRow(new DataRow(resource, Source));
        var (rows, notes) = ForecastResult(store);
        AssertQuantities(rows, "55.5556", "44.4444");
        Assert.Contains("A=16 working hours; R=24 working hours; p=0.4", Assert.Single(notes)["message"]);
        Assert.All(rows, row => Assert.EndsWith(".RESOURCE", row["clndr_id_key"]));
    }

    [Fact]
    public void Shared_curve_is_not_mutated_by_different_assignment_phases_or_repeated_generation()
    {
        XerDataStore store = ForecastStore();
        XerTable assignments = Required(store, "TASKRSRC");
        string[] second = assignments.Rows[0].Fields.ToArray();
        second[assignments.FieldIndexes["taskrsrc_id"]] = "A2";
        second[assignments.FieldIndexes["remain_qty"]] = "200";
        second[assignments.FieldIndexes["act_start_date"]] = "2026-01-30 12:00";
        assignments.AddRow(new DataRow(second, Source));
        var transformer = new XerTransformer(store);
        for (int repeat = 0; repeat < 2; repeat++)
        {
            var (rows, notes) = ForecastResult(store, transformer);
            Assert.Equal(300m, rows.Sum(Quantity));
            Assert.Equal(new[] { 175m, 125m }, rows.GroupBy(row => row["distribution_month"]).Select(group => group.Sum(Quantity)));
            Assert.Equal(2, notes.Length);
            Assert.Equal(2, notes.Select(row => row["taskrsrc_id"]).Distinct().Count());
            Assert.All(notes, note => AssertMethodNote(note, "REMAINING_CURVE_ESTIMATED"));
        }
    }

    [Fact]
    public void Forecast_elapsed_and_remaining_time_respect_inherited_holidays_and_short_shifts()
    {
        XerDataStore store = ForecastStore();
        Set(store, "TASKRSRC", "act_start_date", "2026-01-30 00:00");
        Set(store, "PROJECT", "last_recalc_date", "2026-01-31 08:00");
        Set(store, "CALENDAR", "base_clndr_id", "BASE");
        string week = string.Concat(Enumerable.Range(1, 7).Select(day => Node(day.ToString(CultureInfo.InvariantCulture), "",
            Node("0", "s|00:00|f|24:00"))));
        string exceptions = Exception(new DateTime(2026, 1, 30))
            + Exception(new DateTime(2026, 2, 1), Node("0", "s|00:00|f|06:00"));
        Required(store, "CALENDAR").AddRow(new DataRow(new[] { "BASE", "Base", "CA_Base", "", "24",
            Node("CalendarData", "", Node("DaysOfWeek", "", week), Node("Exceptions", "", exceptions)) }, Source));
        // A=8, R=18, first month t=12; conditional front-loaded area = 9/11.
        var (rows, notes) = ForecastResult(store);
        AssertQuantities(rows, "81.8182", "18.1818");
        Assert.Contains("A=8 working hours; R=18 working hours", Assert.Single(notes)["message"]);
        Assert.Equal("12.00", rows[0]["month_working_hours"]);
        Assert.Equal("6.00", rows[1]["month_working_hours"]);
    }

    [Fact]
    public void Unrepresentable_actual_units_do_not_suppress_a_valid_remaining_forecast_or_double_count_it()
    {
        XerDataStore store = ForecastStore();
        Set(store, "TASKRSRC", "act_reg_qty", "NaN");
        var (rows, notes) = ForecastResult(store);
        AssertQuantities(rows, "75.0000", "25.0000");
        AssertMethodNote(Assert.Single(notes, row => row["allocation_portion"] == ""), "REMAINING_CURVE_ESTIMATED");
        Dictionary<string, string> actual = Assert.Single(notes, row => row["allocation_portion"] == "Actual");
        Assert.Equal("ACTUAL_QUANTITY_INVALID", actual["issue_code"]);
        Assert.Equal("", actual["unallocated_actual_quantity"]);
        Assert.DoesNotContain(notes, row => row["allocation_portion"] == "Remaining");
    }

    [Fact]
    public void Actual_rows_are_identical_with_curved_or_uncurved_remaining_work()
    {
        XerDataStore store = ForecastStore();
        Set(store, "TASKRSRC", "act_start_date", "2025-12-31 12:00");
        Set(store, "TASKRSRC", "act_reg_qty", "75");
        Set(store, "TASKRSRC", "act_ot_qty", "25");
        var (curved, _) = ForecastResult(store);
        Set(store, "TASKRSRC", "curv_id", "");
        var (uncurved, _) = ForecastResult(store);
        Assert.Equal(curved.Where(row => row["is_actual"] == "1").Select(row => JsonSerializer.Serialize(row)),
            uncurved.Where(row => row["is_actual"] == "1").Select(row => JsonSerializer.Serialize(row)));
    }

    [Theory]
    [InlineData("valid")]
    [InlineData("invalid")]
    public void Explicit_profile_remains_authoritative_despite_missing_phase_and_curve(string profile)
    {
        XerDataStore store = ForecastStore();
        Set(store, "TASKRSRC", "curv_id", "MISSING");
        Set(store, "TASKRSRC", "act_start_date", "invalid");
        Set(store, "TASKRSRC", "remain_crv", profile == "valid" ? "25:12;75:12" : "bad-profile");
        if (profile == "invalid") { AssertRejected(store); return; }
        var (rows, notes) = ForecastResult(store);
        AssertQuantities(rows, "25.0000", "75.0000");
        Assert.Empty(notes);
    }

    [Fact]
    public void Tiny_forecast_allocations_conserve_units_without_residue_in_zero_weight_month()
    {
        // A=8,R=24 leaves weights only in the first half of the remaining period.
        XerDataStore store = ForecastStore(Points(i => i >= 6 && i <= 10 ? 20m : 0m));
        Set(store, "TASKRSRC", "remain_qty", "0.0001");
        var (rows, notes) = ForecastResult(store);
        AssertQuantities(rows, "0.0001", "0.0000");
        AssertMethodNote(Assert.Single(notes), "REMAINING_CURVE_ESTIMATED");
    }

    private static XerDataStore ForecastStore(decimal[]? curve = null, int elapsedHours = 8)
    {
        XerDataStore store = Store(curve ?? Loaded(), start: "2026-01-31 12:00", finish: "2026-02-01 12:00");
        Set(store, "TASK", "status_code", "TK_Active");
        Set(store, "PROJECT", "last_recalc_date", "2026-01-31 12:00");
        Set(store, "TASKRSRC", "act_start_date", DateParser.Format(new DateTime(2026, 1, 31, 12, 0, 0).AddHours(-elapsedHours)));
        return store;
    }

    private static void ForecastFields(XerDataStore store, string name, params (string Name, string Value)[] additions)
    {
        XerTable original = Required(store, name);
        string[] headers = original.Headers!.Concat(additions.Select(pair => pair.Name)).Distinct().ToArray();
        XerTable replacement = Table(name, headers);
        var changes = additions.ToDictionary(pair => pair.Name, pair => pair.Value);
        foreach (DataRow row in original.Rows)
            replacement.AddRow(new DataRow(headers.Select(header => changes.TryGetValue(header, out string? value)
                ? value : row.Fields[original.FieldIndexes[header]]).ToArray(), row.SourceFilename)
                { SourceToken = row.SourceToken, OriginalSourceFilename = row.OriginalSourceFilename });
        store.AddTable(replacement);
    }

    private static (Dictionary<string, string>[] Rows, Dictionary<string, string>[] Notes) ForecastResult(
        XerDataStore store, XerTransformer? transformer = null)
    {
        transformer ??= new XerTransformer(store);
        Dictionary<string, string>[] Records(XerTable table) => table.Rows.Select(row => table.Headers!
            .Select((header, index) => (header, row.Fields[index])).ToDictionary(pair => pair.header, pair => pair.Item2)).ToArray();
        return (Records(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()))
            .OrderBy(row => row["distribution_month"], StringComparer.Ordinal).ToArray(), Records(transformer.CreateDataQualityTable()));
    }

    private static void AssertMethodNote(Dictionary<string, string> note, string code)
    {
        Assert.Equal(code, note["issue_code"]);
        Assert.Equal("Warning", note["severity"]);
        Assert.Equal("", note["allocation_portion"]);
        Assert.Equal("", note["unallocated_actual_quantity"]);
        Assert.Equal("", note["unallocated_remaining_quantity"]);
        Assert.Equal("TASKRSRC", note["source_table"]);
        Assert.NotEmpty(note["taskrsrc_id_key"]);
        Assert.NotEmpty(note["source_row_number"]);
        using JsonDocument evidence = JsonDocument.Parse(note["raw_row_json"]);
        Assert.Contains(evidence.RootElement.EnumerateArray(), cell => cell.GetProperty("column").GetString() == "curv_id");
    }
}
