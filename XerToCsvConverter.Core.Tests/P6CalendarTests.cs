using System.Globalization;
using Xunit;

namespace XerToCsvConverter.Core.Tests;

public sealed class P6CalendarTests
{
    private static readonly DateTime Monday = new(2026, 8, 31);

    [Fact]
    public void Standard_week_maps_P6_Sunday_one_and_preserves_split_work()
    {
        var definition = Definition(Blob());
        Assert.Empty(definition.StandardWeek[0]);
        Assert.Empty(definition.StandardWeek[6]);
        Assert.Equal(8m, definition.StandardWeek[1].Sum(slot => slot.WorkHours));
        Assert.Equal(0m, definition.CreateCalculator().CountWorkingHours(Monday.AddHours(12), Monday.AddHours(13)));
    }

    [Fact]
    public void Worked_exception_reads_all_nested_shifts()
    {
        var definition = Definition(Blob(exceptions: Exception(Monday,
            Shift("08:00", "12:00"), Shift("13:00", "17:00", 1))));
        Assert.Equal(8m, definition.Exceptions[Monday].Sum(slot => slot.WorkHours));
        Assert.Equal(8m, definition.CreateCalculator().CountWorkingHours(Monday, Monday.AddDays(1)));
    }

    [Fact]
    public void Empty_exception_replaces_standard_work()
    {
        var definition = Definition(Blob(exceptions: Exception(Monday)));
        Assert.Empty(definition.Exceptions[Monday]);
        Assert.False(definition.CreateCalculator().IsWorkingDay(Monday));
        Assert.Single(definition.Exceptions);
    }

    [Theory]
    [InlineData("00:00", "00:00")]
    [InlineData("00:00", "24:00")]
    public void Explicit_full_day_is_24_hours(string start, string end)
    {
        var definition = Definition(Blob(new Dictionary<int, string> { [2] = Shift(start, end) }));
        Assert.Equal(24m, definition.StandardWeek[1].Sum(slot => slot.WorkHours));
        Assert.Equal(24m, definition.CreateCalculator().CountWorkingHours(Monday, Monday.AddDays(1)));
        Assert.Equal(23m, definition.CreateCalculator().CountWorkingHours(Monday, Monday.AddHours(23)));
    }

    [Fact]
    public void Reversed_attribute_order_is_supported()
    {
        var definition = Definition(Blob(new Dictionary<int, string>
            { [2] = Node("0", "f|12:00|s|08:00") }));
        Assert.Equal(4m, definition.StandardWeek[1].Sum(slot => slot.WorkHours));
    }

    [Fact]
    public void Hours_per_period_does_not_create_availability()
    {
        var definition = Definition(Blob(), hours: "24");
        Assert.Equal(24m, definition.HoursPerDay);
        Assert.Equal(8m, definition.CreateCalculator().CountWorkingHours(Monday, Monday.AddDays(1)));
        Assert.False(definition.CreateCalculator().IsWorkingDay(Monday.AddDays(5)));
    }

    [Theory]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("-8")]
    [InlineData("NaN")]
    [InlineData("invalid")]
    public void Invalid_conversion_hours_do_not_destroy_valid_shift_availability(string hours)
    {
        var definition = Definition(Blob(), hours: hours);
        Assert.Null(definition.HoursPerDay);
        Assert.Equal(8m, definition.CreateCalculator().CountWorkingHours(Monday, Monday.AddDays(1)));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("not a calendar")]
    [InlineData("(0||CalendarData()())")]
    public void Missing_or_malformed_workweek_does_not_invent_Monday_to_Friday(string blob)
    {
        Assert.Throws<InvalidDataException>(() => Definition(blob, hours: "24"));
    }

    [Fact]
    public void Missing_day_is_an_error_but_explicit_empty_day_is_valid()
    {
        string incomplete = Node("CalendarData", "", Node("DaysOfWeek", "", Node("1", "")));
        Assert.Throws<InvalidDataException>(() => Definition(incomplete));
        var definition = Definition(Blob(AllDays("")));
        Assert.All(definition.StandardWeek, day => Assert.Empty(day));
    }

    [Fact]
    public void Duplicate_weekday_is_an_error()
    {
        string days = string.Concat(Enumerable.Range(1, 7).Select(day => Node(day.ToString(CultureInfo.InvariantCulture), "")));
        string blob = Node("CalendarData", "", Node("DaysOfWeek", "", days + Node("2", "")));
        Assert.Throws<InvalidDataException>(() => Definition(blob));
    }

    [Fact]
    public void Unbalanced_calendar_is_an_error()
    {
        Assert.Throws<InvalidDataException>(() => Definition(Blob()[..^1]));
    }

    [Theory]
    [InlineData("24:01", "24:00")]
    [InlineData("24:00", "24:00")]
    [InlineData("08:60", "17:00")]
    [InlineData("-1:00", "17:00")]
    [InlineData("08:00", "25:00")]
    [InlineData("08:00", "12:0")]
    public void Invalid_shift_clocks_are_rejected(string start, string end)
    {
        Assert.Throws<InvalidDataException>(() => Definition(Blob(new Dictionary<int, string>
            { [2] = Shift(start, end) })));
    }

    [Fact]
    public void Ambiguous_shift_markers_are_rejected()
    {
        Assert.Throws<InvalidDataException>(() => Definition(Blob(new Dictionary<int, string>
            { [2] = Node("0", "s|08:00|s|17:00") })));
    }

    [Fact]
    public void Duplicate_exception_dates_are_rejected()
    {
        Assert.Throws<InvalidDataException>(() => Definition(Blob(exceptions:
            Exception(Monday) + Exception(Monday, Shift("08:00", "12:00")))));
    }

    [Theory]
    [InlineData("2147483647")]
    [InlineData("-1")]
    [InlineData("garbage")]
    public void Invalid_exception_dates_do_not_turn_into_fabricated_holidays(string serial)
    {
        Assert.Throws<InvalidDataException>(() => Definition(Blob(exceptions: Node("0", "d|" + serial))));
    }

    [Fact]
    public void OLE_serial_zero_uses_the_real_epoch_and_optional_zero_field_is_supported()
    {
        var definition = Definition(Blob(exceptions: Node("0", "d|0|0")));
        Assert.Contains(new DateTime(1899, 12, 30), definition.Exceptions.Keys);
    }

    [Fact]
    public void Exception_serial_round_trips_to_its_exact_date()
    {
        var definition = Definition(Blob(exceptions: Exception(Monday)));
        Assert.Contains(Monday, definition.Exceptions.Keys);
    }

    [Fact]
    public void Base_holiday_is_inherited_and_child_override_wins()
    {
        var repository = Repository(
            new("base", Blob(exceptions: Exception(Monday))),
            new("child", Blob(), Parent: "base"),
            new("override", Blob(exceptions: Exception(Monday, Shift("10:00", "12:00"))), Parent: "child"));
        Assert.False(repository.Get("source", "child").CreateCalculator().IsWorkingDay(Monday));
        Assert.Equal(2m, repository.Get("source", "override").CreateCalculator()
            .CountWorkingHours(Monday, Monday.AddDays(1)));
    }

    [Fact]
    public void Base_calendar_does_not_replace_child_standard_week()
    {
        var repository = Repository(new("base", Blob(AllDays(Shift("00:00", "24:00")))),
            new("child", Blob(), Parent: "base"));
        Assert.Equal(8m, repository.Get("source", "child").CreateCalculator()
            .CountWorkingHours(Monday, Monday.AddDays(1)));
    }

    [Fact]
    public void Native_calendar_ids_resolve_only_within_their_source()
    {
        var repository = Repository(new("1", Blob(), Source: "source-a"),
            new("1", Blob(AllDays("")), Source: "source-b"));
        Assert.True(repository.Get("source-a", "1").CreateCalculator().IsWorkingDay(Monday));
        Assert.False(repository.Get("source-b", "1").CreateCalculator().IsWorkingDay(Monday));
    }

    [Fact]
    public void Missing_base_is_not_borrowed_from_another_source()
    {
        var repository = Repository(new("base", Blob(), Source: "other"), new("child", Blob(), Parent: "base"));
        Assert.Throws<InvalidDataException>(() => repository.Get("source", "child"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("-1")]
    public void Empty_base_sentinels_mean_no_parent(string parent)
    {
        Assert.Equal("", Repository(new Calendar("c", Blob(), Parent: parent)).Get("source", "c").BaseCalendarId);
    }

    [Fact]
    public void Base_calendar_cycle_is_rejected()
    {
        var repository = Repository(new("a", Blob(), Parent: "b"), new("b", Blob(), Parent: "a"));
        Assert.Throws<InvalidDataException>(() => repository.Get("source", "a"));
    }

    [Fact]
    public void Duplicate_source_calendar_identity_is_rejected()
    {
        Assert.Throws<InvalidDataException>(() => Repository(new("a", Blob()), new("a", Blob())));
    }

    [Fact]
    public void TryGet_returns_false_only_for_missing_identity()
    {
        var repository = Repository(new Calendar("bad", ""));
        Assert.False(repository.TryGet("source", "missing", out var missing));
        Assert.Null(missing);
        Assert.Throws<InvalidDataException>(() => repository.TryGet("source", "bad", out _));
    }

    [Fact]
    public void GetAll_has_stable_source_and_calendar_order()
    {
        var repository = Repository(new("b", Blob(), Source: "b"), new("b", Blob(), Source: "a"),
            new("a", Blob(), Source: "a"));
        Assert.Equal(new[] { "a:a", "a:b", "b:b" }, repository.GetAll()
            .Select(calendar => calendar.SourceToken + ":" + calendar.CalendarId));
    }

    [Fact]
    public void Overnight_shift_is_consistent_for_partial_and_full_intervals()
    {
        var calculator = Definition(Blob(new Dictionary<int, string>
            { [2] = Shift("22:00", "06:00"), [3] = "" })).CreateCalculator();
        Assert.Equal(8m, calculator.CountWorkingHours(Monday.AddHours(22), Monday.AddDays(1).AddHours(6)));
        Assert.Equal(2m, calculator.CountWorkingHours(Monday, Monday.AddDays(1)));
        Assert.Equal(6m, calculator.CountWorkingHours(Monday.AddDays(1), Monday.AddDays(2)));
        Assert.Equal(Monday.AddDays(1).AddHours(6), calculator.AddWorkingHours(Monday.AddHours(22), 8));
    }

    [Fact]
    public void Explicit_holiday_suppresses_incoming_overnight_work()
    {
        var calculator = Definition(Blob(new Dictionary<int, string> { [2] = Shift("22:00", "06:00") },
            Exception(Monday.AddDays(1)))).CreateCalculator();
        Assert.Equal(2m, calculator.CountWorkingHours(Monday.AddHours(22), Monday.AddDays(1).AddHours(6)));
    }

    [Fact]
    public void Holiday_removes_its_normal_overnight_spill_on_next_date()
    {
        var definition = Definition(Blob(new Dictionary<int, string> { [2] = Shift("22:00", "06:00"), [3] = "" },
            Exception(Monday)));
        Assert.Equal(0m, definition.CreateCalculator().CountWorkingHours(Monday.AddDays(1), Monday.AddDays(1).AddHours(6)));
        Assert.Empty(definition.Exceptions[Monday.AddDays(1)]);
    }

    [Fact]
    public void Overnight_exception_spills_and_child_next_day_override_has_precedence()
    {
        var repository = Repository(new("base", Blob(exceptions: Exception(Monday, Shift("22:00", "06:00")))),
            new("child", Blob(exceptions: Exception(Monday.AddDays(1))), Parent: "base"));
        Assert.Equal(8m, repository.Get("source", "base").CreateCalculator()
            .CountWorkingHours(Monday.AddHours(22), Monday.AddDays(1).AddHours(6)));
        Assert.Equal(2m, repository.Get("source", "child").CreateCalculator()
            .CountWorkingHours(Monday.AddHours(22), Monday.AddDays(1).AddHours(6)));
    }

    [Fact]
    public void Unordered_and_overlapping_shifts_are_sorted_and_unioned()
    {
        var definition = Definition(Blob(new Dictionary<int, string>
            { [2] = Shift("13:00", "17:00") + Shift("08:00", "12:00", 1) + Shift("10:00", "14:00", 2) }));
        var calculator = definition.CreateCalculator();
        Assert.Equal(9m, calculator.CountWorkingHours(Monday, Monday.AddDays(1)));
        Assert.Equal(Monday.AddHours(10), calculator.AddWorkingHours(Monday.AddHours(8), 2));
        Assert.Single(definition.StandardWeek[1]);
    }

    [Theory]
    [InlineData(8, 8, 0, 17)]
    [InlineData(8, 4, 0, 12)]
    [InlineData(13, 4, 0, 17)]
    [InlineData(17, -8, 0, 8)]
    [InlineData(12, -4, 0, 8)]
    [InlineData(17, -4, 0, 13)]
    [InlineData(17, 8, 1, 17)]
    public void Projection_retains_exact_working_interval_endpoints(int startHour, int hours, int dayOffset, int resultHour)
    {
        Assert.Equal(Monday.AddDays(dayOffset).AddHours(resultHour),
            WorkingDayCalculator.Default.AddWorkingHours(Monday.AddHours(startHour), hours));
    }

    [Fact]
    public void Projection_crosses_lunch_weekends_and_holidays()
    {
        var calculator = Definition(Blob(exceptions: Exception(Monday))).CreateCalculator();
        DateTime friday = Monday.AddDays(-3);
        Assert.Equal(Monday.AddDays(1).AddHours(9), calculator.AddWorkingHours(friday.AddHours(16), 2));
        Assert.Equal(friday.AddHours(16), calculator.AddWorkingHours(Monday.AddDays(1).AddHours(9), -2));
        Assert.Equal(friday.AddHours(14), calculator.AddWorkingHours(friday.AddHours(11), 2));
    }

    [Fact]
    public void Working_hour_count_is_signed_and_preserves_subsecond_precision()
    {
        DateTime start = Monday.AddHours(8);
        Assert.Equal(-8m, WorkingDayCalculator.Default.CountWorkingHours(Monday.AddHours(17), start));
        Assert.Equal(1m / TimeSpan.TicksPerHour,
            WorkingDayCalculator.Default.CountWorkingHours(start, start.AddTicks(1)));
        Assert.Equal(start.AddSeconds(1), WorkingDayCalculator.Default.AddWorkingHours(start, 1m / 3600m));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(-1)]
    public void No_work_calendar_fails_projection_promptly(int hours)
    {
        var calculator = Definition(Blob(AllDays(""))).CreateCalculator();
        Assert.Throws<InvalidDataException>(() => calculator.AddWorkingHours(Monday, hours));
        Assert.Equal(Monday, calculator.AddWorkingHours(Monday, 0));
    }

    [Fact]
    public void Exception_only_calendar_projects_only_within_available_work()
    {
        var calculator = Definition(Blob(AllDays(""), Exception(Monday, Shift("08:00", "12:00")))).CreateCalculator();
        Assert.Equal(Monday.AddHours(12), calculator.AddWorkingHours(Monday, 4));
        Assert.Throws<InvalidDataException>(() => calculator.AddWorkingHours(Monday, 5));
    }

    [Fact]
    public void Date_range_limits_fail_with_calendar_error_instead_of_datetime_overflow()
    {
        var calculator = Definition(Blob(AllDays(Shift("00:00", "24:00")))).CreateCalculator();
        Assert.Throws<InvalidDataException>(() => calculator.AddWorkingHours(DateTime.MaxValue, 1));
        Assert.Throws<InvalidDataException>(() => calculator.AddWorkingHours(DateTime.MinValue, -1));
    }

    [Fact]
    public void Cancellation_is_observed_by_parsing_counting_and_projection()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.Throws<OperationCanceledException>(() => Repository(new Calendar("c", Blob())).Get("source", "c", cancellation.Token));
        Assert.Throws<OperationCanceledException>(() => WorkingDayCalculator.Default.CountWorkingHours(Monday, Monday.AddDays(1), cancellation.Token));
        Assert.Throws<OperationCanceledException>(() => WorkingDayCalculator.Default.AddWorkingHours(Monday, 1, cancellation.Token));
    }

    [Fact]
    public void Parsing_is_invariant_under_non_English_culture()
    {
        CultureInfo original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");
            var definition = Definition(Blob(new Dictionary<int, string> { [2] = Shift("08:00", "15:30") }), hours: "7.5");
            Assert.Equal(7.5m, definition.HoursPerDay);
            Assert.Equal(7.5m, definition.CreateCalculator().CountWorkingHours(Monday, Monday.AddDays(1)));
        }
        finally { CultureInfo.CurrentCulture = original; }
    }

    [Fact]
    public void Legacy_constructor_does_not_create_work_from_totals_without_slots()
    {
        var hours = new decimal[7];
        hours[1] = 8;
        var slots = Enumerable.Range(0, 7).Select(_ => new List<(TimeSpan, TimeSpan)>()).ToArray();
        Assert.Throws<InvalidDataException>(() => new WorkingDayCalculator(new(), new(), hours, slots));
    }

    [Fact]
    public void Calendar_tables_preserve_conversion_settings_and_report_actual_shift_hours()
    {
        string blob = Blob(new Dictionary<int, string> { [2] = Shift("08:00", "15:30") });
        var transformer = new XerTransformer(Store(new Calendar("c", blob, Hours: "24")));
        XerTable raw = Assert.IsType<XerTable>(transformer.Create10XerCalendar());
        XerTable detailed = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        DataRow rawRow = Assert.Single(raw.Rows);
        Assert.Equal("24", Field(raw, rawRow, "day_hr_cnt"));
        Assert.Equal("40", Field(raw, rawRow, "week_hr_cnt"));
        Assert.Equal("173.3", Field(raw, rawRow, "month_hr_cnt"));
        Assert.Equal("2080", Field(raw, rawRow, "year_hr_cnt"));
        Assert.Equal(blob, Field(raw, rawRow, "clndr_data"));
        DataRow monday = Assert.Single(detailed.Rows, row => Field(detailed, row, "day_of_week") == "Monday");
        Assert.Equal("7.5", Field(detailed, monday, "work_hours"));
        Assert.Equal(Field(raw, rawRow, "clndr_id_key"), Field(detailed, monday, "clndr_id_key"));
    }

    [Theory]
    [InlineData("fr-FR")]
    [InlineData("th-TH")]
    public void Detailed_exception_date_and_hours_match_engine_invariantly(string culture)
    {
        CultureInfo original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
            var store = Store(new Calendar("c", Blob(exceptions: Exception(Monday,
                Shift("08:00", "12:00"), Shift("13:00", "16:30", 1)))));
            var definition = new P6CalendarRepository(store).Get("source", "c");
            var detailed = Assert.IsType<XerTable>(new XerTransformer(store).Create11XerCalendarDetailed());
            DataRow exception = Assert.Single(detailed.Rows, row => Field(detailed, row, "date") == "2026-08-31");
            Assert.Equal("7.5", Field(detailed, exception, "work_hours"));
            Assert.Equal(7.5m, definition.CreateCalculator().CountWorkingHours(Monday, Monday.AddDays(1)));
        }
        finally { CultureInfo.CurrentCulture = original; }
    }

    [Theory]
    [InlineData("")]
    [InlineData("malformed")]
    [InlineData("(0||CalendarData()())")]
    public void Malformed_calendar_is_preserved_raw_but_never_fabricated_in_detailed_table(string blob)
    {
        var transformer = new XerTransformer(Store(new Calendar("c", blob)));
        XerTable raw = Assert.IsType<XerTable>(transformer.Create10XerCalendar());
        Assert.Equal(blob, Field(raw, Assert.Single(raw.Rows), "clndr_data"));
        XerTable detailed = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        Assert.All(detailed.Rows, row => Assert.Equal("", Field(detailed, row, "work_hours")));
        Assert.NotEmpty(transformer.CreateDataQualityTable().Rows);
    }

    [Fact]
    public void Detailed_child_calendar_contains_inherited_holiday_with_matching_raw_key()
    {
        var store = Store(new Calendar("base", Blob(exceptions: Exception(Monday))),
            new Calendar("child", Blob(), Parent: "base"));
        var transformer = new XerTransformer(store);
        XerTable raw = Assert.IsType<XerTable>(transformer.Create10XerCalendar());
        XerTable detailed = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        DataRow child = Assert.Single(raw.Rows, row => Field(raw, row, "clndr_id") == "child");
        DataRow holiday = Assert.Single(detailed.Rows, row => Field(detailed, row, "clndr_id") == "child"
            && Field(detailed, row, "date") == "2026-08-31");
        Assert.Equal("0", Field(detailed, holiday, "work_hours"));
        Assert.Equal("N", Field(detailed, holiday, "working_day"));
        Assert.Equal(Field(raw, child, "clndr_id_key"), Field(detailed, holiday, "clndr_id_key"));
    }

    [Fact]
    public void Detailed_overnight_civil_day_totals_match_the_working_time_engine()
    {
        var store = Store(new Calendar("c", Blob(new Dictionary<int, string>
            { [2] = Shift("22:00", "06:00"), [3] = "" })));
        XerTable detailed = Assert.IsType<XerTable>(new XerTransformer(store).Create11XerCalendarDetailed());
        var calculator = new P6CalendarRepository(store).Get("source", "c").CreateCalculator();
        foreach (DateTime date in new[] { Monday, Monday.AddDays(1) })
        {
            DataRow row = Assert.Single(detailed.Rows, row => Field(detailed, row, "day_of_week") == date.DayOfWeek.ToString());
            Assert.Equal(calculator.CountWorkingHours(date, date.AddDays(1)),
                decimal.Parse(Field(detailed, row, "work_hours"), CultureInfo.InvariantCulture));
        }
    }

    [Fact]
    public void Merged_shift_longer_than_24_hours_is_not_silently_accepted()
    {
        Assert.Throws<InvalidDataException>(() => Definition(Blob(new Dictionary<int, string>
            { [2] = Shift("00:00", "23:00") + Shift("22:00", "06:00", 1) })));
    }

    private static P6CalendarDefinition Definition(string blob, string hours = "8") =>
        Repository(new Calendar("c", blob, Hours: hours)).Get("source", "c");

    private static P6CalendarRepository Repository(params Calendar[] calendars) => new(Store(calendars));

    private static XerDataStore Store(params Calendar[] calendars)
    {
        var store = new XerDataStore();
        var table = new XerTable("CALENDAR");
        table.SetHeaders(new[] { "clndr_id", "clndr_name", "clndr_type", "base_clndr_id", "day_hr_cnt", "clndr_data", "week_hr_cnt", "month_hr_cnt", "year_hr_cnt" });
        foreach (var calendar in calendars)
            table.AddRow(new DataRow(new[] { calendar.Id, calendar.Id, "CA_Project", calendar.Parent, calendar.Hours, calendar.Blob, "40", "173.3", "2080" }, calendar.Source));
        store.AddTable(table);
        return store;
    }

    private static string Field(XerTable table, DataRow row, string field) => row.Fields[table.FieldIndexes[field]];

    private static string Blob(IReadOnlyDictionary<int, string>? overrides = null, string exceptions = "")
    {
        var days = Enumerable.Range(1, 7).Select(day => Node(day.ToString(CultureInfo.InvariantCulture), "",
            overrides is not null && overrides.TryGetValue(day, out string? shifts) ? shifts
                : day is >= 2 and <= 6 ? Shift("08:00", "12:00") + Shift("13:00", "17:00", 1) : ""));
        return Node("CalendarData", "", Node("DaysOfWeek", "", string.Concat(days)), Node("Exceptions", "", exceptions));
    }

    private static Dictionary<int, string> AllDays(string shifts) => Enumerable.Range(1, 7).ToDictionary(day => day, _ => shifts);
    private static string Shift(string start, string end, int index = 0) => Node(index.ToString(CultureInfo.InvariantCulture), "s|" + start + "|f|" + end);
    private static string Exception(DateTime date, params string[] shifts) => Node("0", "d|" +
        (date.Date - new DateTime(1899, 12, 30)).Days.ToString(CultureInfo.InvariantCulture), shifts);
    private static string Node(string name, string attributes, params string[] children) =>
        "(0||" + name + "(" + attributes + ")(" + string.Concat(children) + "))";
    private sealed record Calendar(string Id, string Blob, string Hours = "8", string Parent = "", string Source = "source");
}
