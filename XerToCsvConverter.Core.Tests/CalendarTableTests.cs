using System.Globalization;

namespace XerToCsvConverter.Core.Tests;

/// <summary>Export contracts for raw calendar settings and resolved availability rules.</summary>
public sealed class CalendarTableTests
{
    private static readonly DateTime Monday = new(2026, 8, 31);
    private static readonly string[] RawHeaders =
    {
        "clndr_id", "clndr_name", "clndr_type", "base_clndr_id", "day_hr_cnt",
        "week_hr_cnt", "month_hr_cnt", "year_hr_cnt", "clndr_data", "default_flag"
    };

    [Fact]
    public void Table_10_preserves_all_source_fields_and_period_factors_without_recalculation()
    {
        string blob = P6TestCalendars.WorkWeek("7.5");
        string[] input = { "C1", "A mixed-period calendar", "CA_Project", "0", "24.0000",
            "37.50", "162.50", "1950.00", blob, "N" };
        var store = Store(("occurrence-0001", input));

        XerTable table = Assert.IsType<XerTable>(new XerTransformer(store).Create10XerCalendar());
        Assert.Equal(RawHeaders.Concat(new[] { "clndr_id_key", "MonthUpdate" }), table.Headers);
        DataRow row = Assert.Single(table.Rows);
        Assert.Equal(input, row.Fields.Take(input.Length));
        Assert.Equal("occurrence-0001.C1", Field(table, row, "clndr_id_key"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("invalid")]
    public void Unknown_day_conversion_does_not_change_raw_values_or_valid_table_11_availability(string hours)
    {
        var store = Store(("source", Calendar("C1", P6TestCalendars.WorkWeek("10"), hours: hours)));
        var transformer = new XerTransformer(store);
        XerTable raw = Assert.IsType<XerTable>(transformer.Create10XerCalendar());
        Assert.Equal(hours, Field(raw, Assert.Single(raw.Rows), "day_hr_cnt"));
        XerTable detailed = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        Assert.Equal("10", Field(detailed, StandardDay(detailed, "Monday"), "work_hours"));
    }

    [Fact]
    public void Table_11_keeps_schema_and_emits_seven_explicit_standard_rules_with_consistent_flags()
    {
        var store = Store(("2609-source", Calendar("C1", P6TestCalendars.WorkWeek())));
        XerTable table = Assert.IsType<XerTable>(new XerTransformer(store).Create11XerCalendarDetailed());
        Assert.Equal(new[] { "clndr_id", "clndr_name", "clndr_type", "date", "day_of_week",
            "working_day", "work_hours", "exception_type", "clndr_id_key", "MonthUpdate",
            "day_of_week_num", "working_day_int" }, table.Headers);
        Assert.Equal(7, table.RowCount);
        for (int weekday = 0; weekday < 7; weekday++)
        {
            DataRow row = StandardDay(table, ((DayOfWeek)weekday).ToString());
            bool working = weekday is >= 1 and <= 5;
            Assert.Equal(working ? "8" : "0", Field(table, row, "work_hours"));
            Assert.Equal(working ? "Y" : "N", Field(table, row, "working_day"));
            Assert.Equal(working ? "1" : "0", Field(table, row, "working_day_int"));
            Assert.Equal((weekday == 0 ? 7 : weekday).ToString(CultureInfo.InvariantCulture),
                Field(table, row, "day_of_week_num"));
            Assert.Equal("2026-09-01", Field(table, row, "MonthUpdate"));
            Assert.Equal("Standard", Field(table, row, "exception_type"));
        }
    }

    [Fact]
    public void Repeated_native_ids_in_distinct_input_occurrences_keep_separate_calendars_and_keys()
    {
        // These tokens represent two ordered occurrences of the same filename/path/hash.
        // Identity is the occurrence token, never those repeated external attributes.
        var store = Store(("input-0001", Calendar("1", P6TestCalendars.WorkWeek("8"))),
            ("input-0002", Calendar("1", P6TestCalendars.WorkWeek("10"))));
        var transformer = new XerTransformer(store);
        XerTable raw = Assert.IsType<XerTable>(transformer.Create10XerCalendar());
        XerTable detailed = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        Assert.Equal(2, raw.RowCount);
        Assert.Equal(14, detailed.RowCount);
        foreach (var expected in new[] { (Source: "input-0001", Hours: "8"), (Source: "input-0002", Hours: "10") })
        {
            DataRow rawRow = Assert.Single(raw.Rows, row => row.SourceFilename == expected.Source);
            DataRow monday = Assert.Single(detailed.Rows, row => row.SourceFilename == expected.Source
                && Field(detailed, row, "day_of_week") == "Monday");
            Assert.Equal(expected.Hours, Field(detailed, monday, "work_hours"));
            Assert.Equal(Field(raw, rawRow, "clndr_id_key"), Field(detailed, monday, "clndr_id_key"));
        }
    }

    [Theory]
    [InlineData("Exceptions")]
    [InlineData("HolidayOrExceptions")]
    [InlineData("HolidayOrException")]
    public void Inherited_and_local_exceptions_replace_weekdays_instead_of_adding_to_them(string section)
    {
        string parent = WithExceptions(P6TestCalendars.WorkWeek(), section,
            Exception(Monday), Exception(Monday.AddDays(5), "08:00", "12:00"));
        string child = WithExceptions(P6TestCalendars.WorkWeek(), section,
            Exception(Monday.AddDays(1), "10:00", "14:00"));
        var store = Store(("source", Calendar("base", parent)),
            ("source", Calendar("child", child, parent: "base")));
        XerTable table = Assert.IsType<XerTable>(new XerTransformer(store).Create11XerCalendarDetailed());
        DataRow[] rules = table.Rows.Where(row => Field(table, row, "clndr_id") == "child").ToArray();
        Assert.Equal(10, rules.Length); // Seven weekly rules plus three dated replacements.

        decimal[] expected = { 0, 4, 8, 8, 8, 4, 0 };
        var calculator = new P6CalendarRepository(store).Get("source", "child").CreateCalculator();
        for (int day = 0; day < 7; day++)
        {
            DateTime date = Monday.AddDays(day);
            DataRow? replacement = rules.Where(row => Field(table, row, "date")
                == date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture))
                .Select(row => (DataRow?)row).SingleOrDefault();
            DataRow rule = replacement ?? rules.Single(row => Field(table, row, "date") == ""
                && Field(table, row, "day_of_week") == date.DayOfWeek.ToString());
            Assert.Equal(expected[day], decimal.Parse(Field(table, rule, "work_hours"), CultureInfo.InvariantCulture));
            Assert.Equal(expected[day], calculator.CountWorkingHours(date, date.AddDays(1)));
        }
        Assert.Equal(32m, calculator.CountWorkingHours(Monday, Monday.AddDays(7)));
    }

    [Fact]
    public void Table_11_retains_minute_precision_instead_of_rounding_small_shifts_to_zero()
    {
        string blob = WithExceptions(P6TestCalendars.WorkWeek(), "Exceptions",
            Exception(Monday, "08:00", "08:01"));
        var store = Store(("source", Calendar("C1", blob)));
        XerTable table = Assert.IsType<XerTable>(new XerTransformer(store).Create11XerCalendarDetailed());
        DataRow row = Assert.Single(table.Rows, row => Field(table, row, "date") == "2026-08-31");
        Assert.Equal(1m / 60m, decimal.Parse(Field(table, row, "work_hours"), CultureInfo.InvariantCulture));
        Assert.Equal("Y", Field(table, row, "working_day"));
    }

    [Fact]
    public void Ambiguous_same_source_identity_is_preserved_raw_but_cannot_produce_misleading_detailed_rules()
    {
        var store = Store(("source", Calendar("C1", P6TestCalendars.WorkWeek("8"))),
            ("source", Calendar("C1", P6TestCalendars.WorkWeek("10"))));
        var transformer = new XerTransformer(store);
        Assert.Equal(2, Assert.IsType<XerTable>(transformer.Create10XerCalendar()).RowCount);
        Assert.Null(transformer.Create11XerCalendarDetailed());
    }

    private static string[] Calendar(string id, string blob, string hours = "8", string parent = "") =>
        new[] { id, id, "CA_Project", parent, hours, "40", "173.3", "2080", blob, "N" };

    private static XerDataStore Store(params (string Source, string[] Fields)[] rows)
    {
        var store = new XerDataStore();
        var table = new XerTable("CALENDAR");
        table.SetHeaders(RawHeaders);
        foreach (var row in rows) table.AddRow(new DataRow(row.Fields, row.Source));
        store.AddTable(table);
        return store;
    }

    private static DataRow StandardDay(XerTable table, string day) => Assert.Single(table.Rows,
        row => Field(table, row, "date") == "" && Field(table, row, "day_of_week") == day);
    private static string Field(XerTable table, DataRow row, string name) => row.Fields[table.FieldIndexes[name]];
    private static string WithExceptions(string blob, string section, params string[] exceptions) =>
        blob[..^2] + "(0||" + section + "()(" + string.Concat(exceptions) + "))))";
    private static string Exception(DateTime date, string? start = null, string? end = null) =>
        "(0||0(d|" + (date - new DateTime(1899, 12, 30)).Days.ToString(CultureInfo.InvariantCulture)
        + ")(" + (start is null ? "" : "(0||0(s|" + start + "|f|" + end + ")())") + "))";
}
