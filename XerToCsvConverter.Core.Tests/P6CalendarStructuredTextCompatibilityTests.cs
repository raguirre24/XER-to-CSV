using System.Globalization;

namespace XerToCsvConverter.Core.Tests;

/// <summary>
/// Synthetic P6 structured-text syntax variants. No project/source calendar
/// content is copied into these compatibility fixtures.
/// </summary>
public sealed class P6CalendarStructuredTextCompatibilityTests
{
    private static readonly DateTime Monday = new(2026, 8, 31);
    private const string Source = "synthetic-calendar-source";

    [Theory]
    [InlineData(true, "")]
    [InlineData(false, "")]
    [InlineData(true, "\u007f\u007f")]
    [InlineData(false, "\u007f\u007f")]
    [InlineData(true, " \u007f\t\r\n")]
    [InlineData(false, " \u007f\t\r\n")]
    [InlineData(true, "\u0000\u001f\u0080\u009f")]
    [InlineData(false, "\u0000\u001f\u0080\u009f")]
    public void Optional_names_and_structural_separators_preserve_week_exception_hours_and_raw_blob(
        bool namedRoot, string separator)
    {
        string blob = CalendarBlob(namedRoot, separator);
        XerDataStore store = Store(("C1", "", blob));
        P6CalendarDefinition calendar = new P6CalendarRepository(store).Get(Source, "C1");
        WorkingDayCalculator calculator = calendar.CreateCalculator();
        Assert.Equal(8m, calendar.StandardWeek[1].Sum(interval => interval.WorkHours));
        Assert.Empty(calendar.StandardWeek[0]);
        Assert.Empty(calendar.StandardWeek[6]);
        Assert.Equal(0m, calculator.CountWorkingHours(Monday, Monday.AddDays(1)));
        Assert.Equal(3.5m, calculator.CountWorkingHours(Monday.AddDays(2), Monday.AddDays(3)));
        Assert.Equal(27.5m, calculator.CountWorkingHours(Monday, Monday.AddDays(7)));

        var transformer = new XerTransformer(store);
        XerTable raw = Assert.IsType<XerTable>(transformer.Create10XerCalendar());
        Assert.Equal(blob, Field(raw, Assert.Single(raw.Rows), "clndr_data"));
        XerTable detailed = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        Assert.Equal(9, detailed.RowCount);
        XerTable expected = Assert.IsType<XerTable>(new XerTransformer(
            Store(("C1", "", CalendarBlob(true, "")))).Create11XerCalendarDetailed());
        Assert.Equal(expected.Headers, detailed.Headers);
        Assert.Equal(expected.Rows.Select(row => string.Join("\t", row.Fields)),
            detailed.Rows.Select(row => string.Join("\t", row.Fields)));
        foreach (DateTime date in Enumerable.Range(0, 7).Select(day => Monday.AddDays(day)))
        {
            DataRow[] replacements = detailed.Rows.Where(row => Field(detailed, row, "date")
                == date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)).ToArray();
            DataRow rule = replacements.Length == 0
                ? Assert.Single(detailed.Rows, row => Field(detailed, row, "date") == ""
                    && Field(detailed, row, "day_of_week") == date.DayOfWeek.ToString())
                : Assert.Single(replacements);
            Assert.Equal(calculator.CountWorkingHours(date, date.AddDays(1)),
                decimal.Parse(Field(detailed, rule, "work_hours"), CultureInfo.InvariantCulture));
        }
    }

    [Fact]
    public void Unnamed_control_delimited_base_calendars_supply_exceptions_without_replacing_child_week()
    {
        string parent = CalendarBlob(false, "\u007f\u007f", shift: Shift("00:00", "24:00"));
        string child = CalendarBlob(true, "\u007f", exceptions:
            Exception(Monday.AddDays(2), Shift("09:00", "11:00")));
        XerDataStore store = Store(("base", "", parent), ("child", "base", child));
        P6CalendarDefinition calendar = new P6CalendarRepository(store).Get(Source, "child");
        Assert.Equal(8m, calendar.StandardWeek[1].Sum(interval => interval.WorkHours));
        Assert.Empty(calendar.Exceptions[Monday]);
        Assert.Equal(2m, calendar.Exceptions[Monday.AddDays(2)].Sum(interval => interval.WorkHours));
        Assert.Equal(26m, calendar.CreateCalculator().CountWorkingHours(Monday, Monday.AddDays(7)));

        XerTable detailed = Assert.IsType<XerTable>(new XerTransformer(store).Create11XerCalendarDetailed());
        DataRow holiday = Assert.Single(detailed.Rows, row => Field(detailed, row, "clndr_id") == "child"
            && Field(detailed, row, "date") == "2026-08-31");
        Assert.Equal("0", Field(detailed, holiday, "work_hours"));
        DataRow local = Assert.Single(detailed.Rows, row => Field(detailed, row, "clndr_id") == "child"
            && Field(detailed, row, "date") == "2026-09-02");
        Assert.Equal("2", Field(detailed, local, "work_hours"));
    }

    [Fact]
    public void Structural_control_padding_around_headers_and_attributes_is_supported()
    {
        string blob = CalendarBlob(false, "\u007f").Replace("(0||", "(\u007f0\u007f||\u007f",
            StringComparison.Ordinal).Replace("(s|", "(\u007fs|", StringComparison.Ordinal);
        P6CalendarDefinition calendar = new P6CalendarRepository(Store(("C1", "", blob))).Get(Source, "C1");
        Assert.Equal(8m, calendar.StandardWeek[1].Sum(interval => interval.WorkHours));
        Assert.Equal(3.5m, calendar.Exceptions[Monday.AddDays(2)].Sum(interval => interval.WorkHours));
    }

    [Fact]
    public void Unnamed_leaf_shift_records_are_valid_when_shift_attributes_are_unambiguous()
    {
        string blob = CalendarBlob(false, "\u007f", shift: Node("", "s|08:00|f|16:00"));
        P6CalendarDefinition calendar = new P6CalendarRepository(Store(("C1", "", blob))).Get(Source, "C1");
        Assert.Equal(8m, calendar.StandardWeek[1].Sum(interval => interval.WorkHours));
    }

    [Theory]
    [InlineData("missing weekday")]
    [InlineData("unnamed weekday")]
    [InlineData("duplicate weekday")]
    [InlineData("invalid clock")]
    [InlineData("control inside clock")]
    [InlineData("duplicate exception")]
    [InlineData("invalid record number")]
    [InlineData("missing record number")]
    [InlineData("unbalanced")]
    public void Compatibility_syntax_does_not_weaken_semantic_or_structural_validation(string defect)
    {
        string blob = CalendarBlob(false, "\u007f\u007f");
        blob = defect switch
        {
            "missing weekday" => blob.Replace(Node("1", ""), "", StringComparison.Ordinal),
            "unnamed weekday" => blob.Replace("(0||1()", "(0||()", StringComparison.Ordinal),
            "duplicate weekday" => blob.Replace(Node("1", ""), Node("1", "") + Node("1", ""), StringComparison.Ordinal),
            "invalid clock" => blob.Replace("s|08:00", "s|24:01", StringComparison.Ordinal),
            "control inside clock" => blob.Replace("s|08:00", "s|08:\u007f00", StringComparison.Ordinal),
            "duplicate exception" => blob.Replace(Exception(Monday), Exception(Monday) + Exception(Monday), StringComparison.Ordinal),
            "invalid record number" => blob.Replace("(0||", "(x||", StringComparison.Ordinal),
            "missing record number" => blob.Replace("(0||", "(||", StringComparison.Ordinal),
            "unbalanced" => blob[..^3],
            _ => throw new InvalidOperationException(defect)
        };
        Assert.Throws<InvalidDataException>(() => new P6CalendarRepository(Store(("C1", "", blob))).Get(Source, "C1"));
    }

    [Fact]
    public void Unnamed_control_delimited_calendars_still_reject_inheritance_cycles()
    {
        string blob = CalendarBlob(false, "\u007f");
        var repository = new P6CalendarRepository(Store(("first", "second", blob), ("second", "first", blob)));
        InvalidDataException error = Assert.Throws<InvalidDataException>(() => repository.Get(Source, "first"));
        Assert.Contains("cyclic", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("malformed", "C1")]
    [InlineData("missing base", "missing")]
    [InlineData("duplicate", "C1")]
    [InlineData("blank", "blank CALENDAR.clndr_id")]
    [InlineData("cycle", "cyclic")]
    public void Calendar_diagnostics_identify_original_source_without_changing_occurrence_identity(
        string defect, string expectedDetail)
    {
        string blob = CalendarBlob(false, "\u007f");
        XerDataStore store = defect switch
        {
            "malformed" => Store(("C1", "", "malformed")),
            "missing base" => Store(("C1", "missing", blob)),
            "duplicate" => Store(("C1", "", blob), ("C1", "", blob)),
            "blank" => Store(("", "", blob)),
            "cycle" => Store(("C1", "C2", blob), ("C2", "C1", blob)),
            _ => throw new InvalidOperationException(defect)
        };
        XerTable original = store.GetTable("CALENDAR")!;
        var calendarTable = new XerTable("CALENDAR");
        calendarTable.SetHeaders(original.Headers!);
        calendarTable.AddRows(original.Rows.Select(row => row with
        {
            SourceFilename = "public-namespace#source-000001",
            OriginalSourceFilename = "2607-repeat.xer"
        }));
        store.AddTable(calendarTable);

        InvalidDataException error = Assert.Throws<InvalidDataException>(() =>
            new P6CalendarRepository(store).Get(Source, "C1"));
        Assert.Contains("2607-repeat.xer", error.Message, StringComparison.Ordinal);
        Assert.Contains(Source, error.Message, StringComparison.Ordinal);
        Assert.Contains(expectedDetail, error.Message, StringComparison.Ordinal);
        Assert.All(calendarTable.Rows, row => Assert.Equal(Source, row.SourceToken));
    }

    private static string CalendarBlob(bool namedRoot, string separator, string? shift = null, string? exceptions = null)
    {
        shift ??= Shift("08:00", "12:00") + Shift("13:00", "17:00");
        string days = string.Concat(Enumerable.Range(1, 7).Select(day => separator
            + Node(day.ToString(CultureInfo.InvariantCulture), "", day is >= 2 and <= 6 ? shift : "") + separator));
        exceptions ??= Exception(Monday) + separator + Exception(Monday.AddDays(2), Shift("09:00", "12:30"));
        return separator + Node(namedRoot ? "CalendarData" : "", "", separator
            + Node("DaysOfWeek", "", days) + separator + Node("Exceptions", "", exceptions) + separator) + separator;
    }

    private static string Node(string name, string attributes, string children = "") =>
        "(0||" + name + "(" + attributes + ")(" + children + "))";
    private static string Shift(string start, string finish) => Node("0", "s|" + start + "|f|" + finish);
    private static string Exception(DateTime date, string shifts = "") => Node("0", "d|"
        + (date - new DateTime(1899, 12, 30)).Days.ToString(CultureInfo.InvariantCulture), shifts);
    private static string Field(XerTable table, DataRow row, string name) => row.Fields[table.FieldIndexes[name]];

    private static XerDataStore Store(params (string Id, string Parent, string Blob)[] rows)
    {
        var store = new XerDataStore();
        var table = new XerTable("CALENDAR");
        table.SetHeaders(new[] { "clndr_id", "clndr_name", "clndr_type", "base_clndr_id", "day_hr_cnt", "clndr_data" });
        foreach (var row in rows)
            table.AddRow(new DataRow(new[] { row.Id, row.Id, "CA_Project", row.Parent, "8", row.Blob }, Source));
        store.AddTable(table);
        return store;
    }
}
