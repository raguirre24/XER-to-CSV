using System.Globalization;

namespace XerToCsvConverter.Core.Tests;

public sealed class ResilientCalendarWbsTests
{
    [Fact]
    public void A_bad_calendar_does_not_remove_other_calendars_and_has_only_unknown_availability()
    {
        XerDataStore store = Calendars(("good", P6TestCalendars.WorkWeek(), ""), ("bad", "malformed", ""));
        var transformer = new XerTransformer(store);
        XerTable table = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        Assert.Equal("8", Row(table, "good", "Monday")["work_hours"]);
        Assert.All(Records(table).Where(row => row["clndr_id"] == "bad"), row =>
        {
            Assert.Equal("", row["work_hours"]);
            Assert.Equal("", row["working_day"]);
            Assert.Equal("", row["working_day_int"]);
        });
        Dictionary<string, string> warning = Assert.Single(Records(transformer.CreateDataQualityTable()));
        Assert.Equal("CALENDAR_STRUCTURE_INVALID", warning["issue_code"]);
        Assert.Equal("2", warning["source_row_number"]);
        Assert.Contains("malformed", warning["raw_row_json"], StringComparison.Ordinal);
        Assert.DoesNotContain("internal-token", warning["message"], StringComparison.Ordinal);
        Assert.Null(transformer.GetGenerationFailure(EnhancedTableNames.XerCalendarDetailed11));
    }

    [Fact]
    public void Invalid_weekday_keeps_other_rules_and_propagates_unknown_overnight_spill_only_to_next_day()
    {
        string blob = Week((2, "(0||0(s|22:00|f|invalid)())"));
        var store = Calendars(("C", blob, ""));
        var transformer = new XerTransformer(store);
        XerTable table = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        Assert.Equal("", Row(table, "C", "Monday")["work_hours"]);
        Assert.Equal("", Row(table, "C", "Tuesday")["work_hours"]);
        Assert.Equal("8", Row(table, "C", "Wednesday")["work_hours"]);
        Assert.Equal("0", Row(table, "C", "Sunday")["work_hours"]);
        Assert.Throws<InvalidDataException>(() => new P6CalendarRepository(store).Get("internal-token", "C"));
        Assert.Contains(Records(transformer.CreateDataQualityTable()), row => row["issue_code"] == "CALENDAR_WEEKDAY_INVALID");
    }

    [Fact]
    public void Invalid_dated_exception_does_not_hide_valid_exceptions_or_fall_back_to_weekday_zero()
    {
        DateTime monday = new(2026, 8, 31);
        string blob = Exceptions(P6TestCalendars.WorkWeek(),
            Exception(monday, "(0||0(s|08:00|f|bad)())"),
            Exception(monday.AddDays(1), "(0||0(s|08:00|f|12:00)())"));
        var transformer = new XerTransformer(Calendars(("C", blob, "")));
        XerTable table = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        var rows = Records(table);
        var bad = Assert.Single(rows, row => row["date"] == "2026-08-31");
        Assert.Equal("", bad["work_hours"]);
        Assert.Equal("Exception - Invalid", bad["exception_type"]);
        Assert.Equal("4", Assert.Single(rows, row => row["date"] == "2026-09-01")["work_hours"]);
        Assert.Equal("8", Row(table, "C", "Monday")["work_hours"]);
        Assert.Single(Records(transformer.CreateDataQualityTable()));
    }

    [Fact]
    public void Unknown_exception_date_has_visible_unknown_marker_and_does_not_remove_valid_known_date()
    {
        string blob = Exceptions(P6TestCalendars.WorkWeek(), "(0||0(d|bad)())",
            Exception(new DateTime(2026, 8, 31), ""));
        var transformer = new XerTransformer(Calendars(("C", blob, "")));
        var rows = Records(Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed()));
        Assert.Equal("0", Assert.Single(rows, row => row["date"] == "2026-08-31")["work_hours"]);
        var marker = Assert.Single(rows, row => row["exception_type"] == "Exception - Invalid");
        Assert.Equal("", marker["date"]);
        Assert.Equal("", marker["day_of_week"]);
        Assert.Equal("", marker["work_hours"]);
    }

    [Fact]
    public void Missing_and_duplicate_weekdays_are_unknown_without_discarding_other_weekdays()
    {
        string blob = Week().Replace("(0||2()((0||0(s|08:00|f|16:00)())))", "", StringComparison.Ordinal)
            .Replace("(0||7()())", "(0||7()())(0||7()((0||0(s|08:00|f|12:00)())))", StringComparison.Ordinal);
        var transformer = new XerTransformer(Calendars(("C", blob, "")));
        XerTable table = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        Assert.Equal("", Row(table, "C", "Monday")["work_hours"]);
        Assert.Equal("", Row(table, "C", "Tuesday")["work_hours"]);
        Assert.Equal("", Row(table, "C", "Saturday")["work_hours"]);
        Assert.Equal("", Row(table, "C", "Sunday")["work_hours"]);
        Assert.Equal("8", Row(table, "C", "Wednesday")["work_hours"]);
        Assert.Contains(Records(transformer.CreateDataQualityTable()), row => row["issue_code"] == "CALENDAR_WEEKDAY_MISSING");
        Assert.Contains(Records(transformer.CreateDataQualityTable()), row => row["issue_code"] == "CALENDAR_WEEKDAY_DUPLICATE");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Duplicate_exception_date_remains_unknown_in_both_orders_and_does_not_hide_other_date(bool reverse)
    {
        DateTime monday = new(2026, 8, 31);
        string[] duplicates = [Exception(monday, ""), Exception(monday, "(0||0(s|08:00|f|12:00)())")];
        string blob = Exceptions(P6TestCalendars.WorkWeek(),
            (reverse ? duplicates.Reverse() : duplicates).Append(Exception(monday.AddDays(2), "")).ToArray());
        var transformer = new XerTransformer(Calendars(("C", blob, "")));
        var rows = Records(Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed()));
        Assert.Equal("", Assert.Single(rows, row => row["date"] == "2026-08-31")["work_hours"]);
        Assert.Equal("", Assert.Single(rows, row => row["date"] == "2026-09-01")["work_hours"]);
        Assert.Equal("0", Assert.Single(rows, row => row["date"] == "2026-09-02")["work_hours"]);
        Assert.Contains(Records(transformer.CreateDataQualityTable()), row => row["issue_code"] == "CALENDAR_EXCEPTION_DUPLICATE");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Cyclic_calendar_inheritance_warns_without_erasing_known_local_rules_in_either_input_order(bool reverse)
    {
        var definitions = new[] { ("A", P6TestCalendars.WorkWeek("8"), "B"), ("B", P6TestCalendars.WorkWeek("10"), "A") };
        var transformer = new XerTransformer(Calendars(reverse ? definitions.Reverse().ToArray() : definitions));
        XerTable table = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        Assert.Equal("8", Row(table, "A", "Monday")["work_hours"]);
        Assert.Equal("10", Row(table, "B", "Monday")["work_hours"]);
        Assert.Equal(2, Records(table).Count(row => row["exception_type"] == "Exception - Invalid"));
        Assert.Equal(2, transformer.CreateDataQualityTable().RowCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Duplicate_calendar_identity_never_selects_first_definition_in_either_order(bool reverse)
    {
        var definitions = new[] { ("C", P6TestCalendars.WorkWeek("8"), ""), ("C", P6TestCalendars.WorkWeek("10"), "") };
        var transformer = new XerTransformer(Calendars(reverse ? definitions.Reverse().ToArray() : definitions));
        XerTable table = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        Assert.Equal(14, table.RowCount);
        Assert.All(Records(table), row =>
        {
            Assert.Equal("", row["work_hours"]);
            Assert.Equal("", row["clndr_id_key"]);
        });
        Assert.Equal(2, transformer.CreateDataQualityTable().RowCount);
    }

    [Fact]
    public void Missing_base_keeps_local_week_and_explicit_exceptions_with_incomplete_inheritance_warning()
    {
        string blob = Exceptions(P6TestCalendars.WorkWeek(), Exception(new DateTime(2026, 8, 31), ""));
        var transformer = new XerTransformer(Calendars(("C", blob, "missing")));
        XerTable table = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        Assert.Equal("8", Row(table, "C", "Monday")["work_hours"]);
        Assert.Equal("0", Assert.Single(Records(table), row => row["date"] == "2026-08-31")["work_hours"]);
        Assert.Contains(Records(transformer.CreateDataQualityTable()), row => row["issue_code"] == "CALENDAR_INHERITANCE_INVALID");
    }

    [Fact]
    public void Unknown_inherited_exception_is_preserved_but_a_local_override_can_resolve_that_date()
    {
        DateTime monday = new(2026, 8, 31);
        string parent = Exceptions(P6TestCalendars.WorkWeek(), Exception(monday, "(0||0(s|bad|f|12:00)())"));
        string child = Exceptions(P6TestCalendars.WorkWeek(), Exception(monday, "(0||0(s|08:00|f|12:00)())"));
        var transformer = new XerTransformer(Calendars(("base", parent, ""), ("child", child, "base")));
        var rows = Records(Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed()));
        Assert.Equal("", Assert.Single(rows, row => row["clndr_id"] == "base" && row["date"] == "2026-08-31")["work_hours"]);
        Assert.Equal("4", Assert.Single(rows, row => row["clndr_id"] == "child" && row["date"] == "2026-08-31")["work_hours"]);
    }

    [Fact]
    public void Wbs_retains_every_raw_row_and_only_clears_unusable_derived_identities_and_parent_edges()
    {
        var store = new XerDataStore();
        var source = new XerTable("PROJWBS");
        source.SetHeaders(["wbs_id", "parent_wbs_id", "proj_id", "wbs_name"]);
        string[][] input = [ ["root", "", "P", "Root"], ["valid", "root", "P", "Valid"],
            ["duplicate", "", "P", "First"], ["duplicate", "root", "P", "Second"],
            ["ambiguous-parent", "duplicate", "P", "Ambiguous"], ["", "root", "P", "Missing ID"],
            ["cross", "root", "OTHER", "Cross project"], ["cycle-a", "cycle-b", "P", "A"],
            ["cycle-b", "cycle-a", "P", "B"], ["missing", "absent", "P", "Orphan"] ];
        foreach (var row in input) source.AddRow(new DataRow(row, "2608-repeat.xer", "internal-token", "2608-repeat.xer"));
        store.AddTable(source);
        var transformer = new XerTransformer(store);
        XerTable table = Assert.IsType<XerTable>(transformer.Create03XerProjWbsTable());
        Assert.Equal(input.Length, table.RowCount);
        for (int index = 0; index < input.Length; index++) Assert.Equal(input[index], table.Rows[index].Fields.Take(4));
        var rows = Records(table);
        Assert.Equal("2608-repeat.xer.root", rows[1]["parent_wbs_id_key"]);
        Assert.All(rows.Skip(2), row => Assert.Equal("", row["parent_wbs_id_key"]));
        Assert.Equal("", rows[2]["wbs_id_key"]);
        Assert.Equal("", rows[3]["wbs_id_key"]);
        Assert.Equal("", rows[5]["wbs_id_key"]);
        Assert.Equal("2608-repeat.xer.cycle-a", rows[7]["wbs_id_key"]);
        Assert.Equal(8, transformer.CreateDataQualityTable().RowCount);
        Assert.Null(transformer.GetGenerationFailure(EnhancedTableNames.XerProjWbs03));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Repeated_filenames_and_native_ids_are_independent_for_calendar_and_wbs_rows(bool reverse)
    {
        var store = new XerDataStore();
        var calendars = new XerTable("CALENDAR");
        calendars.SetHeaders(["clndr_id", "clndr_data"]);
        var wbs = new XerTable("PROJWBS");
        wbs.SetHeaders(["wbs_id", "parent_wbs_id", "proj_id"]);
        foreach (int occurrence in reverse ? new[] { 2, 1 } : new[] { 1, 2 })
        {
            string token = "private-" + occurrence;
            string publicName = "repeat.xer#source-" + occurrence;
            calendars.AddRow(new DataRow(["C", P6TestCalendars.WorkWeek(occurrence == 1 ? "8" : "10")], publicName, token, "repeat.xer"));
            wbs.AddRow(new DataRow(["root", "", "P"], publicName, token, "repeat.xer"));
            wbs.AddRow(new DataRow(["child", "root", "P"], publicName, token, "repeat.xer"));
        }
        store.AddTable(calendars);
        store.AddTable(wbs);
        var transformer = new XerTransformer(store);
        var detailed = Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed());
        Assert.Equal(14, detailed.RowCount);
        Assert.Equal(4, Assert.IsType<XerTable>(transformer.Create03XerProjWbsTable()).RowCount);
        Assert.Empty(transformer.CreateDataQualityTable().Rows);
        foreach (int occurrence in new[] { 1, 2 })
        {
            DataRow monday = Assert.Single(detailed.Rows, row => row.SourceToken == "private-" + occurrence
                && row.Fields[detailed.FieldIndexes["day_of_week"]] == "Monday");
            Assert.Equal(occurrence == 1 ? "8" : "10", monday.Fields[detailed.FieldIndexes["work_hours"]]);
        }
    }

    private static XerDataStore Calendars(params (string Id, string Blob, string Parent)[] inputs)
    {
        var store = new XerDataStore();
        var table = new XerTable("CALENDAR");
        table.SetHeaders(["clndr_id", "clndr_name", "clndr_type", "day_hr_cnt", "clndr_data", "base_clndr_id"]);
        foreach (var input in inputs)
            table.AddRow(new DataRow([input.Id, input.Id, "CA_Project", "8", input.Blob, input.Parent],
                "2608-repeat.xer", "internal-token", "2608-repeat.xer"));
        store.AddTable(table);
        return store;
    }

    private static string Week(params (int Day, string Shifts)[] replacements) =>
        "(0||CalendarData()((0||DaysOfWeek()(" + string.Concat(Enumerable.Range(1, 7).Select(day =>
            $"(0||{day}()({(replacements.Any(pair => pair.Day == day) ? replacements.Single(pair => pair.Day == day).Shifts : day is >= 2 and <= 6 ? "(0||0(s|08:00|f|16:00)())" : "")}))")) + "))))";
    private static string Exceptions(string week, params string[] records) => week[..^2]
        + "(0||Exceptions()(" + string.Concat(records) + "))))";
    private static string Exception(DateTime date, string shifts) => "(0||0(d|"
        + (date - new DateTime(1899, 12, 30)).Days.ToString(CultureInfo.InvariantCulture) + ")(" + shifts + "))";
    private static List<Dictionary<string, string>> Records(XerTable table) => table.Rows
        .Select(row => table.Headers!.Zip(row.Fields).ToDictionary(pair => pair.First, pair => pair.Second)).ToList();
    private static Dictionary<string, string> Row(XerTable table, string id, string day) =>
        Assert.Single(Records(table), row => row["clndr_id"] == id && row["day_of_week"] == day && row["date"] == "");
}
