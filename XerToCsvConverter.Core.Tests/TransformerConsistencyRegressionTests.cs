using System.Collections.Concurrent;

namespace XerToCsvConverter.Core.Tests;

public sealed class TransformerConsistencyRegressionTests
{
    private const string Original = "2601-shared.xer";

    [Theory]
    [InlineData("restart_date", "Start", "predecessor_start", "PR_SS")]
    [InlineData("reend_date", "Finish", "predecessor_finish", "PR_FS")]
    public void Malformed_preferred_date_cannot_fall_back_to_valid_early_or_late_dates(
        string preferred, string taskOutput, string relationshipOutput, string relationType)
    {
        XerDataStore store = Store();
        Set(store, "TASK", 0, preferred, "malformed preferred date");
        Set(store, "TASKPRED", 0, "pred_type", relationType);

        var transformer = new XerTransformer(store);
        Dictionary<string, string> task = Records(transformer.Create01XerTaskTable())[0];
        Dictionary<string, string> edge = Assert.Single(Records(Relationships(transformer)));

        Assert.Equal("", task[taskOutput]);
        Assert.Equal("", edge[relationshipOutput]);
        Assert.Equal("", edge["free_float"]);
    }

    [Theory]
    [InlineData("restart_date", "early_start_date", "Start", "predecessor_start")]
    [InlineData("reend_date", "early_end_date", "Finish", "predecessor_finish")]
    public void Absent_preferred_date_can_use_early_date_but_not_late_or_opposite_endpoint(
        string preferred, string early, string taskOutput, string relationshipOutput)
    {
        XerDataStore store = Store();
        string expected = Raw(store, "TASK", 0, early);
        Set(store, "TASK", 0, preferred, "");
        var transformer = new XerTransformer(store);
        Assert.Equal(expected, Records(transformer.Create01XerTaskTable())[0][taskOutput]);
        Assert.Equal(expected, Assert.Single(Records(Relationships(transformer)))[relationshipOutput]);

        Set(store, "TASK", 0, early, "");
        transformer = new XerTransformer(store);
        Assert.Equal("", Records(transformer.Create01XerTaskTable())[0][taskOutput]);
        Assert.Equal("", Assert.Single(Records(Relationships(transformer)))[relationshipOutput]);
    }

    [Theory]
    [InlineData("TK_Active", "2025-12-01 08:00:00", "2026-01-02 17:00:00")]
    [InlineData("TK_Complete", "2025-12-01 08:00:00", "2025-12-05 17:00:00")]
    public void Progressed_display_uses_actual_start_and_status_appropriate_finish(
        string status, string start, string finish)
    {
        XerDataStore store = Store();
        Set(store, "TASK", 0, "status_code", status);
        Set(store, "TASK", 0, "act_start_date", start);
        Set(store, "TASK", 0, "act_end_date", status == "TK_Complete" ? finish : "");
        var transformer = new XerTransformer(store);
        Dictionary<string, string> task = Records(transformer.Create01XerTaskTable())[0];
        Dictionary<string, string> edge = Assert.Single(Records(Relationships(transformer)));

        Assert.Equal(start, task["Start"]);
        Assert.Equal(start, edge["predecessor_start"]);
        Assert.Equal(finish, task["Finish"]);
        Assert.Equal(finish, edge["predecessor_finish"]);
    }

    [Theory]
    [InlineData("TK_Active", "")]
    [InlineData("TK_Active", "bad actual")]
    [InlineData("TK_Complete", "")]
    [InlineData("TK_Complete", "bad actual")]
    public void Missing_or_invalid_actual_start_is_unknown_not_forecast_start(string status, string actual)
    {
        XerDataStore store = Store();
        Set(store, "TASK", 0, "status_code", status);
        Set(store, "TASK", 0, "act_start_date", actual);
        var transformer = new XerTransformer(store);
        Assert.Equal("", Records(transformer.Create01XerTaskTable())[0]["Start"]);
        Assert.Equal("", Assert.Single(Records(Relationships(transformer)))["predecessor_start"]);
    }

    [Theory]
    [InlineData("60", "40", "0", "0", "60.00")]
    [InlineData("0", "0", "60", "40", "60.00")]
    [InlineData("20", "10", "40", "30", "60.00")]
    [InlineData("0", "0", "0", "0", "0.00")]
    [InlineData("", "", "", "", "0.00")]
    [InlineData("0.00001", "0.00003", "0", "0", "25.00")]
    [InlineData("NaN", "40", "60", "40", "")]
    [InlineData("60", "40", "Infinity", "0", "")]
    [InlineData("60", "40", "-1", "0", "")]
    [InlineData("60", "40", "malformed", "0", "")]
    [InlineData("79228162514264337593543950335", "1", "0", "0", "")]
    public void Units_percent_uses_full_time_units_and_preserves_invalid_as_unknown(
        string laborActual, string laborRemaining, string nonlaborActual, string nonlaborRemaining, string expected)
    {
        XerDataStore store = Store();
        Set(store, "TASK", 0, "status_code", "TK_Active");
        Set(store, "TASK", 0, "complete_pct_type", "CP_Units");
        Set(store, "TASK", 0, "act_work_qty", laborActual);
        Set(store, "TASK", 0, "remain_work_qty", laborRemaining);
        Set(store, "TASK", 0, "act_equip_qty", nonlaborActual);
        Set(store, "TASK", 0, "remain_equip_qty", nonlaborRemaining);

        Assert.Equal(expected, Records(new XerTransformer(store).Create01XerTaskTable())[0]["%"]);
    }

    [Theory]
    [InlineData("CP_Phys", "phys_complete_pct", "Infinity")]
    [InlineData("CP_Phys", "phys_complete_pct", "-1")]
    [InlineData("CP_Phys", "phys_complete_pct", "")]
    [InlineData("CP_Drtn", "target_drtn_hr_cnt", "0")]
    [InlineData("CP_Drtn", "target_drtn_hr_cnt", "NaN")]
    [InlineData("CP_Drtn", "remain_drtn_hr_cnt", "Infinity")]
    public void Nonfinite_or_unknown_percent_inputs_do_not_become_zero_or_nonfinite_csv(
        string type, string field, string raw)
    {
        XerDataStore store = Store();
        Set(store, "TASK", 0, "status_code", "TK_Active");
        Set(store, "TASK", 0, "complete_pct_type", type);
        Set(store, "TASK", 0, field, raw);
        Assert.Equal("", Records(new XerTransformer(store).Create01XerTaskTable())[0]["%"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Duplicate_calendar_and_project_conversions_are_unknown_in_either_row_order(bool reverse)
    {
        XerDataStore store = Store();
        Duplicate(store, "CALENDAR", 0, "day_hr_cnt", "10", reverse);
        Duplicate(store, "PROJECT", 0, "last_recalc_date", "2026-02-01 00:00:00", reverse);
        var transformer = new XerTransformer(store);
        Dictionary<string, string> task = Records(transformer.Create01XerTaskTable())[0];
        Dictionary<string, string> edge = Assert.Single(Records(Relationships(transformer)));

        Assert.Equal("", task["Original Duration"]);
        Assert.Equal("", task["Data Date"]);
        Assert.Equal("", edge["total_float"]);
        Assert.Equal("", edge["free_float"]);
        var calendarRows = Records(Assert.IsType<XerTable>(transformer.Create11XerCalendarDetailed()));
        Assert.All(calendarRows, row => Assert.Equal("", row["work_hours"]));
        Assert.Equal(2, Records(transformer.CreateDataQualityTable()).Count(row => row["table_name"] == EnhancedTableNames.XerCalendarDetailed11));
    }

    [Theory]
    [InlineData(false, "P1")]
    [InlineData(true, "P1")]
    [InlineData(false, "P2")]
    [InlineData(true, "P2")]
    public void Duplicate_task_identity_cannot_supply_a_display_key_or_relationship_float(bool reverse, string project)
    {
        XerDataStore store = Store();
        Duplicate(store, "TASK", 0, "proj_id", project, reverse);
        Dictionary<string, string> edge = Assert.Single(Records(Relationships(new XerTransformer(store))));

        Assert.Equal("", edge["pred_task_id_key"]);
        Assert.Equal("", edge["predecessor_start"]);
        Assert.Equal("", edge["predecessor_finish"]);
        Assert.Equal("", edge["free_float"]);
    }

    [Theory]
    [InlineData("duplicate")]
    [InlineData("self_cycle")]
    [InlineData("long_cycle")]
    [InlineData("cross_project_parent")]
    public void Invalid_wbs_identity_or_ancestry_preserves_rows_with_unknown_parent_keys(string defect)
    {
        XerDataStore store = Store();
        XerTable table = Required(store, "PROJWBS");
        if (defect == "duplicate") table.AddRow(table.Rows[0] with { Fields = (string[])table.Rows[0].Fields.Clone() });
        else if (defect == "self_cycle") Set(store, "PROJWBS", 0, "parent_wbs_id", "W1");
        else
        {
            Set(store, "PROJWBS", 0, "parent_wbs_id", "W2");
            table.AddRow(new DataRow(new[] { "W2", defect == "long_cycle" ? "W3" : "", "P1", "Second" }, Original));
            if (defect == "long_cycle")
                table.AddRow(new DataRow(new[] { "W3", "W1", "P1", "Third" }, Original));
            else Set(store, "PROJWBS", 1, "proj_id", "P2");
        }

        var transformer = new XerTransformer(store);
        var rows = Records(Assert.IsType<XerTable>(transformer.Create03XerProjWbsTable()));
        Assert.Equal(table.RowCount, rows.Length);
        Assert.All(rows, row => Assert.Equal("", row["parent_wbs_id_key"]));
        Assert.NotEmpty(transformer.CreateDataQualityTable().Rows);
    }

    [Fact]
    public void Missing_wbs_parent_is_cleared_without_mutating_raw_parent()
    {
        XerDataStore store = Store();
        Set(store, "PROJWBS", 0, "parent_wbs_id", "MISSING");
        Dictionary<string, string> row = Assert.Single(Records(new XerTransformer(store).Create03XerProjWbsTable()));
        Assert.Equal("", row["parent_wbs_id_key"]);
        Assert.Equal("MISSING", row["parent_wbs_id"]);
        Assert.Equal("MISSING", Raw(store, "PROJWBS", 0, "parent_wbs_id"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Explicit_source_tokens_isolate_all_calculations_when_display_filenames_repeat(bool reverse)
    {
        // Direct parsed-data probe: deliberately identical public/display filenames
        // ensure calculation isolation really comes from the explicit source token.
        // The Standard parser itself also disambiguates public keys for such inputs.
        XerDataStore first = Store("occurrence-A", hours: "8", frontLoaded: true);
        XerDataStore second = Store("occurrence-B", hours: "24", frontLoaded: false);
        Set(second, "PROJECT", 0, "last_recalc_date", "2026-01-03 00:00:00");
        Dictionary<string, string[]> expected = new(StringComparer.Ordinal);
        foreach ((string token, XerDataStore source) in new[] { ("occurrence-A", first), ("occurrence-B", second) })
            expected.Add(token, AllCalculationRows(source, token));

        XerDataStore combined = reverse ? second : first;
        combined.MergeStore(reverse ? first : second);
        foreach ((string token, string[] rows) in expected)
            Assert.Equal(rows, AllCalculationRows(combined, token));
        var transformer = new XerTransformer(combined);
        XerTable tasks = Assert.IsType<XerTable>(transformer.Create01XerTaskTable());
        Assert.All(tasks.Rows, row => Assert.Equal(Original, row.OriginalSourceFilename));
        Assert.Equal(new[] { "occurrence-A", "occurrence-B" }, tasks.Rows.Select(row => row.SourceToken).Distinct().Order().ToArray());
        Assert.Equal(2, Assert.IsType<XerTable>(transformer.Create03XerProjWbsTable()).RowCount);
    }

    private static string[] AllCalculationRows(XerDataStore store, string token)
    {
        var transformer = new XerTransformer(store);
        XerTable?[] tables = { transformer.Create01XerTaskTable(), Relationships(transformer),
            transformer.Create11XerCalendarDetailed(), transformer.Create15XerResourceDistribution() };
        return tables.SelectMany(table => Assert.IsType<XerTable>(table).Rows
            .Where(row => row.SourceToken == token).Select(row => table!.Name + "\t" + string.Join('\t', row.Fields)))
            .Order(StringComparer.Ordinal).ToArray();
    }

    private static XerDataStore Store(string? token = null, string hours = "8", bool frontLoaded = true)
    {
        var store = new XerDataStore();
        Add("TASK", new[]
        {
            "task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code", "task_code", "task_name",
            "early_start_date", "early_end_date", "restart_date", "reend_date", "late_start_date", "late_end_date",
            "act_start_date", "act_end_date", "target_drtn_hr_cnt", "remain_drtn_hr_cnt", "total_float_hr_cnt",
            "free_float_hr_cnt", "complete_pct_type", "phys_complete_pct", "act_work_qty", "remain_work_qty",
            "act_equip_qty", "remain_equip_qty", "duration_type"
        }, Task("T1", "2026-01-02"), Task("T2", "2026-01-05"));
        Add("PROJECT", new[] { "proj_id", "last_recalc_date", "clndr_id" }, new[] { "P1", "2026-01-01 00:00:00", "C1" });
        Add("CALENDAR", new[] { "clndr_id", "clndr_name", "clndr_type", "day_hr_cnt", "clndr_data" },
            new[] { "C1", "Calendar", "CA_Project", hours, P6TestCalendars.WorkWeek(hours) });
        Add("PROJWBS", new[] { "wbs_id", "parent_wbs_id", "proj_id", "wbs_name" }, new[] { "W1", "", "P1", "Root" });
        Add("TASKPRED", new[] { "task_pred_id", "task_id", "pred_task_id", "proj_id", "pred_proj_id", "pred_type", "lag_hr_cnt" },
            new[] { "L1", "T2", "T1", "P1", "P1", "PR_FS", "0" });
        Add("RSRC", new[] { "rsrc_id", "rsrc_name", "rsrc_short_name", "rsrc_type", "clndr_id" },
            new[] { "R1", "Resource", "R1", "RT_Labor", "C1" });
        Add("TASKRSRC", new[] { "taskrsrc_id", "task_id", "proj_id", "rsrc_id", "remain_qty", "act_reg_qty", "act_ot_qty", "restart_date", "reend_date", "curv_id" },
            new[] { "A1", "T1", "P1", "R1", "100", "0", "0", "2026-01-30 08:00:00", "2026-02-02 17:00:00", "CURVE1" });
        Add("RSRCCURVDATA", new[] { "curv_id" }.Concat(Enumerable.Range(0, 21).Select(index => "pct_usage_" + index)).ToArray(),
            new[] { "CURVE1" }.Concat(Enumerable.Range(0, 21).Select(index => index == 0 ? "0" : frontLoaded == (index <= 10) ? "8" : "2")).ToArray());
        return store;

        string[] Task(string id, string day) => new[]
        {
            id, "P1", "W1", "C1", "TT_Task", "TK_NotStart", id, id,
            day + " 08:00:00", day + " 17:00:00", day + " 08:00:00", day + " 17:00:00",
            "2026-03-02 08:00:00", "2026-03-02 17:00:00", "", "", "80", "40", "80", "8",
            "CP_Drtn", "0", "0", "0", "0", "0", "DT_FixedDrtn"
        };
        void Add(string name, string[] headers, params string[][] rows)
        {
            var table = new XerTable(name);
            table.SetHeaders(headers);
            foreach (string[] fields in rows)
            {
                Assert.Equal(headers.Length, fields.Length);
                table.AddRow(new DataRow(fields, Original, token ?? Original, Original));
            }
            store.AddTable(table);
        }
    }

    private static void Duplicate(XerDataStore store, string name, int index, string field, string value, bool reverse)
    {
        XerTable original = Required(store, name);
        DataRow duplicate = original.Rows[index] with { Fields = (string[])original.Rows[index].Fields.Clone() };
        duplicate.Fields[original.FieldIndexes[field]] = value;
        var table = new XerTable(name);
        table.SetHeaders(original.Headers!.ToArray());
        if (reverse) table.AddRow(duplicate);
        table.AddRows(original.Rows);
        if (!reverse) table.AddRow(duplicate);
        store.AddTable(table);
    }

    private static XerTable? Relationships(XerTransformer transformer) =>
        transformer.Create06XerPredecessor(new ConcurrentDictionary<string, XerTable>());
    private static XerTable Required(XerDataStore store, string name) => Assert.IsType<XerTable>(store.GetTable(name));
    private static string Raw(XerDataStore store, string name, int row, string field)
    {
        XerTable table = Required(store, name);
        return table.Rows[row].Fields[table.FieldIndexes[field]];
    }
    private static void Set(XerDataStore store, string name, int row, string field, string value)
    {
        XerTable table = Required(store, name);
        table.Rows[row].Fields[table.FieldIndexes[field]] = value;
    }
    private static Dictionary<string, string>[] Records(XerTable? input)
    {
        XerTable table = Assert.IsType<XerTable>(input);
        return table.Rows.Select(row => table.Headers!.Select((header, index) => (header, row.Fields[index]))
            .ToDictionary(pair => pair.header, pair => pair.Item2, StringComparer.OrdinalIgnoreCase)).ToArray();
    }
}
