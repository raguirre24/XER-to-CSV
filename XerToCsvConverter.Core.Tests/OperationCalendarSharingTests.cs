namespace XerToCsvConverter.Core.Tests;

public sealed class OperationCalendarSharingTests
{
    [Fact]
    public void Shared_export_lookup_preserves_resilient_outputs_and_strict_identity_failure()
    {
        XerDataStore store = Store();
        XerTable calendar = store.GetTable("CALENDAR")!;
        calendar.AddRow(new DataRow(["", "Blank identity", "CA_Project", "", "8", "malformed"], "2608.xer", "occurrence"));
        Assert.Throws<InvalidDataException>(() => new P6CalendarRepository(store));
        var publicTransformer = new XerTransformer(store);
        var operationTransformer = XerTransformer.ForExportOperation(store);
        EqualTable(publicTransformer.Create06XerPredecessor(new()), operationTransformer.Create06XerPredecessor(new()));
        EqualTable(publicTransformer.Create15XerResourceDistribution(), operationTransformer.Create15XerResourceDistribution());
        EqualTable(publicTransformer.Create11XerCalendarDetailed(), operationTransformer.Create11XerCalendarDetailed());
        Assert.Equal(XerDataQuality.WriteToBytes(publicTransformer.CreateDataQualityTable()),
            XerDataQuality.WriteToBytes(operationTransformer.CreateDataQualityTable()));
        XerTable diagnostics = operationTransformer.CreateDataQualityTable();
        Assert.Contains(diagnostics.Rows,
            row => row.Fields[diagnostics.FieldIndexes["issue_code"]] == "CALENDAR_IDENTITY_INVALID");
    }

    [Fact]
    public void Public_transformer_observes_calendar_edits_between_transformations()
    {
        XerDataStore store = Store();
        var transformer = new XerTransformer(store);
        transformer.Create06XerPredecessor(new());
        XerTable first = Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution());
        Assert.Equal("16.00", Assert.Single(first.Rows).Fields[first.FieldIndexes["total_working_hours"]]);
        XerTable calendar = store.GetTable("CALENDAR")!;
        DataRow row = Assert.Single(calendar.Rows);
        row.Fields[calendar.FieldIndexes["clndr_data"]] = P6TestCalendars.WorkWeek("10");
        row.Fields[calendar.FieldIndexes["day_hr_cnt"]] = "10";
        XerTable changed = Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution());
        Assert.Equal("20.00", Assert.Single(changed.Rows).Fields[changed.FieldIndexes["total_working_hours"]]);
        EqualTable(new XerTransformer(store).Create15XerResourceDistribution(), changed);
    }

    private static void EqualTable(XerTable? expected, XerTable? actual)
    {
        Assert.NotNull(expected);
        Assert.NotNull(actual);
        Assert.Equal(expected.Headers, actual.Headers);
        Assert.Equal(expected.RowCount, actual.RowCount);
        for (int index = 0; index < expected.RowCount; index++)
            Assert.Equal(expected.Rows[index].Fields, actual.Rows[index].Fields);
    }

    private static XerDataStore Store()
    {
        var store = new XerDataStore();
        Add("CALENDAR", ["clndr_id", "clndr_name", "clndr_type", "base_clndr_id", "day_hr_cnt", "clndr_data"],
            ["C1", "Work week", "CA_Project", "", "8", P6TestCalendars.WorkWeek()]);
        Add("PROJECT", ["proj_id", "last_recalc_date", "proj_short_name", "clndr_id"], ["P1", "2026-08-31 00:00", "PROJECT", "C1"]);
        Add("TASK", ["task_id", "proj_id", "clndr_id", "task_type", "status_code", "task_code", "restart_date", "reend_date",
            "early_start_date", "early_end_date", "act_start_date", "act_end_date", "remain_drtn_hr_cnt", "total_float_hr_cnt"],
            ["T1", "P1", "C1", "TT_Task", "TK_NotStart", "A100", "2026-09-07 08:00", "2026-09-08 18:00", "2026-09-07 08:00", "2026-09-08 18:00", "", "", "16", "0"],
            ["T2", "P1", "C1", "TT_Task", "TK_NotStart", "A200", "2026-09-09 08:00", "2026-09-10 17:00", "2026-09-09 08:00", "2026-09-10 17:00", "", "", "16", "0"]);
        Add("TASKPRED", ["task_pred_id", "task_id", "pred_task_id", "proj_id", "pred_proj_id", "pred_type", "lag_hr_cnt"],
            ["E1", "T2", "T1", "P1", "P1", "PR_FS", "0"]);
        Add("SCHEDOPTIONS", ["proj_id", "sched_calendar_on_relationship_lag"], ["P1", "rcal_Predecessor"]);
        Add("RSRC", ["rsrc_id", "rsrc_short_name", "rsrc_name", "rsrc_type", "unit_id", "clndr_id"], ["R1", "LAB", "Labour", "RT_Labor", "", "C1"]);
        Add("TASKRSRC", ["task_id", "rsrc_id", "act_reg_qty", "act_ot_qty", "remain_qty", "act_start_date", "act_end_date", "restart_date", "reend_date", "proj_id", "taskrsrc_id"],
            ["T1", "R1", "0", "0", "80", "", "", "2026-09-07 08:00", "2026-09-08 18:00", "P1", "RA1"]);
        return store;

        void Add(string name, string[] headers, params string[][] rows)
        {
            var table = new XerTable(name);
            table.SetHeaders(headers);
            foreach (string[] fields in rows) table.AddRow(new DataRow(fields, "2608.xer", "occurrence"));
            store.AddTable(table);
        }
    }
}
