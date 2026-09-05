using System.Collections.Concurrent;
using System.Text;

namespace XerToCsvConverter.Core.Tests;

// Correctness regressions for the approved pre-commit review fixes.
// The explicitly named baseline-policy case preserves its documented legacy policy.
[Trait("Category", "LegacyReviewRegression")]
public sealed class LegacyTableReviewTests
{
    private const string Source = "2601-review.xer";

    [Fact]
    public void Unstarted_activity_display_dates_prefer_exported_remaining_dates()
    {
        XerDataStore store = Store();
        Set(store, "TASK", "restart_date", "2026-02-02 08:00:00");
        Set(store, "TASK", "reend_date", "2026-02-03 17:00:00");

        Dictionary<string, string> row = Single(new XerTransformer(store).Create01XerTaskTable());

        Assert.Equal("2026-02-02 08:00:00", row["Start"]);
        Assert.Equal("2026-02-03 17:00:00", row["Finish"]);
    }

    [Fact]
    public void Active_activity_preserves_actual_start_and_remaining_finish()
    {
        XerDataStore store = Store();
        Set(store, "TASK", "status_code", "TK_Active");
        Set(store, "TASK", "act_start_date", "2026-01-02 08:00:00");
        Set(store, "TASK", "reend_date", "2026-02-03 17:00:00");

        Dictionary<string, string> row = Single(new XerTransformer(store).Create01XerTaskTable());

        Assert.Equal("2026-01-02 08:00:00", row["Start"]);
        Assert.Equal("2026-02-03 17:00:00", row["Finish"]);
    }

    [Fact]
    public void Units_complete_includes_nonlabor_quantities()
    {
        XerDataStore store = Store();
        Set(store, "TASK", "status_code", "TK_Active");
        Set(store, "TASK", "complete_pct_type", "CP_Units");
        Set(store, "TASK", "act_work_qty", "0");
        Set(store, "TASK", "remain_work_qty", "0");
        Set(store, "TASK", "act_equip_qty", "60");
        Set(store, "TASK", "remain_equip_qty", "40");

        Dictionary<string, string> row = Single(new XerTransformer(store).Create01XerTaskTable());

        // Oracle defines Units % Complete over labor AND nonlabor: this case is 60%.
        Assert.Equal("60.00", row["%"]);
    }

    [Fact]
    public void Nonfinite_physical_complete_is_unknown_not_a_numeric_nan()
    {
        XerDataStore store = Store();
        Set(store, "TASK", "status_code", "TK_Active");
        Set(store, "TASK", "complete_pct_type", "CP_Phys");
        Set(store, "TASK", "phys_complete_pct", "NaN");

        Dictionary<string, string> row = Single(new XerTransformer(store).Create01XerTaskTable());

        Assert.Equal("", row["%"]);
    }

    [Fact]
    public void Missing_hours_per_day_remains_unknown_in_task_and_relationship()
    {
        XerDataStore store = Store();
        Set(store, "CALENDAR", "day_hr_cnt", "");
        AddPredecessor(store);
        var transformer = new XerTransformer(store);

        XerTable tasks = Assert.IsType<XerTable>(transformer.Create01XerTaskTable());
        Dictionary<string, string> task = Assert.Single(Records(tasks), row => row["task_id"] == "T1");
        Dictionary<string, string> relation = Single(transformer.Create06XerPredecessor(new ConcurrentDictionary<string, XerTable>()));

        Assert.Equal("", task["total_float"]);
        Assert.Equal("", relation["total_float"]);
        Assert.Equal("", relation["free_float"]);
    }

    [Fact]
    public void Duplicate_calendar_identity_cannot_supply_a_conversion()
    {
        XerDataStore store = Store();
        XerTable calendars = Required(store, "CALENDAR");
        string[] duplicate = Assert.Single(calendars.Rows).Fields.ToArray();
        duplicate[calendars.FieldIndexes["day_hr_cnt"]] = "10";
        calendars.AddRow(new DataRow(duplicate, Source));
        var transformer = new XerTransformer(store);

        Dictionary<string, string> task = Single(transformer.Create01XerTaskTable());

        Assert.Equal("", task["Original Duration"]);
        Assert.Null(transformer.Create11XerCalendarDetailed());
    }

    [Fact]
    public void Standard_merge_preserves_later_curve_fields_and_their_spread()
    {
        XerDataStore first = Store(source: "first-occurrence", includeCurveColumns: false);
        Set(first, "TASKRSRC", "remain_qty", "0");
        XerDataStore second = Store(source: "second-occurrence", includeCurveColumns: true);
        Set(second, "TASKRSRC", "remain_crv", "80:8;20:8");

        Assert.Equal(new[] { "80.0000", "20.0000" }, Records(
            Assert.IsType<XerTable>(new XerTransformer(second).Create15XerResourceDistribution()))
            .Select(row => row["monthly_quantity"]).ToArray());
        first.MergeStore(second);

        Assert.True(Required(first, "TASKRSRC").FieldIndexes.ContainsKey("remain_crv"));
        Assert.True(Required(first, "TASKRSRC").FieldIndexes.ContainsKey("curv_id"));
        Assert.Equal(new[] { "80.0000", "20.0000" }, Records(
            Assert.IsType<XerTable>(new XerTransformer(first).Create15XerResourceDistribution()))
            .Select(row => row["monthly_quantity"]).ToArray());
    }

    [Fact]
    public async Task Standard_stream_inputs_with_repeated_filenames_keep_distinct_keys_and_original_provenance()
    {
        const string xer = "%T\tPROJECT\n%F\tproj_id\tproj_short_name\n%R\t1\tPROJECT\n%E\n";
        using var first = new MemoryStream(Encoding.UTF8.GetBytes(xer));
        using var second = new MemoryStream(Encoding.UTF8.GetBytes(xer));
        XerDataStore store = await new ProcessingService().ParseXerStreamsAsync(
            new[] { ((Stream)first, "2601.xer"), ((Stream)second, "2601.xer") }, null, CancellationToken.None);

        XerTable table = Assert.IsType<XerTable>(new XerTransformer(store).Create02XerProject());

        Assert.Equal(2, table.RowCount);
        Assert.Equal(2, Records(table).Select(row => row["proj_id_key"]).Distinct().Count());
        Assert.Equal(2, table.Rows.Select(row => row.SourceToken).Distinct().Count());
        Assert.All(table.Rows, row => Assert.Equal("2601.xer", row.OriginalSourceFilename));
        Assert.All(Records(table), row => Assert.Equal("2026-01-01", row["MonthUpdate"]));
    }

    [Fact]
    public void Legacy_baseline_policy_remains_global_earliest_month_not_a_governed_project_baseline()
    {
        var tasks = new XerTable("01_XER_TASK");
        tasks.SetHeaders(new[] { "proj_id_key", "task_id_key", "MonthUpdate" });
        tasks.AddRow(new DataRow(new[] { "A.P1", "A.T1", "2026-01-01" }, "A"));
        tasks.AddRow(new DataRow(new[] { "B.P2", "B.T2", "2026-02-01" }, "B"));

        Dictionary<string, string> row = Single(new XerTransformer(new XerDataStore()).Create04XerBaselineTable(tasks));

        Assert.Equal("A.P1", row["proj_id_key"]);
    }

    [Fact]
    public void Wbs_cycle_cannot_produce_an_enhanced_hierarchy()
    {
        var store = new XerDataStore();
        Add(store, "PROJWBS", Source, new[] { "wbs_id", "parent_wbs_id", "proj_id" },
            new[] { "W1", "W2", "P1" }, new[] { "W2", "W1", "P1" });

        Assert.Null(new XerTransformer(store).Create03XerProjWbsTable());
    }

    [Fact]
    public async Task Empty_requested_resource_distribution_emits_headers()
    {
        XerDataStore store = Store();
        Set(store, "TASKRSRC", "remain_qty", "0");
        XerTable table = Assert.IsType<XerTable>(new XerTransformer(store).Create15XerResourceDistribution());
        Assert.Empty(table.Rows);

        Dictionary<string, byte[]> files = await new ProcessingService().ExportTablesToMemoryAsync(store,
            new List<string> { "15_XER_RESOURCE_DISTRIBUTION" }, null, CancellationToken.None);

        string csv = Encoding.UTF8.GetString(files["15_XER_RESOURCE_DISTRIBUTION"]);
        Assert.Contains("monthly_quantity", csv);
        Assert.Single(csv.Trim().Split('\n'));
    }

    [Fact]
    public async Task Failed_requested_task_table_prevents_partial_success()
    {
        // CALENDAR is absent; a requested failed transformation must fail the export.
        var store = new XerDataStore();
        Add(store, "TASK", Source, new[] { "task_id", "proj_id" }, new[] { "T1", "P1" });
        Add(store, "PROJECT", Source, new[] { "proj_id" }, new[] { "P1" });

        await Assert.ThrowsAsync<InvalidDataException>(() => new ProcessingService().ExportTablesToMemoryAsync(store,
            new List<string> { "01_XER_TASK", "02_XER_PROJECT" }, null, CancellationToken.None));
    }

    private static XerDataStore Store(string source = Source, bool includeCurveColumns = true)
    {
        var store = new XerDataStore();
        Add(store, "TASK", source, new[]
        {
            "task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code", "task_code", "task_name",
            "early_start_date", "early_end_date", "restart_date", "reend_date", "act_start_date", "act_end_date",
            "target_drtn_hr_cnt", "remain_drtn_hr_cnt", "total_float_hr_cnt", "free_float_hr_cnt",
            "complete_pct_type", "phys_complete_pct", "act_work_qty", "remain_work_qty", "act_equip_qty", "remain_equip_qty",
            "duration_type"
        }, new[]
        {
            "T1", "P1", "W1", "C1", "TT_Task", "TK_NotStart", "A100", "Activity",
            "2026-01-05 08:00:00", "2026-01-05 17:00:00", "2026-01-05 08:00:00", "2026-01-05 17:00:00", "", "",
            "80", "40", "80", "8", "CP_Drtn", "0", "0", "0", "0", "0", "DT_FixedDrtn"
        });
        Add(store, "PROJECT", source, new[] { "proj_id", "last_recalc_date", "clndr_id" },
            new[] { "P1", "2026-01-01 00:00:00", "C1" });
        Add(store, "CALENDAR", source,
            new[] { "clndr_id", "clndr_name", "clndr_type", "base_clndr_id", "day_hr_cnt", "clndr_data" },
            new[] { "C1", "Calendar", "CA_Project", "", "8", P6TestCalendars.WorkWeek() });
        Add(store, "RSRC", source, new[] { "rsrc_id", "rsrc_short_name", "rsrc_name", "rsrc_type", "clndr_id" },
            new[] { "R1", "LAB", "Labour", "RT_Labor", "C1" });
        var headers = new List<string>
        {
            "taskrsrc_id", "task_id", "proj_id", "rsrc_id", "remain_qty", "act_reg_qty", "act_ot_qty", "restart_date", "reend_date"
        };
        var values = new List<string> { "A1", "T1", "P1", "R1", "100", "0", "0", "2026-01-30 08:00:00", "2026-02-02 17:00:00" };
        if (includeCurveColumns)
        {
            headers.AddRange(new[] { "curv_id", "remain_crv" });
            values.AddRange(new[] { "", "" });
        }
        Add(store, "TASKRSRC", source, headers.ToArray(), values.ToArray());
        return store;
    }

    private static void AddPredecessor(XerDataStore store)
    {
        XerTable tasks = Required(store, "TASK");
        string[] predecessor = Assert.Single(tasks.Rows).Fields.ToArray();
        predecessor[tasks.FieldIndexes["task_id"]] = "T0";
        predecessor[tasks.FieldIndexes["task_code"]] = "A000";
        foreach (string field in new[] { "early_start_date", "restart_date" })
            predecessor[tasks.FieldIndexes[field]] = "2026-01-02 08:00:00";
        foreach (string field in new[] { "early_end_date", "reend_date" })
            predecessor[tasks.FieldIndexes[field]] = "2026-01-02 17:00:00";
        tasks.AddRow(new DataRow(predecessor, Source));
        Add(store, "TASKPRED", Source,
            new[] { "task_pred_id", "task_id", "pred_task_id", "proj_id", "pred_proj_id", "pred_type", "lag_hr_cnt" },
            new[] { "REL1", "T1", "T0", "P1", "P1", "PR_FS", "0" });
    }

    private static void Add(XerDataStore store, string name, string source, string[] headers, params string[][] rows)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        foreach (string[] row in rows) table.AddRow(new DataRow(row, source));
        store.AddTable(table);
    }
    private static XerTable Required(XerDataStore store, string name) => Assert.IsType<XerTable>(store.GetTable(name));
    private static void Set(XerDataStore store, string name, string field, string value)
    {
        XerTable table = Required(store, name);
        Assert.Single(table.Rows).Fields[table.FieldIndexes[field]] = value;
    }
    private static Dictionary<string, string> Single(XerTable? table) => Assert.Single(Records(Assert.IsType<XerTable>(table)));
    private static Dictionary<string, string>[] Records(XerTable table) => table.Rows.Select(row =>
        table.Headers!.Select((header, index) => (header, row.Fields[index]))
            .ToDictionary(pair => pair.header, pair => pair.Item2, StringComparer.OrdinalIgnoreCase))
        .OrderBy(row => row.GetValueOrDefault("distribution_month"), StringComparer.Ordinal).ToArray();
}
