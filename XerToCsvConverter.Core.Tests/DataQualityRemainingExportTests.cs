using System.Globalization;
using System.Text;
using Microsoft.VisualBasic.FileIO;

namespace XerToCsvConverter.Core.Tests;

/// <summary>Source scheduling defects must not discard a table or invent a monthly allocation.</summary>
public sealed class DataQualityRemainingExportTests
{
    private const string Source = "remaining, \"source\".xer";
    private const string Token = "private-remaining-occurrence";
    private const string Distribution = EnhancedTableNames.XerResourceDist15;

    [Fact]
    public void Thirteen_assignments_on_seven_completed_tasks_warn_without_losing_actuals_or_valid_remaining()
    {
        XerDataStore store = Store();
        for (int i = 1; i <= 7; i++) AddTask(store, "COMPLETE" + i, "TK_Complete");
        for (int i = 1; i <= 13; i++) AddAssignment(store, "BAD" + i, "COMPLETE" + ((i - 1) % 7 + 1), actual: "2", remaining: "1.25001");
        AddAssignment(store, "VALID", "T1", actual: "3", remaining: "5");
        string[][] original = store.GetTable("TASKRSRC")!.Rows.Select(row => row.Fields.ToArray()).ToArray();
        var transformer = new XerTransformer(store);

        var rows = Rows(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()));
        var warnings = Rows(transformer.CreateDataQualityTable());

        Assert.Equal(13, warnings.Count);
        Assert.Equal(7, warnings.Select(row => row["task_id_key"]).Distinct().Count());
        Assert.All(warnings, warning =>
        {
            AssertWarning(warning, "Remaining", "REMAINING_ON_COMPLETED");
            Assert.Equal("TK_Complete", warning["status_code"]);
            Assert.Equal("1.25001", warning["remain_qty"]);
            Assert.Equal("1.2500", warning["unallocated_remaining_quantity"]);
            Assert.Equal("", warning["unallocated_actual_quantity"]);
        });
        Assert.Equal(29m, Quantity(rows.Where(row => row["is_actual"] == "1")));
        Assert.Equal(5m, Quantity(rows.Where(row => row["is_actual"] == "0")));
        Assert.Equal(21.25m, Quantity(rows.Where(row => row["is_actual"] == "0"))
            + warnings.Sum(row => Number(row["unallocated_remaining_quantity"])));
        Assert.Equal(Enumerable.Range(1, 13).Select(i => i.ToString(CultureInfo.InvariantCulture)),
            warnings.Select(row => row["source_row_number"]));
        Assert.All(rows, row => Assert.NotEmpty(row["distribution_month"]));
        Assert.Null(transformer.GetGenerationFailure(Distribution));
        for (int i = 0; i < original.Length; i++) Assert.Equal(original[i], store.GetTable("TASKRSRC")!.Rows[i].Fields);
    }

    [Theory]
    [InlineData("restart_date", "", "REMAINING_PERIOD_INVALID")]
    [InlineData("restart_date", " malformed, \"raw\" ", "REMAINING_PERIOD_INVALID")]
    [InlineData("reend_date", "", "REMAINING_PERIOD_INVALID")]
    [InlineData("reend_date", "bad-date", "REMAINING_PERIOD_INVALID")]
    [InlineData("reend_date", "2026-07-26 08:00", "REMAINING_PERIOD_INVALID")]
    [InlineData("reend_date", "2026-07-27 08:00", "REMAINING_PERIOD_INVALID")]
    [InlineData("curv_id", "MISSING", "REMAINING_PROFILE_INVALID")]
    [InlineData("remain_crv", "opaque profile, \"raw\"", "REMAINING_PROFILE_INVALID")]
    public void Bad_remaining_portion_preserves_raw_evidence_actual_portion_and_other_assignment(
        string field, string value, string code)
    {
        XerDataStore store = Store();
        AddAssignment(store, "BAD", "T1", actual: " 1.25 ", overtime: "0.125", remaining: "7.00001");
        Set(store, "TASKRSRC", field, value);
        AddAssignment(store, "GOOD", "T1", actual: "2", remaining: "5");
        var transformer = new XerTransformer(store);

        var rows = Rows(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()));
        var warning = Assert.Single(Rows(transformer.CreateDataQualityTable()));

        AssertWarning(warning, "Remaining", code);
        Assert.Equal(value, warning[field]);
        Assert.Equal("7.00001", warning["remain_qty"]);
        Assert.Equal("7.0000", warning["unallocated_remaining_quantity"]);
        Assert.Equal(3.375m, Quantity(rows.Where(row => row["is_actual"] == "1")));
        Assert.Equal(5m, Quantity(rows.Where(row => row["is_actual"] == "0")));
        Assert.Equal(12m, Quantity(rows.Where(row => row["is_actual"] == "0")) + Number(warning["unallocated_remaining_quantity"]));
    }

    [Fact]
    public void Nonworking_remaining_period_is_unallocated_while_recorded_actuals_retain_their_supported_fallback()
    {
        XerDataStore store = Store();
        AddAssignment(store, "A1", "T1", actual: "3", remaining: "7");
        Set(store, "TASKRSRC", "restart_date", "2026-07-26 08:00");
        Set(store, "TASKRSRC", "reend_date", "2026-07-26 17:00");
        Set(store, "TASKRSRC", "act_start_date", "2026-07-26 08:00");
        Set(store, "TASKRSRC", "act_end_date", "2026-07-26 17:00");
        var transformer = new XerTransformer(store);

        var row = Assert.Single(Rows(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution())));
        var warning = Assert.Single(Rows(transformer.CreateDataQualityTable()));

        Assert.Equal("Actual Elapsed Time", row["distribution_type"]);
        Assert.Equal("3.0000", row["monthly_quantity"]);
        Assert.Equal("0.00", row["month_working_hours"]);
        AssertWarning(warning, "Remaining", "REMAINING_NO_WORKING_TIME");
        Assert.Equal("7.0000", warning["unallocated_remaining_quantity"]);
    }

    [Theory]
    [InlineData("remain_qty", "NaN", "", 3, 0)]
    [InlineData("remain_qty", "Infinity", "", 3, 0)]
    [InlineData("remain_qty", "invalid", "", 3, 0)]
    [InlineData("remain_qty", "1e100", "", 3, 0)]
    [InlineData("remain_qty", "-1.25", "-1.2500", 3, 0)]
    [InlineData("act_reg_qty", "NaN", "", 0, 7)]
    [InlineData("act_reg_qty", "-Infinity", "", 0, 7)]
    [InlineData("act_reg_qty", "  malformed, \"raw\" ", "", 0, 7)]
    [InlineData("act_reg_qty", "-1.25", "-0.2500", 0, 7)]
    [InlineData("act_ot_qty", "NaN", "", 0, 7)]
    [InlineData("act_ot_qty", "-1.25", "0.7500", 0, 7)]
    public void Invalid_quantity_is_not_zero_or_nonfinite_and_does_not_drop_the_valid_opposite_portion(
        string field, string value, string unallocated, int actual, int remaining)
    {
        XerDataStore store = Store();
        AddAssignment(store, "A1", "T1", actual: "2", overtime: "1", remaining: "7");
        Set(store, "TASKRSRC", field, value);
        var transformer = new XerTransformer(store);

        var rows = Rows(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()));
        var warning = Assert.Single(Rows(transformer.CreateDataQualityTable()));
        bool isRemaining = field == "remain_qty";

        AssertWarning(warning, isRemaining ? "Remaining" : "Actual", isRemaining ? "REMAINING_QUANTITY_INVALID" : "ACTUAL_QUANTITY_INVALID");
        Assert.Equal(value, warning[field]);
        Assert.Equal(unallocated, warning[isRemaining ? "unallocated_remaining_quantity" : "unallocated_actual_quantity"]);
        Assert.Equal((decimal)actual, Quantity(rows.Where(row => row["is_actual"] == "1")));
        Assert.Equal((decimal)remaining, Quantity(rows.Where(row => row["is_actual"] == "0")));
    }

    [Fact]
    public void Actuals_on_unstarted_task_warn_without_dropping_valid_remaining_work()
    {
        XerDataStore store = Store();
        Set(store, "TASK", "status_code", "TK_NotStart");
        AddAssignment(store, "A1", "T1", actual: "4", remaining: "7");
        var transformer = new XerTransformer(store);

        var rows = Rows(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()));
        var warning = Assert.Single(Rows(transformer.CreateDataQualityTable()));

        Assert.Equal(7m, Quantity(rows));
        Assert.All(rows, row => Assert.Equal("0", row["is_actual"]));
        AssertWarning(warning, "Actual", "ACTUAL_ON_UNSTARTED");
        Assert.Equal("4.0000", warning["unallocated_actual_quantity"]);
    }

    [Fact]
    public void Invalid_negative_actual_component_is_not_hidden_when_components_sum_to_zero()
    {
        XerDataStore store = Store();
        AddAssignment(store, "A1", "T1", actual: "-1", overtime: "1", remaining: "7");
        var transformer = new XerTransformer(store);

        var rows = Rows(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()));
        var warning = Assert.Single(Rows(transformer.CreateDataQualityTable()));

        Assert.Equal(7m, Quantity(rows));
        AssertWarning(warning, "Actual", "ACTUAL_QUANTITY_INVALID");
        Assert.Equal("-1", warning["act_reg_qty"]);
        Assert.Equal("1", warning["act_ot_qty"]);
        Assert.Equal("0.0000", warning["unallocated_actual_quantity"]);
    }

    [Fact]
    public void Overflowing_actual_sum_preserves_both_components_and_valid_remaining_without_fabricating_a_total()
    {
        XerDataStore store = Store();
        string maximum = decimal.MaxValue.ToString(CultureInfo.InvariantCulture);
        AddAssignment(store, "A1", "T1", actual: maximum, overtime: "1", remaining: "7");
        var transformer = new XerTransformer(store);

        var rows = Rows(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()));
        var warning = Assert.Single(Rows(transformer.CreateDataQualityTable()));

        Assert.Equal(7m, Quantity(rows));
        AssertWarning(warning, "Actual", "ACTUAL_QUANTITY_INVALID");
        Assert.Equal(maximum, warning["act_reg_qty"]);
        Assert.Equal("1", warning["act_ot_qty"]);
        Assert.Equal("", warning["unallocated_actual_quantity"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Both_invalid_portions_export_header_only_15_and_lossless_companion_on_disk_and_memory(bool disk)
    {
        using var output = new TemporaryOutput();
        XerDataStore store = Store();
        AddAssignment(store, "A1", "T1", actual: "2", overtime: "1", remaining: "7");
        Set(store, "TASKRSRC", "act_start_date", "actual, \"bad\"");
        Set(store, "TASKRSRC", "reend_date", "remaining, \"bad\"");
        var service = new ProcessingService();
        Dictionary<string, byte[]> files;
        if (disk)
        {
            StandardDiskExportResult result = await service.ExportTablesWithDiagnosticsAsync(store, [Distribution], output.Path, null, CancellationToken.None);
            Assert.Equal(2, result.WarningCount);
            files = result.Files.ToDictionary(path => Path.GetFileNameWithoutExtension(path), File.ReadAllBytes);
        }
        else
        {
            StandardMemoryExportResult result = await service.ExportTablesToMemoryWithDiagnosticsAsync(store, [Distribution], null, CancellationToken.None);
            Assert.Equal(2, result.WarningCount);
            files = result.Files.ToDictionary(pair => pair.Key, pair => pair.Value);
        }
        Assert.Equal(2, files.Count);
        Assert.Single(Csv(files[Distribution]));
        var warnings = CsvRows(files[XerDataQuality.TableName]);
        Assert.Equal(2, warnings.Count);
        var actual = Assert.Single(warnings, row => row["allocation_portion"] == "Actual");
        var remaining = Assert.Single(warnings, row => row["allocation_portion"] == "Remaining");
        Assert.Equal("3.0000", actual["unallocated_actual_quantity"]);
        Assert.Equal("7.0000", remaining["unallocated_remaining_quantity"]);
        Assert.Equal("actual, \"bad\"", actual["act_start_date"]);
        Assert.Equal("remaining, \"bad\"", remaining["reend_date"]);
        Assert.All(warnings, row => Assert.Equal(Source, row["FileName"]));
        Assert.DoesNotContain(Token, Encoding.UTF8.GetString(files[XerDataQuality.TableName]), StringComparison.Ordinal);
        Assert.Equal("FileName", Csv(files[XerDataQuality.TableName])[0][^1]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Duplicate_assignment_identity_warns_for_every_occurrence_without_choosing_first_or_last(bool reverse)
    {
        XerDataStore store = Store();
        foreach (string quantity in reverse ? new[] { "5", "3" } : new[] { "3", "5" })
            AddAssignment(store, "DUPLICATE", "T1", remaining: quantity);
        AddAssignment(store, "VALID", "T1", remaining: "7");
        var transformer = new XerTransformer(store);

        var rows = Rows(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()));
        var warnings = Rows(transformer.CreateDataQualityTable());

        Assert.Equal(7m, Quantity(rows));
        Assert.Equal(2, warnings.Count);
        Assert.All(warnings, row => AssertWarning(row, "Remaining", "ASSIGNMENT_CONTEXT_INVALID"));
        Assert.Equal(new[] { "1", "2" }, warnings.Select(row => row["source_row_number"]));
        Assert.Equal(reverse ? new[] { "5", "3" } : new[] { "3", "5" }, warnings.Select(row => row["remain_qty"]));
        Assert.Equal(15m, Quantity(rows) + warnings.Sum(row => Number(row["unallocated_remaining_quantity"])));
    }

    [Theory]
    [InlineData("TASKRSRC", "task_id", "missing", "ASSIGNMENT_CONTEXT_INVALID")]
    [InlineData("TASKRSRC", "rsrc_id", "missing", "ASSIGNMENT_CONTEXT_INVALID")]
    [InlineData("TASKRSRC", "taskrsrc_id", "", "ASSIGNMENT_CONTEXT_INVALID")]
    [InlineData("TASKRSRC", "proj_id", "missing", "ASSIGNMENT_CONTEXT_INVALID")]
    [InlineData("TASK", "clndr_id", "missing", "RESOURCE_CALENDAR_INVALID")]
    [InlineData("CALENDAR", "clndr_data", "bad-calendar", "RESOURCE_CALENDAR_INVALID")]
    public void Invalid_context_or_calendar_preserves_both_portions_without_guessing_identities(
        string table, string field, string raw, string code)
    {
        XerDataStore store = Store();
        AddAssignment(store, "BAD", "T1", actual: "3", remaining: "7");
        Set(store, table, field, raw);
        var transformer = new XerTransformer(store);

        Assert.Empty(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()).Rows);
        var warnings = Rows(transformer.CreateDataQualityTable());

        Assert.Equal(2, warnings.Count);
        Assert.All(warnings, row =>
        {
            Assert.Equal(code, row["issue_code"]);
            Assert.Equal("1", row["source_row_number"]);
            Assert.NotEmpty(row["message"]);
        });
        Assert.Equal("3.0000", Assert.Single(warnings, row => row["allocation_portion"] == "Actual")["unallocated_actual_quantity"]);
        Assert.Equal("7.0000", Assert.Single(warnings, row => row["allocation_portion"] == "Remaining")["unallocated_remaining_quantity"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Duplicate_or_missing_calendar_identity_only_affects_assignments_that_require_it(bool blankIdentity)
    {
        XerDataStore store = Store();
        XerTable calendar = store.GetTable("CALENDAR")!;
        string[] bad = calendar.Rows[0].Fields.ToArray();
        if (blankIdentity) bad[calendar.FieldIndexes["clndr_id"]] = "";
        calendar.AddRow(new DataRow(bad, Source, Token, Source));
        string[] valid = bad.ToArray();
        valid[calendar.FieldIndexes["clndr_id"]] = "C2";
        calendar.AddRow(new DataRow(valid, Source, Token, Source));
        AddTask(store, "T2", "TK_Active");
        XerTable tasks = store.GetTable("TASK")!;
        tasks.Rows[1].Fields[tasks.FieldIndexes["clndr_id"]] = "C2";
        AddAssignment(store, "A1", "T1", remaining: "3");
        AddAssignment(store, "A2", "T2", remaining: "7");
        var transformer = new XerTransformer(store);

        var rows = Rows(Assert.IsType<XerTable>(transformer.Create15XerResourceDistribution()));
        var warnings = Rows(transformer.CreateDataQualityTable());

        if (blankIdentity)
        {
            // The unreferenced calendar row cannot invalidate C1 or C2.
            Assert.Empty(warnings);
            Assert.Equal(10m, Quantity(rows));
        }
        else
        {
            var warning = Assert.Single(warnings);
            AssertWarning(warning, "Remaining", "RESOURCE_CALENDAR_INVALID");
            Assert.Equal("A1", warning["taskrsrc_id"]);
            Assert.Equal("3.0000", warning["unallocated_remaining_quantity"]);
            Assert.Equal(7m, Quantity(rows));
            Assert.All(rows, row => Assert.Equal(Source + ".T2", row["task_id_key"]));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Repeated_filename_path_hash_and_native_ids_remain_independent_ordered_occurrences(bool fromDisk)
    {
        using var output = new TemporaryOutput();
        XerDataStore store = Store();
        Set(store, "TASK", "status_code", "TK_Complete");
        AddAssignment(store, "A1", "T1", actual: "3", remaining: "7");
        byte[] bytes = XerBytes(store);
        var service = new ProcessingService();
        XerDataStore parsed;
        if (fromDisk)
        {
            string path = Path.Combine(output.Path, "same.xer");
            await File.WriteAllBytesAsync(path, bytes);
            parsed = await service.ParseMultipleXerFilesAsync([path, path], null, CancellationToken.None);
        }
        else
        {
            using var first = new MemoryStream(bytes);
            using var second = new MemoryStream(bytes);
            parsed = await service.ParseXerStreamsAsync([(first, "same.xer"), (second, "same.xer")], null, CancellationToken.None);
        }
        var result = await service.ExportTablesToMemoryWithDiagnosticsAsync(parsed, [Distribution], null, CancellationToken.None);
        var warnings = CsvRows(result.Files[XerDataQuality.TableName]);
        var rows = CsvRows(result.Files[Distribution]);

        Assert.Equal(2, result.WarningCount);
        Assert.Equal(new[] { "same.xer#source-000001", "same.xer#source-000002" }, warnings.Select(row => row["source_namespace"]));
        Assert.All(warnings, row =>
        {
            AssertWarning(row, "Remaining", "REMAINING_ON_COMPLETED");
            Assert.Equal("1", row["source_row_number"]);
            Assert.Equal("same.xer", row["FileName"]);
            Assert.Equal("7.0000", row["unallocated_remaining_quantity"]);
        });
        Assert.Equal(6m, Quantity(rows));
        Assert.Equal(2, rows.Select(row => row["task_id_key"]).Distinct().Count());
    }

    private static void AssertWarning(IReadOnlyDictionary<string, string> row, string portion, string code)
    {
        Assert.Equal("1.2", row["diagnostic_schema_version"]);
        Assert.Equal("Warning", row["severity"]);
        Assert.Equal(portion, row["allocation_portion"]);
        Assert.Equal(code, row["issue_code"]);
        Assert.Equal(Distribution, row["table_name"]);
        Assert.NotEmpty(row["message"]);
    }

    private static XerDataStore Store()
    {
        var store = new XerDataStore();
        AddTable(store, "PROJECT", ["proj_id", "clndr_id", "last_recalc_date"], ["P1", "C1", "2026-07-25 08:00"]);
        AddTable(store, "TASK", ["task_id", "proj_id", "clndr_id", "task_type", "status_code", "task_code", "duration_type"]);
        AddTask(store, "T1", "TK_Active");
        AddTable(store, "RSRC", ["rsrc_id", "clndr_id", "rsrc_type", "rsrc_name", "rsrc_short_name"],
            ["R1", "C1", "RT_Labor", "Crew, \"one\"", "CREW"]);
        AddTable(store, "CALENDAR", ["clndr_id", "clndr_name", "clndr_type", "day_hr_cnt", "clndr_data"],
            ["C1", "Calendar", "CA_Project", "8", P6TestCalendars.WorkWeek()]);
        AddTable(store, "TASKRSRC", ["taskrsrc_id", "task_id", "rsrc_id", "proj_id", "act_reg_qty", "act_ot_qty",
            "remain_qty", "act_start_date", "act_end_date", "restart_date", "reend_date", "curv_id", "remain_crv"]);
        return store;
    }

    private static void AddTask(XerDataStore store, string task, string status) =>
        store.GetTable("TASK")!.AddRow(new DataRow([task, "P1", "C1", "TT_Task", status, task, "DT_FixedDrtn"], Source, Token, Source));

    private static void AddAssignment(XerDataStore store, string id, string task, string actual = "0", string overtime = "0", string remaining = "0") =>
        store.GetTable("TASKRSRC")!.AddRow(new DataRow([id, task, "R1", "P1", actual, overtime, remaining,
            "2026-07-20 08:00", "2026-07-21 17:00", "2026-07-27 08:00", "2026-07-28 17:00", "", ""], Source, Token, Source));

    private static void AddTable(XerDataStore store, string name, string[] headers, params string[][] rows)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        foreach (string[] row in rows) table.AddRow(new DataRow(row, Source, Token, Source));
        store.AddTable(table);
    }

    private static void Set(XerDataStore store, string name, string field, string raw)
    {
        XerTable table = store.GetTable(name)!;
        Assert.Single(table.Rows).Fields[table.FieldIndexes[field]] = raw;
    }

    private static decimal Number(string raw) => decimal.Parse(raw, CultureInfo.InvariantCulture);
    private static decimal Quantity(IEnumerable<Dictionary<string, string>> rows) => rows.Sum(row => Number(row["monthly_quantity"]));
    private static IReadOnlyList<Dictionary<string, string>> Rows(XerTable table) => table.Rows.Select(row =>
        table.Headers!.Zip(row.Fields).ToDictionary(pair => pair.First, pair => pair.Second)).ToArray();
    private static IReadOnlyList<Dictionary<string, string>> CsvRows(byte[] bytes)
    {
        var csv = Csv(bytes);
        return csv.Skip(1).Select(row => csv[0].Zip(row).ToDictionary(pair => pair.First, pair => pair.Second)).ToArray();
    }

    private static IReadOnlyList<string[]> Csv(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: true)
        { TextFieldType = FieldType.Delimited, HasFieldsEnclosedInQuotes = true, TrimWhiteSpace = false };
        parser.SetDelimiters(",");
        var rows = new List<string[]>();
        while (!parser.EndOfData) rows.Add(parser.ReadFields()!);
        return rows;
    }

    private static byte[] XerBytes(XerDataStore store)
    {
        var text = new StringBuilder();
        foreach (string name in store.TableNames)
        {
            XerTable table = store.GetTable(name)!;
            text.Append("%T\t").AppendLine(name);
            text.Append("%F\t").AppendLine(string.Join('\t', table.Headers!));
            foreach (DataRow row in table.Rows) text.Append("%R\t").AppendLine(string.Join('\t', row.Fields));
        }
        text.AppendLine("%E");
        return Encoding.UTF8.GetBytes(text.ToString());
    }

    private sealed class TemporaryOutput : IDisposable
    {
        private readonly DirectoryInfo _directory = Directory.CreateTempSubdirectory("xer-remaining-warning-");
        public string Path => _directory.FullName;
        public void Dispose() => _directory.Delete(true);
    }
}
