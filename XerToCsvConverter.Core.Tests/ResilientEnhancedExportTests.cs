using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.VisualBasic.FileIO;

namespace XerToCsvConverter.Core.Tests;

public sealed class ResilientEnhancedExportTests
{
    private static readonly string[] Names = ["01_XER_TASK", "02_XER_PROJECT", "03_XER_PROJWBS", "04_XER_BASELINE",
        "06_XER_PREDECESSOR", "07_XER_ACTVTYPE", "08_XER_ACTVCODE", "09_XER_TASKACTV", "10_XER_CALENDAR",
        "11_XER_CALENDAR_DETAILED", "12_XER_RSRC", "13_XER_TASKRSRC", "14_XER_UMEASURE", "15_XER_RESOURCE_DISTRIBUTION"];

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Every_numbered_table_is_published_with_available_rows_despite_unavailable_lookups(bool disk)
    {
        var store = new XerDataStore();
        Add(store, "TASK", ["task_id", "proj_id", "clndr_id", "status_code", "task_name", "remain_drtn_hr_cnt"],
            ["T1", "P1", "missing", "TK_NotStart", "Keep this activity", "8"],
            ["T2", "P1", "missing", "TK_NotStart", "Also keep this activity", "bad-number"]);
        Add(store, "TASKPRED", ["task_pred_id", "task_id", "pred_task_id", "pred_type", "lag_hr_cnt"],
            ["R1", "T2", "T1", "PR_FS", "0"], ["R2", "unknown", "T1", "bad-type", "bad-lag"]);
        Add(store, "PROJWBS", ["wbs_id", "parent_wbs_id", "proj_id"], ["W1", "W2", "P1"], ["W2", "W1", "P1"]);
        Add(store, "RSRC", ["rsrc_id", "rsrc_name"], ["R1", "Keep resource"]);
        Add(store, "TASKRSRC", ["taskrsrc_id", "task_id", "rsrc_id", "remain_qty"], ["A1", "T1", "R1", "10"]);
        var service = new ProcessingService();
        var memory = await service.ExportTablesToMemoryWithDiagnosticsAsync(store, Names.ToList(), null, default);
        Assert.Equal(15, memory.Files.Count);
        Assert.All(Names, name => Assert.True(memory.Files.ContainsKey(name), name));
        Assert.Equal(2, Rows(memory.Files["01_XER_TASK"]).Count);
        Assert.Equal(2, Rows(memory.Files["06_XER_PREDECESSOR"]).Count);
        Assert.Equal(2, Rows(memory.Files["03_XER_PROJWBS"]).Count);
        Assert.Single(Rows(memory.Files["12_XER_RSRC"]));
        Assert.Single(Rows(memory.Files["13_XER_TASKRSRC"]));
        Assert.All(Rows(memory.Files["01_XER_TASK"]), row => Assert.Equal("", row["Remaining Duration"]));
        var warnings = Rows(memory.Files[XerDataQuality.TableName]);
        Assert.Equal(warnings.Count, memory.WarningCount);
        Assert.Contains(warnings, row => row["issue_code"] == "ACTIVITY_CONVERSION_UNAVAILABLE");
        Assert.Contains(warnings, row => row["issue_code"] == "SOURCE_TABLE_UNAVAILABLE");
        Assert.Contains(warnings, row => row["allocation_portion"] == "Remaining" && row["unallocated_remaining_quantity"] == "10.0000");
        Assert.DoesNotContain("private-source-token", Encoding.UTF8.GetString(memory.Files[XerDataQuality.TableName]));
        if (!disk) return;
        var folder = Directory.CreateTempSubdirectory("xer-resilient-export-");
        try
        {
            var result = await service.ExportTablesWithDiagnosticsAsync(store, Names.ToList(), folder.FullName, null, default);
            Assert.Equal(memory.WarningCount, result.WarningCount);
            Assert.Equal(15, result.Files.Count);
            foreach (var pair in memory.Files)
                Assert.Equal(pair.Value, File.ReadAllBytes(Path.Combine(folder.FullName, pair.Key + ".csv")));
        }
        finally { folder.Delete(true); }
    }

    [Fact]
    public void One_overflowing_activity_conversion_keeps_every_other_calculation_and_row()
    {
        var store = new XerDataStore();
        Add(store, "PROJECT", ["proj_id", "last_recalc_date"], ["P1", "2026-01-01"]);
        Add(store, "CALENDAR", ["clndr_id", "day_hr_cnt"], ["tiny", "0.0000000000000000000000000001"], ["normal", "8"]);
        Add(store, "TASK", ["task_id", "proj_id", "clndr_id", "task_name", "status_code", "remain_drtn_hr_cnt", "early_start_date", "early_end_date"],
            ["bad", "P1", "tiny", "Preserved", "TK_NotStart", decimal.MaxValue.ToString(CultureInfo.InvariantCulture), "malformed date", "2026-01-02"],
            ["good", "P1", "normal", "Independent", "TK_NotStart", "16", "2026-01-01", "2026-01-02"]);
        var transformer = new XerTransformer(store);
        var table = transformer.Create01XerTaskTable()!;
        Assert.Equal(2, table.RowCount);
        Assert.Equal("", table.Rows[0].Fields[table.FieldIndexes["Remaining Duration"]]);
        Assert.Equal("Preserved", table.Rows[0].Fields[table.FieldIndexes["task_name"]]);
        Assert.Equal("2026-01-02 00:00:00", table.Rows[0].Fields[table.FieldIndexes["Finish"]]);
        Assert.Equal("2.00", table.Rows[1].Fields[table.FieldIndexes["Remaining Duration"]]);
        Assert.Equal(2, transformer.CreateDataQualityTable().RowCount);
        var diagnostics = transformer.CreateDataQualityTable();
        string json = diagnostics.Rows[0].Fields[diagnostics.FieldIndexes["raw_row_json"]];
        using var evidence = JsonDocument.Parse(json);
        Assert.Contains(evidence.RootElement.EnumerateArray(), item => item.GetProperty("value").GetString() == "malformed date");
        transformer.Create01XerTaskTable();
        Assert.Equal(2, transformer.CreateDataQualityTable().RowCount); // No stale/duplicate warnings on retry.
    }

    [Fact]
    public async Task General_diagnostics_preserve_repeated_stream_occurrences_and_are_deterministic()
    {
        byte[] bytes = Encoding.UTF8.GetBytes("%T\tTASK\n%F\ttask_id\tclndr_id\tstatus_code\tremain_drtn_hr_cnt\n%R\tT1\tmissing\tTK_NotStart\t8\n%E\n");
        async Task<StandardMemoryExportResult> Run()
        {
            using var a = new MemoryStream(bytes);
            using var b = new MemoryStream(bytes);
            var service = new ProcessingService();
            var store = await service.ParseXerStreamsAsync(new[] { ((Stream)a, "same.xer"), ((Stream)b, "same.xer") }, null, default);
            return await service.ExportTablesToMemoryWithDiagnosticsAsync(store, ["01_XER_TASK"], null, default);
        }
        var first = await Run();
        var second = await Run();
        foreach (var pair in first.Files) Assert.Equal(pair.Value, second.Files[pair.Key]);
        var warnings = Rows(first.Files[XerDataQuality.TableName]);
        Assert.Equal(2, warnings.Count);
        Assert.Equal(2, warnings.Select(row => row["source_namespace"]).Distinct().Count());
        Assert.All(warnings, row => { Assert.Equal("1", row["source_row_number"]); Assert.Equal("TASK", row["source_table"]); Assert.Equal("same.xer", row["FileName"]); });
        Assert.Equal(2, Rows(first.Files["01_XER_TASK"]).Count);
    }

    private static void Add(XerDataStore store, string name, string[] headers, params string[][] rows)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        foreach (var row in rows) table.AddRow(new DataRow(row, "2601.xer") { SourceToken = "private-source-token" });
        store.AddTable(table);
    }

    private static List<Dictionary<string, string>> Rows(byte[] csv)
    {
        using var reader = new TextFieldParser(new MemoryStream(csv), Encoding.UTF8) { HasFieldsEnclosedInQuotes = true, TrimWhiteSpace = false };
        reader.SetDelimiters(",");
        string[] headers = reader.ReadFields()!;
        var rows = new List<Dictionary<string, string>>();
        while (!reader.EndOfData)
        {
            string[] values = reader.ReadFields()!;
            Assert.Equal(headers.Length, values.Length);
            rows.Add(headers.Zip(values).ToDictionary(pair => pair.First, pair => pair.Second));
        }
        return rows;
    }
}
