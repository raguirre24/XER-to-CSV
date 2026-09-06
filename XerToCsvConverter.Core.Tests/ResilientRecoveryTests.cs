using System.Text;
using Microsoft.VisualBasic.FileIO;

namespace XerToCsvConverter.Core.Tests;

public sealed class ResilientRecoveryTests
{
    [Fact]
    public void Relationship_recovery_preserves_colliding_raw_fields_without_promoting_them_to_calculated_values()
    {
        var store = new XerDataStore();
        XerTable raw = Add(store, "TASKPRED",
            ["task_pred_id", "task_id", "pred_task_id", "free_float", "raw_free_float", "Start", "FileName", "raw_FileName"],
            ["edge", "successor", "predecessor", "123.75", "existing float text", "raw start text", "raw filename text", "existing filename text"]);
        var transformer = new XerTransformer(store);

        XerTable recovered = transformer.RecoverEnhancedTable(EnhancedTableNames.XerPredecessor06,
            new InvalidOperationException("Injected transform failure."));

        var values = Record(recovered, Assert.Single(recovered.Rows));
        Assert.Equal("edge", values["task_pred_id"]);
        Assert.Equal("successor", values["task_id"]);
        Assert.Equal("predecessor", values["pred_task_id"]);
        Assert.Equal("123.75", values["raw_free_float_2"]);
        Assert.Equal("existing float text", values["raw_free_float"]);
        Assert.Equal("raw start text", values["raw_Start"]);
        Assert.Equal("raw filename text", values["raw_FileName_2"]);
        Assert.Equal("existing filename text", values["raw_FileName"]);
        Assert.Equal("", values["free_float"]);
        Assert.Equal("", values["Start"]);
        Assert.Equal("", values["task_id_key"]);
        Assert.DoesNotContain("FileName", recovered.Headers!, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(raw.Headers!.Length + StandardExportSchema.AddedFields(EnhancedTableNames.XerPredecessor06).Length,
            recovered.Headers!.Length);
        Assert.Equal(raw.Rows[0].Fields, recovered.Rows[0].Fields.Take(raw.Headers.Length));
        var warning = Assert.Single(transformer.CreateDataQualityTable().Rows,
            row => row.Fields[2] == "TABLE_GENERATION_FAILED");
        Assert.Contains("123.75", warning.Fields[33], StringComparison.Ordinal);
        Assert.DoesNotContain("private-token", warning.Fields[22], StringComparison.Ordinal);
    }

    [Fact]
    public void Fixed_task_recovery_keeps_raw_contract_fields_and_diagnostic_evidence_but_all_derived_values_unknown()
    {
        var store = new XerDataStore();
        Add(store, "TASK",
            ["task_id", "task_name", "act_start_date", "Start", "Finish", "total_float", "Free Float", "%", "FileName"],
            ["T1", "Original task", "2026-08-01 08:00", "raw start", "raw finish", "999", "888", "77", "raw filename"]);
        var transformer = new XerTransformer(store);

        XerTable recovered = transformer.RecoverEnhancedTable(EnhancedTableNames.XerTask01,
            new InvalidOperationException("Injected transform failure."));

        var values = Record(recovered, Assert.Single(recovered.Rows));
        Assert.Equal("T1", values["task_id"]);
        Assert.Equal("Original task", values["task_name"]);
        Assert.Equal("2026-08-01 08:00", values["act_start_date"]);
        foreach (string column in new[] { "Start", "Finish", "total_float", "Free Float", "%", "task_id_key", "MonthUpdate" })
            Assert.Equal("", values[column]);
        Assert.Equal(XerTransformer.TaskColumns01, recovered.Headers);
        var warning = Assert.Single(transformer.CreateDataQualityTable().Rows);
        foreach (string rawValue in new[] { "raw start", "raw finish", "999", "888", "77", "raw filename" })
            Assert.Contains(rawValue, warning.Fields[33], StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("CALENDAR", "10_XER_CALENDAR", "clndr_id_key")]
    [InlineData("TASKPRED", "06_XER_PREDECESSOR", "free_float")]
    [InlineData("PROJWBS", "03_XER_PROJWBS", "wbs_id_key")]
    public async Task Empty_source_header_collisions_do_not_block_other_available_tables(
        string sourceName, string enhancedName, string derivedColumn)
    {
        var store = new XerDataStore();
        Add(store, sourceName, ["native_id", "FileName", "raw_FileName", derivedColumn, "raw_" + derivedColumn]);
        Add(store, "PROJECT", ["proj_id", "proj_short_name"], ["P1", "Retained project"]);

        var export = await new ProcessingService().ExportTablesToMemoryWithDiagnosticsAsync(store,
            [enhancedName, EnhancedTableNames.XerProject02], null, CancellationToken.None);

        Assert.Equal(3, export.Files.Count);
        Assert.Contains("Retained project", Encoding.UTF8.GetString(export.Files[EnhancedTableNames.XerProject02]), StringComparison.Ordinal);
        using var stream = new MemoryStream(export.Files[enhancedName], writable: false);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: true)
        { TextFieldType = FieldType.Delimited, HasFieldsEnclosedInQuotes = true };
        parser.SetDelimiters(",");
        string[] headers = parser.ReadFields()!;
        Assert.True(parser.EndOfData);
        Assert.Equal(1, headers.Count(header => header.Equals("FileName", StringComparison.OrdinalIgnoreCase)));
        Assert.Contains("raw_FileName_2", headers);
        Assert.Contains("raw_" + derivedColumn + "_2", headers);
        Assert.Contains(derivedColumn, headers);
        Assert.Equal(headers.Length, headers.Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.Contains(XerDataQuality.TableName, export.Files.Keys);
    }

    private static XerTable Add(XerDataStore store, string name, string[] headers, params string[][] rows)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        foreach (string[] fields in rows)
            table.AddRow(new DataRow(fields, "2608-source.xer", "private-token", "2608-source.xer"));
        store.AddTable(table);
        return table;
    }

    private static Dictionary<string, string> Record(XerTable table, DataRow row) => table.Headers!
        .Zip(row.Fields).ToDictionary(pair => pair.First, pair => pair.Second, StringComparer.OrdinalIgnoreCase);
}
