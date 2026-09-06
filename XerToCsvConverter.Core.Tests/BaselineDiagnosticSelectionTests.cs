using System.Text;
using Microsoft.VisualBasic.FileIO;

namespace XerToCsvConverter.Core.Tests;

public sealed class BaselineDiagnosticSelectionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Baseline_only_warnings_are_scoped_to_retained_rows_and_attributed_to_table_04(bool reverse)
    {
        var export = await new ProcessingService().ExportTablesToMemoryWithDiagnosticsAsync(Store(reverse),
            [EnhancedTableNames.XerBaseline04], null, CancellationToken.None);

        Assert.Equal(2, export.Files.Count);
        Assert.Equal(3, Read(export.Files[EnhancedTableNames.XerBaseline04]).Count);
        var warnings = Read(export.Files[XerDataQuality.TableName]);
        Assert.Equal(2, warnings.Count);
        Assert.Equal(2, export.WarningCount);
        Assert.All(warnings, warning =>
        {
            Assert.Equal(EnhancedTableNames.XerBaseline04, warning["table_name"]);
            Assert.Equal("ACTIVITY_CONVERSION_UNAVAILABLE", warning["issue_code"]);
            Assert.Equal("TASK", warning["source_table"]);
            Assert.Equal("remain_drtn_hr_cnt", warning["column_name"]);
            Assert.Equal("2607-repeat.xer", warning["FileName"]);
            Assert.Equal("bad", warning["raw_value"]);
        });
        Assert.Equal(new[] { "2607-repeat.xer#first", "2607-repeat.xer#second" },
            warnings.Select(warning => warning["source_namespace"]).OrderBy(value => value, StringComparer.Ordinal));
        Assert.Equal("2", Assert.Single(warnings, warning => warning["source_namespace"] == "2607-repeat.xer#first")["source_row_number"]);
        Assert.Equal("1", Assert.Single(warnings, warning => warning["source_namespace"] == "2607-repeat.xer#second")["source_row_number"]);
    }

    [Fact]
    public async Task Exporting_both_activity_and_baseline_reports_each_tables_affected_rows_without_changing_numbered_bytes()
    {
        var service = new ProcessingService();
        var baselineOnly = await service.ExportTablesToMemoryWithDiagnosticsAsync(Store(),
            [EnhancedTableNames.XerBaseline04], null, CancellationToken.None);
        var activitiesOnly = await service.ExportTablesToMemoryWithDiagnosticsAsync(Store(),
            [EnhancedTableNames.XerTask01], null, CancellationToken.None);
        var together = await service.ExportTablesToMemoryWithDiagnosticsAsync(Store(),
            [EnhancedTableNames.XerTask01, EnhancedTableNames.XerBaseline04], null, CancellationToken.None);

        Assert.Equal(baselineOnly.Files[EnhancedTableNames.XerBaseline04], together.Files[EnhancedTableNames.XerBaseline04]);
        Assert.Equal(activitiesOnly.Files[EnhancedTableNames.XerTask01], together.Files[EnhancedTableNames.XerTask01]);
        var warnings = Read(together.Files[XerDataQuality.TableName]);
        Assert.Equal(3, warnings.Count(row => row["table_name"] == EnhancedTableNames.XerTask01));
        Assert.Equal(2, warnings.Count(row => row["table_name"] == EnhancedTableNames.XerBaseline04));
        Assert.Equal(5, together.WarningCount);
    }

    [Fact]
    public void Repeated_baseline_generation_clears_old_mapped_warnings_and_public_diagnostic_API_remains_unfiltered()
    {
        var transformer = new XerTransformer(Store());
        XerTable activities = Assert.IsType<XerTable>(transformer.Create01XerTaskTable());
        transformer.Create04XerBaselineTable(activities);
        transformer.Create04XerBaselineTable(activities);

        Assert.Equal(5, transformer.CreateDataQualityTable().RowCount);
        Assert.Equal(2, transformer.CreateDataQualityTable([EnhancedTableNames.XerBaseline04]).RowCount);
        Assert.Equal(3, transformer.CreateDataQualityTable([EnhancedTableNames.XerTask01]).RowCount);
        Assert.Empty(transformer.CreateDataQualityTable([EnhancedTableNames.XerProject02]).Rows);

        var noMonth = new XerTable(EnhancedTableNames.XerTask01);
        noMonth.SetHeaders(activities.Headers!.ToArray());
        foreach (DataRow row in activities.Rows)
        {
            string[] fields = row.Fields.ToArray();
            fields[activities.FieldIndexes["MonthUpdate"]] = "";
            noMonth.AddRow(row.WithFields(fields));
        }
        Assert.Empty(Assert.IsType<XerTable>(transformer.Create04XerBaselineTable(noMonth)).Rows);
        Assert.Empty(transformer.CreateDataQualityTable([EnhancedTableNames.XerBaseline04]).Rows);
        Assert.Equal(3, transformer.CreateDataQualityTable().RowCount);
    }

    [Fact]
    public void Selected_table_filter_also_excludes_unselected_resource_portion_warnings()
    {
        XerDataStore store = Store();
        var assignments = new XerTable("TASKRSRC");
        assignments.SetHeaders(["taskrsrc_id", "remain_qty"]);
        assignments.AddRow(new DataRow(["A", "bad"], "2607-repeat.xer#first", "private-first", "2607-repeat.xer"));
        store.AddTable(assignments);
        var transformer = new XerTransformer(store);
        transformer.Create01XerTaskTable();
        transformer.Create15XerResourceDistribution();

        Assert.Equal(4, transformer.CreateDataQualityTable().RowCount);
        Assert.Equal(3, transformer.CreateDataQualityTable([EnhancedTableNames.XerTask01]).RowCount);
        XerTable resourceOnly = transformer.CreateDataQualityTable([EnhancedTableNames.XerResourceDist15]);
        Assert.Equal("Remaining", Assert.Single(resourceOnly.Rows).Fields[23]);
    }

    [Fact]
    public void Baseline_warning_mapping_follows_source_evidence_when_a_caller_reorders_or_subsets_activity_rows()
    {
        var transformer = new XerTransformer(Store());
        XerTable activities = Assert.IsType<XerTable>(transformer.Create01XerTaskTable());
        var subset = new XerTable(EnhancedTableNames.XerTask01);
        subset.SetHeaders(activities.Headers!.ToArray());
        // Source-local row 2 is now first and only in this caller-provided table.
        DataRow selected = Assert.Single(activities.Rows, row => row.SourceToken == "private-first"
            && row.Fields[activities.FieldIndexes["task_id"]] == "same-native-id");
        subset.AddRow(selected.WithFields(selected.Fields.ToArray()));
        Assert.Single(Assert.IsType<XerTable>(transformer.Create04XerBaselineTable(subset)).Rows);

        DataRow warning = Assert.Single(transformer.CreateDataQualityTable([EnhancedTableNames.XerBaseline04]).Rows);
        Assert.Equal("2", warning.Fields[5]);
        Assert.Equal("2607-repeat.xer#first", warning.Fields[4]);
        Assert.Equal("ACTIVITY_CONVERSION_UNAVAILABLE", warning.Fields[2]);
    }

    private static XerDataStore Store(bool reverse = false)
    {
        var store = new XerDataStore();
        var tasks = new XerTable("TASK");
        tasks.SetHeaders(["task_id", "clndr_id", "status_code", "target_drtn_hr_cnt", "remain_drtn_hr_cnt"]);
        var calendars = new XerTable("CALENDAR");
        calendars.SetHeaders(["clndr_id", "day_hr_cnt", "clndr_data"]);
        var inputs = new[]
        {
            (Token: "private-first", Namespace: "2607-repeat.xer#first", Original: "2607-repeat.xer"),
            (Token: "private-second", Namespace: "2607-repeat.xer#second", Original: "2607-repeat.xer"),
            (Token: "private-later", Namespace: "2608-later.xer", Original: "2608-later.xer")
        };
        foreach (var input in reverse ? inputs.Reverse() : inputs)
        {
            if (input.Token == "private-first")
                tasks.AddRow(new DataRow(["valid", "C", "TK_NotStart", "8", "8"], input.Namespace, input.Token, input.Original));
            tasks.AddRow(new DataRow(["same-native-id", "C", "TK_NotStart", "8", "bad"], input.Namespace, input.Token, input.Original));
            calendars.AddRow(new DataRow(["C", "8", P6TestCalendars.WorkWeek()], input.Namespace, input.Token, input.Original));
        }
        store.AddTable(tasks);
        store.AddTable(calendars);
        return store;
    }

    private static List<Dictionary<string, string>> Read(byte[] csv)
    {
        using var stream = new MemoryStream(csv, writable: false);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: true)
        { TextFieldType = FieldType.Delimited, HasFieldsEnclosedInQuotes = true, TrimWhiteSpace = false };
        parser.SetDelimiters(",");
        string[] headers = parser.ReadFields()!;
        var rows = new List<Dictionary<string, string>>();
        while (!parser.EndOfData)
            rows.Add(headers.Zip(parser.ReadFields()!).ToDictionary(pair => pair.First, pair => pair.Second));
        return rows;
    }
}
