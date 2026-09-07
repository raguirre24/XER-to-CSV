using System.Collections.Concurrent;

namespace XerToCsvConverter.Core.Tests;

public sealed class RelationshipExportContractTests
{
    private static readonly string[] AllowanceColumns =
        ["free_float", "free_float_status", "free_float_basis", "free_float_reason"];

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Standard_calculated_fields_are_adjacent_and_cannot_be_replaced_by_imported_names(bool historical)
    {
        XerDataStore store = Store(historical);
        XerTable raw = store.GetTable("TASKPRED")!;
        string[] originalHeaders = raw.Headers!.ToArray();
        string[] originalFields = raw.Rows[0].Fields.ToArray();
        var transformer = new XerTransformer(store);
        RelationshipFloatAssessment assessment = Assert.Single(transformer.AssessRelationships());
        XerTable output = Assert.IsType<XerTable>(transformer.Create06XerPredecessor(new ConcurrentDictionary<string, XerTable>()));
        DataRow row = Assert.Single(output.Rows);
        string Value(string name) => row.Fields[output.FieldIndexes[name]];

        Assert.Equal(AllowanceColumns, output.Headers!.Skip(output.FieldIndexes["free_float"]).Take(4));
        Assert.Equal(assessment.FormattedDays, Value("free_float"));
        Assert.Equal(assessment.AllowanceStatus.ToString(), Value("free_float_status"));
        Assert.Equal(assessment.CalculationBasis, Value("free_float_basis"));
        Assert.Equal(assessment.ReasonCode, Value("free_float_reason"));
        Assert.Equal(historical ? "Historical" : "Finite", Value("free_float_status"));
        Assert.Equal("999", Value("raw_free_float"));
        Assert.Equal("spoofed status", Value("raw_free_float_status_2"));
        Assert.Equal("existing raw status", Value("raw_free_float_status"));
        Assert.Equal("spoofed basis", Value("raw_free_float_basis"));
        Assert.Equal("spoofed reason", Value("raw_free_float_reason"));
        Assert.Equal(originalHeaders, raw.Headers);
        Assert.Equal(originalFields, raw.Rows[0].Fields);
    }

    [Fact]
    public void Header_only_and_recovery_relationships_preserve_the_same_status_contract()
    {
        XerDataStore populated = Store(false);
        XerTable raw = populated.GetTable("TASKPRED")!;
        var empty = new XerDataStore();
        var emptyRelationships = new XerTable("TASKPRED");
        emptyRelationships.SetHeaders(raw.Headers!.ToArray());
        empty.AddTable(emptyRelationships);
        XerTable headerOnly = Assert.IsType<XerTable>(StandardExportSchema.CreateIfSourceEmpty(empty, EnhancedTableNames.XerPredecessor06));
        XerTable recovered = new XerTransformer(populated).RecoverEnhancedTable(EnhancedTableNames.XerPredecessor06,
            new InvalidOperationException("Injected failure"));
        Assert.Equal(headerOnly.Headers, recovered.Headers);
        Assert.Empty(headerOnly.Rows);
        DataRow row = Assert.Single(recovered.Rows);
        string Value(string name) => row.Fields[recovered.FieldIndexes[name]];
        Assert.Equal("", Value("free_float"));
        Assert.Equal("RequiresContext", Value("free_float_status"));
        Assert.Equal("UnresolvedContext", Value("free_float_basis"));
        Assert.Equal("TableGenerationFailed", Value("free_float_reason"));
        Assert.Equal("spoofed status", Value("raw_free_float_status_2"));
        Assert.Equal(AllowanceColumns, recovered.Headers!.Skip(recovered.FieldIndexes["free_float"]).Take(4));
    }

    private static XerDataStore Store(bool historical)
    {
        var store = new XerDataStore();
        Add("PROJECT", ["proj_id", "clndr_id", "last_recalc_date"], ["P1", "C1", "2026-09-01 08:00"]);
        Add("CALENDAR", ["clndr_id", "day_hr_cnt", "clndr_data"], ["C1", "8", P6TestCalendars.WorkWeek()]);
        Add("TASK", ["task_id", "proj_id", "clndr_id", "status_code", "task_type", "restart_date", "reend_date", "act_start_date", "act_end_date"],
            ["T1", "P1", "C1", historical ? "TK_Complete" : "TK_NotStart", "TT_Task", "2026-09-07 08:00", "2026-09-07 17:00",
                historical ? "2026-08-28 08:00" : "", historical ? "2026-08-28 17:00" : ""],
            ["T2", "P1", "C1", "TK_NotStart", "TT_Task", "2026-09-08 17:00", "2026-09-09 17:00", "", ""]);
        Add("TASKPRED", ["task_pred_id", "proj_id", "pred_proj_id", "task_id", "pred_task_id", "pred_type", "lag_hr_cnt",
                "free_float", "free_float_status", "free_float_basis", "free_float_reason", "raw_free_float_status"],
            ["R1", "P1", "P1", "T2", "T1", "PR_FS", "0", "999", "spoofed status", "spoofed basis", "spoofed reason", "existing raw status"]);
        return store;

        void Add(string name, string[] headers, params string[][] rows)
        {
            var table = new XerTable(name);
            table.SetHeaders(headers);
            foreach (string[] row in rows) table.AddRow(new DataRow(row, "2609.xer", sourceToken: "occurrence-1"));
            store.AddTable(table);
        }
    }
}
