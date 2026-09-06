using XerToCsvConverter.ProgrammeReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ReviewDataQualityCollectorTests
{
    [Fact]
    public void Rebound_clones_keep_source_local_ordinals_and_invalid_cell_state_independent()
    {
        var store = new XerDataStore();
        var raw = new XerTable("TASK");
        raw.SetHeaders(["task_id", "early_start_date"]);
        raw.AddRow(new DataRow(["T1", "bad-date"], "same.xer", "occurrence-a"));
        DataRow first = raw.Rows[0];
        raw.AddRow(new DataRow(["T0", "2026-01-01"], "same.xer", "occurrence-b"));
        raw.AddRow(first with { SourceToken = "occurrence-b" });
        store.AddTable(raw);
        Assert.Same(first.RawEvidenceIdentity, raw.Rows[2].RawEvidenceIdentity);

        var generated = new XerTable("01_XER_TASK");
        generated.SetHeaders(["task_id_key", "early_start_date"]);
        string[] sharedFields = ["same.xer.T1", "bad-date"];
        generated.AddRow(first.WithFields(sharedFields));
        generated.AddRow(raw.Rows[1].WithFields(["same.xer.T0", "2026-01-01"]));
        generated.AddRow(raw.Rows[2].WithFields(sharedFields));
        var collector = new ReviewDataQualityCollector(store);
        ReviewRowEvidence a = collector.ForRow(generated, generated.Rows[0]);
        ReviewRowEvidence b = collector.ForRow(generated, generated.Rows[2]);
        Assert.Equal(1, a.Ordinal);
        Assert.Equal(2, b.Ordinal);
        Assert.Equal("", collector.Evaluate(a, "early_start_date", "bad-date",
            () => throw new ProgrammeReviewValidationException("Invalid date")));
        Assert.True(collector.HasInvalidColumn(a, "early_start_date"));
        Assert.False(collector.HasInvalidColumn(b, "early_start_date"));
        collector.Warn(b, "TEST_WARNING", "Second occurrence");
        int ordinal = Array.IndexOf(XerDataQuality.Columns, "source_row_number");
        Assert.Equal(new[] { "1", "2" }, collector.Rows.Select(row => row.Fields[ordinal]));
        Assert.Equal(new[] { "occurrence-a", "occurrence-b" }, collector.Rows.Select(row => row.SourceToken));
    }
}
