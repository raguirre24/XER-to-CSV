using System.Text;

namespace XerToCsvConverter.Core.Tests;

public sealed class RawFieldProvenanceTests
{
    private const string Lag = "sched_calendar_on_relationship_lag";
    private const string Retained = "sched_retained_logic";
    private const string Override = "sched_progress_override";
    private const string Truncated = "%T\tSCHEDOPTIONS\n%F\tproj_id\tsched_retained_logic\tsched_progress_override\n%R\tP1\tY\n%E\n";
    private const string ExplicitBlank = "%T\tSCHEDOPTIONS\n%F\tsched_calendar_on_relationship_lag\tproj_id\tsched_progress_override\tsched_retained_logic\n%R\t\tP1\tN\tY\n%E\n";

    [Theory]
    [InlineData("disk", false)]
    [InlineData("disk", true)]
    [InlineData("stream", false)]
    [InlineData("stream", true)]
    [InlineData("async-stream", false)]
    [InlineData("async-stream", true)]
    public async Task Parsed_heterogeneous_rows_retain_absent_omitted_and_blank_states_in_both_merge_orders(
        string transport, bool reverse)
    {
        XerDataStore truncated = await Parse(Truncated, transport);
        XerDataStore explicitBlank = await Parse(ExplicitBlank, transport);
        var merged = new XerDataStore();
        XerDataStore[] ordered = reverse ? [explicitBlank, truncated] : [truncated, explicitBlank];
        foreach (XerDataStore store in ordered) merged.MergeStore(store);

        XerTable table = merged.GetTable("SCHEDOPTIONS")!;
        Assert.Equal(2, table.RowCount);
        DataRow shortRow = table.Rows[reverse ? 1 : 0];
        DataRow blankRow = table.Rows[reverse ? 0 : 1];
        AssertField(shortRow, Lag, XerRawFieldState.AbsentHeader, "");
        AssertField(shortRow, Override, XerRawFieldState.OmittedCell, "");
        AssertField(shortRow, Retained.ToUpperInvariant(), XerRawFieldState.Present, "Y");
        AssertField(blankRow, Lag, XerRawFieldState.Blank, "");
        AssertField(blankRow, Override, XerRawFieldState.Present, "N");
        AssertField(blankRow, Retained, XerRawFieldState.Present, "Y");
        // Both public cells are blank after alignment, but their source evidence is not equal.
        Assert.Equal("", shortRow.Fields[table.FieldIndexes[Lag]]);
        Assert.Equal("", blankRow.Fields[table.FieldIndexes[Lag]]);
        Assert.NotEqual(shortRow.SourceToken, blankRow.SourceToken);
        Assert.Equal("same.xer", shortRow.OriginalSourceFilename);
        Assert.Equal("same.xer", blankRow.OriginalSourceFilename);
        Assert.Equal(3, truncated.GetTable("SCHEDOPTIONS")!.Headers!.Length);
    }

    [Theory]
    [InlineData("disk")]
    [InlineData("stream")]
    [InlineData("async-stream")]
    public async Task Present_malformed_text_and_trailing_explicit_blank_are_not_omitted(string transport)
    {
        const string content = "%T\tSCHEDOPTIONS\n%F\tproj_id\tsched_retained_logic\tsched_progress_override\n%R\tP1\tperhaps\t\n%R\tP2\t   \tN\n%E\n";
        XerTable table = (await Parse(content, transport)).GetTable("SCHEDOPTIONS")!;
        AssertField(table.Rows[0], Retained, XerRawFieldState.Present, "perhaps");
        AssertField(table.Rows[0], Override, XerRawFieldState.Blank, "");
        AssertField(table.Rows[1], Retained, XerRawFieldState.Blank, "   ");
        AssertField(table.Rows[1], Override, XerRawFieldState.Present, "N");
    }

    [Fact]
    public void Constructor_snapshot_is_immutable_despite_current_array_and_header_mutation()
    {
        string[] values = ["P1", "Y"];
        string[] headers = ["proj_id", Retained, Override];
        var row = new DataRow(values, "same.xer", "occurrence");
        values[1] = "N";
        var table = new XerTable("SCHEDOPTIONS");
        table.SetHeaders(headers);
        table.AddRow(row);
        headers[1] = "changed_header";
        DataRow captured = table.Rows[0];
        captured.Fields[1] = "still not raw";

        AssertField(captured, Retained, XerRawFieldState.Present, "Y");
        AssertField(captured, Override, XerRawFieldState.OmittedCell, "");
        AssertField(captured, "changed_header", XerRawFieldState.AbsentHeader, "");
        Assert.Equal("still not raw", captured.Fields[1]);
        Assert.Equal(new XerRawField("still not raw", XerRawFieldState.Present), captured.GetEvaluationField(Retained));
        Assert.Equal("occurrence", captured.SourceToken);
    }

    [Fact]
    public void AddRows_alignment_copies_and_source_rebinding_keep_original_field_evidence()
    {
        var original = new XerTable("SCHEDOPTIONS");
        original.SetHeaders(["proj_id", Retained, Override]);
        original.AddRows([new DataRow(["P1", "Y"], "same.xer", "original-occurrence")]);
        DataRow captured = Assert.Single(original.Rows);
        Assert.Equal(3, captured.Fields.Length);

        var transformed = new XerTable("SCHEDOPTIONS");
        transformed.SetHeaders([Lag, "proj_id", Override, Retained]);
        transformed.AddRows([captured.WithFields(["rcal_24Hour", "other project", "N", "N"]) with
        {
            SourceToken = "rebound-occurrence", SourceFilename = "new-public-name", OriginalSourceFilename = "original name"
        }]);
        DataRow copied = Assert.Single(transformed.Rows);
        AssertField(copied, "proj_id", XerRawFieldState.Present, "P1");
        AssertField(copied, Retained, XerRawFieldState.Present, "Y");
        AssertField(copied, Override, XerRawFieldState.OmittedCell, "");
        AssertField(copied, Lag, XerRawFieldState.AbsentHeader, "");
        Assert.Equal(new XerRawField("N", XerRawFieldState.Present), copied.GetEvaluationField(Retained));
        Assert.Equal(new XerRawField("other project", XerRawFieldState.Present), copied.GetEvaluationField("proj_id"));
        Assert.Equal(new XerRawField("", XerRawFieldState.OmittedCell), copied.GetEvaluationField(Override));
        Assert.Equal(new XerRawField("", XerRawFieldState.AbsentHeader), copied.GetEvaluationField(Lag));
        Assert.Equal("rebound-occurrence", copied.SourceToken);
        Assert.Equal("new-public-name", copied.SourceFilename);
        Assert.Equal("original name", copied.OriginalSourceFilename);

        var store = new XerDataStore();
        store.AddTable(transformed);
        XerDataStore rebound = XerSourceIdentity.CreateOrdered(["duplicate.xer"])[0].ApplyTo(store);
        DataRow reboundRow = Assert.Single(rebound.GetTable("SCHEDOPTIONS")!.Rows);
        Assert.NotEqual(copied.SourceToken, reboundRow.SourceToken);
        AssertField(reboundRow, Retained, XerRawFieldState.Present, "Y");
        AssertField(reboundRow, Override, XerRawFieldState.OmittedCell, "");
        AssertField(reboundRow, Lag, XerRawFieldState.AbsentHeader, "");
        Assert.Equal(new XerRawField("N", XerRawFieldState.Present), reboundRow.GetEvaluationField(Retained));
    }

    [Fact]
    public void Parsed_data_cell_edits_are_evaluated_without_altering_original_blank_or_present_evidence()
    {
        var table = new XerTable("SCHEDOPTIONS");
        table.SetHeaders(["proj_id", Retained, Override, Lag]);
        table.AddRow(new DataRow(["P1", "Y", "", "rcal_Predecessor"], "same.xer"));
        DataRow row = table.Rows[0];
        row.Fields[1] = "N";
        row.Fields[2] = "Y";
        row.Fields[3] = "";
        AssertField(row, Retained, XerRawFieldState.Present, "Y");
        AssertField(row, Override, XerRawFieldState.Blank, "");
        AssertField(row, Lag, XerRawFieldState.Present, "rcal_Predecessor");
        Assert.Equal(new XerRawField("N", XerRawFieldState.Present), row.GetEvaluationField(Retained));
        Assert.Equal(new XerRawField("Y", XerRawFieldState.Present), row.GetEvaluationField(Override));
        Assert.Equal(new XerRawField("", XerRawFieldState.Blank), row.GetEvaluationField(Lag));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Ordered_stream_occurrences_preserve_provenance_across_repeated_names_and_hashes(bool reverse)
    {
        string[] content = reverse ? [ExplicitBlank, Truncated, ExplicitBlank] : [Truncated, ExplicitBlank, Truncated];
        using var first = Bytes(content[0]);
        using var second = Bytes(content[1]);
        using var third = Bytes(content[2]);
        XerDataStore store = await new ProcessingService().ParseXerStreamsAsync(
            [((Stream)first, "same.xer"), ((Stream)second, "same.xer"), ((Stream)third, "same.xer")], null, CancellationToken.None);
        DataRow[] rows = store.GetTable("SCHEDOPTIONS")!.Rows.ToArray();

        Assert.Equal(3, rows.Select(row => row.SourceToken).Distinct().Count());
        Assert.Equal(3, rows.Select(row => row.SourceFilename).Distinct().Count());
        for (int i = 0; i < rows.Length; i++)
        {
            Assert.EndsWith($":source-{i + 1:D6}", rows[i].SourceToken);
            Assert.Equal("same.xer", rows[i].OriginalSourceFilename);
            bool shortRow = content[i] == Truncated;
            AssertField(rows[i], Lag, shortRow ? XerRawFieldState.AbsentHeader : XerRawFieldState.Blank, "");
            AssertField(rows[i], Override, shortRow ? XerRawFieldState.OmittedCell : XerRawFieldState.Present, shortRow ? "" : "N");
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Ordered_disk_occurrences_preserve_evidence_across_repeated_paths(bool reverse)
    {
        string directory = Path.Combine(Path.GetTempPath(), "xer-provenance-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            string shortPath = Path.Combine(directory, "short.xer");
            string blankPath = Path.Combine(directory, "blank.xer");
            await File.WriteAllTextAsync(shortPath, Truncated);
            await File.WriteAllTextAsync(blankPath, ExplicitBlank);
            string[] paths = reverse ? [blankPath, shortPath, blankPath] : [shortPath, blankPath, shortPath];
            XerDataStore store = await new ProcessingService().ParseMultipleXerFilesAsync(paths.ToList(), null, CancellationToken.None);
            DataRow[] rows = store.GetTable("SCHEDOPTIONS")!.Rows.ToArray();
            Assert.Equal(3, rows.Select(row => row.SourceToken).Distinct().Count());
            for (int i = 0; i < rows.Length; i++)
            {
                bool shortRow = paths[i] == shortPath;
                AssertField(rows[i], Lag, shortRow ? XerRawFieldState.AbsentHeader : XerRawFieldState.Blank, "");
                AssertField(rows[i], Override, shortRow ? XerRawFieldState.OmittedCell : XerRawFieldState.Present, shortRow ? "" : "N");
                Assert.EndsWith($":source-{i + 1:D6}", rows[i].SourceToken);
            }
        }
        finally { Directory.Delete(directory, recursive: true); }
    }

    private static void AssertField(DataRow row, string name, XerRawFieldState state, string value)
    {
        XerRawField field = row.GetRawField(name);
        Assert.Equal(state, field.State);
        Assert.Equal(value, field.RawValue);
    }

    private static MemoryStream Bytes(string content) => new(Encoding.UTF8.GetBytes(content));

    private static async Task<XerDataStore> Parse(string content, string transport)
    {
        var parser = new XerParser();
        if (transport == "disk")
        {
            string directory = Path.Combine(Path.GetTempPath(), "xer-provenance-tests-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            try
            {
                string path = Path.Combine(directory, "same.xer");
                await File.WriteAllTextAsync(path, content);
                return parser.ParseXerFile(path, null, CancellationToken.None);
            }
            finally { Directory.Delete(directory, recursive: true); }
        }
        using var stream = Bytes(content);
        return transport == "async-stream"
            ? await parser.ParseXerStreamAsync(stream, "same.xer", null, CancellationToken.None)
            : parser.ParseXerStream(stream, "same.xer", null, CancellationToken.None);
    }
}
