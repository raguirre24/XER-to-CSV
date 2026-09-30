using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ReviewPerformanceRegressionTests
{
    private static readonly string[] EdgeValues =
        ["", "A", "AA", "A\u001f", "A\u001fB", "\u001f", "\u001f\u001f", "\0", "\0A", " ", "é", "中", "😀", "\ud800", "\udfff"];

    [Fact]
    public void Virtual_joined_order_matches_published_ordinal_keys_and_stable_ties()
    {
        string[] columns = ["a", "b", "c"];
        var random = new Random(43091);
        var rows = new List<SortRow>();
        for (int index = 0; index < 300; index++)
        {
            var values = columns.ToDictionary(column => column,
                _ => EdgeValues[random.Next(EdgeValues.Length)]);
            rows.Add(new SortRow(index, index % 3, values));
            if (index % 7 == 0)
                rows.Add(new SortRow(index + 1000, index % 3, new Dictionary<string, string>(values)));
        }
        var expected = rows.OrderBy(row => string.Join('\u001f', columns.Take(2).Select(column => row.Values[column])), StringComparer.Ordinal)
            .ThenBy(row => string.Join('\u001f', columns.Select(column => row.Values[column])), StringComparer.Ordinal)
            .ThenBy(row => row.Source).ToArray();
        var actual = ReviewOutputOrdering.Sort(rows, columns.Take(2).ToArray(), columns,
            row => row.Values, (left, right) => left.Source.CompareTo(right.Source));
        Assert.Equal(expected.Select(row => row.Ordinal), actual.Select(row => row.Ordinal));

        foreach (SortRow left in rows.Take(35))
        foreach (SortRow right in rows.TakeLast(35))
        {
            int published = StringComparer.Ordinal.Compare(string.Join('\u001f', columns.Select(column => left.Values[column])),
                string.Join('\u001f', columns.Select(column => right.Values[column])));
            Assert.Equal(Math.Sign(published), Math.Sign(ReviewOutputOrdering.CompareJoined(left.Values, right.Values, columns)));
        }
        Assert.Equal(0, ReviewOutputOrdering.CompareJoined(rows[0].Values, rows[1].Values, Array.Empty<string>()));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Profile_sort_preserves_published_separator_and_prefix_order(bool tender)
    {
        if (tender)
        {
            var source = TenderReviewNamingTests.Source(0, "same.xer", "2026-09-05");
            var resolved = TenderReviewNaming.Resolve(TenderReviewNamingTests.Request([source]), item => item.SourceSha256!);
            TenderReviewTableContract contract = TenderReviewContract.GetTable("15_XER_RESOURCE_DISTRIBUTION");
            var rows = EdgeValues.Select((value, index) => new TenderReviewOutputRow(resolved.Sources[0],
                Values(contract.Columns.Select(column => column.Name), value, index))).ToList();
            rows.Insert(2, new TenderReviewOutputRow(resolved.Sources[0], new Dictionary<string, string>(rows[1].Values)));
            var expected = rows.OrderBy(row => string.Join('\u001f', contract.SortColumns.Select(column => row.Values[column])), StringComparer.Ordinal)
                .ThenBy(row => string.Join('\u001f', contract.Columns.Select(column => row.Values[column.Name])), StringComparer.Ordinal)
                .ThenBy(row => row.Source.InputIndex).ToArray();
            var actual = TenderReviewTransformer.Sort(contract, rows);
            for (int index = 0; index < expected.Length; index++) Assert.Same(expected[index], actual[index]);
        }
        else
        {
            var snapshot = ProgrammeReviewNamingTests.Snapshot("same.xer", ProgrammeReviewSnapshotKind.Baseline,
                "BL01", "2026-01-31", "2026-01-31");
            var resolved = ProgrammeReviewNaming.Resolve(ProgrammeReviewNamingTests.Request([snapshot]), item => item.SourceSha256!);
            ProgrammeReviewTableContract contract = ProgrammeReviewContract.GetTable("15_XER_RESOURCE_DISTRIBUTION");
            var rows = EdgeValues.Select((value, index) => new ProgrammeReviewOutputRow(resolved.Snapshots[0],
                Values(contract.Columns.Select(column => column.Name), value, index))).ToList();
            rows.Insert(2, new ProgrammeReviewOutputRow(resolved.Snapshots[0], new Dictionary<string, string>(rows[1].Values)));
            var expected = rows.OrderBy(row => string.Join('\u001f', contract.SortColumns.Select(column => row.Values[column])), StringComparer.Ordinal)
                .ThenBy(row => string.Join('\u001f', contract.Columns.Select(column => row.Values[column.Name])), StringComparer.Ordinal)
                .ThenBy(row => row.Snapshot.CanonicalXerFilename, StringComparer.Ordinal).ToArray();
            var actual = ProgrammeReviewTransformer.Sort(contract, rows);
            for (int index = 0; index < expected.Length; index++) Assert.Same(expected[index], actual[index]);
        }
    }

    [Fact]
    public void Virtual_joined_order_preserves_separator_collisions_and_control_character_prefixes()
    {
        string[] columns = ["a", "b"];
        var boundaryLeft = new Dictionary<string, string> { ["a"] = "A", ["b"] = "\u001fB" };
        var boundaryRight = new Dictionary<string, string> { ["a"] = "A\u001f", ["b"] = "B" };
        Assert.Equal(0, ReviewOutputOrdering.CompareJoined(boundaryLeft, boundaryRight, columns));
        var prefixLeft = new Dictionary<string, string> { ["a"] = "A", ["b"] = "" };
        var prefixRight = new Dictionary<string, string> { ["a"] = "A\0", ["b"] = "" };
        Assert.True(ReviewOutputOrdering.CompareJoined(prefixLeft, prefixRight, columns) > 0);
        Assert.True(StringComparer.Ordinal.Compare(prefixLeft["a"], prefixRight["a"]) < 0);
    }

    [Fact]
    public void Bound_columns_preserve_primary_alias_precedence_and_missing_cell_evidence()
    {
        var table = new XerTable("GENERATED");
        table.SetHeaders(["pRiMaRy", "ALIAS", "other"]);
        table.AddRow(new DataRow(["", "alias value"], "source.xer", "occurrence"));
        var columns = new[]
        {
            new ProgrammeReviewColumn("primary", ProgrammeReviewColumnType.Text, SourceAliases: ["alias"]),
            new ProgrammeReviewColumn("missing", ProgrammeReviewColumnType.Text, SourceAliases: ["alias", "other"]),
            new ProgrammeReviewColumn("absent", ProgrammeReviewColumnType.Text, SourceAliases: ["not-there"]),
            new ProgrammeReviewColumn("other", ProgrammeReviewColumnType.Text)
        };
        var bindings = ReviewProjectionAccess.Bind(table, columns, column => column.Name, column => column.SourceAliases);
        DataRow row = table.Rows[0];
        Assert.Equal("", XerTable.GetFieldValueSafe(row, bindings["primary"]));
        Assert.Equal("alias value", XerTable.GetFieldValueSafe(row, bindings["missing"]));
        Assert.Equal(-1, bindings["absent"]);
        Assert.Equal(XerRawFieldState.OmittedCell, row.GetRawField("other").State);
        Assert.Equal(XerRawFieldState.AbsentHeader, row.GetRawField("absent").State);
    }

    [Fact]
    public void Tender_merge_matches_frozen_alignment_without_mutating_incoming_rows_or_raw_evidence()
    {
        var incoming = new[]
        {
            Store(["Task_ID", "remain_qty"], ["T1"], "stage-a"),
            Store(["TASK_id", "REMAIN_QTY"], ["T2", " "], "stage-b"),
            Store(["remain_crv", "remain_qty", "task_id"], ["80:8;20:8", "bad-number", "T3"], "stage-c"),
            Store(["task_id"], ["T4"], "stage-d")
        };
        var originalRows = incoming.Select(store => store.GetTable("TASKRSRC")!.Rows.ToArray()).ToArray();
        var originalFields = originalRows.Select(rows => rows.Select(row => row.Fields.ToArray()).ToArray()).ToArray();
        var expected = new XerDataStore();
        var actual = new XerDataStore();
        foreach (XerDataStore source in incoming)
        {
            FrozenMerge(expected, source);
            TenderReviewBundleService.MergeTenderStoreWithUnionSchema(actual, source);
        }
        XerTable expectedTable = expected.GetTable("TASKRSRC")!, actualTable = actual.GetTable("TASKRSRC")!;
        Assert.Equal(expectedTable.Headers, actualTable.Headers);
        Assert.Equal(4, actualTable.RowCount);
        for (int index = 0; index < actualTable.RowCount; index++)
        {
            DataRow expectedRow = expectedTable.Rows[index], actualRow = actualTable.Rows[index];
            Assert.Equal(expectedRow.Fields, actualRow.Fields);
            Assert.Equal(expectedRow.SourceToken, actualRow.SourceToken);
            Assert.Equal(expectedRow.SourceFilename, actualRow.SourceFilename);
            Assert.Equal(expectedRow.OriginalSourceFilename, actualRow.OriginalSourceFilename);
            Assert.Same(originalRows[index][0].RawEvidenceIdentity, actualRow.RawEvidenceIdentity);
            Assert.Equal(XerDataQuality.RawRowJson(expectedTable, expectedRow), XerDataQuality.RawRowJson(actualTable, actualRow));
            foreach (string header in actualTable.Headers!)
            {
                Assert.Equal(expectedRow.GetRawField(header), actualRow.GetRawField(header));
                Assert.Equal(expectedRow.GetEvaluationField(header), actualRow.GetEvaluationField(header));
            }
            Assert.Single(incoming[index].GetTable("TASKRSRC")!.Rows);
            Assert.Equal(originalFields[index][0], incoming[index].GetTable("TASKRSRC")!.Rows[0].Fields);
        }
        Assert.Equal(XerRawFieldState.OmittedCell, actualTable.Rows[0].GetRawField("remain_qty").State);
        Assert.Equal(XerRawFieldState.AbsentHeader, actualTable.Rows[0].GetRawField("remain_crv").State);
        Assert.Equal(XerRawFieldState.Blank, actualTable.Rows[1].GetRawField("remain_qty").State);
        Assert.Equal(XerRawFieldState.Present, actualTable.Rows[2].GetRawField("remain_qty").State);
    }

    [Theory]
    [InlineData("2026-03-01T23:00:00-08:00", ProgrammeReviewColumnType.Date)]
    [InlineData("8.5000", ProgrammeReviewColumnType.Number)]
    [InlineData("1e308", ProgrammeReviewColumnType.Number)]
    [InlineData("+12.0", ProgrammeReviewColumnType.Integer)]
    [InlineData("yes", ProgrammeReviewColumnType.Boolean)]
    [InlineData("  Resource, \"A\"  ", ProgrammeReviewColumnType.Text)]
    public void Successful_first_normalisation_is_already_canonical(string raw, ProgrammeReviewColumnType type)
    {
        var programmeColumn = new ProgrammeReviewColumn("value", type);
        var tenderColumn = new TenderReviewColumn("value", (TenderReviewColumnType)type);
        string programme = ProgrammeReviewCsv.Normalize(raw, programmeColumn, "first");
        string tender = TenderReviewCsv.Normalize(raw, tenderColumn, "first");
        Assert.Equal(programme, ProgrammeReviewCsv.Normalize(programme, programmeColumn, "final"));
        Assert.Equal(tender, TenderReviewCsv.Normalize(tender, tenderColumn, "final"));
    }

    private static Dictionary<string, string> Values(IEnumerable<string> names, string value, int index) =>
        names.Select((name, column) => (name, value: column % 2 == 0 ? value : EdgeValues[(index + column) % EdgeValues.Length]))
            .ToDictionary(item => item.name, item => item.value, StringComparer.OrdinalIgnoreCase);

    private static XerDataStore Store(string[] headers, string[] fields, string source)
    {
        var table = new XerTable("TASKRSRC");
        table.SetHeaders(headers);
        table.AddRow(new DataRow(fields, source, source, "same.xer"));
        var store = new XerDataStore();
        store.AddTable(table);
        return store;
    }

    // Frozen pre-optimisation merge is the differential oracle. It deliberately
    // realigns every accumulated row, independent of the append/widen fast paths.
    private static void FrozenMerge(XerDataStore target, XerDataStore incoming)
    {
        foreach (string name in incoming.TableNames)
        {
            XerTable source = incoming.GetTable(name)!;
            XerTable? existing = target.GetTable(name);
            var headers = (existing?.Headers ?? []).ToList();
            var known = headers.ToHashSet(StringComparer.OrdinalIgnoreCase);
            foreach (string header in source.Headers!)
                if (known.Add(header)) headers.Add(header);
            var union = new XerTable(name);
            union.SetHeaders(headers.ToArray());
            foreach (XerTable table in existing is null ? [source] : new[] { existing, source })
            foreach (DataRow row in table.Rows)
                union.AddRow(row.WithFields(headers.Select(header => table.FieldIndexes.TryGetValue(header, out int index)
                    ? XerTable.GetFieldValueSafe(row, index) : string.Empty).ToArray()));
            target.AddTable(union);
        }
    }

    private sealed record SortRow(int Ordinal, int Source, IReadOnlyDictionary<string, string> Values);
}
