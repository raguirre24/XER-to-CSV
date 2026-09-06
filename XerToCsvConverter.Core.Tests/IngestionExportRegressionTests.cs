using System.Text;

namespace XerToCsvConverter.Core.Tests;

public sealed class IngestionExportRegressionTests
{
    [Fact]
    public void Merge_unions_reordered_and_later_only_headers_without_mutating_inputs()
    {
        XerDataStore first = Raw("TASKRSRC", ["taskrsrc_id", "remain_qty"], ["A", "100"]);
        XerDataStore second = Raw("TASKRSRC", ["remain_crv", "remain_qty", "taskrsrc_id", "curv_id"],
            ["80:8;20:8", "100", "B", "C"]);
        var merged = new XerDataStore();
        merged.MergeStore(first);
        merged.MergeStore(second);

        XerTable table = merged.GetTable("TASKRSRC")!;
        Assert.Equal(new[] { "taskrsrc_id", "remain_qty", "remain_crv", "curv_id" }, table.Headers);
        Assert.Equal(new[] { "A", "100", "", "" }, table.Rows[0].Fields);
        Assert.Equal(new[] { "B", "100", "80:8;20:8", "C" }, table.Rows[1].Fields);
        Assert.Equal(2, first.GetTable("TASKRSRC")!.Headers!.Length);
        Assert.Equal("remain_crv", second.GetTable("TASKRSRC")!.Headers![0]);
    }

    [Fact]
    public async Task Ordered_occurrences_keep_distinct_tokens_public_keys_and_original_provenance()
    {
        const string content = "%T\tPROJECT\n%F\tproj_id\n%R\tP1\n%E\n";
        using var first = Bytes(content);
        using var second = Bytes(content);
        using var reserved = Bytes(content);
        using var unique = Bytes(content);
        var progress = new RecordingProgress();
        XerDataStore store = await new ProcessingService().ParseXerStreamsAsync(new[]
        {
            ((Stream)first, "2601.xer"), ((Stream)second, "2601.xer"),
            ((Stream)reserved, "2601.xer#source-000001"), ((Stream)unique, "2602.xer")
        }, progress, CancellationToken.None);
        DataRow[] raw = store.GetTable("PROJECT")!.Rows.ToArray();

        Assert.Equal(4, raw.Select(row => row.SourceToken).Distinct().Count());
        for (int i = 0; i < raw.Length; i++) Assert.EndsWith($":source-{i + 1:D6}", raw[i].SourceToken);
        Assert.Equal(4, raw.Select(row => row.SourceFilename).Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.Equal("2602.xer", raw[3].SourceFilename);
        Assert.Equal("2601.xer#source-000001", raw[2].SourceFilename);
        Assert.Equal("2601.xer", raw[0].OriginalSourceFilename);
        Assert.Equal("2601.xer", raw[1].OriginalSourceFilename);
        XerTable enhanced = new XerTransformer(store).Create02XerProject()!;
        Assert.Equal(4, enhanced.Rows.Select(row => row.Fields[enhanced.FieldIndexes["proj_id_key"]]).Distinct().Count());
        Assert.All(enhanced.Rows, row => Assert.Equal(
            Assert.Single(raw, source => source.SourceToken == row.SourceToken).OriginalSourceFilename,
            row.OriginalSourceFilename));
        foreach (var item in progress.Items.Where(item => item.InputIndex.HasValue))
            Assert.Equal(raw[item.InputIndex!.Value].SourceToken, item.SourceToken);
    }

    [Fact]
    public async Task Repeated_file_paths_and_parallel_parse_completion_preserve_input_order_and_schema()
    {
        using var folder = new TemporaryFolder();
        string first = Path.Combine(folder.Path, "2601.xer");
        string second = Path.Combine(folder.Path, "2602.xer");
        File.WriteAllText(first, "%T\tPROJECT\n%F\tproj_id\n%R\tFIRST\n%E\n");
        File.WriteAllText(second, "%T\tPROJECT\n%F\textra\tproj_id\n%R\tkept\tSECOND\n%E\n");
        XerDataStore store = await new ProcessingService().ParseMultipleXerFilesAsync(
            [first, second, first], null, CancellationToken.None);
        XerTable table = store.GetTable("PROJECT")!;

        Assert.Equal(new[] { "proj_id", "extra" }, table.Headers);
        Assert.Equal(new[] { "FIRST", "SECOND", "FIRST" }, table.Rows.Select(row => row.Fields[0]));
        Assert.Equal(new[] { "", "kept", "" }, table.Rows.Select(row => row.Fields[1]));
        Assert.Equal(3, table.Rows.Select(row => row.SourceToken).Distinct().Count());
    }

    [Fact]
    public async Task Repeated_full_schedule_inputs_isolate_all_calendar_dependent_tables_and_csv_provenance()
    {
        string fixture = System.IO.Path.GetFullPath(System.IO.Path.Combine(AppContext.BaseDirectory,
            "../../../..", "review/fixtures/J5001_C_BL01_2026-01-31.xer"));
        byte[] bytes = File.ReadAllBytes(fixture);
        using var first = new MemoryStream(bytes);
        using var second = new MemoryStream(bytes);
        XerDataStore store = await new ProcessingService().ParseXerStreamsAsync(new[]
        {
            ((Stream)first, "2601.xer"), ((Stream)second, "2601.xer")
        }, null, CancellationToken.None);
        var transformer = new XerTransformer(store);
        XerTable[] tables =
        [
            transformer.Create01XerTaskTable()!,
            transformer.Create06XerPredecessor(new())!,
            transformer.Create10XerCalendar()!,
            transformer.Create11XerCalendarDetailed()!,
            transformer.Create15XerResourceDistribution()!
        ];

        foreach (XerTable table in tables)
        {
            Assert.NotNull(table);
            Assert.Equal(2, table.Rows.Select(row => row.SourceToken).Distinct().Count());
            Assert.All(table.Rows, row =>
            {
                Assert.Equal("2601.xer", row.OriginalSourceFilename);
                Assert.StartsWith("2601.xer#source-", row.SourceFilename);
                Assert.Equal("2026-01-01", row.Fields[table.FieldIndexes["MonthUpdate"]]);
            });
            using var csv = new MemoryStream();
            new CsvExporter().WriteTableToStream(table, csv);
            Assert.All(CsvLines(csv.ToArray()).Skip(1), line => Assert.EndsWith(",2601.xer", line));
        }
        Assert.Equal(2, tables[1].Rows.Select(row => row.Fields[tables[1].FieldIndexes["task_id_key"]]).Distinct().Count());
        Assert.Equal(2, tables[4].Rows.Select(row => row.Fields[tables[4].FieldIndexes["task_id_key"]]).Distinct().Count());
        Assert.All(tables[4].Rows, row => Assert.Equal("100.0000", row.Fields[tables[4].FieldIndexes["monthly_quantity"]]));
    }

    [Fact]
    public async Task Missing_requested_input_does_not_return_a_partial_file_batch()
    {
        using var folder = new TemporaryFolder();
        await Assert.ThrowsAsync<InvalidDataException>(() => new ProcessingService().ParseMultipleXerFilesAsync(
            [Path.Combine(folder.Path, "missing.xer")], null, CancellationToken.None));
    }

    [Fact]
    public async Task Public_namespace_normalization_does_not_conflate_whitespace_variants()
    {
        using var first = Bytes("%T\tPROJECT\n%F\tproj_id\n%R\tP1\n");
        using var second = Bytes("%T\tPROJECT\n%F\tproj_id\n%R\tP1\n");
        XerDataStore store = await new ProcessingService().ParseXerStreamsAsync(new[]
        {
            ((Stream)first, "2601.xer"), ((Stream)second, " 2601.xer ")
        }, null, CancellationToken.None);
        XerTable table = new XerTransformer(store).Create02XerProject()!;
        Assert.Equal(2, table.Rows.Select(row => row.Fields[table.FieldIndexes["proj_id_key"]]).Distinct().Count());
        Assert.Contains(table.Rows, row => row.OriginalSourceFilename == " 2601.xer ");
    }

    [Fact]
    public async Task Independently_parsed_batches_have_unique_tokens_and_reject_ambiguous_public_namespaces()
    {
        async Task<XerDataStore> Parse(string filename)
        {
            using var stream = Bytes("%T\tPROJECT\n%F\tproj_id\n%R\tP1\n");
            return await new ProcessingService().ParseXerStreamsAsync(new[] { ((Stream)stream, filename) }, null, CancellationToken.None);
        }
        XerDataStore merged = await Parse("2601.xer");
        merged.MergeStore(await Parse("2602.xer"));
        Assert.Equal(2, merged.GetTable("PROJECT")!.Rows.Select(row => row.SourceToken).Distinct().Count());
        Assert.Equal(2, (await new ProcessingService().ExportTablesToMemoryAsync(merged, ["02_XER_PROJECT"], null, CancellationToken.None)).Count);
        merged.MergeStore(await Parse("2601.xer"));
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(() => new ProcessingService().ExportTablesToMemoryAsync(
            merged, ["02_XER_PROJECT"], null, CancellationToken.None));
        Assert.Contains("one ordered batch", error.Message);
    }

    [Fact]
    public async Task Header_only_raw_tables_survive_all_parser_entrypoints_and_export()
    {
        const string content = "%T\tTASKRSRC\n%F\ttaskrsrc_id\ttask_id\n%E\n";
        using var stream = Bytes(content);
        XerDataStore store = await new XerParser().ParseXerStreamAsync(stream, "2601.xer", null, CancellationToken.None);
        Assert.Empty(store.GetTable("TASKRSRC")!.Rows);
        Dictionary<string, byte[]> result = await new ProcessingService().ExportTablesToMemoryAsync(store,
            ["TASKRSRC", "13_XER_TASKRSRC", "15_XER_RESOURCE_DISTRIBUTION"], null, CancellationToken.None);

        Assert.Equal(new[] { "13_XER_TASKRSRC", "15_XER_RESOURCE_DISTRIBUTION", "TASKRSRC", XerDataQuality.TableName },
            result.Keys.OrderBy(name => name, StringComparer.Ordinal));
        Assert.All(result.Values, bytes => Assert.Single(CsvLines(bytes)));
        Assert.Equal(string.Join(',', XerDataQuality.Columns.Append("FileName")),
            Assert.Single(CsvLines(result[XerDataQuality.TableName])));
        Assert.Contains("monthly_quantity", Encoding.UTF8.GetString(result["15_XER_RESOURCE_DISTRIBUTION"]));
        Assert.Contains("rsrc_id_key,task_id_key,MonthUpdate,FileName", Encoding.UTF8.GetString(result["13_XER_TASKRSRC"]));
    }

    [Fact]
    public async Task All_fourteen_empty_enhanced_schemas_match_their_populated_exports()
    {
        string fixture = System.IO.Path.GetFullPath(System.IO.Path.Combine(AppContext.BaseDirectory,
            "../../../..", "review/fixtures/J5001_C_BL01_2026-01-31.xer"));
        using var stream = new MemoryStream(File.ReadAllBytes(fixture));
        XerDataStore populated = new XerParser().ParseXerStream(stream, "2601.xer", null, CancellationToken.None);
        var empty = new XerDataStore();
        foreach (string name in populated.TableNames)
            empty.MergeStore(Raw(name, populated.GetTable(name)!.Headers!.ToArray()));
        List<string> selected =
        [
            "01_XER_TASK", "02_XER_PROJECT", "03_XER_PROJWBS", "04_XER_BASELINE", "06_XER_PREDECESSOR",
            "07_XER_ACTVTYPE", "08_XER_ACTVCODE", "09_XER_TASKACTV", "10_XER_CALENDAR",
            "11_XER_CALENDAR_DETAILED", "12_XER_RSRC", "13_XER_TASKRSRC", "14_XER_UMEASURE",
            "15_XER_RESOURCE_DISTRIBUTION"
        ];
        var service = new ProcessingService();
        Dictionary<string, byte[]> full = await service.ExportTablesToMemoryAsync(populated, selected, null, CancellationToken.None);
        Dictionary<string, byte[]> headers = await service.ExportTablesToMemoryAsync(empty, selected, null, CancellationToken.None);
        Assert.Equal(15, headers.Count); // Fourteen numbered tables plus the current data-quality companion.
        Assert.Equal(CsvLines(full[XerDataQuality.TableName])[0],
            Assert.Single(CsvLines(headers[XerDataQuality.TableName])));
        foreach (string name in selected)
            Assert.Equal(CsvLines(full[name])[0], Assert.Single(CsvLines(headers[name])));
    }

    [Fact]
    public async Task Empty_selected_csv_replaces_stale_data_without_touching_unrelated_files()
    {
        using var folder = new TemporaryFolder();
        string selected = Path.Combine(folder.Path, "TASK.csv");
        string unrelated = Path.Combine(folder.Path, "keep.csv");
        File.WriteAllText(selected, "obsolete data");
        File.WriteAllText(unrelated, "leave alone");
        XerDataStore store = Raw("TASK", ["task_id"]);

        List<string> result = await new ProcessingService().ExportTablesAsync(store, ["TASK"], folder.Path, null, CancellationToken.None);

        Assert.Equal(selected, Assert.Single(result));
        Assert.Equal("task_id,FileName", Assert.Single(CsvLines(File.ReadAllBytes(selected))));
        Assert.Equal("leave alone", File.ReadAllText(unrelated));
        Assert.Empty(Directory.GetDirectories(folder.Path));
    }

    [Fact]
    public async Task Missing_numbered_source_is_explicit_header_only_without_blocking_other_available_outputs()
    {
        using var folder = new TemporaryFolder();
        string selected = Path.Combine(folder.Path, "PROJECT.csv");
        File.WriteAllText(selected, "original");
        XerDataStore store = Raw("PROJECT", ["proj_id"], ["P1"]);

        var result = await new ProcessingService().ExportTablesWithDiagnosticsAsync(
            store, ["PROJECT", "01_XER_TASK"], folder.Path, null, CancellationToken.None);

        Assert.Contains("P1", File.ReadAllText(selected));
        Assert.Single(CsvLines(File.ReadAllBytes(Path.Combine(folder.Path, "01_XER_TASK.csv"))));
        Assert.Equal(1, result.WarningCount);
        Assert.Contains("SOURCE_TABLE_UNAVAILABLE", File.ReadAllText(Path.Combine(folder.Path, XerDataQuality.FileName)));
        Assert.Equal(3, Directory.GetFiles(folder.Path).Length);
    }

    [Fact]
    public async Task Cancellation_after_staging_keeps_the_whole_previous_selected_set()
    {
        using var folder = new TemporaryFolder();
        File.WriteAllText(Path.Combine(folder.Path, "A.csv"), "old A");
        File.WriteAllText(Path.Combine(folder.Path, "Z.csv"), "old Z");
        XerDataStore store = Raw("A", ["id"], ["new A"]);
        store.MergeStore(Raw("Z", ["id"], ["new Z"]));
        using var cancellation = new CancellationTokenSource();
        var progress = new InlineProgress<(int percent, string message)>(item =>
        {
            if (item.percent == 95) cancellation.Cancel();
        });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new ProcessingService().ExportTablesAsync(
            store, ["A", "Z"], folder.Path, progress, cancellation.Token));

        Assert.Equal("old A", File.ReadAllText(Path.Combine(folder.Path, "A.csv")));
        Assert.Equal("old Z", File.ReadAllText(Path.Combine(folder.Path, "Z.csv")));
        Assert.Empty(Directory.GetDirectories(folder.Path));
    }

    [Fact]
    public async Task Later_publication_failure_rolls_back_already_replaced_files()
    {
        if (!OperatingSystem.IsWindows()) return; // Windows deny-delete file sharing is the injected failure.
        using var folder = new TemporaryFolder();
        File.WriteAllText(Path.Combine(folder.Path, "A.csv"), "old A");
        File.WriteAllText(Path.Combine(folder.Path, "Z.csv"), "old Z");
        XerDataStore store = Raw("A", ["id"], ["new A"]);
        store.MergeStore(Raw("Z", ["id"], ["new Z"]));
        using (var locked = new FileStream(Path.Combine(folder.Path, "Z.csv"), FileMode.Open, FileAccess.Read, FileShare.Read))
            await Assert.ThrowsAnyAsync<IOException>(() => new ProcessingService().ExportTablesAsync(
                store, ["A", "Z"], folder.Path, null, CancellationToken.None));

        Assert.Equal("old A", File.ReadAllText(Path.Combine(folder.Path, "A.csv")));
        Assert.Equal("old Z", File.ReadAllText(Path.Combine(folder.Path, "Z.csv")));
        Assert.Empty(Directory.GetDirectories(folder.Path));
    }

    [Theory]
    [InlineData("../outside")]
    [InlineData("..\\outside")]
    [InlineData("C:\\outside")]
    [InlineData("/outside")]
    [InlineData("A:stream")]
    [InlineData("CON")]
    [InlineData("lpt1")]
    [InlineData("..")]
    public async Task Unsafe_table_names_are_rejected_before_disk_or_zip_serialization(string name)
    {
        using var folder = new TemporaryFolder();
        XerDataStore store = Raw(name, ["id"], ["unsafe"]);
        var service = new ProcessingService();
        await Assert.ThrowsAsync<InvalidDataException>(() => service.ExportTablesAsync(store, [name], folder.Path, null, CancellationToken.None));
        await Assert.ThrowsAsync<InvalidDataException>(() => service.ExportTablesToMemoryAsync(store, [name], null, CancellationToken.None));
        using var sync = Bytes($"%T\t{name}\n%F\tid\n%R\tunsafe\n");
        using var asyncStream = Bytes($"%T\t{name}\n%F\tid\n%R\tunsafe\n");
        Assert.Throws<InvalidDataException>(() => new XerParser().ParseXerStream(sync, "unsafe.xer", null, CancellationToken.None));
        await Assert.ThrowsAsync<InvalidDataException>(() => new XerParser().ParseXerStreamAsync(asyncStream, "unsafe.xer", null, CancellationToken.None));
        Assert.Empty(Directory.GetFileSystemEntries(folder.Path));
    }

    private static MemoryStream Bytes(string content) => new(Encoding.UTF8.GetBytes(content));

    private static XerDataStore Raw(string name, string[] headers, params string[][] rows)
    {
        var store = new XerDataStore();
        var table = new XerTable(name);
        table.SetHeaders(headers);
        foreach (string[] row in rows) table.AddRow(new DataRow(row, "2601.xer"));
        store.AddTable(table);
        return store;
    }

    private static string[] CsvLines(byte[] bytes) => Encoding.UTF8.GetString(bytes).TrimStart('\uFEFF')
        .Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);

    private sealed class RecordingProgress : IProgress<ProcessingService.DetailedProgress>
    {
        internal List<ProcessingService.DetailedProgress> Items { get; } = new();
        public void Report(ProcessingService.DetailedProgress value) => Items.Add(value);
    }

    private sealed class InlineProgress<T>(Action<T> action) : IProgress<T>
    {
        public void Report(T value) => action(value);
    }

    private sealed class TemporaryFolder : IDisposable
    {
        internal string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "xer-ingestion-review-" + Guid.NewGuid().ToString("N"));
        internal TemporaryFolder() => Directory.CreateDirectory(Path);
        public void Dispose() => Directory.Delete(Path, recursive: true);
    }
}
