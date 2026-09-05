using System.Text;

namespace XerToCsvConverter.Core.Tests;

public sealed class HeaderValidationRegressionTests
{
    [Theory]
    [InlineData("remain_qty\tREMAIN_QTY")]
    [InlineData("remain_qty\tremain_qty")]
    [InlineData("remain_qty\t")]
    [InlineData("\tremain_qty")]
    [InlineData(" \tremain_qty")]
    [InlineData("")]
    public async Task Every_raw_parser_entrypoint_rejects_ambiguous_source_headers(string headers)
    {
        string content = $"%T\tTASKRSRC\n%F\t{headers}\n%R\t80\t20\n%E\n";
        using var sync = Bytes(content);
        using var asyncInput = Bytes(content);
        var parser = new XerParser();
        Assert.Throws<InvalidDataException>(() => parser.ParseXerStream(sync, "source.xer", null, CancellationToken.None));
        await Assert.ThrowsAsync<InvalidDataException>(() => parser.ParseXerStreamAsync(asyncInput, "source.xer", null, CancellationToken.None));
        using var folder = new TestFolder();
        string path = Path.Combine(folder.Path, "source.xer");
        File.WriteAllText(path, content);
        Assert.Throws<InvalidDataException>(() => parser.ParseXerFile(path, null, CancellationToken.None));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Duplicate_headers_are_rejected_in_either_merge_order_without_losing_values(bool malformedFirst)
    {
        XerDataStore valid = Store("TASKRSRC", ["remain_qty"], ["100"]);
        XerDataStore malformed = Store("TASKRSRC", ["remain_qty", "REMAIN_QTY"], ["80", "20"]);
        var merged = new XerDataStore();
        if (malformedFirst)
            Assert.Throws<InvalidDataException>(() => merged.MergeStore(malformed));
        else
        {
            merged.MergeStore(valid);
            Assert.Throws<InvalidDataException>(() => merged.MergeStore(malformed));
            Assert.Equal(new[] { "100" }, Assert.Single(merged.GetTable("TASKRSRC")!.Rows).Fields);
        }

        Assert.Equal(new[] { "80", "20" }, Assert.Single(malformed.GetTable("TASKRSRC")!.Rows).Fields);
        Assert.Equal(new[] { "remain_qty", "REMAIN_QTY" }, malformed.GetTable("TASKRSRC")!.Headers);
        Assert.Equal(malformedFirst ? 0 : 1, merged.TableCount);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("ID")]
    public void Merge_preflights_all_incoming_tables_before_copying_any(string conflictingHeader)
    {
        var incoming = Store("A_VALID", ["id"], ["kept"]);
        XerTable invalid = Store("Z_INVALID", ["id", conflictingHeader], ["80", "20"]).GetTable("Z_INVALID")!;
        incoming.AddTable(invalid);
        var target = new XerDataStore();
        Assert.Throws<InvalidDataException>(() => target.MergeStore(incoming));
        Assert.Equal(0, target.TableCount);
        Assert.Equal(2, incoming.TableCount);
    }

    [Fact]
    public void Merge_also_rejects_an_already_ambiguous_target()
    {
        XerDataStore target = Store("TASKRSRC", ["remain_qty", "REMAIN_QTY"], ["80", "20"]);
        XerDataStore incoming = Store("TASKRSRC", ["remain_qty"], ["100"]);
        Assert.Throws<InvalidDataException>(() => target.MergeStore(incoming));
        Assert.Equal(new[] { "80", "20" }, Assert.Single(target.GetTable("TASKRSRC")!.Rows).Fields);
    }

    [Fact]
    public void Valid_case_and_header_order_are_preserved_while_values_map_by_name()
    {
        XerDataStore target = Store("TASKRSRC", ["Task_ID", "remain_qty"], ["A", "100"]);
        XerDataStore incoming = Store("TASKRSRC", ["REMAIN_QTY", "TASK_id", "remain_crv"], ["100", "B", "80:8;20:8"]);
        target.MergeStore(incoming);
        XerTable table = target.GetTable("TASKRSRC")!;
        Assert.Equal(new[] { "Task_ID", "remain_qty", "remain_crv" }, table.Headers);
        Assert.Equal(new[] { "B", "100", "80:8;20:8" }, table.Rows[1].Fields);
        Assert.Equal(new[] { "REMAIN_QTY", "TASK_id", "remain_crv" }, incoming.GetTable("TASKRSRC")!.Headers);
    }

    [Theory]
    [InlineData("")]
    [InlineData("ID")]
    public async Task Invalid_direct_store_cannot_publish_or_return_a_partial_export(string conflictingHeader)
    {
        XerDataStore store = Store("A_VALID", ["id"], ["new value"]);
        store.AddTable(Store("Z_INVALID", ["id", conflictingHeader], ["80", "20"]).GetTable("Z_INVALID")!);
        using var folder = new TestFolder();
        string existing = Path.Combine(folder.Path, "A_VALID.csv");
        File.WriteAllText(existing, "existing export");
        var service = new ProcessingService();
        await Assert.ThrowsAsync<InvalidDataException>(() => service.ExportTablesAsync(
            store, ["A_VALID", "Z_INVALID"], folder.Path, null, CancellationToken.None));
        await Assert.ThrowsAsync<InvalidDataException>(() => service.ExportTablesToMemoryAsync(
            store, ["A_VALID", "Z_INVALID"], null, CancellationToken.None));
        Assert.Equal("existing export", File.ReadAllText(existing));
        Assert.Equal(new[] { existing }, Directory.GetFileSystemEntries(folder.Path));
    }

    private static MemoryStream Bytes(string value) => new(Encoding.UTF8.GetBytes(value));

    private static XerDataStore Store(string name, string[] headers, string[] fields)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        table.AddRow(new DataRow(fields, "2601.xer"));
        var store = new XerDataStore();
        store.AddTable(table);
        return store;
    }

    private sealed class TestFolder : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "xer-header-review-" + Guid.NewGuid().ToString("N"));
        public TestFolder() => Directory.CreateDirectory(Path);
        public void Dispose() => Directory.Delete(Path, recursive: true);
    }
}
