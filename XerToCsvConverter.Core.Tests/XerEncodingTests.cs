using System.Text;
using XerToCsvConverter.ProgrammeReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class XerEncodingTests
{
    private const string ExpectedWindows1252Text = "Road – Stage € Owner’s £100 ¥";

    [Fact]
    public void File_parser_preserves_windows_1252_text()
    {
        string path = Path.Combine(Path.GetTempPath(), $"xer-encoding-{Guid.NewGuid():N}.xer");
        try
        {
            File.WriteAllBytes(path, Windows1252Xer());

            XerDataStore store = new XerParser().ParseXerFile(path, null, CancellationToken.None);

            Assert.Equal(ExpectedWindows1252Text, TaskName(store));
            Assert.DoesNotContain('\uFFFD', TaskName(store));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public void Stream_parser_preserves_windows_1252_text()
    {
        using var stream = new MemoryStream(Windows1252Xer());

        XerDataStore store = new XerParser().ParseXerStream(
            stream, "windows-1252.xer", null, CancellationToken.None);

        Assert.Equal(ExpectedWindows1252Text, TaskName(store));
        Assert.DoesNotContain('\uFFFD', TaskName(store));
    }

    [Fact]
    public void Stream_parser_keeps_valid_utf8_text()
    {
        byte[] bytes = Encoding.UTF8.GetBytes(
            "%T\tTASK\r\n%F\ttask_id\ttask_name\r\n%R\t1\tMāngere Bridge\r\n%E\r\n");
        using var stream = new MemoryStream(bytes);

        XerDataStore store = new XerParser().ParseXerStream(
            stream, "utf8.xer", null, CancellationToken.None);

        Assert.Equal("Māngere Bridge", TaskName(store));
    }

    [Theory]
    [InlineData("utf8")]
    [InlineData("utf16le")]
    [InlineData("utf16be")]
    [InlineData("utf32le")]
    [InlineData("utf32be")]
    public async Task Explicit_bom_encodings_remain_strict_and_preserve_text_across_all_entrypoints(string format)
    {
        Encoding encoding = format switch
        {
            "utf8" => new UTF8Encoding(true, true),
            "utf16le" => new UnicodeEncoding(false, true, true),
            "utf16be" => new UnicodeEncoding(true, true, true),
            "utf32le" => new UTF32Encoding(false, true, true),
            _ => new UTF32Encoding(true, true, true)
        };
        const string content = "%T\tPROJECT\n%F\tproj_id\tlast_recalc_date\n%R\tP1\t2026-01-31\n"
            + "%T\tTASK\n%F\ttask_id\ttask_name\n%R\t1\tMāngere Bridge\n%E\n";
        byte[] bytes = encoding.GetPreamble().Concat(encoding.GetBytes(content)).ToArray();
        using var sync = new MemoryStream(bytes);
        using var asyncStream = new MemoryStream(bytes);
        Assert.Equal("Māngere Bridge", TaskName(new XerParser().ParseXerStream(sync, "bom.xer", null, CancellationToken.None)));
        Assert.Equal("Māngere Bridge", TaskName(await new XerParser().ParseXerStreamAsync(asyncStream, "bom.xer", null, CancellationToken.None)));
        Assert.Equal(new DateOnly(2026, 1, 31), await ProgrammeReviewXerMetadataReader.ReadSingleProjectDataDateAsync(bytes));
        string path = Path.Combine(Path.GetTempPath(), $"xer-bom-{Guid.NewGuid():N}.xer");
        try
        {
            File.WriteAllBytes(path, bytes);
            Assert.Equal("Māngere Bridge", TaskName(new XerParser().ParseXerFile(path, null, CancellationToken.None)));
            Assert.Equal(new DateOnly(2026, 1, 31), await ProgrammeReviewXerMetadataReader.ReadSingleProjectDataDateAsync(path));
        }
        finally { if (File.Exists(path)) File.Delete(path); }
    }

    [Fact]
    public async Task Utf8_bom_with_invalid_body_is_rejected_without_replacement_or_legacy_fallback()
    {
        byte[] bytes = new byte[] { 0xef, 0xbb, 0xbf }.Concat(Encoding.ASCII.GetBytes(
            "%T\tPROJECT\n%F\tproj_id\tproj_short_name\tlast_recalc_date\n%R\tP1\tCaf"))
            .Concat(new byte[] { 0xe9 }).Concat(Encoding.ASCII.GetBytes("\t2026-01-31\n%E\n")).ToArray();
        using var sync = new MemoryStream(bytes);
        using var asyncStream = new MemoryStream(bytes);
        Assert.Throws<DecoderFallbackException>(() => new XerParser().ParseXerStream(sync, "mixed.xer", null, CancellationToken.None));
        await Assert.ThrowsAsync<DecoderFallbackException>(() => new XerParser().ParseXerStreamAsync(asyncStream, "mixed.xer", null, CancellationToken.None));
        await Assert.ThrowsAsync<DecoderFallbackException>(async () => await ProgrammeReviewXerMetadataReader.ReadSingleProjectDataDateAsync(bytes));
        string path = Path.Combine(Path.GetTempPath(), $"xer-malformed-bom-{Guid.NewGuid():N}.xer");
        try
        {
            File.WriteAllBytes(path, bytes);
            Assert.Throws<DecoderFallbackException>(() => new XerParser().ParseXerFile(path, null, CancellationToken.None));
            await Assert.ThrowsAsync<DecoderFallbackException>(async () => await ProgrammeReviewXerMetadataReader.ReadSingleProjectDataDateAsync(path));
        }
        finally { if (File.Exists(path)) File.Delete(path); }
    }

    private static string TaskName(XerDataStore store)
    {
        XerTable table = Assert.IsType<XerTable>(store.GetTable("TASK"));
        DataRow row = Assert.Single(table.Rows);
        return row.Fields[table.FieldIndexes["task_name"]];
    }

    private static byte[] Windows1252Xer()
    {
        byte[] prefix = Encoding.ASCII.GetBytes(
            "%T\tTASK\r\n%F\ttask_id\ttask_name\r\n%R\t1\tRoad ");
        byte[] suffix = Encoding.ASCII.GetBytes("\r\n%E\r\n");
        byte[] encodedText =
        {
            0x96, 0x20, 0x53, 0x74, 0x61, 0x67, 0x65, 0x20,
            0x80, 0x20, 0x4f, 0x77, 0x6e, 0x65, 0x72, 0x92,
            0x73, 0x20, 0xa3, 0x31, 0x30, 0x30, 0xa0, 0xa5
        };
        return prefix.Concat(encodedText).Concat(suffix).ToArray();
    }
}
