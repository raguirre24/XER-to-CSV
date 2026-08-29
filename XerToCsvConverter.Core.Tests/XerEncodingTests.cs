using System.Text;

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
