using System.Text;
using Xunit;

namespace XerToCsvConverter.Core.Tests;

public sealed class CsvBufferedWriterTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(7)]
    public void Buffered_records_match_original_writer_bytes_for_ragged_unicode_and_escaped_cells(int columns)
    {
        var table = new XerTable("raw");
        table.SetHeaders(Enumerable.Range(0, columns).Select(index => index % 2 == 0 ? $" heading {index} " : $"header,\"{index}\"").ToArray());
        string[] values = ["", " ordinary ", "comma,value", "\"quoted\"\"", "line\r\nbreak", "雪🌏é", new string('x', 17000) + "\"end"];
        table.AddRow(new DataRow(values, "raw.xer") { OriginalSourceFilename = " source,\"🌏.xer " });
        table.AddRow(new DataRow([], "empty.xer"));
        table.AddRow(new DataRow(["value", null!], "null.xer"));
        using var actual = new MemoryStream();
        new CsvExporter().WriteTableToStream(table, actual);
        using var expected = new MemoryStream();
        using (var writer = new StreamWriter(expected, Encoding.UTF8, 131072, leaveOpen: true))
        {
            Record(table.Headers!, FieldNames.FileName);
            foreach (DataRow row in table.Rows) Record(row.Fields, row.OriginalSourceFilename);
            void Record(string[] fields, string filename)
            {
                for (int index = 0; index <= fields.Length; index++)
                {
                    if (index > 0) writer.Write(',');
                    string value = (index == fields.Length ? filename : fields[index]) ?? "";
                    bool quote = value.IndexOfAny([',', '\r', '\n', '"']) >= 0 || value.StartsWith(' ') || value.EndsWith(' ');
                    if (quote) writer.Write('"');
                    writer.Write(quote ? value.Replace("\"", "\"\"") : value);
                    if (quote) writer.Write('"');
                }
                writer.WriteLine();
            }
        }
        Assert.Equal(expected.ToArray(), actual.ToArray());
        Assert.True(actual.CanWrite);
    }

    [Fact]
    public void Cancellation_is_checked_before_any_header_or_row_write()
    {
        var table = new XerTable("raw");
        table.SetHeaders(["value"]);
        table.AddRow(new DataRow(["value"], "raw.xer"));
        using var output = new MemoryStream();
        Assert.Throws<OperationCanceledException>(() => new CsvExporter().WriteTableToStream(table, output, new CancellationToken(true)));
        Assert.Equal(0, output.Length);
    }
}
