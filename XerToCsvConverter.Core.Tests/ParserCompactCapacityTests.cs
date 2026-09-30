using System.Text;

namespace XerToCsvConverter.Core.Tests;

public sealed class ParserCompactCapacityTests
{
    [Theory]
    [InlineData("utf8", false)]
    [InlineData("utf8", true)]
    [InlineData("windows1252", false)]
    [InlineData("windows1252", true)]
    [InlineData("utf16bom", false)]
    [InlineData("utf16bom", true)]
    public async Task Compact_private_parser_preserves_encoded_csv_and_raw_evidence_for_ragged_sources(string encoding, bool asynchronous)
    {
        string name = encoding == "windows1252" ? "Café “quoted”" : "Café 雪🌏";
        string text = "%T\tRAW\r\n%F\tid\tvalue\tomitted\r\n%R\tA\t" + name + "\r\n"
            + "%R\tB\tvalue\textra\toriginal excess cell\r\n%T\tEMPTY\r\n%F\ttoken\r\n%E\r\n";
        byte[] bytes = Encode(text, encoding);
        XerDataStore expected = await Parse(new XerParser(), bytes, asynchronous);
        XerDataStore actual = await Parse(new XerParser(64), bytes, asynchronous);
        Assert.Equal(expected.TableNames, actual.TableNames);
        foreach (string tableName in expected.TableNames)
        {
            XerTable left = expected.GetTable(tableName)!, right = actual.GetTable(tableName)!;
            Assert.Equal(left.Headers, right.Headers);
            Assert.Equal(left.RowCount, right.RowCount);
            Assert.Equal(64, right.Capacity);
            Assert.Equal(PerformanceConfig.DefaultTableCapacity, left.Capacity);
            using var oldCsv = new MemoryStream();
            using var newCsv = new MemoryStream();
            new CsvExporter().WriteTableToStream(left, oldCsv);
            new CsvExporter().WriteTableToStream(right, newCsv);
            Assert.Equal(oldCsv.ToArray(), newCsv.ToArray());
            for (int index = 0; index < left.RowCount; index++)
            {
                Assert.Equal(left.Rows[index].Fields, right.Rows[index].Fields);
                Assert.Equal(left.Rows[index].OriginalSourceFilename, right.Rows[index].OriginalSourceFilename);
                Assert.Equal(XerDataQuality.RawRowJson(left, left.Rows[index]), XerDataQuality.RawRowJson(right, right.Rows[index]));
                foreach (string header in left.Headers!)
                {
                    Assert.Equal(left.Rows[index].GetRawField(header), right.Rows[index].GetRawField(header));
                    Assert.Equal(left.Rows[index].GetEvaluationField(header), right.Rows[index].GetEvaluationField(header));
                }
            }
        }
        Assert.Equal(name, actual.GetTable("RAW")!.Rows[0].Fields[1]);
        Assert.Equal(XerRawFieldState.OmittedCell, actual.GetTable("RAW")!.Rows[0].GetRawField("omitted").State);
        Assert.Equal("", actual.GetTable("RAW")!.Rows[0].Fields[2]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Explicit_bad_utf8_bom_keeps_the_public_decoder_rejection(bool asynchronous)
    {
        byte[] invalid = [0xef, 0xbb, 0xbf, 0xff];
        DecoderFallbackException expected = await Assert.ThrowsAsync<DecoderFallbackException>(
            () => Parse(new XerParser(), invalid, asynchronous));
        DecoderFallbackException actual = await Assert.ThrowsAsync<DecoderFallbackException>(
            () => Parse(new XerParser(64), invalid, asynchronous));
        Assert.Equal(expected.Message, actual.Message);
    }

    private static async Task<XerDataStore> Parse(XerParser parser, byte[] bytes, bool asynchronous)
    {
        using var input = new MemoryStream(bytes, writable: false);
        return asynchronous ? await parser.ParseXerStreamAsync(input, "source.xer", null, default)
            : parser.ParseXerStream(input, "source.xer", null, default);
    }

    private static byte[] Encode(string text, string encoding) => encoding switch
    {
        "windows1252" => XerTextEncoding.Windows1252.GetBytes(text),
        "utf16bom" => [.. Encoding.Unicode.GetPreamble(), .. Encoding.Unicode.GetBytes(text)],
        _ => new UTF8Encoding(false, true).GetBytes(text)
    };
}
