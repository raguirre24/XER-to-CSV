using System.Text;

namespace XerToCsvConverter.Core.Tests;

public sealed class ParserOccurrenceIdentityTests
{
    private const string Content = "%T\tPROJECT\n%F\tproj_id\n%R\tP1\n%T\tTASK\n%F\ttask_id\n%R\tT1\n%E\n";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Independent_same_name_parses_have_distinct_occurrences_and_cannot_publish_aliased_keys(bool asynchronous)
    {
        XerDataStore first = await Parse("same.xer", asynchronous);
        XerDataStore second = await Parse("same.xer", asynchronous);
        string firstToken = Assert.Single(first.GetTable("PROJECT")!.Rows).SourceToken;
        string secondToken = Assert.Single(second.GetTable("PROJECT")!.Rows).SourceToken;
        Assert.NotEqual(firstToken, secondToken);
        first.MergeStore(second);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(() =>
            new ProcessingService().ExportTablesToMemoryAsync(first, [EnhancedTableNames.XerProject02], null, CancellationToken.None));
        Assert.Contains("ordered batch", error.Message, StringComparison.Ordinal);
        Assert.All(first.GetTable("PROJECT")!.Rows, row =>
        {
            Assert.Equal("same.xer", row.SourceFilename);
            Assert.Equal("same.xer", row.OriginalSourceFilename);
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Distinct_names_export_and_each_parse_uses_one_token_across_all_its_tables(bool asynchronous)
    {
        XerDataStore first = await Parse("2601.xer", asynchronous);
        XerDataStore second = await Parse("2602.xer", asynchronous);
        string firstToken = Assert.Single(first.TableNames.SelectMany(name => first.GetTable(name)!.Rows)
            .Select(row => row.SourceToken).Distinct());
        string secondToken = Assert.Single(second.TableNames.SelectMany(name => second.GetTable(name)!.Rows)
            .Select(row => row.SourceToken).Distinct());
        Assert.NotEqual(firstToken, secondToken);
        first.MergeStore(second);

        Dictionary<string, byte[]> result = await new ProcessingService().ExportTablesToMemoryAsync(
            first, [EnhancedTableNames.XerProject02], null, CancellationToken.None);
        string csv = Encoding.UTF8.GetString(result[EnhancedTableNames.XerProject02]);
        Assert.Contains("2601.xer.P1", csv, StringComparison.Ordinal);
        Assert.Contains("2602.xer.P1", csv, StringComparison.Ordinal);
        Assert.DoesNotContain("parser-source-", csv, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Sync_and_async_calls_to_the_same_parser_instance_do_not_share_an_occurrence()
    {
        var parser = new XerParser();
        using var firstStream = new MemoryStream(Encoding.UTF8.GetBytes(Content));
        using var secondStream = new MemoryStream(Encoding.UTF8.GetBytes(Content));
        XerDataStore first = parser.ParseXerStream(firstStream, "same.xer", null, CancellationToken.None);
        XerDataStore second = await parser.ParseXerStreamAsync(secondStream, "same.xer", null, CancellationToken.None);
        Assert.NotEqual(Assert.Single(first.GetTable("TASK")!.Rows).SourceToken,
            Assert.Single(second.GetTable("TASK")!.Rows).SourceToken);
    }

    private static async Task<XerDataStore> Parse(string name, bool asynchronous)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(Content));
        var parser = new XerParser();
        return asynchronous
            ? await parser.ParseXerStreamAsync(stream, name, null, CancellationToken.None)
            : parser.ParseXerStream(stream, name, null, CancellationToken.None);
    }
}
