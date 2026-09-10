using Xunit;

namespace XerToCsvConverter.TenderReview.Surface.Tests;

public sealed class ReviewDiagnosticMessageTests
{
    [Fact]
    public void MessagesPreserveOriginalFilenameOrderAndRepeatedOccurrences()
    {
        XerTable table = CreateTable();
        table.AddRow(Warning("ONE", "First", "same.xer", "internal-one"));
        table.AddRow(Warning("TENDER_PROJECT_CODE_MAPPED", "Map QAC000623-01-02 to QAC000623", "same.xer", "internal-two"));
        table.AddRow(Warning("THREE", "Third", "other.xer", "internal-three"));

        Assert.Equal(new[]
        {
            "same.xer: [ONE] First",
            "same.xer: [TENDER_PROJECT_CODE_MAPPED] Map QAC000623-01-02 to QAC000623",
            "other.xer: [THREE] Third"
        }, XerDataQuality.GetMessages(table));
    }

    [Theory]
    [InlineData("TENDER_PROJECT_CODE_MAPPED", "Map QAC000623-01-02 to QAC000623")]
    [InlineData("TENDER_PROJECT_STATE_UNKNOWN", "Blank State cannot match state-based access")]
    public void ProjectMetadataWarningsBeyondOrdinaryDisplayLimitRemainVisible(string code, string message)
    {
        XerTable table = CreateTable();
        table.AddRow(Warning("ONE", "First", "one.xer", "internal-one"));
        table.AddRow(Warning("TWO", "Second", "two.xer", "internal-two"));
        table.AddRow(Warning(code, message, "three.xer", "internal-three"));

        IReadOnlyList<string> messages = XerDataQuality.GetMessages(table, maximumMessages: 1);

        Assert.Equal(3, messages.Count);
        Assert.Equal("one.xer: [ONE] First", messages[0]);
        Assert.Contains($"[{code}] {message}", messages[1], StringComparison.Ordinal);
        Assert.Contains("1 additional source diagnostic(s)", messages[2], StringComparison.Ordinal);
    }

    [Fact]
    public void MessagesDoNotExposeInternalCorrelationTokens()
    {
        XerTable table = CreateTable();
        table.AddRow(Warning("CODE", "Details for internal-token", "stage.xer", "internal-token"));

        string message = Assert.Single(XerDataQuality.GetMessages(table));

        Assert.DoesNotContain("internal-token", message, StringComparison.Ordinal);
        Assert.Contains("stage.xer", message, StringComparison.Ordinal);
    }

    [Fact]
    public void EmptyAndUnavailableDiagnosticsProduceNoMessages()
    {
        Assert.Empty(XerDataQuality.GetMessages(null));
        Assert.Empty(XerDataQuality.GetMessages(CreateTable()));
        Assert.Empty(XerDataQuality.GetMessages(new XerTable("OTHER")));
        Assert.Throws<ArgumentOutOfRangeException>(() => XerDataQuality.GetMessages(null, -1));
    }

    private static XerTable CreateTable()
    {
        var table = new XerTable(XerDataQuality.TableName);
        table.SetHeaders(XerDataQuality.Columns);
        return table;
    }

    private static DataRow Warning(string code, string message, string filename, string token) =>
        XerDataQuality.CreateWarning("02_XER_PROJECT", code, message,
            new DataRow(Array.Empty<string>(), "public-stage", token, filename), 1);
}
