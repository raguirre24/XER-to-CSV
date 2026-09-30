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

    [Theory]
    [InlineData("selected", "selected baseline.xer", "selected_ISSUE", "Reselected activity in selected baseline.xer remains unavailable.")]
    [InlineData("C1", "C1 calendar.xer", "C1_WARNING", "Calendar BC12 in C1 calendar.xer has unknown availability.")]
    [InlineData("internal-token", "internal-token stage.xer", "internal-token_WARNING", "Read internal-token stage.xer and pre-internal-token-suffix.")]
    public void Public_provenance_and_legitimate_token_substrings_are_preserved(string token, string filename, string code, string detail)
    {
        XerTable table = CreateTable();
        table.AddRow(Warning(code, detail, filename, token));
        Assert.Equal($"{filename}: [{code}] {detail}", Assert.Single(XerDataQuality.GetMessages(table)));
    }

    [Theory]
    [InlineData("Details for 'internal-token'.", "Details for '[source]'.")]
    [InlineData("Raw key internal-token.T1 is unavailable.", "Raw key [source].T1 is unavailable.")]
    [InlineData("internal-token / internal-token", "[source] / [source]")]
    public void Delimited_identity_references_are_redacted_without_rewriting_other_text(string detail, string expected)
    {
        XerTable table = CreateTable();
        table.AddRow(Warning("CODE", detail, "stage.xer", "internal-token"));
        Assert.Equal($"stage.xer: [CODE] {expected}", Assert.Single(XerDataQuality.GetMessages(table)));
    }

    [Fact]
    public void Caller_supplied_occurrence_annotations_are_removed_at_the_display_boundary()
    {
        XerTable table = CreateTable();
        DataRow row = Warning("CODE", "Unavailable", "stage.xer", "internal-token");
        row.Fields[22] = "Unavailable (input occurrence 'internal-token') (occurrence 'internal-token').";
        table.AddRow(row);
        Assert.Equal("stage.xer: [CODE] Unavailable.", Assert.Single(XerDataQuality.GetMessages(table)));
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
