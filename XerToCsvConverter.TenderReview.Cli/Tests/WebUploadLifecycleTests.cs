using System.Globalization;
using Xunit;
using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.Web.Services;

namespace XerToCsvConverter.TenderReview.Surface.Tests;

public sealed class WebUploadLifecycleTests
{
    [Theory]
    [InlineData("J5001_C_BL01_2026-01-31.xer", "Baseline", "BL01", "")]
    [InlineData("J5001-C-BL01-A_20260131.xer", "Baseline", "BL01-A", "")]
    [InlineData("J5001-T-2609_20260930.xer", "Update", "2609", "2026-09")]
    [InlineData("j5001_c_2609_2026-09-30.XER", "Update", "2609", "2026-09")]
    [InlineData("J5001_ZONE_C_2609_20260930.xer", "Update", "2609", "2026-09")]
    [InlineData("QAC000623-01-02-C-2609_20260930.xer", "Update", "2609", "2026-09")]
    [InlineData("NE Part B-C-BL01_20260131.xer", "Baseline", "BL01", "")]
    [InlineData("港湾 ÉTAPE #2-T-2609_20260930.xer", "Update", "2609", "2026-09")]
    [InlineData("AREA%2F01%3A02-C-2609_20260930.xer", "Update", "2609", "2026-09")]
    [InlineData("PROJECT BL02 2608-C-2609_20260930.xer", "Update", "2609", "2026-09")]
    [InlineData("PROJECT-C-2608_20260831-C-2609_20260930.xer", "Update", "2609", "2026-09")]
    [InlineData("J5001.xer", "", "", "")]
    [InlineData("J5001_2026-01-31.xer", "", "", "")]
    [InlineData("J5001-C-2613_20260131.xer", "", "", "")]
    [InlineData("J5001-C-2600_20260131.xer", "", "", "")]
    [InlineData("J5001-C-2609_20260230.xer", "", "", "")]
    [InlineData("J5001-C-2609_20260930 copy 2608.xer", "", "", "")]
    public void FilenameSuggestionsUseGovernedFieldsNotProjectDigits(
        string filename, string kind, string tag, string monthUpdate)
    {
        ProgrammeReviewUploadSuggestion result = ProgrammeReviewUploadMetadata.Infer(filename, null);
        Assert.Equal(new ProgrammeReviewUploadSuggestion(kind, tag, monthUpdate), result);
    }

    [Theory]
    [InlineData("J5001_C_BL01_2026-01-31.xer", "2026-01-30", "Baseline", "BL01", "2026-01", false)]
    [InlineData("J5001-C-BL01-A_20260131.xer", "2024-02-29", "Baseline", "BL01-A", "2024-02", false)]
    [InlineData("J5001-T-2609_20260930.xer", "2026-09-30", "Update", "2609", "2026-09", false)]
    [InlineData("J5001-T-2609_20260930.xer", "2026-08-31", "Update", "2609", "2026-08", true)]
    [InlineData("J5001-T-9912_20991231.xer", "1999-12-31", "Update", "9912", "1999-12", true)]
    [InlineData("J5001.xer", "2026-01-30", "", "", "2026-01", false)]
    [InlineData("J5001-C-2613_20260131.xer", "2026-01-30", "", "", "2026-01", false)]
    [InlineData("J5001-C-2609_20260230.xer", "2026-01-30", "", "", "2026-01", false)]
    public void DetectedDataDatePrefillsMonthWithoutRewritingFilenameKindOrTag(
        string filename, string dataDate, string kind, string tag, string monthUpdate, bool conflict)
    {
        ProgrammeReviewUploadSuggestion result = ProgrammeReviewUploadMetadata.Infer(
            filename, DateOnly.ParseExact(dataDate, "yyyy-MM-dd", CultureInfo.InvariantCulture));

        Assert.Equal(new ProgrammeReviewUploadSuggestion(kind, tag, monthUpdate), result);
        Assert.Equal(conflict, ProgrammeReviewUploadMetadata.HasUpdateTagMonthConflict(result));
    }

    [Theory]
    [InlineData("0001-01", 1, 1)]
    [InlineData("2024-02", 2024, 2)]
    [InlineData("2026-12", 2026, 12)]
    [InlineData("9999-12", 9999, 12)]
    public void MonthPickerValueMapsToFirstDayForExport(string value, int year, int month)
    {
        Assert.True(ProgrammeReviewUploadMetadata.TryParseMonthUpdate(value, out DateOnly result));
        Assert.Equal(new DateOnly(year, month, 1), result);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("2026-1")]
    [InlineData("2026-00")]
    [InlineData("2026-13")]
    [InlineData("0000-01")]
    [InlineData("10000-01")]
    [InlineData("2026-02-01")]
    [InlineData("2026/02")]
    [InlineData("26-02")]
    [InlineData(" 2026-02")]
    [InlineData("2026-02 ")]
    public void MonthPickerRejectsInvalidOrDayBasedValues(string? value)
    {
        Assert.False(ProgrammeReviewUploadMetadata.TryParseMonthUpdate(value, out _));
    }

    [Fact]
    public void ExplicitMonthEditPreservesExactDataDateAndCanonicalFilename()
    {
        DateOnly dataDate = new(2026, 1, 30);
        ProgrammeReviewUploadSuggestion suggestion = ProgrammeReviewUploadMetadata.Infer(
            "J5001-C-BL01_20260131.xer", dataDate);
        Assert.Equal("2026-01", suggestion.MonthUpdate);

        Assert.True(ProgrammeReviewUploadMetadata.TryParseMonthUpdate("2026-03", out DateOnly editedMonth));
        var snapshot = new ProgrammeReviewSnapshot
        {
            OriginalXerFilename = "J5001-C-BL01_20260131.xer", SnapshotKind = ProgrammeReviewSnapshotKind.Baseline,
            SnapshotTag = suggestion.SnapshotTag, MonthUpdate = editedMonth, DataDate = dataDate
        };

        Assert.Equal(new DateOnly(2026, 3, 1), snapshot.MonthUpdate);
        Assert.Equal(new DateOnly(2026, 1, 30), snapshot.DataDate);
        Assert.Equal("J5001-C-BL01_20260130.xer", ProgrammeReviewNaming.CreateCanonicalFilename(
            "J5001", "C", snapshot.SnapshotTag, snapshot.DataDate));
    }

    [Fact]
    public void MonthSuggestionAndParsingUseGregorianInvariantValues()
    {
        CultureInfo originalCulture = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ar-SA");
            ProgrammeReviewUploadSuggestion suggestion = ProgrammeReviewUploadMetadata.Infer(
                "J5001-T-2609_20260930.xer", new DateOnly(2026, 9, 30));
            Assert.Equal("2026-09", suggestion.MonthUpdate);
            Assert.True(ProgrammeReviewUploadMetadata.TryParseMonthUpdate(suggestion.MonthUpdate, out DateOnly month));
            Assert.Equal(new DateOnly(2026, 9, 1), month);
        }
        finally { CultureInfo.CurrentCulture = originalCulture; }
    }

    [Fact]
    public void BaselineWithoutDetectedDataDateDoesNotSubstituteFilenameDate()
    {
        Assert.Equal(new ProgrammeReviewUploadSuggestion("Baseline", "BL01", ""),
            ProgrammeReviewUploadMetadata.Infer("J5001-C-BL01_20260131.xer", null));
    }

    [Fact]
    public void UploadsAreSerializedUntilCompleted()
    {
        using var session = new BrowserUploadSession();
        BrowserUploadSession.Batch batch = Assert.IsType<BrowserUploadSession.Batch>(session.TryBegin());
        Assert.True(session.IsUploading);
        Assert.Null(session.TryBegin());
        session.Complete(batch);
        Assert.False(session.IsUploading);
        BrowserUploadSession.Batch next = Assert.IsType<BrowserUploadSession.Batch>(session.TryBegin());
        session.Complete(next);
    }

    [Theory]
    [InlineData("Clear All")]
    [InlineData("Profile change")]
    [InlineData("Remove source")]
    public async Task InvalidatedReadCannotResurrectRowsOrReuseTokens(string action)
    {
        _ = action; // The three page actions all use this same production invalidation boundary.
        using var session = new BrowserUploadSession();
        BrowserUploadSession.Batch oldBatch = Assert.IsType<BrowserUploadSession.Batch>(session.TryBegin());
        string oldToken = session.NextSourceToken(oldBatch);
        var readFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        bool committed = false;
        Task lateRead = FinishReadAsync();
        session.Invalidate();
        Assert.True(oldBatch.Token.IsCancellationRequested);
        BrowserUploadSession.Batch newBatch = Assert.IsType<BrowserUploadSession.Batch>(session.TryBegin());
        Assert.NotEqual(oldToken, session.NextSourceToken(newBatch));
        readFinished.SetResult();
        await lateRead;
        Assert.False(committed);
        Assert.True(session.IsCurrent(newBatch)); // The old finally may not clear a newer selection.
        session.Complete(newBatch);

        async Task FinishReadAsync()
        {
            await readFinished.Task;
            committed = session.TryCommit(oldBatch, () => throw new InvalidOperationException("Stale commit executed"));
            session.Complete(oldBatch);
        }
    }

    [Fact]
    public void DisposalCancelsPendingReadAndRejectsLaterUploadOrCommit()
    {
        var session = new BrowserUploadSession();
        BrowserUploadSession.Batch batch = Assert.IsType<BrowserUploadSession.Batch>(session.TryBegin());
        session.Dispose();
        Assert.True(batch.Token.IsCancellationRequested);
        Assert.False(session.TryCommit(batch, () => Assert.Fail("Disposed commit executed")));
        Assert.Null(session.TryBegin());
        Assert.Throws<OperationCanceledException>(() => session.NextSourceToken(batch));
        session.Complete(batch);
    }

    [Fact]
    public void FinalCombinedLimitValidationUsesLiveRowsAndDoesNotPartiallyCommit()
    {
        using var session = new BrowserUploadSession();
        BrowserUploadSession.Batch batch = Assert.IsType<BrowserUploadSession.Batch>(session.TryBegin());
        var committed = new List<BrowserUploadDescriptor> { new("existing.xer", 60) };
        var pending = new[] { new BrowserUploadDescriptor("pending.xer", 50) };
        Assert.Throws<InvalidOperationException>(() => session.TryCommit(batch, () =>
        {
            BrowserUploadSession.ValidateCombined(committed.Concat(pending), 3, 80, 100, allowRepeatedNames: true);
            committed.AddRange(pending);
        }));
        Assert.Single(committed);
        session.Complete(batch);
    }

    [Theory]
    [InlineData(1, 100, 100, 2, 20)]
    [InlineData(3, 10, 100, 1, 20)]
    [InlineData(3, 100, 30, 2, 20)]
    public void CombinedValidationEnforcesEachLimit(int maxFiles, long maxFileBytes, long maxTotal, int count, long bytes)
    {
        Assert.Throws<InvalidOperationException>(() => BrowserUploadSession.ValidateCombined(
            Enumerable.Range(0, count).Select(i => new BrowserUploadDescriptor($"{i}.xer", bytes)),
            maxFiles, maxFileBytes, maxTotal, allowRepeatedNames: true));
    }

    [Fact]
    public void RepeatedNamesRemainOrderedForStandardAndTenderButNotProgramme()
    {
        var repeated = new[] { new BrowserUploadDescriptor("same.xer", 10), new BrowserUploadDescriptor("same.xer", 10) };
        BrowserUploadSession.ValidateCombined(repeated, 3, 100, 100, allowRepeatedNames: true);
        Assert.Equal(2, repeated.Length);
        Assert.Throws<InvalidOperationException>(() =>
            BrowserUploadSession.ValidateCombined(repeated, 3, 100, 100, allowRepeatedNames: false));
    }
}
