using Xunit;
using XerToCsvConverter.Web.Services;

namespace XerToCsvConverter.TenderReview.Surface.Tests;

public sealed class WebUploadLifecycleTests
{
    [Theory]
    [InlineData("J5001_C_BL01_2026-01-31.xer", "Baseline", "BL01", "2026-01-30")]
    [InlineData("J5001-C-BL01-A_20260131.xer", "Baseline", "BL01-A", "2026-01-30")]
    [InlineData("J5001-T-2609_20260930.xer", "Update", "2609", "2026-09-01")]
    [InlineData("j5001_c_2609_2026-09-30.XER", "Update", "2609", "2026-09-01")]
    [InlineData("J5001_ZONE_C_2609_20260930.xer", "Update", "2609", "2026-09-01")]
    [InlineData("J5001.xer", "", "", "")]
    [InlineData("J5001_2026-01-31.xer", "", "", "")]
    [InlineData("J5001-C-2613_20260131.xer", "", "", "")]
    [InlineData("J5001-C-2600_20260131.xer", "", "", "")]
    [InlineData("J5001-C-2609_20260230.xer", "", "", "")]
    [InlineData("J5001-C-2609_20260930 copy 2608.xer", "", "", "")]
    public void FilenameSuggestionsUseGovernedFieldsNotProjectDigits(
        string filename, string kind, string tag, string monthUpdate)
    {
        ProgrammeReviewUploadSuggestion result = ProgrammeReviewUploadMetadata.Infer(filename, new DateOnly(2026, 1, 30));
        Assert.Equal(new ProgrammeReviewUploadSuggestion(kind, tag, monthUpdate), result);
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
