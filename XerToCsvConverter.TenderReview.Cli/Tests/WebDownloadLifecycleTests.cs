using Microsoft.JSInterop;
using Xunit;
using XerToCsvConverter.Web.Services;

namespace XerToCsvConverter.TenderReview.Surface.Tests;

public sealed class WebDownloadLifecycleTests
{
    [Fact]
    public async Task CancellationBeforePreparationDoesNotCallJavaScript()
    {
        var js = new RecordingJsRuntime();
        using var content = new MemoryStream(new byte[] { 1, 2 });
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            new DownloadService(js).DownloadFileAsync("export.zip", content, cancellationToken: cancellation.Token));
        Assert.Empty(js.Calls);
        Assert.True(content.CanRead);
    }

    [Fact]
    public async Task CancellationDuringPreparationWaitsForStreamThenDiscardsWithoutClickHandoff()
    {
        var prepareEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var prepareFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        using var content = new MemoryStream(new byte[] { 1, 2 });
        var js = new RecordingJsRuntime
        {
            OnInvoke = async identifier =>
            {
                if (identifier != "xerDownloads.prepare") return;
                prepareEntered.SetResult();
                await prepareFinished.Task;
                Assert.True(content.CanRead);
            }
        };
        Task operation = new DownloadService(js).DownloadFileAsync("export.zip", content, cancellationToken: cancellation.Token);
        await prepareEntered.Task;
        cancellation.Cancel();
        Assert.False(operation.IsCompleted);
        prepareFinished.SetResult();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation);
        Assert.Equal(new[] { "xerDownloads.prepare", "xerDownloads.discard" }, js.Calls);
        Assert.All(js.Tokens, token => Assert.False(token.CanBeCanceled));
    }

    [Fact]
    public async Task CancellationAtHandoffDoesNotFalselyReportCancelledDownload()
    {
        using var cancellation = new CancellationTokenSource();
        using var content = new MemoryStream(new byte[] { 1, 2 });
        var js = new RecordingJsRuntime
        {
            OnInvoke = identifier =>
            {
                if (identifier == "xerDownloads.commit") cancellation.Cancel();
                return Task.CompletedTask;
            }
        };
        await new DownloadService(js).DownloadFileAsync("export.zip", content, cancellationToken: cancellation.Token);
        Assert.True(cancellation.IsCancellationRequested);
        Assert.Equal(new[] { "xerDownloads.prepare", "xerDownloads.commit", "xerDownloads.discard" }, js.Calls);
        Assert.True(content.CanRead);
    }

    [Fact]
    public async Task PreparationFailureStillDiscardsAndNeverHandsOff()
    {
        using var content = new MemoryStream(new byte[] { 1 });
        var js = new RecordingJsRuntime
        {
            OnInvoke = identifier => identifier == "xerDownloads.prepare"
                ? Task.FromException(new JSException("Synthetic failure"))
                : Task.CompletedTask
        };
        await Assert.ThrowsAsync<JSException>(() => new DownloadService(js).DownloadFileAsync("export.zip", content));
        Assert.Equal(new[] { "xerDownloads.prepare", "xerDownloads.discard" }, js.Calls);
    }

    [Fact]
    public async Task UncertainInteropCancellationAtHandoffIsNotClaimedAsNoDownload()
    {
        using var content = new MemoryStream(new byte[] { 1 });
        var js = new RecordingJsRuntime
        {
            OnInvoke = identifier => identifier == "xerDownloads.commit"
                ? Task.FromException(new OperationCanceledException("Synthetic interop timeout after click"))
                : Task.CompletedTask
        };
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            new DownloadService(js).DownloadFileAsync("export.zip", content));
        Assert.Contains("may already have started", error.Message, StringComparison.Ordinal);
        Assert.Equal(new[] { "xerDownloads.prepare", "xerDownloads.commit", "xerDownloads.discard" }, js.Calls);
    }

    private sealed class RecordingJsRuntime : IJSRuntime
    {
        public List<string> Calls { get; } = new();
        public List<CancellationToken> Tokens { get; } = new();
        public Func<string, Task>? OnInvoke { get; init; }

        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) =>
            InvokeAsync<TValue>(identifier, CancellationToken.None, args);

        public async ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args)
        {
            Calls.Add(identifier);
            Tokens.Add(cancellationToken);
            if (OnInvoke is not null) await OnInvoke(identifier);
            return default!;
        }
    }
}
