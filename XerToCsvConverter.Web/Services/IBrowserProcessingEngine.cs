using XerToCsvConverter.BrowserProcessing;

namespace XerToCsvConverter.Web.Services;

internal interface IBrowserProcessingEngine : IAsyncDisposable
{
    bool IsFallback { get; }
    string? FallbackReason { get; }
    Task<BrowserResult> RegisterAsync(BrowserSource source, int selectionIndex, CancellationToken cancellationToken = default);
    Task<BrowserResult> RunAsync(BrowserJob job, IProgress<ProcessingService.DetailedProgress>? progress = null,
        CancellationToken cancellationToken = default);
    Task CommitAsync(BrowserResult result, CancellationToken cancellationToken = default);
    Task RemoveAsync(string sourceToken);
    Task ClearAsync();
    Task InvalidateAsync();
}
