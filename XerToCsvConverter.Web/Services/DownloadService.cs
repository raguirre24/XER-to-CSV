using Microsoft.JSInterop;

namespace XerToCsvConverter.Web.Services;

public class DownloadService(IJSRuntime jsRuntime)
{
    private readonly IJSRuntime _jsRuntime = jsRuntime;

    public async Task DownloadFileAsync(
        string filename,
        Stream content,
        string contentType = "application/octet-stream",
        CancellationToken cancellationToken = default)
    {
        string downloadId = await PrepareFileAsync(filename, content, contentType, cancellationToken);
        await CommitPreparedAsync(downloadId, cancellationToken);
    }

    public async Task<string> PrepareFileAsync(
        string filename,
        Stream content,
        string contentType = "application/octet-stream",
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);
        if (!content.CanRead) throw new ArgumentException("The download stream must be readable.", nameof(content));
        cancellationToken.ThrowIfCancellationRequested();
        if (content.CanSeek) content.Position = 0;

        using var streamReference = new DotNetStreamReference(content, leaveOpen: true);
        string downloadId = Guid.NewGuid().ToString("N");
        try
        {
            // Keep the stream alive until JavaScript finishes reading it. Cancelling only the
            // managed interop wait would leave JavaScript running against a disposed reference.
            await _jsRuntime.InvokeVoidAsync(
                "xerDownloads.prepare", downloadId, filename, contentType, streamReference);
            cancellationToken.ThrowIfCancellationRequested();
            return downloadId;
        }
        catch
        {
            await DiscardPreparedAsync(downloadId);
            throw;
        }
    }

    public async Task CommitPreparedAsync(string downloadId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(downloadId);
        bool handoffAttempted = false;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            // Once submitted, browser delivery cannot be recalled. Do not cancel the
            // interop wait or perform a post-handoff cancellation check.
            handoffAttempted = true;
            await _jsRuntime.InvokeVoidAsync("xerDownloads.commit", downloadId);
        }
        catch (Exception ex) when (handoffAttempted)
        {
            // An interop timeout/disconnection does not prove that link.click() never ran.
            throw new InvalidOperationException(
                "Browser download handoff could not be confirmed. The download may already have started; check browser downloads before retrying.", ex);
        }
        finally
        {
            await DiscardPreparedAsync(downloadId);
        }
    }

    public async Task DiscardPreparedAsync(string downloadId)
    {
        try { await _jsRuntime.InvokeVoidAsync("xerDownloads.discard", downloadId); }
        catch (JSDisconnectedException) { }
        catch (JSException) { /* A disconnected page cannot retain a usable download. */ }
        catch (OperationCanceledException) { /* Cleanup cannot undo an acknowledged handoff. */ }
    }
}
