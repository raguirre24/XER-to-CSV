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
        ArgumentNullException.ThrowIfNull(content);
        if (!content.CanRead) throw new ArgumentException("The download stream must be readable.", nameof(content));
        cancellationToken.ThrowIfCancellationRequested();
        if (content.CanSeek) content.Position = 0;

        using var streamReference = new DotNetStreamReference(content, leaveOpen: true);
        string downloadId = Guid.NewGuid().ToString("N");
        bool handoffAttempted = false;
        try
        {
            // Keep the stream alive until JavaScript finishes reading it. Cancelling only the
            // managed interop wait would leave JavaScript running against a disposed reference.
            await _jsRuntime.InvokeVoidAsync(
                "xerDownloads.prepare", downloadId, filename, contentType, streamReference);
            cancellationToken.ThrowIfCancellationRequested();

            // This is the irreversible boundary. No cancellation token or post-handoff
            // cancellation check: once submitted, browser delivery cannot be recalled.
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
            try { await _jsRuntime.InvokeVoidAsync("xerDownloads.discard", downloadId); }
            catch (JSDisconnectedException) { }
            catch (JSException) { /* A disconnected page cannot retain a usable download. */ }
            catch (OperationCanceledException) { /* Cleanup cannot undo an acknowledged handoff. */ }
        }
    }
}
