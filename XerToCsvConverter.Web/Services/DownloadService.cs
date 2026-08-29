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
        await _jsRuntime.InvokeVoidAsync(
            "downloadFileFromStream", cancellationToken, filename, contentType, streamReference);
        cancellationToken.ThrowIfCancellationRequested();
    }
}
