using Microsoft.JSInterop;

namespace XerToCsvConverter.Web.Services;

public class DownloadService(IJSRuntime jsRuntime)
{
    private readonly IJSRuntime _jsRuntime = jsRuntime;

    public async Task DownloadFileAsync(string filename, byte[] content, string contentType = "application/octet-stream")
    {
        var base64 = Convert.ToBase64String(content);
        await _jsRuntime.InvokeVoidAsync("downloadFile", filename, contentType, base64);
    }
}
