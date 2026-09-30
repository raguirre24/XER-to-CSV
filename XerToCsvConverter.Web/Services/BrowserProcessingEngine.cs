using System.Text.Json;
using Microsoft.JSInterop;
using XerToCsvConverter.BrowserProcessing;

namespace XerToCsvConverter.Web.Services;

/// <summary>Owns one worker or fallback session and the lifetime of its browser operations.</summary>
internal sealed class BrowserProcessingEngine : IBrowserProcessingEngine
{
    private readonly IJSRuntime _js;
    private readonly DownloadService _downloads;
    private readonly BrowserProcessingSession _fallbackSession;
    private readonly Dictionary<string, PreparedDownload> _prepared = new(StringComparer.Ordinal);
    private Task<IJSObjectReference>? _moduleTask;
    private IJSObjectReference? _module;
    private ActiveOperation? _active;
    private TaskCompletionSource? _commitSettled;
    private TaskCompletionSource? _mutationSettled;
    private long _generation = 1;
    private bool _disposed;
    public bool IsFallback { get; private set; }
    public string? FallbackReason { get; private set; }

    public BrowserProcessingEngine(IJSRuntime js)
    {
        _js = js ?? throw new ArgumentNullException(nameof(js));
        _downloads = new DownloadService(js);
        _fallbackSession = new BrowserProcessingSession(ReadBytesAsync);
    }

    public Task<BrowserResult> RegisterAsync(BrowserSource source, int selectionIndex, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (selectionIndex < 0) throw new ArgumentOutOfRangeException(nameof(selectionIndex));
        return ExecuteAsync(new BrowserJob { Kind = "metadata", Sources = [source] }, null, cancellationToken, selectionIndex);
    }

    public Task<BrowserResult> RunAsync(BrowserJob job, IProgress<ProcessingService.DetailedProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        return ExecuteAsync(job, progress, cancellationToken);
    }

    private async Task<BrowserResult> ExecuteAsync(BrowserJob requested, IProgress<ProcessingService.DetailedProgress>? progress,
        CancellationToken cancellationToken, int? selectionIndex = null)
    {
        ThrowIfDisposed();
        if (_active is not null || _commitSettled is not null)
            throw new InvalidOperationException("A browser processing operation is already running.");
        await AwaitMutationsAsync();
        ThrowIfDisposed();
        if (_active is not null || _commitSettled is not null)
            throw new InvalidOperationException("A browser processing operation is already running.");
        cancellationToken.ThrowIfCancellationRequested();
        BrowserJob job = requested with
        {
            JobId = Guid.NewGuid().ToString("N"), Generation = _generation,
            Sources = requested.Sources.ToArray(), Tables = requested.Tables.ToArray(),
            ProgrammeRequest = requested.ProgrammeRequest is { } programme
                ? programme with { Snapshots = programme.Snapshots.ToArray() } : null,
            TenderRequest = requested.TenderRequest is { } tender
                ? tender with { Sources = tender.Sources.ToArray() } : null
        };
        using var operation = new ActiveOperation(job.JobId, job.Generation, cancellationToken);
        _active = operation;
        using var receiver = DotNetObjectReference.Create(new ProgressReceiver(value =>
        {
            if (_active == operation && operation.Generation == _generation && !operation.Token.IsCancellationRequested)
                progress?.Report(ReadProgress(value));
        }));
        using CancellationTokenRegistration cancellation = operation.Token.Register(() => _ = CancelWorkerAsync(operation.JobId));
        BrowserResult? result = null;
        string stage = "worker-startup";
        try
        {
            IJSObjectReference module = await GetModuleAsync();
            operation.Token.ThrowIfCancellationRequested();
            if (selectionIndex is int index)
            {
                BrowserSource source = job.Sources[0];
                await module.InvokeVoidAsync("registerSelection", source.SourceToken, index, source.Name, source.Size);
                operation.Token.ThrowIfCancellationRequested();
            }
            if (!IsFallback)
            {
                stage = "worker-processing";
                try
                {
                    // Cancellation asks JS to stop/terminate its job; it never abandons the
                    // interop wait or disposes the callback while JS may still be using it.
                    result = await module.InvokeAsync<BrowserResult>("run", job, receiver);
                }
                catch (JSException exception) when (exception.Message.StartsWith("XER_WORKER_STARTUP:", StringComparison.Ordinal))
                {
                    operation.Token.ThrowIfCancellationRequested();
                    IsFallback = true;
                    FallbackReason = exception.Message;
                    progress?.Report(new ProcessingService.DetailedProgress
                    {
                        Percent = 0, Message = "The processing worker could not start; continuing in this browser tab."
                    });
                }
            }
            if (IsFallback)
            {
                stage = "fallback-processing";
                result = await _fallbackSession.RunAsync(job, progress, operation.Token);
                if (result.ZipContent is { } zip)
                {
                    stage = "download-preparation";
                    using (zip)
                    {
                        string id = await _downloads.PrepareFileAsync(result.Filename, zip, "application/zip", operation.Token);
                        result = result with { ZipContent = null, DownloadId = id };
                    }
                }
            }
            if (result is null) throw new InvalidOperationException("The browser worker returned no processing result.");
            if (!string.IsNullOrWhiteSpace(result.DownloadId))
                _prepared.Add(result.DownloadId, new PreparedDownload(!IsFallback, job.JobId, job.Generation));
            operation.Token.ThrowIfCancellationRequested();
            if (operation.Generation != _generation || result.Generation != job.Generation || result.JobId != job.JobId)
                throw new OperationCanceledException("The browser result belongs to an invalidated operation.");
            return result;
        }
        catch (Exception exception)
        {
            if (result?.DownloadId is { } id) await DiscardAsync(id);
            operation.Token.ThrowIfCancellationRequested();
            if (exception is OperationCanceledException) throw;
            throw new BrowserProcessingException(stage, exception.Message, exception);
        }
        finally
        {
            if (_active == operation) _active = null;
            operation.Settled.TrySetResult();
        }
    }

    public async Task CommitAsync(BrowserResult result, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(result);
        ThrowIfDisposed();
        if (_active is not null || _commitSettled is not null)
            throw new InvalidOperationException("A browser processing operation is already running.");
        await AwaitMutationsAsync();
        ThrowIfDisposed();
        if (_active is not null || _commitSettled is not null)
            throw new InvalidOperationException("A browser processing operation is already running.");
        if (result.DownloadId is not { Length: > 0 } id || !_prepared.TryGetValue(id, out PreparedDownload? prepared))
            throw new InvalidOperationException("The prepared browser download is no longer available.");
        var settled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _commitSettled = settled;
        bool handoffAttempted = false, fallbackCommitStarted = false;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (prepared.Generation != _generation || result.Generation != _generation || result.JobId != prepared.JobId)
                throw new OperationCanceledException("The prepared browser download belongs to an invalidated operation.");
            if (prepared.Worker)
            {
                handoffAttempted = true;
                await (await GetModuleAsync()).InvokeVoidAsync("commit", id);
            }
            else
            {
                fallbackCommitStarted = true;
                await _downloads.CommitPreparedAsync(id, cancellationToken);
            }
        }
        catch (Exception exception) when (handoffAttempted)
        {
            throw new InvalidOperationException(
                "Browser download handoff could not be confirmed. The download may already have started; check browser downloads before retrying.", exception);
        }
        finally
        {
            if (prepared.Worker || !fallbackCommitStarted) await DiscardAsync(id);
            else _prepared.Remove(id);
            _commitSettled = null;
            settled.TrySetResult();
        }
    }

    public Task RemoveAsync(string sourceToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceToken);
        return MutateAsync(() => _fallbackSession.Remove(sourceToken), "remove", sourceToken);
    }

    public Task ClearAsync() => MutateAsync(_fallbackSession.Clear, "clear");

    public Task InvalidateAsync() => MutateAsync(_fallbackSession.InvalidateDerived, "invalidate");

    private async Task MutateAsync(Action mutateFallback, string method, params object?[] arguments)
    {
        ThrowIfDisposed();
        Task previous = _mutationSettled?.Task ?? Task.CompletedTask;
        var settled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _mutationSettled = settled;
        _generation++;
        try
        {
            await previous;
            await SettleActiveAsync();
            await DiscardAllAsync();
            mutateFallback();
            if (_module is not null) await _module.InvokeVoidAsync(method, arguments);
        }
        finally
        {
            if (_mutationSettled == settled) _mutationSettled = null;
            settled.TrySetResult();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        _generation++;
        await SettleActiveAsync();
        if (_mutationSettled is { } mutation) await mutation.Task;
        await DiscardAllAsync();
        _fallbackSession.Clear();
        if (_module is not null)
        {
            try { await _module.InvokeVoidAsync("dispose"); }
            catch (JSDisconnectedException) { }
            catch (JSException) { }
            await _module.DisposeAsync();
        }
    }

    private Task<IJSObjectReference> GetModuleAsync() => _moduleTask ??= ImportModuleAsync();
    private async Task<IJSObjectReference> ImportModuleAsync()
    {
        string version = await _js.InvokeAsync<string>("xerDownloads.applicationVersion");
        _module = await _js.InvokeAsync<IJSObjectReference>("import", "./js/workerEngine.js?v=" + Uri.EscapeDataString(version));
        return _module;
    }

    private async Task<byte[]> ReadBytesAsync(BrowserSource source, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        byte[] bytes = await (await GetModuleAsync()).InvokeAsync<byte[]>("readBytes", source.SourceToken);
        cancellationToken.ThrowIfCancellationRequested();
        return bytes;
    }

    private async Task CancelWorkerAsync(string jobId)
    {
        if (_module is null || IsFallback) return;
        try { await _module.InvokeVoidAsync("cancel", jobId); }
        catch (JSDisconnectedException) { }
        catch (JSException) { }
        catch (OperationCanceledException) { }
    }

    private async Task SettleActiveAsync()
    {
        if (_active is { } active)
        {
            active.Cancellation.Cancel();
            await active.Settled.Task;
        }
        if (_commitSettled is { } commit) await commit.Task;
    }

    private async Task AwaitMutationsAsync()
    {
        while (_mutationSettled is { } mutation) await mutation.Task;
    }

    private async Task DiscardAllAsync()
    {
        foreach (string id in _prepared.Keys.ToArray()) await DiscardAsync(id);
    }

    private async Task DiscardAsync(string id)
    {
        if (!_prepared.Remove(id, out PreparedDownload? prepared)) return;
        if (!prepared.Worker) await _downloads.DiscardPreparedAsync(id);
        else if (_module is not null)
        {
            try { await _module.InvokeVoidAsync("discard", id); }
            catch (JSDisconnectedException) { }
            catch (JSException) { }
            catch (OperationCanceledException) { }
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private static ProcessingService.DetailedProgress ReadProgress(JsonElement value)
    {
        int? Number(string name) => Property(name) is { ValueKind: JsonValueKind.Number } number && number.TryGetInt32(out int integer) ? integer : null;
        string? Text(string name) => Property(name) is { ValueKind: JsonValueKind.String } text ? text.GetString() : null;
        JsonElement? Property(string name)
        {
            if (value.ValueKind == JsonValueKind.Object)
                foreach (JsonProperty property in value.EnumerateObject())
                    if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase)) return property.Value;
            return null;
        }
        return new ProcessingService.DetailedProgress
        {
            Percent = Number("percent") ?? 0, Message = Text("message"), FilePath = Text("filePath"), FileStatus = Text("fileStatus"),
            StatusColor = Text("statusColor"), InputIndex = Number("inputIndex"), SourceToken = Text("sourceToken")
        };
    }

    internal sealed class ProgressReceiver(Action<JsonElement> report)
    {
        [JSInvokable]
        public void OnProgress(JsonElement value) => report(value);
    }

    private sealed record PreparedDownload(bool Worker, string JobId, long Generation);
    private sealed class ActiveOperation(string jobId, long generation, CancellationToken cancellationToken) : IDisposable
    {
        public string JobId { get; } = jobId;
        public long Generation { get; } = generation;
        public CancellationTokenSource Cancellation { get; } = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        public CancellationToken Token => Cancellation.Token;
        public TaskCompletionSource Settled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Dispose() => Cancellation.Dispose();
    }
}

internal sealed class BrowserProcessingException(string stage, string message, Exception innerException)
    : InvalidOperationException($"{stage}: {message}", innerException)
{
    public string Stage { get; } = stage;
}
