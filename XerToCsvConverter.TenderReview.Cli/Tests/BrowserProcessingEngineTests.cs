using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.JSInterop;
using XerToCsvConverter.BrowserProcessing;
using XerToCsvConverter.TenderReview;
using XerToCsvConverter.Web.Services;
using Xunit;

namespace XerToCsvConverter.TenderReview.Surface.Tests;

public sealed class BrowserProcessingEngineTests
{
    [Fact]
    public async Task Registration_keeps_a_file_occurrence_and_runs_only_its_metadata_job()
    {
        var js = new FakeJs();
        await using var engine = new BrowserProcessingEngine(js);
        var source = new BrowserSource("token-a", "stage.xer", 42);
        BrowserResult result = await engine.RegisterAsync(source, 2);

        Assert.Equal(new[] { "registerSelection", "run" }, js.Module.Calls.Select(c => c.Identifier));
        Assert.Equal(new object?[] { "token-a", 2, "stage.xer", 42L }, js.Module.Calls[0].Arguments);
        BrowserJob job = Assert.IsType<BrowserJob>(js.Module.Calls[1].Arguments[0]);
        Assert.Equal("metadata", job.Kind);
        Assert.Equal(source, Assert.Single(job.Sources));
        Assert.NotEmpty(job.JobId);
        Assert.Equal(1, job.Generation);
        Assert.Equal(job.JobId, result.JobId);
        Assert.Equal(job.Generation, result.Generation);
        Assert.False(engine.IsFallback);
        Assert.Null(engine.FallbackReason);
    }

    [Fact]
    public async Task Worker_bridge_import_uses_the_current_page_version_as_an_escaped_query()
    {
        var js = new FakeJs { ApplicationVersion = "build /?+%#é" };
        await using var engine = new BrowserProcessingEngine(js);
        _ = await engine.RunAsync(Job("parse"));
        Invocation import = Assert.Single(js.Calls, call => call.Identifier == "import");
        Assert.Equal("./js/workerEngine.js?v=build%20%2F%3F%2B%25%23%C3%A9", Assert.Single(import.Arguments));
        Assert.Equal(new[] { "xerDownloads.applicationVersion", "import" }, js.Calls.Select(call => call.Identifier));
    }

    [Fact]
    public async Task Jobs_snapshot_caller_collections_and_map_worker_progress_fields()
    {
        var entered = Gate();
        var release = Gate();
        var js = new FakeJs();
        BrowserJob? captured = null;
        js.Module.Handler = async call =>
        {
            if (call.Identifier != "run") return null;
            captured = Assert.IsType<BrowserJob>(call.Arguments[0]);
            var receiver = Assert.IsType<DotNetObjectReference<BrowserProcessingEngine.ProgressReceiver>>(call.Arguments[1]);
            entered.SetResult();
            await release.Task;
            using JsonDocument payload = JsonDocument.Parse("""{"Percent":47,"message":"Parsing","filePath":"stage.xer","fileStatus":"Parsing","statusColor":"Blue","inputIndex":1,"sourceToken":"token-a"}""");
            receiver.Value.OnProgress(payload.RootElement);
            return Result(captured);
        };
        var sources = new List<BrowserSource> { new("token-a", "stage.xer", 42) };
        var tables = new List<string> { "TASK" };
        var requested = new BrowserJob { Kind = "export", Sources = sources, Tables = tables, JobId = "caller", Generation = 999 };
        var progress = new ProgressRecorder();
        await using var engine = new BrowserProcessingEngine(js);
        Task<BrowserResult> operation = engine.RunAsync(requested, progress);
        await entered.Task;
        sources.Clear();
        tables.Add("PROJECT");
        release.SetResult();
        _ = await operation;

        Assert.NotNull(captured);
        Assert.Single(captured.Sources);
        Assert.Equal(new[] { "TASK" }, captured.Tables);
        Assert.NotEqual(requested.JobId, captured.JobId);
        Assert.Equal(1, captured.Generation);
        ProcessingService.DetailedProgress update = Assert.Single(progress.Values);
        Assert.Equal(47, update.Percent);
        Assert.Equal("Parsing", update.Message);
        Assert.Equal("stage.xer", update.FilePath);
        Assert.Equal("Parsing", update.FileStatus);
        Assert.Equal("Blue", update.StatusColor);
        Assert.Equal(1, update.InputIndex);
        Assert.Equal("token-a", update.SourceToken);
        Assert.All(js.Module.Calls, call => Assert.False(call.Token.CanBeCanceled));
    }

    [Fact]
    public async Task Tender_processing_freezes_ordered_duplicate_occurrences_and_rejects_an_overlapping_upload()
    {
        var entered = Gate();
        var release = Gate();
        var js = new FakeJs();
        BrowserJob? captured = null;
        js.Module.Handler = async call =>
        {
            if (call.Identifier != "run") return null;
            captured = Assert.IsType<BrowserJob>(call.Arguments[0]);
            entered.SetResult();
            await release.Task;
            return Result(captured);
        };
        string firstToken = TenderReviewNaming.CreateSourceToken(0), secondToken = TenderReviewNaming.CreateSourceToken(1);
        var sources = new List<BrowserSource> { new(firstToken, "same.xer", 42), new(secondToken, "same.xer", 42) };
        var stages = new List<TenderReviewSource>
        {
            new() { SourceToken = firstToken, OriginalXerFilename = "same.xer", StatusDate = new DateOnly(2026, 9, 5) },
            new() { SourceToken = secondToken, OriginalXerFilename = "same.xer", StatusDate = new DateOnly(2026, 9, 6) }
        };
        var request = new TenderReviewBundleRequest { ProjectCode = "J5001", ProjectName = "Test Project", Sources = stages };
        await using var engine = new BrowserProcessingEngine(js);
        Task<BrowserResult> operation = engine.RunAsync(new BrowserJob { Kind = "tender", Sources = sources, TenderRequest = request });
        await entered.Task;
        await Assert.ThrowsAsync<InvalidOperationException>(() => engine.RegisterAsync(new BrowserSource("new-source", "new.xer", 5), 0));
        sources.Clear();
        stages.Reverse();
        release.SetResult();
        _ = await operation;

        Assert.NotNull(captured);
        Assert.Equal(new[] { firstToken, secondToken }, captured.Sources.Select(source => source.SourceToken));
        Assert.Equal(new[] { "same.xer", "same.xer" }, captured.Sources.Select(source => source.Name));
        Assert.Equal(new[] { firstToken, secondToken }, captured.TenderRequest!.Sources.Select(source => source.SourceToken));
        Assert.Equal(new[] { new DateOnly(2026, 9, 5), new DateOnly(2026, 9, 6) }, captured.TenderRequest.Sources.Select(source => source.StatusDate));
        Assert.DoesNotContain(js.Module.Calls, call => call.Identifier == "registerSelection");
        Assert.Equal(1, js.Module.Calls.Count(call => call.Identifier == "run"));
    }

    [Fact]
    public async Task Startup_failure_switches_once_to_cached_in_page_processing_and_prepared_downloads()
    {
        byte[] file = Encoding.UTF8.GetBytes("%T\tPROJECT\r\n%F\tproj_id\tlast_recalc_date\r\n%R\tP1\t2026-08-31 08:00\r\n%E\r\n");
        var js = new FakeJs();
        js.Module.Handler = call => call.Identifier switch
        {
            "run" => Task.FromException<object?>(new JSException("XER_WORKER_STARTUP: synthetic boot failure")),
            "readBytes" => Task.FromResult<object?>(file.ToArray()),
            _ => Task.FromResult<object?>(null)
        };
        byte[]? zipBytes = null;
        js.Handler = async call =>
        {
            if (call.Identifier == "xerDownloads.prepare")
            {
                var reference = Assert.IsType<DotNetStreamReference>(call.Arguments[3]);
                Assert.True(reference.Stream.CanRead);
                using var copy = new MemoryStream();
                await reference.Stream.CopyToAsync(copy);
                zipBytes = copy.ToArray();
            }
            return null;
        };
        await using var engine = new BrowserProcessingEngine(js);
        var source = new BrowserSource("token-a", "stage.xer", file.Length);
        BrowserResult metadata = await engine.RegisterAsync(source, 0);
        Assert.Equal("2026-08-31", metadata.DataDate);
        Assert.Equal(Convert.ToHexString(SHA256.HashData(file)).ToLowerInvariant(), metadata.SourceSha256);
        Assert.True(engine.IsFallback);
        Assert.Contains("synthetic boot failure", engine.FallbackReason, StringComparison.Ordinal);
        source = source with { SourceSha256 = metadata.SourceSha256 };
        BrowserResult parsed = await engine.RunAsync(Job("parse", source));
        BrowserResult exported = await engine.RunAsync(Job("export", source) with { Tables = ["PROJECT"] });

        Assert.Equal(1, parsed.ParsedSources);
        Assert.Equal(0, exported.ParsedSources);
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "run"));
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "readBytes"));
        Assert.NotNull(exported.DownloadId);
        Assert.Null(exported.ZipContent);
        Assert.NotNull(zipBytes);
        using var zip = new ZipArchive(new MemoryStream(zipBytes), ZipArchiveMode.Read);
        Assert.NotNull(zip.GetEntry("PROJECT.csv"));
        Assert.DoesNotContain(js.Calls, c => c.Identifier == "xerDownloads.commit");
        await engine.CommitAsync(exported);
        Assert.Equal(new[] { "xerDownloads.prepare", "xerDownloads.commit", "xerDownloads.discard" },
            js.Calls.Where(c => c.Identifier is "xerDownloads.prepare" or "xerDownloads.commit" or "xerDownloads.discard").Select(c => c.Identifier));
        Assert.DoesNotContain(js.Module.Calls, c => c.Identifier is "commit" or "discard");
        Assert.All(js.Calls, call => Assert.False(call.Token.CanBeCanceled));
    }

    [Fact]
    public async Task Processing_errors_are_staged_without_retrying_or_reading_input_bytes()
    {
        var js = new FakeJs();
        js.Module.Handler = call => call.Identifier == "run"
            ? Task.FromException<object?>(new JSException("Invalid source table"))
            : Task.FromResult<object?>(null);
        await using var engine = new BrowserProcessingEngine(js);
        BrowserProcessingException error = await Assert.ThrowsAsync<BrowserProcessingException>(() => engine.RunAsync(Job("parse")));
        Assert.Equal("worker-processing", error.Stage);
        Assert.Contains("Invalid source table", error.Message, StringComparison.Ordinal);
        Assert.False(engine.IsFallback);
        Assert.DoesNotContain(js.Module.Calls, c => c.Identifier == "readBytes");
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "run"));
    }

    [Fact]
    public async Task Fallback_errors_are_staged_and_never_restart_the_worker()
    {
        var js = new FakeJs();
        js.Module.Handler = call => call.Identifier switch
        {
            "run" => Task.FromException<object?>(new JSException("XER_WORKER_STARTUP: unavailable")),
            "readBytes" => Task.FromResult<object?>(new byte[] { 1, 2 }),
            _ => Task.FromResult<object?>(null)
        };
        await using var engine = new BrowserProcessingEngine(js);
        BrowserProcessingException error = await Assert.ThrowsAsync<BrowserProcessingException>(() =>
            engine.RegisterAsync(new BrowserSource("token-a", "stage.xer", 3), 0));
        Assert.Equal("fallback-processing", error.Stage);
        Assert.Contains("Browser file length changed", error.Message, StringComparison.Ordinal);
        await Assert.ThrowsAsync<BrowserProcessingException>(() => engine.RunAsync(Job("parse", new BrowserSource("token-a", "stage.xer", 3))));
        Assert.True(engine.IsFallback);
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "run"));
        Assert.DoesNotContain(js.Calls, c => c.Identifier == "xerDownloads.commit");
    }

    [Fact]
    public async Task Cancellation_keeps_callback_alive_until_worker_settles_and_discards_a_late_result()
    {
        var entered = Gate();
        var release = Gate();
        var cancelEntered = Gate();
        var js = new FakeJs();
        DotNetObjectReference<BrowserProcessingEngine.ProgressReceiver>? callback = null;
        js.Module.Handler = async call =>
        {
            if (call.Identifier == "cancel") cancelEntered.TrySetResult();
            if (call.Identifier != "run") return null;
            BrowserJob job = Assert.IsType<BrowserJob>(call.Arguments[0]);
            callback = Assert.IsType<DotNetObjectReference<BrowserProcessingEngine.ProgressReceiver>>(call.Arguments[1]);
            entered.SetResult();
            await release.Task;
            return Result(job, "late-zip");
        };
        using var cancellation = new CancellationTokenSource();
        var progress = new ProgressRecorder();
        await using var engine = new BrowserProcessingEngine(js);
        Task<BrowserResult> operation = engine.RunAsync(Job("export"), progress, cancellation.Token);
        await entered.Task;
        cancellation.Cancel();
        await cancelEntered.Task;
        Assert.False(operation.IsCompleted);
        using JsonDocument payload = JsonDocument.Parse("""{"percent":70,"message":"late progress"}""");
        callback!.Value.OnProgress(payload.RootElement);
        Assert.Empty(progress.Values);
        release.SetResult();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation);
        Assert.Contains(js.Module.Calls, c => c.Identifier == "discard" && Equals(c.Arguments[0], "late-zip"));
        Assert.DoesNotContain(js.Module.Calls, c => c.Identifier == "commit");
        Assert.All(js.Module.Calls, call => Assert.False(call.Token.CanBeCanceled));
    }

    [Fact]
    public async Task A_second_processing_operation_is_rejected_until_the_first_settles()
    {
        var entered = Gate();
        var release = Gate();
        var js = new FakeJs();
        js.Module.Handler = async call =>
        {
            if (call.Identifier != "run") return null;
            entered.TrySetResult();
            await release.Task;
            return Result(Assert.IsType<BrowserJob>(call.Arguments[0]));
        };
        await using var engine = new BrowserProcessingEngine(js);
        Task<BrowserResult> first = engine.RunAsync(Job("parse"));
        await entered.Task;
        await Assert.ThrowsAsync<InvalidOperationException>(() => engine.RunAsync(Job("parse")));
        release.SetResult();
        _ = await first;
        _ = await engine.RunAsync(Job("parse"));
        Assert.Equal(2, js.Module.Calls.Count(c => c.Identifier == "run"));
    }

    [Fact]
    public async Task Invalidation_cancels_settles_and_blocks_new_jobs_until_both_runtimes_are_updated()
    {
        var entered = Gate();
        var releaseRun = Gate();
        var enteredInvalidate = Gate();
        var releaseInvalidate = Gate();
        var js = new FakeJs();
        js.Module.Handler = async call =>
        {
            if (call.Identifier == "invalidate")
            {
                enteredInvalidate.SetResult();
                await releaseInvalidate.Task;
            }
            if (call.Identifier != "run") return null;
            entered.TrySetResult();
            await releaseRun.Task;
            return Result(Assert.IsType<BrowserJob>(call.Arguments[0]), "invalidated-zip");
        };
        await using var engine = new BrowserProcessingEngine(js);
        Task<BrowserResult> operation = engine.RunAsync(Job("export"));
        await entered.Task;
        Task mutation = engine.InvalidateAsync();
        Assert.False(mutation.IsCompleted);
        Assert.DoesNotContain(js.Module.Calls, c => c.Identifier == "invalidate");
        releaseRun.SetResult();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation);
        await enteredInvalidate.Task;
        Task<BrowserResult> queued = engine.RunAsync(Job("parse"));
        Assert.False(queued.IsCompleted);
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "run"));
        releaseInvalidate.SetResult();
        await mutation;
        BrowserResult next = await queued;
        Assert.Equal(2, next.Generation);
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "discard"));
    }

    [Fact]
    public async Task Overlapping_mutations_are_serialized_before_a_queued_run_or_commit()
    {
        var entered = Gate();
        var release = Gate();
        var js = new FakeJs();
        js.Module.Handler = async call =>
        {
            if (call.Identifier == "invalidate")
            {
                entered.SetResult();
                await release.Task;
            }
            BrowserJob? job = call.Identifier == "run" ? Assert.IsType<BrowserJob>(call.Arguments[0]) : null;
            return job is null ? null : Result(job, job.JobId);
        };
        await using var engine = new BrowserProcessingEngine(js);
        BrowserResult prepared = await engine.RunAsync(Job("export"));
        Task first = engine.InvalidateAsync();
        await entered.Task;
        Task second = engine.RemoveAsync("token-a");
        Task commit = engine.CommitAsync(prepared);
        Task<BrowserResult> next = engine.RunAsync(Job("parse"));
        Assert.False(second.IsCompleted);
        Assert.False(commit.IsCompleted);
        Assert.False(next.IsCompleted);
        Assert.DoesNotContain(js.Module.Calls, c => c.Identifier == "remove");
        release.SetResult();
        await Task.WhenAll(first, second);
        await Assert.ThrowsAsync<InvalidOperationException>(() => commit);
        Assert.Equal(3, (await next).Generation);
        Assert.DoesNotContain(js.Module.Calls, c => c.Identifier == "commit");
        string[] mutations = js.Module.Calls.Where(c => c.Identifier is "invalidate" or "remove").Select(c => c.Identifier).ToArray();
        Assert.Equal(new[] { "invalidate", "remove" }, mutations);
    }

    [Fact]
    public async Task A_stale_worker_result_is_rejected_and_its_prepared_download_discarded()
    {
        var js = new FakeJs();
        js.Module.Handler = call => Task.FromResult<object?>(call.Identifier == "run"
            ? Result(Assert.IsType<BrowserJob>(call.Arguments[0]), "stale-zip") with { Generation = 0 }
            : null);
        await using var engine = new BrowserProcessingEngine(js);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => engine.RunAsync(Job("export")));
        Assert.Contains(js.Module.Calls, c => c.Identifier == "discard" && Equals(c.Arguments[0], "stale-zip"));
        Assert.DoesNotContain(js.Module.Calls, c => c.Identifier == "commit");
    }

    [Fact]
    public async Task Pre_handoff_cancellation_discards_but_cancellation_at_handoff_does_not_claim_failure()
    {
        var js = new FakeJs();
        using var cancellation = new CancellationTokenSource();
        js.Module.Handler = call =>
        {
            if (call.Identifier == "commit") cancellation.Cancel();
            return Task.FromResult<object?>(call.Identifier == "run"
                ? Result(Assert.IsType<BrowserJob>(call.Arguments[0]), Guid.NewGuid().ToString("N")) : null);
        };
        await using var engine = new BrowserProcessingEngine(js);
        BrowserResult first = await engine.RunAsync(Job("export"));
        using var preCancelled = new CancellationTokenSource();
        preCancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => engine.CommitAsync(first, preCancelled.Token));
        Assert.DoesNotContain(js.Module.Calls, c => c.Identifier == "commit");
        BrowserResult second = await engine.RunAsync(Job("export"));
        await engine.CommitAsync(second, cancellation.Token);
        Assert.True(cancellation.IsCancellationRequested);
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "commit"));
        Assert.Equal(2, js.Module.Calls.Count(c => c.Identifier == "discard"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => engine.CommitAsync(second));
    }

    [Fact]
    public async Task Uncertain_worker_handoff_is_reported_as_possibly_started_and_cannot_be_recommitted()
    {
        var js = new FakeJs();
        js.Module.Handler = call => call.Identifier == "commit"
            ? Task.FromException<object?>(new OperationCanceledException("Synthetic timeout after click"))
            : Task.FromResult<object?>(call.Identifier == "run" ? Result(Assert.IsType<BrowserJob>(call.Arguments[0]), "zip") : null);
        await using var engine = new BrowserProcessingEngine(js);
        BrowserResult result = await engine.RunAsync(Job("export"));
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() => engine.CommitAsync(result));
        Assert.Contains("may already have started", error.Message, StringComparison.Ordinal);
        await Assert.ThrowsAsync<InvalidOperationException>(() => engine.CommitAsync(result));
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "commit"));
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "discard"));
    }

    [Fact]
    public async Task Session_updates_discard_prepared_downloads_and_disposal_releases_the_module_once()
    {
        var js = new FakeJs();
        js.Module.Handler = call => Task.FromResult<object?>(call.Identifier == "run"
            ? Result(Assert.IsType<BrowserJob>(call.Arguments[0]), Guid.NewGuid().ToString("N")) : null);
        var engine = new BrowserProcessingEngine(js);
        BrowserResult first = await engine.RunAsync(Job("export"));
        await engine.RemoveAsync("token-a");
        await Assert.ThrowsAsync<InvalidOperationException>(() => engine.CommitAsync(first));
        BrowserResult second = await engine.RunAsync(Job("export"));
        Assert.Equal(2, second.Generation);
        await engine.ClearAsync();
        _ = await engine.RunAsync(Job("export"));
        await engine.DisposeAsync();
        await engine.DisposeAsync();
        Assert.Equal(3, js.Module.Calls.Count(c => c.Identifier == "discard"));
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "remove"));
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "clear"));
        Assert.Equal(1, js.Module.Calls.Count(c => c.Identifier == "dispose"));
        Assert.Equal(1, js.Module.DisposeCount);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => engine.RunAsync(Job("parse")));
    }

    [Fact]
    public async Task Disposal_waits_for_active_metadata_before_releasing_its_callback_and_module()
    {
        var entered = Gate();
        var release = Gate();
        var js = new FakeJs();
        js.Module.Handler = async call =>
        {
            if (call.Identifier != "run") return null;
            entered.SetResult();
            await release.Task;
            var callback = Assert.IsType<DotNetObjectReference<BrowserProcessingEngine.ProgressReceiver>>(call.Arguments[1]);
            using JsonDocument payload = JsonDocument.Parse("""{"percent":50}""");
            callback.Value.OnProgress(payload.RootElement);
            return Result(Assert.IsType<BrowserJob>(call.Arguments[0]));
        };
        var engine = new BrowserProcessingEngine(js);
        Task<BrowserResult> metadata = engine.RegisterAsync(new BrowserSource("token-a", "stage.xer", 42), 0);
        await entered.Task;
        Task disposal = engine.DisposeAsync().AsTask();
        Assert.False(disposal.IsCompleted);
        Assert.DoesNotContain(js.Module.Calls, c => c.Identifier == "dispose");
        Assert.Equal(0, js.Module.DisposeCount);
        release.SetResult();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => metadata);
        await disposal;
        Assert.Contains(js.Module.Calls, c => c.Identifier == "cancel");
        Assert.Contains(js.Module.Calls, c => c.Identifier == "dispose");
        Assert.Equal(1, js.Module.DisposeCount);
    }

    private static TaskCompletionSource Gate() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private static BrowserJob Job(string kind, params BrowserSource[] sources) => new() { Kind = kind, Sources = sources };
    private static BrowserResult Result(BrowserJob job, string? downloadId = null) => new()
    {
        JobId = job.JobId, Generation = job.Generation, DownloadId = downloadId
    };

    private sealed class ProgressRecorder : IProgress<ProcessingService.DetailedProgress>
    {
        public List<ProcessingService.DetailedProgress> Values { get; } = [];
        public void Report(ProcessingService.DetailedProgress value) => Values.Add(value);
    }

    private sealed record Invocation(string Identifier, object?[] Arguments, CancellationToken Token);

    private sealed class FakeJs : IJSRuntime
    {
        public FakeModule Module { get; } = new();
        public string ApplicationVersion { get; init; } = "test-build";
        public List<Invocation> Calls { get; } = [];
        public Func<Invocation, Task<object?>>? Handler { get; set; }
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) => InvokeAsync<TValue>(identifier, CancellationToken.None, args);
        public async ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken token, object?[]? args)
        {
            var call = new Invocation(identifier, args ?? [], token);
            Calls.Add(call);
            if (identifier == "xerDownloads.applicationVersion") return (TValue)(object)ApplicationVersion;
            if (identifier == "import")
            {
                Assert.StartsWith("./js/workerEngine.js?v=", Assert.IsType<string>(Assert.Single(call.Arguments)), StringComparison.Ordinal);
                return (TValue)(object)Module;
            }
            object? result = Handler is null ? null : await Handler(call);
            return result is TValue value ? value : default!;
        }
    }

    private sealed class FakeModule : IJSObjectReference
    {
        public List<Invocation> Calls { get; } = [];
        public int DisposeCount { get; private set; }
        public Func<Invocation, Task<object?>>? Handler { get; set; }
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) => InvokeAsync<TValue>(identifier, CancellationToken.None, args);
        public async ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken token, object?[]? args)
        {
            var call = new Invocation(identifier, args ?? [], token);
            Calls.Add(call);
            object? result = Handler is not null ? await Handler(call)
                : identifier == "run" ? Result(Assert.IsType<BrowserJob>(call.Arguments[0])) : null;
            return result is TValue value ? value : default!;
        }
        public ValueTask DisposeAsync() { DisposeCount++; return ValueTask.CompletedTask; }
    }
}
