using System.Collections.ObjectModel;
using System.Security.Cryptography;

namespace XerToCsvConverter.ProgrammeReview;

/// <summary>
/// Public entrypoint for generating an atomic, report-ready Programme Review CSV bundle.
/// This is deliberately separate from the legacy Enhanced Power BI export workflow.
/// </summary>
public sealed class ProgrammeReviewBundleService
{
    /// <summary>
    /// Parses the configured XER paths, computes/verifies source hashes, retains the latest baseline and
    /// post-baseline updates, and publishes a local bundle directory atomically.
    /// </summary>
    public async Task<ProgrammeReviewBundleResult> BuildFromXerFilesAsync(
        ProgrammeReviewBundleRequest request,
        string outputRoot,
        IProgress<ProcessingService.DetailedProgress>? parseProgress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Snapshots is null)
            throw new ProgrammeReviewValidationException("Snapshots are required.");

        var snapshots = new List<ProgrammeReviewSnapshot>(request.Snapshots.Count);
        var sourceStreams = new Dictionary<string, FileStream>(StringComparer.Ordinal);
        var fullPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            foreach (ProgrammeReviewSnapshot snapshot in request.Snapshots)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string path = snapshot.XerFilePath?.Trim() ?? string.Empty;
                if (path.Length == 0)
                    throw new ProgrammeReviewValidationException(
                        $"xer_file_path is required for '{snapshot.OriginalXerFilename}' when building from files.");
                string fullPath = Path.GetFullPath(path);
                if (!File.Exists(fullPath))
                    throw new ProgrammeReviewValidationException($"XER file does not exist: '{fullPath}'.");
                if (!fullPaths.Add(fullPath))
                    throw new ProgrammeReviewValidationException($"The XER path '{fullPath}' was configured more than once.");
                if (!string.Equals(Path.GetFileName(fullPath), snapshot.OriginalXerFilename, StringComparison.Ordinal))
                    throw new ProgrammeReviewValidationException(
                        $"original_xer_filename '{snapshot.OriginalXerFilename}' must exactly match the path filename '{Path.GetFileName(fullPath)}'.");

                var sourceStream = new FileStream(fullPath, new FileStreamOptions
                {
                    Mode = FileMode.Open,
                    Access = FileAccess.Read,
                    Share = FileShare.Read,
                    BufferSize = PerformanceConfig.FileReadBufferSize,
                    Options = FileOptions.SequentialScan
                });
                sourceStreams.Add(snapshot.OriginalXerFilename, sourceStream);
                string computedHash = ComputeSourceSha256(sourceStream);
                if (!string.IsNullOrWhiteSpace(snapshot.SourceSha256)
                    && !string.Equals(snapshot.SourceSha256.Trim(), computedHash, StringComparison.OrdinalIgnoreCase))
                    throw new ProgrammeReviewValidationException(
                        $"Configured source_sha256 does not match '{snapshot.OriginalXerFilename}'.");
                snapshots.Add(snapshot with { XerFilePath = fullPath, SourceSha256 = computedHash });
            }

            ProgrammeReviewBundleRequest hydrated = request with { Snapshots = snapshots };
            ResolvedProgrammeReviewRequest resolved = ProgrammeReviewNaming.Resolve(
                hydrated,
                snapshot => snapshot.SourceSha256
                    ?? throw new ProgrammeReviewValidationException($"Source SHA-256 is missing for '{snapshot.OriginalXerFilename}'."));

            var parser = new ProcessingService();
            List<string> retainedPaths = resolved.Snapshots
                .Select(s => s.Source.XerFilePath
                    ?? throw new ProgrammeReviewValidationException($"XER path is missing for '{s.OriginalXerFilename}'."))
                .ToList();
            XerDataStore dataStore = await parser.ParseMultipleXerFilesAsync(retainedPaths, parseProgress, cancellationToken)
                .ConfigureAwait(false);

            foreach (ResolvedProgrammeReviewSnapshot retained in resolved.Snapshots)
            {
                cancellationToken.ThrowIfCancellationRequested();
                FileStream sourceStream = sourceStreams[retained.OriginalXerFilename];
                string afterParseHash = ComputeSourceSha256(sourceStream);
                if (!string.Equals(afterParseHash, retained.SourceSha256, StringComparison.Ordinal))
                    throw new ProgrammeReviewValidationException(
                        $"XER file '{retained.OriginalXerFilename}' changed while it was being parsed. No bundle was published.");
            }

            return BuildResolved(dataStore, resolved, outputRoot, cancellationToken);
        }
        finally
        {
            foreach (FileStream stream in sourceStreams.Values) stream.Dispose();
        }
    }

    /// <summary>
    /// Builds a bundle from an already parsed, merged XER data store. Each snapshot must supply its source SHA-256.
    /// This is the integration point for desktop/web callers that already parsed the XER inputs.
    /// </summary>
    public Task<ProgrammeReviewBundleResult> BuildFromParsedDataAsync(
        XerDataStore dataStore,
        ProgrammeReviewBundleRequest request,
        string outputRoot,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataStore);
        ResolvedProgrammeReviewRequest resolved = ProgrammeReviewNaming.Resolve(
            request,
            snapshot => snapshot.SourceSha256
                ?? throw new ProgrammeReviewValidationException(
                    $"source_sha256 is required for parsed snapshot '{snapshot.OriginalXerFilename}'."));
        return Task.FromResult(BuildResolved(dataStore, resolved, outputRoot, cancellationToken));
    }

    /// <summary>
    /// Browser-safe entrypoint that validates and hashes uploaded XER bytes, parses only the retained
    /// baseline/update history, and returns the complete eleven-file bundle without using the file system.
    /// </summary>
    public async Task<ProgrammeReviewInMemoryBundleResult> BuildFromXerBytesAsync(
        ProgrammeReviewBundleRequest request,
        IReadOnlyDictionary<string, byte[]> xerFiles,
        IProgress<ProcessingService.DetailedProgress>? parseProgress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(xerFiles);
        if (request.Snapshots is null)
            throw new ProgrammeReviewValidationException("Snapshots are required.");

        string[] configuredNames = request.Snapshots
            .Select(snapshot => snapshot.OriginalXerFilename)
            .ToArray();
        if (configuredNames.Distinct(StringComparer.OrdinalIgnoreCase).Count() != configuredNames.Length)
            throw new ProgrammeReviewValidationException("Configured XER filenames must be unique and case-exact.");
        string[] uploadedNames = xerFiles.Keys.ToArray();
        if (uploadedNames.Length != configuredNames.Length
            || configuredNames.Except(uploadedNames, StringComparer.Ordinal).Any()
            || uploadedNames.Except(configuredNames, StringComparer.Ordinal).Any())
            throw new ProgrammeReviewValidationException(
                "Uploaded XER files must exactly match the configured snapshot filenames.");

        var snapshots = new List<ProgrammeReviewSnapshot>(request.Snapshots.Count);
        var ownedXerFiles = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (ProgrammeReviewSnapshot snapshot in request.Snapshots)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!xerFiles.TryGetValue(snapshot.OriginalXerFilename, out byte[]? content) || content is null)
                throw new ProgrammeReviewValidationException(
                    $"Uploaded XER content is missing for '{snapshot.OriginalXerFilename}'.");

            byte[] ownedContent = content.ToArray();
            string computedHash = ProgrammeReviewCsv.ComputeSha256(ownedContent);
            if (!string.IsNullOrWhiteSpace(snapshot.SourceSha256)
                && !string.Equals(snapshot.SourceSha256.Trim(), computedHash, StringComparison.OrdinalIgnoreCase))
                throw new ProgrammeReviewValidationException(
                    $"Configured source_sha256 does not match '{snapshot.OriginalXerFilename}'.");
            ownedXerFiles.Add(snapshot.OriginalXerFilename, ownedContent);
            snapshots.Add(snapshot with { XerFilePath = null, SourceSha256 = computedHash });
            await Task.Delay(1, cancellationToken);
        }

        ProgrammeReviewBundleRequest hydrated = request with { Snapshots = snapshots };
        ResolvedProgrammeReviewRequest resolved = ProgrammeReviewNaming.Resolve(
            hydrated,
            snapshot => snapshot.SourceSha256
                ?? throw new ProgrammeReviewValidationException(
                    $"Source SHA-256 is missing for '{snapshot.OriginalXerFilename}'."));

        var ownedStreams = new List<MemoryStream>(resolved.Snapshots.Count);
        try
        {
            var inputs = new List<(Stream stream, string fileName)>(resolved.Snapshots.Count);
            foreach (ResolvedProgrammeReviewSnapshot snapshot in resolved.Snapshots)
            {
                var stream = new MemoryStream(ownedXerFiles[snapshot.OriginalXerFilename], writable: false);
                ownedStreams.Add(stream);
                inputs.Add((stream, snapshot.OriginalXerFilename));
            }

            var parser = new ProcessingService();
            XerDataStore dataStore = await parser.ParseXerStreamsAsync(inputs, parseProgress, cancellationToken)
                .ConfigureAwait(false);
            parseProgress?.Report(new ProcessingService.DetailedProgress
            {
                Percent = 100,
                Message = "Building and validating Programme Review tables..."
            });
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            return await BuildResolvedInMemoryAsync(dataStore, resolved, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            foreach (MemoryStream stream in ownedStreams) stream.Dispose();
        }
    }

    /// <summary>Creates an in-memory bundle from a caller-owned parsed data store.</summary>
    public Task<ProgrammeReviewInMemoryBundleResult> BuildFromParsedDataToMemoryAsync(
        XerDataStore dataStore,
        ProgrammeReviewBundleRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataStore);
        ResolvedProgrammeReviewRequest resolved = ProgrammeReviewNaming.Resolve(
            request,
            snapshot => snapshot.SourceSha256
                ?? throw new ProgrammeReviewValidationException(
                    $"source_sha256 is required for parsed snapshot '{snapshot.OriginalXerFilename}'."));
        return BuildResolvedInMemoryAsync(dataStore, resolved, cancellationToken);
    }

    private static ProgrammeReviewBundleResult BuildResolved(
        XerDataStore dataStore,
        ResolvedProgrammeReviewRequest request,
        string outputRoot,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(outputRoot))
            throw new ProgrammeReviewValidationException("An output root directory is required.");

        string root = Path.GetFullPath(outputRoot);
        Directory.CreateDirectory(root);
        string finalPath = Path.Combine(root, request.BundleId);
        if (Directory.Exists(finalPath) || File.Exists(finalPath))
            throw new ProgrammeReviewValidationException(
                $"Bundle target already exists: '{finalPath}'. Choose a new exported_at_utc value or remove the prior target explicitly.");

        string stagingName = $".{request.BundleId}.{Guid.NewGuid():N}.staging";
        string stagingPath = Path.Combine(root, stagingName);
        Directory.CreateDirectory(stagingPath);

        try
        {
            var transformer = new ProgrammeReviewTransformer(dataStore, request);
            IReadOnlyList<ProgrammeReviewOutputTable> tables = transformer.Build(cancellationToken);
            var hashes = new Dictionary<string, string>(StringComparer.Ordinal);

            // The manifest must be absent until all ten table files are fully written and hashed.
            foreach (ProgrammeReviewOutputTable table in tables)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string csvPath = Path.Combine(stagingPath, table.Contract.FileName);
                hashes[table.Contract.FileName] = ProgrammeReviewCsv.WriteTable(csvPath, table, cancellationToken);
            }

            IReadOnlyList<ProgrammeReviewManifestRow> manifestRows =
                CreateManifestRows(request, tables, hashes);

            string manifestPath = Path.Combine(stagingPath, ProgrammeReviewContract.ManifestFileName);
            ProgrammeReviewCsv.WriteManifest(manifestPath, manifestRows, cancellationToken);

            string[] expectedFiles = ProgrammeReviewContract.Tables.Select(t => t.FileName)
                .Append(ProgrammeReviewContract.ManifestFileName)
                .Order(StringComparer.Ordinal)
                .ToArray();
            string[] actualFiles = Directory.EnumerateFiles(stagingPath).Select(Path.GetFileName)
                .Where(n => n is not null).Cast<string>().Order(StringComparer.Ordinal).ToArray();
            if (!expectedFiles.SequenceEqual(actualFiles, StringComparer.Ordinal))
                throw new ProgrammeReviewValidationException(
                    $"Staged bundle file set is invalid. Expected [{string.Join(", ", expectedFiles)}], got [{string.Join(", ", actualFiles)}].");

            cancellationToken.ThrowIfCancellationRequested();
            Directory.Move(stagingPath, finalPath);
            return new ProgrammeReviewBundleResult(
                request.BundleId,
                finalPath,
                new ReadOnlyCollection<ProgrammeReviewManifestRow>(manifestRows.ToList()),
                new ReadOnlyDictionary<string, string>(hashes));
        }
        catch (Exception originalError)
        {
            try
            {
                DeleteOwnedStagingDirectory(root, stagingPath);
            }
            catch (Exception cleanupError)
            {
                throw new AggregateException(
                    $"Bundle generation failed and staging cleanup also failed at '{stagingPath}'.",
                    originalError,
                    cleanupError);
            }
            throw;
        }
    }

    private static async Task<ProgrammeReviewInMemoryBundleResult> BuildResolvedInMemoryAsync(
        XerDataStore dataStore,
        ResolvedProgrammeReviewRequest request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        var transformer = new ProgrammeReviewTransformer(dataStore, request);
        IReadOnlyList<ProgrammeReviewOutputTable> tables = transformer.Build(cancellationToken);
        await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var hashes = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (ProgrammeReviewOutputTable table in tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] content = ProgrammeReviewCsv.WriteTableToBytes(table, cancellationToken);
            files.Add(table.Contract.FileName, content);
            hashes.Add(table.Contract.FileName, ProgrammeReviewCsv.ComputeSha256(content));
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        }

        IReadOnlyList<ProgrammeReviewManifestRow> manifestRows =
            CreateManifestRows(request, tables, hashes);
        files.Add(
            ProgrammeReviewContract.ManifestFileName,
            ProgrammeReviewCsv.WriteManifestToBytes(manifestRows, cancellationToken));
        await Task.Delay(1, cancellationToken).ConfigureAwait(false);

        string[] expectedFiles = ProgrammeReviewContract.Tables.Select(t => t.FileName)
            .Append(ProgrammeReviewContract.ManifestFileName)
            .Order(StringComparer.Ordinal)
            .ToArray();
        string[] actualFiles = files.Keys.Order(StringComparer.Ordinal).ToArray();
        if (!expectedFiles.SequenceEqual(actualFiles, StringComparer.Ordinal))
            throw new ProgrammeReviewValidationException(
                $"In-memory bundle file set is invalid. Expected [{string.Join(", ", expectedFiles)}], got [{string.Join(", ", actualFiles)}].");

        return new ProgrammeReviewInMemoryBundleResult(
            request.BundleId,
            new ReadOnlyDictionary<string, byte[]>(files),
            new ReadOnlyCollection<ProgrammeReviewManifestRow>(manifestRows.ToList()),
            new ReadOnlyDictionary<string, string>(hashes));
    }

    private static IReadOnlyList<ProgrammeReviewManifestRow> CreateManifestRows(
        ResolvedProgrammeReviewRequest request,
        IReadOnlyList<ProgrammeReviewOutputTable> tables,
        IReadOnlyDictionary<string, string> hashes)
    {
        var manifestRows = new List<ProgrammeReviewManifestRow>(request.Snapshots.Count * tables.Count);
        foreach (ResolvedProgrammeReviewSnapshot snapshot in request.Snapshots
            .OrderBy(s => s.CanonicalXerFilename, StringComparer.Ordinal))
        {
            foreach (ProgrammeReviewOutputTable table in tables)
            {
                long sourceRows = table.Rows.LongCount(r => ReferenceEquals(r.Snapshot, snapshot));
                manifestRows.Add(new ProgrammeReviewManifestRow(
                    ProgrammeReviewContract.SchemaVersion,
                    request.BundleId,
                    ProgrammeReviewContract.CompleteStatus,
                    request.ParserVersion,
                    request.ProjectCode,
                    request.ProjectName,
                    request.ProgrammeType,
                    snapshot.OriginalXerFilename,
                    snapshot.CanonicalXerFilename,
                    snapshot.SnapshotKind.ToString(),
                    snapshot.SnapshotTag,
                    snapshot.MonthUpdate,
                    snapshot.EffectiveUpdateDate,
                    snapshot.DataDate,
                    snapshot.SourceSha256,
                    table.Contract.TableName,
                    sourceRows,
                    hashes[table.Contract.FileName],
                    request.ExportedAtUtc));
            }
        }
        return manifestRows;
    }

    private static string ComputeSourceSha256(Stream stream)
    {
        if (!stream.CanSeek)
            throw new ProgrammeReviewValidationException("A seekable XER source stream is required for hash verification.");
        stream.Position = 0;
        string hash = Convert.ToHexString(SHA256.HashData(stream)).ToLowerInvariant();
        stream.Position = 0;
        return hash;
    }

    private static void DeleteOwnedStagingDirectory(string root, string stagingPath)
    {
        string resolvedRoot = Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        string resolvedStaging = Path.GetFullPath(stagingPath);
        if (!string.Equals(Path.GetDirectoryName(resolvedStaging), resolvedRoot, StringComparison.OrdinalIgnoreCase)
            || !Path.GetFileName(resolvedStaging).StartsWith(".", StringComparison.Ordinal)
            || !Path.GetFileName(resolvedStaging).EndsWith(".staging", StringComparison.Ordinal))
            throw new InvalidOperationException($"Refusing to delete an unowned staging path '{resolvedStaging}'.");
        if (Directory.Exists(resolvedStaging)) Directory.Delete(resolvedStaging, recursive: true);
    }
}
