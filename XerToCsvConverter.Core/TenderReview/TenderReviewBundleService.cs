using System.Collections.ObjectModel;
using System.Security.Cryptography;

namespace XerToCsvConverter.TenderReview;

/// <summary>Builds an independent, deterministic and atomic Tender Review CSV bundle.</summary>
public sealed class TenderReviewBundleService
{
    public async Task<TenderReviewBundleResult> BuildFromXerFilesAsync(
        TenderReviewBundleRequest request,
        string outputRoot,
        IProgress<ProcessingService.DetailedProgress>? parseProgress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Sources is null)
            throw new TenderReviewValidationException("Tender sources are required.");

        var opened = new List<OpenedTenderSource>(request.Sources.Count);
        try
        {
            var hydratedSources = new List<TenderReviewSource>(request.Sources.Count);
            foreach (TenderReviewSource source in request.Sources)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string path = source.XerFilePath?.Trim() ?? string.Empty;
                if (path.Length == 0)
                    throw new TenderReviewValidationException(
                        $"xer_file_path is required for Tender source token '{source.SourceToken}'.");
                string fullPath = Path.GetFullPath(path);
                if (!File.Exists(fullPath))
                    throw new TenderReviewValidationException($"XER file does not exist: '{fullPath}'.");
                string pathFilename = Path.GetFileName(fullPath);
                if (!string.Equals(pathFilename, source.OriginalXerFilename, StringComparison.Ordinal))
                    throw new TenderReviewValidationException(
                        $"original_xer_filename '{source.OriginalXerFilename}' must exactly match the path filename '{pathFilename}'.");

                var stream = new FileStream(fullPath, new FileStreamOptions
                {
                    Mode = FileMode.Open,
                    Access = FileAccess.Read,
                    Share = FileShare.Read,
                    BufferSize = PerformanceConfig.FileReadBufferSize,
                    Options = FileOptions.SequentialScan
                });
                // Register ownership before hashing/validation so every exceptional path closes the handle.
                opened.Add(new OpenedTenderSource(source, stream));
                string computedHash = ComputeSourceSha256(stream);
                ValidateConfiguredHash(source, computedHash);
                TenderReviewSource hydrated = source with
                {
                    XerFilePath = fullPath,
                    SourceSha256 = computedHash
                };
                hydratedSources.Add(hydrated);
            }

            TenderReviewBundleRequest hydratedRequest = request with { Sources = hydratedSources };
            ResolvedTenderReviewRequest resolved = ResolveWithConfiguredHashes(hydratedRequest);
            var parseInputs = new List<TenderParseInput>(resolved.Sources.Count);
            foreach (ResolvedTenderReviewSource source in resolved.Sources)
            {
                OpenedTenderSource item = opened.Single(candidate =>
                    string.Equals(candidate.Source.SourceToken, source.SourceToken, StringComparison.Ordinal));
                item.Stream.Position = 0;
                parseInputs.Add(new TenderParseInput(
                    item.Stream, source.SourceToken, source.OriginalXerFilename));
            }

            XerDataStore dataStore = await ParseTenderInputsAsync(
                    parseInputs, parseProgress, cancellationToken)
                .ConfigureAwait(false);
            foreach (ResolvedTenderReviewSource source in resolved.Sources)
            {
                cancellationToken.ThrowIfCancellationRequested();
                OpenedTenderSource item = opened.Single(candidate =>
                    string.Equals(candidate.Source.SourceToken, source.SourceToken, StringComparison.Ordinal));
                string afterParseHash = ComputeSourceSha256(item.Stream);
                if (!string.Equals(afterParseHash, source.SourceSha256, StringComparison.Ordinal))
                    throw new TenderReviewValidationException(
                        $"Tender XER input {source.InputIndex + 1} ('{source.OriginalXerFilename}') changed while it was being parsed. No bundle was published.");
            }

            parseProgress?.Report(new ProcessingService.DetailedProgress
            {
                Percent = 100,
                Message = "Building and validating Tender Review tables..."
            });
            return BuildResolved(dataStore, resolved, outputRoot, cancellationToken);
        }
        finally
        {
            foreach (OpenedTenderSource item in opened) item.Stream.Dispose();
        }
    }

    public async Task<TenderReviewInMemoryBundleResult> BuildFromXerBytesAsync(
        TenderReviewBundleRequest request,
        IReadOnlyList<TenderReviewSourceBytes> xerFiles,
        IProgress<ProcessingService.DetailedProgress>? parseProgress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(xerFiles);
        if (request.Sources is null)
            throw new TenderReviewValidationException("Tender sources are required.");
        if (xerFiles.Count != request.Sources.Count)
            throw new TenderReviewValidationException(
                "Ordered Tender XER content must contain exactly one item for every configured source.");

        var hydratedSources = new List<TenderReviewSource>(request.Sources.Count);
        for (int index = 0; index < request.Sources.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            TenderReviewSource source = request.Sources[index];
            TenderReviewSourceBytes upload = xerFiles[index]
                ?? throw new TenderReviewValidationException(
                    $"Tender XER content at input position {index + 1} is null.");
            if (!string.Equals(source.SourceToken, upload.SourceToken, StringComparison.Ordinal))
                throw new TenderReviewValidationException(
                    $"Tender XER content at input position {index + 1} has source token '{upload.SourceToken}', expected '{source.SourceToken}'. Ordered token association must be preserved.");
            if (upload.Content is null)
                throw new TenderReviewValidationException(
                    $"Tender XER content is missing for source token '{source.SourceToken}'.");

            string computedHash = TenderReviewCsv.ComputeSha256(upload.Content);
            ValidateConfiguredHash(source, computedHash);
            hydratedSources.Add(source with { XerFilePath = null, SourceSha256 = computedHash });
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        }

        TenderReviewBundleRequest hydratedRequest = request with { Sources = hydratedSources };
        ResolvedTenderReviewRequest resolved = ResolveWithConfiguredHashes(hydratedRequest);
        var streams = new List<MemoryStream>(resolved.Sources.Count);
        try
        {
            var parseInputs = new List<TenderParseInput>(resolved.Sources.Count);
            for (int index = 0; index < resolved.Sources.Count; index++)
            {
                var stream = new MemoryStream(xerFiles[index].Content, writable: false);
                streams.Add(stream);
                parseInputs.Add(new TenderParseInput(
                    stream,
                    resolved.Sources[index].SourceToken,
                    resolved.Sources[index].OriginalXerFilename));
            }

            XerDataStore dataStore = await ParseTenderInputsAsync(
                    parseInputs, parseProgress, cancellationToken)
                .ConfigureAwait(false);
            for (int index = 0; index < resolved.Sources.Count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string afterParseHash = TenderReviewCsv.ComputeSha256(xerFiles[index].Content);
                if (!string.Equals(afterParseHash, resolved.Sources[index].SourceSha256, StringComparison.Ordinal))
                    throw new TenderReviewValidationException(
                        $"Tender XER input {index + 1} ('{resolved.Sources[index].OriginalXerFilename}') changed while it was being parsed. No bundle was created.");
            }

            parseProgress?.Report(new ProcessingService.DetailedProgress
            {
                Percent = 100,
                Message = "Building and validating Tender Review tables..."
            });
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            return await BuildResolvedInMemoryAsync(dataStore, resolved, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            foreach (MemoryStream stream in streams) stream.Dispose();
        }
    }

    /// <summary>
    /// Builds from data already parsed with each DataRow.SourceFilename set to the matching Tender source token.
    /// </summary>
    internal Task<TenderReviewBundleResult> BuildFromParsedDataAsync(
        XerDataStore dataStore,
        TenderReviewBundleRequest request,
        string outputRoot,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataStore);
        return Task.FromResult(BuildResolved(
            dataStore,
            ResolveWithConfiguredHashes(request),
            outputRoot,
            cancellationToken));
    }

    /// <summary>
    /// Builds in memory from data already parsed with each DataRow.SourceFilename set to the matching Tender source token.
    /// </summary>
    internal Task<TenderReviewInMemoryBundleResult> BuildFromParsedDataToMemoryAsync(
        XerDataStore dataStore,
        TenderReviewBundleRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataStore);
        return BuildResolvedInMemoryAsync(
            dataStore,
            ResolveWithConfiguredHashes(request),
            cancellationToken);
    }

    private static TenderReviewBundleResult BuildResolved(
        XerDataStore dataStore,
        ResolvedTenderReviewRequest preparedRequest,
        string outputRoot,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        dataStore = BindGovernedSourceTokens(dataStore, preparedRequest, cancellationToken);
        if (string.IsNullOrWhiteSpace(outputRoot))
            throw new TenderReviewValidationException("An output root directory is required.");

        string root = Path.GetFullPath(outputRoot);
        Directory.CreateDirectory(root);
        string finalPath = Path.Combine(root, preparedRequest.BundleId);
        if (Directory.Exists(finalPath) || File.Exists(finalPath))
            throw new TenderReviewValidationException(
                $"Bundle target already exists: '{finalPath}'. Choose a new exported_at_utc value or remove the prior target explicitly.");

        string stagingName = $".{preparedRequest.BundleId}.{Guid.NewGuid():N}.staging";
        string stagingPath = Path.Combine(root, stagingName);
        Directory.CreateDirectory(stagingPath);

        try
        {
            TenderReviewTransformationResult transformed =
                new TenderReviewTransformer(dataStore, preparedRequest).Build(cancellationToken);
            var hashes = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (TenderReviewOutputTable table in transformed.Tables)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string csvPath = Path.Combine(stagingPath, table.Contract.FileName);
                hashes.Add(table.Contract.FileName,
                    TenderReviewCsv.WriteTable(csvPath, table, cancellationToken));
            }
            byte[] dataQualityBytes = XerDataQuality.WriteToBytes(transformed.DataQualityTable, cancellationToken);
            File.WriteAllBytes(Path.Combine(stagingPath, XerDataQuality.FileName), dataQualityBytes);
            hashes.Add(XerDataQuality.FileName, TenderReviewCsv.ComputeSha256(dataQualityBytes));

            IReadOnlyList<TenderReviewManifestRow> manifestRows =
                CreateManifestRows(transformed.Request, transformed.Tables, transformed.DataQualityTable, hashes);
            ValidateManifestConsistency(transformed.Request, transformed.Tables, transformed.DataQualityTable, hashes, manifestRows);

            string manifestPath = Path.Combine(stagingPath, TenderReviewContract.ManifestFileName);
            TenderReviewCsv.WriteManifest(manifestPath, manifestRows, cancellationToken);
            ValidateFileEnvelope(stagingPath);
            foreach ((string fileName, string expectedHash) in hashes)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string actualHash = TenderReviewCsv.ComputeSha256(Path.Combine(stagingPath, fileName));
                if (!string.Equals(actualHash, expectedHash, StringComparison.Ordinal))
                    throw new TenderReviewValidationException(
                        $"Staged Tender CSV hash validation failed for '{fileName}'.");
            }

            cancellationToken.ThrowIfCancellationRequested();
            Directory.Move(stagingPath, finalPath);
            return new TenderReviewBundleResult(
                transformed.Request.BundleId,
                finalPath,
                new ReadOnlyCollection<TenderReviewManifestRow>(manifestRows.ToList()),
                new ReadOnlyDictionary<string, string>(hashes))
            {
                WarningCount = transformed.DataQualityTable.RowCount
            };
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
                    $"Tender bundle generation failed and staging cleanup also failed at '{stagingPath}'.",
                    originalError,
                    cleanupError);
            }
            throw;
        }
    }

    private static async Task<TenderReviewInMemoryBundleResult> BuildResolvedInMemoryAsync(
        XerDataStore dataStore,
        ResolvedTenderReviewRequest preparedRequest,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        dataStore = BindGovernedSourceTokens(dataStore, preparedRequest, cancellationToken);
        await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        TenderReviewTransformationResult transformed =
            new TenderReviewTransformer(dataStore, preparedRequest).Build(cancellationToken);
        var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var hashes = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (TenderReviewOutputTable table in transformed.Tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] content = TenderReviewCsv.WriteTableToBytes(table, cancellationToken);
            files.Add(table.Contract.FileName, content);
            hashes.Add(table.Contract.FileName, TenderReviewCsv.ComputeSha256(content));
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        }
        byte[] dataQualityBytes = XerDataQuality.WriteToBytes(transformed.DataQualityTable, cancellationToken);
        files.Add(XerDataQuality.FileName, dataQualityBytes);
        hashes.Add(XerDataQuality.FileName, TenderReviewCsv.ComputeSha256(dataQualityBytes));

        IReadOnlyList<TenderReviewManifestRow> manifestRows =
            CreateManifestRows(transformed.Request, transformed.Tables, transformed.DataQualityTable, hashes);
        ValidateManifestConsistency(transformed.Request, transformed.Tables, transformed.DataQualityTable, hashes, manifestRows);
        files.Add(
            TenderReviewContract.ManifestFileName,
            TenderReviewCsv.WriteManifestToBytes(manifestRows, cancellationToken));
        ValidateFileEnvelope(files.Keys);
        foreach ((string fileName, string expectedHash) in hashes)
        {
            if (!string.Equals(
                TenderReviewCsv.ComputeSha256(files[fileName]), expectedHash, StringComparison.Ordinal))
                throw new TenderReviewValidationException(
                    $"In-memory Tender CSV hash validation failed for '{fileName}'.");
        }

        return new TenderReviewInMemoryBundleResult(
            transformed.Request.BundleId,
            new ReadOnlyDictionary<string, byte[]>(files),
            new ReadOnlyCollection<TenderReviewManifestRow>(manifestRows.ToList()),
            new ReadOnlyDictionary<string, string>(hashes))
        {
            WarningCount = transformed.DataQualityTable.RowCount
        };
    }

    private static IReadOnlyList<TenderReviewManifestRow> CreateManifestRows(
        ResolvedTenderReviewRequest request,
        IReadOnlyList<TenderReviewOutputTable> tables,
        XerTable dataQuality,
        IReadOnlyDictionary<string, string> hashes)
    {
        var rows = new List<TenderReviewManifestRow>(request.Sources.Count * (tables.Count + 1));
        foreach (ResolvedTenderReviewSource source in request.Sources.OrderBy(item => item.InputIndex))
        {
            DateOnly dataDate = source.DataDate ?? throw new TenderReviewValidationException(
                $"{source.OriginalXerFilename}: P6 data_date was not resolved.");
            var artifacts = tables.Select(table => (
                table.Contract.TableName,
                table.Contract.FileName,
                RowCount: table.Rows.LongCount(row => ReferenceEquals(row.Source, source))))
                .Append((TableName: XerDataQuality.TableName, FileName: XerDataQuality.FileName,
                    RowCount: dataQuality.Rows.LongCount(row => string.Equals(
                        row.SourceToken, source.SourceToken, StringComparison.Ordinal))));
            foreach (var artifact in artifacts)
            {
                rows.Add(new TenderReviewManifestRow(
                    TenderReviewContract.SchemaVersion,
                    TenderReviewContract.BundleProfile,
                    request.BundleId,
                    TenderReviewContract.CompleteStatus,
                    request.ParserVersion,
                    request.ProjectCode,
                    request.ProjectName,
                    source.OriginalXerFilename,
                    source.CanonicalXerFilename,
                    source.StatusDate,
                    source.StatusDate,
                    dataDate,
                    source.SourceSha256,
                    artifact.TableName,
                    artifact.RowCount,
                    hashes[artifact.FileName],
                    request.ExportedAtUtc));
            }
        }
        return rows;
    }

    private static void ValidateManifestConsistency(
        ResolvedTenderReviewRequest request,
        IReadOnlyList<TenderReviewOutputTable> tables,
        XerTable dataQuality,
        IReadOnlyDictionary<string, string> hashes,
        IReadOnlyList<TenderReviewManifestRow> manifestRows)
    {
        if (manifestRows.Count != request.Sources.Count * (tables.Count + 1))
            throw new TenderReviewValidationException("Tender manifest row envelope is invalid.");

        foreach (TenderReviewOutputTable table in tables)
        {
            TenderReviewManifestRow[] tableRows = manifestRows.Where(row =>
                string.Equals(row.TableName, table.Contract.TableName, StringComparison.Ordinal)).ToArray();
            if (tableRows.LongCount() != request.Sources.Count
                || tableRows.Sum(row => row.RowCount) != table.Rows.LongCount())
                throw new TenderReviewValidationException(
                    $"Tender manifest total/per-stage row counts do not reconcile for '{table.Contract.TableName}'.");
            if (tableRows.Any(row =>
                !string.Equals(row.CsvSha256, hashes[table.Contract.FileName], StringComparison.Ordinal)))
                throw new TenderReviewValidationException(
                    $"Tender manifest CSV hashes are inconsistent for '{table.Contract.TableName}'.");
        }

        TenderReviewManifestRow[] warningRows = manifestRows.Where(row =>
            string.Equals(row.TableName, XerDataQuality.TableName, StringComparison.Ordinal)).ToArray();
        if (warningRows.Length != request.Sources.Count
            || warningRows.Sum(row => row.RowCount) != dataQuality.RowCount
            || warningRows.Any(row => !string.Equals(
                row.CsvSha256, hashes[XerDataQuality.FileName], StringComparison.Ordinal)))
            throw new TenderReviewValidationException("Tender data-quality manifest counts/hashes do not reconcile.");

        foreach (ResolvedTenderReviewSource source in request.Sources)
        {
            TenderReviewManifestRow[] sourceRows = manifestRows.Where(row =>
                string.Equals(row.CanonicalXerFilename, source.CanonicalXerFilename, StringComparison.Ordinal)).ToArray();
            if (sourceRows.Length != tables.Count + 1
                || sourceRows.Any(row => !string.Equals(
                    row.SourceSha256, source.SourceSha256, StringComparison.Ordinal)))
                throw new TenderReviewValidationException(
                    $"Tender manifest source hash/count validation failed for input {source.InputIndex + 1}.");
        }
    }

    private static void ValidateFileEnvelope(string directory) =>
        ValidateFileEnvelope(Directory.EnumerateFiles(directory)
            .Select(Path.GetFileName).Where(name => name is not null).Cast<string>());

    private static void ValidateFileEnvelope(IEnumerable<string> actualFileNames)
    {
        string[] expected = TenderReviewContract.Tables.Select(table => table.FileName)
            .Append(XerDataQuality.FileName)
            .Append(TenderReviewContract.ManifestFileName)
            .Order(StringComparer.Ordinal).ToArray();
        string[] actual = actualFileNames.Order(StringComparer.Ordinal).ToArray();
        if (!expected.SequenceEqual(actual, StringComparer.Ordinal))
            throw new TenderReviewValidationException(
                $"Tender bundle file set is invalid. Expected [{string.Join(", ", expected)}], got [{string.Join(", ", actual)}].");
    }

    private static ResolvedTenderReviewRequest ResolveWithConfiguredHashes(
        TenderReviewBundleRequest request) => TenderReviewNaming.Resolve(
        request,
        source => source.SourceSha256
            ?? throw new TenderReviewValidationException(
                $"source_sha256 is required for parsed Tender source token '{source.SourceToken}'."));

    private static async Task<XerDataStore> ParseTenderInputsAsync(
        IReadOnlyList<TenderParseInput> inputs,
        IProgress<ProcessingService.DetailedProgress>? progress,
        CancellationToken cancellationToken)
    {
        StringInternPool.Clear();
        DateParser.ClearCache();
        var parser = new XerParser();
        var merged = new XerDataStore();

        for (int index = 0; index < inputs.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            TenderParseInput input = inputs[index];
            input.Stream.Position = 0;
            int sourceIndex = index;
            progress?.Report(new ProcessingService.DetailedProgress
            {
                Percent = sourceIndex * 100 / inputs.Count,
                Message = $"Parsing Tender source: {input.DisplayName}",
                FilePath = input.SourceToken,
                FileStatus = "Processing",
                StatusColor = "Blue"
            });
            Action<int, string> parserProgress = (percent, message) =>
            {
                int overall = (sourceIndex * 100 + Math.Clamp(percent, 0, 100)) / inputs.Count;
                progress?.Report(new ProcessingService.DetailedProgress
                {
                    Percent = overall,
                    Message = message,
                    FilePath = input.SourceToken,
                    FileStatus = "Processing",
                    StatusColor = "Blue"
                });
            };

            XerDataStore single = await parser.ParseXerStreamAsync(
                    input.Stream,
                    input.SourceToken,
                    parserProgress,
                    cancellationToken)
                .ConfigureAwait(false);
            // The generic parser mints an occurrence token for each invocation.
            // Tender already governs that identity through its ordered source
            // request; keep every raw table on the same requested token while
            // retaining the actual display filename as separate provenance.
            single = new XerSourceIdentity(input.SourceToken, input.DisplayName, input.SourceToken)
                .ApplyTo(single);
            MergeTenderStoreWithUnionSchema(merged, single);
            progress?.Report(new ProcessingService.DetailedProgress
            {
                Percent = (sourceIndex + 1) * 100 / inputs.Count,
                Message = $"Completed Tender source: {input.DisplayName}",
                FilePath = input.SourceToken,
                FileStatus = "Success",
                StatusColor = "DarkGreen"
            });
        }

        return merged;
    }

    private static XerDataStore BindGovernedSourceTokens(XerDataStore store,
        ResolvedTenderReviewRequest request, CancellationToken cancellationToken)
    {
        var sources = request.Sources.ToDictionary(source => source.SourceToken, StringComparer.Ordinal);
        var parsedTokens = new Dictionary<string, string>(StringComparer.Ordinal);
        bool needsBinding = false;
        foreach (string name in store.TableNames)
        {
            foreach (DataRow row in store.GetTable(name)!.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!sources.TryGetValue(row.SourceFilename, out ResolvedTenderReviewSource? source))
                    throw new TenderReviewValidationException(
                        $"Raw {name} contains an unconfigured Tender source token '{row.SourceFilename}'.");
                if (parsedTokens.TryGetValue(source.SourceToken, out string? existing)
                    && !string.Equals(existing, row.SourceToken, StringComparison.Ordinal))
                    throw new TenderReviewValidationException(
                        $"Tender source token '{source.SourceToken}' contains multiple parser occurrences. Keep inputs as separate ordered Tender sources.");
                parsedTokens[source.SourceToken] = row.SourceToken;
                needsBinding |= row.SourceToken != source.SourceToken
                    || row.OriginalSourceFilename != source.OriginalXerFilename;
            }
        }
        if (!needsBinding) return store;

        // Parsed-data callers still associate rows using their governed
        // SourceFilename token. Rebind generic-parser occurrence metadata without
        // changing any caller-owned row or source field array.
        var bound = new XerDataStore();
        foreach (string name in store.TableNames)
        {
            XerTable original = store.GetTable(name)!;
            var table = new XerTable(name, original.RowCount);
            if (original.Headers is not null) table.SetHeaders(original.Headers.ToArray());
            foreach (DataRow row in original.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ResolvedTenderReviewSource source = sources[row.SourceFilename];
                table.AddRow(row with
                {
                    SourceToken = source.SourceToken,
                    OriginalSourceFilename = source.OriginalXerFilename
                });
            }
            bound.AddTable(table);
        }
        return bound;
    }

    private static void MergeTenderStoreWithUnionSchema(
        XerDataStore target,
        XerDataStore incoming)
    {
        foreach (string tableName in incoming.TableNames)
        {
            XerTable sourceTable = incoming.GetTable(tableName)
                ?? throw new TenderReviewValidationException(
                    $"Parsed Tender table '{tableName}' disappeared during merge.");
            XerTable? targetTable = target.GetTable(tableName);
            if (targetTable is null)
            {
                target.AddTable(sourceTable);
                continue;
            }

            string[] existingHeaders = targetTable.Headers ?? Array.Empty<string>();
            string[] incomingHeaders = sourceTable.Headers ?? Array.Empty<string>();
            var unionHeaders = new List<string>(existingHeaders);
            var knownHeaders = existingHeaders.ToHashSet(StringComparer.OrdinalIgnoreCase);
            foreach (string header in incomingHeaders)
            {
                if (knownHeaders.Add(header)) unionHeaders.Add(header);
            }

            var union = new XerTable(
                tableName,
                targetTable.RowCount + sourceTable.RowCount);
            union.SetHeaders(unionHeaders.ToArray());
            AddRowsWithUnionSchema(union, targetTable);
            AddRowsWithUnionSchema(union, sourceTable);
            target.AddTable(union);
        }
    }

    private static void AddRowsWithUnionSchema(XerTable destination, XerTable source)
    {
        if (destination.Headers is null || source.Headers is null) return;
        foreach (DataRow row in source.Rows)
        {
            var values = new string[destination.Headers.Length];
            for (int index = 0; index < destination.Headers.Length; index++)
            {
                string header = destination.Headers[index];
                values[index] = source.FieldIndexes.TryGetValue(header, out int sourceIndex)
                    ? XerTable.GetFieldValueSafe(row, sourceIndex)
                    : string.Empty;
            }
            destination.AddRow(row.WithFields(values));
        }
    }

    private static void ValidateConfiguredHash(TenderReviewSource source, string computedHash)
    {
        if (!string.IsNullOrWhiteSpace(source.SourceSha256)
            && !string.Equals(source.SourceSha256.Trim(), computedHash, StringComparison.OrdinalIgnoreCase))
            throw new TenderReviewValidationException(
                $"Configured source_sha256 does not match Tender source token '{source.SourceToken}' ('{source.OriginalXerFilename}').");
    }

    private static string ComputeSourceSha256(Stream stream)
    {
        if (!stream.CanSeek)
            throw new TenderReviewValidationException(
                "A seekable Tender XER source stream is required for hash verification.");
        stream.Position = 0;
        string hash = Convert.ToHexString(SHA256.HashData(stream)).ToLowerInvariant();
        stream.Position = 0;
        return hash;
    }

    private static void DeleteOwnedStagingDirectory(string root, string stagingPath)
    {
        string resolvedRoot = Path.GetFullPath(root)
            .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        string resolvedStaging = Path.GetFullPath(stagingPath);
        if (!string.Equals(Path.GetDirectoryName(resolvedStaging), resolvedRoot,
                StringComparison.OrdinalIgnoreCase)
            || !Path.GetFileName(resolvedStaging).StartsWith(".", StringComparison.Ordinal)
            || !Path.GetFileName(resolvedStaging).EndsWith(".staging", StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Refusing to delete an unowned Tender staging path '{resolvedStaging}'.");
        if (Directory.Exists(resolvedStaging)) Directory.Delete(resolvedStaging, recursive: true);
    }

    private sealed record OpenedTenderSource(TenderReviewSource Source, FileStream Stream);
    private sealed record TenderParseInput(Stream Stream, string SourceToken, string DisplayName);
}
