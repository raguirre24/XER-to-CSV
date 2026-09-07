using System.Collections.Concurrent;
using System.Globalization;

namespace XerToCsvConverter.ProgrammeReview;

internal sealed class ProgrammeReviewTransformer
{
    private static readonly HashSet<string> NamespacedKeyColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "proj_id_key", "wbs_id_key", "parent_wbs_id_key", "calendar_id_key", "clndr_id_key",
        "task_id_key", "pred_task_id_key", "task_pred_id_key", "actv_code_type_id_key",
        "actv_code_id_key", "rsrc_id_key"
    };

    private static readonly HashSet<string> TaskHistoryColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "Finish_Variance_Previous_Month", "Driven_DataDate", "Variance_Finish_BL",
        "Variance_Finish_Adjusted_BL", "Baseline Finish", "Baseline Start", "Previous Month Start",
        "Previous Month Finish", "Planned Not Completed Last Period", "Start_Variance_Previous_Month",
        "PreviousDataDate", "Previous Remaining Working Days", "Planned Last Period",
        "Completed Last Period", "Completed of Planned Last Period", "Baseline Effective_Early_End",
        "Baseline Effective_Late_End", "Adjusted Baseline Finish", "Adjusted Baseline Start",
        "Adjusted Baseline Source Month", "Adjusted Baseline Source"
    };

    private readonly XerDataStore _dataStore;
    private readonly ResolvedProgrammeReviewRequest _request;
    private readonly IReadOnlyDictionary<string, ResolvedProgrammeReviewSnapshot> _snapshotByOriginal;
    private readonly Dictionary<string, string> _projectNativeIdBySource = new(StringComparer.OrdinalIgnoreCase);
    private readonly ReviewDataQualityCollector _quality;
    private readonly Dictionary<IReadOnlyDictionary<string, string>, ReviewRowEvidence> _outputEvidence = new(ReferenceEqualityComparer.Instance);

    internal XerTable DataQualityTable { get; private set; } = null!;

    internal ProgrammeReviewTransformer(XerDataStore dataStore, ResolvedProgrammeReviewRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataStore);
        _request = request ?? throw new ArgumentNullException(nameof(request));
        _snapshotByOriginal = request.Snapshots.ToDictionary(s => s.OriginalXerFilename, StringComparer.OrdinalIgnoreCase);
        _dataStore = CreateRetainedDataStore(dataStore, cancellationToken);
        _quality = new ReviewDataQualityCollector(_dataStore);
    }

    internal IReadOnlyList<ProgrammeReviewOutputTable> Build(CancellationToken cancellationToken)
    {
        ValidateSingleProjectPerSnapshot();
        var transformer = new XerTransformer(_dataStore);
        var enhanced = new Dictionary<string, XerTable?>(StringComparer.OrdinalIgnoreCase)
        {
            [EnhancedTableNames.XerTask01] = transformer.Create01XerTaskTable(),
            [EnhancedTableNames.XerProject02] = transformer.Create02XerProject(),
            [EnhancedTableNames.XerProjWbs03] = transformer.Create03XerProjWbsTable(),
            [EnhancedTableNames.XerPredecessor06] = transformer.Create06XerPredecessor(new ConcurrentDictionary<string, XerTable>()),
            [EnhancedTableNames.XerActvType07] = transformer.Create07XerActvType(),
            [EnhancedTableNames.XerActvCode08] = transformer.Create08XerActvCode(),
            [EnhancedTableNames.XerTaskActv09] = transformer.Create09XerTaskActv(),
            [EnhancedTableNames.XerCalendar10] = transformer.Create10XerCalendar(),
            [EnhancedTableNames.XerRsrc12] = transformer.Create12XerRsrc(),
            [EnhancedTableNames.XerResourceDist15] = transformer.Create15XerResourceDistribution()
        };
        ValidateTransformOutcomes(enhanced, transformer);

        ProgrammeReviewTableContract taskContract = ProgrammeReviewContract.GetTable("01_XER_TASK");
        IReadOnlyList<ProgrammeReviewOutputRow> taskRows = BuildTaskRows(
            RequireEnhancedTable(enhanced, taskContract), taskContract, cancellationToken);
        var result = new List<ProgrammeReviewOutputTable>
        {
            new(taskContract, Sort(taskContract, taskRows))
        };

        foreach (ProgrammeReviewTableContract contract in ProgrammeReviewContract.Tables.Skip(1))
        {
            cancellationToken.ThrowIfCancellationRequested();
            enhanced.TryGetValue(contract.EnhancedTableName, out XerTable? source);
            IReadOnlyList<ProgrammeReviewOutputRow> rows = source is null
                ? Array.Empty<ProgrammeReviewOutputRow>()
                : BuildProjectedRows(source, contract, cancellationToken);
            result.Add(new ProgrammeReviewOutputTable(contract, Sort(contract, rows)));
        }

        ValidateRequiredSnapshotCoverage(result);
        ValidateKeysAndRelationships(result);
        XerTable warnings = transformer.CreateDataQualityTable();
        warnings.AddRows(_quality.Rows);
        DataQualityTable = BuildDataQualityTable(warnings, cancellationToken);
        return result;
    }

    private XerTable BuildDataQualityTable(XerTable source, CancellationToken cancellationToken)
    {
        var result = new XerTable(XerDataQuality.TableName, source.RowCount);
        result.SetHeaders(source.Headers!.ToArray());
        if (source.RowCount == 0) return result;

        // Correlate diagnostic rows by their internal occurrence token. Filenames
        // remain provenance, and the public namespace is the governed snapshot.
        var snapshotsByToken = new Dictionary<string, ResolvedProgrammeReviewSnapshot>(StringComparer.Ordinal);
        foreach (string tableName in _dataStore.TableNames)
        {
            foreach (DataRow row in _dataStore.GetTable(tableName)!.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ResolvedProgrammeReviewSnapshot snapshot = _snapshotByOriginal[row.SourceFilename];
                if (snapshotsByToken.TryGetValue(row.SourceToken, out ResolvedProgrammeReviewSnapshot? existing)
                    && !ReferenceEquals(existing, snapshot))
                    throw new ProgrammeReviewValidationException("A data-quality source occurrence belongs to multiple snapshots.");
                snapshotsByToken[row.SourceToken] = snapshot;
            }
        }

        foreach (DataRow row in source.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!snapshotsByToken.TryGetValue(row.SourceToken, out ResolvedProgrammeReviewSnapshot? snapshot))
                throw new ProgrammeReviewValidationException("A data-quality row has an unresolved retained source occurrence.");
            string[] values = row.Fields.ToArray();
            foreach (string key in new[] { "proj_id_key", "task_id_key", "rsrc_id_key", "taskrsrc_id_key" })
            {
                string raw = values[source.FieldIndexes[key]];
                try { values[source.FieldIndexes[key]] = Namespace(raw, snapshot, key); }
                catch (ProgrammeReviewValidationException)
                {
                    values[source.FieldIndexes[key]] = string.Empty;
                    values[source.FieldIndexes["message"]] += $" Diagnostic key '{key}' cannot be namespaced and remains blank; raw value: '{raw}'.";
                }
            }
            values[source.FieldIndexes["source_namespace"]] = ProgrammeReviewNaming.NamespacePrefix(
                _request.ProjectCode, _request.ProgrammeType, snapshot.SnapshotTag).TrimEnd(':');
            result.AddRow(row with { Fields = values, OriginalSourceFilename = snapshot.OriginalXerFilename });
        }
        return result;
    }

    // Parsed-data callers can supply snapshots which naming resolution discarded.
    // Filter before any shared calculation (including calendars and resource curves),
    // exactly as the file/byte entrypoints do, without mutating caller-owned rows.
    private XerDataStore CreateRetainedDataStore(XerDataStore source, CancellationToken cancellationToken)
    {
        bool hasDiscardedRows = false;
        foreach (string tableName in source.TableNames)
        {
            foreach (DataRow row in source.GetTable(tableName)!.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!_snapshotByOriginal.ContainsKey(row.SourceFilename))
                {
                    hasDiscardedRows = true;
                    break;
                }
            }
            if (hasDiscardedRows) break;
        }
        // File/byte paths already parse only retained sources. Avoid duplicating
        // their complete row arrays, particularly in a browser's limited heap.
        if (!hasDiscardedRows) return source;

        var retained = new XerDataStore();
        foreach (string tableName in source.TableNames)
        {
            XerTable original = source.GetTable(tableName)!;
            var table = new XerTable(original.Name);
            if (original.Headers is not null) table.SetHeaders((string[])original.Headers.Clone());
            foreach (DataRow row in original.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_snapshotByOriginal.ContainsKey(row.SourceFilename))
                    table.AddRow(row with { Fields = (string[])row.Fields.Clone() });
            }
            // A table belonging exclusively to discarded inputs is not part of
            // the retained schema either (its headers may be incomplete/invalid).
            if (table.Rows.Count > 0 || original.Rows.Count == 0) retained.AddTable(table);
        }
        return retained;
    }

    private XerTable RequireEnhancedTable(
        IReadOnlyDictionary<string, XerTable?> enhanced,
        ProgrammeReviewTableContract contract)
    {
        if (!enhanced.TryGetValue(contract.EnhancedTableName, out XerTable? table) || table is null)
            throw new ProgrammeReviewValidationException(
                $"Required enhanced source table '{contract.EnhancedTableName}' could not be generated. TASK, PROJECT and CALENDAR data are prerequisites.");
        return table;
    }

    private IReadOnlyList<ProgrammeReviewOutputRow> BuildTaskRows(
        XerTable source,
        ProgrammeReviewTableContract contract,
        CancellationToken cancellationToken)
    {
        var rows = new List<TaskRow>();
        foreach (DataRow dataRow in source.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_snapshotByOriginal.TryGetValue(dataRow.SourceFilename, out ResolvedProgrammeReviewSnapshot? snapshot))
                continue;

            var reader = new EnhancedRowReader(source, dataRow);
            ReviewRowEvidence evidence = _quality.ForRow(source, dataRow);
            string context = $"{contract.TableName}/{snapshot.OriginalXerFilename}";
            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (ProgrammeReviewColumn column in contract.Columns)
            {
                if (TaskHistoryColumns.Contains(column.Name)) continue;

                string raw = reader.Get(column);
                values[column.Name] = _quality.Evaluate(evidence, column.Name, raw, () =>
                {
                    string selected = column.Name switch
                    {
                        "filename" => snapshot.CanonicalXerFilename,
                        "ProjectCode" => _request.ProjectCode,
                        "ProjectName" => _request.ProjectName,
                        "UpdateDate" => Iso(snapshot.EffectiveUpdateDate),
                        "monthupdate" => Iso(snapshot.MonthUpdate),
                        "data_date" => Iso(snapshot.DataDate),
                        _ when NamespacedKeyColumns.Contains(column.Name) => Namespace(reader.Get(column), snapshot, column.Name),
                        _ => reader.Get(column)
                    };
                    return ProgrammeReviewCsv.Normalize(selected, column, context);
                });
            }

            string nativeProjectId = _quality.Evaluate(evidence, "proj_id_key", reader.Get("proj_id_key"),
                () => ExtractNativeId(reader.Get("proj_id_key"), snapshot.OriginalXerFilename, "proj_id_key"));
            if (!_projectNativeIdBySource.TryGetValue(snapshot.OriginalXerFilename, out string? existingProject)
                || !string.Equals(existingProject, nativeProjectId, StringComparison.OrdinalIgnoreCase))
                _quality.Warn(evidence, "REVIEW_PROJECT_REFERENCE_INVALID",
                    $"TASK proj_id '{nativeProjectId}' does not match the governed source PROJECT identity.", "proj_id_key", reader.Get("proj_id_key"));

            rows.Add(new TaskRow(snapshot, values));
            _outputEvidence[values] = evidence;
        }

        PopulateHistory(rows);
        return rows.Select(row => new ProgrammeReviewOutputRow(row.Snapshot, Finalize(contract, row.Values))).ToArray();
    }

    private IReadOnlyList<ProgrammeReviewOutputRow> BuildProjectedRows(
        XerTable source,
        ProgrammeReviewTableContract contract,
        CancellationToken cancellationToken)
    {
        // No raw value is projected from an empty table. Its partial header set
        // must not prevent the fixed header-only optional output; required-table
        // coverage remains enforced after projection.
        if (source.Rows.Count == 0) return Array.Empty<ProgrammeReviewOutputRow>();
        var rows = new List<ProgrammeReviewOutputRow>();

        foreach (DataRow dataRow in source.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_snapshotByOriginal.TryGetValue(dataRow.SourceFilename, out ResolvedProgrammeReviewSnapshot? snapshot))
                continue;
            var reader = new EnhancedRowReader(source, dataRow);
            ReviewRowEvidence evidence = _quality.ForRow(source, dataRow);

            string context = $"{contract.TableName}/{snapshot.OriginalXerFilename}";
            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (ProgrammeReviewColumn column in contract.Columns)
            {
                string original = reader.Get(column);
                if (column.Name is not ("ProjectCode" or "monthupdate" or "last_recalc_date")
                    && !source.FieldIndexes.ContainsKey(column.Name)
                    && !column.SourceAliases.Any(source.FieldIndexes.ContainsKey))
                    _quality.Warn(evidence, "REVIEW_SOURCE_COLUMN_MISSING",
                        "The source column is unavailable; the row is preserved with a blank projected cell.", column.Name);
                values[column.Name] = _quality.Evaluate(evidence, column.Name, original, () =>
                {
                    string raw = column.Name switch
                    {
                        "ProjectCode" => _request.ProjectCode,
                        "monthupdate" => Iso(snapshot.MonthUpdate),
                        "last_recalc_date" => Iso(snapshot.DataDate),
                        "free_float" when contract.TableName == "06_XER_PREDECESSOR"
                            && reader.Get("free_float_status") is not ("Finite" or "Estimated") => string.Empty,
                        _ when NamespacedKeyColumns.Contains(column.Name) => Namespace(reader.Get(column), snapshot, column.Name),
                        _ => reader.Get(column)
                    };

                    if (contract.TableName == "15_XER_RESOURCE_DISTRIBUTION"
                        && column.Name == "unit"
                        && string.Equals(reader.Get("rsrc_type"), "RT_Labor", StringComparison.OrdinalIgnoreCase)
                        && string.Equals(raw, "unit/time", StringComparison.OrdinalIgnoreCase))
                        raw = "hours";

                    return ProgrammeReviewCsv.Normalize(raw, column, context);
                });
            }

            if (contract.TableName == "02_XER_PROJECT") ValidateDataDate(reader, snapshot, context, "last_recalc_date");
            _outputEvidence[values] = evidence;
            rows.Add(new ProgrammeReviewOutputRow(snapshot, Finalize(contract, values)));
        }
        return rows;
    }

    private void PopulateHistory(IReadOnlyList<TaskRow> rows)
    {
        if (rows.Count == 0) return;
        DateOnly projectFirstMonth = rows.Min(r => r.Snapshot.MonthUpdate);
        DateOnly[] dataDates = rows.Select(DataDate).Distinct().Order().ToArray();
        var ambiguousCodes = rows.GroupBy(r => (r.Snapshot.EffectiveUpdateDate, r["task_code"]))
            .Where(g => string.IsNullOrWhiteSpace(g.Key.Item2) || g.Count() > 1)
            .Select(g => g.Key.Item2).ToHashSet(StringComparer.Ordinal);
        var duplicateIds = rows.GroupBy(r => r["task_id_key"], StringComparer.Ordinal)
            .Where(g => string.IsNullOrWhiteSpace(g.Key) || g.Count() > 1)
            .SelectMany(g => g.Select(r => r["task_code"]));
        ambiguousCodes.UnionWith(duplicateIds);
        var invalidHistoryCodes = rows.Where(r => _quality.HasInvalidColumn(_outputEvidence[r.Values],
            "Start", "Finish", "early_start_date", "early_end_date", "late_end_date", "remaining_duration", "status_code")
                || r["status_code"] is not ("Not Started" or "In Progress" or "Complete")
                || Number(r, "remaining_duration") is < 0m)
            .Select(r => r["task_code"]).ToHashSet(StringComparer.Ordinal);
        foreach (TaskRow row in rows.Where(r => ambiguousCodes.Contains(r["task_code"]) || invalidHistoryCodes.Contains(r["task_code"])))
        {
            foreach (string column in TaskHistoryColumns) row[column] = string.Empty;
            bool ambiguous = ambiguousCodes.Contains(row["task_code"]);
            _quality.Warn(_outputEvidence[row.Values], ambiguous ? "REVIEW_HISTORY_AMBIGUOUS" : "REVIEW_HISTORY_INPUT_INVALID",
                ambiguous
                    ? "History is blank because activity business identity or native task identity is blank or duplicated in a snapshot; no occurrence was selected or discarded."
                    : "History is blank because a source endpoint or other required history input is invalid in this activity's snapshot chain; invalid evidence is not treated as zero variance.",
                "task_code", row["task_code"]);
        }
        rows = rows.Where(r => !ambiguousCodes.Contains(r["task_code"]) && !invalidHistoryCodes.Contains(r["task_code"])).ToArray();
        if (rows.Count == 0) return;
        DateOnly baselineUpdate = _request.Snapshots.Min(s => s.EffectiveUpdateDate);
        DateOnly[] updateDates = _request.Snapshots.Select(s => s.EffectiveUpdateDate).Distinct().Order().ToArray();

        var byTask = rows.GroupBy(r => r["task_code"], StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.OrderBy(r => r.Snapshot.EffectiveUpdateDate).ToArray(), StringComparer.Ordinal);
        var byUpdateAndTask = rows.ToDictionary(
            r => (r.Snapshot.EffectiveUpdateDate, r["task_code"]), r => r);

        foreach ((string _, TaskRow[] taskRows) in byTask)
        {
            TaskRow? baseline = taskRows.FirstOrDefault(r => r.Snapshot.EffectiveUpdateDate == baselineUpdate);
            DateOnly firstAddedMonth = taskRows.Min(r => r.Snapshot.MonthUpdate);
            TaskRow[] firstMonthRows = taskRows.Where(r => r.Snapshot.MonthUpdate == firstAddedMonth).ToArray();
            DateOnly? adjustedStart = MinDate(firstMonthRows.Select(Start));
            DateOnly? adjustedFinish = MaxDate(firstMonthRows.Select(Finish));

            for (int i = 0; i < taskRows.Length; i++)
            {
                TaskRow current = taskRows[i];
                TaskRow? previousTask = i > 0 ? taskRows[i - 1] : null;
                int updateIndex = Array.IndexOf(updateDates, current.Snapshot.EffectiveUpdateDate);
                DateOnly? previousProjectUpdate = updateIndex > 0 ? updateDates[updateIndex - 1] : null;
                TaskRow? previousProjectTask = previousProjectUpdate is { } priorUpdate
                    && byUpdateAndTask.TryGetValue((priorUpdate, current["task_code"]), out TaskRow? found)
                        ? found
                        : null;
                int dataIndex = Array.IndexOf(dataDates, DataDate(current));
                DateOnly? previousDataDate = dataIndex > 0 ? dataDates[dataIndex - 1] : null;

                DateOnly? currentStart = Start(current);
                DateOnly? currentFinish = Finish(current);
                DateOnly? priorStart = previousTask is null ? null : Start(previousTask);
                DateOnly? priorFinish = previousTask is null ? null : Finish(previousTask);
                DateOnly currentDataDate = DataDate(current);

                current["Finish_Variance_Previous_Month"] = WeekdayDiff(priorFinish, currentFinish, zeroWhenMissing: true);
                current["Start_Variance_Previous_Month"] = WeekdayDiff(priorStart, currentStart, zeroWhenMissing: true);
                current["Variance_Finish_BL"] = WeekdayDiff(baseline is null ? null : Finish(baseline), currentFinish, zeroWhenMissing: false);
                current["Variance_Finish_Adjusted_BL"] = WeekdayDiff(adjustedFinish, currentFinish, zeroWhenMissing: false);
                current["Driven_DataDate"] = CalculateDrivenDataDate(current, previousTask);
                current["Baseline Finish"] = Iso(baseline is null ? null : Finish(baseline));
                current["Baseline Start"] = Iso(baseline is null ? null : Start(baseline));
                current["Previous Month Start"] = Iso(priorStart ?? currentStart);
                current["Previous Month Finish"] = Iso(priorFinish ?? currentFinish);
                current["PreviousDataDate"] = Iso(previousDataDate);
                current["Previous Remaining Working Days"] = previousTask?["remaining_duration"] ?? string.Empty;

                bool planned = previousProjectTask is not null
                    && Finish(previousProjectTask) is { } priorProjectFinish
                    && priorProjectFinish > DataDate(previousProjectTask)
                    && priorProjectFinish <= currentDataDate;
                bool complete = IsStatus(current, "Complete");
                bool previousWasPresentAndIncomplete = previousProjectTask is not null && !IsStatus(previousProjectTask, "Complete");
                current["Planned Not Completed Last Period"] = BoolInt(planned && !complete);
                current["Planned Last Period"] = BoolInt(planned);
                current["Completed Last Period"] = BoolInt(complete && previousWasPresentAndIncomplete);
                current["Completed of Planned Last Period"] = BoolInt(planned && complete);
                current["Baseline Effective_Early_End"] = Iso(baseline is null ? null : EffectiveEarlyEnd(baseline));
                current["Baseline Effective_Late_End"] = Iso(baseline is null ? null : EffectiveLateEnd(baseline));
                current["Adjusted Baseline Finish"] = Iso(adjustedFinish);
                current["Adjusted Baseline Start"] = Iso(adjustedStart);
                current["Adjusted Baseline Source Month"] = Iso(firstAddedMonth);
                current["Adjusted Baseline Source"] = firstAddedMonth == projectFirstMonth ? "Original Baseline" : "Later Update";
            }
        }
    }

    private string CalculateDrivenDataDate(TaskRow current, TaskRow? previous)
    {
        string[] eligibleTypes = { "TT_Task", "TT_Rsrc", "TT_Mile", "TT_FinMile" };
        DateOnly? effectiveStart = EffectiveStart(current);
        DateOnly? effectiveFinish = EffectiveFinish(current);
        if (!eligibleTypes.Contains(current["task_type"], StringComparer.Ordinal)
            || current["task_code"].Length == 0
            || IsStatus(current, "Complete")
            || effectiveStart is null
            || effectiveFinish is null)
            return "Not Driven";

        DateOnly dataDate = DataDate(current);
        if (previous is not null)
        {
            int periodDays = dataDate.DayNumber - DataDate(previous).DayNumber;
            if (periodDays > 0
                && IsStatus(current, "Not Started")
                && IsStatus(previous, "Not Started")
                && EffectiveStart(previous) is { } priorStart
                && EffectiveFinish(previous) is { } priorFinish
                && effectiveStart.Value.DayNumber - priorStart.DayNumber > 0
                && Math.Abs((effectiveStart.Value.DayNumber - priorStart.DayNumber) - periodDays) <= 1
                && Math.Abs((effectiveFinish.Value.DayNumber - effectiveStart.Value.DayNumber)
                    - (priorFinish.DayNumber - priorStart.DayNumber)) <= 1)
                return "Not Started - Slippage Matches Update Period";

            if (periodDays > 0
                && IsStatus(current, "In Progress")
                && IsStatus(previous, "In Progress")
                && Number(current, "remaining_duration") is { } remaining
                && Number(previous, "remaining_duration") is { } priorRemaining
                && remaining >= priorRemaining - 1m
                && EffectiveFinish(previous) is { } previousFinish
                && effectiveFinish.Value.DayNumber - previousFinish.DayNumber > 0
                && Math.Abs((effectiveFinish.Value.DayNumber - previousFinish.DayNumber) - periodDays) <= 1)
                return "In Progress - Stagnating Progress";
        }

        if (IsStatus(current, "Not Started") && effectiveStart == dataDate)
            return "Not Started - Early Start Sitting on Data Date";
        if (IsStatus(current, "In Progress")
            && Date(current, "early_start_date") is not null
            && Number(current, "remaining_duration") is > 0m
            && effectiveStart == dataDate)
            return "In Progress - Remaining Work Sitting on Data Date";
        return "Not Driven";
    }

    private void ValidateRequiredSnapshotCoverage(IReadOnlyList<ProgrammeReviewOutputTable> tables)
    {
        foreach (ProgrammeReviewOutputTable table in tables.Where(t => t.Contract.SourceRequired))
        {
            foreach (ResolvedProgrammeReviewSnapshot snapshot in _request.Snapshots)
            {
                if (!table.Rows.Any(r => ReferenceEquals(r.Snapshot, snapshot)))
                    WarnMissingSource(table.Contract.TableName, snapshot);
            }
        }
    }

    private void ValidateTransformOutcomes(IDictionary<string, XerTable?> enhanced,
        XerTransformer transformer)
    {
        foreach (var pair in enhanced.ToArray())
        {
            if (pair.Value is not null) continue;
            bool required = ProgrammeReviewContract.Tables.Any(t => t.EnhancedTableName == pair.Key && t.SourceRequired);
            // Optional absence is a normal header-only contract. A required or
            // failed transformation is recovered through the shared row-preserving
            // Core recovery path, with original source evidence in its warnings.
            if (required || transformer.GetGenerationFailure(pair.Key) is not null)
                enhanced[pair.Key] = transformer.RecoverEnhancedTable(pair.Key);
        }
    }

    private void ValidateSingleProjectPerSnapshot()
    {
        XerTable? projects = _dataStore.GetTable(TableNames.Project);
        if (projects?.Headers is null || !projects.FieldIndexes.TryGetValue(FieldNames.ProjectId, out int projectIdIndex))
            throw new ProgrammeReviewValidationException("Raw PROJECT data with a proj_id column is required.");

        foreach (ResolvedProgrammeReviewSnapshot snapshot in _request.Snapshots)
        {
            string[] projectIds = projects.Rows
                .Where(row => string.Equals(row.SourceFilename, snapshot.OriginalXerFilename, StringComparison.OrdinalIgnoreCase))
                .Select(row => XerTable.GetFieldValueSafe(row, projectIdIndex).Trim())
                .Where(value => value.Length > 0)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
            if (projectIds.Length != 1)
                throw new ProgrammeReviewValidationException(
                    $"{snapshot.OriginalXerFilename}: expected exactly one native PROJECT/proj_id, found {projectIds.Length}. Split multi-project exports before publication.");
            _projectNativeIdBySource[snapshot.OriginalXerFilename] = projectIds[0];
        }
    }

    private void ValidateKeysAndRelationships(IReadOnlyList<ProgrammeReviewOutputTable> tables)
    {
        var byName = tables.ToDictionary(t => t.Contract.TableName, StringComparer.OrdinalIgnoreCase);
        foreach (ProgrammeReviewOutputTable table in tables)
        {
            if (table.Contract.KeyColumns.Count > 0)
                WarnDuplicateKeys(table, table.Contract.KeyColumns);

            foreach (ProgrammeReviewOutputRow row in table.Rows)
            {
                string expectedPrefix = ProgrammeReviewNaming.NamespacePrefix(
                    _request.ProjectCode,
                    _request.ProgrammeType,
                    row.Snapshot.SnapshotTag);
                foreach (string keyColumn in table.Contract.Columns.Select(c => c.Name).Where(NamespacedKeyColumns.Contains))
                {
                    string value = row.Values[keyColumn];
                    if (value.Length == 0) continue;
                    if (!value.StartsWith(expectedPrefix, StringComparison.Ordinal)
                        || value.Length == expectedPrefix.Length
                        || value.Contains('|', StringComparison.Ordinal)
                        || value[expectedPrefix.Length..].Contains(ProgrammeReviewNaming.NamespaceDelimiter, StringComparison.Ordinal))
                        throw new ProgrammeReviewValidationException(
                            $"{table.Contract.TableName}: '{keyColumn}' does not use the row's schema {ProgrammeReviewContract.SchemaVersion} relationship-key namespace.");
                }
            }
        }

        HashSet<string> tasks = Values(byName["01_XER_TASK"], "task_id_key");
        HashSet<string> wbs = Values(byName["03_XER_PROJWBS"], "wbs_id_key");
        HashSet<string> projects = Values(byName["02_XER_PROJECT"], "proj_id_key");
        HashSet<string> calendars = Values(byName["10_XER_CALENDAR"], "clndr_id_key");
        HashSet<string> types = Values(byName["07_XER_ACTVTYPE"], "actv_code_type_id_key");
        HashSet<string> codes = Values(byName["08_XER_ACTVCODE"], "actv_code_id_key");
        HashSet<string> resources = Values(byName["12_XER_RSRC"], "rsrc_id_key");

        RequireReferences(byName["01_XER_TASK"], "wbs_id_key", wbs);
        RequireReferences(byName["01_XER_TASK"], "proj_id_key", projects);
        RequireReferences(byName["01_XER_TASK"], "calendar_id_key", calendars);
        RequireReferences(byName["03_XER_PROJWBS"], "parent_wbs_id_key", wbs, allowBlank: true);
        RequireReferences(byName["06_XER_PREDECESSOR"], "task_id_key", tasks);
        RequireReferences(byName["06_XER_PREDECESSOR"], "pred_task_id_key", tasks);
        RequireReferences(byName["08_XER_ACTVCODE"], "actv_code_type_id_key", types);
        RequireReferences(byName["09_XER_TASKACTV"], "task_id_key", tasks);
        RequireReferences(byName["09_XER_TASKACTV"], "actv_code_id_key", codes);
        RequireReferences(byName["15_XER_RESOURCE_DISTRIBUTION"], "task_id_key", tasks);
        RequireReferences(byName["15_XER_RESOURCE_DISTRIBUTION"], "rsrc_id_key", resources);
    }

    private void RequireReferences(
        ProgrammeReviewOutputTable table,
        string column,
        HashSet<string> valid,
        bool allowBlank = false)
    {
        foreach (ProgrammeReviewOutputRow row in table.Rows)
        {
            string value = row.Values[column];
            if (allowBlank && value.Length == 0) continue;
            if (!valid.Contains(value))
                _quality.Warn(_outputEvidence[row.Values], "REVIEW_REFERENCE_UNRESOLVED",
                    $"{table.Contract.TableName}.{column} contains an orphan or blank key '{value}'; the row is preserved.", column, value);
        }
    }

    private static HashSet<string> Values(ProgrammeReviewOutputTable table, string column) =>
        table.Rows.Select(r => r.Values[column]).Where(v => v.Length > 0).ToHashSet(StringComparer.Ordinal);

    private string Namespace(string raw, ResolvedProgrammeReviewSnapshot snapshot, string columnName)
    {
        if (raw.Length == 0) return string.Empty;
        try
        {
            return ProgrammeReviewNaming.NamespaceKey(
                raw,
                snapshot.OriginalXerFilename,
                _request.ProjectCode,
                _request.ProgrammeType,
                snapshot.SnapshotTag);
        }
        catch (ProgrammeReviewValidationException ex)
        {
            throw new ProgrammeReviewValidationException(
                $"{snapshot.OriginalXerFilename}: cannot namespace '{columnName}'. {ex.Message}", ex);
        }
    }

    private static string ExtractNativeId(string value, string originalFilename, string columnName)
    {
        if (string.IsNullOrWhiteSpace(value)) return string.Empty;
        string trimmed = value.Trim();
        string prefix = originalFilename + ".";
        if (trimmed.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) return trimmed[prefix.Length..];
        if (!trimmed.Contains('.', StringComparison.Ordinal)) return trimmed;
        throw new ProgrammeReviewValidationException(
            $"{originalFilename}: '{columnName}' key '{value}' has an unexpected source prefix.");
    }

    private IReadOnlyDictionary<string, string> Finalize(
        ProgrammeReviewTableContract contract,
        Dictionary<string, string> values)
    {
        foreach (ProgrammeReviewColumn column in contract.Columns)
        {
            values.TryGetValue(column.Name, out string? raw);
            // Cells were assessed before projection. A blank rejected cell must
            // not be rejected again by the fixed contract's required-cell rule.
            values[column.Name] = string.IsNullOrWhiteSpace(raw) ? string.Empty
                : _quality.Evaluate(_outputEvidence[values], column.Name, raw,
                    () => ProgrammeReviewCsv.Normalize(raw, column, contract.TableName));
        }
        return values;
    }

    private void WarnDuplicateKeys(ProgrammeReviewOutputTable table, IReadOnlyList<string> columns)
    {
        foreach (var group in table.Rows.GroupBy(r => string.Join("\u001f", columns.Select(c => r.Values[c])), StringComparer.Ordinal))
        {
            bool missing = group.Any(r => columns.Any(c => string.IsNullOrWhiteSpace(r.Values[c])));
            if (!missing && group.Count() == 1) continue;
            foreach (ProgrammeReviewOutputRow row in group)
                _quality.Warn(_outputEvidence[row.Values], "REVIEW_KEY_AMBIGUOUS",
                    "The review key is blank or duplicated; every source occurrence is preserved. Consumer key uniqueness must be repaired upstream.",
                    string.Join(',', columns), group.Key);
        }
    }

    private void WarnMissingSource(string tableName, ResolvedProgrammeReviewSnapshot snapshot)
    {
        XerTable projects = _dataStore.GetTable(TableNames.Project)!;
        DataRow anchor = projects.Rows.First(r => string.Equals(r.SourceFilename, snapshot.OriginalXerFilename, StringComparison.OrdinalIgnoreCase));
        _quality.Rows.Add(XerDataQuality.CreateWarning(tableName, "REVIEW_SOURCE_UNAVAILABLE",
            $"No available rows for '{tableName}' in this snapshot; the numbered CSV retains its header.", anchor, 0));
    }

    internal static IReadOnlyList<ProgrammeReviewOutputRow> Sort(
        ProgrammeReviewTableContract contract,
        IReadOnlyList<ProgrammeReviewOutputRow> rows) => rows
            .OrderBy(r => string.Join("\u001f", contract.SortColumns.Select(c => r.Values[c])), StringComparer.Ordinal)
            .ThenBy(r => string.Join("\u001f", contract.Columns.Select(c => r.Values[c.Name])), StringComparer.Ordinal)
            .ThenBy(r => r.Snapshot.CanonicalXerFilename, StringComparer.Ordinal)
            .ToArray();

    private static void ValidateDataDate(
        EnhancedRowReader reader,
        ResolvedProgrammeReviewSnapshot snapshot,
        string context,
        string sourceColumn = "data_date")
    {
        string raw = reader.GetOptional(sourceColumn, sourceColumn == "data_date" ? "Data Date" : sourceColumn);
        if (raw.Length == 0)
            throw new ProgrammeReviewValidationException(
                $"{context}: XER source column '{sourceColumn}' is blank; manifest metadata cannot silently replace a missing source date.");
        string normalized = ProgrammeReviewCsv.Normalize(raw,
            new ProgrammeReviewColumn(sourceColumn, ProgrammeReviewColumnType.Date), context);
        if (!string.Equals(normalized, Iso(snapshot.DataDate), StringComparison.Ordinal))
            throw new ProgrammeReviewValidationException(
                $"{context}: manifest data_date {snapshot.DataDate:yyyy-MM-dd} does not match XER {sourceColumn} {normalized}.");
    }

    private static string WeekdayDiff(DateOnly? from, DateOnly? to, bool zeroWhenMissing)
    {
        if (from is null || to is null) return zeroWhenMissing ? "0" : string.Empty;
        return CalculateWeekdayVariance(from.Value, to.Value).ToString(CultureInfo.InvariantCulture);
    }

    internal static long CalculateWeekdayVariance(DateOnly from, DateOnly to)
    {
        if (from == to) return 0;
        int sign = to > from ? 1 : -1;
        DateOnly min = from < to ? from : to;
        DateOnly max = from < to ? to : from;
        int days = max.DayNumber - min.DayNumber;
        int fullWeeks = days / 7;
        int remainder = days % 7;
        int isoDayOfWeek = min.DayOfWeek == DayOfWeek.Sunday ? 7 : (int)min.DayOfWeek;
        int weekdays = fullWeeks * 5
            + Math.Max(0, Math.Min(remainder, 6 - isoDayOfWeek))
            + Math.Max(0, remainder - (8 - isoDayOfWeek));
        return sign * weekdays;
    }

    private static DateOnly? EffectiveStart(TaskRow row) => Date(row, "early_start_date") ?? Start(row);
    private static DateOnly? EffectiveFinish(TaskRow row) => Date(row, "early_end_date") ?? Finish(row);
    private static DateOnly? EffectiveEarlyEnd(TaskRow row) => IsStatus(row, "Complete") ? Finish(row) : Date(row, "early_end_date");
    private static DateOnly? EffectiveLateEnd(TaskRow row) => IsStatus(row, "Complete") ? Finish(row) : Date(row, "late_end_date");
    private static DateOnly? Start(TaskRow row) => Date(row, "Start");
    private static DateOnly? Finish(TaskRow row) => Date(row, "Finish");
    private static DateOnly DataDate(TaskRow row) => Date(row, "data_date")!.Value;

    private static DateOnly? Date(TaskRow row, string column) =>
        DateOnly.TryParseExact(row[column], "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateOnly value)
            ? value
            : null;

    private static decimal? Number(TaskRow row, string column) =>
        decimal.TryParse(row[column], NumberStyles.Float, CultureInfo.InvariantCulture, out decimal value) ? value : null;

    private static DateOnly? MinDate(IEnumerable<DateOnly?> values) =>
        values.Where(v => v.HasValue).Select(v => v!.Value).Order().Cast<DateOnly?>().FirstOrDefault();

    private static DateOnly? MaxDate(IEnumerable<DateOnly?> values) =>
        values.Where(v => v.HasValue).Select(v => v!.Value).OrderDescending().Cast<DateOnly?>().FirstOrDefault();

    private static bool IsStatus(TaskRow row, string status) =>
        string.Equals(row["status_code"], status, StringComparison.Ordinal);

    private static string BoolInt(bool value) => value ? "1" : "0";
    private static string Iso(DateOnly value) => value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    private static string Iso(DateOnly? value) => value?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? string.Empty;

    private sealed class TaskRow
    {
        internal TaskRow(ResolvedProgrammeReviewSnapshot snapshot, Dictionary<string, string> values)
        {
            Snapshot = snapshot;
            Values = values;
        }

        internal ResolvedProgrammeReviewSnapshot Snapshot { get; }
        internal Dictionary<string, string> Values { get; }
        internal string this[string column]
        {
            get => Values.TryGetValue(column, out string? value) ? value : string.Empty;
            set => Values[column] = value;
        }
    }

    private sealed class EnhancedRowReader
    {
        private readonly XerTable _table;
        private readonly DataRow _row;

        internal EnhancedRowReader(XerTable table, DataRow row)
        {
            _table = table;
            _row = row;
        }

        internal string Get(ProgrammeReviewColumn column) =>
            GetOptional(new[] { column.Name }.Concat(column.SourceAliases).ToArray());

        internal string Get(string name, params string[] aliases) =>
            GetOptional(new[] { name }.Concat(aliases).ToArray());

        internal string GetOptional(params string[] names)
        {
            foreach (string name in names)
            {
                if (_table.FieldIndexes.TryGetValue(name, out int index))
                    return XerTable.GetFieldValueSafe(_row, index);
            }
            return string.Empty;
        }
    }
}
