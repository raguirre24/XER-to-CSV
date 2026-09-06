using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Globalization;

namespace XerToCsvConverter.TenderReview;

internal sealed record TenderReviewTransformationResult(
    ResolvedTenderReviewRequest Request,
    IReadOnlyList<TenderReviewOutputTable> Tables)
{
    public required XerTable DataQualityTable { get; init; }
}

internal sealed class TenderReviewTransformer
{
    private static readonly HashSet<string> NamespacedKeyColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "proj_id_key", "wbs_id_key", "parent_wbs_id_key", "calendar_id_key", "clndr_id_key",
        "task_id_key", "pred_task_id_key", "task_pred_id_key", "actv_code_type_id_key",
        "actv_code_id_key", "rsrc_id_key"
    };

    private readonly XerDataStore _dataStore;
    private ResolvedTenderReviewRequest _request;
    private IReadOnlyDictionary<string, ResolvedTenderReviewSource> _sourceByToken;
    private IReadOnlyDictionary<RawKey, RawTask> _rawTasks = new Dictionary<RawKey, RawTask>();
    private IReadOnlyDictionary<RawKey, RawCalendar> _rawCalendars = new Dictionary<RawKey, RawCalendar>();
    private readonly Dictionary<(string Source, int Ordinal), RawTask> _rawTasksByOrdinal = new();
    private readonly Dictionary<(string Source, int Ordinal), RawRelationship> _rawRelationshipsByOrdinal = new();
    private readonly ReviewDataQualityCollector _quality;
    private readonly Dictionary<IReadOnlyDictionary<string, string>, ReviewRowEvidence> _outputEvidence = new(ReferenceEqualityComparer.Instance);

    internal TenderReviewTransformer(XerDataStore dataStore, ResolvedTenderReviewRequest request)
    {
        _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
        _request = request ?? throw new ArgumentNullException(nameof(request));
        _sourceByToken = CreateSourceLookup(request.Sources);
        _quality = new ReviewDataQualityCollector(_dataStore);
    }

    internal TenderReviewTransformationResult Build(CancellationToken cancellationToken)
    {
        _request = ResolveProjectMetadata(cancellationToken);
        _sourceByToken = CreateSourceLookup(_request.Sources);
        BuildRawSemanticLookups(cancellationToken);

        var transformer = new XerTransformer(_dataStore);
        var enhanced = new Dictionary<string, XerTable?>(StringComparer.OrdinalIgnoreCase)
        {
            [EnhancedTableNames.XerTask01] = transformer.Create01XerTaskTable(),
            [EnhancedTableNames.XerProject02] = transformer.Create02XerProject(),
            [EnhancedTableNames.XerProjWbs03] = transformer.Create03XerProjWbsTable(),
            [EnhancedTableNames.XerPredecessor06] = transformer.Create06XerPredecessor(
                new ConcurrentDictionary<string, XerTable>()),
            [EnhancedTableNames.XerActvType07] = transformer.Create07XerActvType(),
            [EnhancedTableNames.XerActvCode08] = transformer.Create08XerActvCode(),
            [EnhancedTableNames.XerTaskActv09] = transformer.Create09XerTaskActv(),
            [EnhancedTableNames.XerCalendar10] = transformer.Create10XerCalendar(),
            [EnhancedTableNames.XerRsrc12] = transformer.Create12XerRsrc(),
            [EnhancedTableNames.XerResourceDist15] = transformer.Create15XerResourceDistribution()
        };
        cancellationToken.ThrowIfCancellationRequested();
        ValidateTransformOutcomes(enhanced, transformer);

        TenderReviewTableContract taskContract = TenderReviewContract.GetTable("01_XER_TASK");
        IReadOnlyList<TenderReviewOutputRow> taskRows = BuildTaskRows(
            RequireEnhancedTable(enhanced, taskContract), taskContract, cancellationToken);
        var tables = new List<TenderReviewOutputTable>
        {
            new(taskContract, Sort(taskContract, taskRows))
        };

        foreach (TenderReviewTableContract contract in TenderReviewContract.Tables.Skip(1))
        {
            cancellationToken.ThrowIfCancellationRequested();
            enhanced.TryGetValue(contract.EnhancedTableName, out XerTable? source);
            IReadOnlyList<TenderReviewOutputRow> rows = source is null
                ? Array.Empty<TenderReviewOutputRow>()
                : BuildProjectedRows(source, contract, cancellationToken);
            if (contract.TableName == "15_XER_RESOURCE_DISTRIBUTION")
                rows = AggregateResourceDistribution(contract, rows, (group, message) =>
                {
                    foreach (TenderReviewOutputRow row in group)
                        _quality.Warn(_outputEvidence[row.Values], "REVIEW_AGGREGATION_UNAVAILABLE", message,
                            "monthly_quantity", row.Values["monthly_quantity"]);
                }, (aggregate, first) => _outputEvidence[aggregate.Values] = _outputEvidence[first.Values]);
            tables.Add(new TenderReviewOutputTable(contract, Sort(contract, rows)));
        }

        ValidateRequiredSourceCoverage(tables);
        ValidateKeysAndRelationships(tables);
        XerTable warnings = transformer.CreateDataQualityTable();
        warnings.AddRows(_quality.Rows);
        return new TenderReviewTransformationResult(
            _request,
            new ReadOnlyCollection<TenderReviewOutputTable>(tables))
        {
            DataQualityTable = BuildDataQualityTable(warnings, cancellationToken)
        };
    }

    private XerTable BuildDataQualityTable(XerTable source, CancellationToken cancellationToken)
    {
        var result = new XerTable(XerDataQuality.TableName, source.RowCount);
        result.SetHeaders(source.Headers!.ToArray());
        foreach (DataRow row in source.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.TryGetValue(row.SourceToken, out ResolvedTenderReviewSource? tenderSource))
                throw new TenderReviewValidationException("A data-quality row has an unresolved Tender source occurrence.");
            string[] values = row.Fields.ToArray();
            foreach (string key in new[] { "proj_id_key", "task_id_key", "rsrc_id_key", "taskrsrc_id_key" })
            {
                string raw = values[source.FieldIndexes[key]];
                try { values[source.FieldIndexes[key]] = Namespace(raw, tenderSource, key); }
                catch (TenderReviewValidationException)
                {
                    values[source.FieldIndexes[key]] = string.Empty;
                    string visible = raw.Replace(tenderSource.SourceToken + ".", tenderSource.OriginalXerFilename + ".", StringComparison.Ordinal);
                    values[source.FieldIndexes["message"]] += $" Diagnostic key '{key}' cannot be namespaced and remains blank; raw value: '{visible}'.";
                }
            }
            values[source.FieldIndexes["source_namespace"]] = TenderReviewNaming.NamespacePrefix(
                _request.ProjectCode, tenderSource.StatusDate).TrimEnd(':');
            result.AddRow(row with { Fields = values, OriginalSourceFilename = tenderSource.OriginalXerFilename });
        }
        return result;
    }

    private ResolvedTenderReviewRequest ResolveProjectMetadata(CancellationToken cancellationToken)
    {
        XerTable? projects = _dataStore.GetTable(TableNames.Project);
        if (projects?.Headers is null)
            throw new TenderReviewValidationException("Raw PROJECT data is required for every Tender source.");

        RequireRawHeader(projects, FieldNames.ProjectId);
        RequireRawHeader(projects, FieldNames.LastRecalcDate);
        RequireRawHeader(projects, "proj_short_name");
        int projectIdIndex = projects.FieldIndexes[FieldNames.ProjectId];
        int dataDateIndex = projects.FieldIndexes[FieldNames.LastRecalcDate];
        int projectCodeIndex = projects.FieldIndexes["proj_short_name"];

        var resolved = new List<ResolvedTenderReviewSource>(_request.Sources.Count);
        foreach (ResolvedTenderReviewSource source in _request.Sources)
        {
            cancellationToken.ThrowIfCancellationRequested();
            DataRow[] rows = projects.Rows
                .Where(row => string.Equals(row.SourceFilename, source.SourceToken, StringComparison.Ordinal))
                .ToArray();
            if (rows.Length != 1)
                throw new TenderReviewValidationException(
                    $"Tender input {source.InputIndex + 1} ('{source.OriginalXerFilename}') must contain exactly one PROJECT row; found {rows.Length}.");

            DataRow row = rows[0];
            string nativeProjectId = XerTable.GetFieldValueSafe(row, projectIdIndex).Trim();
            if (nativeProjectId.Length == 0)
                throw new TenderReviewValidationException(
                    $"{source.OriginalXerFilename}: PROJECT.proj_id is blank.");

            string sourceProjectCode;
            try
            {
                sourceProjectCode = TenderReviewNaming.NormalizeProjectCode(
                    XerTable.GetFieldValueSafe(row, projectCodeIndex));
            }
            catch (TenderReviewValidationException ex)
            {
                throw new TenderReviewValidationException(
                    $"{source.OriginalXerFilename}: PROJECT.proj_short_name is invalid. {ex.Message}", ex);
            }
            if (!TenderReviewNaming.IsSameProjectIdentity(sourceProjectCode, _request.ProjectCode))
                throw new TenderReviewValidationException(
                    $"{source.OriginalXerFilename}: PROJECT.proj_short_name '{sourceProjectCode}' does not match Tender project code '{_request.ProjectCode}'. A bundle must contain exactly one project.");

            string rawDataDate = XerTable.GetFieldValueSafe(row, dataDateIndex).Trim();
            if (rawDataDate.Length == 0)
                throw new TenderReviewValidationException(
                    $"{source.OriginalXerFilename}: PROJECT.last_recalc_date (P6 Data Date) is blank.");
            string normalized = TenderReviewCsv.NormalizeDate(
                rawDataDate, source.OriginalXerFilename, FieldNames.LastRecalcDate);
            DateOnly dataDate = DateOnly.ParseExact(normalized, "yyyy-MM-dd", CultureInfo.InvariantCulture);
            TenderReviewNaming.ValidateMetadataDate(dataDate, "data_date", source.OriginalXerFilename);

            resolved.Add(source with { DataDate = dataDate, NativeProjectId = nativeProjectId });
        }

        return _request with
        {
            Sources = new ReadOnlyCollection<ResolvedTenderReviewSource>(resolved)
        };
    }

    private void BuildRawSemanticLookups(CancellationToken cancellationToken)
    {
        _rawTasks = BuildRawTaskLookup(cancellationToken);
        _rawCalendars = BuildRawCalendarLookup(cancellationToken);
        BuildRawRelationshipLookup(cancellationToken);
    }

    private IReadOnlyDictionary<RawKey, RawTask> BuildRawTaskLookup(
        CancellationToken cancellationToken)
    {
        XerTable? table = _dataStore.GetTable(TableNames.Task);
        var result = new Dictionary<RawKey, RawTask>();
        if (table?.Headers is null) return result;
        var ambiguous = new HashSet<RawKey>();
        foreach (DataRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.ContainsKey(row.SourceFilename)) continue;
            string taskId = Raw(table, row, FieldNames.TaskId).Trim();
            var task = new RawTask(
                row.SourceFilename,
                taskId,
                Raw(table, row, FieldNames.ProjectId).Trim(),
                Raw(table, row, FieldNames.CalendarId).Trim(),
                Raw(table, row, FieldNames.StatusCode).Trim(),
                Raw(table, row, FieldNames.ActStartDate),
                Raw(table, row, FieldNames.ActEndDate),
                Raw(table, row, FieldNames.RestartDate),
                Raw(table, row, FieldNames.ReendDate),
                Raw(table, row, FieldNames.EarlyStartDate),
                Raw(table, row, FieldNames.EarlyEndDate),
                Raw(table, row, FieldNames.RemainDurationHrCnt),
                Raw(table, row, FieldNames.TotalFloatHrCnt),
                Raw(table, row, FieldNames.FreeFloatHrCnt));
            ReviewRowEvidence evidence = _quality.ForRow(table, row);
            _rawTasksByOrdinal[(row.SourceFilename, evidence.Ordinal)] = task;
            var key = new RawKey(row.SourceFilename, taskId);
            if (taskId.Length == 0 || ambiguous.Contains(key) || !result.TryAdd(key, task))
            {
                ambiguous.Add(key);
                result.Remove(key);
            }
        }
        return result;
    }

    private IReadOnlyDictionary<RawKey, RawCalendar> BuildRawCalendarLookup(
        CancellationToken cancellationToken)
    {
        XerTable? table = _dataStore.GetTable(TableNames.Calendar);
        var result = new Dictionary<RawKey, RawCalendar>();
        if (table?.Headers is null) return result;
        var ambiguous = new HashSet<RawKey>();
        foreach (DataRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.ContainsKey(row.SourceFilename)) continue;
            string calendarId = Raw(table, row, FieldNames.ClndrId).Trim();
            var calendar = new RawCalendar(calendarId, Raw(table, row, FieldNames.DayHourCount));
            var key = new RawKey(row.SourceFilename, calendarId);
            if (calendarId.Length == 0 || ambiguous.Contains(key) || !result.TryAdd(key, calendar))
            {
                ambiguous.Add(key);
                result.Remove(key);
            }
        }
        return result;
    }

    private void BuildRawRelationshipLookup(
        CancellationToken cancellationToken)
    {
        XerTable? table = _dataStore.GetTable(TableNames.TaskPred);
        if (table?.Headers is null) return;
        foreach (DataRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.ContainsKey(row.SourceFilename)) continue;
            string relationshipId = Raw(table, row, "task_pred_id").Trim();
            var relationship = new RawRelationship(
                relationshipId,
                Raw(table, row, FieldNames.TaskId).Trim(),
                Raw(table, row, FieldNames.PredTaskId).Trim(),
                Raw(table, row, FieldNames.ProjectId).Trim(),
                Raw(table, row, "pred_proj_id").Trim(),
                Raw(table, row, FieldNames.LagHrCnt));
            _rawRelationshipsByOrdinal[(row.SourceFilename, _quality.ForRow(table, row).Ordinal)] = relationship;
        }
    }


    private static IReadOnlyDictionary<string, ResolvedTenderReviewSource> CreateSourceLookup(
        IReadOnlyList<ResolvedTenderReviewSource> sources) =>
        sources.ToDictionary(source => source.SourceToken, StringComparer.Ordinal);

    private static XerTable RequireEnhancedTable(
        IReadOnlyDictionary<string, XerTable?> enhanced,
        TenderReviewTableContract contract)
    {
        if (!enhanced.TryGetValue(contract.EnhancedTableName, out XerTable? table) || table is null)
            throw new TenderReviewValidationException(
                $"Required enhanced source table '{contract.EnhancedTableName}' could not be generated. TASK, PROJECT and CALENDAR data are prerequisites.");
        return table;
    }

    private IReadOnlyList<TenderReviewOutputRow> BuildTaskRows(
        XerTable source,
        TenderReviewTableContract contract,
        CancellationToken cancellationToken)
    {
        var rows = new List<TenderReviewOutputRow>();
        foreach (DataRow dataRow in source.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.TryGetValue(dataRow.SourceFilename, out ResolvedTenderReviewSource? tenderSource))
                continue;

            var reader = new EnhancedRowReader(source, dataRow);
            ReviewRowEvidence evidence = _quality.ForRow(source, dataRow);
            string context = $"{contract.TableName}/{tenderSource.OriginalXerFilename}/input-{tenderSource.InputIndex + 1}";
            if (!_rawTasksByOrdinal.TryGetValue((tenderSource.SourceToken, evidence.Ordinal), out RawTask? rawTask))
                throw new InvalidOperationException("A generated TASK occurrence lost its raw row correlation.");

            string nativeProjectId = _quality.Evaluate(evidence, "proj_id_key", reader.Get("proj_id_key"),
                () => ExtractNativeId(reader.Get("proj_id_key"), tenderSource.SourceToken, "proj_id_key", tenderSource.OriginalXerFilename));
            if (!string.Equals(nativeProjectId, tenderSource.NativeProjectId, StringComparison.OrdinalIgnoreCase))
                _quality.Warn(evidence, "REVIEW_PROJECT_REFERENCE_INVALID",
                    $"TASK proj_id '{nativeProjectId}' does not match the source PROJECT proj_id '{tenderSource.NativeProjectId}'; the row is preserved.", "proj_id_key", reader.Get("proj_id_key"));

            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (TenderReviewColumn column in contract.Columns)
            {
                string original = reader.Get(column);
                if (column.Name is not ("ProjectCode" or "monthupdate" or "last_recalc_date" or "udf_datalake_status_date"
                    or "add_date" or "state" or "region" or "tender_status" or "filename" or "ProjectName" or "status_date" or "UpdateDate")
                    && !source.FieldIndexes.ContainsKey(column.Name)
                    && !column.SourceAliases.Any(source.FieldIndexes.ContainsKey))
                    _quality.Warn(evidence, "REVIEW_SOURCE_COLUMN_MISSING",
                        "The source column is unavailable; the row is preserved with a blank projected cell.", column.Name);
                values[column.Name] = _quality.Evaluate(evidence, column.Name, original, () =>
                {
                    string raw = column.Name switch
                    {
                        "filename" => tenderSource.CanonicalXerFilename,
                        "ProjectCode" => _request.ProjectCode,
                        "ProjectName" => _request.ProjectName,
                        "status_date" or "UpdateDate" => Iso(tenderSource.StatusDate),
                        "monthupdate" or "data_date" => Iso(RequiredDataDate(tenderSource)),
                        "Start" => DeriveTaskStart(rawTask, context),
                        "Finish" => DeriveTaskFinish(rawTask, context),
                        "remaining_duration" => ConvertTaskHours(
                            rawTask.RemainingDurationHours, rawTask, FieldNames.RemainDurationHrCnt, context),
                        "total_float" when IsComplete(rawTask) => string.Empty,
                        "total_float" => ConvertTaskHours(
                            rawTask.TotalFloatHours, rawTask, FieldNames.TotalFloatHrCnt, context),
                        "free_float" when IsComplete(rawTask) => string.Empty,
                        "free_float" => ConvertTaskHours(
                            rawTask.FreeFloatHours, rawTask, FieldNames.FreeFloatHrCnt, context),
                        _ when NamespacedKeyColumns.Contains(column.Name) =>
                            Namespace(reader.Get(column), tenderSource, column.Name),
                        _ => reader.Get(column)
                    };
                    return TenderReviewCsv.Normalize(raw, column, context);
                });
            }

            _outputEvidence[values] = evidence;
            rows.Add(new TenderReviewOutputRow(tenderSource, Finalize(contract, values)));
        }

        WarnDuplicateKeys(new TenderReviewOutputTable(contract, rows), new[] { "task_code" });
        return rows;
    }

    private IReadOnlyList<TenderReviewOutputRow> BuildProjectedRows(
        XerTable source,
        TenderReviewTableContract contract,
        CancellationToken cancellationToken)
    {
        // A legitimate empty optional table still emits the governed schema,
        // regardless of which raw columns its header-only input contains.
        // Required-table coverage is checked separately after projection.
        if (source.Rows.Count == 0) return Array.Empty<TenderReviewOutputRow>();
        var rows = new List<TenderReviewOutputRow>();

        foreach (DataRow dataRow in source.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.TryGetValue(dataRow.SourceFilename, out ResolvedTenderReviewSource? tenderSource))
                continue;
            var reader = new EnhancedRowReader(source, dataRow);
            ReviewRowEvidence evidence = _quality.ForRow(source, dataRow);

            string context = $"{contract.TableName}/{tenderSource.OriginalXerFilename}/input-{tenderSource.InputIndex + 1}";
            RawRelationship? relationship = contract.TableName == "06_XER_PREDECESSOR"
                ? _rawRelationshipsByOrdinal.GetValueOrDefault((tenderSource.SourceToken, evidence.Ordinal))
                : null;
            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (TenderReviewColumn column in contract.Columns)
            {
                string original = reader.Get(column);
                if (column.Name is not ("ProjectCode" or "monthupdate" or "last_recalc_date" or "udf_datalake_status_date"
                    or "add_date" or "state" or "region" or "tender_status")
                    && !source.FieldIndexes.ContainsKey(column.Name)
                    && !column.SourceAliases.Any(source.FieldIndexes.ContainsKey))
                    _quality.Warn(evidence, "REVIEW_SOURCE_COLUMN_MISSING",
                        "The source column is unavailable; the row is preserved with a blank projected cell.", column.Name);
                values[column.Name] = _quality.Evaluate(evidence, column.Name, original, () =>
                {
                    string raw = column.Name switch
                    {
                        "ProjectCode" => _request.ProjectCode,
                        "monthupdate" or "last_recalc_date" => Iso(RequiredDataDate(tenderSource)),
                        "udf_datalake_status_date" => Iso(tenderSource.StatusDate),
                        "distribution_month" when contract.TableName == "15_XER_RESOURCE_DISTRIBUTION" =>
                            NormalizeDistributionMonth(reader.Get(column), context),
                        "lag" when relationship is not null =>
                            ConvertRelationshipLag(relationship, tenderSource, context),
                        "total_float" when relationship is not null =>
                            ConvertRelationshipTotalFloat(relationship, tenderSource, context),
                        "free_float" when relationship is not null =>
                            ValidateRelationshipFreeFloat(
                                reader.Get(column), relationship, tenderSource, context),
                        _ when NamespacedKeyColumns.Contains(column.Name) =>
                            Namespace(reader.Get(column), tenderSource, column.Name),
                        _ => reader.Get(column)
                    };

                    if (contract.TableName == "15_XER_RESOURCE_DISTRIBUTION"
                        && column.Name == "unit"
                        && string.Equals(reader.Get("rsrc_type"), "RT_Labor", StringComparison.OrdinalIgnoreCase)
                        && string.Equals(raw, "unit/time", StringComparison.OrdinalIgnoreCase))
                        raw = "hours";

                    return TenderReviewCsv.Normalize(raw, column, context);
                });
            }

            if (contract.TableName == "02_XER_PROJECT")
                ValidateDataDate(reader, tenderSource, context, "last_recalc_date");
            _outputEvidence[values] = evidence;
            rows.Add(new TenderReviewOutputRow(tenderSource, Finalize(contract, values)));
        }
        return rows;
    }

    private static string DeriveTaskStart(RawTask task, string context)
    {
        return task.StatusCode switch
        {
            "TK_NotStart" => FirstNonBlank(task.RestartDate, task.EarlyStartDate),
            "TK_Active" or "TK_Complete" => task.ActualStartDate,
            _ => throw new TenderReviewValidationException(
                $"{context}: TASK.status_code '{task.StatusCode}' is not a recognised P6 activity status for Start derivation.")
        };
    }

    private static string DeriveTaskFinish(RawTask task, string context)
    {
        return task.StatusCode switch
        {
            "TK_Complete" => task.ActualEndDate,
            "TK_NotStart" or "TK_Active" => FirstNonBlank(task.ReendDate, task.EarlyEndDate),
            _ => throw new TenderReviewValidationException(
                $"{context}: TASK.status_code '{task.StatusCode}' is not a recognised P6 activity status for Finish derivation.")
        };
    }

    private static bool IsComplete(RawTask task) =>
        string.Equals(task.StatusCode, "TK_Complete", StringComparison.OrdinalIgnoreCase);

    private string ConvertTaskHours(
        string rawHours,
        RawTask task,
        string rawField,
        string context)
    {
        if (string.IsNullOrWhiteSpace(rawHours)) return string.Empty;
        decimal hours = ParseHours(rawHours, rawField, context);
        decimal hoursPerDay = RequireHoursPerDay(
            task, rawField, context);
        return FormatDays(hours, hoursPerDay);
    }

    private string ConvertRelationshipLag(
        RawRelationship relationship,
        ResolvedTenderReviewSource source,
        string context)
    {
        if (string.IsNullOrWhiteSpace(relationship.LagHours)) return string.Empty;
        decimal lagHours = ParseHours(relationship.LagHours, FieldNames.LagHrCnt, context);
        if (lagHours == 0m) return "0";

        (RawTask successor, RawTask predecessor) =
            RequireRelationshipTasks(relationship, source, context);
        // P6 lag days use the predecessor's time-period conversion even when another
        // calendar supplies the working intervals used to schedule the lag.
        decimal hoursPerDay = RequireHoursPerDay(
            predecessor, FieldNames.LagHrCnt, context);
        return FormatDays(lagHours, hoursPerDay);
    }

    private string ConvertRelationshipTotalFloat(
        RawRelationship relationship,
        ResolvedTenderReviewSource source,
        string context)
    {
        (RawTask successor, _) = RequireRelationshipTasks(relationship, source, context);
        if (IsComplete(successor)) return string.Empty;
        if (string.IsNullOrWhiteSpace(successor.TotalFloatHours)) return string.Empty;
        decimal hours = ParseHours(
            successor.TotalFloatHours, FieldNames.TotalFloatHrCnt, context);
        decimal hoursPerDay = RequireHoursPerDay(
            successor, FieldNames.TotalFloatHrCnt, context);
        return FormatDays(hours, hoursPerDay);
    }

    private string ValidateRelationshipFreeFloat(
        string inheritedValue,
        RawRelationship relationship,
        ResolvedTenderReviewSource source,
        string context)
    {
        if (string.IsNullOrWhiteSpace(inheritedValue)) return string.Empty;
        (RawTask successor, RawTask predecessor) =
            RequireRelationshipTasks(relationship, source, context);
        _ = RequireHoursPerDay(predecessor, "relationship free_float", context);

        decimal lagHours = string.IsNullOrWhiteSpace(relationship.LagHours)
            ? 0m
            : ParseHours(relationship.LagHours, FieldNames.LagHrCnt, context);
        return inheritedValue;
    }


    private (RawTask Successor, RawTask Predecessor) RequireRelationshipTasks(
        RawRelationship relationship,
        ResolvedTenderReviewSource source,
        string context)
    {
        if (!_rawTasks.TryGetValue(
            new RawKey(source.SourceToken, relationship.SuccessorTaskId), out RawTask? successor))
            throw new TenderReviewValidationException(
                $"{context}: TASKPRED.task_id '{relationship.SuccessorTaskId}' is an unresolved required relationship.");
        if (!_rawTasks.TryGetValue(
            new RawKey(source.SourceToken, relationship.PredecessorTaskId), out RawTask? predecessor))
            throw new TenderReviewValidationException(
                $"{context}: TASKPRED.pred_task_id '{relationship.PredecessorTaskId}' is an unresolved required relationship.");
        ValidateProjectContext(successor, relationship.SuccessorProjectId, FieldNames.ProjectId, FieldNames.TaskId);
        ValidateProjectContext(predecessor, relationship.PredecessorProjectId, "pred_proj_id", FieldNames.PredTaskId);
        return (successor, predecessor);

        void ValidateProjectContext(RawTask task, string declaredProject, string projectField, string taskField)
        {
            if (task.ProjectId.Length == 0
                || !string.Equals(task.ProjectId, source.NativeProjectId, StringComparison.OrdinalIgnoreCase))
                throw new TenderReviewValidationException(
                    $"{context}: TASKPRED.{taskField} '{task.TaskId}' belongs to TASK.proj_id '{task.ProjectId}', not the selected source project '{source.NativeProjectId}'. External endpoints are not supported in a Tender stage.");
            // Missing optional project metadata may be inferred from the uniquely
            // resolved task. An explicit mismatch cannot be inferred away.
            if (declaredProject.Length > 0
                && !string.Equals(declaredProject, task.ProjectId, StringComparison.OrdinalIgnoreCase))
                throw new TenderReviewValidationException(
                    $"{context}: TASKPRED.{projectField} '{declaredProject}' does not match TASK.proj_id '{task.ProjectId}' for {taskField} '{task.TaskId}'. The external endpoint is unresolved.");
        }
    }

    private decimal RequireHoursPerDay(RawTask task, string rawField, string context)
        => RequireHoursPerDay(task.SourceToken, task.CalendarId, "TASK.clndr_id", rawField, context);

    private decimal RequireHoursPerDay(string sourceToken, string calendarId,
        string referenceField, string rawField, string context)
    {
        if (string.IsNullOrWhiteSpace(calendarId))
            throw new TenderReviewValidationException(
                $"{context}: nonblank '{rawField}' cannot be converted because {referenceField} is blank.");
        if (!_rawCalendars.TryGetValue(
            new RawKey(sourceToken, calendarId), out RawCalendar? calendar))
            throw new TenderReviewValidationException(
                $"{context}: nonblank '{rawField}' cannot be converted because CALENDAR '{calendarId}' is missing for the source token.");

        string rawHours = calendar.DayHours.Trim();
        if (!decimal.TryParse(rawHours, NumberStyles.Float,
                CultureInfo.InvariantCulture, out decimal hoursPerDay)
            || hoursPerDay <= 0m)
            throw new TenderReviewValidationException(
                $"{context}: nonblank '{rawField}' requires a finite positive CALENDAR.day_hr_cnt for selected calendar '{calendarId}', got '{calendar.DayHours}'.");
        return hoursPerDay;
    }

    private static decimal ParseHours(string raw, string field, string context)
    {
        if (!decimal.TryParse(raw.Trim(), NumberStyles.Float,
            CultureInfo.InvariantCulture, out decimal hours))
            throw new TenderReviewValidationException(
                $"{context}: '{raw}' is not a finite invariant P6 hour value for '{field}'.");
        return hours;
    }

    private static string FormatDays(decimal hours, decimal hoursPerDay) =>
        (hours / hoursPerDay).ToString("F2", CultureInfo.InvariantCulture);

    private static string FirstNonBlank(string first, string fallback) =>
        string.IsNullOrWhiteSpace(first) ? fallback : first;

    private static string NormalizeDistributionMonth(string raw, string context)
    {
        string value = raw.Trim();
        if (DateTime.TryParseExact(value, "yyyy-MM-dd", CultureInfo.CurrentCulture,
                DateTimeStyles.None, out DateTime parsedDate)
            || DateTime.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture,
                DateTimeStyles.None, out parsedDate))
            return parsedDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        throw new TenderReviewValidationException(
            $"{context}: '{raw}' is not a valid inherited resource distribution_month.");
    }

    internal static IReadOnlyList<TenderReviewOutputRow> AggregateResourceDistribution(
        TenderReviewTableContract contract,
        IReadOnlyList<TenderReviewOutputRow> rows,
        Action<IReadOnlyList<TenderReviewOutputRow>, string>? warning = null,
        Action<TenderReviewOutputRow, TenderReviewOutputRow>? aggregatedEvidence = null)
    {
        if (rows.Count < 2) return rows;
        string[] grain = { "task_id_key", "rsrc_id_key", "is_actual", "distribution_month" };
        var aggregated = new List<TenderReviewOutputRow>();
        foreach (IGrouping<string, TenderReviewOutputRow> group in rows.GroupBy(
            row => row.Source.SourceToken + "\u001f" + string.Join("\u001f", grain.Select(column => row.Values[column])),
            StringComparer.Ordinal))
        {
            TenderReviewOutputRow first = group.First();
            var values = new Dictionary<string, string>(first.Values, StringComparer.OrdinalIgnoreCase);
            decimal total = 0m;
            string? failure = null;
            foreach (TenderReviewOutputRow row in group)
            {
                if (!ReferenceEquals(row.Source, first.Source))
                    throw new TenderReviewValidationException(
                        "Resource-distribution aggregation crossed Tender source identities.");
                foreach (TenderReviewColumn column in contract.Columns.Where(column =>
                    column.Name != "monthly_quantity" && !grain.Contains(column.Name, StringComparer.Ordinal)))
                {
                    if (!string.Equals(row.Values[column.Name], first.Values[column.Name],
                        StringComparison.Ordinal))
                        failure ??= $"15_XER_RESOURCE_DISTRIBUTION contains conflicting '{column.Name}' values at its output grain; individual contributions are retained.";
                }
                if (!decimal.TryParse(row.Values["monthly_quantity"], NumberStyles.Float,
                    CultureInfo.InvariantCulture, out decimal quantity))
                    failure ??= "15_XER_RESOURCE_DISTRIBUTION.monthly_quantity cannot be aggregated as a decimal; individual contributions are retained.";
                if (failure is null)
                {
                    try { total += quantity; }
                    catch (OverflowException)
                    {
                        failure = "15_XER_RESOURCE_DISTRIBUTION aggregate exceeds decimal range; individual contributions are retained without clamping or quantity loss.";
                    }
                }
            }
            if (failure is not null)
            {
                TenderReviewOutputRow[] contributions = group.ToArray();
                warning?.Invoke(contributions, failure);
                aggregated.AddRange(contributions);
                continue;
            }
            values["monthly_quantity"] = total.ToString("G29", CultureInfo.InvariantCulture);
            var aggregate = new TenderReviewOutputRow(first.Source, values);
            aggregatedEvidence?.Invoke(aggregate, first);
            aggregated.Add(aggregate);
        }
        return aggregated;
    }

    private void ValidateTransformOutcomes(IDictionary<string, XerTable?> enhanced,
        XerTransformer transformer)
    {
        foreach (var pair in enhanced.ToArray())
        {
            if (pair.Value is not null) continue;
            bool required = TenderReviewContract.Tables.Any(t => t.EnhancedTableName == pair.Key && t.SourceRequired);
            // Optional absence is a normal header-only contract. A required or
            // failed transformation is recovered through the shared row-preserving
            // Core recovery path, with original source evidence in its warnings.
            if (required || transformer.GetGenerationFailure(pair.Key) is not null)
                enhanced[pair.Key] = transformer.RecoverEnhancedTable(pair.Key);
        }
    }

    private void ValidateRequiredSourceCoverage(IReadOnlyList<TenderReviewOutputTable> tables)
    {
        foreach (TenderReviewOutputTable table in tables.Where(table => table.Contract.SourceRequired))
        {
            foreach (ResolvedTenderReviewSource source in _request.Sources)
            {
                if (!table.Rows.Any(row => ReferenceEquals(row.Source, source)))
                    WarnMissingSource(table.Contract.TableName, source);
            }
        }
    }

    private void ValidateKeysAndRelationships(IReadOnlyList<TenderReviewOutputTable> tables)
    {
        var byName = tables.ToDictionary(table => table.Contract.TableName, StringComparer.OrdinalIgnoreCase);
        foreach (TenderReviewOutputTable table in tables)
        {
            if (table.Contract.KeyColumns.Count > 0)
                WarnDuplicateKeys(table, table.Contract.KeyColumns);

            foreach (TenderReviewOutputRow row in table.Rows)
            {
                string expectedPrefix = TenderReviewNaming.NamespacePrefix(
                    _request.ProjectCode, row.Source.StatusDate);
                foreach (string keyColumn in table.Contract.Columns.Select(column => column.Name)
                    .Where(NamespacedKeyColumns.Contains))
                {
                    string value = row.Values[keyColumn];
                    if (value.Length == 0) continue;
                    if (!value.StartsWith(expectedPrefix, StringComparison.Ordinal)
                        || value.Length == expectedPrefix.Length
                        || value.Contains('|', StringComparison.Ordinal)
                        || value[expectedPrefix.Length..].Contains(
                            TenderReviewNaming.NamespaceDelimiter, StringComparison.Ordinal))
                        throw new TenderReviewValidationException(
                            $"{table.Contract.TableName}: '{keyColumn}' does not use the row's Tender status-date relationship-key namespace.");
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
        TenderReviewOutputTable table,
        string column,
        HashSet<string> valid,
        bool allowBlank = false)
    {
        foreach (TenderReviewOutputRow row in table.Rows)
        {
            string value = row.Values[column];
            if (allowBlank && value.Length == 0) continue;
            if (!valid.Contains(value))
                _quality.Warn(_outputEvidence[row.Values], "REVIEW_REFERENCE_UNRESOLVED",
                    $"{table.Contract.TableName}.{column} contains an orphan or blank key '{value}'; the row is preserved.", column, value);
        }
    }

    private static HashSet<string> Values(TenderReviewOutputTable table, string column) =>
        table.Rows.Select(row => row.Values[column]).Where(value => value.Length > 0)
            .ToHashSet(StringComparer.Ordinal);

    private string Namespace(
        string raw,
        ResolvedTenderReviewSource source,
        string columnName)
    {
        if (raw.Length == 0) return string.Empty;
        try
        {
            return TenderReviewNaming.NamespaceKey(
                raw, source.SourceToken, _request.ProjectCode, source.StatusDate);
        }
        catch (TenderReviewValidationException ex)
        {
            throw new TenderReviewValidationException(
                $"{source.OriginalXerFilename}/input-{source.InputIndex + 1}: cannot namespace '{columnName}'. {ex.Message}", ex);
        }
    }

    private static string ExtractNativeId(
        string value,
        string sourceToken,
        string columnName,
        string originalFilename)
    {
        if (string.IsNullOrWhiteSpace(value)) return string.Empty;
        string trimmed = value.Trim();
        string prefix = sourceToken + ".";
        if (trimmed.StartsWith(prefix, StringComparison.Ordinal)) return trimmed[prefix.Length..];
        if (!trimmed.Contains('.', StringComparison.Ordinal)) return trimmed;
        throw new TenderReviewValidationException(
            $"{originalFilename}: '{columnName}' key '{value}' has an unexpected Tender source-token prefix.");
    }

    private static void RequireRawHeader(XerTable table, string header)
    {
        if (!table.FieldIndexes.ContainsKey(header))
            throw new TenderReviewValidationException(
                $"Raw table '{table.Name}' is missing required field '{header}'.");
    }

    private IReadOnlyDictionary<string, string> Finalize(
        TenderReviewTableContract contract,
        Dictionary<string, string> values)
    {
        foreach (TenderReviewColumn column in contract.Columns)
        {
            values.TryGetValue(column.Name, out string? raw);
            values[column.Name] = string.IsNullOrWhiteSpace(raw) ? string.Empty
                : _quality.Evaluate(_outputEvidence[values], column.Name, raw,
                    () => TenderReviewCsv.Normalize(raw, column, contract.TableName));
        }
        return values;
    }

    private void WarnDuplicateKeys(TenderReviewOutputTable table, IReadOnlyList<string> columns)
    {
        foreach (var group in table.Rows.GroupBy(r => (r.Source.SourceToken,
            Key: string.Join("\u001f", columns.Select(c => r.Values[c])))))
        {
            bool missing = group.Any(r => columns.Any(c => string.IsNullOrWhiteSpace(r.Values[c])));
            if (!missing && group.Count() == 1) continue;
            foreach (TenderReviewOutputRow row in group)
                _quality.Warn(_outputEvidence[row.Values], "REVIEW_KEY_AMBIGUOUS",
                    "The review key is blank or duplicated; every source occurrence is preserved. Consumer key uniqueness must be repaired upstream.",
                    string.Join(',', columns), group.Key.Key);
        }
    }

    private void WarnMissingSource(string tableName, ResolvedTenderReviewSource source)
    {
        XerTable projects = _dataStore.GetTable(TableNames.Project)!;
        DataRow anchor = projects.Rows.First(r => r.SourceFilename == source.SourceToken);
        _quality.Rows.Add(XerDataQuality.CreateWarning(tableName, "REVIEW_SOURCE_UNAVAILABLE",
            $"No available rows for '{tableName}' in this stage; the numbered CSV retains its header.", anchor, 0));
    }

    internal static IReadOnlyList<TenderReviewOutputRow> Sort(
        TenderReviewTableContract contract,
        IReadOnlyList<TenderReviewOutputRow> rows) => rows
            .OrderBy(row => string.Join("\u001f",
                contract.SortColumns.Select(column => row.Values[column])), StringComparer.Ordinal)
            .ThenBy(row => string.Join("\u001f",
                contract.Columns.Select(column => row.Values[column.Name])), StringComparer.Ordinal)
            .ThenBy(row => row.Source.InputIndex)
            .ToArray();

    private static void ValidateDataDate(
        EnhancedRowReader reader,
        ResolvedTenderReviewSource source,
        string context,
        string sourceColumn = "data_date")
    {
        string raw = reader.GetOptional(
            sourceColumn, sourceColumn == "data_date" ? "Data Date" : sourceColumn);
        if (raw.Length == 0)
            throw new TenderReviewValidationException(
                $"{context}: XER source column '{sourceColumn}' is blank; status_date cannot replace a missing P6 Data Date.");
        string normalized = TenderReviewCsv.NormalizeDate(raw, context, sourceColumn);
        DateOnly dataDate = RequiredDataDate(source);
        if (!string.Equals(normalized, Iso(dataDate), StringComparison.Ordinal))
            throw new TenderReviewValidationException(
                $"{context}: resolved P6 data_date {dataDate:yyyy-MM-dd} does not match transformed {sourceColumn} {normalized}.");
    }

    private static DateOnly RequiredDataDate(ResolvedTenderReviewSource source) =>
        source.DataDate ?? throw new TenderReviewValidationException(
            $"{source.OriginalXerFilename}: P6 data_date was not resolved from PROJECT.last_recalc_date.");

    private static string Iso(DateOnly value) =>
        value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    private static string Raw(XerTable table, DataRow row, string fieldName) =>
        table.FieldIndexes.TryGetValue(fieldName, out int index)
            ? XerTable.GetFieldValueSafe(row, index)
            : string.Empty;

    private readonly record struct RawKey(string SourceToken, string NativeId);

    private sealed record RawTask(
        string SourceToken,
        string TaskId,
        string ProjectId,
        string CalendarId,
        string StatusCode,
        string ActualStartDate,
        string ActualEndDate,
        string RestartDate,
        string ReendDate,
        string EarlyStartDate,
        string EarlyEndDate,
        string RemainingDurationHours,
        string TotalFloatHours,
        string FreeFloatHours);

    private sealed record RawCalendar(string CalendarId, string DayHours);

    private sealed record RawRelationship(
        string RelationshipId,
        string SuccessorTaskId,
        string PredecessorTaskId,
        string SuccessorProjectId,
        string PredecessorProjectId,
        string LagHours);

    private sealed class EnhancedRowReader
    {
        private readonly XerTable _table;
        private readonly DataRow _row;

        internal EnhancedRowReader(XerTable table, DataRow row)
        {
            _table = table;
            _row = row;
        }

        internal string Get(TenderReviewColumn column) =>
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
