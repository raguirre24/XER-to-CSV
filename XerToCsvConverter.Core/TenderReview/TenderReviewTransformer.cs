using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Globalization;

namespace XerToCsvConverter.TenderReview;

internal sealed record TenderReviewTransformationResult(
    ResolvedTenderReviewRequest Request,
    IReadOnlyList<TenderReviewOutputTable> Tables);

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
    private IReadOnlyDictionary<RawKey, RawRelationship> _rawRelationships =
        new Dictionary<RawKey, RawRelationship>();
    private IReadOnlyDictionary<RawKey, string> _lagCalendarSettings =
        new Dictionary<RawKey, string>();

    internal TenderReviewTransformer(XerDataStore dataStore, ResolvedTenderReviewRequest request)
    {
        _dataStore = dataStore ?? throw new ArgumentNullException(nameof(dataStore));
        _request = request ?? throw new ArgumentNullException(nameof(request));
        _sourceByToken = CreateSourceLookup(request.Sources);
    }

    internal TenderReviewTransformationResult Build(CancellationToken cancellationToken)
    {
        _request = ResolveProjectMetadata(cancellationToken);
        _sourceByToken = CreateSourceLookup(_request.Sources);
        BuildRawSemanticLookups(cancellationToken);
        ValidateRawWbsParents(cancellationToken);

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
        ValidateOptionalTransformOutcomes(enhanced);

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
            if (contract.SourceRequired && source is null)
                throw new TenderReviewValidationException(
                    $"Required enhanced source table '{contract.EnhancedTableName}' could not be generated.");
            IReadOnlyList<TenderReviewOutputRow> rows = source is null
                ? Array.Empty<TenderReviewOutputRow>()
                : BuildProjectedRows(source, contract, cancellationToken);
            if (contract.TableName == "15_XER_RESOURCE_DISTRIBUTION")
                rows = AggregateResourceDistribution(contract, rows);
            tables.Add(new TenderReviewOutputTable(contract, Sort(contract, rows)));
        }

        ValidateRequiredSourceCoverage(tables);
        ValidateKeysAndRelationships(tables);
        return new TenderReviewTransformationResult(
            _request,
            new ReadOnlyCollection<TenderReviewOutputTable>(tables));
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
        _rawRelationships = BuildRawRelationshipLookup(cancellationToken);
        _lagCalendarSettings = BuildLagCalendarSettings(cancellationToken);
        ValidateRawTaskResourceReferences(cancellationToken);
    }

    private IReadOnlyDictionary<RawKey, RawTask> BuildRawTaskLookup(
        CancellationToken cancellationToken)
    {
        XerTable? table = _dataStore.GetTable(TableNames.Task);
        if (table?.Headers is null)
            throw new TenderReviewValidationException("Raw TASK data is required for every Tender source.");
        RequireRawHeader(table, FieldNames.TaskId);
        RequireRawHeader(table, FieldNames.ProjectId);
        RequireRawHeader(table, FieldNames.CalendarId);
        RequireRawHeader(table, FieldNames.StatusCode);

        var result = new Dictionary<RawKey, RawTask>();
        foreach (DataRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.ContainsKey(row.SourceFilename)) continue;
            string taskId = Raw(table, row, FieldNames.TaskId).Trim();
            if (taskId.Length == 0)
                throw new TenderReviewValidationException(
                    $"Tender source token '{row.SourceFilename}' contains a TASK row with blank task_id.");
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
            if (!result.TryAdd(new RawKey(row.SourceFilename, taskId), task))
                throw new TenderReviewValidationException(
                    $"Tender source token '{row.SourceFilename}' contains duplicate TASK.task_id '{taskId}'.");
        }
        return result;
    }

    private IReadOnlyDictionary<RawKey, RawCalendar> BuildRawCalendarLookup(
        CancellationToken cancellationToken)
    {
        XerTable? table = _dataStore.GetTable(TableNames.Calendar);
        if (table?.Headers is null)
            throw new TenderReviewValidationException("Raw CALENDAR data is required for every Tender source.");
        RequireRawHeader(table, FieldNames.ClndrId);

        var result = new Dictionary<RawKey, RawCalendar>();
        foreach (DataRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.ContainsKey(row.SourceFilename)) continue;
            string calendarId = Raw(table, row, FieldNames.ClndrId).Trim();
            if (calendarId.Length == 0)
                throw new TenderReviewValidationException(
                    $"Tender source token '{row.SourceFilename}' contains a CALENDAR row with blank clndr_id.");
            var calendar = new RawCalendar(calendarId, Raw(table, row, FieldNames.DayHourCount));
            if (!result.TryAdd(new RawKey(row.SourceFilename, calendarId), calendar))
                throw new TenderReviewValidationException(
                    $"Tender source token '{row.SourceFilename}' contains duplicate CALENDAR.clndr_id '{calendarId}'.");
        }
        return result;
    }

    private IReadOnlyDictionary<RawKey, RawRelationship> BuildRawRelationshipLookup(
        CancellationToken cancellationToken)
    {
        XerTable? table = _dataStore.GetTable(TableNames.TaskPred);
        if (table?.Headers is null) return new Dictionary<RawKey, RawRelationship>();
        RequireRawHeader(table, "task_pred_id");
        RequireRawHeader(table, FieldNames.TaskId);
        RequireRawHeader(table, FieldNames.PredTaskId);

        var result = new Dictionary<RawKey, RawRelationship>();
        foreach (DataRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.ContainsKey(row.SourceFilename)) continue;
            string relationshipId = Raw(table, row, "task_pred_id").Trim();
            if (relationshipId.Length == 0)
                throw new TenderReviewValidationException(
                    $"Tender source token '{row.SourceFilename}' contains a TASKPRED row with blank task_pred_id.");
            var relationship = new RawRelationship(
                relationshipId,
                Raw(table, row, FieldNames.TaskId).Trim(),
                Raw(table, row, FieldNames.PredTaskId).Trim(),
                Raw(table, row, FieldNames.ProjectId).Trim(),
                Raw(table, row, "pred_proj_id").Trim(),
                Raw(table, row, FieldNames.LagHrCnt));
            if (!result.TryAdd(new RawKey(row.SourceFilename, relationshipId), relationship))
                throw new TenderReviewValidationException(
                    $"Tender source token '{row.SourceFilename}' contains duplicate TASKPRED.task_pred_id '{relationshipId}'.");
            ResolvedTenderReviewSource source = _sourceByToken[row.SourceFilename];
            _ = RequireRelationshipTasks(relationship, source,
                $"06_XER_PREDECESSOR/{source.OriginalXerFilename}/TASKPRED '{relationshipId}'");
        }
        return result;
    }

    private IReadOnlyDictionary<RawKey, string> BuildLagCalendarSettings(
        CancellationToken cancellationToken)
    {
        XerTable? table = _dataStore.GetTable("SCHEDOPTIONS");
        if (table?.Headers is null
            || !table.FieldIndexes.ContainsKey(FieldNames.ProjectId)
            || !table.FieldIndexes.ContainsKey("sched_calendar_on_relationship_lag"))
            return new Dictionary<RawKey, string>();

        var result = new Dictionary<RawKey, string>();
        foreach (DataRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.ContainsKey(row.SourceFilename)) continue;
            string projectId = Raw(table, row, FieldNames.ProjectId).Trim();
            if (projectId.Length == 0) continue;
            string setting = Raw(table, row, "sched_calendar_on_relationship_lag").Trim();
            if (setting.Length == 0) setting = "rcal_Successor"; // P6's explicitly unselected default.
            if (!result.TryAdd(new RawKey(row.SourceFilename, projectId), setting))
                throw new TenderReviewValidationException(
                    $"Tender source token '{row.SourceFilename}' contains multiple SCHEDOPTIONS rows for proj_id '{projectId}'.");
        }
        return result;
    }

    private void ValidateRawTaskResourceReferences(CancellationToken cancellationToken)
    {
        XerTable? assignments = _dataStore.GetTable(TableNames.TaskRsrc);
        if (assignments?.Headers is null
            || !assignments.Rows.Any(row => _sourceByToken.ContainsKey(row.SourceFilename)))
            return;
        RequireRawHeader(assignments, FieldNames.TaskId);
        RequireRawHeader(assignments, FieldNames.RsrcId);

        XerTable? resources = _dataStore.GetTable(TableNames.Rsrc);
        if (resources?.Headers is null)
            throw new TenderReviewValidationException(
                "Raw RSRC data is required when Tender TASKRSRC rows are present.");
        RequireRawHeader(resources, FieldNames.RsrcId);
        HashSet<RawKey> resourceKeys = resources.Rows
            .Where(row => _sourceByToken.ContainsKey(row.SourceFilename))
            .Select(row => new RawKey(
                row.SourceFilename,
                Raw(resources, row, FieldNames.RsrcId).Trim()))
            .Where(key => key.NativeId.Length > 0)
            .ToHashSet();

        foreach (DataRow row in assignments.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.ContainsKey(row.SourceFilename)) continue;

            string taskId = Raw(assignments, row, FieldNames.TaskId).Trim();
            if (taskId.Length == 0)
                throw new TenderReviewValidationException(
                    $"Tender source token '{row.SourceFilename}' contains a TASKRSRC row with blank task_id.");
            if (!_rawTasks.ContainsKey(new RawKey(row.SourceFilename, taskId)))
                throw new TenderReviewValidationException(
                    $"Tender source token '{row.SourceFilename}' contains TASKRSRC.task_id '{taskId}' that does not resolve to a raw TASK row in the same source.");

            string resourceId = Raw(assignments, row, FieldNames.RsrcId).Trim();
            if (resourceId.Length == 0)
                throw new TenderReviewValidationException(
                    $"Tender source token '{row.SourceFilename}' contains a TASKRSRC row with blank rsrc_id.");
            if (!resourceKeys.Contains(new RawKey(row.SourceFilename, resourceId)))
                throw new TenderReviewValidationException(
                    $"Tender source token '{row.SourceFilename}' contains TASKRSRC.rsrc_id '{resourceId}' that does not resolve to a raw RSRC row in the same source.");
        }
    }

    private void ValidateRawWbsParents(CancellationToken cancellationToken)
    {
        XerTable? wbs = _dataStore.GetTable(TableNames.ProjWbs);
        if (wbs?.Headers is null)
            throw new TenderReviewValidationException("Raw PROJWBS data is required for every Tender source.");
        RequireRawHeader(wbs, FieldNames.WbsId);
        RequireRawHeader(wbs, FieldNames.ParentWbsId);

        int idIndex = wbs.FieldIndexes[FieldNames.WbsId];
        int parentIndex = wbs.FieldIndexes[FieldNames.ParentWbsId];
        foreach (ResolvedTenderReviewSource source in _request.Sources)
        {
            cancellationToken.ThrowIfCancellationRequested();
            DataRow[] sourceRows = wbs.Rows.Where(row =>
                string.Equals(row.SourceFilename, source.SourceToken, StringComparison.Ordinal)).ToArray();
            var ids = sourceRows.Select(row => XerTable.GetFieldValueSafe(row, idIndex).Trim())
                .Where(id => id.Length > 0)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            foreach (DataRow row in sourceRows)
            {
                string parent = XerTable.GetFieldValueSafe(row, parentIndex).Trim();
                if (parent.Length > 0 && !ids.Contains(parent))
                    throw new TenderReviewValidationException(
                        $"{source.OriginalXerFilename}: PROJWBS.parent_wbs_id contains an unresolved required relationship '{parent}'.");
            }
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
        RequireHeaders(source, contract.Columns.Where(column =>
            column.Name is not ("filename" or "ProjectCode" or "ProjectName" or "UpdateDate"
                or "status_date" or "monthupdate")));

        var rows = new List<TenderReviewOutputRow>();
        foreach (DataRow dataRow in source.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.TryGetValue(dataRow.SourceFilename, out ResolvedTenderReviewSource? tenderSource))
                continue;

            var reader = new EnhancedRowReader(source, dataRow);
            string context = $"{contract.TableName}/{tenderSource.OriginalXerFilename}/input-{tenderSource.InputIndex + 1}";
            string nativeTaskId = ExtractNativeId(
                reader.Get("task_id_key"), tenderSource.SourceToken, "task_id_key",
                tenderSource.OriginalXerFilename);
            if (!_rawTasks.TryGetValue(
                new RawKey(tenderSource.SourceToken, nativeTaskId), out RawTask? rawTask))
                throw new TenderReviewValidationException(
                    $"{context}: transformed TASK row '{nativeTaskId}' has no token-correlated raw TASK row.");

            string nativeProjectId = ExtractNativeId(
                reader.Get("proj_id_key"), tenderSource.SourceToken, "proj_id_key", tenderSource.OriginalXerFilename);
            if (!string.Equals(nativeProjectId, tenderSource.NativeProjectId, StringComparison.OrdinalIgnoreCase))
                throw new TenderReviewValidationException(
                    $"{context}: TASK proj_id '{nativeProjectId}' does not match the source PROJECT proj_id '{tenderSource.NativeProjectId}'.");

            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (TenderReviewColumn column in contract.Columns)
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
                values[column.Name] = TenderReviewCsv.Normalize(raw, column, context);
            }

            ValidateDataDate(reader, tenderSource, context);
            rows.Add(new TenderReviewOutputRow(tenderSource, Finalize(contract, values)));
        }

        foreach (ResolvedTenderReviewSource tenderSource in _request.Sources)
        {
            if (!rows.Any(row => ReferenceEquals(row.Source, tenderSource)))
                throw new TenderReviewValidationException(
                    $"No TASK rows were found for Tender input {tenderSource.InputIndex + 1} ('{tenderSource.OriginalXerFilename}').");
        }

        EnsureUnique(rows.Select(row => row.Values["task_id_key"]), "01_XER_TASK.task_id_key");
        EnsureUnique(rows.Select(row =>
            $"{row.Source.CanonicalXerFilename}\u001f{row.Values["task_code"]}"),
            "01_XER_TASK.(filename,task_code)");
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
        RequireHeaders(source, contract.Columns.Where(column => column.Name is not
            ("ProjectCode" or "monthupdate" or "last_recalc_date" or "udf_datalake_status_date"
                or "add_date" or "state" or "region" or "tender_status")));
        var rows = new List<TenderReviewOutputRow>();

        foreach (DataRow dataRow in source.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_sourceByToken.TryGetValue(dataRow.SourceFilename, out ResolvedTenderReviewSource? tenderSource))
                continue;
            var reader = new EnhancedRowReader(source, dataRow);
            if (RequiresSelectedProject(contract.TableName)
                && !BelongsToSelectedProject(reader, tenderSource))
                continue;

            string context = $"{contract.TableName}/{tenderSource.OriginalXerFilename}/input-{tenderSource.InputIndex + 1}";
            RawRelationship? relationship = contract.TableName == "06_XER_PREDECESSOR"
                ? RequireRawRelationship(reader, tenderSource, context)
                : null;
            if (relationship is not null)
                ValidateLagCalendarSetting(relationship, tenderSource, context);
            if (contract.TableName == "15_XER_RESOURCE_DISTRIBUTION")
                ValidateResourceDistributionCalendar(reader, tenderSource, context);
            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (TenderReviewColumn column in contract.Columns)
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

                values[column.Name] = TenderReviewCsv.Normalize(raw, column, context);
            }

            if (contract.TableName == "02_XER_PROJECT")
                ValidateDataDate(reader, tenderSource, context, "last_recalc_date");
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

    private RawRelationship RequireRawRelationship(
        EnhancedRowReader reader,
        ResolvedTenderReviewSource source,
        string context)
    {
        string nativeId = ExtractNativeId(
            reader.Get("task_pred_id_key", "task_pred_id"),
            source.SourceToken,
            "task_pred_id_key",
            source.OriginalXerFilename);
        if (!_rawRelationships.TryGetValue(
            new RawKey(source.SourceToken, nativeId), out RawRelationship? relationship))
            throw new TenderReviewValidationException(
                $"{context}: transformed TASKPRED row '{nativeId}' has no token-correlated raw TASKPRED row.");
        return relationship;
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
        _ = ResolveLagCalendarKind(
            relationship, successor, source, context, requireExplicit: true);
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
        _ = ResolveLagCalendarKind(
            relationship, successor, source, context, requireExplicit: lagHours != 0m);
        return inheritedValue;
    }

    private void ValidateLagCalendarSetting(
        RawRelationship relationship,
        ResolvedTenderReviewSource source,
        string context)
    {
        (RawTask successor, RawTask predecessor) = RequireRelationshipTasks(relationship, source, context);
        decimal lagHours = string.IsNullOrWhiteSpace(relationship.LagHours)
            ? 0m
            : ParseHours(relationship.LagHours, FieldNames.LagHrCnt, context);
        RelationshipLagCalendar kind = ResolveLagCalendarKind(
            relationship, successor, source, context, requireExplicit: lagHours != 0m);
        if (lagHours == 0m || kind == RelationshipLagCalendar.TwentyFourHour) return;
        string calendarId = kind == RelationshipLagCalendar.Successor ? successor.CalendarId : predecessor.CalendarId;
        if (kind == RelationshipLagCalendar.ProjectDefault)
        {
            XerTable? projects = _dataStore.GetTable(TableNames.Project);
            DataRow? project = projects?.Rows.Cast<DataRow?>().FirstOrDefault(row => row.HasValue
                && row.Value.SourceFilename == source.SourceToken
                && Raw(projects, row.Value, FieldNames.ProjectId) == successor.ProjectId);
            calendarId = project.HasValue ? Raw(projects!, project.Value, FieldNames.ClndrId) : string.Empty;
            if (string.IsNullOrWhiteSpace(calendarId))
                throw new TenderReviewValidationException($"{context}: project lag scheduling requires PROJECT.clndr_id.");
        }
        try
        {
            _calendarRepository ??= new P6CalendarRepository(_dataStore);
            _ = _calendarRepository.Get(source.SourceToken, calendarId);
        }
        catch (InvalidDataException ex)
        {
            throw new TenderReviewValidationException($"{context}: invalid relationship lag calendar: {ex.Message}");
        }
    }

    private P6CalendarRepository? _calendarRepository;

    private RelationshipLagCalendar ResolveLagCalendarKind(
        RawRelationship relationship,
        RawTask successor,
        ResolvedTenderReviewSource source,
        string context,
        bool requireExplicit)
    {
        _lagCalendarSettings.TryGetValue(
            new RawKey(source.SourceToken, successor.ProjectId), out string? setting);
        if (string.IsNullOrWhiteSpace(setting))
        {
            if (requireExplicit)
                throw new TenderReviewValidationException(
                    $"{context}: non-zero TASKPRED.lag_hr_cnt for relationship '{relationship.RelationshipId}' requires exported SCHEDOPTIONS.sched_calendar_on_relationship_lag metadata.");
            return RelationshipLagCalendar.Successor;
        }

        if (RelationshipLagCalendarPolicy.TryParse(setting, out RelationshipLagCalendar kind))
            return kind;
        throw new TenderReviewValidationException(
            $"{context}: SCHEDOPTIONS.sched_calendar_on_relationship_lag '{setting}' is not recognised; expected rcal_Predecessor, rcal_Successor, rcal_24Hour or rcal_ProjDefault (rcal_Project is also accepted)." );
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

    private void ValidateResourceDistributionCalendar(
        EnhancedRowReader reader,
        ResolvedTenderReviewSource source,
        string context)
    {
        string nativeTaskId = ExtractNativeId(
            reader.Get("task_id_key"), source.SourceToken, "task_id_key",
            source.OriginalXerFilename);
        if (!_rawTasks.TryGetValue(
            new RawKey(source.SourceToken, nativeTaskId), out RawTask? task))
            throw new TenderReviewValidationException(
                $"{context}: TASKRSRC.task_id '{nativeTaskId}' is an unresolved required relationship.");
        string selectedCalendarId = ExtractNativeId(
            reader.Get(FieldNames.ClndrIdKey), source.SourceToken, FieldNames.ClndrIdKey,
            source.OriginalXerFilename);
        _ = RequireHoursPerDay(source.SourceToken, selectedCalendarId,
            "resource distribution calendar", "resource distribution", context);
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
        IReadOnlyList<TenderReviewOutputRow> rows)
    {
        if (rows.Count < 2) return rows;
        string[] grain = { "task_id_key", "rsrc_id_key", "is_actual", "distribution_month" };
        var aggregated = new List<TenderReviewOutputRow>();
        foreach (IGrouping<string, TenderReviewOutputRow> group in rows.GroupBy(
            row => string.Join("\u001f", grain.Select(column => row.Values[column])),
            StringComparer.Ordinal))
        {
            TenderReviewOutputRow first = group.First();
            var values = new Dictionary<string, string>(first.Values, StringComparer.OrdinalIgnoreCase);
            decimal total = 0m;
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
                        throw new TenderReviewValidationException(
                            $"15_XER_RESOURCE_DISTRIBUTION contains conflicting '{column.Name}' values at its output grain.");
                }
                if (!decimal.TryParse(row.Values["monthly_quantity"], NumberStyles.Float,
                    CultureInfo.InvariantCulture, out decimal quantity))
                    throw new TenderReviewValidationException(
                        "15_XER_RESOURCE_DISTRIBUTION.monthly_quantity is not an invariant number.");
                total += quantity;
            }
            values["monthly_quantity"] = total.ToString("G29", CultureInfo.InvariantCulture);
            aggregated.Add(new TenderReviewOutputRow(first.Source, values));
        }
        return aggregated;
    }

    private void ValidateOptionalTransformOutcomes(IReadOnlyDictionary<string, XerTable?> enhanced)
    {
        var rawSourceByEnhanced = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [EnhancedTableNames.XerPredecessor06] = TableNames.TaskPred,
            [EnhancedTableNames.XerActvType07] = TableNames.ActvType,
            [EnhancedTableNames.XerActvCode08] = TableNames.ActvCode,
            [EnhancedTableNames.XerTaskActv09] = TableNames.TaskActv,
            [EnhancedTableNames.XerRsrc12] = TableNames.Rsrc,
            [EnhancedTableNames.XerResourceDist15] = TableNames.TaskRsrc
        };

        foreach ((string enhancedName, string rawName) in rawSourceByEnhanced)
        {
            if (enhanced.TryGetValue(enhancedName, out XerTable? transformed) && transformed is not null)
                continue;
            XerTable? raw = _dataStore.GetTable(rawName);
            bool retainedRawRows = raw?.Rows.Any(row => _sourceByToken.ContainsKey(row.SourceFilename)) == true;
            if (retainedRawRows)
                throw new TenderReviewValidationException(
                    $"Enhanced transformation '{enhancedName}' failed even though raw table '{rawName}' contains Tender rows.");
        }
    }

    private void ValidateRequiredSourceCoverage(IReadOnlyList<TenderReviewOutputTable> tables)
    {
        foreach (TenderReviewOutputTable table in tables.Where(table => table.Contract.SourceRequired))
        {
            foreach (ResolvedTenderReviewSource source in _request.Sources)
            {
                if (!table.Rows.Any(row => ReferenceEquals(row.Source, source)))
                    throw new TenderReviewValidationException(
                        $"Required table '{table.Contract.TableName}' has no rows for Tender input {source.InputIndex + 1} ('{source.OriginalXerFilename}').");
            }
        }
    }

    private void ValidateKeysAndRelationships(IReadOnlyList<TenderReviewOutputTable> tables)
    {
        var byName = tables.ToDictionary(table => table.Contract.TableName, StringComparer.OrdinalIgnoreCase);
        foreach (TenderReviewOutputTable table in tables)
        {
            if (table.Contract.KeyColumns.Count > 0)
                EnsureUnique(table.Rows.Select(row => string.Join("\u001f",
                    table.Contract.KeyColumns.Select(column => row.Values[column]))),
                    $"{table.Contract.TableName}.({string.Join(',', table.Contract.KeyColumns)})");

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

    private static void RequireReferences(
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
                throw new TenderReviewValidationException(
                    $"{table.Contract.TableName}.{column} contains an unresolved required relationship '{value}'.");
        }
    }

    private static HashSet<string> Values(TenderReviewOutputTable table, string column) =>
        table.Rows.Select(row => row.Values[column]).Where(value => value.Length > 0)
            .ToHashSet(StringComparer.Ordinal);

    private static void EnsureUnique(IEnumerable<string> values, string context)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (string value in values)
        {
            if (value.Length == 0 || !seen.Add(value))
                throw new TenderReviewValidationException(
                    $"Duplicate or blank key detected for {context}: '{value}'.");
        }
    }

    private bool BelongsToSelectedProject(
        EnhancedRowReader reader,
        ResolvedTenderReviewSource source)
    {
        string raw = reader.GetOptional("proj_id_key", "proj_id");
        if (raw.Length == 0) return true;
        string native = ExtractNativeId(
            raw, source.SourceToken, "proj_id", source.OriginalXerFilename);
        return string.Equals(native, source.NativeProjectId, StringComparison.OrdinalIgnoreCase);
    }

    private static bool RequiresSelectedProject(string tableName) => tableName is
        "02_XER_PROJECT" or "03_XER_PROJWBS" or "06_XER_PREDECESSOR"
        or "09_XER_TASKACTV" or "15_XER_RESOURCE_DISTRIBUTION";

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

    private static void RequireHeaders(XerTable table, IEnumerable<TenderReviewColumn> columns)
    {
        foreach (TenderReviewColumn column in columns)
        {
            if (table.FieldIndexes.ContainsKey(column.Name)) continue;
            if (column.SourceAliases.Any(table.FieldIndexes.ContainsKey)) continue;
            throw new TenderReviewValidationException(
                $"Enhanced table '{table.Name}' is missing required source column '{column.Name}' (aliases: {string.Join(", ", column.SourceAliases)}).");
        }
    }

    private static void RequireRawHeader(XerTable table, string header)
    {
        if (!table.FieldIndexes.ContainsKey(header))
            throw new TenderReviewValidationException(
                $"Raw table '{table.Name}' is missing required field '{header}'.");
    }

    private static IReadOnlyDictionary<string, string> Finalize(
        TenderReviewTableContract contract,
        Dictionary<string, string> values)
    {
        foreach (TenderReviewColumn column in contract.Columns)
        {
            values.TryGetValue(column.Name, out string? raw);
            values[column.Name] = TenderReviewCsv.Normalize(raw, column, contract.TableName);
        }
        return values;
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
