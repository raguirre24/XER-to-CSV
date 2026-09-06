namespace XerToCsvConverter;

public partial class XerTransformer
{
    private XerTable? Create03XerProjWbsTableCore()
    {
        ClearGenerationFailure(EnhancedTableNames.XerProjWbs03);
        XerTable? source = _dataStore.GetTable(TableNames.ProjWbs);
        if (!IsTableValid(source)) return null;
        string Read(DataRow row, string field) => GetFieldValue(row.Fields, source.FieldIndexes, field).Trim();
        var nodes = source.Rows.GroupBy(row => (Source: row.SourceToken, Id: Read(row, FieldNames.WbsId)))
            .ToDictionary(group => group.Key, group => group.ToArray());
        var parents = new Dictionary<(string Source, string Id), (string Source, string Id)>();
        foreach (var pair in nodes.Where(pair => pair.Key.Id.Length > 0 && pair.Value.Length == 1))
        {
            DataRow row = pair.Value[0];
            string parentId = Read(row, FieldNames.ParentWbsId);
            var parentKey = (pair.Key.Source, parentId);
            if (parentId.Length == 0 || !nodes.TryGetValue(parentKey, out var parentRows) || parentRows.Length != 1) continue;
            if (Read(row, FieldNames.ProjectId) != Read(parentRows[0], FieldNames.ProjectId)) continue;
            parents.Add(pair.Key, parentKey);
        }
        var cyclic = new HashSet<(string Source, string Id)>();
        var complete = new HashSet<(string Source, string Id)>();
        foreach (var key in parents.Keys)
        {
            var path = new List<(string Source, string Id)>();
            var positions = new Dictionary<(string Source, string Id), int>();
            var current = key;
            while (!complete.Contains(current))
            {
                if (positions.TryGetValue(current, out int cycleStart))
                {
                    cyclic.UnionWith(path.Skip(cycleStart));
                    break;
                }
                positions.Add(current, path.Count);
                path.Add(current);
                if (!parents.TryGetValue(current, out current)) break;
            }
            complete.UnionWith(path);
        }
        string[] headers = CreateEnhancedHeaders(source, EnhancedTableNames.XerProjWbs03,
            new[] { FieldNames.WbsIdKey, FieldNames.ParentWbsIdKey, FieldNames.MonthUpdate });
        var result = new XerTable(EnhancedTableNames.XerProjWbs03, source.RowCount);
        result.SetHeaders(headers);
        var ordinals = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (DataRow row in source.Rows)
        {
            int ordinal = ordinals[row.SourceToken] = ordinals.GetValueOrDefault(row.SourceToken) + 1;
            string id = Read(row, FieldNames.WbsId);
            string parentId = Read(row, FieldNames.ParentWbsId);
            var key = (Source: row.SourceToken, Id: id);
            string? issue = null;
            string? message = null;
            bool usableId = id.Length > 0 && nodes[key].Length == 1;
            bool usableParent = parents.TryGetValue(key, out var parent);
            if (!usableId)
            {
                issue = "WBS_IDENTITY_INVALID";
                message = id.Length == 0 ? "PROJWBS.wbs_id is blank; its derived identity and parent key are unknown."
                    : $"PROJWBS.wbs_id '{id}' is duplicated within this input; competing rows are retained without selecting an identity.";
                usableParent = false;
            }
            else if (cyclic.Contains(key))
            {
                issue = "WBS_PARENT_CYCLE";
                message = $"WBS '{id}' participates in a parent cycle; its parent key is blank.";
                usableParent = false;
            }
            else if (parentId.Length > 0 && !usableParent)
            {
                issue = "WBS_PARENT_UNRESOLVED";
                message = $"WBS '{id}' parent '{parentId}' is absent, ambiguous or belongs to another project; its parent key is blank.";
            }
            string[] values = new string[headers.Length];
            Array.Copy(row.Fields, values, Math.Min(row.Fields.Length, source.Headers!.Length));
            values[^3] = usableId ? CreateKey(row.SourceFilename, id) : "";
            values[^2] = usableParent ? CreateKey(row.SourceFilename, parent.Id) : "";
            values[^1] = ParseMonthUpdateFromFilename(row.OriginalSourceFilename);
            result.AddRow(row.WithFields(values));
            if (issue is not null)
                RecordDataQualityWarning(EnhancedTableNames.XerProjWbs03, issue, message!, row,
                    ordinal, TableNames.ProjWbs, usableId ? FieldNames.ParentWbsId : FieldNames.WbsId,
                    usableId ? parentId : id, XerDataQuality.RawRowJson(source, row));
        }
        return result;
    }
}
