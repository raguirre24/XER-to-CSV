using System.Collections.ObjectModel;

namespace XerToCsvConverter.TenderReview;

public enum TenderReviewColumnType
{
    Text,
    Date,
    Number,
    Integer,
    Boolean
}

public sealed record TenderReviewColumn(
    string Name,
    TenderReviewColumnType Type,
    bool Nullable = true,
    params string[] SourceAliases);

public sealed record TenderReviewTableContract(
    string TableName,
    string FileName,
    string EnhancedTableName,
    bool SourceRequired,
    IReadOnlyList<TenderReviewColumn> Columns,
    IReadOnlyList<string> KeyColumns,
    IReadOnlyList<string> SortColumns);

/// <summary>Versioned, ordered schema for the independent Tender Review export profile.</summary>
public static class TenderReviewContract
{
    public const string SchemaVersion = "1.0";
    public const string BundleProfile = "tender_review";
    public const string ManifestFileName = "XER_CSV_MANIFEST.csv";
    public const string CompleteStatus = "COMPLETE";

    public static IReadOnlyList<string> ManifestColumns { get; } = new ReadOnlyCollection<string>(new[]
    {
        "schema_version", "bundle_profile", "bundle_id", "bundle_status", "parser_version",
        "project_code", "project_name", "original_xer_filename", "canonical_xer_filename",
        "status_date", "update_date", "data_date", "source_sha256", "table_name", "row_count",
        "csv_sha256", "exported_at_utc"
    });

    private static TenderReviewColumn C(
        string name,
        TenderReviewColumnType type = TenderReviewColumnType.Text,
        bool nullable = true,
        params string[] aliases) => new(name, type, nullable, aliases);

    private static readonly IReadOnlyList<TenderReviewTableContract> ContractTables =
        new ReadOnlyCollection<TenderReviewTableContract>(new[]
        {
            new TenderReviewTableContract(
                "01_XER_TASK", "01_XER_TASK.csv", EnhancedTableNames.XerTask01, true,
                new[]
                {
                    C("status_code", nullable: false),
                    C("task_code", nullable: false),
                    C("total_float", TenderReviewColumnType.Number),
                    C("task_type", nullable: false),
                    C("id_name", TenderReviewColumnType.Text, false, "ID_Name"),
                    C("early_start_date", TenderReviewColumnType.Date),
                    C("calendar_id_key", nullable: false),
                    C("task_id_key", nullable: false),
                    C("driving_path_flag"),
                    C("remaining_duration", TenderReviewColumnType.Number, true, "Remaining Duration", "remainingduration"),
                    C("early_end_date", TenderReviewColumnType.Date),
                    C("monthupdate", TenderReviewColumnType.Date, false, "MonthUpdate"),
                    C("status_date", TenderReviewColumnType.Date, false),
                    C("task_name", nullable: false),
                    C("data_date", TenderReviewColumnType.Date, false, "Data Date"),
                    C("act_end_date", TenderReviewColumnType.Date),
                    C("Finish", TenderReviewColumnType.Date),
                    C("proj_id_key", nullable: false),
                    C("wbs_id_key", nullable: false),
                    C("free_float", TenderReviewColumnType.Number, true, "Free Float"),
                    C("cstr_type"),
                    C("Start", TenderReviewColumnType.Date),
                    C("filename", TenderReviewColumnType.Text, false, "FileName"),
                    C("late_end_date", TenderReviewColumnType.Date),
                    C("ProjectCode", nullable: false),
                    C("UpdateDate", TenderReviewColumnType.Date, false),
                    C("ProjectName", nullable: false)
                },
                new[] { "task_id_key" },
                new[] { "status_date", "task_code", "task_id_key" }),

            new TenderReviewTableContract(
                "02_XER_PROJECT", "02_XER_PROJECT.csv", EnhancedTableNames.XerProject02, true,
                new[]
                {
                    C("last_recalc_date", TenderReviewColumnType.Date, false),
                    C("proj_id_key", nullable: false),
                    C("monthupdate", TenderReviewColumnType.Date, false, "MonthUpdate"),
                    C("ProjectCode", nullable: false),
                    C("add_date", TenderReviewColumnType.Date),
                    C("state"),
                    C("region"),
                    C("tender_status"),
                    C("udf_datalake_status_date", TenderReviewColumnType.Date, false)
                },
                new[] { "proj_id_key" },
                new[] { "monthupdate", "proj_id_key" }),

            new TenderReviewTableContract(
                "03_XER_PROJWBS", "03_XER_PROJWBS.csv", EnhancedTableNames.XerProjWbs03, true,
                new[]
                {
                    C("wbs_name", nullable: false),
                    C("wbs_id_key", nullable: false),
                    C("parent_wbs_id_key"),
                    C("ProjectCode", nullable: false)
                },
                new[] { "wbs_id_key" }, new[] { "wbs_id_key" }),

            new TenderReviewTableContract(
                "06_XER_PREDECESSOR", "06_XER_PREDECESSOR.csv", EnhancedTableNames.XerPredecessor06, false,
                new[]
                {
                    C("task_id_key", nullable: false),
                    C("pred_type", nullable: false),
                    C("predecessor_status_code"),
                    C("task_type"),
                    C("predecessor_task_type"),
                    C("lag", TenderReviewColumnType.Number),
                    C("start", TenderReviewColumnType.Date, true, "Start"),
                    C("finish", TenderReviewColumnType.Date, true, "Finish"),
                    C("predecessor_start", TenderReviewColumnType.Date),
                    C("predecessor_finish", TenderReviewColumnType.Date),
                    C("free_float", TenderReviewColumnType.Number, true, "Free Float"),
                    C("pred_task_id_key", nullable: false),
                    C("status_code"),
                    C("total_float", TenderReviewColumnType.Number),
                    C("task_pred_id_key", TenderReviewColumnType.Text, false, "task_pred_id"),
                    C("ProjectCode", nullable: false)
                },
                new[] { "task_pred_id_key" },
                new[] { "task_id_key", "pred_task_id_key", "task_pred_id_key" }),

            new TenderReviewTableContract(
                "07_XER_ACTVTYPE", "07_XER_ACTVTYPE.csv", EnhancedTableNames.XerActvType07, false,
                new[] { C("actv_code_type_id_key", nullable: false), C("actv_code_type", nullable: false) },
                new[] { "actv_code_type_id_key" }, new[] { "actv_code_type_id_key" }),

            new TenderReviewTableContract(
                "08_XER_ACTVCODE", "08_XER_ACTVCODE.csv", EnhancedTableNames.XerActvCode08, false,
                new[]
                {
                    C("actv_code_id_key", nullable: false),
                    C("actv_code_name", nullable: false),
                    C("actv_code_type_id_key", nullable: false)
                },
                new[] { "actv_code_id_key" }, new[] { "actv_code_id_key" }),

            new TenderReviewTableContract(
                "09_XER_TASKACTV", "09_XER_TASKACTV.csv", EnhancedTableNames.XerTaskActv09, false,
                new[] { C("task_id_key", nullable: false), C("actv_code_id_key", nullable: false) },
                new[] { "task_id_key", "actv_code_id_key" }, new[] { "task_id_key", "actv_code_id_key" }),

            new TenderReviewTableContract(
                "10_XER_CALENDAR", "10_XER_CALENDAR.csv", EnhancedTableNames.XerCalendar10, true,
                new[] { C("clndr_id_key", nullable: false), C("clndr_name", nullable: false) },
                new[] { "clndr_id_key" }, new[] { "clndr_id_key" }),

            new TenderReviewTableContract(
                "12_XER_RSRC", "12_XER_RSRC.csv", EnhancedTableNames.XerRsrc12, false,
                new[] { C("rsrc_id_key", nullable: false), C("def_qty_per_hr") },
                new[] { "rsrc_id_key" }, new[] { "rsrc_id_key" }),

            new TenderReviewTableContract(
                "15_XER_RESOURCE_DISTRIBUTION", "15_XER_RESOURCE_DISTRIBUTION.csv", EnhancedTableNames.XerResourceDist15, false,
                new[]
                {
                    C("task_id_key", nullable: false),
                    C("rsrc_id_key", nullable: false),
                    C("is_actual", TenderReviewColumnType.Boolean, false),
                    C("distribution_month", TenderReviewColumnType.Date, false),
                    C("monthly_quantity", TenderReviewColumnType.Number, false),
                    C("rsrc_name"),
                    C("rsrc_type"),
                    C("unit", TenderReviewColumnType.Text, true, "Unit"),
                    C("ProjectCode", nullable: false)
                },
                new[] { "task_id_key", "rsrc_id_key", "is_actual", "distribution_month" },
                new[] { "task_id_key", "rsrc_id_key", "distribution_month", "is_actual" })
        });

    public static IReadOnlyList<TenderReviewTableContract> Tables => ContractTables;

    public static TenderReviewTableContract GetTable(string tableName) =>
        ContractTables.FirstOrDefault(table =>
            string.Equals(table.TableName, tableName, StringComparison.OrdinalIgnoreCase))
        ?? throw new KeyNotFoundException($"Unknown Tender Review table '{tableName}'.");
}
