using System.Collections.ObjectModel;

namespace XerToCsvConverter.ProgrammeReview;

/// <summary>The logical CSV types accepted by the Programme Review Power Query loader.</summary>
public enum ProgrammeReviewColumnType
{
    Text,
    Date,
    Number,
    Integer,
    Boolean
}

public sealed record ProgrammeReviewColumn(
    string Name,
    ProgrammeReviewColumnType Type,
    bool Nullable = true,
    params string[] SourceAliases);

public sealed record ProgrammeReviewTableContract(
    string TableName,
    string FileName,
    string EnhancedTableName,
    bool SourceRequired,
    IReadOnlyList<ProgrammeReviewColumn> Columns,
    IReadOnlyList<string> KeyColumns,
    IReadOnlyList<string> SortColumns);

/// <summary>
/// Versioned, ordered schema shared by the Programme Review parser profile and its Power BI loader.
/// The legacy Enhanced Power BI export does not use or mutate this contract.
/// </summary>
public static class ProgrammeReviewContract
{
    public const string SchemaVersion = "1.0";
    public const string ManifestFileName = "XER_CSV_MANIFEST.csv";
    public const string CompleteStatus = "complete";

    private static ProgrammeReviewColumn C(
        string name,
        ProgrammeReviewColumnType type = ProgrammeReviewColumnType.Text,
        bool nullable = true,
        params string[] aliases) => new(name, type, nullable, aliases);

    private static readonly IReadOnlyList<ProgrammeReviewTableContract> ContractTables =
        new ReadOnlyCollection<ProgrammeReviewTableContract>(new[]
        {
            new ProgrammeReviewTableContract(
                "01_XER_TASK", "01_XER_TASK.csv", EnhancedTableNames.XerTask01, true,
                new[]
                {
                    C("status_code", nullable: false),
                    C("task_code", nullable: false),
                    C("total_float", ProgrammeReviewColumnType.Number),
                    C("task_type", nullable: false),
                    C("id_name", ProgrammeReviewColumnType.Text, false, "ID_Name"),
                    C("early_start_date", ProgrammeReviewColumnType.Date),
                    C("calendar_id_key", nullable: false),
                    C("task_id_key", nullable: false),
                    C("driving_path_flag"),
                    C("remaining_duration", ProgrammeReviewColumnType.Number, true, "Remaining Duration", "remainingduration"),
                    C("early_end_date", ProgrammeReviewColumnType.Date),
                    C("monthupdate", ProgrammeReviewColumnType.Date, false, "MonthUpdate"),
                    C("task_name", nullable: false),
                    C("data_date", ProgrammeReviewColumnType.Date, false, "Data Date"),
                    C("act_end_date", ProgrammeReviewColumnType.Date),
                    C("Finish", ProgrammeReviewColumnType.Date),
                    C("proj_id_key", nullable: false),
                    C("wbs_id_key", nullable: false),
                    C("free_float", ProgrammeReviewColumnType.Number, true, "Free Float"),
                    C("cstr_type"),
                    C("Start", ProgrammeReviewColumnType.Date),
                    C("filename", ProgrammeReviewColumnType.Text, false, "FileName"),
                    C("late_end_date", ProgrammeReviewColumnType.Date),
                    C("ProjectCode", nullable: false),
                    C("UpdateDate", ProgrammeReviewColumnType.Date, false),
                    C("ProjectName", nullable: false),
                    C("Finish_Variance_Previous_Month", ProgrammeReviewColumnType.Integer, false),
                    C("Driven_DataDate", nullable: false),
                    C("Variance_Finish_BL", ProgrammeReviewColumnType.Integer),
                    C("Variance_Finish_Adjusted_BL", ProgrammeReviewColumnType.Integer),
                    C("Baseline Finish", ProgrammeReviewColumnType.Date),
                    C("Baseline Start", ProgrammeReviewColumnType.Date),
                    C("Previous Month Start", ProgrammeReviewColumnType.Date),
                    C("Previous Month Finish", ProgrammeReviewColumnType.Date),
                    C("Planned Not Completed Last Period", ProgrammeReviewColumnType.Integer, false),
                    C("Start_Variance_Previous_Month", ProgrammeReviewColumnType.Integer, false),
                    C("PreviousDataDate", ProgrammeReviewColumnType.Date),
                    C("Previous Remaining Working Days", ProgrammeReviewColumnType.Number),
                    C("Planned Last Period", ProgrammeReviewColumnType.Integer, false),
                    C("Completed Last Period", ProgrammeReviewColumnType.Integer, false),
                    C("Completed of Planned Last Period", ProgrammeReviewColumnType.Integer, false),
                    C("Baseline Effective_Early_End", ProgrammeReviewColumnType.Date),
                    C("Baseline Effective_Late_End", ProgrammeReviewColumnType.Date),
                    C("Adjusted Baseline Finish", ProgrammeReviewColumnType.Date),
                    C("Adjusted Baseline Start", ProgrammeReviewColumnType.Date),
                    C("Adjusted Baseline Source Month", ProgrammeReviewColumnType.Date),
                    C("Adjusted Baseline Source")
                },
                new[] { "task_id_key" },
                new[] { "UpdateDate", "task_code", "task_id_key" }),

            new ProgrammeReviewTableContract(
                "02_XER_PROJECT", "02_XER_PROJECT.csv", EnhancedTableNames.XerProject02, true,
                new[]
                {
                    C("last_recalc_date", ProgrammeReviewColumnType.Date, false),
                    C("proj_id_key", nullable: false),
                    C("monthupdate", ProgrammeReviewColumnType.Date, false, "MonthUpdate"),
                    C("ProjectCode", nullable: false)
                },
                new[] { "proj_id_key" }, new[] { "monthupdate", "proj_id_key" }),

            new ProgrammeReviewTableContract(
                "03_XER_PROJWBS", "03_XER_PROJWBS.csv", EnhancedTableNames.XerProjWbs03, true,
                new[]
                {
                    C("wbs_name", nullable: false),
                    C("wbs_id_key", nullable: false),
                    C("parent_wbs_id_key"),
                    C("ProjectCode", nullable: false)
                },
                new[] { "wbs_id_key" }, new[] { "wbs_id_key" }),

            new ProgrammeReviewTableContract(
                "06_XER_PREDECESSOR", "06_XER_PREDECESSOR.csv", EnhancedTableNames.XerPredecessor06, false,
                new[]
                {
                    C("task_id_key", nullable: false),
                    C("pred_type", nullable: false),
                    C("predecessor_status_code"),
                    C("task_type"),
                    C("predecessor_task_type"),
                    C("lag", ProgrammeReviewColumnType.Number),
                    C("start", ProgrammeReviewColumnType.Date, true, "Start"),
                    C("finish", ProgrammeReviewColumnType.Date, true, "Finish"),
                    C("predecessor_start", ProgrammeReviewColumnType.Date),
                    C("predecessor_finish", ProgrammeReviewColumnType.Date),
                    C("free_float", ProgrammeReviewColumnType.Number, true, "Free Float"),
                    C("pred_task_id_key", nullable: false),
                    C("status_code"),
                    C("total_float", ProgrammeReviewColumnType.Number),
                    C("task_pred_id_key", ProgrammeReviewColumnType.Text, false, "task_pred_id"),
                    C("ProjectCode", nullable: false)
                },
                new[] { "task_pred_id_key" }, new[] { "task_id_key", "pred_task_id_key", "task_pred_id_key" }),

            new ProgrammeReviewTableContract(
                "07_XER_ACTVTYPE", "07_XER_ACTVTYPE.csv", EnhancedTableNames.XerActvType07, false,
                new[] { C("actv_code_type_id_key", nullable: false), C("actv_code_type", nullable: false) },
                new[] { "actv_code_type_id_key" }, new[] { "actv_code_type_id_key" }),

            new ProgrammeReviewTableContract(
                "08_XER_ACTVCODE", "08_XER_ACTVCODE.csv", EnhancedTableNames.XerActvCode08, false,
                new[]
                {
                    C("actv_code_id_key", nullable: false),
                    C("actv_code_name", nullable: false),
                    C("actv_code_type_id_key", nullable: false)
                },
                new[] { "actv_code_id_key" }, new[] { "actv_code_id_key" }),

            new ProgrammeReviewTableContract(
                "09_XER_TASKACTV", "09_XER_TASKACTV.csv", EnhancedTableNames.XerTaskActv09, false,
                new[] { C("task_id_key", nullable: false), C("actv_code_id_key", nullable: false) },
                new[] { "task_id_key", "actv_code_id_key" }, new[] { "task_id_key", "actv_code_id_key" }),

            new ProgrammeReviewTableContract(
                "10_XER_CALENDAR", "10_XER_CALENDAR.csv", EnhancedTableNames.XerCalendar10, true,
                new[] { C("clndr_id_key", nullable: false), C("clndr_name", nullable: false) },
                new[] { "clndr_id_key" }, new[] { "clndr_id_key" }),

            new ProgrammeReviewTableContract(
                "12_XER_RSRC", "12_XER_RSRC.csv", EnhancedTableNames.XerRsrc12, false,
                new[] { C("rsrc_id_key", nullable: false), C("def_qty_per_hr") },
                new[] { "rsrc_id_key" }, new[] { "rsrc_id_key" }),

            new ProgrammeReviewTableContract(
                "15_XER_RESOURCE_DISTRIBUTION", "15_XER_RESOURCE_DISTRIBUTION.csv", EnhancedTableNames.XerResourceDist15, false,
                new[]
                {
                    C("task_id_key", nullable: false),
                    C("rsrc_id_key", nullable: false),
                    C("is_actual", ProgrammeReviewColumnType.Boolean, false),
                    C("distribution_month", ProgrammeReviewColumnType.Date, false),
                    C("monthly_quantity", ProgrammeReviewColumnType.Number, false),
                    C("rsrc_name"),
                    C("rsrc_type"),
                    C("unit", ProgrammeReviewColumnType.Text, true, "Unit"),
                    C("ProjectCode", nullable: false)
                },
                Array.Empty<string>(), new[] { "task_id_key", "rsrc_id_key", "distribution_month", "is_actual" })
        });

    public static IReadOnlyList<ProgrammeReviewTableContract> Tables => ContractTables;

    public static ProgrammeReviewTableContract GetTable(string tableName) =>
        ContractTables.FirstOrDefault(t => string.Equals(t.TableName, tableName, StringComparison.OrdinalIgnoreCase))
        ?? throw new KeyNotFoundException($"Unknown Programme Review table '{tableName}'.");
}
