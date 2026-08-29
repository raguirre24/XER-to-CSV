using XerToCsvConverter.ProgrammeReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ProgrammeReviewContractTests
{
    [Fact]
    public void Contract_has_exact_ten_file_set_and_47_task_columns()
    {
        string[] expectedFiles =
        {
            "01_XER_TASK.csv", "02_XER_PROJECT.csv", "03_XER_PROJWBS.csv",
            "06_XER_PREDECESSOR.csv", "07_XER_ACTVTYPE.csv", "08_XER_ACTVCODE.csv",
            "09_XER_TASKACTV.csv", "10_XER_CALENDAR.csv", "12_XER_RSRC.csv",
            "15_XER_RESOURCE_DISTRIBUTION.csv"
        };

        Assert.Equal(expectedFiles, ProgrammeReviewContract.Tables.Select(t => t.FileName));
        Assert.Equal(47, ProgrammeReviewContract.GetTable("01_XER_TASK").Columns.Count);
        Assert.Equal(
            "status_code,task_code,total_float,task_type,id_name,early_start_date,calendar_id_key,task_id_key,driving_path_flag,remaining_duration,early_end_date,monthupdate,task_name,data_date,act_end_date,Finish,proj_id_key,wbs_id_key,free_float,cstr_type,Start,filename,late_end_date,ProjectCode,UpdateDate,ProjectName,Finish_Variance_Previous_Month,Driven_DataDate,Variance_Finish_BL,Variance_Finish_Adjusted_BL,Baseline Finish,Baseline Start,Previous Month Start,Previous Month Finish,Planned Not Completed Last Period,Start_Variance_Previous_Month,PreviousDataDate,Previous Remaining Working Days,Planned Last Period,Completed Last Period,Completed of Planned Last Period,Baseline Effective_Early_End,Baseline Effective_Late_End,Adjusted Baseline Finish,Adjusted Baseline Start,Adjusted Baseline Source Month,Adjusted Baseline Source",
            string.Join(',', ProgrammeReviewContract.GetTable("01_XER_TASK").Columns.Select(c => c.Name)));
    }

    [Fact]
    public void Contract_preserves_fact_grains_without_inventing_resource_assignment_key()
    {
        Assert.Equal(new[] { "task_id_key", "actv_code_id_key" },
            ProgrammeReviewContract.GetTable("09_XER_TASKACTV").KeyColumns);
        Assert.Empty(ProgrammeReviewContract.GetTable("15_XER_RESOURCE_DISTRIBUTION").KeyColumns);
        Assert.Equal(ProgrammeReviewColumnType.Text,
            ProgrammeReviewContract.GetTable("12_XER_RSRC").Columns.Single(c => c.Name == "def_qty_per_hr").Type);
    }
}
