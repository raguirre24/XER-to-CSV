using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class TenderReviewContractTests
{
    [Fact]
    public void Contract_has_exact_profile_file_envelope_and_headers()
    {
        string[] expectedFiles =
        {
            "01_XER_TASK.csv", "02_XER_PROJECT.csv", "03_XER_PROJWBS.csv",
            "06_XER_PREDECESSOR.csv", "07_XER_ACTVTYPE.csv", "08_XER_ACTVCODE.csv",
            "09_XER_TASKACTV.csv", "10_XER_CALENDAR.csv", "12_XER_RSRC.csv",
            "15_XER_RESOURCE_DISTRIBUTION.csv"
        };
        var expectedHeaders = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["01_XER_TASK"] = "status_code,task_code,total_float,task_type,id_name,early_start_date,calendar_id_key,task_id_key,driving_path_flag,remaining_duration,early_end_date,monthupdate,status_date,task_name,data_date,act_end_date,Finish,proj_id_key,wbs_id_key,free_float,cstr_type,Start,filename,late_end_date,ProjectCode,UpdateDate,ProjectName",
            ["02_XER_PROJECT"] = "last_recalc_date,proj_id_key,monthupdate,ProjectCode,add_date,state,region,tender_status,udf_datalake_status_date",
            ["03_XER_PROJWBS"] = "wbs_name,wbs_id_key,parent_wbs_id_key,ProjectCode",
            ["06_XER_PREDECESSOR"] = "task_id_key,pred_type,predecessor_status_code,task_type,predecessor_task_type,lag,start,finish,predecessor_start,predecessor_finish,free_float,pred_task_id_key,status_code,total_float,task_pred_id_key,ProjectCode",
            ["07_XER_ACTVTYPE"] = "actv_code_type_id_key,actv_code_type",
            ["08_XER_ACTVCODE"] = "actv_code_id_key,actv_code_name,actv_code_type_id_key",
            ["09_XER_TASKACTV"] = "task_id_key,actv_code_id_key",
            ["10_XER_CALENDAR"] = "clndr_id_key,clndr_name",
            ["12_XER_RSRC"] = "rsrc_id_key,def_qty_per_hr",
            ["15_XER_RESOURCE_DISTRIBUTION"] = "task_id_key,rsrc_id_key,is_actual,distribution_month,monthly_quantity,rsrc_name,rsrc_type,unit,ProjectCode"
        };

        Assert.Equal("1.0", TenderReviewContract.SchemaVersion);
        Assert.Equal("tender_review", TenderReviewContract.BundleProfile);
        Assert.Equal("COMPLETE", TenderReviewContract.CompleteStatus);
        Assert.Equal(expectedFiles, TenderReviewContract.Tables.Select(table => table.FileName));
        Assert.Equal(expectedHeaders.Count, TenderReviewContract.Tables.Count);
        foreach (TenderReviewTableContract table in TenderReviewContract.Tables)
            Assert.Equal(expectedHeaders[table.TableName],
                string.Join(',', table.Columns.Select(column => column.Name)));
        Assert.Equal(
            new[] { "task_id_key", "rsrc_id_key", "is_actual", "distribution_month" },
            TenderReviewContract.GetTable("15_XER_RESOURCE_DISTRIBUTION").KeyColumns);
    }

    [Fact]
    public void Manifest_header_is_exact_and_ordered()
    {
        Assert.Equal(
            "schema_version,bundle_profile,bundle_id,bundle_status,parser_version,project_code,project_name,original_xer_filename,canonical_xer_filename,status_date,update_date,data_date,source_sha256,table_name,row_count,csv_sha256,exported_at_utc",
            string.Join(',', TenderReviewContract.ManifestColumns));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Aggregation_never_crosses_sources_with_blank_keys_and_preserves_conflicting_contributions(bool sameSource)
    {
        var request = TenderReviewNamingTests.Request(new[]
        {
            TenderReviewNamingTests.Source(0, "same.xer", "2026-09-05"),
            TenderReviewNamingTests.Source(1, "same.xer", "2026-09-06")
        });
        var resolved = TenderReviewNaming.Resolve(request, s => s.SourceSha256!);
        var contract = TenderReviewContract.GetTable("15_XER_RESOURCE_DISTRIBUTION");
        var firstValues = contract.Columns.ToDictionary(c => c.Name, _ => "", StringComparer.Ordinal);
        firstValues["is_actual"] = "false";
        firstValues["distribution_month"] = "2026-09-01";
        firstValues["monthly_quantity"] = "2";
        firstValues["rsrc_name"] = "First label";
        var secondValues = new Dictionary<string, string>(firstValues, StringComparer.Ordinal)
        {
            ["monthly_quantity"] = "3",
            ["rsrc_name"] = "Second label"
        };
        var first = new TenderReviewOutputRow(resolved.Sources[0], firstValues);
        var second = new TenderReviewOutputRow(resolved.Sources[sameSource ? 0 : 1], secondValues);
        var warnings = new List<string>();
        var rows = TenderReviewTransformer.AggregateResourceDistribution(contract, new[] { first, second },
            (_, message) => warnings.Add(message));
        Assert.Equal(2, rows.Count);
        Assert.Same(first.Source, rows[0].Source);
        Assert.Same(second.Source, rows[1].Source);
        foreach (string column in contract.Columns.Select(c => c.Name))
        {
            Assert.Equal(first.Values[column], rows[0].Values[column]);
            Assert.Equal(second.Values[column], rows[1].Values[column]);
        }
        if (sameSource) Assert.Single(warnings);
        else Assert.Empty(warnings);
    }
}
