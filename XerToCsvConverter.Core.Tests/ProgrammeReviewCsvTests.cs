using System.Text;
using XerToCsvConverter.ProgrammeReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ProgrammeReviewCsvTests
{
    [Fact]
    public void Csv_is_utf8_without_bom_crlf_quoted_and_preserves_text()
    {
        string root = NewTempDirectory();
        try
        {
            ResolvedProgrammeReviewRequest request = ProgrammeReviewNaming.Resolve(
                ProgrammeReviewNamingTests.Request(new[]
                {
                    ProgrammeReviewNamingTests.Snapshot("base.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-31")
                }),
                snapshot => snapshot.SourceSha256!);
            ResolvedProgrammeReviewSnapshot snapshot = request.Snapshots.Single();
            ProgrammeReviewTableContract contract = ProgrammeReviewContract.GetTable("15_XER_RESOURCE_DISTRIBUTION");
            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["task_id_key"] = "CSV::J123::C::BL01::1",
                ["rsrc_id_key"] = "CSV::J123::C::BL01::2",
                ["is_actual"] = ProgrammeReviewCsv.Normalize("1", contract.Columns[2], "test"),
                ["distribution_month"] = ProgrammeReviewCsv.Normalize("2026-02-01 00:00:00", contract.Columns[3], "test"),
                ["monthly_quantity"] = ProgrammeReviewCsv.Normalize("8.5000", contract.Columns[4], "test"),
                ["rsrc_name"] = "  Resource, \"A\"  ",
                ["rsrc_type"] = "RT_Labor",
                ["unit"] = "hours",
                ["ProjectCode"] = "J123"
            };
            string path = Path.Combine(root, contract.FileName);
            ProgrammeReviewCsv.WriteTable(path,
                new ProgrammeReviewOutputTable(contract, new[] { new ProgrammeReviewOutputRow(snapshot, values) }),
                CancellationToken.None);

            byte[] bytes = File.ReadAllBytes(path);
            Assert.False(bytes.AsSpan().StartsWith(new byte[] { 0xEF, 0xBB, 0xBF }));
            string text = Encoding.UTF8.GetString(bytes);
            Assert.Contains("\r\n", text, StringComparison.Ordinal);
            Assert.DoesNotContain("\n", text.Replace("\r\n", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
            Assert.Contains("true,2026-02-01,8.5,\"  Resource, \"\"A\"\"  \"", text, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Resource_fact_ties_are_deterministically_ordered_by_remaining_contract_columns()
    {
        ResolvedProgrammeReviewRequest request = ProgrammeReviewNaming.Resolve(
            ProgrammeReviewNamingTests.Request(new[]
            {
                ProgrammeReviewNamingTests.Snapshot("base.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-31")
            }), snapshot => snapshot.SourceSha256!);
        ResolvedProgrammeReviewSnapshot snapshot = request.Snapshots.Single();
        ProgrammeReviewTableContract contract = ProgrammeReviewContract.GetTable("15_XER_RESOURCE_DISTRIBUTION");

        ProgrammeReviewOutputRow Row(string quantity) => new(snapshot,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["task_id_key"] = "CSV::J123::C::BL01::1",
                ["rsrc_id_key"] = "CSV::J123::C::BL01::2",
                ["is_actual"] = "false",
                ["distribution_month"] = "2026-01-01",
                ["monthly_quantity"] = quantity,
                ["rsrc_name"] = "Resource",
                ["rsrc_type"] = "RT_Labor",
                ["unit"] = "hours",
                ["ProjectCode"] = "J123"
            });

        IReadOnlyList<ProgrammeReviewOutputRow> sorted = ProgrammeReviewTransformer.Sort(contract,
            new[] { Row("2"), Row("1") });
        Assert.Equal(new[] { "1", "2" }, sorted.Select(r => r.Values["monthly_quantity"]));
    }

    private static string NewTempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), "XerToCsvConverter.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }
}
