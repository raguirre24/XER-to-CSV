using System.Text;
using XerToCsvConverter.ProgrammeReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ProgrammeReviewXerMetadataReaderTests
{
    [Fact]
    public async Task Reads_data_date_from_reordered_project_fields_and_timestamp()
    {
        byte[] xer = Encoding.UTF8.GetBytes(
            "%T\tPROJECT\r\n" +
            "%F\tproj_short_name\tlast_recalc_date\tproj_id\r\n" +
            "%R\tTest\t2026-08-25 17:00\t3651\r\n" +
            "%T\tTASK\r\n%F\ttask_id\r\n%R\t1\r\n%E\r\n");

        DateOnly? result = await ProgrammeReviewXerMetadataReader
            .ReadSingleProjectDataDateAsync(xer);

        Assert.Equal(new DateOnly(2026, 8, 25), result);
    }

    [Fact]
    public async Task Reads_data_date_when_windows_1252_bytes_precede_project_table()
    {
        byte[] prefix = Encoding.ASCII.GetBytes(
            "%T\tTASK\r\n%F\ttask_id\ttask_name\r\n%R\t1\tRoad ");
        byte[] windows1252Text = { 0x96, 0x20, 0x80 };
        byte[] suffix = Encoding.ASCII.GetBytes(
            "\r\n%T\tPROJECT\r\n" +
            "%F\tproj_id\tlast_recalc_date\r\n" +
            "%R\t3651\t2026-02-26 08:00:00\r\n%E\r\n");
        byte[] xer = prefix.Concat(windows1252Text).Concat(suffix).ToArray();

        DateOnly? result = await ProgrammeReviewXerMetadataReader
            .ReadSingleProjectDataDateAsync(xer);

        Assert.Equal(new DateOnly(2026, 2, 26), result);
    }

    [Fact]
    public async Task Reads_data_date_directly_from_file_path()
    {
        string path = Path.Combine(Path.GetTempPath(), $"xer-metadata-{Guid.NewGuid():N}.xer");
        try
        {
            await File.WriteAllTextAsync(
                path,
                "%T\tPROJECT\r\n" +
                "%F\tproj_id\tlast_recalc_date\r\n" +
                "%R\t3651\t2026-08-25 17:00\r\n" +
                "%T\tTASK\r\n");

            DateOnly? result = await ProgrammeReviewXerMetadataReader
                .ReadSingleProjectDataDateAsync(path);

            Assert.Equal(new DateOnly(2026, 8, 25), result);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task File_path_reader_reopens_for_windows_1252_fallback()
    {
        string path = Path.Combine(Path.GetTempPath(), $"xer-metadata-{Guid.NewGuid():N}.xer");
        try
        {
            byte[] prefix = Encoding.ASCII.GetBytes("%T\tTASK\r\n%F\ttask_name\r\n%R\tRoad ");
            byte[] windows1252Text = { 0x96, 0x20, 0x80 };
            byte[] suffix = Encoding.ASCII.GetBytes(
                "\r\n%T\tPROJECT\r\n" +
                "%F\tproj_id\tlast_recalc_date\r\n" +
                "%R\t3651\t2026-02-26 08:00:00\r\n%E\r\n");
            await File.WriteAllBytesAsync(path, prefix.Concat(windows1252Text).Concat(suffix).ToArray());

            DateOnly? result = await ProgrammeReviewXerMetadataReader
                .ReadSingleProjectDataDateAsync(path);

            Assert.Equal(new DateOnly(2026, 2, 26), result);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task File_path_reader_reports_missing_file()
    {
        string path = Path.Combine(Path.GetTempPath(), $"missing-xer-{Guid.NewGuid():N}.xer");

        await Assert.ThrowsAsync<FileNotFoundException>(async () =>
            await ProgrammeReviewXerMetadataReader.ReadSingleProjectDataDateAsync(path));
    }

    [Theory]
    [InlineData("%T\tPROJECT\r\n%F\tproj_id\tlast_recalc_date\r\n%E\r\n")]
    [InlineData("%T\tPROJECT\r\n%F\tproj_id\tlast_recalc_date\r\n%R\t1\tinvalid\r\n%E\r\n")]
    [InlineData("%T\tPROJECT\r\n%F\tproj_id\r\n%R\t1\r\n%E\r\n")]
    [InlineData("%T\tPROJECT\r\n%F\tproj_id\tlast_recalc_date\r\n%R\t1\t2026-01-01\r\n%R\t2\t2026-01-01\r\n%E\r\n")]
    public async Task Returns_null_when_data_date_is_missing_invalid_or_ambiguous(string content)
    {
        DateOnly? result = await ProgrammeReviewXerMetadataReader
            .ReadSingleProjectDataDateAsync(Encoding.UTF8.GetBytes(content));

        Assert.Null(result);
    }

    [Fact]
    public async Task Honors_cancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await ProgrammeReviewXerMetadataReader.ReadSingleProjectDataDateAsync(
                Encoding.UTF8.GetBytes("%T\tPROJECT\r\n"), cancellation.Token));
    }

    [Theory]
    [InlineData("2026-08-25", "2026-07-31", "2026-08-25")]
    [InlineData("2025-12-31", "2026-01-02 17:00", "2026-01-02")]
    [InlineData("2026-02-28", "2026-02-15", "2026-02-28")]
    public async Task Browser_metadata_suggests_latest_date_with_explicit_warning_while_public_reader_remains_strict(
        string firstDate, string secondDate, string expectedDate)
    {
        byte[] xer = ProjectRows(firstDate, secondDate);
        ProgrammeReviewDetectedMetadata metadata = await ProgrammeReviewXerMetadataReader.ReadProjectMetadataAsync(xer);

        Assert.Equal(DateOnly.ParseExact(expectedDate, "yyyy-MM-dd"), metadata.DataDate);
        Assert.Equal(2, metadata.ProjectRowCount);
        Assert.Equal(0, metadata.InvalidDataDateRowCount);
        Assert.Equal(2, metadata.DataDates.Count);
        Assert.True(metadata.DataDates[0] < metadata.DataDates[1]);
        string warning = Assert.Single(metadata.Warnings);
        Assert.Contains("different data dates", warning);
        Assert.Contains(expectedDate, warning);
        Assert.Contains("editable", warning);
        Assert.Null(await ProgrammeReviewXerMetadataReader.ReadSingleProjectDataDateAsync(xer));
    }

    [Fact]
    public async Task Shared_multi_project_date_is_unambiguous_for_browser_prefill()
    {
        ProgrammeReviewDetectedMetadata metadata = await ProgrammeReviewXerMetadataReader
            .ReadProjectMetadataAsync(ProjectRows("2026-08-25 08:00", "2026-08-25 17:00"));

        Assert.Equal(new DateOnly(2026, 8, 25), metadata.DataDate);
        Assert.Equal(2, metadata.ProjectRowCount);
        Assert.Single(metadata.DataDates);
        Assert.Empty(metadata.Warnings);
    }

    [Theory]
    [InlineData("")]
    [InlineData("invalid")]
    public async Task Invalid_project_date_is_not_silently_replaced_in_metadata(string invalid)
    {
        ProgrammeReviewDetectedMetadata metadata = await ProgrammeReviewXerMetadataReader
            .ReadProjectMetadataAsync(ProjectRows(invalid, "2026-08-25"));

        Assert.Null(metadata.DataDate);
        Assert.Equal(1, metadata.InvalidDataDateRowCount);
        Assert.Contains("missing or invalid", Assert.Single(metadata.Warnings));
    }

    [Fact]
    public async Task Rich_metadata_uses_windows_1252_fallback_and_leaves_caller_stream_open()
    {
        byte[] prefix = Encoding.ASCII.GetBytes("%T\tPROJECT\r\n%F\tproj_id\tproj_short_name\tlast_recalc_date\r\n%R\t1\tRoad ");
        byte[] suffix = Encoding.ASCII.GetBytes("\t2026-07-31\r\n%R\t2\tOther\t2026-08-25\r\n%T\tTASK\r\n");
        using var input = new MemoryStream(prefix.Concat(new byte[] { 0x96 }).Concat(suffix).ToArray());

        ProgrammeReviewDetectedMetadata metadata = await ProgrammeReviewXerMetadataReader.ReadProjectMetadataAsync(input);

        Assert.Equal(new DateOnly(2026, 8, 25), metadata.DataDate);
        Assert.Equal(2, metadata.ProjectRowCount);
        Assert.Single(metadata.Warnings);
        Assert.True(input.CanRead);
    }

    private static byte[] ProjectRows(string firstDate, string secondDate) => Encoding.UTF8.GetBytes(
        "%T\tPROJECT\r\n%F\tproj_id\tlast_recalc_date\r\n"
        + $"%R\t1\t{firstDate}\r\n%R\t2\t{secondDate}\r\n%T\tTASK\r\n%F\ttask_id\r\n%R\t1\r\n");
}
