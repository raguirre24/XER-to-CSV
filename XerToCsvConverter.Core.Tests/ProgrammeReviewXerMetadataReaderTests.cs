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
}
