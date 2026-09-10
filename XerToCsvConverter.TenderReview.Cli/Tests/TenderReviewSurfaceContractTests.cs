using Xunit;
using XerToCsvConverter.TenderReview;
using XerToCsvConverter.TenderReview.Cli;
using XerToCsvConverter.Web.Services;

namespace XerToCsvConverter.TenderReview.Surface.Tests;

public sealed class TenderReviewSurfaceContractTests
{
    [Fact]
    public void BrowserDefaultScriptUsesLocalCalendarGettersAndLoadsBeforeBlazor()
    {
        string webRoot = FindWebProjectRoot();
        string script = File.ReadAllText(Path.Combine(webRoot, "wwwroot", "js", "browserLocalDate.js"));
        string index = File.ReadAllText(Path.Combine(webRoot, "wwwroot", "index.html"));

        Assert.Contains("getFullYear()", script, StringComparison.Ordinal);
        Assert.Contains("getMonth()", script, StringComparison.Ordinal);
        Assert.Contains("getDate()", script, StringComparison.Ordinal);
        Assert.DoesNotContain("toISOString", script, StringComparison.Ordinal);
        Assert.DoesNotContain("getUTC", script, StringComparison.Ordinal);

        int localDateScript = index.IndexOf("js/browserLocalDate.js", StringComparison.Ordinal);
        int blazorScript = index.IndexOf("_framework/blazor.webassembly.js", StringComparison.Ordinal);
        Assert.True(localDateScript >= 0, "index.html must load the browser-local date script.");
        Assert.True(blazorScript > localDateScript, "The browser-local date script must load before Blazor starts.");
    }

    [Fact]
    public void WebUploadCapturesAddTimeDateForEveryProfileAndDoesNotRedefaultOnProfileChange()
    {
        string webRoot = FindWebProjectRoot();
        string page = File.ReadAllText(Path.Combine(webRoot, "Pages", "Index.razor"));

        Assert.Equal("2026-09-05", TenderReviewWebContract.CaptureAddedLocalIsoDate("2026-09-05"));
        Assert.Contains(
            "string addedLocalStatusDate = await CaptureBrowserLocalDateForUploadAsync(tenderReview);",
            page,
            StringComparison.Ordinal);
        Assert.Contains("StatusDate = addedLocalStatusDate", page, StringComparison.Ordinal);
        Assert.DoesNotContain("HandleExportProfileChanged", page, StringComparison.Ordinal);
    }

    [Fact]
    public void TenderWebZipTimestampIsClampedAndRoundedToDosPrecision()
    {
        Assert.Equal(
            new DateTimeOffset(1980, 1, 1, 0, 0, 0, TimeSpan.Zero),
            TenderReviewWebContract.GetDeterministicZipEntryTimestamp(
                new DateTimeOffset(1970, 1, 1, 0, 0, 1, TimeSpan.Zero)));
        Assert.Equal(
            new DateTimeOffset(2026, 9, 5, 1, 2, 2, TimeSpan.Zero),
            TenderReviewWebContract.GetDeterministicZipEntryTimestamp(
                new DateTimeOffset(2026, 9, 5, 1, 2, 3, 987, TimeSpan.Zero)));
        Assert.Equal(
            new DateTimeOffset(2107, 12, 31, 23, 59, 58, TimeSpan.Zero),
            TenderReviewWebContract.GetDeterministicZipEntryTimestamp(
                new DateTimeOffset(2200, 1, 1, 0, 0, 0, TimeSpan.Zero)));
    }

    [Fact]
    public void TenderWebZipUsesOneManifestDerivedTimestampWithoutChangingProgrammePackaging()
    {
        string webRoot = FindWebProjectRoot();
        string page = File.ReadAllText(Path.Combine(webRoot, "Pages", "Index.razor"));

        Assert.Contains("bundle.ManifestRows[0].ExportedAtUtc", page, StringComparison.Ordinal);
        Assert.Contains("entry.LastWriteTime = zipEntryTimestamp;", page, StringComparison.Ordinal);
        Assert.Equal(
            1,
            page.Split("entry.LastWriteTime = zipEntryTimestamp;", StringSplitOptions.None).Length - 1);
    }

    [Fact]
    public void TenderWebFreezesOrderedSourcesAndBlocksUploadMutationWhileProcessing()
    {
        string webRoot = FindWebProjectRoot();
        string page = File.ReadAllText(Path.Combine(webRoot, "Pages", "Index.razor"));

        Assert.Contains("disabled=\"@_isProcessing\"", page, StringComparison.Ordinal);
        Assert.Contains("if (_isBusy)\n            return;", page.Replace("\r\n", "\n"), StringComparison.Ordinal);
        Assert.Contains("UploadedFile[] stagedFiles = _uploadedFiles.ToArray();", page, StringComparison.Ordinal);
        Assert.Contains("TryCreateTenderReviewRequest(stagedFiles", page, StringComparison.Ordinal);
        Assert.Contains("TenderReviewSourceBytes[] sourceBytes = stagedFiles", page, StringComparison.Ordinal);
        Assert.Contains("foreach (UploadedFile file in stagedFiles)", page, StringComparison.Ordinal);
        Assert.Contains("Exported {stagedFiles.Length} ordered Tender stage(s)", page, StringComparison.Ordinal);
    }

    [Fact]
    public void WebMappingPreservesOrderAndRepeatedOriginalFilenames()
    {
        TenderReviewSource[] sources = TenderReviewWebContract.CreateOrderedSources(new[]
        {
            new TenderReviewWebSourceInput(
                TenderReviewNaming.CreateSourceToken(0),
                "tender.xer",
                "2026-09-01"),
            new TenderReviewWebSourceInput(
                TenderReviewNaming.CreateSourceToken(1),
                "tender.xer",
                "2026-09-05")
        });

        Assert.Equal(2, sources.Length);
        Assert.Equal("tender.xer", sources[0].OriginalXerFilename);
        Assert.Equal("tender.xer", sources[1].OriginalXerFilename);
        Assert.Equal(new DateOnly(2026, 9, 1), sources[0].StatusDate);
        Assert.Equal(new DateOnly(2026, 9, 5), sources[1].StatusDate);
        Assert.NotEqual(sources[0].SourceToken, sources[1].SourceToken);
    }

    [Fact]
    public void WebMappingRejectsDuplicateStatusDatesWithoutRejectingRepeatedNames()
    {
        var inputs = new[]
        {
            new TenderReviewWebSourceInput("tender-source-000001", "same.xer", "2026-09-05"),
            new TenderReviewWebSourceInput("tender-source-000002", "same.xer", "2026-09-05")
        };

        ArgumentException error = Assert.Throws<ArgumentException>(() =>
            TenderReviewWebContract.CreateOrderedSources(inputs));

        Assert.Contains("already used", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("2026-09-05", 2026, 9, 5)]
    [InlineData("2026-02-28", 2026, 2, 28)]
    public void BrowserLocalDateContractAcceptsOnlyIsoCalendarDates(
        string value,
        int year,
        int month,
        int day)
    {
        Assert.Equal(
            new DateOnly(year, month, day),
            TenderReviewWebContract.ParseBrowserLocalIsoDate(value));
    }

    [Theory]
    [InlineData("2026-9-5")]
    [InlineData("2026-09-05T00:00:00Z")]
    [InlineData("")]
    public void BrowserLocalDateContractRejectsNonIsoValues(string value)
    {
        Assert.Throws<ArgumentException>(() => TenderReviewWebContract.ParseBrowserLocalIsoDate(value));
    }

    [Theory]
    [InlineData(" c5001 ", "C5001")]
    [InlineData("QAC000623-01-02", "QAC000623-01-02")]
    [InlineData(" NE Part B ", "NE PART B")]
    [InlineData(" Étape/港湾::#2% ", "ÉTAPE/港湾::#2%")]
    public void CliMappingUsesOrderedInternalTokensAndAllowsRepeatedPathsAndNames(
        string projectCode, string normalizedProjectCode)
    {
        string configDirectory = Path.GetFullPath(Path.Combine(Path.GetTempPath(), "tender-cli-config"));
        var configuration = new TenderReviewCliConfiguration
        {
            ProjectCode = projectCode,
            ProjectName = "Example Tender",
            ExportedAtUtc = new DateTimeOffset(2026, 9, 5, 1, 2, 3, TimeSpan.Zero),
            Sources = new[]
            {
                new TenderReviewCliSource
                {
                    XerFilePath = Path.Combine("stages", "same.xer"),
                    OriginalXerFilename = "same.xer",
                    StatusDate = new DateOnly(2026, 9, 1)
                },
                new TenderReviewCliSource
                {
                    XerFilePath = Path.Combine("stages", "same.xer"),
                    OriginalXerFilename = "same.xer",
                    StatusDate = new DateOnly(2026, 9, 5)
                }
            }
        };

        TenderReviewBundleRequest request = TenderReviewCliRequestMapper.CreateRequest(
            configuration,
            configDirectory);

        Assert.Equal(projectCode, request.ProjectCode);
        Assert.Equal(normalizedProjectCode, TenderReviewNaming.NormalizeProjectCode(request.ProjectCode));
        Assert.Equal(2, request.Sources.Count);
        Assert.Equal(TenderReviewNaming.CreateSourceToken(0), request.Sources[0].SourceToken);
        Assert.Equal(TenderReviewNaming.CreateSourceToken(1), request.Sources[1].SourceToken);
        Assert.Equal(request.Sources[0].XerFilePath, request.Sources[1].XerFilePath);
        Assert.Equal("same.xer", request.Sources[0].OriginalXerFilename);
        Assert.Equal("same.xer", request.Sources[1].OriginalXerFilename);
        Assert.Equal(new DateOnly(2026, 9, 1), request.Sources[0].StatusDate);
        Assert.Equal(new DateOnly(2026, 9, 5), request.Sources[1].StatusDate);
    }

    private static string FindWebProjectRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            string candidate = Path.Combine(directory.FullName, "XerToCsvConverter.Web");
            if (File.Exists(Path.Combine(candidate, "wwwroot", "index.html")))
                return candidate;
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate XerToCsvConverter.Web from the test output directory.");
    }
}
