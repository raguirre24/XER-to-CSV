using Xunit;

namespace XerToCsvConverter.TenderReview.Surface.Tests;

/// <summary>
/// Checks the UI/CLI wiring that is not hosted by this cross-platform test project.
/// Core tests separately verify the emitted warning counts and companion contents.
/// </summary>
public sealed class ExportDataQualitySurfaceTests
{
    [Theory]
    [InlineData("MainForm.cs", "ExportTablesWithDiagnosticsAsync(", "result.WarningCount")]
    [InlineData("MainForm.ProgrammeReview.cs", "BuildFromXerFilesAsync(", "result.WarningCount")]
    [InlineData("MainForm.TenderReview.cs", "BuildFromXerFilesAsync(", "result.WarningCount")]
    public void WindowsSurfacesUseTypedWarningsAndIdentifyTheApplicableDiagnostics(
        string path, string serviceCall, string warningCount)
    {
        string source = ReadSource(path);

        Assert.Contains(serviceCall, source, StringComparison.Ordinal);
        Assert.Contains(warningCount, source, StringComparison.Ordinal);
        Assert.Contains("completed with warnings", source, StringComparison.Ordinal);
        Assert.Contains("data-quality issue(s)", source, StringComparison.Ordinal);
        if (path == "MainForm.cs")
        {
            Assert.Contains("XER_DATA_QUALITY.csv", source, StringComparison.Ordinal);
            Assert.Contains("affected tables and fields, original source values, and any unallocated actual or remaining quantities", source, StringComparison.Ordinal);
        }
        else
        {
            Assert.DoesNotContain("XER_DATA_QUALITY.csv", source, StringComparison.Ordinal);
            Assert.Contains("XerDataQuality.GetMessages(result.DataQualityTable)", source, StringComparison.Ordinal);
            Assert.Contains("activity log", source, StringComparison.Ordinal);
            Assert.Contains("ten report CSV files and the manifest only", source, StringComparison.Ordinal);
        }
        Assert.Contains("Export Completed with Warnings", source, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("XerToCsvConverter.ProgrammeReview.Cli/Program.cs")]
    [InlineData("XerToCsvConverter.TenderReview.Cli/Program.cs")]
    public void CliWarningsUseStderrWithoutChangingTheSuccessfulPathAndExitCode(string path)
    {
        string source = ReadSource(path);
        int condition = source.IndexOf("if (result.WarningCount > 0)", StringComparison.Ordinal);
        Assert.True(condition >= 0, "CLI success handling must inspect the typed warning count.");
        int warning = source.IndexOf("Console.Error.WriteLine", condition, StringComparison.Ordinal);
        Assert.True(warning > condition, "Warnings must be written to stderr.");
        int pathOutput = source.IndexOf("Console.WriteLine(result.BundlePath);", warning, StringComparison.Ordinal);
        Assert.True(pathOutput > warning, "The published path must remain the stdout result.");
        int exit = source.IndexOf("return 0;", pathOutput, StringComparison.Ordinal);

        Assert.True(exit > pathOutput);
        string warningBlock = source[condition..pathOutput];
        Assert.Contains("completed with warnings", warningBlock, StringComparison.Ordinal);
        Assert.Contains("{result.WarningCount}", warningBlock, StringComparison.Ordinal);
        Assert.DoesNotContain("XER_DATA_QUALITY.csv", warningBlock, StringComparison.Ordinal);
        Assert.Contains("XerDataQuality.GetMessages(result.DataQualityTable)", warningBlock, StringComparison.Ordinal);
        Assert.Contains("Console.Error.WriteLine(message);", warningBlock, StringComparison.Ordinal);
        Assert.Contains("ten report CSV files and the manifest only", warningBlock, StringComparison.Ordinal);
        Assert.DoesNotContain("Console.WriteLine(", warningBlock, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("CreateProgrammeReviewBundle", "Programme Review bundle", "bundle.WarningCount")]
    [InlineData("CreateTenderReviewBundle", "Tender Review bundle", "bundle.WarningCount")]
    [InlineData("ExportTables", "Export", "result.WarningCount")]
    public void WebOnlyMarksWarningCompletionAfterBrowserDownloadHandoff(
        string methodName, string subject, string warningCount)
    {
        string page = ReadSource("XerToCsvConverter.Web/Pages/Index.razor");
        string method = ReadPrivateMethod(page, $"private async Task {methodName}(");
        int handoff = method.IndexOf("await _downloadService!.DownloadFileAsync(", StringComparison.Ordinal);
        string completionCall = methodName == "ExportTables"
            ? $"SetExportCompletion(\"{subject}\", {warningCount});"
            : $"SetReviewExportCompletion(\"{subject}\", {warningCount}, bundle.DataQualityTable);";
        int completion = method.IndexOf(completionCall, StringComparison.Ordinal);

        Assert.True(handoff >= 0 && completion > handoff,
            "A successful bundle generation alone must not claim browser handoff completion.");
        Assert.Contains($"{warningCount} > 0 ? \"Warnings\" : \"Success\"", method, StringComparison.Ordinal);
        Assert.Contains("Check browser downloads for saved-file delivery.", method, StringComparison.Ordinal);
    }

    [Fact]
    public void WebWarningBannerPersistsAfterCompletionAndClearsForTheNextOperation()
    {
        string page = ReadSource("XerToCsvConverter.Web/Pages/Index.razor");
        string completion = ReadPrivateMethod(page, "private void SetExportCompletion(");
        string end = ReadPrivateMethod(page, "private void EndOperation(");
        string begin = ReadPrivateMethod(page, "private void BeginOperation(");
        string clear = ReadPrivateMethod(page, "private void ClearResults(");

        Assert.Contains("class=\"warning-banner\" role=\"status\">@_exportWarningMessage", page, StringComparison.Ordinal);
        Assert.Contains("warningCount > 0", completion, StringComparison.Ordinal);
        Assert.Contains("{warningCount} data-quality issue(s)", completion, StringComparison.Ordinal);
        Assert.Contains("XER_DATA_QUALITY.csv", completion, StringComparison.Ordinal);
        Assert.Contains("affected tables and fields, original source values, and any unallocated actual or remaining quantities", completion, StringComparison.Ordinal);
        Assert.DoesNotContain("_exportWarningMessage", end, StringComparison.Ordinal);
        Assert.Contains("_exportWarningMessage = string.Empty;", begin, StringComparison.Ordinal);
        Assert.Contains("_exportWarningMessage = string.Empty;", clear, StringComparison.Ordinal);
    }

    [Fact]
    public void WebStandardUsesDiagnosticResultAndReviewsReportElevenFilesWithLoggedDiagnostics()
    {
        string page = ReadSource("XerToCsvConverter.Web/Pages/Index.razor");

        Assert.Contains("StandardMemoryExportResult result = await _processingService.ExportTablesToMemoryWithDiagnosticsAsync(",
            page, StringComparison.Ordinal);
        Assert.Contains("Dictionary<string, byte[]> csvData = result.Files;", page, StringComparison.Ordinal);
        Assert.Equal(2, page.Split("10 report CSV files plus XER_CSV_MANIFEST.csv (11 files)",
            StringSplitOptions.None).Length - 1);
        Assert.DoesNotContain("10 report CSV files plus XER_DATA_QUALITY.csv", page, StringComparison.Ordinal);
        string reviewCompletion = ReadPrivateMethod(page, "private void SetReviewExportCompletion(");
        Assert.Contains("XerDataQuality.GetMessages(dataQualityTable)", reviewCompletion, StringComparison.Ordinal);
        Assert.Contains("LogActivity(message);", reviewCompletion, StringComparison.Ordinal);
        Assert.Contains("_exportWarningMessage = warningCount > 0", reviewCompletion, StringComparison.Ordinal);
        Assert.Contains("activity log", reviewCompletion, StringComparison.Ordinal);
        Assert.DoesNotContain("XER_DATA_QUALITY.csv", reviewCompletion, StringComparison.Ordinal);
        Assert.Contains(".file-status.warnings", ReadSource("XerToCsvConverter.Web/wwwroot/css/app.css"),
            StringComparison.Ordinal);
    }

    [Fact]
    public void EnhancedAvailabilityDependsOnPrimarySourceNotAllCalculationLookups()
    {
        string windows = ReadSource("MainForm.cs");
        int start = windows.IndexOf("private void CheckEnhancedTableDependencies()", StringComparison.Ordinal);
        int finish = windows.IndexOf("private bool CheckSpecificPbiDependency", start, StringComparison.Ordinal);
        string availability = windows[start..finish];
        Assert.Contains("bool flag = _dataStore.ContainsTable(\"TASK\");", availability, StringComparison.Ordinal);
        Assert.Contains("_canCreateTask01 = flag;", availability, StringComparison.Ordinal);
        Assert.Contains("bool canCreatePredecessor = _dataStore.ContainsTable(TableNames.TaskPred);", availability, StringComparison.Ordinal);
        Assert.Contains("_canCreateResourceDist15 = flag4;", availability, StringComparison.Ordinal);
        Assert.DoesNotContain("&&", availability, StringComparison.Ordinal);

        string web = ReadPrivateMethod(ReadSource("XerToCsvConverter.Web/Pages/Index.razor"),
            "private void AddEnhancedTableNames()");
        Assert.Contains("if (_dataStore.ContainsTable(TableNames.Task))", web, StringComparison.Ordinal);
        Assert.Contains("if (_dataStore.ContainsTable(TableNames.TaskPred))", web, StringComparison.Ordinal);
        Assert.Contains("if (_dataStore.ContainsTable(TableNames.TaskRsrc))", web, StringComparison.Ordinal);
        Assert.DoesNotContain("&&", web, StringComparison.Ordinal);
    }

    private static string ReadPrivateMethod(string source, string signature)
    {
        int start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"Missing method: {signature}");
        int next = source.IndexOf("\n    private ", start + signature.Length, StringComparison.Ordinal);
        return next < 0 ? source[start..] : source[start..next];
    }

    private static string ReadSource(string relativePath)
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "XER to CSV.csproj")))
                return File.ReadAllText(Path.Combine(directory.FullName, relativePath.Replace('/', Path.DirectorySeparatorChar)))
                    .Replace("\r\n", "\n", StringComparison.Ordinal);
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate the XER to CSV repository from the test output directory.");
    }
}
