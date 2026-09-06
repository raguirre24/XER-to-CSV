using System.Text.Json;
using XerToCsvConverter.TenderReview;
using XerToCsvConverter.TenderReview.Cli;

return await TenderReviewCliApplication.RunAsync(args);

public static class TenderReviewCliApplication
{
    public static async Task<int> RunAsync(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        if (args.Length == 0 || args.Contains("--help", StringComparer.OrdinalIgnoreCase))
        {
            PrintUsage();
            return args.Length == 0 ? 2 : 0;
        }

        string? configPath = GetOption(args, "--config");
        string? outputRoot = GetOption(args, "--output-root");
        if (string.IsNullOrWhiteSpace(configPath) || string.IsNullOrWhiteSpace(outputRoot))
        {
            Console.Error.WriteLine("Both --config and --output-root are required.");
            PrintUsage();
            return 2;
        }

        try
        {
            string fullConfigPath = Path.GetFullPath(configPath);
            if (!File.Exists(fullConfigPath))
                throw new TenderReviewValidationException($"Configuration file does not exist: '{fullConfigPath}'.");

            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
                ReadCommentHandling = JsonCommentHandling.Skip,
                AllowTrailingCommas = true
            };
            await using FileStream stream = File.OpenRead(fullConfigPath);
            TenderReviewCliConfiguration? configuration = await JsonSerializer.DeserializeAsync<TenderReviewCliConfiguration>(
                stream,
                options);
            if (configuration is null)
                throw new TenderReviewValidationException("Configuration JSON did not contain a Tender Review request.");

            string configDirectory = Path.GetDirectoryName(fullConfigPath) ?? Directory.GetCurrentDirectory();
            TenderReviewBundleRequest request = TenderReviewCliRequestMapper.CreateRequest(configuration, configDirectory);

            using var cancellation = new CancellationTokenSource();
            ConsoleCancelEventHandler handler = (_, eventArgs) =>
            {
                eventArgs.Cancel = true;
                cancellation.Cancel();
            };
            Console.CancelKeyPress += handler;
            try
            {
                var progress = new Progress<XerToCsvConverter.ProcessingService.DetailedProgress>(p =>
                    Console.Error.WriteLine($"[{p.Percent,3}%] {p.Message}"));
                var service = new TenderReviewBundleService();
                TenderReviewBundleResult result = await service.BuildFromXerFilesAsync(
                    request,
                    Path.GetFullPath(outputRoot),
                    progress,
                    cancellation.Token);
                if (result.WarningCount > 0)
                    Console.Error.WriteLine($"Tender Review bundle completed with warnings: {result.WarningCount} data-quality issue(s). " +
                                            "See XER_DATA_QUALITY.csv in the bundle for affected tables and fields, original source values, and any unallocated actual or remaining quantities.");
                Console.WriteLine(result.BundlePath);
                return 0;
            }
            finally
            {
                Console.CancelKeyPress -= handler;
            }
        }
        catch (OperationCanceledException)
        {
            Console.Error.WriteLine("Tender Review bundle generation was cancelled; no partial bundle was published.");
            return 130;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Tender Review bundle generation failed: {ex.Message}");
            return 1;
        }
    }

    private static string? GetOption(string[] args, string name)
    {
        for (int index = 0; index < args.Length; index++)
        {
            if (!string.Equals(args[index], name, StringComparison.OrdinalIgnoreCase))
                continue;
            return index + 1 < args.Length ? args[index + 1] : null;
        }

        return null;
    }

    private static void PrintUsage()
    {
        Console.Error.WriteLine("Usage:");
        Console.Error.WriteLine("  dotnet run --project XerToCsvConverter.TenderReview.Cli -- --config <tender.json> --output-root <directory>");
        Console.Error.WriteLine();
        Console.Error.WriteLine("The JSON sources array is ordered. Each source requires xer_file_path and status_date (yyyy-MM-dd);");
        Console.Error.WriteLine("original_xer_filename is optional and defaults to the path's filename. Repeated paths and names are valid.");
    }
}
