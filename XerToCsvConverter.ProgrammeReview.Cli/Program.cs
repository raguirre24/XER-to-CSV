using System.Text.Json;
using System.Text.Json.Serialization;
using XerToCsvConverter.ProgrammeReview;

return await RunAsync(args);

static async Task<int> RunAsync(string[] args)
{
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
            throw new ProgrammeReviewValidationException($"Configuration file does not exist: '{fullConfigPath}'.");

        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            ReadCommentHandling = JsonCommentHandling.Skip,
            AllowTrailingCommas = true
        };
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseLower));
        await using FileStream stream = File.OpenRead(fullConfigPath);
        ProgrammeReviewBundleRequest? parsed = await JsonSerializer.DeserializeAsync<ProgrammeReviewBundleRequest>(stream, options);
        if (parsed is null)
            throw new ProgrammeReviewValidationException("Configuration JSON did not contain a bundle request.");

        string configDirectory = Path.GetDirectoryName(fullConfigPath) ?? Directory.GetCurrentDirectory();
        ProgrammeReviewBundleRequest request = parsed with
        {
            Snapshots = parsed.Snapshots.Select(snapshot => snapshot with
            {
                XerFilePath = string.IsNullOrWhiteSpace(snapshot.XerFilePath)
                    ? snapshot.XerFilePath
                    : Path.GetFullPath(snapshot.XerFilePath, configDirectory)
            }).ToArray()
        };

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
            var service = new ProgrammeReviewBundleService();
            ProgrammeReviewBundleResult result = await service.BuildFromXerFilesAsync(
                request, Path.GetFullPath(outputRoot), progress, cancellation.Token);
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
        Console.Error.WriteLine("Programme Review bundle generation was cancelled; no partial bundle was published.");
        return 130;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Programme Review bundle generation failed: {ex.Message}");
        return 1;
    }
}

static string? GetOption(string[] args, string name)
{
    for (int i = 0; i < args.Length; i++)
    {
        if (!string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase)) continue;
        return i + 1 < args.Length ? args[i + 1] : null;
    }
    return null;
}

static void PrintUsage()
{
    Console.Error.WriteLine("Usage:");
    Console.Error.WriteLine("  dotnet run --project XerToCsvConverter.ProgrammeReview.Cli -- --config <bundle.json> --output-root <directory>");
}
