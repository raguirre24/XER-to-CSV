namespace XerToCsvConverter.RelationshipAudit.Cli;

/// <summary>On-demand audit only; normal export profiles and their files are not changed.</summary>
public static class RelationshipAuditCliApplication
{
    public static async Task<int> RunAsync(string[] args, TextWriter? output = null,
        TextWriter? error = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        output ??= Console.Out;
        error ??= Console.Error;
        if (args.Length == 1 && args[0].Equals("--help", StringComparison.OrdinalIgnoreCase))
        {
            PrintUsage(output);
            return 0;
        }

        RelationshipAuditCliArguments options;
        try { options = RelationshipAuditCliArguments.Parse(args); }
        catch (ArgumentException ex)
        {
            error.WriteLine(ex.Message);
            PrintUsage(error);
            return 2;
        }

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        ConsoleCancelEventHandler handler = (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cancellation.Cancel();
        };
        Console.CancelKeyPress += handler;
        try
        {
            cancellation.Token.ThrowIfCancellationRequested();
            string[] inputs = options.Inputs.Select(Path.GetFullPath).ToArray();
            string destination = RelationshipAuditCsv.ValidateDestination(options.Output, inputs, options.Overwrite);
            var service = new ProcessingService();
            XerDataStore store = await service.ParseMultipleXerFilesAsync(inputs.ToList(), null, cancellation.Token)
                .ConfigureAwait(false);
            IReadOnlyList<RelationshipFloatAssessment> assessments = new XerTransformer(store)
                .AssessRelationships(cancellation.Token);
            RelationshipAuditCsv.Publish(assessments, destination, inputs, options.Overwrite, cancellation.Token);
            output.WriteLine(destination);
            error.WriteLine($"Relationship audit published: {assessments.Count} assessment(s). " +
                "Nonnumeric assessments are explained in the audit; they do not constitute publication failure.");
            return 0;
        }
        catch (OperationCanceledException)
        {
            error.WriteLine("Relationship audit cancelled; no partial audit was published.");
            return 130;
        }
        catch (Exception ex)
        {
            error.WriteLine($"Relationship audit failed: {ex.Message}");
            return 1;
        }
        finally { Console.CancelKeyPress -= handler; }
    }

    private static void PrintUsage(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  dotnet run --project XerToCsvConverter.RelationshipAudit.Cli -- --input <source.xer> [--input <source.xer> ...] --output <audit.csv> [--overwrite]");
        writer.WriteLine("Inputs are ordered occurrences. Repeated paths, filenames and content are retained.");
        writer.WriteLine("The output parent folder must already exist. Existing files are protected unless --overwrite is supplied.");
        writer.WriteLine("Exit codes: 0 published/help; 1 failure; 2 usage error; 130 cancelled.");
    }
}
