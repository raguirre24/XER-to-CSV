namespace XerToCsvConverter.RelationshipAudit.Cli;

/// <summary>Inputs remain ordered occurrences: repeated paths are deliberately retained.</summary>
public sealed record RelationshipAuditCliArguments(IReadOnlyList<string> Inputs, string Output, bool Overwrite)
{
    public static RelationshipAuditCliArguments Parse(IReadOnlyList<string> arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        var inputs = new List<string>();
        string? output = null;
        bool overwrite = false;
        for (int i = 0; i < arguments.Count; i++)
        {
            string option = arguments[i];
            if (option.Equals("--overwrite", StringComparison.OrdinalIgnoreCase))
            {
                if (overwrite) throw new ArgumentException("--overwrite must appear at most once.");
                overwrite = true;
                continue;
            }
            if (!option.Equals("--input", StringComparison.OrdinalIgnoreCase)
                && !option.Equals("--output", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException($"Unknown option '{option}'.");
            if (++i >= arguments.Count || string.IsNullOrWhiteSpace(arguments[i])
                || arguments[i].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"{option} requires a path.");
            if (option.Equals("--input", StringComparison.OrdinalIgnoreCase))
                inputs.Add(arguments[i]);
            else
            {
                if (output is not null) throw new ArgumentException("--output must appear exactly once.");
                output = arguments[i];
            }
        }
        if (inputs.Count == 0 || output is null)
            throw new ArgumentException("At least one --input and exactly one --output are required.");
        return new RelationshipAuditCliArguments(inputs.AsReadOnly(), output, overwrite);
    }
}
