namespace XerToCsvConverter;

internal static class StandardExportPublication
{
    internal static void ValidateTableName(string tableName)
    {
        if (string.IsNullOrEmpty(tableName) || tableName.Length > 200
            || tableName.Any(c => !(c is >= 'A' and <= 'Z' or >= 'a' and <= 'z'
                or >= '0' and <= '9' or '_' or '-')))
            throw new InvalidDataException($"Unsafe XER table name '{tableName}'. Only ASCII letters, digits, underscores and hyphens are accepted for CSV/ZIP filenames.");
        string upper = tableName.ToUpperInvariant();
        if (upper is "CON" or "PRN" or "AUX" or "NUL"
            || (upper.Length == 4 && (upper.StartsWith("COM", StringComparison.Ordinal)
                || upper.StartsWith("LPT", StringComparison.Ordinal)) && upper[3] is >= '1' and <= '9'))
            throw new InvalidDataException($"Reserved CSV/ZIP filename '{tableName}'.");
    }

    internal static string Destination(string outputDirectory, string tableName)
    {
        ValidateTableName(tableName);
        string root = Path.GetFullPath(outputDirectory);
        string path = Path.GetFullPath(Path.Combine(root, tableName + ".csv"));
        string prefix = Path.TrimEndingDirectorySeparator(root) + Path.DirectorySeparatorChar;
        if (!path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("CSV output resolved outside its selected folder.");
        return path;
    }

    // All selected files are prepared first. Publication is atomic per file and rollback-protected
    // as a set; it is not a transaction visible atomically to concurrent directory readers.
    internal static List<string> Write(XerTable[] tables, string outputDirectory, CsvExporter exporter,
        IProgress<(int percent, string message)>? progress, CancellationToken cancellationToken)
    {
        string root = Path.GetFullPath(outputDirectory);
        string[] destinations = tables.Select(table => Destination(root, table.Name)).ToArray();
        cancellationToken.ThrowIfCancellationRequested();
        Directory.CreateDirectory(root);
        if ((File.GetAttributes(root) & FileAttributes.ReparsePoint) != 0)
            throw new IOException("The selected export folder must not be a filesystem link.");

        string stage = Path.Combine(root, ".xer-export-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(stage);
        string backup = Path.Combine(stage, "backup");
        Directory.CreateDirectory(backup);
        var moved = new List<(string Destination, string Backup, bool HadOriginal, bool Published)>();
        bool cleanupSafe = true;
        try
        {
            for (int i = 0; i < tables.Length; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string destination = destinations[i];
                if (Directory.Exists(destination))
                    throw new IOException($"CSV destination '{destination}' is a directory.");
                if (File.Exists(destination) && (File.GetAttributes(destination) & FileAttributes.ReparsePoint) != 0)
                    throw new IOException($"CSV destination '{destination}' must not be a filesystem link.");
                exporter.WriteTableToCsv(tables[i], Destination(stage, tables[i].Name));
                progress?.Report((80 + (i + 1) * 15 / Math.Max(1, tables.Length), $"Prepared: {tables[i].Name}"));
            }

            try
            {
                for (int i = 0; i < tables.Length; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    string destination = destinations[i];
                    string previous = Destination(backup, tables[i].Name);
                    bool hadOriginal = File.Exists(destination);
                    if (hadOriginal) File.Move(destination, previous);
                    moved.Add((destination, previous, hadOriginal, false));
                    File.Move(Destination(stage, tables[i].Name), destination);
                    moved[^1] = (destination, previous, hadOriginal, true);
                }
            }
            catch (Exception publicationError)
            {
                var rollbackErrors = new List<Exception>();
                foreach (var item in moved.AsEnumerable().Reverse())
                {
                    try
                    {
                        if (item.Published) File.Delete(item.Destination);
                        if (item.HadOriginal) File.Move(item.Backup, item.Destination);
                    }
                    catch (Exception ex) { rollbackErrors.Add(ex); }
                }
                if (rollbackErrors.Count != 0)
                {
                    cleanupSafe = false; // Keep originals recoverable; never erase a failed rollback's backups.
                    throw new AggregateException($"CSV publication failed and rollback was incomplete. Recovery files remain at '{stage}'.",
                        new[] { publicationError }.Concat(rollbackErrors));
                }
                throw;
            }

            progress?.Report((100, $"Published all {tables.Length} requested CSV files."));
            return destinations.ToList();
        }
        finally
        {
            if (cleanupSafe && Directory.Exists(stage)) Directory.Delete(stage, recursive: true);
        }
    }
}
