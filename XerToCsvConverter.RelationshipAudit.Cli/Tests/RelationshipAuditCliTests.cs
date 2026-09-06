using System.Globalization;
using System.Text;
using System.Text.Json;
using XerToCsvConverter.RelationshipAudit.Cli;
using Xunit;

namespace XerToCsvConverter.RelationshipAudit.Tests;

public sealed class RelationshipAuditCliTests
{
    [Fact]
    public void Arguments_preserve_order_repeated_paths_and_overwrite()
    {
        RelationshipAuditCliArguments result = RelationshipAuditCliArguments.Parse(
            ["--input", "second.xer", "--output", "audit.csv", "--input", "first.xer", "--input", "second.xer", "--overwrite"]);
        Assert.Equal(new[] { "second.xer", "first.xer", "second.xer" }, result.Inputs);
        Assert.Equal("audit.csv", result.Output);
        Assert.True(result.Overwrite);
    }

    public static TheoryData<string[]> BadArguments => new()
    {
        Array.Empty<string>(), new[] { "--input", "a.xer" }, new[] { "--output", "audit.csv" },
        new[] { "--input", "--output", "audit.csv" }, new[] { "--input", "a.xer", "--output" },
        new[] { "--input", "a.xer", "--output", "audit.csv", "--unknown" },
        new[] { "--input", "a.xer", "--output", "audit.csv", "--output", "b.csv" },
        new[] { "--input", "a.xer", "--output", "audit.csv", "--overwrite", "--overwrite" },
        new[] { "--input", "a.xer", "--output", "audit.csv", "--help" }
    };

    [Theory]
    [MemberData(nameof(BadArguments))]
    public async Task Invalid_arguments_return_usage_code(string[] arguments)
    {
        using var output = new StringWriter();
        using var error = new StringWriter();
        Assert.Equal(2, await RelationshipAuditCliApplication.RunAsync(arguments, output, error));
        Assert.Contains("Usage:", error.ToString(), StringComparison.Ordinal);
        Assert.Equal("", output.ToString());
    }

    [Fact]
    public async Task Help_does_not_require_input_or_output()
    {
        using var output = new StringWriter();
        Assert.Equal(0, await RelationshipAuditCliApplication.RunAsync(["--help"], output, new StringWriter()));
        Assert.Contains("Repeated paths", output.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Missing_input_is_fatal_without_publication()
    {
        using var folder = new TestFolder();
        string destination = Path.Combine(folder.Path, "audit.csv");
        Assert.Equal(1, await RelationshipAuditCliApplication.RunAsync(
            ["--input", Path.Combine(folder.Path, "missing.xer"), "--output", destination], new StringWriter(), new StringWriter()));
        Assert.Empty(Directory.GetFiles(folder.Path));
    }

    [Fact]
    public async Task Pre_cancelled_cli_returns_130_without_publication()
    {
        using var folder = new TestFolder();
        string input = folder.CreateInput();
        string destination = Path.Combine(folder.Path, "audit.csv");
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.Equal(130, await RelationshipAuditCliApplication.RunAsync(
            ["--input", input, "--output", destination], new StringWriter(), new StringWriter(), cancellation.Token));
        Assert.False(File.Exists(destination));
    }

    [Fact]
    public async Task Repeated_input_occurrences_have_deterministic_namespaces_and_preserve_every_row()
    {
        using var folder = new TestFolder();
        string input = folder.CreateInput();
        string destination = Path.Combine(folder.Path, "audit.csv");
        string[] args = ["--input", input, "--input", input, "--output", destination];
        Assert.Equal(0, await RelationshipAuditCliApplication.RunAsync(args, new StringWriter(), new StringWriter()));
        byte[] first = File.ReadAllBytes(destination);
        List<string[]> records = ParseCsv(Encoding.UTF8.GetString(first));
        Assert.Equal(3, records.Count);
        int source = Array.IndexOf(records[0], "source_namespace");
        int ordinal = Array.IndexOf(records[0], "source_row_number");
        int original = Array.IndexOf(records[0], "FileName");
        Assert.Equal("input.xer#source-000001", records[1][source]);
        Assert.Equal("input.xer#source-000002", records[2][source]);
        Assert.Equal("1", records[1][ordinal]);
        Assert.Equal("1", records[2][ordinal]);
        Assert.Equal("input.xer", records[1][original]);
        Assert.Equal("input.xer", records[2][original]);
        Assert.Equal("Calculated", records[1][Array.IndexOf(records[0], "classification")]);
        Assert.Equal("0", records[1][Array.IndexOf(records[0], "free_float")]);
        Assert.DoesNotContain("SourceToken", Encoding.UTF8.GetString(first), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, await RelationshipAuditCliApplication.RunAsync([.. args, "--overwrite"], new StringWriter(), new StringWriter()));
        Assert.Equal(first, File.ReadAllBytes(destination));
    }

    [Fact]
    public async Task Audit_values_match_table06_through_disk_and_web_stream_parsing()
    {
        using var folder = new TestFolder();
        string input = folder.CreateInput();
        var service = new ProcessingService();
        XerDataStore disk = await service.ParseMultipleXerFilesAsync([input, input], null, default);
        using var first = new MemoryStream(File.ReadAllBytes(input));
        using var second = new MemoryStream(File.ReadAllBytes(input));
        XerDataStore web = await service.ParseXerStreamsAsync([(first, "input.xer"), (second, "input.xer")], null, default);
        var diskExport = await service.ExportTablesToMemoryAsync(disk, [EnhancedTableNames.XerPredecessor06], null, default);
        var webExport = await service.ExportTablesToMemoryAsync(web, [EnhancedTableNames.XerPredecessor06], null, default);
        Assert.Equal(diskExport[EnhancedTableNames.XerPredecessor06], webExport[EnhancedTableNames.XerPredecessor06]);
        List<string[]> records = ParseCsv(Encoding.UTF8.GetString(diskExport[EnhancedTableNames.XerPredecessor06]).TrimStart('\uFEFF'));
        var audit = new XerTransformer(disk).AssessRelationships();
        Assert.Equal(2, audit.Count);
        for (int index = 0; index < audit.Count; index++)
        {
            Assert.Equal(audit[index].FormattedDays, records[index + 1][Array.IndexOf(records[0], "free_float")]);
            Assert.Equal(audit[index].SuccessorIdKey, records[index + 1][Array.IndexOf(records[0], "task_id_key")]);
        }
        using var diskAudit = new StringWriter();
        using var webAudit = new StringWriter();
        RelationshipAuditCsv.Write(audit, diskAudit);
        RelationshipAuditCsv.Write(new XerTransformer(web).AssessRelationships(), webAudit);
        Assert.Equal(diskAudit.ToString(), webAudit.ToString());
    }

    [Fact]
    public async Task Unsupported_relationship_is_a_successful_explained_audit_not_a_fatal_export()
    {
        using var folder = new TestFolder();
        string input = folder.CreateInput(type: "TT_LOE");
        string destination = Path.Combine(folder.Path, "audit.csv");
        Assert.Equal(0, await RelationshipAuditCliApplication.RunAsync(
            ["--input", input, "--output", destination], new StringWriter(), new StringWriter()));
        List<string[]> records = ParseCsv(File.ReadAllText(destination));
        Assert.Equal("Unsupported", records[1][Array.IndexOf(records[0], "classification")]);
        Assert.Equal("", records[1][Array.IndexOf(records[0], "free_float")]);
        Assert.NotEmpty(records[1][Array.IndexOf(records[0], "reason_code")]);
    }

    [Fact]
    public void Csv_preserves_quotes_commas_newlines_typed_nulls_and_raw_field_states()
    {
        var assessment = new RelationshipFloatAssessment
        {
            FileName = "input,\"quoted\".xer", SourceNamespace = "namespace", SourceRowNumber = 1,
            Classification = RelationshipFloatClassification.Calculated, FloatHours = -1.25m,
            FloatDays = -0.15625m, Message = "first\r\nsecond, \"quoted\"", RawLag = "bad\tvalue",
            ProjectDataDate = new DateTime(2026, 9, 6, 17, 0, 0).AddTicks(123),
            InputEvidence = new Dictionary<string, RelationshipFieldEvidence>
            {
                ["successor_options.sched_retained_logic"] = new("", "ExplicitBlank"),
                ["relationship.lag_hr_cnt"] = new("bad\tvalue", "Malformed")
            }
        };
        using var output = new StringWriter(CultureInfo.GetCultureInfo("fr-FR"));
        RelationshipAuditCsv.Write([assessment], output);
        List<string[]> records = ParseCsv(output.ToString());
        Assert.Equal(2, records.Count);
        Assert.Equal(RelationshipAuditCsv.Columns.Count, records[1].Length);
        string Field(string name) => records[1][Array.IndexOf(records[0], name)];
        Assert.Equal(assessment.FileName, Field("FileName"));
        Assert.Equal(assessment.Message, Field("message"));
        Assert.Equal("-1.25", Field("free_float_hours"));
        Assert.Equal("-0.15625", Field("free_float"));
        Assert.Equal("", Field("effective_lag_hours"));
        Assert.Equal("2026-09-06 17:00:00.0000123", Field("project_data_date"));
        Assert.Equal("ExplicitBlank", Field("sched_retained_logic_state"));
        using JsonDocument evidence = JsonDocument.Parse(Field("input_evidence"));
        Assert.Equal("Malformed", evidence.RootElement.GetProperty("relationship.lag_hr_cnt").GetProperty("state").GetString());
        Assert.Equal("bad\tvalue", evidence.RootElement.GetProperty("relationship.lag_hr_cnt").GetProperty("raw_value").GetString());
    }

    [Fact]
    public void Empty_audit_is_header_only_and_uses_the_fixed_schema()
    {
        using var writer = new StringWriter();
        RelationshipAuditCsv.Write([], writer);
        List<string[]> rows = ParseCsv(writer.ToString());
        Assert.Single(rows);
        Assert.Equal(RelationshipAuditCsv.Columns, rows[0]);
    }

    [Fact]
    public void Existing_output_is_protected_unless_overwrite_is_explicit()
    {
        using var folder = new TestFolder();
        string destination = Path.Combine(folder.Path, "audit.csv");
        File.WriteAllText(destination, "original");
        Assert.Throws<IOException>(() => RelationshipAuditCsv.Publish([], destination, []));
        Assert.Equal("original", File.ReadAllText(destination));
        RelationshipAuditCsv.Publish([], destination, [], overwrite: true);
        Assert.StartsWith("audit_schema_version,", File.ReadAllText(destination), StringComparison.Ordinal);
        Assert.Single(Directory.GetFiles(folder.Path));
    }

    [Fact]
    public void Input_cannot_be_overwritten_even_with_explicit_overwrite()
    {
        using var folder = new TestFolder();
        string input = Path.Combine(folder.Path, "input.csv");
        File.WriteAllText(input, "input content");
        Assert.Throws<IOException>(() => RelationshipAuditCsv.Publish([], input, [input], overwrite: true));
        Assert.Equal("input content", File.ReadAllText(input));
    }

    [Theory]
    [InlineData("output.xer")]
    [InlineData("CON.csv")]
    [InlineData("LPT1.csv")]
    [InlineData("audit:stream.csv")]
    public void Unsafe_output_filenames_are_rejected(string filename)
    {
        using var folder = new TestFolder();
        Assert.Throws<ArgumentException>(() => RelationshipAuditCsv.Publish([], Path.Combine(folder.Path, filename), []));
        Assert.Empty(Directory.GetFiles(folder.Path));
    }

    [Fact]
    public void Missing_parent_is_not_created_implicitly()
    {
        using var folder = new TestFolder();
        string parent = Path.Combine(folder.Path, "missing");
        Assert.Throws<DirectoryNotFoundException>(() => RelationshipAuditCsv.Publish([], Path.Combine(parent, "audit.csv"), []));
        Assert.False(Directory.Exists(parent));
    }

    [Fact]
    public void Cancellation_during_preparation_preserves_original_and_removes_only_temporary_file()
    {
        using var folder = new TestFolder();
        using var cancellation = new CancellationTokenSource();
        string destination = Path.Combine(folder.Path, "audit.csv");
        File.WriteAllText(destination, "original");
        IEnumerable<RelationshipFloatAssessment> Interrupted()
        {
            yield return new RelationshipFloatAssessment();
            cancellation.Cancel();
            yield return new RelationshipFloatAssessment();
        }
        Assert.Throws<OperationCanceledException>(() => RelationshipAuditCsv.Publish(
            Interrupted(), destination, [], overwrite: true, cancellation.Token));
        Assert.Equal("original", File.ReadAllText(destination));
        Assert.Single(Directory.GetFiles(folder.Path));
    }

    [Fact]
    public void Generation_failure_does_not_publish_partial_output()
    {
        using var folder = new TestFolder();
        string destination = Path.Combine(folder.Path, "audit.csv");
        IEnumerable<RelationshipFloatAssessment> Failed()
        {
            yield return new RelationshipFloatAssessment();
            throw new InvalidOperationException("deliberate test failure");
        }
        Assert.Throws<InvalidOperationException>(() => RelationshipAuditCsv.Publish(Failed(), destination, []));
        Assert.Empty(Directory.GetFiles(folder.Path));
    }

    [Fact]
    public void Output_appearing_during_preparation_is_not_overwritten_without_permission()
    {
        using var folder = new TestFolder();
        string destination = Path.Combine(folder.Path, "audit.csv");
        IEnumerable<RelationshipFloatAssessment> WithConcurrentOutput()
        {
            yield return new RelationshipFloatAssessment();
            File.WriteAllText(destination, "new output from another writer");
        }
        Assert.Throws<IOException>(() => RelationshipAuditCsv.Publish(WithConcurrentOutput(), destination, []));
        Assert.Equal("new output from another writer", File.ReadAllText(destination));
        Assert.Single(Directory.GetFiles(folder.Path));
    }

    [SymlinkFact]
    public void Linked_output_input_and_parent_are_refused_without_touching_targets()
    {
        using var folder = new TestFolder();
        string original = Path.Combine(folder.Path, "original.csv");
        string link = Path.Combine(folder.Path, "linked.csv");
        File.WriteAllText(original, "original source");
        File.CreateSymbolicLink(link, original);
        Assert.Throws<IOException>(() => RelationshipAuditCsv.Publish([], link, [], overwrite: true));
        Assert.Throws<IOException>(() => RelationshipAuditCsv.Publish([], Path.Combine(folder.Path, "new.csv"), [link]));
        Assert.Equal("original source", File.ReadAllText(original));

        string linkedDirectory = Path.Combine(folder.Path, "linked-parent");
        Directory.CreateSymbolicLink(linkedDirectory, folder.Path);
        Assert.Throws<IOException>(() => RelationshipAuditCsv.Publish([], Path.Combine(linkedDirectory, "new.csv"), []));
        // Remove the link explicitly, never recursively traverse its directory target.
        Directory.Delete(linkedDirectory);
        Assert.False(File.Exists(Path.Combine(folder.Path, "new.csv")));
    }

    [SymlinkFact]
    public void Dangling_output_link_is_refused()
    {
        using var folder = new TestFolder();
        string link = Path.Combine(folder.Path, "linked.csv");
        File.CreateSymbolicLink(link, Path.Combine(folder.Path, "absent.csv"));
        Assert.Throws<IOException>(() => RelationshipAuditCsv.Publish([], link, [], overwrite: true));
        Assert.NotNull(new FileInfo(link).LinkTarget);
        Assert.False(File.Exists(Path.Combine(folder.Path, "absent.csv")));
    }

    internal static List<string[]> ParseCsv(string content)
    {
        var rows = new List<string[]>();
        var row = new List<string>();
        var field = new StringBuilder();
        bool quoted = false;
        for (int i = 0; i < content.Length; i++)
        {
            char c = content[i];
            if (c == '"')
            {
                if (quoted && i + 1 < content.Length && content[i + 1] == '"') { field.Append('"'); i++; }
                else quoted = !quoted;
            }
            else if (c == ',' && !quoted) { row.Add(field.ToString()); field.Clear(); }
            else if (c == '\n' && !quoted)
            {
                if (field.Length > 0 && field[^1] == '\r') field.Length--;
                row.Add(field.ToString()); field.Clear(); rows.Add(row.ToArray()); row.Clear();
            }
            else field.Append(c);
        }
        Assert.False(quoted);
        return rows;
    }

    private sealed class TestFolder : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "xer-audit-tests-" + Guid.NewGuid().ToString("N"));
        public TestFolder() => Directory.CreateDirectory(Path);
        public string CreateInput(string type = "TT_Task")
        {
            string shift = "(0||0(s|08:00|f|16:00)())";
            string days = string.Concat(Enumerable.Range(1, 7).Select(day => $"(0||{day}()({shift}))"));
            string calendar = $"(0||CalendarData()((0||DaysOfWeek()({days}))(0||Exceptions()())))";
            string content = "ERMHDR\t8.0\t2026-09-06\r\n"
                + "%T\tPROJECT\r\n%F\tproj_id\tclndr_id\tlast_recalc_date\r\n%R\tP1\tC1\t2026-09-01 08:00\r\n"
                + "%T\tCALENDAR\r\n%F\tclndr_id\tclndr_name\tclndr_type\tday_hr_cnt\tclndr_data\r\n%R\tC1\tTest\tCA_Base\t8\t" + calendar + "\r\n"
                + "%T\tSCHEDOPTIONS\r\n%F\tproj_id\tsched_calendar_on_relationship_lag\tsched_retained_logic\tsched_progress_override\r\n%R\tP1\trcal_Predecessor\tY\tN\r\n"
                + "%T\tTASK\r\n%F\ttask_id\tproj_id\tclndr_id\ttask_type\tstatus_code\trestart_date\treend_date\r\n"
                + $"%R\tT1\tP1\tC1\t{type}\tTK_NotStart\t2026-09-07 08:00\t2026-09-07 16:00\r\n"
                + "%R\tT2\tP1\tC1\tTT_Task\tTK_NotStart\t2026-09-08 08:00\t2026-09-08 16:00\r\n"
                + "%T\tTASKPRED\r\n%F\ttask_pred_id\tproj_id\tpred_proj_id\ttask_id\tpred_task_id\tpred_type\tlag_hr_cnt\r\n%R\tR1\tP1\tP1\tT2\tT1\tPR_FS\t0\r\n%E\r\n";
            string file = System.IO.Path.Combine(Path, "input.xer");
            File.WriteAllText(file, content, new UTF8Encoding(false));
            return file;
        }
        public void Dispose() => Directory.Delete(Path, recursive: true);
    }
}

/// <summary>Report the platform gate as a real skipped test, not a silent success.</summary>
public sealed class SymlinkFactAttribute : FactAttribute
{
    private static readonly Lazy<bool> Supported = new(CheckSupport);
    public SymlinkFactAttribute()
    {
        if (!Supported.Value) Skip = "This host does not permit unprivileged symbolic-link creation.";
    }
    private static bool CheckSupport()
    {
        string folder = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "xer-audit-link-probe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        string link = System.IO.Path.Combine(folder, "link");
        try
        {
            File.CreateSymbolicLink(link, System.IO.Path.Combine(folder, "absent"));
            return true;
        }
        catch (UnauthorizedAccessException) { return false; }
        catch (PlatformNotSupportedException) { return false; }
        catch (IOException ex) when ((ex.HResult & 0xFFFF) == 1314) { return false; }
        finally
        {
            File.Delete(link);
            Directory.Delete(folder);
        }
    }
}
