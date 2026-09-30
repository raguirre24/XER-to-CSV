using System.Text.Json;

namespace XerToCsvConverter.Core.Tests;

public sealed partial class RelationshipAssessmentTests
{
    [Fact]
    public void Cached_relationship_inputs_match_frozen_full_evidence_for_release_matrix()
    {
        foreach (object[] item in ReleaseMatrix())
        {
            var fixture = new Schedule();
            fixture.Relationship["pred_type"] = (string)item[0];
            SetStatus(fixture.Predecessor, (string)item[1]);
            SetStatus(fixture.Successor, (string)item[2]);
            fixture.SetMode((string)item[3]);
            AssertFrozenRelationshipEvidence(fixture.Store());
        }
    }

    [Fact]
    public void Cached_relationship_inputs_preserve_absent_blank_malformed_ambiguous_and_suspension_evidence()
    {
        Action<Schedule>[] variants =
        [
            fixture => fixture.Predecessor.Remove("restart_date"),
            fixture => fixture.Predecessor["restart_date"] = " ",
            fixture => fixture.Predecessor["restart_date"] = "malformed preferred start",
            fixture => fixture.Predecessor["reend_date"] = "malformed preferred finish",
            fixture => fixture.Options.Remove("sched_retained_logic"),
            fixture => fixture.Options["sched_retained_logic"] = " ",
            fixture => fixture.Options["sched_retained_logic"] = "bad",
            fixture => fixture.Options.Remove("sched_calendar_on_relationship_lag"),
            fixture => fixture.Options["sched_calendar_on_relationship_lag"] = " ",
            fixture => fixture.Options["sched_calendar_on_relationship_lag"] = "unknown calendar",
            fixture => fixture.Project.Remove("last_recalc_date"),
            fixture => fixture.Project["last_recalc_date"] = "malformed data date",
            fixture => fixture.ExtraProject = new Dictionary<string, string>(fixture.Project),
            fixture => fixture.ExtraOptions = new Dictionary<string, string>(fixture.Options),
            fixture => fixture.Predecessor["suspend_date"] = "malformed suspension",
            fixture => fixture.Predecessor["suspend_date"] = "2026-01-05 08:00",
            fixture => { fixture.Predecessor["suspend_date"] = "2026-01-05 08:00"; fixture.Predecessor["resume_date"] = "2026-01-05 12:00"; },
            fixture => { fixture.Predecessor["suspend_date"] = "2026-01-05 08:00"; fixture.Predecessor["resume_date"] = "2026-01-06 08:00"; },
            fixture => { fixture.Predecessor["suspend_date"] = "2026-01-06 08:00"; fixture.Predecessor["resume_date"] = "2026-01-05 08:00"; }
        ];
        foreach (Action<Schedule> change in variants)
        foreach (string status in new[] { "TK_NotStart", "TK_Active" })
        {
            var fixture = new Schedule();
            SetStatus(fixture.Predecessor, status);
            SetStatus(fixture.Successor, "TK_Active");
            fixture.Relationship["lag_hr_cnt"] = "2";
            change(fixture);
            AssertFrozenRelationshipEvidence(fixture.Store());
        }
    }

    [Fact]
    public void Public_assessment_rebuilds_task_project_and_options_snapshots_after_caller_edits()
    {
        XerDataStore store = ActiveFixture().Store();
        var transformer = new XerTransformer(store);
        RelationshipFloatAssessment before = Assert.Single(transformer.AssessRelationships());
        Assert.Equal("CalculatedRetainedRemainingRelationship", before.ReasonCode);
        XerTable tasks = store.GetTable("TASK")!;
        DataRow predecessor = tasks.Rows[0];
        predecessor.Fields[tasks.FieldIndexes["reend_date"]] = "malformed edited date";
        AssertFrozenRelationshipEvidence(store, transformer);
        predecessor.Fields[tasks.FieldIndexes["reend_date"]] = "2026-01-05 17:00";
        XerTable options = store.GetTable("SCHEDOPTIONS")!;
        options.Rows[0].Fields[options.FieldIndexes["sched_retained_logic"]] = "N";
        options.Rows[0].Fields[options.FieldIndexes["sched_progress_override"]] = "Y";
        AssertFrozenRelationshipEvidence(store, transformer);
        XerTable projects = store.GetTable("PROJECT")!;
        projects.Rows[0].Fields[projects.FieldIndexes["last_recalc_date"]] = "malformed edited data date";
        AssertFrozenRelationshipEvidence(store, transformer);
    }

    [Fact]
    public void Cached_options_preserve_truncated_cells_and_options_without_a_project_row()
    {
        foreach (string tableName in new[] { "SCHEDOPTIONS", "PROJECT", "TASK" })
        {
            XerDataStore store = ActiveFixture().Store();
            XerTable original = store.GetTable(tableName)!;
            var truncated = new XerTable(tableName);
            truncated.SetHeaders(original.Headers!.ToArray());
            foreach (DataRow row in original.Rows)
                truncated.AddRow(new DataRow(row.Fields.Take(1).ToArray(), row.SourceFilename,
                    row.SourceToken, row.OriginalSourceFilename));
            store.AddTable(truncated);
            AssertFrozenRelationshipEvidence(store);
        }
        XerDataStore optionsOnly = ActiveFixture().Store();
        var projects = new XerTable("PROJECT");
        projects.SetHeaders(["proj_id", "clndr_id", "last_recalc_date"]);
        optionsOnly.AddTable(projects);
        AssertFrozenRelationshipEvidence(optionsOnly);
    }

    [Theory]
    [InlineData("J5001_C_BL01_2026-01-31.xer")]
    [InlineData("J5001_C_BL01_Retained_2026-01-31.xer")]
    public void Published_review_fixtures_match_frozen_relationship_assessments_and_full_evidence(string name)
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "review", "fixtures", name)))
            directory = directory.Parent;
        Assert.NotNull(directory);
        using var input = File.OpenRead(Path.Combine(directory.FullName, "review", "fixtures", name));
        XerDataStore store = new XerParser().ParseXerStream(input, name, null, CancellationToken.None);
        AssertFrozenRelationshipEvidence(store);
    }

    private static void AssertFrozenRelationshipEvidence(XerDataStore store, XerTransformer? transformer = null)
    {
        IReadOnlyList<RelationshipFloatAssessment> expected = new FrozenRelationshipAssessmentOracle(store).AssessRelationships();
        transformer ??= new XerTransformer(store);
        IReadOnlyList<RelationshipFloatAssessment> actual = transformer.AssessRelationships();
        Assert.Equal(JsonSerializer.Serialize(expected), JsonSerializer.Serialize(actual));
        AssertExportAgreement(transformer, expected);
    }
}
