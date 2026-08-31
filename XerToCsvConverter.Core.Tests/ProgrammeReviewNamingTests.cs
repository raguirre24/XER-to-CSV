using XerToCsvConverter.ProgrammeReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ProgrammeReviewNamingTests
{

    [Fact]
    public void Canonical_name_and_namespace_do_not_depend_on_legacy_filename_shape()
    {
        string canonical = ProgrammeReviewNaming.CreateCanonicalFilename(
            "j_123", "c", "2607", new DateOnly(2026, 7, 31));
        Assert.Equal("J_123-C-2607_20260731.xer", canonical);
        Assert.Equal(
            "CSV::J_123::C::2607::42",
            ProgrammeReviewNaming.NamespaceKey(
                "2607 legacy source.xer.42", "2607 legacy source.xer",
                "j_123", "c", "2607"));
    }

    [Theory]
    [InlineData("legacy.xer.42|7")]
    [InlineData("legacy.xer.42::7")]
    public void Namespace_rejects_native_ids_that_are_not_path_safe(string sourceKey)
    {
        ProgrammeReviewValidationException error = Assert.Throws<ProgrammeReviewValidationException>(() =>
            ProgrammeReviewNaming.NamespaceKey(sourceKey, "legacy.xer", "J123", "C", "2607"));

        Assert.Contains("reserved", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Namespace_is_deterministic_and_separates_project_programme_and_snapshot()
    {
        const string sourceFile = "legacy.xer";
        const string sourceKey = "legacy.xer.42";

        string canonical = ProgrammeReviewNaming.NamespaceKey(sourceKey, sourceFile, "J123", "C", "2607");
        string normalizedEquivalent = ProgrammeReviewNaming.NamespaceKey(sourceKey, sourceFile, "j123", "c", "2607");
        string otherSnapshot = ProgrammeReviewNaming.NamespaceKey(sourceKey, sourceFile, "J123", "C", "2608");
        string otherProject = ProgrammeReviewNaming.NamespaceKey(sourceKey, sourceFile, "J124", "C", "2607");
        string otherProgramme = ProgrammeReviewNaming.NamespaceKey(sourceKey, sourceFile, "J123", "T", "2607");

        Assert.Equal("CSV::J123::C::2607::42", canonical);
        Assert.Equal(canonical, normalizedEquivalent);
        Assert.Equal(4, new[] { canonical, otherSnapshot, otherProject, otherProgramme }.Distinct(StringComparer.Ordinal).Count());
        Assert.DoesNotContain('|', canonical);
    }

    [Fact]
    public void Namespace_is_safe_for_Dax_path()
    {
        string key = ProgrammeReviewNaming.NamespaceKey(
            "source.xer.42",
            "source.xer",
            "J123",
            "C",
            "2607");

        Assert.DoesNotContain('|', key);
    }

    [Fact]
    public void Namespace_rejects_the_Dax_path_separator_in_source_values()
    {
        Assert.Throws<ProgrammeReviewValidationException>(() =>
            ProgrammeReviewNaming.NamespaceKey(
                "source.xer.4|2",
                "source.xer",
                "J123",
                "C",
                "2607"));
    }

    [Fact]
    public void Resolve_ranks_letter_revision_and_discards_pre_anchor_updates()
    {
        ProgrammeReviewBundleRequest request = Request(new[]
        {
            Snapshot("base-a.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01-A", "2026-01-31", "2026-01-31"),
            Snapshot("base-b.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01-B", "2026-02-28", "2026-02-28"),
            Snapshot("old.xer", ProgrammeReviewSnapshotKind.Update, "2601", "2026-01-01", "2026-01-30"),
            Snapshot("kept.xer", ProgrammeReviewSnapshotKind.Update, "2603", "2026-03-01", "2026-03-27")
        });

        ResolvedProgrammeReviewRequest resolved = ProgrammeReviewNaming.Resolve(request, snapshot => snapshot.SourceSha256!);

        Assert.Equal(new[] { "base-b.xer", "kept.xer" }, resolved.Snapshots.Select(s => s.OriginalXerFilename));
        Assert.Equal("BL01-B", resolved.Snapshots[0].SnapshotTag);
        Assert.True(resolved.Snapshots[0].EffectiveUpdateDate < resolved.Snapshots[1].EffectiveUpdateDate);
    }

    [Fact]
    public void Resolve_rejects_duplicate_retained_tags()
    {
        ProgrammeReviewBundleRequest request = Request(new[]
        {
            Snapshot("baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-31"),
            Snapshot("update-a.xer", ProgrammeReviewSnapshotKind.Update, "2602", "2026-02-01", "2026-02-20"),
            Snapshot("update-b.xer", ProgrammeReviewSnapshotKind.Update, "2602", "2026-02-01", "2026-02-21")
        });

        Assert.Throws<ProgrammeReviewValidationException>(() =>
            ProgrammeReviewNaming.Resolve(request, snapshot => snapshot.SourceSha256!));
    }

    [Fact]
    public void Baseline_effective_update_is_month_end_shifted_around_retained_collision()
    {
        ProgrammeReviewSnapshot baseline = Snapshot(
            "baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-02-01", "2026-02-01");
        ProgrammeReviewSnapshot update = Snapshot(
            "update.xer", ProgrammeReviewSnapshotKind.Update, "2602", "2026-02-28", "2026-02-27") with
        {
            UpdateDate = new DateOnly(2026, 2, 28)
        };

        ResolvedProgrammeReviewRequest resolved = ProgrammeReviewNaming.Resolve(
            Request(new[] { baseline, update }), snapshot => snapshot.SourceSha256!);

        Assert.Equal(new DateOnly(2026, 1, 31), resolved.Snapshots.Single(s => s.SnapshotKind == ProgrammeReviewSnapshotKind.Baseline).EffectiveUpdateDate);
        Assert.Equal(new DateOnly(2026, 2, 28), resolved.Snapshots.Single(s => s.SnapshotKind == ProgrammeReviewSnapshotKind.Update).EffectiveUpdateDate);
    }

    [Fact]
    public void Baseline_cannot_override_Athena_month_end_seed()
    {
        ProgrammeReviewSnapshot baseline = Snapshot(
            "baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-15", "2026-01-15") with
        {
            UpdateDate = new DateOnly(2026, 1, 15)
        };

        Assert.Throws<ProgrammeReviewValidationException>(() =>
            ProgrammeReviewNaming.Resolve(Request(new[] { baseline }), snapshot => snapshot.SourceSha256!));
    }

    internal static ProgrammeReviewBundleRequest Request(IReadOnlyList<ProgrammeReviewSnapshot> snapshots) => new()
    {
        ProjectCode = "J123",
        ProjectName = "Test Project",
        ProgrammeType = "C",
        ParserVersion = "test",
        ExportedAtUtc = new DateTimeOffset(2026, 8, 29, 1, 2, 3, TimeSpan.Zero),
        Snapshots = snapshots
    };

    internal static ProgrammeReviewSnapshot Snapshot(
        string file,
        ProgrammeReviewSnapshotKind kind,
        string tag,
        string monthUpdate,
        string dataDate) => new()
    {
        OriginalXerFilename = file,
        SnapshotKind = kind,
        SnapshotTag = tag,
        MonthUpdate = DateOnly.Parse(monthUpdate),
        DataDate = DateOnly.Parse(dataDate),
        SourceSha256 = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(file))).ToLowerInvariant()
    };
}
