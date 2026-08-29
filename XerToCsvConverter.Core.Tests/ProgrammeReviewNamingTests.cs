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
            "CSV|J_123_C_20260829T010203Z_deadbeef|J_123-C-2607_20260731.xer.42",
            ProgrammeReviewNaming.NamespaceKey(
                "2607 legacy source.xer.42", "2607 legacy source.xer",
                "J_123_C_20260829T010203Z_deadbeef", canonical));
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
