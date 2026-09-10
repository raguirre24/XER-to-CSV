using System.Security.Cryptography;
using System.Text;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class TenderReviewNamingTests
{
    [Fact]
    public void Local_status_date_uses_frozen_local_calendar_day()
    {
        var time = new FrozenTimeProvider(
            new DateTimeOffset(2026, 9, 4, 11, 30, 0, TimeSpan.Zero),
            TimeZoneInfo.CreateCustomTimeZone("NZ-test", TimeSpan.FromHours(12), "NZ-test", "NZ-test"));

        Assert.Equal(new DateOnly(2026, 9, 4), DateOnly.FromDateTime(time.GetUtcNow().DateTime));
        Assert.Equal(new DateOnly(2026, 9, 4), TenderReviewDefaults.GetLocalStatusDate(time));

        var crossesMidnight = new FrozenTimeProvider(
            new DateTimeOffset(2026, 9, 4, 12, 30, 0, TimeSpan.Zero),
            TimeZoneInfo.CreateCustomTimeZone("NZ-test-2", TimeSpan.FromHours(12), "NZ-test-2", "NZ-test-2"));
        Assert.Equal(new DateOnly(2026, 9, 5), TenderReviewDefaults.GetLocalStatusDate(crossesMidnight));
    }

    [Fact]
    public void Canonical_and_compact_keys_use_status_date_not_source_name()
    {
        DateOnly statusDate = new(2026, 9, 5);
        string token = TenderReviewNaming.CreateSourceToken(0);

        Assert.Equal("tender-source-000001", token);
        Assert.Equal("J5001-TENDER-20260905.xer",
            TenderReviewNaming.CreateCanonicalFilename(" j5001 ", statusDate));
        Assert.Equal("CSV::J5001::TENDER::20260905::42",
            TenderReviewNaming.NamespaceKey($"{token}.42", token, "j5001", statusDate));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" \t ")]
    [InlineData(null)]
    public void Project_code_requires_a_nonblank_value(string? value)
    {
        TenderReviewValidationException error = Assert.Throws<TenderReviewValidationException>(() =>
            TenderReviewNaming.NormalizeProjectCode(value));
        Assert.Contains("required", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("J_5001", "J_5001")]
    [InlineData("  j_5001  ", "J_5001")]
    [InlineData("NE Part B", "NE PART B")]
    [InlineData("  ne   part   b  ", "NE   PART   B")]
    [InlineData("QAC000623-01-02", "QAC000623-01-02")]
    [InlineData("J.5001", "J.5001")]
    [InlineData("Å5001 / 北", "Å5001 / 北")]
    [InlineData("A::B|C%20D", "A::B|C%20D")]
    [InlineData("C5001", "C5001")]
    public void Project_code_preserves_name_characters_and_only_trims_and_uppercases(string input, string expected)
    {
        Assert.Equal(expected, TenderReviewNaming.NormalizeProjectCode(input));
    }

    [Fact]
    public void Tender_project_identity_is_exact_without_C_and_J_aliases()
    {
        Assert.False(TenderReviewNaming.IsSameProjectIdentity("C5001", "j5001"));
        Assert.False(TenderReviewNaming.IsSameProjectIdentity("J5001", "C5001"));
        Assert.False(TenderReviewNaming.IsSameProjectIdentity("CIVIL", "JIVIL"));
        Assert.False(TenderReviewNaming.IsSameProjectIdentity("A B", "A  B"));
        Assert.False(TenderReviewNaming.IsSameProjectIdentity("NE Part B", "NE_PART_B"));
        Assert.False(TenderReviewNaming.IsSameProjectIdentity("NE_PART_B", "NE Part B"));
        Assert.True(TenderReviewNaming.IsSameProjectIdentity("qac000623-01-02", "QAC000623-01-02"));
        Assert.False(TenderReviewNaming.IsSameProjectIdentity("QAC000623-01-02", "QAC000623_01_02"));
        Assert.False(TenderReviewNaming.IsSameProjectIdentity("5001", "J5001"));
        Assert.False(TenderReviewNaming.IsSameProjectIdentity("CJ5001", "J5001"));
    }

    [Fact]
    public async Task Metadata_reader_reads_project_code_and_name_from_xer_stream()
    {
        string xer = "ERMHDR\t23.12\t2026-09-05\n%T\tPROJECT\n%F\tproj_id\tproj_short_name\n%R\tP1\tNE Part B\n%T\tPROJWBS\n%F\twbs_id\tparent_wbs_id\twbs_name\n%R\tW1\t\tNorth East Highway Part B\n%E\n";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(xer));
        TenderReviewProjectIdentity? identity = await TenderReviewXerMetadataReader.ReadProjectIdentityAsync(stream);
        Assert.NotNull(identity);
        Assert.Equal("NE Part B", identity.ProjectCode);
        Assert.Equal("North East Highway Part B", identity.ProjectName);
        Assert.Equal("NE PART B", TenderReviewNaming.NormalizeProjectCode(identity.ProjectCode));
    }

    [Fact]
    public void Resolve_preserves_order_and_accepts_repeated_names_and_hashes()
    {
        string duplicateHash = Hash("same-content");
        TenderReviewBundleRequest request = Request(new[]
        {
            Source(0, "same.xer", "2026-09-05", duplicateHash),
            Source(1, "same.xer", "2026-09-06", duplicateHash)
        });

        ResolvedTenderReviewRequest resolved = TenderReviewNaming.Resolve(
            request, source => source.SourceSha256!);

        Assert.Equal(new[] { 0, 1 }, resolved.Sources.Select(source => source.InputIndex));
        Assert.Equal(new[] { "same.xer", "same.xer" },
            resolved.Sources.Select(source => source.OriginalXerFilename));
        Assert.All(resolved.Sources, source => Assert.Equal(duplicateHash, source.SourceSha256));
        Assert.Matches("^J5001_TENDER_20260905T010203Z_[a-f0-9]{8}$", resolved.BundleId);
    }

    [Fact]
    public void Resolve_rejects_duplicate_stage_dates_and_case_colliding_tokens()
    {
        string hash = Hash("same");
        TenderReviewValidationException duplicateDate = Assert.Throws<TenderReviewValidationException>(() =>
            TenderReviewNaming.Resolve(Request(new[]
            {
                Source(0, "a.xer", "2026-09-05", hash),
                Source(1, "b.xer", "2026-09-05", hash)
            }), source => source.SourceSha256!));
        Assert.Contains("unique status_date", duplicateDate.Message, StringComparison.OrdinalIgnoreCase);

        TenderReviewSource first = Source(0, "a.xer", "2026-09-05", hash) with
        {
            SourceToken = "Tender-Source"
        };
        TenderReviewSource second = Source(1, "b.xer", "2026-09-06", hash) with
        {
            SourceToken = "tender-source"
        };
        TenderReviewValidationException duplicateToken = Assert.Throws<TenderReviewValidationException>(() =>
            TenderReviewNaming.Resolve(Request(new[] { first, second }), source => source.SourceSha256!));
        Assert.Contains("source token", duplicateToken.Message, StringComparison.OrdinalIgnoreCase);
    }

    internal static TenderReviewBundleRequest Request(IReadOnlyList<TenderReviewSource> sources) => new()
    {
        ProjectCode = "J5001",
        ProjectName = "Tender Test Project",
        // General schedule fixtures have known manual metadata. Blank-State behaviour
        // is tested explicitly, without adding unrelated warnings to calculation assertions.
        State = "NSW",
        ParserVersion = "test",
        ExportedAtUtc = new DateTimeOffset(2026, 9, 5, 1, 2, 3, TimeSpan.Zero),
        Sources = sources
    };

    internal static TenderReviewSource Source(
        int index,
        string originalFilename,
        string statusDate,
        string? sourceHash = null) => new()
    {
        SourceToken = TenderReviewNaming.CreateSourceToken(index),
        OriginalXerFilename = originalFilename,
        StatusDate = DateOnly.Parse(statusDate),
        SourceSha256 = sourceHash ?? Hash($"source-{index}")
    };

    internal static string Hash(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();

    private sealed class FrozenTimeProvider(DateTimeOffset utcNow, TimeZoneInfo localTimeZone)
        : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;
        public override TimeZoneInfo LocalTimeZone => localTimeZone;
    }
}
