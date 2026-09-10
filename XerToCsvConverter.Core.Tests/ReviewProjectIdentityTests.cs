using XerToCsvConverter.ProgrammeReview;
using XerToCsvConverter.TenderReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ReviewProjectIdentityTests
{
    [Theory]
    [InlineData("QAC000623-01-02", "QAC000623-01-02")]
    [InlineData("  ne Part B  ", "NE PART B")]
    [InlineData("J_5001", "J_5001")]
    [InlineData("Å / 北 ", "%C3%85 %2F %E5%8C%97")]
    [InlineData("A::B|C%20D", "A%3A%3AB%7CC%2520D")]
    [InlineData("../../a\\b", "%2E%2E%2F%2E%2E%2FA%5CB")]
    [InlineData("CON.txt", "CON%2ETXT")]
    [InlineData("Phase, \"A\"", "PHASE%2C %22A%22")]
    public void Profiles_accept_business_names_and_escape_only_identity_components(string supplied, string component)
    {
        string normalized = supplied.Trim().ToUpperInvariant();
        Assert.Equal(normalized, ProgrammeReviewNaming.NormalizeProjectCode(supplied));
        Assert.Equal(normalized, TenderReviewNaming.NormalizeProjectCode(supplied));
        Assert.Equal(normalized, Uri.UnescapeDataString(component));
        DateOnly date = new(2026, 9, 10);
        Assert.Equal($"{component}-TENDER-20260910.xer", TenderReviewNaming.CreateCanonicalFilename(supplied, date));
        Assert.Equal($"{component}-C-BL01_20260910.xer", ProgrammeReviewNaming.CreateCanonicalFilename(supplied, "C", "BL01", date));
        Assert.Equal($"CSV::{component}::TENDER::20260910::42",
            TenderReviewNaming.NamespaceKey("source.42", "source", supplied, date));
        Assert.Equal($"CSV::{component}::C::BL01::42",
            ProgrammeReviewNaming.NamespaceKey("source.xer.42", "source.xer", supplied, "C", "BL01"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" \t\r\n ")]
    public void Profiles_reject_missing_business_identity(string? code)
    {
        Assert.Throws<ProgrammeReviewValidationException>(() => ProgrammeReviewNaming.NormalizeProjectCode(code));
        Assert.Throws<TenderReviewValidationException>(() => TenderReviewNaming.NormalizeProjectCode(code));
    }

    [Fact]
    public void Punctuation_whitespace_and_literal_escapes_do_not_collapse_project_identities()
    {
        string[] codes = ["A/B", "A\\B", "A_B", "A B", "A  B", "A-B", "A%2FB", "A::B", "A|B", "A%3A%3AB", "北", "%E5%8C%97"];
        var keys = codes.Select(code => TenderReviewNaming.NamespaceKey("42", "source", code, new DateOnly(2026, 9, 10))).ToArray();
        Assert.Equal(codes.Length, keys.Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.All(keys, key =>
        {
            Assert.DoesNotContain('|', key);
            Assert.Equal(5, key.Split("::", StringSplitOptions.None).Length);
        });
    }

    [Fact]
    public void Long_names_have_bounded_deterministic_filenames_and_complete_distinct_namespaces()
    {
        string code = new string('北', 200);
        string component = ReviewProjectIdentity.FileComponent(code);
        Assert.Matches("^~[0-9A-F]{64}$", component);
        Assert.Equal(component, ReviewProjectIdentity.FileComponent(code));
        Assert.NotEqual(component, ReviewProjectIdentity.FileComponent(code + "A"));
        Assert.NotEqual(component, ReviewProjectIdentity.FileComponent(component));
        Assert.Equal(code, Uri.UnescapeDataString(ReviewProjectIdentity.EncodeComponent(code)));
        Assert.True(TenderReviewNaming.CreateCanonicalFilename(code, new DateOnly(2026, 9, 10)).Length < 150);
        Assert.Contains(ReviewProjectIdentity.EncodeComponent(code),
            ProgrammeReviewNaming.NamespaceKey("42", "source.xer", code, "C", "BL01"), StringComparison.Ordinal);
    }
}
