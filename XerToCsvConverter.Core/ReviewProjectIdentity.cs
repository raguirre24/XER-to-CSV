using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace XerToCsvConverter;

/// <summary>Separates a review project's business code from its filename and key components.</summary>
public static class ReviewProjectIdentity
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private const int MaximumFileComponentLength = 100;

    public static string NormalizeCode(string? value)
    {
        string code = value?.Trim().ToUpperInvariant() ?? string.Empty;
        if (code.Length == 0)
            throw new ArgumentException("Project code is required.", nameof(value));
        return code;
    }

    /// <summary>
    /// Escapes punctuation and Unicode as UTF-8 percent bytes. Literal percent signs are also
    /// escaped, so distinct normalized names cannot share a key. Spaces and hyphens stay readable.
    /// This component is opaque to reporting joins; ProjectCode carries the unescaped business code.
    /// </summary>
    public static string EncodeComponent(string normalizedCode)
    {
        var encoded = new StringBuilder();
        foreach (byte value in StrictUtf8.GetBytes(normalizedCode))
        {
            if (value is >= (byte)'A' and <= (byte)'Z' or >= (byte)'a' and <= (byte)'z'
                or >= (byte)'0' and <= (byte)'9' or (byte)'_' or (byte)'-' or (byte)' ')
                encoded.Append((char)value);
            else
                encoded.Append('%').Append(value.ToString("X2", CultureInfo.InvariantCulture));
        }
        return encoded.ToString();
    }

    /// <summary>
    /// Bounds generated filenames/folders independently of the unrestricted business code.
    /// The reserved '~' prefix cannot collide with a literal code, where '~' becomes '%7E'.
    /// </summary>
    public static string FileComponent(string normalizedCode)
    {
        string encoded = EncodeComponent(normalizedCode);
        return encoded.Length <= MaximumFileComponentLength
            ? encoded
            : "~" + Convert.ToHexString(SHA256.HashData(StrictUtf8.GetBytes(normalizedCode)));
    }
}
