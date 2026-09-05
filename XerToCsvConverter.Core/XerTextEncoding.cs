using System.Text;

namespace XerToCsvConverter;

/// <summary>
/// A BOM is an explicit encoding declaration and must never install a replacement decoder.
/// BOM-less input is tried as strict UTF-8, then Windows-1252 for legacy P6 exports.
/// </summary>
internal static class XerTextEncoding
{
    internal static readonly Encoding Utf8 = new UTF8Encoding(false, true);
    internal static readonly Encoding Windows1252 = CreateWindows1252();

    internal readonly record struct Selection(Encoding Encoding, bool HasBom, long ContentPosition);

    internal static Selection Detect(Stream stream)
    {
        long start = stream.Position;
        Span<byte> prefix = stackalloc byte[4];
        int count = 0;
        while (count < prefix.Length)
        {
            int value = stream.ReadByte();
            if (value < 0) break;
            prefix[count++] = (byte)value;
        }

        Encoding encoding = Utf8;
        int skip = 0;
        if (count >= 4 && prefix[0] == 0xff && prefix[1] == 0xfe && prefix[2] == 0 && prefix[3] == 0)
            (encoding, skip) = (new UTF32Encoding(false, false, true), 4);
        else if (count >= 4 && prefix[0] == 0 && prefix[1] == 0 && prefix[2] == 0xfe && prefix[3] == 0xff)
            (encoding, skip) = (new UTF32Encoding(true, false, true), 4);
        else if (count >= 3 && prefix[0] == 0xef && prefix[1] == 0xbb && prefix[2] == 0xbf)
            skip = 3;
        else if (count >= 2 && prefix[0] == 0xff && prefix[1] == 0xfe)
            (encoding, skip) = (new UnicodeEncoding(false, false, true), 2);
        else if (count >= 2 && prefix[0] == 0xfe && prefix[1] == 0xff)
            (encoding, skip) = (new UnicodeEncoding(true, false, true), 2);

        stream.Position = start + skip;
        return new Selection(encoding, skip != 0, stream.Position);
    }

    private static Encoding CreateWindows1252()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        return Encoding.GetEncoding(1252, EncoderFallback.ExceptionFallback, DecoderFallback.ExceptionFallback);
    }
}
