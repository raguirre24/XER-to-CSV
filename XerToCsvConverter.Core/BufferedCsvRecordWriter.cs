using System.Buffers;

namespace XerToCsvConverter;

/// <summary>Operation-owned reusable record buffer for the review CSV contract.</summary>
internal sealed class BufferedCsvRecordWriter(TextWriter writer)
{
    private static readonly SearchValues<char> Special = SearchValues.Create(",\"\r\n");
    private char[] _buffer = new char[4096];
    private int _length;
    private bool _first;

    internal void WriteHeader(IReadOnlyList<string> names)
    {
        Begin();
        foreach (string name in names) Append(name);
        End();
    }

    internal void WriteProjected(IReadOnlyList<string> names, IReadOnlyDictionary<string, string> values)
    {
        Begin();
        foreach (string name in names) Append(values[name]);
        End();
    }

    private void Begin() { _length = 0; _first = true; }

    private void Append(string? value)
    {
        value ??= string.Empty;
        int required = checked(_length + value.Length * 2 + 3);
        if (required > _buffer.Length) Array.Resize(ref _buffer, Math.Max(required, checked(_buffer.Length * 2)));
        if (!_first) _buffer[_length++] = ',';
        _first = false;
        ReadOnlySpan<char> span = value.AsSpan();
        bool quote = span.IndexOfAny(Special) >= 0
            || span.Length > 0 && (char.IsWhiteSpace(span[0]) || char.IsWhiteSpace(span[^1]));
        if (quote) _buffer[_length++] = '"';
        if (quote)
        {
            while (span.IndexOf('"') is int position && position >= 0)
            {
                span[..position].CopyTo(_buffer.AsSpan(_length));
                _length += position;
                _buffer[_length++] = '"';
                _buffer[_length++] = '"';
                span = span[(position + 1)..];
            }
        }
        span.CopyTo(_buffer.AsSpan(_length));
        _length += span.Length;
        if (quote) _buffer[_length++] = '"';
    }

    private void End() { writer.Write(_buffer, 0, _length); writer.WriteLine(); }
}
