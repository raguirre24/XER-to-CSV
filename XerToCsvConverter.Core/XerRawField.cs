namespace XerToCsvConverter;

/// <summary>
/// The structural state of a field in an original XER row, before padding or schema merging.
/// Present values are validated by their typed consumer; this does not certify their meaning.
/// </summary>
public enum XerRawFieldState
{
    AbsentHeader,
    OmittedCell,
    Blank,
    Present
}

/// <summary>Immutable raw text and its original structural presence.</summary>
public readonly record struct XerRawField(string RawValue, XerRawFieldState State);
