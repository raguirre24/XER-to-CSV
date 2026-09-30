namespace XerToCsvConverter;

public partial class XerTransformer
{
    private bool _shareResilientCalendarLookup;
    private readonly object _resilientCalendarGate = new();
    private P6CalendarRepository? _resilientCalendarRepository;

    // Export operations finish source rebinding/union before creating a transformer.
    // The public constructor remains suitable for callers editing parsed input
    // between transformations; it must not retain a stale calendar snapshot.
    internal static XerTransformer ForExportOperation(XerDataStore store) =>
        new(store) { _shareResilientCalendarLookup = true };

    private P6CalendarRepository GetResilientCalendarRepository()
    {
        if (!_shareResilientCalendarLookup)
            return new P6CalendarRepository(_dataStore, isolateInvalidIdentities: true);
        lock (_resilientCalendarGate)
            return _resilientCalendarRepository ??=
                new P6CalendarRepository(_dataStore, isolateInvalidIdentities: true);
    }
}
