using System.Globalization;

namespace XerToCsvConverter;

// A piecewise-constant allocation over working time, represented cumulatively.
// P6's pct_usage_1..20 are band quantities, not spline/control-point heights.
internal sealed class RemainingResourceProfile
{
    private readonly decimal[] _ends;
    private readonly decimal[] _quantities;
    private readonly decimal _duration;
    private readonly decimal _quantity;
    private readonly bool _usesTicks;

    internal string DistributionType { get; }
    internal bool IsUniform { get; }

    private RemainingResourceProfile(decimal[] ends, decimal[] quantities, string type, bool uniform = false, bool usesTicks = false)
    {
        _ends = ends;
        _quantities = quantities;
        _duration = ends[^1];
        _quantity = quantities[^1];
        DistributionType = type;
        IsUniform = uniform;
        _usesTicks = usesTicks;
    }

    internal decimal CumulativeShare(long workingTicks, long totalTicks)
    {
        if (workingTicks == 0) return 0;
        if (workingTicks == totalTicks) return 1;
        decimal progress = workingTicks / (decimal)totalTicks;
        // Preserve the exact existing arithmetic for an assigned linear curve.
        if (IsUniform) return progress;
        decimal position = _usesTicks ? workingTicks : progress * _duration;
        int band = Array.BinarySearch(_ends, position);
        if (band >= 0) return _quantities[band] / _quantity;
        band = ~band;
        decimal start = band == 0 ? 0 : _ends[band - 1];
        decimal before = band == 0 ? 0 : _quantities[band - 1];
        decimal share = (before + (_quantities[band] - before)
            * ((position - start) / (_ends[band] - start))) / _quantity;
        return Math.Clamp(share, 0, 1);
    }

    internal static RemainingResourceProfile FromCurve(Func<string, string> read, string id)
    {
        decimal[] percentages = Enumerable.Range(0, 21)
            .Select(index => Number(read($"pct_usage_{index}"), $"curve '{id}' pct_usage_{index}")).ToArray();
        if (percentages.Any(value => value > 100))
            throw new InvalidDataException($"Curve '{id}' percentages must not exceed 100.");
        // The 0% entry describes already-used resource under P6's actuals rules.
        // Never turn it into another 5% duration band or invent an instant allocation.
        if (percentages[0] != 0)
            throw new InvalidDataException($"Curve '{id}' has a nonzero 0% entry. Its P6 actuals semantics require an exported remain_crv profile.");
        decimal sum = percentages.Sum();
        // Permit only small decimal serialization/proration noise, not arbitrary
        // rescaling of an invalid curve. Normalize accepted values by their sum.
        if (Math.Abs(sum - 100) > 0.001m)
            throw new InvalidDataException($"Curve '{id}' percentages must total 100 (tolerance 0.001); got {sum.ToString(CultureInfo.InvariantCulture)}.");
        var cumulative = new decimal[20];
        decimal allocated = 0;
        for (int i = 0; i < 20; i++) cumulative[i] = allocated += percentages[i + 1];
        return new RemainingResourceProfile(Enumerable.Range(1, 20).Select(value => (decimal)value).ToArray(),
            cumulative, "Resource Curve", percentages.Skip(1).All(value => value == percentages[1]));
    }

    internal static RemainingResourceProfile FromManual(string raw, decimal remainingQuantity, decimal workingHours)
    {
        // XER remain_crv uses quantity:period-working-hours pairs. Each pair is
        // anchored after the previous pair in assignment-calendar working time.
        // Zero-quantity pairs are intentional gaps and must not be discarded.
        // A trailing delimiter is harmless; internal empty bands are still invalid.
        string[] bands = raw.Trim().TrimEnd(';').Split(';');
        var ends = new decimal[bands.Length];
        var quantities = new decimal[bands.Length];
        decimal durationTicks = 0;
        decimal quantity = 0;
        for (int i = 0; i < bands.Length; i++)
        {
            string[] pair = bands[i].Split(':');
            if (pair.Length != 2)
                throw new InvalidDataException($"remain_crv band {i + 1} requires quantity:working-hours.");
            decimal units = Number(pair[0], $"remain_crv band {i + 1} quantity");
            decimal hours = Number(pair[1], $"remain_crv band {i + 1} working-hours");
            decimal ticks = hours * TimeSpan.TicksPerHour;
            if (hours == 0 || ticks != decimal.Truncate(ticks))
                throw new InvalidDataException($"remain_crv band {i + 1} requires positive working-hours representable as whole ticks.");
            ends[i] = durationTicks += ticks;
            quantities[i] = quantity += units;
        }
        if (durationTicks != decimal.Round(workingHours * TimeSpan.TicksPerHour, 0))
            throw new InvalidDataException("remain_crv duration does not reconcile to the assignment's remaining calendar working time.");
        if (quantity <= 0 || decimal.Round(quantity, 4, MidpointRounding.ToEven)
            != decimal.Round(remainingQuantity, 4, MidpointRounding.ToEven))
            throw new InvalidDataException("remain_crv quantities do not reconcile to remain_qty at four-decimal export precision.");
        return new RemainingResourceProfile(ends, quantities, "Remaining Units Profile", usesTicks: true);
    }

    private static decimal Number(string raw, string field)
    {
        if (!decimal.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal value) || value < 0)
            throw new InvalidDataException($"{field} requires a finite nonnegative invariant number; got '{raw}'.");
        return value;
    }
}

internal sealed class ResourceCurveRepository
{
    private readonly XerTable? _table;
    private readonly Dictionary<(string Source, string Id), DataRow[]> _rows;
    private readonly Dictionary<(string Source, string Id), RemainingResourceProfile> _cache = new();

    internal ResourceCurveRepository(XerDataStore store)
    {
        _table = store.GetTable("RSRCCURVDATA");
        _rows = _table?.Rows.GroupBy(row => (row.SourceToken, Read(row, "curv_id").Trim()))
            .ToDictionary(group => group.Key, group => group.ToArray()) ?? new();
    }

    internal RemainingResourceProfile Get(string source, string id)
    {
        if (_cache.TryGetValue((source, id), out var profile)) return profile;
        if (!_rows.TryGetValue((source, id), out var rows) || rows.Length != 1)
            throw new InvalidDataException($"Curve '{id}' requires exactly one RSRCCURVDATA definition in its source occurrence. "
                + "Missing/ambiguous definitions and opaque RSRCCURV.curv_data are not replaced by a uniform spread; export remain_crv for manual profiles.");
        profile = RemainingResourceProfile.FromCurve(field => Read(rows[0], field), id);
        _cache.Add((source, id), profile);
        return profile;
    }

    private string Read(DataRow row, string field) => _table is not null
        && _table.FieldIndexes.TryGetValue(field, out int index) && index < row.Fields.Length
            ? row.Fields[index] : "";
}
