using System.Globalization;

namespace XerToCsvConverter;

/// <summary>Resolves calendars and inherited exceptions strictly within an input source.</summary>
public sealed class P6CalendarRepository
{
    private readonly Dictionary<(string Source, string Calendar), RawCalendar> _raw = new();
    private readonly Dictionary<(string Source, string Calendar), ResolvedCalendar> _resolved = new();
    private readonly HashSet<(string Source, string Calendar)> _invalidIdentities = new();
    private readonly object _gate = new();

    public P6CalendarRepository(XerDataStore dataStore, bool isolateInvalidIdentities = false)
    {
        ArgumentNullException.ThrowIfNull(dataStore);
        XerTable? table = dataStore.GetTable("CALENDAR");
        if (table is null || table.RowCount == 0) return;
        if (!table.FieldIndexes.ContainsKey("clndr_id"))
            throw new InvalidDataException("CALENDAR.clndr_id is required to resolve working calendars.");
        foreach (var row in table.Rows)
        {
            string id = Read(table, row, "clndr_id").Trim();
            if (id.Length == 0)
            {
                if (isolateInvalidIdentities) continue;
                throw new InvalidDataException($"Source '{row.SourceToken}' contains a blank CALENDAR.clndr_id.");
            }
            var key = (row.SourceToken, id);
            if (_invalidIdentities.Contains(key)) continue;
            string parent = Read(table, row, "base_clndr_id").Trim();
            if (parent is "0" or "-1") parent = string.Empty;
            var raw = new RawCalendar(row.SourceToken, id, Read(table, row, "clndr_name"),
                Read(table, row, "clndr_type"), parent, Read(table, row, "day_hr_cnt"),
                Read(table, row, "clndr_data"));
            if (!_raw.TryAdd(key, raw))
            {
                if (isolateInvalidIdentities)
                {
                    _raw.Remove(key);
                    _invalidIdentities.Add(key);
                    continue;
                }
                throw new InvalidDataException($"Source '{row.SourceToken}' contains duplicate CALENDAR.clndr_id '{id}'.");
            }
        }
    }

    public P6CalendarDefinition Get(string sourceToken, string calendarId,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
            return Resolve((sourceToken, calendarId.Trim()), new HashSet<(string, string)>(),
                cancellationToken).Definition;
    }

    /// <summary>False means the identity is absent. Present but malformed calendars throw.</summary>
    public bool TryGet(string sourceToken, string calendarId, out P6CalendarDefinition? definition,
        CancellationToken cancellationToken = default)
    {
        if (!_raw.ContainsKey((sourceToken, calendarId.Trim()))
            && !_invalidIdentities.Contains((sourceToken, calendarId.Trim())))
        {
            definition = null;
            return false;
        }
        definition = Get(sourceToken, calendarId, cancellationToken);
        return true;
    }

    public IReadOnlyList<P6CalendarDefinition> GetAll(CancellationToken cancellationToken = default) =>
        _raw.Keys.OrderBy(key => key.Source, StringComparer.Ordinal)
            .ThenBy(key => key.Calendar, StringComparer.Ordinal)
            .Select(key => Get(key.Source, key.Calendar, cancellationToken)).ToArray();

    private ResolvedCalendar Resolve((string Source, string Calendar) key,
        HashSet<(string, string)> visiting, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_invalidIdentities.Contains(key))
            throw new InvalidDataException($"Source '{key.Source}' contains duplicate CALENDAR.clndr_id '{key.Calendar}'.");
        if (_resolved.TryGetValue(key, out var cached)) return cached;
        if (!_raw.TryGetValue(key, out var raw))
            throw new InvalidDataException($"Source '{key.Source}' is missing CALENDAR '{key.Calendar}'.");
        if (visiting.Count >= 256 || !visiting.Add(key))
            throw new InvalidDataException($"Source '{key.Source}', CALENDAR '{key.Calendar}': cyclic or excessively deep base-calendar inheritance.");
        try
        {
            P6ParsedCalendar parsed = P6CalendarParser.Parse(raw.Data, cancellationToken);
            var exceptions = new SortedDictionary<DateTime, IReadOnlyList<P6WorkInterval>>();
            if (raw.Parent.Length > 0)
            {
                var parent = Resolve((raw.Source, raw.Parent), visiting, cancellationToken);
                foreach (var pair in parent.RawExceptions) exceptions.Add(pair.Key, pair.Value);
            }
            foreach (var pair in parsed.Exceptions) exceptions[pair.Key] = pair.Value;
            var definition = new P6CalendarDefinition(raw.Source, raw.Id, raw.Name, raw.Type,
                raw.Parent, raw.HoursPerDay, parsed.Week, exceptions);
            var result = new ResolvedCalendar(definition, exceptions);
            _resolved.Add(key, result);
            return result;
        }
        catch (InvalidDataException exception)
        {
            throw new InvalidDataException($"Source '{raw.Source}', CALENDAR '{raw.Id}': {exception.Message}", exception);
        }
        finally { visiting.Remove(key); }
    }

    private static string Read(XerTable table, DataRow row, string column) =>
        table.FieldIndexes.TryGetValue(column, out int index) && index < row.Fields.Length
            ? row.Fields[index] ?? string.Empty : string.Empty;

    private sealed record RawCalendar(string Source, string Id, string Name, string Type,
        string Parent, string HoursPerDay, string Data);
    private sealed record ResolvedCalendar(P6CalendarDefinition Definition,
        IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> RawExceptions);
}

internal sealed record P6ParsedCalendar(IReadOnlyList<IReadOnlyList<P6WorkInterval>> Week,
    IReadOnlyDictionary<DateTime, IReadOnlyList<P6WorkInterval>> Exceptions);

/// <summary>Parses the balanced XER calendar tree; no regular expression truncates nested shifts.</summary>
internal static class P6CalendarParser
{
    internal static P6ParsedCalendar Parse(string text, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(text))
            throw new InvalidDataException("clndr_data is blank; working availability cannot be inferred from hours per day.");
        var roots = new TreeReader(text, cancellationToken).Read();
        var allNodes = Descendants(roots).ToArray();
        var weekNodes = allNodes.Where(node => node.Name == "DaysOfWeek").ToArray();
        if (weekNodes.Length != 1)
            throw new InvalidDataException("clndr_data must contain exactly one DaysOfWeek section.");
        var week = new IReadOnlyList<P6WorkInterval>[7];
        foreach (var node in weekNodes[0].Children)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!int.TryParse(node.Name, NumberStyles.None, CultureInfo.InvariantCulture, out int day)
                || day < 1 || day > 7 || node.Attributes.Length != 0)
                throw new InvalidDataException("DaysOfWeek must contain day records numbered 1 (Sunday) through 7 (Saturday).");
            if (week[day - 1] is not null)
                throw new InvalidDataException($"DaysOfWeek repeats day {day}.");
            week[day - 1] = ReadShifts(node);
        }
        if (week.Any(day => day is null))
            throw new InvalidDataException("DaysOfWeek does not explicitly define all seven weekdays.");

        var exceptions = new SortedDictionary<DateTime, IReadOnlyList<P6WorkInterval>>();
        foreach (var section in allNodes.Where(node => node.Name is
                     "Exceptions" or "HolidayOrExceptions" or "HolidayOrException"))
        {
            foreach (var node in section.Children)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var parts = node.Attributes.Split('|', StringSplitOptions.TrimEntries);
                if (parts.Length is not (2 or 3) || parts[0] != "d"
                    || (parts.Length == 3 && parts[2] != "0")
                    || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out int serial))
                    throw new InvalidDataException($"Invalid calendar exception date record '{node.Attributes}'.");
                DateTime date;
                try { date = new DateTime(1899, 12, 30).AddDays(serial); }
                catch (ArgumentOutOfRangeException exception)
                { throw new InvalidDataException($"Calendar exception serial '{serial}' is outside the supported date range.", exception); }
                if (!exceptions.TryAdd(date, ReadShifts(node)))
                    throw new InvalidDataException($"Calendar repeats exception date {date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}.");
            }
        }
        return new(week, exceptions);
    }

    private static IReadOnlyList<P6WorkInterval> ReadShifts(Node day)
    {
        var intervals = new List<P6WorkInterval>();
        foreach (var node in day.Children)
        {
            if (node.Children.Count != 0)
                throw new InvalidDataException("A calendar shift unexpectedly contains nested records.");
            var fields = node.Attributes.Split('|', StringSplitOptions.TrimEntries);
            if (fields.Length != 4 || !((fields[0] == "s" && fields[2] == "f")
                    || (fields[0] == "f" && fields[2] == "s")))
                throw new InvalidDataException($"Invalid calendar shift '{node.Attributes}'.");
            TimeSpan start = ParseClock(fields[0] == "s" ? fields[1] : fields[3], false);
            TimeSpan end = ParseClock(fields[0] == "f" ? fields[1] : fields[3], true);
            if (end < start || (start == TimeSpan.Zero && end == TimeSpan.Zero))
                end += TimeSpan.FromDays(1);
            if (end > start) intervals.Add(new(start, end));
        }
        return P6CalendarNormalization.Merge(intervals);
    }

    private static TimeSpan ParseClock(string clock, bool allowMidnightEnd)
    {
        var fields = clock.Split(':');
        if (fields.Length != 2 || fields[0].Length is < 1 or > 2 || fields[1].Length != 2
            || !int.TryParse(fields[0], NumberStyles.None, CultureInfo.InvariantCulture, out int hours)
            || !int.TryParse(fields[1], NumberStyles.None, CultureInfo.InvariantCulture, out int minutes)
            || hours > 24 || minutes >= 60 || (hours == 24 && (!allowMidnightEnd || minutes != 0)))
            throw new InvalidDataException($"Invalid calendar clock '{clock}'.");
        return TimeSpan.FromMinutes(hours * 60 + minutes);
    }

    private static IEnumerable<Node> Descendants(IEnumerable<Node> nodes)
    {
        foreach (var node in nodes)
        {
            yield return node;
            foreach (var child in Descendants(node.Children)) yield return child;
        }
    }

    private sealed record Node(string Name, string Attributes, List<Node> Children);

    private sealed class TreeReader(string text, CancellationToken cancellationToken)
    {
        private int _position;
        private int _nodes;

        internal List<Node> Read()
        {
            var nodes = new List<Node>();
            SkipSpace();
            while (_position < text.Length)
            {
                nodes.Add(ReadNode(0));
                SkipSpace();
            }
            return nodes;
        }

        private Node ReadNode(int depth)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (depth > 64 || ++_nodes > 1_000_000)
                throw new InvalidDataException("Calendar structure exceeds the supported nesting or record limit.");
            Expect('(');
            int headerStart = _position;
            while (_position < text.Length && text[_position] != '(')
            {
                if (text[_position] == ')') Fail();
                _position++;
            }
            string header = text[headerStart.._position].Trim();
            int separator = header.IndexOf("||", StringComparison.Ordinal);
            if (separator < 1 || !int.TryParse(header[..separator].Trim(), NumberStyles.None,
                    CultureInfo.InvariantCulture, out _) || header[(separator + 2)..].Trim().Length == 0)
                throw new InvalidDataException($"Invalid calendar record header '{header}'.");
            string name = header[(separator + 2)..].Trim();
            Expect('(');
            int attributesStart = _position;
            while (_position < text.Length && text[_position] != ')')
            {
                if (text[_position] == '(') Fail();
                _position++;
            }
            string attributes = text[attributesStart.._position].Trim();
            Expect(')');
            Expect('(');
            var children = new List<Node>();
            SkipSpace();
            while (_position < text.Length && text[_position] == '(')
            {
                children.Add(ReadNode(depth + 1));
                SkipSpace();
            }
            Expect(')');
            Expect(')');
            return new(name, attributes, children);
        }

        private void Expect(char expected)
        {
            SkipSpace();
            if (_position >= text.Length || text[_position] != expected) Fail();
            _position++;
        }

        private void SkipSpace()
        {
            while (_position < text.Length && char.IsWhiteSpace(text[_position])) _position++;
        }

        private void Fail() => throw new InvalidDataException($"Malformed calendar parentheses near character {_position}.");
    }
}
