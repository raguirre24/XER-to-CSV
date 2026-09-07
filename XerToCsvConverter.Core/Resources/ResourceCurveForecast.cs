using System.Globalization;

namespace XerToCsvConverter;

internal sealed record RemainingDistributionPlan(RemainingResourceProfile? Profile,
    ResourceForecastClock? Clock = null, string? MethodCode = null, string? MethodMessage = null);

// Forecast availability is separate from the raw calendar diagnostics and from
// the historical actuals allocator. Only active named-curve allocations use it.
internal sealed class ResourceForecastClock
{
    private readonly WorkingDayCalculator _calendar;
    private readonly DateTime? _suspend;
    private readonly DateTime? _resume;

    internal ResourceForecastClock(WorkingDayCalculator calendar, string suspend, string resume, DateTime remainingFinish)
    {
        _calendar = calendar;
        if (string.IsNullOrWhiteSpace(suspend) && string.IsNullOrWhiteSpace(resume)) return;
        _suspend = DateParser.TryParse(suspend)?.Date;
        _resume = DateParser.TryParse(resume)?.Date;
        if (!_suspend.HasValue || (!string.IsNullOrWhiteSpace(resume) && (!_resume.HasValue || _resume < _suspend)))
            throw new InvalidDataException("Malformed suspend/resume dates prevent establishing remaining working availability.");
        if (!_resume.HasValue && _suspend < remainingFinish)
            throw new InvalidDataException("An open suspension overlaps the remaining period; no resume date or working availability is invented.");
    }

    internal decimal CountHours(DateTime start, DateTime finish)
    {
        decimal hours = _calendar.CountWorkingHours(start, finish);
        if (_suspend.HasValue && _resume.HasValue)
        {
            DateTime left = start > _suspend.Value ? start : _suspend.Value;
            DateTime right = finish < _resume.Value ? finish : _resume.Value;
            if (right > left) hours -= _calendar.CountWorkingHours(left, right);
        }
        return hours;
    }

    internal string Description => _suspend.HasValue
        ? $"suspend_date={DateParser.Format(_suspend.Value)}; resume_date={(_resume.HasValue ? DateParser.Format(_resume.Value) : "blank")}; "
        : "";

    internal static bool HasSingleWorkingMonth(DateTime start, DateTime finish, Func<DateTime, DateTime, decimal> hours)
    {
        int workingMonths = 0;
        DateTime cursor = start;
        while (cursor < finish)
        {
            DateTime end = cursor.Year == finish.Year && cursor.Month == finish.Month
                ? finish : new DateTime(cursor.Year, cursor.Month, 1).AddMonths(1);
            if (hours(cursor, end) > 0 && ++workingMonths > 1) return false;
            cursor = end;
        }
        return workingMonths == 1;
    }
}

internal static class ResourceCurveForecast
{
    internal static RemainingDistributionPlan Resolve(RemainingResourceProfile named, ResourceForecastClock clock,
        DateTime remainingStart, DateTime remainingFinish, Func<string, XerRawField> assignment,
        string dataDateText, bool actualQuantityIsZero)
    {
        decimal remainingHours = clock.CountHours(remainingStart, remainingFinish);
        if (remainingHours <= 0)
            throw new InvalidDataException("The remaining period has no working availability after recorded suspensions.");
        // These allocations need no estimated phase, even if actual dates are missing.
        if (named.IsUniform || ResourceForecastClock.HasSingleWorkingMonth(remainingStart, remainingFinish, clock.CountHours))
            return new(named, clock);

        decimal? elapsedHours = null, phase = null;
        string? reason = null;
        XerRawField actualStart = assignment("act_start_date"), actualFinish = assignment("act_end_date");
        bool Known(string field) => assignment(field).State is XerRawFieldState.Blank or XerRawFieldState.Present;
        if (actualStart.State == XerRawFieldState.Blank && actualFinish.State == XerRawFieldState.Blank
            && actualQuantityIsZero && Known("act_reg_qty") && Known("act_ot_qty"))
        {
            elapsedHours = 0;
        }
        else
        {
            DateTime? start = DateParser.TryParse(actualStart.RawValue);
            DateTime? dataDate = DateParser.TryParse(dataDateText);
            if (!start.HasValue || !dataDate.HasValue || !Known("act_end_date"))
                reason = "Assignment actual-start, actual-finish presence or project Data Date evidence is missing/invalid.";
            else if (actualFinish.State != XerRawFieldState.Blank || start > dataDate || dataDate > remainingStart)
                reason = "Assignment progress dates conflict with its Data Date or remaining period.";
            else elapsedHours = clock.CountHours(start.Value, dataDate.Value);
        }

        RemainingResourceProfile? profile = null;
        if (elapsedHours.HasValue)
        {
            phase = elapsedHours.Value / (elapsedHours.Value + remainingHours);
            profile = named.RemainingTail(phase.Value);
            if (profile is null) reason = "The named curve has no weight remaining after the calculated progress point.";
        }
        bool fallback = profile is null;
        string Number(decimal? value) => value?.ToString("G29", CultureInfo.InvariantCulture) ?? "unavailable";
        string method = fallback ? "Working Hours Fallback" : "Resource Curve Forecast";
        string message = $"Allocated remaining units using {method}; this is an exporter forecast, not native P6 timephased output. "
            + $"A={Number(elapsedHours)} working hours; R={Number(remainingHours)} working hours; p={Number(phase)}; "
            + clock.Description + (reason is null ? "Retained and normalized the remaining curve bands." : $"Reason: {reason}");
        return new(profile ?? RemainingResourceProfile.Uniform(method), clock,
            fallback ? "REMAINING_CURVE_UNIFORM_FALLBACK" : "REMAINING_CURVE_ESTIMATED", message);
    }
}
