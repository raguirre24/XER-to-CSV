namespace XerToCsvConverter;

/// <summary>
/// The greatest signed predecessor working-time movement whose lagged endpoint does
/// not exceed a fixed successor endpoint. This is a relationship delay allowance,
/// not an activity float or a classification of P6 driving relationships.
/// </summary>
public static class RelationshipFreeFloatCalculator
{
    public static decimal CalculateHours(WorkingDayCalculator predecessorCalendar,
        WorkingDayCalculator lagCalendar, DateTime predecessorEndpoint,
        DateTime successorEndpoint, decimal lagHours, bool predecessorIsStart,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(predecessorCalendar);
        ArgumentNullException.ThrowIfNull(lagCalendar);
        cancellationToken.ThrowIfCancellationRequested();
        // Match the calendar engine's DateTime precision once, before inversion.
        // Sub-tick input must not accidentally turn into a nonworking-time snap.
        long lagTicks = checked((long)decimal.Round(lagHours * TimeSpan.TicksPerHour, 0,
            MidpointRounding.AwayFromZero));

        DateTime latestEndpoint = FindLatestLagInput(lagCalendar, successorEndpoint,
            lagTicks, cancellationToken);
        // CountWorkingHours is an integral number of time ticks divided by ticks/hour.
        // Restore that integer before adding/subtracting one tick; do not round days.
        long movementTicks = checked((long)decimal.Round(
            predecessorCalendar.CountWorkingHours(predecessorEndpoint, latestEndpoint,
                cancellationToken) * TimeSpan.TicksPerHour, 0));

        DateTime Move(long ticks)
        {
            // An unchanged event retains its exact exported timestamp, including a
            // milestone or finish at a shift start. Canonicalise only moved events.
            if (ticks == 0) return predecessorEndpoint;
            DateTime moved = predecessorCalendar.AddWorkingTicks(predecessorEndpoint,
                ticks, cancellationToken);
            return predecessorIsStart
                ? NextWorkStart(predecessorCalendar, moved, cancellationToken)
                : PreviousWorkFinish(predecessorCalendar, moved, cancellationToken);
        }

        bool IsFeasible(long ticks) => lagCalendar.AddWorkingTicks(Move(ticks),
            lagTicks, cancellationToken) <= successorEndpoint;

        // Working-time integration has no width across nonworking gaps. Its inverse
        // may therefore choose the wrong side of a Start/Finish boundary. At most one
        // work tick must be removed to obtain the greatest representable allowance.
        if (!IsFeasible(movementTicks)) movementTicks = checked(movementTicks - 1);
        if (!IsFeasible(movementTicks) || IsFeasible(checked(movementTicks + 1)))
            throw new InvalidDataException(
                "The relationship delay allowance could not be verified at calendar precision.");

        return movementTicks / (decimal)TimeSpan.TicksPerHour;
    }

    private static DateTime FindLatestLagInput(WorkingDayCalculator calendar,
        DateTime deadline, long lagTicks, CancellationToken cancellationToken)
    {
        if (lagTicks == 0) return deadline;

        // Subtracting lag gives a candidate, not generally the inverse: addition is
        // monotone but flat over nonworking gaps, and jumps at work boundaries.
        DateTime candidate = calendar.AddWorkingTicks(deadline, checked(-lagTicks), cancellationToken);
        if (calendar.AddWorkingTicks(candidate, lagTicks, cancellationToken) > deadline)
        {
            // A negative lag can put the candidate exactly at the beginning of the
            // next feasible work segment, whose backwards projection jumps past a
            // deadline in a nonworking gap. The preceding tick is the last input.
            candidate = candidate.AddTicks(-1);
        }
        else
        {
            // Both signs of lag can produce a plateau from one shift's finish to
            // the next shift's start. The rightmost input, not its first occurrence,
            // is the latest predecessor endpoint permitted by the relationship.
            candidate = NextWorkStart(calendar, candidate, cancellationToken);
        }

        if (calendar.AddWorkingTicks(candidate, lagTicks, cancellationToken) > deadline
            || calendar.AddWorkingTicks(candidate.AddTicks(1), lagTicks, cancellationToken) <= deadline)
            throw new InvalidDataException(
                "The relationship lag inverse could not be verified at calendar precision.");
        return candidate;
    }

    private static DateTime NextWorkStart(WorkingDayCalculator calendar, DateTime endpoint,
        CancellationToken cancellationToken) =>
        calendar.AddWorkingTicks(endpoint, 1, cancellationToken).AddTicks(-1);

    private static DateTime PreviousWorkFinish(WorkingDayCalculator calendar, DateTime endpoint,
        CancellationToken cancellationToken) =>
        calendar.AddWorkingTicks(endpoint, -1, cancellationToken).AddTicks(1);
}
