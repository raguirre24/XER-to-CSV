const assert = require('node:assert/strict');

function median(values) {
    if (!values.length) return null;
    assert.ok(values.every(Number.isFinite), 'A measured value is not finite');
    const sorted = values.toSorted((a, b) => a - b);
    return (sorted[Math.floor((sorted.length - 1) / 2)] + sorted[Math.floor(sorted.length / 2)]) / 2;
}

function matchedReductions(before, after) {
    const candidates = new Map(after.map(run => [run.iteration, run]));
    assert.equal(candidates.size, after.length, 'Duplicate candidate pair identity');
    assert.equal(new Set(before.map(run => run.iteration)).size, before.length, 'Duplicate baseline pair identity');
    assert.equal(before.length, after.length, 'Unmatched pair count');
    const pairs = before.toSorted((a, b) => a.iteration - b.iteration).map(run => {
        const candidate = candidates.get(run.iteration);
        assert.ok(candidate, `Missing candidate pair ${run.iteration}`);
        assert.ok(Number.isFinite(run.processingMs) && run.processingMs > 0 &&
            Number.isFinite(candidate.processingMs) && candidate.processingMs > 0, 'Invalid processing duration');
        return { iteration: run.iteration, baselineMs: run.processingMs, candidateMs: candidate.processingMs,
            reduction: (1 - candidate.processingMs / run.processingMs) * 100 };
    });
    const reductions = pairs.map(pair => pair.reduction);
    return { pairs, median: median(reductions), minimum: Math.min(...reductions), maximum: Math.max(...reductions) };
}

// Missing measurements differ from an explicit measured zero. Totals combine
// independent jobs in one operation; they do not invent absent phase values.
function measuredTotal(records, key, select) {
    const values = records.map(record => select(record)?.[key]).filter(Number.isFinite);
    return values.length ? values.reduce((sum, value) => sum + value, 0) : null;
}

function cycleSummary(cycles, key) {
    const values = cycles.map(cycle => cycle[key]).filter(Number.isFinite);
    const first = Number.isFinite(cycles[0]?.[key]) ? cycles[0][key] : null;
    const last = Number.isFinite(cycles.at(-1)?.[key]) ? cycles.at(-1)[key] : null;
    return { first, last, minimum: values.length ? Math.min(...values) : null,
        maximum: values.length ? Math.max(...values) : null, available: values.length, cycles: cycles.length };
}

function hotCycleScopes(evidence) {
    const scopes = [{ profile: 'Enhanced selected', dataset: 'Real baseline + small synthetic updates',
        baselineInputBytes: evidence.baselineInputBytes, exportScope: (evidence.enhancedTables || []).join(', '),
        cycles: evidence.cycles, cacheAfterClear: evidence.cacheAfterClear }];
    for (const profile of ['programme', 'tender']) {
        const review = evidence.reviewProfiles?.[profile];
        assert.ok(review?.cycles?.length, `Missing nested ${profile} memory evidence`);
        scopes.push({ profile, dataset: 'Small synthetic baseline + updates', baselineInputBytes: review.baselineInputBytes,
            exportScope: `Complete review bundle (${review.initial.fileCount} files)`, cycles: review.cycles,
            cacheAfterClear: review.afterClear.retainedCacheBytes, sourcesAfterClear: review.afterClear.retainedSourceCount });
    }
    return scopes;
}

function tapCounts(text) {
    const counts = {};
    for (const name of ['tests', 'pass', 'fail', 'cancelled', 'skipped', 'todo']) {
        const matches = [...text.matchAll(new RegExp(`^# ${name} (\\d+)\\s*$`, 'gm'))];
        assert.equal(matches.length, 1, `Expected one complete TAP summary for ${name}`);
        counts[name] = Number(matches[0][1]);
    }
    assert.equal(counts.tests, counts.pass + counts.fail + counts.cancelled + counts.skipped + counts.todo,
        'TAP summary counts do not reconcile');
    return counts;
}

function trxSkipped(counts) {
    assert.ok(Number.isFinite(counts.total) && Number.isFinite(counts.executed) &&
        Number.isFinite(counts.notExecuted), 'Incomplete TRX skip counters');
    return Math.max(counts.notExecuted, counts.total - counts.executed);
}

function observedPhaseShares(run) {
    assert.ok(Number.isFinite(run.processingMs) && run.processingMs > 0, 'Invalid observer processing duration');
    const validationMs = measuredTotal(run.observerPhases, 'exclusive.StandaloneValidation', record => record.metrics);
    const transformationMs = measuredTotal(run.observerPhases, 'exclusive.Transformation', record => record.metrics);
    assert.ok(Number.isFinite(validationMs) && Number.isFinite(transformationMs), 'Missing exclusive observer diagnosis phases');
    return { mode: run.mode, processingMs: run.processingMs, validationMs, transformationMs,
        validationPercent: validationMs / run.processingMs * 100, transformationPercent: transformationMs / run.processingMs * 100 };
}

module.exports = { median, matchedReductions, measuredTotal, cycleSummary, hotCycleScopes, tapCounts, trxSkipped, observedPhaseShares };
