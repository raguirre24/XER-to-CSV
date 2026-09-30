const test = require('node:test');
const assert = require('node:assert/strict');
const { median, matchedReductions, measuredTotal, cycleSummary, hotCycleScopes, tapCounts, trxSkipped } = require('./PerformanceReportEvidence.cjs');

test('matched pairs expose tiering without conflating the ratio of medians', () => {
    const before = [61.525, 60.794, 56.012, 33.440, 33.159].map((processingMs, iteration) => ({ processingMs, iteration }));
    const after = [34.002, 34.670, 18.093, 18.643, 18.630].map((processingMs, iteration) => ({ processingMs, iteration })).reverse();
    const result = matchedReductions(before, after);
    assert.equal(result.pairs.length, 5);
    assert.ok(result.minimum > 42 && result.minimum < 43);
    assert.ok(result.median > 44 && result.median < 45);
    assert.ok(result.maximum > 67 && result.maximum < 68);
    assert.ok((1 - median(after.map(run => run.processingMs)) / median(before.map(run => run.processingMs))) * 100 > 66);
    assert.throws(() => matchedReductions(before, after.slice(1)), /Unmatched pair/);
    assert.throws(() => matchedReductions(before, after.map(run => ({ ...run, iteration: 0 }))), /Duplicate/);
});

test('phase totals preserve explicit zero and report absent browser measurements as unavailable', () => {
    const records = [{ metrics: { 'exclusive.Parsing': 0 } }, { metrics: { 'exclusive.Parsing': 7 } }];
    assert.equal(measuredTotal(records, 'exclusive.Parsing', record => record.metrics), 7);
    assert.equal(measuredTotal(records.slice(0, 1), 'exclusive.Parsing', record => record.metrics), 0);
    assert.equal(measuredTotal(records, 'exclusive.zipTransfer', record => record.metrics), null);
    assert.equal(median([]), null);
});

test('cycle summaries retain endpoint gaps and separate cumulative allocations from memory', () => {
    const cycles = [{ allocatedBytes: 20 }, { managedHeapBytes: 50, allocatedBytes: 30, cumulativeAllocatedBytes: 50 },
        { managedHeapBytes: 40, allocatedBytes: 40, cumulativeAllocatedBytes: 90 }];
    assert.deepEqual(cycleSummary(cycles, 'managedHeapBytes'),
        { first: null, last: 40, minimum: 40, maximum: 50, available: 2, cycles: 3 });
    assert.equal(cycleSummary(cycles, 'allocatedBytes').last, 40);
    assert.equal(cycleSummary(cycles, 'cumulativeAllocatedBytes').last, 90);
});

test('complete TAP evidence is required and failure summaries remain visible', () => {
    const evidence = '# tests 28\n# pass 28\n# fail 0\n# cancelled 0\n# skipped 0\n# todo 0\n';
    assert.equal(tapCounts(evidence).pass, 28);
    assert.throws(() => tapCounts('# tests 28\n# pass 28\n'), /complete TAP/);
    assert.throws(() => tapCounts(evidence + evidence), /complete TAP/);
    assert.equal(tapCounts(evidence.replace('# pass 28', '# pass 27').replace('# fail 0', '# fail 1')).fail, 1);
});

test('hot memory scopes keep real selected Enhanced separate from small complete reviews', () => {
    const realCycles = [{ retainedCacheBytes: 100 }], reviewCycles = [{ retainedCacheBytes: 5 }];
    const review = { baselineInputBytes: 2048, initial: { fileCount: 12 }, cycles: reviewCycles,
        afterClear: { retainedCacheBytes: 0, retainedSourceCount: 0 } };
    const scopes = hotCycleScopes({ baselineInputBytes: 13424712, enhancedTables: ['01', '06', '15'], cycles: realCycles,
        cacheAfterClear: 0, reviewProfiles: { programme: review, tender: review } });
    assert.equal(scopes.length, 3);
    assert.equal(scopes[0].baselineInputBytes, 13424712);
    assert.equal(scopes[0].exportScope, '01, 06, 15');
    assert.equal(scopes[0].cycles, realCycles);
    assert.equal(scopes[1].baselineInputBytes, 2048);
    assert.equal(scopes[1].exportScope, 'Complete review bundle (12 files)');
    assert.equal(scopes[1].cycles, reviewCycles);
    assert.equal(scopes[2].sourcesAfterClear, 0);
    assert.throws(() => hotCycleScopes({ cycles: realCycles }), /Missing nested programme/);
});

test('TRX skips include discovered but unexecuted CLI platform cases', () => {
    assert.equal(trxSkipped({ total: 38, executed: 36, notExecuted: 0 }), 2);
    assert.equal(trxSkipped({ total: 38, executed: 36, notExecuted: 2 }), 2);
    assert.equal(trxSkipped({ total: 1469, executed: 1469, notExecuted: 0 }), 0);
    assert.throws(() => trxSkipped({ notExecuted: 0 }), /Incomplete TRX/);
});
