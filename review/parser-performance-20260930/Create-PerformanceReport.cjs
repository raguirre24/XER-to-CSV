// Generate the reviewable report only after timing and lifecycle runs finish.
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { median, matchedReductions, measuredTotal, cycleSummary, hotCycleScopes, tapCounts, trxSkipped, observedPhaseShares } = require('./PerformanceReportEvidence.cjs');
const root = path.resolve(__dirname, '../../artifacts/parser-performance-20260930');
const read = name => JSON.parse(fs.readFileSync(path.join(root, name), 'utf8').replace(/^\uFEFF/, ''));
const escape = value => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
const seconds = ms => Number.isFinite(ms) ? (ms / 1000).toFixed(2) : 'unavailable';
const mib = bytes => Number.isFinite(bytes) ? (bytes / 1024 / 1024).toFixed(1) : 'unavailable';
const table = (columns, rows) => `<table><thead><tr>${columns.map(c => `<th>${escape(c)}</th>`).join('')}</tr></thead><tbody>${rows.map(row => `<tr>${row.map(cell => `<td>${escape(cell)}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
const benchmark = read('browser-benchmark.json');
const parity = read('browser-output-parity.json');
const native = read('native-r3-parity.json');
const nativeBuild = read('native-candidate-r3/build-evidence.json');
const observerNative = read('native-baseline-telemetry-parity.json');
const lifecycle = ['chrome', 'msedge'].map(browser => read(`browser-lifecycle-${browser}.json`));
const incremental = ['chrome', 'msedge'].map(browser => read(`browser-incremental-${browser}.json`));
const cache = read('native-cache-estimates-r3/report.json');
const release = read('release-evidence/release-summary.json');
const machine = read('machine.json');
const observer = read('browser-observer-phases.json');

// Fail before writing either output. Old, partial or failed artifacts must not
// silently supply a plausible-looking report for the final R3 package.
assert.equal(release.Revision, 'R3', 'Final R3 release evidence is required');
for (const key of ['ExitCode', 'WarningCount', 'ErrorCount']) assert.equal(release[key], 0, `Release ${key}`);
assert.equal(nativeBuild.CoreAssemblySha256, release.NativeCoreDllSha256, 'Native/published Core mismatch');
assert.equal(native.Candidate, 'candidate-r3');
for (const [name, evidence] of [['candidate', native], ['observer', observerNative]]) {
    assert.equal(evidence.Cases, 32, `${name} native case coverage`);
    assert.equal(evidence.Failures, 0, `${name} native differences`);
    assert.equal(evidence.Results.length, 32, `${name} native result count`);
    assert.ok(evidence.Results.every(result => result.Passed === true), `${name} native failure`);
}
assert.equal(release.NativeParityCasesPassed, native.Cases);
assert.equal(nativeBuild.NativeParityCases, native.Cases);
assert.equal(nativeBuild.NativeParityFailures, 0);
assert.equal(release.ComparatorTestsPassed, 3, 'Browser byte-comparison regression tests are required');
const releaseTime = fs.statSync(path.join(root, 'release-evidence/final-release-publish.log')).mtimeMs;
function finalBrowserReport(report, name, timestamp) {
    assert.ok(!report.error, `${name} failed: ${report.error}`);
    assert.ok(Number.isFinite(Date.parse(timestamp)) && Date.parse(timestamp) >= releaseTime,
        `${name} predates the final package publish`);
}
finalBrowserReport(benchmark, 'Primary benchmark', benchmark.createdAt);
assert.equal(benchmark.baseline, '90b98d3');
assert.equal(parity.passed, true, 'Browser extracted-output comparison failed');
assert.deepEqual(parity.errors, []);
assert.equal(parity.comparedPairs, parity.comparisons.length);
const requiredReports = ['browser-benchmark.json', 'browser-incremental-chrome.json', 'browser-incremental-msedge.json'];
for (const filename of requiredReports) {
    assert.ok(parity.reports.some(report => path.resolve(report) === path.join(root, filename)), `Missing parity input ${filename}`);
    assert.ok(Date.parse(parity.createdAt) >= fs.statSync(path.join(root, filename)).mtimeMs,
        `Browser parity predates ${filename}`);
}
const assetText = fs.readFileSync(path.join(root, 'release-evidence/candidate-web-assets-sha256.csv'), 'utf8');
const assetHashes = new Map([...assetText.matchAll(/^"([^"]+)","\d+","([a-f\d]{64})"\r?$/gm)]
    .map(match => [match[1].replaceAll('\\', '/'), match[2].toLowerCase()]));
function finalAssets(hashes, name) {
    for (const asset of ['js/workerEngine.js', 'worker/_framework/blazor.boot.json']) {
        assert.ok(assetHashes.has(asset), `Release inventory is missing ${asset}`);
        assert.equal(hashes?.[asset]?.toLowerCase(), assetHashes.get(asset), `${name} used another package: ${asset}`);
    }
}
const memoryCheckNames = ['real-stream-blob-discard-and-ten-teardown-cycles-memory', 'hot-worker-ten-update-export-remove-cycles-memory'];
for (const report of lifecycle) {
    finalBrowserReport(report, `${report.channel} lifecycle`, report.timestamp);
    assert.equal(report.checks.length, 9, 'Full nine-check lifecycle coverage is required');
    assert.ok(report.checks.every(check => check.passed === true), 'Lifecycle check failed');
    assert.ok(report.checks.some(check => check.name === 'worker-startup-failure-ui-fallback-parses-and-downloads'), 'Full UI fallback check missing');
    finalAssets(report.workerAssetHashes, `${report.channel} lifecycle`);
    for (const name of memoryCheckNames) {
        const evidence = report.checks.find(check => check.name === name)?.evidence;
        assert.equal(evidence?.cycles?.length, 10, `Missing ten-cycle evidence: ${report.channel}/${name}`);
        const sourceHash = evidence.inputSha256 || evidence.baselineInputSha256;
        assert.equal(sourceHash?.toLowerCase(), cache.Sources[0].SourceSha256.toLowerCase(), 'Lifecycle real source differs');
        assert.ok(evidence.cycles.every(cycle => cycle.retainedCacheBytes <= cycle.cacheBudgetBytes), 'Cache estimate exceeds its budget');
    }
    const hot = report.checks.find(check => check.name === memoryCheckNames[1]).evidence;
    assert.deepEqual(hot.enhancedTables, ['01_XER_TASK', '06_XER_PREDECESSOR', '15_XER_RESOURCE_DISTRIBUTION'],
        'Real hot-memory export scope differs');
    for (const scope of hotCycleScopes(hot)) {
        assert.equal(scope.cycles.length, 10, `Missing ten-cycle ${scope.profile} evidence`);
        assert.equal(scope.cacheAfterClear, 0, `${scope.profile} cache was not cleared`);
        assert.ok(scope.cycles.every(cycle => cycle.parsedSources === 1 && cycle.cacheHits === 1 &&
            cycle.retainedSourceCount === 2 && cycle.liveWorkers === 1 && cycle.createdWorkers === 0 &&
            cycle.preparedUrls === 0 && cycle.retainedCacheBytes <= cycle.cacheBudgetBytes), `${scope.profile} hot-cycle check failed`);
        if (scope.profile !== 'Enhanced selected') {
            assert.equal(scope.sourcesAfterClear, 0);
            const review = hot.reviewProfiles[scope.profile];
            assert.equal(review.initial.fileCount, 12, 'Complete review bundle coverage differs');
            assert.ok(review.cycles.every(cycle => cycle.fileCount === review.initial.fileCount &&
                /^[a-f\d]{64}$/.test(cycle.diagnosticSha256)), 'Review cycle bundle/diagnostic evidence is incomplete');
        }
    }
}
for (const report of incremental) {
    finalBrowserReport(report, `${report.channel} incremental`, report.createdAt);
    assert.equal(report.baseline, '90b98d3');
    assert.ok(report.summaries.length > 0 && report.comparisons.length > 0, 'Incomplete incremental report');
    assert.ok(report.comparisons.every(pair => pair.matchedExactCsvBytesManifestCellsAndOrder === true), 'Incremental output difference');
    for (const run of report.runs.filter(run => run.variant === 'candidate')) finalAssets(run.assetHashes, `${report.channel} incremental`);
    for (const mode of ['legacy', 'programme', 'tender']) {
        for (const scenario of ['repeated-export', 'adding-update', 'evicted-reread', ...(mode === 'legacy' ? [] : ['metadata-edit'])]) {
            const item = report.summaries.find(summary => summary.dataset === 'synthetic' && summary.mode === mode && summary.scenario === scenario);
            assert.equal(item?.pairs, 5, `Incomplete incremental group: ${report.channel}/${mode}/${scenario}`);
        }
        for (const scenario of ['repeated-export', 'adding-update']) assert.ok(report.summaries.some(summary =>
            summary.dataset === 'real' && summary.mode === mode && summary.scenario === scenario && summary.pairs >= 1),
        `Missing real incremental group: ${report.channel}/${mode}/${scenario}`);
    }
    for (const item of report.summaries) assert.equal(report.comparisons.filter(pair => pair.dataset === item.dataset &&
        pair.mode === item.mode && pair.scenario === item.scenario).length, item.pairs, 'Incremental pair counts differ');
}
const allBrowserRuns = [benchmark, ...incremental].flatMap(report => report.runs.filter(run => !run.warmup));
assert.equal(parity.comparedPairs, allBrowserRuns.length / 2, 'Browser comparison coverage differs from current reports');
for (const run of allBrowserRuns) {
    assert.deepEqual(run.errors || [], []);
    assert.ok(parity.comparisons.some(pair => path.resolve(run.variant === 'candidate' ? pair.candidateZip : pair.baselineZip) === path.resolve(run.zip)),
        'A current browser ZIP has no matching parity result');
}
assert.ok(!observer.error, 'Frozen observer run failed');
for (const mode of ['legacy', 'programme', 'tender']) assert.ok(observer.runs.some(run => !run.warmup && run.browser === 'chrome' && run.mode === mode &&
    run.variant === 'baseline' && run.observerPhases?.length && !run.errors?.length), `Missing frozen observer mode ${mode}`);
assert.equal(release.WebNodeTestsEvidence, 'release-evidence/final-web-node-tests.tap', 'Final Node TAP evidence is required');
const nodeTests = tapCounts(fs.readFileSync(path.join(root, release.WebNodeTestsEvidence), 'utf8'));
assert.equal(nodeTests.pass, release.WebNodeTestsPassed);
assert.equal(nodeTests.pass, 28, 'Final Web Node coverage changed');
for (const key of ['fail', 'cancelled', 'skipped', 'todo']) assert.equal(nodeTests[key], 0, `Web Node ${key}`);
assert.ok(typeof release.StageIsolationEvidence === 'string' && release.StageIsolationEvidence.startsWith('stage-validation/'),
    'Complete archived stage-isolation evidence is required');
function containedPath(filename, parent) {
    const resolved = path.resolve(filename), prefix = path.resolve(parent) + path.sep;
    assert.ok(resolved.toLowerCase().startsWith(prefix.toLowerCase()), `Evidence path escapes ${parent}`);
    return resolved;
}
const stageFilename = containedPath(path.join(root, release.StageIsolationEvidence), root);
const stageEvidence = JSON.parse(fs.readFileSync(stageFilename, 'utf8').replace(/^\uFEFF/, ''));
assert.equal(stageEvidence.Status, 'passed', 'Archived stage validation did not finish successfully');
assert.equal(stageEvidence.NativeParity, true, 'Archived stages require full native parity');
assert.equal(stageEvidence.PinnedSdk, machine.Sdk);
assert.deepEqual(stageEvidence.Stages.map(stage => stage.Stage), ['A', 'C', 'B', 'E'], 'Archived stage coverage differs');
const stageRows = [];
for (const stage of stageEvidence.Stages) {
    assert.equal(stage.Status, 'passed', `Archived stage ${stage.Stage} failed`);
    assert.match(stage.Commit, /^[a-f\d]{40}$/);
    assert.equal(stage.Tests.length, 2, `Archived stage ${stage.Stage} suite coverage differs`);
    assert.equal(stage.Tests.filter(test => test.Project.includes('Core.Tests')).length, 1);
    assert.equal(stage.Tests.filter(test => test.Project.includes('RelationshipAudit.Cli.Tests')).length, 1);
    const locator = stage.FixtureLocatorIsolation;
    assert.equal(locator?.Passed, true, `Archived stage ${stage.Stage} fixture isolation is missing`);
    const sourceRoot = containedPath(locator.SourceRoot, path.join(root, 'stage-validation'));
    const archiveRoot = path.dirname(sourceRoot);
    assert.equal(path.basename(archiveRoot), `${stage.Stage}-${stage.Commit.slice(0, 7)}`, 'Archived source identity differs');
    if (path.dirname(archiveRoot) !== path.dirname(stageFilename)) {
        const previousFile = containedPath(stage.ReusedFromEvidence, path.join(root, 'stage-validation'));
        const previous = JSON.parse(fs.readFileSync(previousFile, 'utf8').replace(/^\uFEFF/, ''));
        const reused = previous.Stages.find(item => item.Stage === stage.Stage && item.Commit === stage.Commit);
        assert.equal(reused?.Status, 'passed', 'Reused stage did not pass');
        assert.equal(reused.FixtureLocatorIsolation.SourceRoot, locator.SourceRoot, 'Reused archive differs');
        assert.equal(reused.NativeParityEvidence, stage.NativeParityEvidence, 'Reused native proof differs');
        assert.deepEqual(reused.Tests.map(test => test.Trx).sort(), stage.Tests.map(test => test.Trx).sort(), 'Reused TRX proofs differ');
    }
    assert.equal(path.resolve(locator.FixedRelativeRoot), sourceRoot, 'Fixed-relative fixtures escaped their archived stage');
    assert.equal(locator.AncestorLookup.length, 2, 'Archived fixture locator coverage differs');
    assert.deepEqual(locator.AncestorLookup.map(fixture => fixture.Fixture).sort(),
        ['J5001_C_BL01_2026-01-31.xer', 'J5001_C_BL01_Retained_2026-01-31.xer']);
    for (const fixture of locator.AncestorLookup) {
        containedPath(fixture.ResolvedPath, path.join(sourceRoot, 'review/fixtures'));
        assert.match(fixture.Sha256, /^[a-f\d]{64}$/i);
    }
    const counts = [];
    for (const project of stage.Tests) {
        assert.ok(project.Passed > 0 && project.Executed > 0, 'Empty archived test suite');
        assert.equal(project.Failed, 0, 'Archived test failure');
        assert.equal(project.SkippedOrNotExecuted, project.Project.includes('Core.Tests') ? 0 : 2,
            'Archived suite skipped coverage differs');
        const trxPath = containedPath(project.Trx, archiveRoot);
        const xml = fs.readFileSync(trxPath, 'utf8');
        const trx = Object.fromEntries([...xml.match(/<Counters\b[^>]*>/)[0].matchAll(/(\w+)="(\d+)"/g)]
            .map(match => [match[1], Number(match[2])]));
        assert.equal(trx.passed, project.Passed); assert.equal(trx.executed, project.Executed);
        assert.equal(trx.failed, 0); assert.equal(trx.error, 0);
        assert.equal(trxSkipped(trx), project.SkippedOrNotExecuted);
        counts.push(`${project.Project.includes('Core.Tests') ? 'Core' : 'CLI'}: ${project.Passed} passed, ${project.SkippedOrNotExecuted} skipped`);
    }
    assert.equal(stage.NativeIterations, 2, 'Archived stage native comparison must cover both iterations');
    const nativePath = containedPath(stage.NativeParityEvidence, archiveRoot);
    const stageParity = JSON.parse(fs.readFileSync(nativePath, 'utf8').replace(/^\uFEFF/, ''));
    assert.equal(stageParity.Cases, 32); assert.equal(stageParity.Failures, 0);
    assert.equal(stageParity.Results.length, 32);
    assert.ok(stageParity.Results.every(result => result.Passed === true), `Archived stage ${stage.Stage} output difference`);
    stageRows.push([stage.Stage, stage.Commit.slice(0, 12), counts.join('; '), '32/32 match × 2', 'Both fixtures isolated']);
}
const measured = benchmark.runs.filter(run => !run.warmup && Number.isInteger(run.iteration));
const groups = [];
for (const browser of ['chrome', 'msedge']) for (const mode of ['legacy', 'programme', 'tender']) {
    const before = measured.filter(run => run.browser === browser && run.mode === mode && run.variant === 'baseline');
    const after = measured.filter(run => run.browser === browser && run.mode === mode && run.variant === 'candidate');
    if (before.length !== 5 || after.length !== 5) throw new Error(`Incomplete five-pair group: ${browser}/${mode}`);
    assert.deepEqual(before.map(run => run.iteration).sort(), [0, 1, 2, 3, 4]);
    assert.deepEqual(after.map(run => run.iteration).sort(), [0, 1, 2, 3, 4]);
    for (const variant of ['baseline', 'candidate']) {
        assert.ok(benchmark.runs.some(run => run.browser === browser && run.mode === mode && run.variant === variant &&
            run.iteration === 'second-history'), 'Missing second/history browser pair');
        assert.equal(benchmark.runs.filter(run => run.browser === browser && run.mode === mode && run.variant === variant &&
            run.warmup).length, 1, 'One primary warm-up per browser/mode/package is required');
    }
    for (const run of after) {
        const processing = run.workerMetrics.filter(metric => metric.kind !== 'metadata');
        assert.ok(processing.length && processing.every(metric => metric.cacheHits === 0 && metric.cacheBudgetBytes === 268435456),
            'Primary run was not a fresh parse with the normal cache budget');
        assert.equal(processing.reduce((sum, metric) => sum + metric.parsedSources, 0), run.files.length);
    }
    const paired = matchedReductions(before, after);
    const baselineMs = median(before.map(run => run.processingMs)), candidateMs = median(after.map(run => run.processingMs));
    groups.push({ browser, mode, version: after[0].browserVersion, baselineMs, candidateMs,
        improvement: (1 - candidateMs / baselineMs) * 100,
        pairedMedianReduction: paired.median, pairedReductionMinimum: paired.minimum, pairedReductionMaximum: paired.maximum,
        pairs: paired.pairs,
        beforeTotal: median(before.map(run => run.totalMs)), afterTotal: median(after.map(run => run.totalMs)),
        beforeGap: median(before.map(run => run.maxPageTimerGapMs)), afterGap: median(after.map(run => run.maxPageTimerGapMs)) });
}
const phases = ['FileReading', 'Metadata', 'SourceSha256', 'Parsing', 'Merging', 'Transformation',
    'StandaloneValidation', 'CsvSerialization', 'CsvHashing', 'ZipCompressionFinalization',
    'resultSerialization', 'zipTransfer', 'downloadPreparation', 'browserHandoff'];
const phaseRows = [];
const observedDiagnosis = observer.runs.filter(run => !run.warmup && run.browser === 'chrome').map(observedPhaseShares);
const diagnosisRows = observedDiagnosis.map(item => [item.mode, seconds(item.processingMs), seconds(item.validationMs),
    item.validationPercent.toFixed(1) + '%', seconds(item.transformationMs), item.transformationPercent.toFixed(1) + '%']);
for (const mode of ['legacy', 'programme', 'tender']) for (const phase of phases) {
    const after = measured.filter(run => run.browser === 'chrome' && run.mode === mode && run.variant === 'candidate')
        .map(run => measuredTotal(run.workerMetrics, phase, metric => metric.stageMilliseconds)).filter(Number.isFinite);
    const before = observer.runs.filter(run => !run.warmup && run.browser === 'chrome' && run.mode === mode)
        .map(run => measuredTotal(run.observerPhases, `exclusive.${phase}`, record => record.metrics)).filter(Number.isFinite);
    phaseRows.push([mode, phase, seconds(median(before)), seconds(median(after)), before.length, after.length]);
}
const firstUse = benchmark.runs.filter(run => run.warmup).map(run => [run.browser, run.mode, run.variant,
    seconds(run.uiStartupMs), seconds(run.uploadMs), seconds(run.processingMs), seconds(run.totalMs)]);
const testRows = [];
for (const [name, file] of [['Core', 'final-core-r3.trx'], ['Surface', 'final-surface.trx'], ['Relationship CLI', 'final-relationship-cli.trx']]) {
    const xml = fs.readFileSync(path.join(root, 'final-tests', file), 'utf8');
    const counts = Object.fromEntries([...xml.match(/<Counters\b[^>]*>/)[0].matchAll(/(\w+)="(\d+)"/g)].map(match => [match[1], Number(match[2])]));
    const skipped = trxSkipped(counts);
    assert.equal(counts.failed, 0, `${name} tests failed`);
    assert.equal(skipped, name === 'Relationship CLI' ? 2 : 0, `${name} test skips changed`);
    if (name === 'Core') {
        assert.equal(counts.passed, release.CoreTestsPassed);
        assert.equal(counts.passed, nativeBuild.CoreTestsPassed);
        assert.equal(counts.passed, 1469, 'Final compact-parser Core suite is required');
    } else assert.equal(counts.passed, name === 'Surface' ? 100 : 36, `${name} full suite coverage changed`);
    testRows.push([name, counts.passed, counts.failed, skipped]);
}
testRows.push(['JavaScript download/worker/input hash', nodeTests.pass, nodeTests.fail, nodeTests.skipped],
    ['Fixed-time native byte/diagnostic comparisons', native.Cases, native.Failures, 0],
    ['Matched browser ZIP comparisons', parity.comparedPairs, parity.errors.length, 0]);
for (const item of lifecycle) testRows.push([`Packaged ${item.channel} lifecycle`, item.checks.filter(check => check.passed).length,
    item.checks.filter(check => !check.passed).length, 0]);
const memoryRows = measured.filter(run => run.variant === 'candidate').map(run => {
    const result = run.workerMetrics.at(-1);
    return [run.browser, run.mode, run.iteration, mib(result.allocatedBytes), mib(result.managedHeapBytes),
        mib(result.sampledManagedHeapHighWaterBytes), mib(result.retainedCacheBytes), mib(result.zipBytes), mib(result.zipBufferCapacityBytes)];
});
const incrementalRows = incremental.flatMap(report => (report?.summaries || []).map(item => [report.channel, item.dataset, item.mode, item.scenario,
    item.pairs, seconds(item.baselineMedianMs), seconds(item.candidateMedianMs)]));
const hotCycleRows = [], hotSummaryRows = [], allocationRows = [], hotSummaries = [];
for (const report of lifecycle) {
    const evidence = report.checks.find(check => check.name === memoryCheckNames[1]).evidence;
    for (const scope of hotCycleScopes(evidence)) {
        const cycles = scope.cycles;
        const metrics = [['Retained cache estimate', 'retainedCacheBytes'], ['Worker managed sample', 'managedHeapBytes'],
            ['Sampled managed high-water', 'sampledManagedHeapHighWaterBytes'], ['Collected main JS heap', 'mainJsHeapUsedBytes'],
            ['Browser process working set sample', 'browserProcessWorkingSetBytes'], ['Browser process private sample', 'browserProcessPrivateBytes'],
            ['ZIP buffer capacity', 'zipBufferCapacityBytes']];
        const summaries = Object.fromEntries([...metrics.map(([, key]) => key), 'retainedSourceCount', 'cacheHits', 'liveWorkers',
            'allocatedBytes', 'cumulativeAllocatedBytes'].map(key => [key, cycleSummary(cycles, key)]));
        hotSummaries.push({ browser: report.channel, profile: scope.profile, dataset: scope.dataset,
            baselineInputBytes: scope.baselineInputBytes, exportScope: scope.exportScope, ...summaries,
            cacheAfterClear: scope.cacheAfterClear, sourcesAfterClear: scope.sourcesAfterClear });
        for (const [label, key] of metrics) {
            const item = summaries[key];
            hotSummaryRows.push([report.channel, scope.profile, label, mib(item.first), mib(item.last),
                `${mib(item.minimum)}–${mib(item.maximum)}`, `${item.available}/${item.cycles}`]);
        }
        for (const [index, cycle] of cycles.entries()) {
            hotCycleRows.push([report.channel, scope.profile, index + 1, cycle.parsedSources, cycle.cacheHits, cycle.retainedSourceCount,
                mib(cycle.retainedCacheBytes), mib(cycle.managedHeapBytes), mib(cycle.mainJsHeapUsedBytes),
                mib(cycle.browserProcessWorkingSetBytes), mib(cycle.browserProcessPrivateBytes),
                cycle.liveWorkers, cycle.createdWorkers, cycle.preparedUrls]);
            allocationRows.push([report.channel, scope.profile, index + 1, mib(cycle.allocatedBytes), mib(cycle.cumulativeAllocatedBytes)]);
        }
    }
}
const summary = { createdAt: new Date().toISOString(), groups, passedPerformanceTarget: groups.every(group => group.improvement >= 25),
    nativeCases: native.Cases, nativeFailures: native.Failures, browserPairs: parity.comparedPairs, browserParityPassed: parity.passed,
    lifecyclePassed: lifecycle.every(report => report.checks.every(check => check.passed)), machine, observedDiagnosis, hotWorkerCycles: hotSummaries,
    stageIsolationEvidence: release.StageIsolationEvidence, stageIsolationPassed: stageEvidence.Status === 'passed', tests: testRows };
fs.writeFileSync(path.join(root, 'final-summary.json'), JSON.stringify(summary, null, 2));
const html = `<!doctype html><html lang="en-NZ"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Parser performance: before and after</title><style>body{font:16px/1.5 system-ui,sans-serif;color:#152c3b;background:#f4f7fa;margin:0}main{max-width:1180px;margin:auto;padding:32px}h1,h2{line-height:1.2}h2{margin-top:36px}table{border-collapse:collapse;width:100%;background:white;margin:20px 0;font-size:14px}th,td{border:1px solid #d8e0e6;padding:8px;text-align:left}th{background:#e7eef4}p{max-width:100ch}.status{font-size:20px;padding:16px;background:${summary.passedPerformanceTarget ? '#dcf3e5' : '#ffe2d9'};border-radius:8px}.scroll{overflow:auto}code{background:#e7eef4;padding:2px 4px}</style><main>
<h1>Parser performance: before and after</h1><p class="status">${summary.passedPerformanceTarget ? 'All six browser/mode groups meet the 25% processing-time target.' : 'The processing-time target remains incomplete.'}</p>
<p>Frozen complete working code: <code>90b98d3</code>. Published compatibility and ordering reference: <code>9254d6a</code>. The frozen archive SHA-256 is <code>B356083271EB1A32E65B6C8CF3B81D4B4A925BAB307A1C6602407DE8E3B5C4E8</code>. Existing unpublished CSV-to-ZIP changes belong to the baseline and receive no credit here. Validation and full diagnostics remain enabled.</p>
<h2>Primary fresh parsing</h2><p>Five matched pairs per mode/browser after one warm-up for each implementation, alternating order. Both packages use SDK 8.0.421, WebAssembly 8.0.27, Release interpreter builds, and Fastest ZIP compression. The normal 256 MiB candidate cache remains enabled; sources are removed and re-registered between runs, and every measured source is freshly parsed with zero cache hits. The 13,424,712-byte sample SHA-256 is <code>${cache.Sources[0].SourceSha256}</code>. Browser automation uses installed Chrome and Edge in headless mode, served locally beneath the actual GitHub Pages path <code>/XER-to-CSV/</code>.</p>
<p>Machine: ${escape(machine.OS)}; ${escape(machine.Processor.map(processor => processor.Name).join(', '))}; ${(machine.PhysicalMemoryBytes / 1024 ** 3).toFixed(1)} GiB physical memory; Node ${escape(machine.NodeVersion)}. Build: ${escape(machine.Build)}; SDK ${escape(machine.Sdk)}, WASM runtime ${escape(machine.WebAssemblyRuntime)}, compression ${escape(machine.ZipCompression)}.</p>
<div class="scroll">${table(['Browser/version','Mode','Before median s','After median s','Reduction of medians','Median paired reduction','Paired reduction range','Before user elapsed s','After user elapsed s'], groups.map(group => [group.browser+' '+group.version, group.mode, seconds(group.baselineMs), seconds(group.candidateMs), group.improvement.toFixed(1)+'%', group.pairedMedianReduction.toFixed(1)+'%', group.pairedReductionMinimum.toFixed(1)+'%–'+group.pairedReductionMaximum.toFixed(1)+'%', seconds(group.beforeTotal), seconds(group.afterTotal)]))}</div>
<p>Reduction of medians is <code>1 − median(after) / median(before)</code>, the defined performance target. Paired reduction is calculated separately for each matching iteration, then summarised by its median and range. These measures can differ when runtime tiering and run-to-run variation affect the paired observations.</p>
${table(['Browser','Mode','Pair','Before s','After s','Matched reduction'],groups.flatMap(group => group.pairs.map(pair => [group.browser,group.mode,pair.iteration+1,seconds(pair.baselineMs),seconds(pair.candidateMs),pair.reduction.toFixed(1)+'%'])))}
<p>Processing runs from Parse/Validate through browser download handoff; Legacy includes parsing and Enhanced export. User elapsed additionally includes file admission and metadata. Startup is excluded from the matched processing medians and recorded below. These local results do not measure GitHub Pages network latency.</p>
<h2>What caused the delay</h2><p>The separate validation passes accounted for a small share of observed frozen runtime, while transformation dominated. The table divides each exclusive phase duration by that mode's measured processing time in the separate observer-only Chrome run. These are attribution observations, not the primary five-pair speed measurements, and receive no credit towards the performance target.</p>
${table(['Mode','Observed processing s','Standalone validation s','Validation share','Transformation s','Transformation share'],diagnosisRows)}
<p>Inline cell conversion, normalisation and their data checks belong to Transformation. The standalone share therefore does not isolate every validation-related instruction. Required-field checks, malformed-value reporting, source evidence, relationships and classifications are preserved. Saved work comes from conversion reuse, column bindings, calendar indexes, less row copying, buffered CSV writing and native browser hashing. The worker also keeps the page responsive.</p>
${table(['Browser','Mode','Before median maximum 50-ms timer gap ms','After median maximum gap ms'], groups.map(group => [group.browser,group.mode,group.beforeGap.toFixed(0),group.afterGap.toFixed(0)]))}
<h2>Phase evidence</h2><p>Seconds per operation. Candidate values are medians from the five Chrome primary runs. Before attribution comes from the separate observer-only frozen package, whose native outputs match the original frozen package in all 32 cases; that package is excluded from speed claims. Only exclusive phase values may be summed. Table-detail and coarse pipeline values in raw JSON overlap these phases. Inline conversion belongs to transformation. Missing browser-only phases in the original implementation are labelled unavailable rather than assumed zero.</p>
${table(['Mode','Phase','Observed frozen s','Candidate median s','Frozen observations','Candidate observations'],phaseRows)}
<h2>First use</h2><p>The first warm-up includes worker startup inside file admission. UI startup is listed separately; total elapsed begins at file selection. These are single observations per package/mode, rather than five-pair startup medians.</p>
${table(['Browser','Mode','Package','UI startup s','File admission s','Processing s','Total user elapsed s'],firstUse)}
<h2>Correctness and tests</h2><p>Fixed timestamps are used for native before/after comparisons. Extracted CSV bytes, headers, row order, file sets, manifests, SHA-256 values, complete diagnostic CSVs and malformed-input errors agree. The review bundles retain all eleven tables plus their manifest and existing schema versions. The faster sort explicitly reproduces published joined-key ordering for prefixes, separator/control characters and stable ties; the edge-case correction from the unpublished sort is covered by a published-order oracle. Browser comparisons normalize only validated export timestamps and their matching bundle directory spans. No data/diagnostic cells are ignored.</p>
${table(['Check','Passed','Failed','Skipped'],testRows)}
<p>The starting position was 1,363 passing Core tests, 36 passing relationship CLI tests with two skips, and five download plus three surface failures. Download lifecycle and surface behaviour now have passing behavioural coverage. Calendar and relationship tests retain frozen implementations as test-only differential oracles. The two CLI skips are the existing Windows symlink cases.</p>
<h3>Independent stage checks</h3><p>Stages A, C, B and E are archived from their exact commits into independent source directories and built with the pinned SDK. Each archive runs its complete Core/relationship CLI suites and the 32-case streaming comparison against the frozen baseline. Fixture locator evidence verifies both the fixed-relative and ancestor-lookup paths stay within that archive. Historical Web source-text surface tests are explicitly excluded from these archived stages; final stage D has the separate 100-test behavioural surface proof above.</p>
${table(['Stage','Commit','Automated suites','Native streaming outputs','Fixture paths'],stageRows)}
<h2>Repeated exports and changes</h2><p>These timings compare the actual actions required by each package. Frozen Legacy releases its parsed <code>_dataStore</code> after every export, so each repeated-export measurement includes its required Parse action before Export. The candidate retains its parsed raw/Standard view and exports directly. This retention change is part of the authorised streaming/session work. Review baseline exports already reparse internally. Synthetic schedules are small, so their subsecond results can be dominated by timing noise; the five synthetic pairs and single real-source pairs are shown separately and do not establish the primary performance target.</p>
${table(['Browser','Dataset','Mode','Action','Matched pairs','Before median s','After median s'],incrementalRows)}
<p>Lifecycle checks cover cancellation during startup, parsing, processing and transfer; the 250-ms termination timer; stale messages; two-chunk acknowledgement limits; preparation/discard/retry; worker startup failure and in-page fallback; metadata/profile changes; eviction; repeated Selected/All exports; and ten consecutive update/export cycles.</p>
<h2>Memory evidence</h2><p>The 256 MiB limit bounds conservative retained raw-cache estimates, including original evidence, excess source cells, headers and unused row slots. The two real sources total ${mib(cache.CombinedSourceEstimateBytes)} MiB and fit together. Active views, input buffers, ZIP buffers, browser Blob storage and runtime memory are additional. Cumulative allocated bytes measure allocation work, not retained memory. Managed samples and process working/private sets are point indicators, and sampled high-water values are not total browser peaks. The Core snapshots precede worker result JSON serialization and browser preparation; those have separate size/time evidence.</p>
<div class="scroll">${table(['Browser','Mode','Pair','Allocated MiB','Managed sample MiB','Sampled high-water MiB','Retained estimate MiB','ZIP MiB','ZIP capacity MiB'],memoryRows)}</div>
<h3>Ten updates on one continuously retained worker per profile</h3><p>The real 13.4 MB baseline test selects Enhanced <code>01_XER_TASK</code>, <code>06_XER_PREDECESSOR</code> and <code>15_XER_RESOURCE_DISTRIBUTION</code>, retaining the Standard derived view while replacing small synthetic updates. Programme and Tender each use a separate small synthetic baseline/update pair and export complete review bundles through ten changing updates and project-name metadata edits. Each profile keeps one worker throughout its ten cycles, discards every prepared output and clears only after the final cycle. This tests logical retention through real calculations and full review lifetimes; it does not measure real-history review peaks or all-fourteen-table Enhanced peaks.</p>
<p>Values below are observed first/last samples and observed ranges. Process working/private values cover the isolated browser processes available to the harness. Missing process measurements are unavailable. The collected page heap excludes worker/native Blob storage and includes the harness's accumulated event/progress and URL/worker descriptor histories. That instrumentation overhead means small page-heap growth is not a clean application-only leak measure. Neither these samples nor a small first-to-last change prove a total-memory peak or a steady-state plateau.</p>
<div class="scroll">${table(['Browser','Profile','Indicator','First MiB','Last MiB','Observed range MiB','Available cycles'],hotSummaryRows)}</div>
<div class="scroll">${table(['Browser','Profile','Cycle','Parsed','Cache hits','Retained sources','Cache estimate MiB','Managed sample MiB','Collected page heap MiB','Process working MiB','Process private MiB','Live workers','New workers','Prepared URLs'],hotCycleRows)}</div>
<h3>Allocation work in the hot-worker cycles</h3><p>Per-cycle allocation is the sum of managed metadata/parse/export job counters for Enhanced, or metadata/full-review job counters for each review profile. Cumulative allocation starts separately at cycle one for each profile; it excludes initial baseline preparation, source removal/clear jobs and browser JavaScript/native allocation. Runtime allocation counters are coarse and can report zero for a small review job; zero does not prove that job allocated nothing. These counters describe allocation work and must not be added to retained-memory samples.</p>
${table(['Browser','Profile','Cycle','Job allocations MiB','Cumulative job allocations MiB'],allocationRows)}
<p>The separate clear/restart check records ten complete worker teardowns, zero cache/source counts after clear, and zero live workers/prepared URLs. Main-page garbage collection is requested only in these memory experiments, after output disposal; normal processing timings do not force collection. The lifecycle JSON retains every cycle and its measurement scope.</p>
<h2>Release readiness and reproduction</h2><p>The UI and dedicated single-threaded .NET 8 worker are packaged together with matching version identifiers; the local Pages base path and forced startup-fallback path are exercised in both browsers. Release publish: ${release.WarningCount} warnings, ${release.ErrorCount} errors. The complete implementation diff against frozen <code>90b98d3</code> passes <code>git diff --check</code>. The branch remains local; publication requires a separate release request. All schedule processing and cached inputs stay within the browser session.</p>
<p>Reproduction commands, observer patch and harness sources are beside this report. Private schedules, extracted evidence, ZIPs and raw timing reports remain under ignored <code>artifacts/parser-performance-20260930/</code>. The independent primary report is <code>browser-benchmark.json</code>; correctness is <code>browser-output-parity.json</code>; native parity is <code>native-r3-parity.json</code>; detailed lifecycle reports are <code>browser-lifecycle-chrome.json</code> and <code>browser-lifecycle-msedge.json</code>. The generator refuses missing, partial, failed or stale final evidence and does not fall back to an older revision.</p>
</main></html>`;
fs.writeFileSync(path.join(__dirname, 'before-after-report.html'), html);
console.log(JSON.stringify(summary, null, 2));
