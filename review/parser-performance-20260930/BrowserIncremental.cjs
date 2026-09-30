// Paired incremental browser workloads. Runs the published UI in installed
// Chrome/Edge; each runtime is warmed before measured work. No builds occur.
// node review/parser-performance-20260930/BrowserIncremental.cjs --channel chrome --pairs 5
// Optional one-pair real workloads: --real-primary PATH --real-prior PATH --real-pairs 1
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { dependency } = require('./browser-dependencies.cjs');
const { chromium } = dependency('playwright');
const JSZip = dependency('jszip');
const args = Object.fromEntries(Array.from({ length: Math.floor((process.argv.length - 2) / 2) }, (_, index) =>
    [process.argv[2 + index * 2].replace(/^--/, ''), process.argv[3 + index * 2]]));
const repo = path.resolve(__dirname, '../..');
const channel = args.channel || 'chrome', pairs = Number(args.pairs || 5);
const output = path.resolve(args.output || path.join(repo, 'artifacts/parser-performance-20260930', `browser-incremental-${channel}.json`));
const report = { createdAt: new Date().toISOString(), channel, baseline: '90b98d3', runs: [], comparisons: [], summaries: [],
    measurement: 'Click/upload to browser download event; ZIP saving, CSV verification, resets and seed warmups are excluded. Legacy baseline repeated export includes its required Parse action because the frozen UI releases its data store after each export; candidate retains the parsed view and exports directly. Only time-dependent bundle directory timestamps and manifest bundle_id/exported_at_utc are normalised. All other CSV bytes and row/entry order are compared; manifest-declared table hashes are reconciled.' };
function save() { fs.mkdirSync(path.dirname(output), { recursive: true }); fs.writeFileSync(output, JSON.stringify(report, null, 2)); }
const hash = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
function source(name, bytes) {
    const lines = bytes.toString('utf8').split(/\r?\n/), begin = lines.findIndex(line => line === '%T\tPROJECT');
    const headers = lines[begin + 1].split('\t').slice(1), row = lines[begin + 2].split('\t').slice(1);
    const dataDate = row[headers.indexOf('last_recalc_date')].slice(0, 10);
    assert.match(dataDate, /^\d{4}-\d{2}-\d{2}$/);
    return { name, bytes, dataDate, inputBytes: bytes.length, sha256: hash(bytes) };
}
const fixture = fs.readFileSync(path.join(repo, 'review/fixtures/J5001_C_BL01_2026-01-31.xer'));
const synthetic = { name: 'synthetic', repeat: source('2601 seed.xer', fixture),
    prior: source('2601 seed.xer', fixture), update: source('2602 update.xer', Buffer.from(fixture.toString('utf8')
        .replaceAll('2026-01-30', '2026-02-27').replaceAll('Synthetic A100', 'Synthetic updated A100'))) };
const datasets = [synthetic];
if (args['real-primary']) {
    const repeat = source(path.basename(args['real-primary']), fs.readFileSync(args['real-primary']));
    datasets.push({ name: 'real', repeat,
        prior: args['real-prior'] ? source(path.basename(args['real-prior']), fs.readFileSync(args['real-prior'])) : repeat,
        update: repeat });
}
function monthEnd(date) {
    const [year, month] = date.split('-').map(Number);
    return new Date(Date.UTC(year, month, 0)).toISOString().slice(0, 10);
}
function parseCsv(text) {
    const rows = [], row = []; let cell = '', quoted = false;
    for (let index = 0; index < text.length; index++) {
        const character = text[index];
        if (character === '"') { if (quoted && text[index + 1] === '"') { cell += '"'; index++; } else quoted = !quoted; }
        else if (!quoted && character === ',') { row.push(cell); cell = ''; }
        else if (!quoted && (character === '\r' || character === '\n')) {
            if (character === '\r' && text[index + 1] === '\n') index++;
            row.push(cell); rows.push(row.splice(0)); cell = '';
        } else cell += character;
    }
    if (cell.length || row.length) { row.push(cell); rows.push(row); }
    return rows;
}
async function zipEvidence(file) {
    const zip = await JSZip.loadAsync(fs.readFileSync(file)), evidence = [], manifests = [];
    const entries = Object.values(zip.files).filter(entry => !entry.dir);
    const roots = new Set(entries.filter(entry => entry.name.includes('/')).map(entry => entry.name.split('/')[0]));
    assert.ok(roots.size <= 1, 'A bundle must contain one root directory.');
    for (const entry of entries) {
        const bytes = await entry.async('nodebuffer'), rows = parseCsv(bytes.toString('utf8').replace(/^\uFEFF/, ''));
        if (/MANIFEST/i.test(entry.name)) {
            manifests.push(rows);
            for (const name of ['bundle_id', 'exported_at_utc']) {
                const column = rows[0].indexOf(name);
                if (column >= 0) for (const row of rows.slice(1)) row[column] = name === 'bundle_id'
                    ? row[column].replace(/_\d{8}T\d{6}Z_/, '_EXPORT_TIME_') : 'EXPORT_TIME';
            }
        }
        const segments = entry.name.split('/');
        const normalizedName = segments.map((segment, index) => index < segments.length - 1
            ? segment.replace(/_\d{8}T\d{6}Z_/, '_EXPORT_TIME_') : segment).join('/');
        evidence.push({ name: entry.name, normalizedName, byteCount: bytes.length, csvSha256: hash(bytes),
            normalizedCellsSha256: hash(JSON.stringify(rows)), dataRows: Math.max(0, rows.length - 1) });
    }
    for (const rows of manifests) {
        const table = rows[0].indexOf('table_name'), declaredHash = rows[0].indexOf('csv_sha256');
        assert.ok(table >= 0 && declaredHash >= 0, 'Review manifest must declare table hashes.');
        for (const row of rows.slice(1)) {
            const entry = evidence.find(entry => path.posix.basename(entry.name) === row[table]
                || path.posix.basename(entry.name) === row[table] + '.csv');
            assert.ok(entry, `Manifest table ${row[table]} must exist in ZIP.`);
            assert.equal(row[declaredHash], entry.csvSha256, `Manifest table ${row[table]} hash must match exact CSV bytes.`);
        }
    }
    return evidence;
}
async function prepare(browser, variant, mode) {
    const page = await browser.newPage({ acceptDownloads: true }); page.setDefaultTimeout(120000);
    const state = { page, variant, mode, errors: [], budget: undefined };
    page.on('pageerror', error => state.errors.push(error.message));
    await page.addInitScript(() => {
        window.__incremental = { jobs: [], results: [], cacheBudgetBytes: undefined };
        const NativeWorker = Worker;
        window.Worker = class extends NativeWorker {
            constructor(...args) {
                super(...args);
                this.addEventListener('message', ({ data }) => {
                    if (data.kind === 'result') {
                        const { zipContent, diagnostics, diagnosticMessages, csvSha256ByFile, ...summary } = data.result;
                        __incremental.results.push(summary);
                    }
                });
            }
            postMessage(value, ...rest) {
                if (value.kind === 'run') {
                    if (__incremental.cacheBudgetBytes !== undefined) value.job.cacheBudgetBytes = __incremental.cacheBudgetBytes;
                    __incremental.jobs.push(JSON.parse(JSON.stringify(value.job)));
                }
                return super.postMessage(value, ...rest);
            }
        };
    });
    const base = args[`${variant}-url`] || `http://127.0.0.1:${variant === 'baseline' ? 8766 : 8767}/XER-to-CSV/`;
    await page.goto(base); await page.locator('input[type=file]').waitFor({ state: 'attached' });
    state.applicationVersion = await page.locator('meta[name="application-version"]').getAttribute('content');
    state.assetHashes = await page.evaluate(async variant => {
        const hashes = {};
        for (const file of variant === 'candidate' ? ['_framework/blazor.boot.json', 'worker/_framework/blazor.boot.json', 'js/workerEngine.js'] : ['_framework/blazor.boot.json']) {
            const bytes = await (await fetch(new URL(file, document.baseURI))).arrayBuffer();
            hashes[file] = [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))]
                .map(byte => byte.toString(16).padStart(2, '0')).join('');
        }
        return hashes;
    }, variant);
    if (mode === 'legacy') await page.locator('#chkPowerBi').check();
    else {
        await page.locator('.profile-option').filter({ hasText: mode === 'programme' ? 'Programme Review' : 'Tender Review' }).locator('input').check();
        await page.getByPlaceholder(mode === 'programme' ? 'Example: QAC000623-01-02' : 'Example: QAC000623', { exact: true }).fill('INCREMENTAL');
        await page.getByPlaceholder('Governed project name').fill('Parser incremental verification');
        if (mode === 'programme') await page.locator('.form-grid select').selectOption('C');
        await page.locator('body').click({ position: { x: 5, y: 5 } });
    }
    return state;
}
async function reset(state) {
    while (await state.page.locator('.file-item').count()) await state.page.locator('.btn-remove').first().click();
    await state.page.waitForFunction(() => !document.querySelector('input[type=file]').disabled);
    await state.page.evaluate(() => { __incremental.cacheBudgetBytes = undefined; __incremental.jobs.length = 0; __incremental.results.length = 0; });
    state.sources = [];
}
async function configure(state) {
    const { page, mode, sources } = state;
    if (mode === 'programme') {
        const rows = page.locator('.snapshot-table tbody tr');
        for (let index = 0; index < sources.length; index++) {
            const row = rows.nth(index), item = sources[index];
            await row.locator('select').selectOption(index === 0 ? 'Baseline' : 'Update');
            await row.locator('input').nth(0).fill(index === 0 ? 'BL01' : item.dataDate.slice(2, 7).replace('-', ''));
            await row.locator('input').nth(1).fill(item.dataDate.slice(0, 7) + '-01');
            await row.locator('input').nth(2).fill(item.dataDate);
        }
    } else if (mode === 'tender') {
        const inputs = page.locator('.tender-stage-table tbody input');
        for (let index = 0; index < sources.length; index++) await inputs.nth(index).fill(monthEnd(sources[index].dataDate));
    }
    await page.locator('body').click({ position: { x: 5, y: 5 } });
}
async function add(state, item) {
    const input = state.page.locator('input[type=file]');
    await input.evaluate(element => { element.value = ''; });
    await input.setInputFiles({ name: item.name, mimeType: 'application/octet-stream', buffer: item.bytes });
    state.sources.push(item);
    await state.page.waitForFunction(count => document.querySelectorAll('.file-item').length === count
        && !document.querySelector('input[type=file]').disabled, state.sources.length);
    await configure(state);
}
async function parseLegacy(state) {
    await state.page.getByRole('button', { name: 'Parse and load tables from uploaded XER files', exact: true }).click();
    await state.page.getByRole('button', { name: 'Export Power BI as ZIP', exact: true }).waitFor();
    await state.page.waitForFunction(() => ![...document.querySelectorAll('button')].find(button => button.innerText === 'Export Power BI as ZIP')?.disabled);
}
async function exportOne(state, dataset, scenario, pair, warmup = false, operation) {
    const { page, mode, variant } = state;
    await page.evaluate(() => { __incremental.jobs.length = 0; __incremental.results.length = 0; });
    const started = performance.now();
    if (operation) await operation();
    const event = page.waitForEvent('download');
    await page.getByRole('button', { name: mode === 'legacy' ? 'Export Power BI as ZIP'
        : `Validate and download ${mode === 'programme' ? 'Programme' : 'Tender'} Review bundle`, exact: true }).click();
    const download = await event, elapsedMs = performance.now() - started;
    assert.equal(await download.failure(), null);
    await page.waitForFunction(() => !document.querySelector('input[type=file]').disabled);
    const file = path.join(path.dirname(output), 'incremental-zips', channel, dataset.name, mode, scenario, `${variant}-${warmup ? 'warmup' : pair}.zip`);
    fs.mkdirSync(path.dirname(file), { recursive: true }); await download.saveAs(file);
    const worker = await page.evaluate(() => ({ jobs: __incremental.jobs, results: __incremental.results }));
    const run = { dataset: dataset.name, mode, variant, scenario, pair, warmup, elapsedMs,
        applicationVersion: state.applicationVersion, assetHashes: state.assetHashes, sourceNames: state.sources.map(item => item.name),
        inputBytes: state.sources.reduce((sum, item) => sum + item.inputBytes, 0),
        worker, files: await zipEvidence(file), errors: state.errors.slice(), zip: file };
    report.runs.push(run); save();
    console.log(JSON.stringify({ dataset: run.dataset, mode, variant, scenario, pair, warmup, elapsedMs: Math.round(elapsedMs),
        cache: worker.results.map(result => ({ parsed: result.parsedSources, hits: result.cacheHits, retained: result.retainedCacheBytes })) }));
    return run;
}
function compare(first, second) {
    const signature = run => run.files.map(file => ({ name: file.normalizedName, rows: file.dataRows,
        sha256: /MANIFEST/i.test(file.name) ? file.normalizedCellsSha256 : file.csvSha256 }));
    assert.deepEqual(signature(first), signature(second), `${first.dataset}/${first.mode}/${first.scenario}/${first.pair} exact CSV/manifest cell/order parity`);
    report.comparisons.push({ dataset: first.dataset, mode: first.mode, scenario: first.scenario, pair: first.pair,
        matchedExactCsvBytesManifestCellsAndOrder: true, baselineMs: first.elapsedMs, candidateMs: second.elapsedMs,
        speedup: first.elapsedMs / second.elapsedMs }); save();
}
function median(values) { const sorted = values.toSorted((left, right) => left - right); return (sorted[Math.floor((sorted.length - 1) / 2)] + sorted[Math.floor(sorted.length / 2)]) / 2; }
(async () => {
    let browser, currentState;
    try {
        browser = await chromium.launch({ channel, headless: true }); report.browserVersion = browser.version();
        for (const dataset of datasets) for (const mode of (args.modes || 'legacy,programme,tender').split(',')) {
            const states = {};
            for (const variant of ['baseline', 'candidate']) states[variant] = await prepare(browser, variant, mode);
            const count = dataset.name === 'real' ? Number(args['real-pairs'] || 1) : pairs;
            const scenarios = ['repeated-export', ...(dataset.name === 'synthetic' && mode !== 'legacy' ? ['metadata-edit'] : []),
                ...(dataset.prior.sha256 !== dataset.update.sha256 ? ['adding-update'] : []),
                ...(dataset.name === 'synthetic' ? ['evicted-reread'] : [])];
            for (const scenario of scenarios) {
                if (scenario !== 'adding-update') for (const state of Object.values(states)) {
                    await reset(state); await add(state, dataset.repeat);
                    if (mode === 'legacy') await parseLegacy(state);
                    await exportOne(state, dataset, scenario, -1, true);
                }
                for (let pair = 0; pair < count; pair++) {
                    const results = {};
                    for (const variant of pair % 2 ? ['candidate', 'baseline'] : ['baseline', 'candidate']) {
                        const state = states[variant];
                        currentState = state;
                        if (scenario === 'adding-update') {
                            await reset(state); await add(state, dataset.prior);
                            if (mode === 'legacy') await parseLegacy(state);
                            await exportOne(state, dataset, scenario, `seed-${pair}`, true);
                        }
                        const operation = scenario === 'adding-update' ? async () => { await add(state, dataset.update); if (mode === 'legacy') await parseLegacy(state); }
                            : scenario === 'repeated-export' && mode === 'legacy' && variant === 'baseline' ? async () => { await parseLegacy(state); }
                            : scenario === 'metadata-edit' ? async () => {
                                await state.page.getByPlaceholder('Governed project name').fill(`Parser incremental metadata ${pair}`);
                                await state.page.locator('body').click({ position: { x: 5, y: 5 } });
                            } : scenario === 'evicted-reread' ? async () => {
                                await state.page.evaluate(() => { __incremental.cacheBudgetBytes = 0; });
                                if (mode === 'legacy') await parseLegacy(state);
                            } : undefined;
                        results[variant] = await exportOne(state, dataset, scenario, pair, false, operation);
                        if (variant === 'candidate') {
                            const actual = results[variant].worker.results;
                            assert.ok(actual.length > 0, 'Candidate must run the real worker, without fallback.');
                            const parsed = actual.reduce((sum, result) => sum + result.parsedSources, 0);
                            if (scenario === 'adding-update' || scenario === 'evicted-reread') assert.equal(parsed, 1);
                            else assert.equal(parsed, 0);
                            if (scenario === 'evicted-reread') assert.ok(actual.every(result => result.retainedCacheBytes === 0));
                        }
                    }
                    compare(results.baseline, results.candidate);
                }
            }
            for (const state of Object.values(states)) await state.page.close();
        }
        for (const dataset of datasets) for (const mode of ['legacy', 'programme', 'tender'])
        for (const scenario of new Set(report.comparisons.filter(pair => pair.dataset === dataset.name && pair.mode === mode).map(pair => pair.scenario))) {
            const matched = report.comparisons.filter(pair => pair.dataset === dataset.name && pair.mode === mode && pair.scenario === scenario);
            report.summaries.push({ dataset: dataset.name, mode, scenario, pairs: matched.length,
                baselineMedianMs: median(matched.map(pair => pair.baselineMs)), candidateMedianMs: median(matched.map(pair => pair.candidateMs)),
                medianPairedSpeedup: median(matched.map(pair => pair.speedup)) });
        }
    } catch (error) {
        report.error = String(error.stack || error); process.exitCode = 1;
        if (currentState) report.failureState = { variant: currentState.variant, mode: currentState.mode,
            sourceNames: currentState.sources?.map(source => source.name), errors: currentState.errors,
            body: await currentState.page.locator('body').innerText().catch(() => undefined),
            worker: await currentState.page.evaluate(() => ({ jobs: __incremental.jobs, results: __incremental.results })).catch(() => undefined) };
    }
    finally { save(); if (browser) await browser.close(); console.log(JSON.stringify({ output, comparisons: report.comparisons.length, error: report.error })); }
})();
