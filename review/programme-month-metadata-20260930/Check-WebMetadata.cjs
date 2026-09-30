// Local, synthetic browser smoke; run only after the before/after packages are ready.
// node review/programme-month-metadata-20260930/Check-WebMetadata.cjs --url http://127.0.0.1:8767/XER-to-CSV/ --before-url http://127.0.0.1:8766/XER-to-CSV/ --channel chrome
// Requires Playwright and JSZip via the portable performance dependency helper.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const { dependency } = require('../parser-performance-20260930/browser-dependencies.cjs');
const { inspectCsv, readZip, comparePair } = require('../parser-performance-20260930/Compare-BrowserOutputs.cjs');
const repo = path.resolve(__dirname, '../..');
const defaultFixture = path.join(repo, 'review/fixtures/J5001_C_BL01_2026-01-31.xer');
const tables = ['01_XER_TASK', '02_XER_PROJECT', '03_XER_PROJWBS', '06_XER_PREDECESSOR',
    '07_XER_ACTVTYPE', '08_XER_ACTVCODE', '09_XER_TASKACTV', '10_XER_CALENDAR',
    '11_XER_CALENDAR_DETAILED', '12_XER_RSRC', '15_XER_RESOURCE_DISTRIBUTION'];
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');

function xerSections(bytes) {
    const lines = bytes.toString('utf8').replace(/^\uFEFF/, '').split(/\r?\n/);
    const sections = new Map(); let current;
    for (const line of lines) {
        const values = line.split('\t');
        if (values[0] === '%T') { current = { headers: [], rows: [] }; sections.set(values[1], current); }
        else if (values[0] === '%F' && current) current.headers = values.slice(1);
        else if (values[0] === '%R' && current) current.rows.push(values.slice(1));
    }
    return sections;
}

// Add a distinct, valid project/WBS/task membership without modifying the original project.
function multiProjectFixture(original, secondDate = '2026-02-25', reverseProjects = false) {
    const sections = xerSections(original), additions = new Map();
    for (const [table, updates] of Object.entries({
        PROJECT: { proj_id: 'P2', last_recalc_date: secondDate ? secondDate + ' 00:00' : '', proj_short_name: 'J5001_SECOND' },
        PROJWBS: { wbs_id: 'W2', parent_wbs_id: '', proj_id: 'P2', wbs_name: 'Synthetic second project', wbs_short_name: 'ROOT2' },
        TASK: { task_id: 'T3', proj_id: 'P2', wbs_id: 'W2', task_code: 'B100', task_name: 'Synthetic second project task' },
        SCHEDOPTIONS: { proj_id: 'P2' }
    })) {
        const section = sections.get(table); assert.ok(section?.rows.length, `Fixture requires ${table}`);
        const row = [...section.rows[0]];
        for (const [column, value] of Object.entries(updates)) {
            const index = section.headers.indexOf(column); assert.ok(index >= 0, `Fixture requires ${table}.${column}`); row[index] = value;
        }
        additions.set(table, '%R\t' + row.join('\t'));
    }
    const lines = original.toString('utf8').trimEnd().split(/\r?\n/), result = [];
    let table;
    for (const line of lines) {
        if (line.startsWith('%T\t') || line === '%E') {
            if (additions.has(table)) { result.push(additions.get(table)); additions.delete(table); }
            table = line.startsWith('%T\t') ? line.slice(3) : undefined;
        }
        result.push(line);
    }
    assert.equal(additions.size, 0, 'Every synthetic row must be inserted');
    if (reverseProjects) {
        const projectStart = result.indexOf('%T\tPROJECT');
        const first = projectStart + 2; assert.ok(result[first].startsWith('%R\t'));
        [result[first], result[first + 1]] = [result[first + 1], result[first]];
    }
    return Buffer.from(result.join('\r\n') + '\r\n');
}

function csvRows(file) {
    const csv = inspectCsv(file.bytes, true);
    return csv.records.slice(1).map(row => Object.fromEntries(csv.headers.map((name, index) => [name, row.fields[index]])));
}
function headerEvidence(zip) { return Object.fromEntries(zip.entries.map(file => [file.name, file.csv.headers])); }
function assertHeaders(zip, expected) {
    assert.deepEqual(Object.keys(headerEvidence(zip)).sort(), Object.keys(expected).sort(), 'CSV file envelope changed');
    for (const file of zip.entries) assert.deepEqual(file.csv.headers, expected[file.name], `Exact header contract changed: ${file.name}`);
}
function assertProgrammeDates(zip, expected) {
    const projects = csvRows(zip.byName.get('02_XER_PROJECT.csv'));
    assert.equal(projects.length, Object.keys(expected.projectDates).length);
    for (const [nativeId, date] of Object.entries(expected.projectDates)) {
        const key = `CSV::J5001::C::BL01::${nativeId}`;
        const matches = projects.filter(row => row.proj_id_key === key); assert.equal(matches.length, 1, `Expected one project key ${key}`);
        assert.equal(matches[0].last_recalc_date, date, `Own PROJECT date changed: ${nativeId}`);
    }
    const projectKeys = new Set(projects.map(row => row.proj_id_key));
    const tasks = csvRows(zip.byName.get('01_XER_TASK.csv'));
    assert.ok(tasks.length > 0);
    for (const task of tasks) {
        assert.equal(task.data_date, expected.snapshotDate, 'TASK data_date must keep the governed file-level snapshot date');
        assert.equal(task.monthupdate, expected.month + '-01', 'Month input must become a first-day DateOnly');
        assert.ok(projectKeys.has(task.proj_id_key), 'TASK must retain source-project membership');
    }
    if (expected.multi) {
        assert.equal(tasks.find(row => row.task_code === 'B100')?.proj_id_key, 'CSV::J5001::C::BL01::P2');
        assert.ok(tasks.some(row => row.proj_id_key === 'CSV::J5001::C::BL01::P1'));
    }
    const manifest = csvRows(zip.byName.get('XER_CSV_MANIFEST.csv'));
    assert.equal(manifest.length, 11);
    for (const row of manifest) {
        assert.equal(row.schema_version, '5.0'); assert.equal(row.data_date, expected.snapshotDate);
        assert.equal(row.monthupdate, expected.month + '-01'); assert.equal(row.snapshot_kind, 'baseline'); assert.equal(row.snapshot_tag, 'BL01');
        assert.equal(row.source_sha256, sha256(expected.sourceBytes)); assert.equal(row.original_xer_filename, expected.sourceName);
    }
    return { snapshotDate: expected.snapshotDate, monthUpdate: expected.month + '-01', projectDates: expected.projectDates,
        taskRows: tasks.length, projectRows: projects.length, manifestRows: manifest.length };
}
function assertTenderDates(zip, sourceBytes, sourceName) {
    const manifest = csvRows(zip.byName.get('XER_CSV_MANIFEST.csv'));
    assert.equal(manifest.length, 11);
    for (const row of manifest) {
        assert.equal(row.schema_version, '4.0'); assert.equal(row.bundle_profile, 'tender_review');
        assert.equal(row.data_date, '2026-01-30'); assert.equal(row.status_date, '2026-09-30');
        assert.equal(row.source_sha256, sha256(sourceBytes)); assert.equal(row.original_xer_filename, sourceName);
    }
}

function argumentsFrom(argv) {
    const args = {};
    for (let index = 0; index < argv.length; index += 2) {
        assert.match(argv[index], /^--[a-z-]+$/); assert.ok(argv[index + 1], `Missing value: ${argv[index]}`);
        args[argv[index].slice(2)] = argv[index + 1];
    }
    return args;
}
async function main() {
    const args = argumentsFrom(process.argv.slice(2)), channel = args.channel || 'chrome';
    const output = path.resolve(args.out || path.join(repo, `artifacts/programme-month-metadata-20260930/web-metadata-${channel}.json`));
    const url = args.url || 'http://127.0.0.1:8767/XER-to-CSV/';
    const fixturePath = path.resolve(args.fixture || defaultFixture), single = fs.readFileSync(fixturePath);
    const singleName = path.basename(fixturePath), multi = multiProjectFixture(single), invalid = multiProjectFixture(single, '');
    const report = { createdAt: new Date().toISOString(), channel, url, beforeUrl: args['before-url'] || null,
        fixture: { path: fixturePath, sha256: sha256(single) }, checks: [], exports: [], comparisons: [], screenshots: [], errors: [], passed: false };
    const browser = await dependency('playwright').chromium.launch({ channel, headless: true });
    report.browserVersion = browser.version();
    fs.mkdirSync(path.dirname(output), { recursive: true });
    const save = () => fs.writeFileSync(output, JSON.stringify(report, null, 2));
    let counter = 0;
    async function screenshot(page, name) {
        const filename = path.join(path.dirname(output), 'screenshots', `${channel}-${name}.png`);
        fs.mkdirSync(path.dirname(filename), { recursive: true });
        await page.screenshot({ path: filename, fullPage: true }); report.screenshots.push(filename);
    }
    async function open(base, mode) {
        const page = await browser.newPage({ acceptDownloads: true }); page.setDefaultTimeout(120000);
        page.on('pageerror', error => report.errors.push({ browserError: error.message }));
        await page.addInitScript(() => {
            window.__metadataSmoke = { ready: 0, results: [], errors: [] };
            const Original = Worker;
            window.Worker = class extends Original {
                constructor(...args) {
                    super(...args); this.addEventListener('message', ({ data }) => {
                        if (data.kind === 'ready') __metadataSmoke.ready++;
                        if (data.kind === 'result') {
                            const r = data.result; __metadataSmoke.results.push({ dataDate: r.dataDate, metadataWarnings: r.metadataWarnings,
                                sourceSha256: r.sourceSha256, fileCount: r.fileCount, diagnosticSha256: r.diagnosticSha256, warningCount: r.warningCount });
                        }
                        if (['error', 'startup-error'].includes(data.kind)) __metadataSmoke.errors.push(data.message);
                    });
                }
            };
        });
        await page.goto(base); await page.locator('input[type=file]').waitFor({ state: 'attached' });
        if (mode !== 'standard') await profile(page, mode);
        return page;
    }
    async function profile(page, mode) {
        await page.locator('.profile-option').filter({ hasText: mode === 'programme' ? 'Programme Review' : 'Tender Review' }).locator('input').check();
        await page.getByPlaceholder(mode === 'programme' ? 'Example: QAC000623-01-02' : 'Example: QAC000623', { exact: true }).fill('J5001');
        await page.getByPlaceholder('Governed project name').fill('Synthetic metadata verification');
        if (mode === 'programme') await page.locator('.form-grid select').selectOption('C');
        await page.locator('body').click({ position: { x: 5, y: 5 } });
    }
    async function upload(page, name, bytes) {
        await page.locator('input[type=file]').setInputFiles({ name, mimeType: 'application/octet-stream', buffer: bytes });
        await page.waitForFunction(() => document.querySelectorAll('.file-item').length === 1 && !document.querySelector('input[type=file]').disabled);
        const worker = await page.evaluate(() => __metadataSmoke);
        assert.ok(worker.ready > 0, 'Actual worker must boot; fallback is covered by separate lifecycle tests');
        const result = worker.results.findLast(result => result.sourceSha256 === sha256(bytes));
        assert.ok(result, 'Worker metadata must hash the exact uploaded immutable File');
        return result;
    }
    async function clear(page) {
        await page.getByRole('button', { name: 'Clear all uploaded files', exact: true }).click();
        await page.waitForFunction(() => document.querySelectorAll('.file-item').length === 0 && !document.querySelector('input[type=file]').disabled);
    }
    async function fields(page, name) {
        const month = page.getByLabel('Month update for ' + name, { exact: true });
        return { type: await month.getAttribute('type'), month: await month.inputValue(),
            kind: await page.getByLabel('Snapshot kind for ' + name, { exact: true }).inputValue(),
            tag: await page.getByLabel('Snapshot tag for ' + name, { exact: true }).inputValue(),
            date: await page.getByLabel('Data date for ' + name, { exact: true }).inputValue() };
    }
    async function configureProgramme(page, name, month = '2026-01', date = '2026-01-30') {
        await page.getByLabel('Snapshot kind for ' + name, { exact: true }).selectOption('Baseline');
        await page.getByLabel('Snapshot tag for ' + name, { exact: true }).fill('BL01');
        const input = page.getByLabel('Month update for ' + name, { exact: true });
        await input.fill(await input.getAttribute('type') === 'month' ? month : month + '-01');
        await page.getByLabel('Data date for ' + name, { exact: true }).fill(date);
        await page.locator('body').click({ position: { x: 5, y: 5 } });
    }
    async function exportBundle(page, mode, scenario, variant, expected) {
        const downloadEvent = page.waitForEvent('download');
        await page.getByRole('button', { name: `Validate and download ${mode === 'programme' ? 'Programme' : 'Tender'} Review bundle`, exact: true }).click();
        const download = await downloadEvent; assert.equal(await download.failure(), null);
        const filename = path.join(path.dirname(output), 'smoke-zips', channel, `${++counter}-${variant}-${scenario}.zip`);
        fs.mkdirSync(path.dirname(filename), { recursive: true }); await download.saveAs(filename);
        await page.waitForFunction(() => !document.querySelector('input[type=file]').disabled);
        const zip = await readZip(filename, mode); assert.equal(zip.entries.length, 12);
        const worker = await page.evaluate(() => __metadataSmoke.results.findLast(result => result.fileCount > 0));
        assert.equal(worker.fileCount, 12); assert.match(worker.diagnosticSha256, /^[a-f0-9]{64}$/);
        const evidence = { mode, scenario, variant, zip: filename, zipSha256: zip.zipSha256, fileCount: zip.entries.length,
            csv: zip.entries.map(file => ({ name: file.name, sha256: file.sha256, headers: file.csv.headers, rows: file.csv.dataRows })),
            worker, workerMetrics: [{ kind: mode, ...worker }],
            applicationVersion: await page.locator('meta[name="application-version"]').getAttribute('content') };
        if (mode === 'programme') evidence.dateChecks = assertProgrammeDates(zip, expected);
        else assertTenderDates(zip, single, singleName);
        report.exports.push(evidence); save(); return { zip, evidence };
    }
    try {
        const reference = {};
        if (args['before-url']) {
            for (const mode of ['programme', 'tender']) {
                const page = await open(args['before-url'], mode); await upload(page, singleName, single);
                if (mode === 'programme') await configureProgramme(page, singleName);
                else { await page.getByLabel(`Status date for Tender stage 1, ${singleName}`, { exact: true }).fill('2026-09-30'); await page.locator('body').click({ position: { x: 5, y: 5 } }); }
                reference[mode] = await exportBundle(page, mode, 'single-project', 'before',
                    { snapshotDate: '2026-01-30', month: '2026-01', projectDates: { P1: '2026-01-30' }, sourceBytes: single, sourceName: singleName });
                await page.close();
            }
        } else {
            const referenceReport = path.resolve(args['reference-report'] || path.join(repo, 'artifacts/parser-performance-20260930/browser-benchmark.json'));
            const frozen = JSON.parse(fs.readFileSync(referenceReport));
            for (const mode of ['programme', 'tender']) {
                const run = frozen.runs.find(run => run.mode === mode && run.variant === 'candidate' && !run.warmup);
                assert.ok(run?.zip, 'Frozen single-profile header evidence is required');
                reference[mode] = { zip: await readZip(run.zip, mode), headerOnlyEvidence: run.zip };
            }
        }
        const page = await open(url, 'programme');
        await upload(page, 'unknown-schedule.xer', single);
        const unknown = await fields(page, 'unknown-schedule.xer');
        assert.deepEqual(unknown, { type: 'month', month: '2026-01', kind: '', tag: '', date: '2026-01-30' });
        report.checks.push({ name: 'Unknown filename gets detected month; kind/tag remain explicit', fields: unknown }); await clear(page);
        await upload(page, singleName, single);
        const governed = await fields(page, singleName);
        assert.deepEqual(governed, { type: 'month', month: '2026-01', kind: 'Baseline', tag: 'BL01', date: '2026-01-30' });
        await screenshot(page, 'governed-month-prefill');
        report.checks.push({ name: 'Governed baseline preserves kind/tag and uses detected month', fields: governed });
        const golden = await exportBundle(page, 'programme', 'single-project', 'after',
            { snapshotDate: '2026-01-30', month: '2026-01', projectDates: { P1: '2026-01-30' }, sourceBytes: single, sourceName: singleName });
        assertHeaders(golden.zip, headerEvidence(reference.programme.zip));
        if (reference.programme.evidence) report.comparisons.push({ mode: 'programme', ...await comparePair(reference.programme.evidence, golden.evidence) });
        // Exercise the explicit month onchange handler, rather than only its initial prefill.
        await configureProgramme(page, singleName, '2026-02');
        assert.equal((await fields(page, singleName)).month, '2026-02');
        const editedMonth = await exportBundle(page, 'programme', 'single-project-edited-month', 'after',
            { snapshotDate: '2026-01-30', month: '2026-02', projectDates: { P1: '2026-01-30' }, sourceBytes: single, sourceName: singleName });
        assertHeaders(editedMonth.zip, headerEvidence(reference.programme.zip));
        report.checks.push({ name: 'Editing the month input changes exported monthupdate to its first day', monthUpdate: '2026-02-01', dataDate: '2026-01-30' });
        await clear(page);
        const conflictName = 'J5001-C-2602_20260228.xer'; await upload(page, conflictName, single);
        const conflict = await fields(page, conflictName);
        assert.deepEqual(conflict, { type: 'month', month: '2026-01', kind: 'Update', tag: '2602', date: '2026-01-30' });
        assert.match(await page.getByLabel('Metadata warnings for ' + conflictName, { exact: true }).innerText(), /Filename update tag.*2602.*differs.*2026-01/i);
        await screenshot(page, 'filename-tag-conflict');
        report.checks.push({ name: 'Conflicting governed update tag stays editable and warns', fields: conflict }); await clear(page);
        await page.close();

        // Metadata must survive changing export profile after registration.
        const multiPage = await open(url, 'standard'), multiName = 'J5001-C-BL01_20260225.xer';
        const multiMetadata = await upload(multiPage, multiName, multi);
        assert.equal(multiMetadata.dataDate, '2026-02-25'); assert.ok(multiMetadata.metadataWarnings.some(text => /different data dates/i.test(text)));
        await profile(multiPage, 'programme'); const multiFields = await fields(multiPage, multiName);
        assert.deepEqual(multiFields, { type: 'month', month: '2026-02', kind: 'Baseline', tag: 'BL01', date: '2026-02-25' });
        const notice = await multiPage.getByLabel('Metadata warnings for ' + multiName, { exact: true }).innerText();
        assert.match(notice, /different data dates/i); assert.match(notice, /2026-02-25/);
        await screenshot(multiPage, 'multi-project-standard-to-programme');
        report.checks.push({ name: 'Standard upload retains two-project metadata when switching to Programme', fields: multiFields, metadataWarnings: multiMetadata.metadataWarnings });
        const multiExport = await exportBundle(multiPage, 'programme', 'two-project-distinct-dates', 'after',
            { snapshotDate: '2026-02-25', month: '2026-02', projectDates: { P1: '2026-01-30', P2: '2026-02-25' }, multi: true, sourceBytes: multi, sourceName: multiName });
        assertHeaders(multiExport.zip, headerEvidence(reference.programme.zip));
        const log = await multiPage.getByRole('log').innerText(); assert.match(log, /REVIEW_MULTIPLE_PROJECTS|native PROJECT identities/i);
        await screenshot(multiPage, 'multi-project-complete-export');
        report.checks.push({ name: 'Programme retains both project dates and governed task date with source warning', fileCount: 12 });
        await clear(multiPage); const invalidName = 'J5001-C-BL01_20260225-invalid.xer';
        const invalidMetadata = await upload(multiPage, invalidName, invalid);
        assert.equal(invalidMetadata.dataDate ?? null, null); assert.ok(invalidMetadata.metadataWarnings.some(text => /missing or invalid/i.test(text)));
        const missing = await fields(multiPage, invalidName); assert.equal(missing.month, ''); assert.equal(missing.date, '');
        report.checks.push({ name: 'One invalid project date leaves month/data date blank for manual entry', fields: missing, metadataWarnings: invalidMetadata.metadataWarnings });
        await multiPage.close();

        const tenderPage = await open(url, 'tender'); await upload(tenderPage, singleName, single);
        await tenderPage.getByLabel(`Status date for Tender stage 1, ${singleName}`, { exact: true }).fill('2026-09-30');
        await tenderPage.locator('body').click({ position: { x: 5, y: 5 } });
        const tender = await exportBundle(tenderPage, 'tender', 'single-project', 'after');
        assertHeaders(tender.zip, headerEvidence(reference.tender.zip));
        if (reference.tender.evidence) report.comparisons.push({ mode: 'tender', ...await comparePair(reference.tender.evidence, tender.evidence) });
        await clear(tenderPage); await upload(tenderPage, multiName, multi);
        let unexpectedDownload = false; tenderPage.on('download', () => { unexpectedDownload = true; });
        await tenderPage.getByRole('button', { name: 'Validate and download Tender Review bundle', exact: true }).click();
        await tenderPage.waitForFunction(() => /exactly one PROJECT row/i.test(document.body.innerText)
            && !document.querySelector('input[type=file]').disabled);
        assert.equal(unexpectedDownload, false, 'Tender multi-PROJECT rejection must not hand off a partial download');
        await screenshot(tenderPage, 'tender-multi-project-rejection');
        report.checks.push({ name: 'Tender exactly-one-PROJECT rule still rejects two projects before download' });
        await tenderPage.close(); report.passed = report.errors.length === 0;
    } catch (error) { report.errors.push({ error: error.message }); }
    finally { await browser.close(); save(); }
    console.log(JSON.stringify({ passed: report.passed, checks: report.checks.length, exports: report.exports.length, exactComparisons: report.comparisons.length, output }));
    if (!report.passed) process.exitCode = 1;
}

module.exports = { xerSections, multiProjectFixture, csvRows, headerEvidence, assertHeaders, assertProgrammeDates, assertTenderDates, argumentsFrom };
if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; });
