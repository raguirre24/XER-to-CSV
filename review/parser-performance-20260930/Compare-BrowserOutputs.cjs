// Run only after all browser timing workloads have finished. No browser/build is started.
// node review/parser-performance-20260930/Compare-BrowserOutputs.cjs --report artifacts/parser-performance-20260930/browser-benchmark.json
// --report accepts comma-separated reports; --out defaults to browser-output-parity.json.
// ZIP dates are covered by the fixed-timestamp native suite. This compares extracted bytes.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const JSZip = require('./browser-dependencies.cjs').dependency('jszip');
const repo = path.resolve(__dirname, '../..');
const evidenceRoot = path.join(repo, 'artifacts/parser-performance-20260930');
const reviewTables = ['01_XER_TASK', '02_XER_PROJECT', '03_XER_PROJWBS', '06_XER_PREDECESSOR',
    '07_XER_ACTVTYPE', '08_XER_ACTVCODE', '09_XER_TASKACTV', '10_XER_CALENDAR',
    '11_XER_CALENDAR_DETAILED', '12_XER_RSRC', '15_XER_RESOURCE_DISTRIBUTION'];
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const utf8 = new TextDecoder('utf-8', { fatal: true });

// Offsets refer to original bytes, including quoted cells and UTF-8/BOM bytes.
// Only a manifest retains cell values; large data CSVs retain counts and headers.
function inspectCsv(bytes, keepRecords = false) {
    let offset = bytes.subarray(0, 3).equals(Buffer.from([239, 187, 191])) ? 3 : 0;
    let fieldStart = offset, quoted = false, closedQuote = false, columns = 0, count = 0, headerEnd = 0;
    let fields = [], spans = [], headers;
    const records = [];
    function field(end) {
        columns++;
        if (count === 0 || keepRecords) {
            let raw = bytes.subarray(fieldStart, end);
            if (raw[0] === 34) raw = raw.subarray(1, raw.length - 1);
            fields.push(utf8.decode(raw).replaceAll('""', '"'));
            spans.push({ start: fieldStart, end });
        }
        closedQuote = false;
    }
    function row(end) {
        if (count === 0) { headers = fields; headerEnd = end; }
        else assert.equal(columns, headers.length, `CSV column count changed at data row ${count}`);
        if (keepRecords) records.push({ fields, spans });
        count++;
        fields = []; spans = []; columns = 0;
    }
    for (; offset < bytes.length; offset++) {
        const byte = bytes[offset];
        if (byte === 34) {
            if (quoted && bytes[offset + 1] === 34) { offset++; continue; }
            if (quoted) { quoted = false; closedQuote = true; }
            else { assert.equal(offset, fieldStart, 'Unexpected CSV quote'); quoted = true; }
        } else if (!quoted && byte === 44) {
            field(offset); fieldStart = offset + 1;
        } else if (!quoted && (byte === 13 || byte === 10)) {
            field(offset);
            if (byte === 13 && bytes[offset + 1] === 10) offset++;
            row(offset + 1); fieldStart = offset + 1;
        } else if (closedQuote) assert.fail('Unexpected bytes after a quoted CSV cell');
    }
    assert.equal(quoted, false, 'Unclosed CSV quote');
    if (fieldStart < bytes.length || columns) { field(bytes.length); row(bytes.length); }
    assert.ok(headers?.length, 'CSV header is missing');
    assert.equal(new Set(headers).size, headers.length, 'CSV headers are duplicated');
    return { headers, headerSha256: sha256(bytes.subarray(0, headerEnd)), dataRows: count - 1, records };
}

function replaceSpans(bytes, replacements) {
    const ordered = replacements.toSorted((a, b) => a.start - b.start);
    const pieces = []; let offset = 0;
    for (const item of ordered) {
        assert.ok(item.start >= offset && item.end >= item.start && item.end <= bytes.length, 'Overlapping/out-of-range normalization');
        pieces.push(bytes.subarray(offset, item.start), Buffer.from(item.value)); offset = item.end;
    }
    pieces.push(bytes.subarray(offset));
    return Buffer.concat(pieces);
}

function manifestEvidence(bytes, mode, files) {
    const csv = inspectCsv(bytes, true), headers = csv.headers;
    const column = name => { const index = headers.indexOf(name); assert.ok(index >= 0, `Manifest column missing: ${name}`); return index; };
    const indexes = Object.fromEntries(['bundle_id', 'bundle_status', 'canonical_xer_filename', 'source_sha256',
        'table_name', 'row_count', 'csv_sha256', 'exported_at_utc'].map(name => [name, column(name)]));
    const rows = csv.records.slice(1);
    assert.ok(rows.length, 'Manifest has no data rows');
    const bundleId = rows[0].fields[indexes.bundle_id];
    const identity = /^(.*)_(\d{8}T\d{6}Z)_([a-f0-9]{8})$/.exec(bundleId);
    assert.ok(identity, 'Bundle ID does not match the documented timestamp-derived naming contract');
    if (mode === 'tender') assert.ok(identity[1].endsWith('_TENDER'), 'Tender bundle identity prefix is invalid');
    const normalizedBundleId = `${identity[1]}_EXPORT_TIME_${identity[3]}`;
    const replacements = [], groups = new Map(), totals = new Map();
    function cellContentSpan(span) {
        return bytes[span.start] === 34 ? { start: span.start + 1, end: span.end - 1 } : span;
    }
    for (const row of rows) {
        const value = name => row.fields[indexes[name]];
        assert.equal(value('bundle_id'), bundleId, 'Manifest bundle identity varies within one ZIP');
        assert.equal(value('bundle_status').toUpperCase(), 'COMPLETE', 'Manifest is not complete');
        assert.match(value('exported_at_utc'), /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)$/);
        const instant = new Date(value('exported_at_utc'));
        assert.ok(Number.isFinite(instant.getTime()), 'Manifest export timestamp is invalid');
        const timestampId = instant.toISOString().slice(0, 19).replaceAll('-', '').replaceAll(':', '') + 'Z';
        assert.equal(identity[2], timestampId, 'Bundle ID timestamp differs from manifest export timestamp');
        const timeSpan = cellContentSpan(row.spans[indexes.exported_at_utc]);
        replacements.push({ ...timeSpan, value: 'EXPORT_TIME' });
        const bundleSpan = cellContentSpan(row.spans[indexes.bundle_id]);
        const timeStart = bundleSpan.start + Buffer.byteLength(identity[1] + '_');
        assert.equal(bytes.subarray(timeStart, timeStart + identity[2].length).toString(), identity[2]);
        replacements.push({ start: timeStart, end: timeStart + identity[2].length, value: 'EXPORT_TIME' });
        const table = value('table_name');
        assert.ok(reviewTables.includes(table), 'Manifest declares an unexpected table');
        const file = files.get(table + '.csv');
        assert.ok(file, `Manifest-declared CSV missing: ${table}.csv`);
        assert.match(value('csv_sha256'), /^[a-f0-9]{64}$/);
        assert.equal(value('csv_sha256'), file.sha256, `Manifest CSV hash mismatch: ${table}`);
        assert.match(value('source_sha256'), /^[a-f0-9]{64}$/);
        assert.match(value('row_count'), /^\d+$/);
        const rowCount = Number(value('row_count'));
        assert.ok(Number.isSafeInteger(rowCount), 'Manifest row count is out of range');
        totals.set(table, (totals.get(table) || 0) + rowCount);
        const source = value('canonical_xer_filename');
        const group = groups.get(source) || { sha256: value('source_sha256'), tables: new Set() };
        assert.equal(group.sha256, value('source_sha256'), 'One source has inconsistent manifest hashes');
        assert.equal(group.tables.has(table), false, 'Manifest source/table declaration is duplicated');
        group.tables.add(table); groups.set(source, group);
    }
    for (const table of reviewTables) assert.equal(totals.get(table), files.get(table + '.csv').csv.dataRows, `Manifest row total mismatch: ${table}`);
    for (const group of groups.values()) assert.equal(group.tables.size, 11, 'Manifest source does not declare all 11 tables');
    assert.equal(rows.length, groups.size * 11, 'Manifest row envelope is incomplete');
    return { normalizedBytes: replaceSpans(bytes, replacements), bundleId, normalizedBundleId,
        sourceCount: groups.size, rowCount: rows.length, normalizedByteSpans: replacements.length };
}

async function readZip(filename, mode) {
    const raw = fs.readFileSync(filename), zip = await JSZip.loadAsync(raw, { checkCRC32: true });
    const entries = [], byName = new Map();
    for (const entry of Object.values(zip.files).filter(item => !item.dir)) {
        assert.ok(!entry.name.startsWith('/') && !entry.name.split('/').includes('..'), 'Unsafe ZIP entry path');
        assert.ok(entry.name.endsWith('.csv'), 'ZIP contains a non-CSV file');
        const name = path.posix.basename(entry.name);
        assert.equal(byName.has(name), false, 'ZIP contains duplicate CSV basenames');
        const bytes = await entry.async('nodebuffer');
        const file = { path: entry.name, name, bytes, sha256: sha256(bytes), csv: inspectCsv(bytes),
            zipTimestampUtc: entry.date.toISOString() };
        entries.push(file); byName.set(name, file);
    }
    for (const table of reviewTables) assert.ok(byName.has(table + '.csv'), `Required table missing: ${table}`);
    let manifest;
    if (mode !== 'legacy') {
        assert.equal(entries.length, 12, 'Review ZIP must contain 11 tables and one manifest');
        const file = byName.get('XER_CSV_MANIFEST.csv');
        assert.ok(file, 'Review manifest is missing');
        manifest = manifestEvidence(file.bytes, mode, byName);
        file.normalizedBytes = manifest.normalizedBytes;
        for (const item of entries) {
            const prefix = manifest.bundleId + '/';
            assert.ok(item.path.startsWith(prefix), 'Review ZIP path differs from its manifest bundle identity');
            item.normalizedPath = manifest.normalizedBundleId + '/' + item.path.slice(prefix.length);
        }
    } else {
        assert.equal(byName.has('XER_CSV_MANIFEST.csv'), false, 'Unexpected Legacy manifest');
        for (const name of ['04_XER_BASELINE.csv', '13_XER_TASKRSRC.csv', '14_XER_UMEASURE.csv'])
            assert.ok(byName.has(name), `Required Legacy table missing: ${name}`);
        assert.equal(entries.length, 14 + Number(byName.has('XER_DATA_QUALITY.csv')), 'Legacy ZIP file envelope is invalid');
    }
    for (const entry of entries) { entry.normalizedBytes ??= entry.bytes; entry.normalizedPath ??= entry.path; }
    return { entries, byName, manifest, zipSha256: sha256(raw), zipBytes: raw.length };
}

function workerSummary(run) {
    return run.workerMetrics?.findLast(item => ['export', 'programme', 'tender'].includes(item.kind))
        || run.worker?.results?.findLast(item => item.fileCount > 0);
}
function diagnosticEvidence(run, zip) {
    const worker = workerSummary(run), quality = zip.byName.get('XER_DATA_QUALITY.csv');
    if (worker?.fileCount !== undefined) assert.equal(worker.fileCount, zip.entries.length, 'Worker file count differs from ZIP');
    for (const [file, hash] of Object.entries(worker?.csvSha256ByFile || {})) {
        assert.ok(zip.byName.has(file), 'Worker CSV hash names a missing file');
        assert.equal(hash, zip.byName.get(file).sha256, `Worker CSV hash mismatch: ${file}`);
    }
    if (quality && worker?.diagnosticSha256) assert.equal(worker.diagnosticSha256, quality.sha256, 'Worker diagnostic hash differs from Legacy quality CSV');
    const warningCount = run.warningCount ?? worker?.warningCount ?? (run.mode === 'legacy' ? quality?.csv.dataRows || 0 : undefined);
    if (run.mode === 'legacy' && warningCount > 0) assert.ok(quality, 'Legacy diagnostic CSV is missing despite reported warnings');
    if (quality && warningCount !== undefined) assert.equal(warningCount, quality.csv.dataRows, 'Legacy warning count differs from quality rows');
    const messages = run.diagnosticMessages ?? run.warningMessages ?? run.ui?.diagnosticMessages;
    return { warningCount: warningCount ?? null, displayMessages: Array.isArray(messages) ? messages : null,
        fullDiagnosticSha256: worker?.diagnosticSha256 || quality?.sha256 || null,
        fullDiagnosticEvidence: worker?.diagnosticSha256 ? 'worker full diagnostic CSV digest' : quality ? 'Legacy ZIP quality CSV digest' : 'not captured in baseline browser; fixed native parity is authoritative' };
}

async function comparePair(baseline, candidate) {
    const mode = baseline.mode, first = await readZip(baseline.zip, mode), second = await readZip(candidate.zip, mode);
    const left = new Map(first.entries.map(entry => [entry.normalizedPath, entry]));
    const right = new Map(second.entries.map(entry => [entry.normalizedPath, entry]));
    assert.deepEqual([...left.keys()].sort(), [...right.keys()].sort(), 'Extracted ZIP file sets differ');
    const files = [];
    for (const name of [...left.keys()].sort()) {
        const a = left.get(name), b = right.get(name);
        assert.deepEqual(a.csv.headers, b.csv.headers, `CSV headers differ: ${a.name}`);
        assert.equal(a.csv.headerSha256, b.csv.headerSha256, `CSV header bytes differ: ${a.name}`);
        assert.equal(a.csv.dataRows, b.csv.dataRows, `CSV row counts differ: ${a.name}`);
        if (!a.normalizedBytes.equals(b.normalizedBytes)) {
            let offset = 0;
            while (offset < Math.min(a.normalizedBytes.length, b.normalizedBytes.length) && a.normalizedBytes[offset] === b.normalizedBytes[offset]) offset++;
            assert.fail(`CSV bytes/row order differ in ${a.name} at normalized byte ${offset}`);
        }
        files.push({ name, headers: a.csv.headers, dataRows: a.csv.dataRows,
            baselineBytes: a.bytes.length, candidateBytes: b.bytes.length,
            baselineSha256: a.sha256, candidateSha256: b.sha256, normalizedSha256: sha256(a.normalizedBytes),
            normalizedOnlyManifestExportTime: a.name === 'XER_CSV_MANIFEST.csv',
            baselineZipTimestampUtc: a.zipTimestampUtc, candidateZipTimestampUtc: b.zipTimestampUtc });
    }
    const diagnostics = { baseline: diagnosticEvidence(baseline, first), candidate: diagnosticEvidence(candidate, second) };
    for (const field of ['warningCount', 'displayMessages', 'fullDiagnosticSha256'])
        if (diagnostics.baseline[field] !== null && diagnostics.candidate[field] !== null)
            assert.deepEqual(diagnostics.baseline[field], diagnostics.candidate[field], `Diagnostic ${field} differs`);
    return { passed: true, fileSetsEqual: true, extractedCsvBytesHeadersAndRowOrderEqual: true,
        zipEntryOrderEqual: first.entries.map(item => item.normalizedPath).join('\n') === second.entries.map(item => item.normalizedPath).join('\n'),
        baselineZipSha256: first.zipSha256, candidateZipSha256: second.zipSha256,
        baselineZipBytes: first.zipBytes, candidateZipBytes: second.zipBytes,
        manifestsVerified: mode !== 'legacy', manifestSourceCount: first.manifest?.sourceCount,
        manifestRows: first.manifest?.rowCount, normalizedManifestByteSpans: first.manifest?.normalizedByteSpans,
        diagnostics, files };
}

function runIdentity(run, report) {
    return { browser: run.browser || report.channel, dataset: run.dataset || 'primary', mode: run.mode,
        scenario: run.scenario || 'fresh', iteration: run.iteration ?? run.pair,
        sourceNames: run.sourceNames || run.files?.filter(item => typeof item === 'string') || [] };
}

async function main() {
    const args = {};
    for (let index = 2; index < process.argv.length; index += 2) args[process.argv[index].replace(/^--/, '')] = process.argv[index + 1];
    const reports = (args.report || path.join(evidenceRoot, 'browser-benchmark.json')).split(',').map(name => path.resolve(name));
    const output = path.resolve(args.out || path.join(evidenceRoot, 'browser-output-parity.json'));
    const result = { createdAt: new Date().toISOString(), reports, passed: false, comparisons: [], errors: [],
        policy: 'All extracted CSV bytes, headers, row order and file sets match exactly except original manifest exported_at_utc cell contents and the validated matching bundle ID timestamp bytes/folder prefix. No other cell, GUID, identity hash or filename normalization is allowed. ZIP dates are recorded; fixed native timestamp/diagnostic parity remains authoritative.' };
    const groups = new Map();
    try {
        for (const filename of reports) {
            const report = JSON.parse(fs.readFileSync(filename, 'utf8'));
            for (const run of report.runs) {
                if (run.warmup) continue;
                assert.ok(['baseline', 'candidate'].includes(run.variant), 'Unexpected benchmark variant');
                const identity = runIdentity(run, report), key = JSON.stringify(identity);
                const group = groups.get(key) || { identity };
                assert.equal(group[run.variant], undefined, `Duplicate measured benchmark identity: ${key}`);
                assert.ok(fs.existsSync(run.zip), 'Benchmark ZIP is missing');
                assert.deepEqual(run.errors || [], [], 'Benchmark run reported browser errors');
                group[run.variant] = run; groups.set(key, group);
            }
        }
        if (args['require-complete'] !== 'false') {
            for (const browser of (args.browsers || 'chrome,msedge').split(',')) for (const mode of ['legacy', 'programme', 'tender']) {
                const selected = [...groups.values()].filter(group => group.identity.browser === browser && group.identity.mode === mode && group.identity.scenario === 'fresh');
                for (let iteration = 0; iteration < Number(args.pairs || 5); iteration++)
                    assert.ok(selected.some(group => group.identity.iteration === iteration), `Missing measured pair ${browser}/${mode}/${iteration}`);
                if (args['require-history'] !== 'false') assert.ok(selected.some(group => group.identity.iteration === 'second-history'), `Missing second/history pair ${browser}/${mode}`);
            }
        }
        const fullDiagnostics = new Map();
        for (const group of groups.values()) {
            const { identity, baseline, candidate } = group;
            if (!baseline || !candidate) { result.errors.push({ ...identity, error: 'Matched baseline/candidate run is missing' }); continue; }
            try {
                const comparison = await comparePair(baseline, candidate);
                const stableKey = JSON.stringify({ ...identity, browser: undefined, iteration: identity.iteration === 'second-history' ? 'second-history' : undefined });
                const full = comparison.diagnostics.candidate.fullDiagnosticSha256;
                if (full) {
                    if (fullDiagnostics.has(stableKey)) assert.equal(full, fullDiagnostics.get(stableKey), 'Candidate full diagnostic digest differs across matched same-profile runs/browsers');
                    else fullDiagnostics.set(stableKey, full);
                }
                result.comparisons.push({ ...identity, baselineZip: baseline.zip, candidateZip: candidate.zip, ...comparison });
            } catch (error) { result.errors.push({ ...identity, error: error.message }); }
        }
        result.passed = result.comparisons.length > 0 && result.errors.length === 0;
        result.comparedPairs = result.comparisons.length;
        result.sameProfileDiagnosticGroups = fullDiagnostics.size;
    } catch (error) { result.errors.push({ error: error.message }); }
    fs.mkdirSync(path.dirname(output), { recursive: true }); fs.writeFileSync(output, JSON.stringify(result, null, 2));
    console.log(JSON.stringify({ passed: result.passed, comparedPairs: result.comparisons.length, errors: result.errors.length, output }));
    if (!result.passed) process.exitCode = 1;
}

module.exports = { inspectCsv, replaceSpans, manifestEvidence, readZip, comparePair, runIdentity };
if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; });
