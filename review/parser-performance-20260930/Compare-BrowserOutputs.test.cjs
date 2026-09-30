// Tiny in-memory verifier tests; these do not load browser benchmark outputs.
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { createHash } = require('node:crypto');
const { inspectCsv, manifestEvidence } = require('./Compare-BrowserOutputs.cjs');
const tables = ['01_XER_TASK', '02_XER_PROJECT', '03_XER_PROJWBS', '06_XER_PREDECESSOR',
    '07_XER_ACTVTYPE', '08_XER_ACTVCODE', '09_XER_TASKACTV', '10_XER_CALENDAR',
    '11_XER_CALENDAR_DETAILED', '12_XER_RSRC', '15_XER_RESOURCE_DISTRIBUTION'];
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const csvBytes = Buffer.from('key,value\r\n');
const files = new Map(tables.map(name => [name + '.csv', { sha256: sha256(csvBytes), csv: inspectCsv(csvBytes) }]));
function manifest(time = '2026-09-30T02:00:00Z', extra = 'same arbitrary GUID 12fd56e4-6c76-4731-a104-0bdef42a09a5') {
    const timestamp = time.replaceAll('-', '').replaceAll(':', '');
    const headers = ['bundle_id', 'bundle_status', 'canonical_xer_filename', 'source_sha256', 'table_name',
        'row_count', 'csv_sha256', 'exported_at_utc', 'extra'];
    const rows = tables.map(name => [`VERIFY_TENDER_${timestamp}_12345678`, 'COMPLETE', 'source.xer', 'a'.repeat(64), name,
        '0', sha256(csvBytes), time, extra]);
    return Buffer.from([headers.join(','), ...rows.map(row => row.join(','))].join('\r\n') + '\r\n');
}

test('CSV byte parser handles BOM, quoted delimiters/newlines, escaped quotes, UTF-8 and trailing empty cells', () => {
    const bytes = Buffer.from('\uFEFFone,two,three\r\n"a,\r\n""quoted""",é,\r\nlast,row,\r\n');
    const csv = inspectCsv(bytes, true);
    assert.deepEqual(csv.headers, ['one', 'two', 'three']);
    assert.equal(csv.dataRows, 2);
    assert.deepEqual(csv.records[1].fields, ['a,\r\n"quoted"', 'é', '']);
    assert.deepEqual(csv.records[2].fields, ['last', 'row', '']);
    assert.equal(bytes.subarray(csv.records[1].spans[1].start, csv.records[1].spans[1].end).toString(), 'é');
    assert.throws(() => inspectCsv(Buffer.from('one,two\r\n"unclosed,x\r\n')), /Unclosed/);
    assert.throws(() => inspectCsv(Buffer.from('one,two\r\none\r\n')), /column count/);
});

test('only validated manifest export-time spans are normalized, preserving every other byte and arbitrary GUID', () => {
    const first = manifestEvidence(manifest(), 'tender', files);
    const second = manifestEvidence(manifest('2026-09-30T02:12:34Z'), 'tender', files);
    assert.equal(first.sourceCount, 1);
    assert.equal(first.rowCount, 11);
    assert.equal(first.normalizedByteSpans, 22);
    assert.equal(first.normalizedBytes.equals(second.normalizedBytes), true);
    assert.equal(first.normalizedBundleId, second.normalizedBundleId);
    const changed = manifestEvidence(manifest('2026-09-30T02:12:34Z', 'different arbitrary GUID 4b88f913-728e-4416-b9e8-f444c079abce'), 'tender', files);
    assert.equal(first.normalizedBytes.equals(changed.normalizedBytes), false);
    const hashChanged = manifestEvidence(Buffer.from(manifest().toString().replaceAll('_12345678', '_87654321')), 'tender', files);
    assert.equal(first.normalizedBytes.equals(hashChanged.normalizedBytes), false);
});

test('manifest hashes, row totals, all 11 tables, and bundle timestamp linkage are mandatory', () => {
    assert.throws(() => manifestEvidence(Buffer.from(manifest().toString().replaceAll(sha256(csvBytes), '0'.repeat(64))), 'tender', files), /hash mismatch/);
    assert.throws(() => manifestEvidence(Buffer.from(manifest().toString().replaceAll(',0,', ',1,')), 'tender', files), /row total/);
    const missing = new Map(files); missing.delete(tables[0] + '.csv');
    assert.throws(() => manifestEvidence(manifest(), 'tender', missing), /missing/);
    const mismatch = Buffer.from(manifest().toString().replaceAll('20260930T020000Z', '20260930T020001Z'));
    assert.throws(() => manifestEvidence(mismatch, 'tender', files), /timestamp differs/);
});
