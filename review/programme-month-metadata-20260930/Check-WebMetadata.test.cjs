// Pure, tiny harness checks; these do not launch browsers or load performance ZIPs.
const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const { inspectCsv } = require('../parser-performance-20260930/Compare-BrowserOutputs.cjs');
const { xerSections, multiProjectFixture, assertHeaders, assertProgrammeDates, argumentsFrom } = require('./Check-WebMetadata.cjs');
const fixture = fs.readFileSync(path.resolve(__dirname, '../fixtures/J5001_C_BL01_2026-01-31.xer'));

function objects(section) { return section.rows.map(row => Object.fromEntries(section.headers.map((name, index) => [name, row[index]]))); }
test('synthetic second project has its own date and valid WBS/task membership without changing original rows', () => {
    const original = xerSections(fixture), multi = xerSections(multiProjectFixture(fixture));
    for (const table of ['PROJECT', 'PROJWBS', 'TASK', 'SCHEDOPTIONS'])
        assert.deepEqual(multi.get(table).rows.slice(0, original.get(table).rows.length), original.get(table).rows);
    assert.deepEqual(objects(multi.get('PROJECT')).map(row => [row.proj_id, row.last_recalc_date]), [['P1', '2026-01-30 00:00'], ['P2', '2026-02-25 00:00']]);
    const addedTask = objects(multi.get('TASK')).find(row => row.task_id === 'T3');
    assert.equal(addedTask.proj_id, 'P2'); assert.equal(addedTask.wbs_id, 'W2'); assert.equal(addedTask.task_code, 'B100');
    assert.equal(objects(multi.get('PROJWBS')).find(row => row.wbs_id === 'W2').proj_id, 'P2');
    assert.deepEqual(objects(xerSections(multiProjectFixture(fixture, '2026-02-25', true)).get('PROJECT')).map(row => row.proj_id), ['P2', 'P1']);
});
test('missing-date variant leaves exactly the second PROJECT date blank', () => {
    const projects = objects(xerSections(multiProjectFixture(fixture, '')).get('PROJECT'));
    assert.equal(projects[0].last_recalc_date, '2026-01-30 00:00'); assert.equal(projects[1].last_recalc_date, '');
});
test('exact schema check detects reordered columns and changed file envelope', () => {
    const zip = { entries: [{ name: '01.csv', csv: { headers: ['a', 'b'] } }] };
    assertHeaders(zip, { '01.csv': ['a', 'b'] });
    assert.throws(() => assertHeaders(zip, { '01.csv': ['b', 'a'] }), /Exact header contract/);
    assert.throws(() => assertHeaders(zip, { '02.csv': ['a', 'b'] }), /envelope changed/);
});

function sampleBundle(projectDate = '2026-01-30', taskDate = '2026-02-25') {
    const entry = text => { const bytes = Buffer.from(text); return { bytes, csv: inspectCsv(bytes) }; };
    const hash = createHash('sha256').update(fixture).digest('hex');
    return { byName: new Map([
        ['02_XER_PROJECT.csv', entry(`proj_id_key,last_recalc_date\r\nCSV::J5001::C::BL01::P1,${projectDate}\r\nCSV::J5001::C::BL01::P2,2026-02-25\r\n`)],
        ['01_XER_TASK.csv', entry(`task_code,proj_id_key,data_date,monthupdate\r\nA100,CSV::J5001::C::BL01::P1,${taskDate},2026-02-01\r\nB100,CSV::J5001::C::BL01::P2,${taskDate},2026-02-01\r\n`)],
        ['XER_CSV_MANIFEST.csv', entry('schema_version,data_date,monthupdate,snapshot_kind,snapshot_tag,source_sha256,original_xer_filename\r\n'
            + Array.from({ length: 11 }, () => `5.0,2026-02-25,2026-02-01,baseline,BL01,${hash},source.xer`).join('\r\n') + '\r\n')]
    ]) };
}
const expected = { snapshotDate: '2026-02-25', month: '2026-02', projectDates: { P1: '2026-01-30', P2: '2026-02-25' }, multi: true, sourceBytes: fixture, sourceName: 'source.xer' };
test('date checks require own project dates, governed snapshot date, and project membership', () => {
    assert.equal(assertProgrammeDates(sampleBundle(), expected).taskRows, 2);
    assert.throws(() => assertProgrammeDates(sampleBundle('2026-02-25'), expected), /Own PROJECT date changed/);
    assert.throws(() => assertProgrammeDates(sampleBundle('2026-01-30', '2026-01-30'), expected), /governed file-level snapshot date/);
});
test('CLI arguments remain explicit value pairs', () => {
    assert.deepEqual(argumentsFrom(['--channel', 'msedge', '--url', 'http://localhost/']), { channel: 'msedge', url: 'http://localhost/' });
    assert.throws(() => argumentsFrom(['--url']), /Missing value/);
});
