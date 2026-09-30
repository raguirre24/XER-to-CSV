// Runs the shipped module with isolated browser surfaces and a deterministic clock.
// node --test XerToCsvConverter.Web/tests/workerEngine.test.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const engineSource = fs.readFileSync(path.join(__dirname, '../wwwroot/js/workerEngine.js'), 'utf8');
const downloadSource = fs.readFileSync(path.join(__dirname, '../wwwroot/js/downloads.js'), 'utf8');
const NativeReadableStream = globalThis.ReadableStream;
let moduleIdentity = 0;
const cleanups = new WeakMap();

async function flush() {
    for (let index = 0; index < 12; index++) await Promise.resolve();
}

function clock() {
    const pending = new Map();
    let next = 0, now = 0;
    return {
        setTimeout(callback, delay) {
            const id = ++next;
            pending.set(id, { callback, at: now + delay });
            return id;
        },
        clearTimeout(id) { pending.delete(id); },
        now() { return now; },
        advance(milliseconds) {
            const end = now + milliseconds;
            for (;;) {
                const due = [...pending.entries()].filter(([, timer]) => timer.at <= end)
                    .sort((left, right) => left[1].at - right[1].at)[0];
                if (!due) break;
                now = due[1].at;
                pending.delete(due[0]);
                due[1].callback();
            }
            now = end;
        },
        delays() { return [...pending.values()].map(timer => timer.at - now); }
    };
}

async function browser(t, options = {}) {
    const saved = new Map(['document', 'Worker', 'xerDownloads', 'xerProcessingMetrics', 'ReadableStream', 'setTimeout', 'clearTimeout', 'performance']
        .map(name => [name, Object.getOwnPropertyDescriptor(globalThis, name)]));
    const timers = clock();
    const workers = [], blobs = [], clicks = [], revoked = [], streams = [], discards = [];
    const input = { files: [] };
    const metadata = { content: '2026.09.30 test+version' };
    const document = {
        baseURI: 'https://example.test/parser/',
        querySelector(selector) { assert.equal(selector, 'meta[name="application-version"]'); return metadata; },
        getElementById(id) { assert.equal(id, 'xer-file-input'); return input; },
        body: { appendChild() {} },
        createElement() { return {
            click() { clicks.push({ url: this.href, filename: this.download }); },
            remove() {}
        }; }
    };
    class Worker {
        constructor(url, configuration) {
            if (options.constructorError) throw new Error(options.constructorError);
            this.url = url;
            this.configuration = configuration;
            this.messages = [];
            this.terminated = false;
            workers.push(this);
        }
        postMessage(message) { this.messages.push(message); }
        terminate() { this.terminated = true; }
        emit(data) { return Promise.resolve(this.onmessage?.({ data })); }
        ready(version = metadata.content, protocol = 1) { return this.emit({ kind: 'ready', version, protocol }); }
        fail(message) { this.onerror?.({ message }); }
    }
    class ReadableStream extends NativeReadableStream {
        constructor(source, strategy) { super(source, strategy); streams.push(this); }
    }
    const downloadContext = {
        window: {}, document, Blob,
        URL: {
            createObjectURL(blob) { blobs.push(blob); return `blob:test-${blobs.length}`; },
            revokeObjectURL(url) { revoked.push(url); }
        },
        setTimeout: timers.setTimeout
    };
    vm.runInNewContext(downloadSource, downloadContext);
    const downloads = downloadContext.window.xerDownloads;
    const discard = downloads.discard;
    downloads.discard = id => { discards.push(id); discard(id); };
    const metrics = [];
    Object.assign(globalThis, { document, Worker, xerDownloads: downloads, xerProcessingMetrics: metrics, ReadableStream,
        performance: { now: timers.now },
        setTimeout: timers.setTimeout, clearTimeout: timers.clearTimeout });
    const api = await import(`data:text/javascript;base64,${Buffer.from(engineSource + `\n//# sourceURL=workerEngine-test-${++moduleIdentity}.js`).toString('base64')}`);
    if (!cleanups.has(t)) {
        cleanups.set(t, []);
        t.after(() => { for (const cleanup of cleanups.get(t).reverse()) cleanup(); });
    }
    cleanups.get(t).push(() => {
        api.dispose();
        for (const [name, descriptor] of saved) {
            if (descriptor) Object.defineProperty(globalThis, name, descriptor);
            else delete globalThis[name];
        }
    });
    function select(token = 'source-a', name = 'update.xer', bytes = [0, 1, 255]) {
        const data = Uint8Array.from(bytes);
        let reads = 0;
        const file = Object.freeze({ name, size: data.length,
            async arrayBuffer() { reads++; return data.slice().buffer; } });
        input.files = [file];
        api.registerSelection(token, 0, name, data.length);
        return { file, source: { sourceToken: token, name, size: data.length }, reads: () => reads };
    }
    function job(id, source, generation = 1, kind = 'metadata') {
        return { protocol: 1, jobId: id, generation, kind, sources: source ? [source] : [], tables: [] };
    }
    function run(dto, callback) {
        let pending;
        try { pending = Promise.resolve(api.run(dto, callback)); }
        catch (error) { pending = Promise.reject(error); }
        // Tests deliberately inject errors before awaiting the pending job.
        pending.catch(() => {});
        return pending;
    }
    async function start(dto, callback) {
        const pending = run(dto, callback);
        const current = workers.at(-1);
        await current.ready();
        await flush();
        assert.equal(current.messages.at(-1)?.kind, 'run');
        return { pending, worker: current };
    }
    async function result(worker, dto, extra = {}) {
        await worker.emit({ kind: 'result', jobId: dto.jobId,
            result: { jobId: dto.jobId, generation: dto.generation, ...extra } });
    }
    return { api, workers, input, metadata, timers, downloads, blobs, clicks, revoked, streams,
        metrics, discards, select, job, run, start, result };
}

test('startup uses the page version and forwards camelCase descriptors with immutable File handles', async t => {
    const b = await browser(t);
    const selected = b.select();
    const dto = b.job('metadata-one', selected.source, 7);
    const { pending, worker } = await b.start(dto);
    assert.equal(worker.url.href, 'https://example.test/parser/worker/main.js?v=2026.09.30%20test%2Bversion');
    assert.deepEqual(worker.configuration, { type: 'module', name: 'XER processing' });
    assert.deepEqual(worker.messages, [{ kind: 'run', job: dto, files: [{ token: selected.source.sourceToken, file: selected.file }] }]);
    assert.equal(worker.messages[0].files[0].file, selected.file);
    assert.equal(selected.reads(), 0);
    await b.result(worker, dto, { dataDate: '2026-08-31' });
    assert.equal((await pending).dataDate, '2026-08-31');
    assert.equal(b.blobs.length, 0);
    assert.equal(b.timers.delays().length, 0);
});

test('version and protocol mismatch stop startup and retain a fallback-only error prefix', async t => {
    for (const [version, protocol] of [['old-version', 1], ['2026.09.30 test+version', 2]]) {
        const b = await browser(t);
        const selected = b.select();
        const pending = b.run(b.job('mismatch', selected.source));
        await b.workers[0].ready(version, protocol);
        await assert.rejects(pending, /^Error: XER_WORKER_STARTUP: .*versions differ/);
        assert.equal(b.workers[0].terminated, true);
        assert.equal(b.workers[0].messages.length, 0);
        await assert.rejects(b.run(b.job('retry', selected.source)), /XER_WORKER_STARTUP:/);
        assert.equal(b.workers.length, 1);
    }
});

test('constructor failure, startup error, and timeout are distinct from a processing failure', async t => {
    const constructor = await browser(t, { constructorError: 'Workers unavailable' });
    const selected = constructor.select();
    await assert.rejects(constructor.run(constructor.job('constructor', selected.source)), /XER_WORKER_STARTUP: Workers unavailable/);
    assert.equal(constructor.workers.length, 0);
    const b = await browser(t);
    const source = b.select().source;
    const startup = b.run(b.job('startup', source));
    await b.workers[0].emit({ kind: 'startup-error', message: 'Runtime download failed' });
    await assert.rejects(startup, /XER_WORKER_STARTUP: Runtime download failed/);
    assert.equal(b.workers[0].terminated, true);
    b.api.clear();
    b.select();
    const timeout = b.run(b.job('timeout', source));
    b.timers.advance(30000);
    await assert.rejects(timeout, /XER_WORKER_STARTUP: Worker initialisation timed out/);
    b.api.clear();
    b.select();
    const dto = b.job('processing', source);
    const { pending, worker } = await b.start(dto);
    await worker.emit({ kind: 'error', jobId: dto.jobId, message: 'Invalid XER schema' });
    await assert.rejects(pending, error => error.message === 'Invalid XER schema');
    const retry = b.run(b.job('retry', source));
    await flush();
    await b.result(worker, b.job('retry', source));
    await retry;
    assert.equal(b.workers.length, 3);
});

test('fatal worker events use the startup fallback prefix only before initialisation', async t => {
    const b = await browser(t);
    const selected = b.select();
    const starting = b.run(b.job('starting', selected.source));
    b.workers[0].fail('Worker import failed');
    await assert.rejects(starting, /XER_WORKER_STARTUP: Worker import failed/);
    assert.equal(b.workers[0].terminated, true);
    b.api.clear();
    const registered = b.select();
    const running = await b.start(b.job('running', registered.source));
    running.worker.fail('Worker memory exhausted');
    await assert.rejects(running.pending, error => error.message === 'Worker memory exhausted');
    assert.equal(running.worker.terminated, true);
    const dto = b.job('after-fatal', registered.source, 2);
    const retry = await b.start(dto);
    assert.equal(retry.worker.messages[0].files[0].file, registered.file);
    await b.result(retry.worker, dto);
    await retry.pending;
});

test('selection identity validates the native handle while allowing two occurrences of the same file', async t => {
    const b = await browser(t);
    const selected = b.select('first');
    assert.throws(() => b.api.registerSelection('first', 0, selected.file.name, selected.file.size), /already registered/);
    assert.throws(() => b.api.registerSelection('second', 0, 'wrong.xer', selected.file.size), /no longer available/);
    assert.throws(() => b.api.registerSelection('second', 0, selected.file.name, selected.file.size + 1), /no longer available/);
    assert.throws(() => b.api.registerSelection('second', 1, selected.file.name, selected.file.size), /no longer available/);
    b.api.registerSelection('second', 0, selected.file.name, selected.file.size);
    const dto = b.job('two-occurrences', selected.source);
    dto.sources.push({ ...selected.source, sourceToken: 'second' });
    const { pending, worker } = await b.start(dto);
    assert.deepEqual(worker.messages[0].files.map(item => item.token), ['first', 'second']);
    assert.equal(worker.messages[0].files[0].file, selected.file);
    assert.equal(worker.messages[0].files[1].file, selected.file);
    b.api.remove('first');
    await assert.rejects(b.api.readBytes('first'), /no longer registered/);
    assert.deepEqual([...await b.api.readBytes('second')], [0, 1, 255]);
    await b.result(worker, dto);
    await pending;
});

test('pending startup can be cancelled within 250 ms and a late ready cannot start the cancelled job', async t => {
    const b = await browser(t);
    const source = b.select().source;
    const pending = b.run(b.job('pending-startup', source));
    const worker = b.workers[0];
    b.api.cancel('pending-startup');
    b.timers.advance(250);
    await assert.rejects(pending, /XER_CANCELLED/);
    assert.equal(worker.terminated, true);
    await worker.ready();
    assert.equal(worker.messages.filter(message => message.kind === 'run').length, 0);
    const dto = b.job('after-startup-cancel', source, 2);
    const next = await b.start(dto);
    await b.result(next.worker, dto);
    await next.pending;
    assert.equal(b.workers.length, 2);
});

test('single active job is enforced while starting and while processing', async t => {
    const b = await browser(t);
    const source = b.select().source;
    const dto = b.job('one', source);
    const first = b.run(dto);
    const second = b.run(b.job('two', source));
    await b.workers[0].ready();
    await assert.rejects(second, /Another browser processing operation is active/);
    await flush();
    await assert.rejects(b.run(b.job('three', source)), /Another browser processing operation is active/);
    assert.equal(b.workers[0].messages.filter(message => message.kind === 'run').length, 1);
    await b.result(b.workers[0], dto);
    await first;
});

test('ordered ArrayBuffer chunks are acknowledged and prepare exact bytes without an automatic download', async t => {
    const b = await browser(t);
    const dto = b.job('zip', b.select().source, 9, 'export');
    const { pending, worker } = await b.start(dto);
    for (const [sequence, bytes] of [[0, [0, 1, 2]], [1, [3, 254, 255]]])
        await worker.emit({ kind: 'chunk', jobId: dto.jobId, sequence, bytes: Uint8Array.from(bytes).buffer });
    assert.deepEqual(worker.messages.filter(message => message.kind === 'ack'), [
        { kind: 'ack', jobId: 'zip', sequence: 0 }, { kind: 'ack', jobId: 'zip', sequence: 1 }
    ]);
    await b.result(worker, dto, { downloadId: dto.jobId, filename: 'result.zip' });
    assert.equal((await pending).downloadId, 'zip');
    assert.equal(b.blobs.length, 1);
    assert.deepEqual([...new Uint8Array(await b.blobs[0].arrayBuffer())], [0, 1, 2, 3, 254, 255]);
    assert.equal(b.blobs[0].type, 'application/zip');
    assert.equal(b.streams[0].locked, false);
    assert.equal(b.clicks.length, 0);
    b.api.commit('zip');
    assert.deepEqual(b.clicks, [{ url: 'blob:test-1', filename: 'result.zip' }]);
    assert.throws(() => b.api.commit('zip'), /no longer exists/);
    b.timers.advance(1000);
    assert.deepEqual(b.revoked, ['blob:test-1']);
});

test('out-of-order chunks and non-ArrayBuffer payloads reject transfer and prevent a late Blob', async t => {
    for (const chunk of [{ sequence: 1, bytes: new ArrayBuffer(1) }, { sequence: 0, bytes: new Uint8Array([1]) }]) {
        const b = await browser(t);
        const dto = b.job('bad-chunk', b.select().source, 1, 'export');
        const { pending, worker } = await b.start(dto);
        await worker.emit({ kind: 'chunk', jobId: dto.jobId, ...chunk });
        await assert.rejects(pending, /ZIP transfer order is invalid/);
        assert.equal(worker.terminated, true);
        assert.equal(worker.messages.some(message => message.kind === 'ack'), false);
        await b.result(worker, dto, { downloadId: dto.jobId, filename: 'invalid.zip' });
        assert.equal(b.blobs.length, 0);
        assert.throws(() => b.api.commit(dto.jobId), /no longer exists/);
    }
});

test('serialization, preparation and browser handoff are separate scalar metrics without diagnostic contents', async t => {
    const b = await browser(t);
    const dto = b.job('timed-zip', b.select().source, 4, 'export');
    const { pending, worker } = await b.start(dto);
    await worker.emit({ kind: 'result-metrics', jobId: 'old-job', resultSerializationMs: 999, resultJsonBytes: 999 });
    await worker.emit({ kind: 'result-metrics', jobId: dto.jobId, resultSerializationMs: -1, resultJsonBytes: 123 });
    await worker.emit({ kind: 'result-metrics', jobId: dto.jobId, resultSerializationMs: 4.5, resultJsonBytes: 3500,
        browserFileReadMs: 10, webCryptoSha256Ms: 6 });
    await worker.emit({ kind: 'chunk', jobId: dto.jobId, sequence: 0, bytes: Uint8Array.from([7, 8]).buffer });
    const prepare = b.downloads.prepare;
    b.downloads.prepare = async (...args) => { b.timers.advance(11); await prepare(...args); };
    const diagnostics = [{ rawFields: [{ rawValue: 'private schedule evidence' }] }];
    await b.result(worker, dto, { downloadId: dto.jobId, filename: 'timed.zip',
        stageMilliseconds: { zipTransfer: 8, FileReading: 18, SourceSha256: 1 }, diagnostics });
    const result = await pending;
    assert.deepEqual(result.stageMilliseconds, { zipTransfer: 8, FileReading: 12, SourceSha256: 7,
        resultSerialization: 4.5, browserFileRead: 10, webCryptoSha256: 6, downloadPreparation: 11 });
    assert.equal(result.diagnostics, diagnostics);
    assert.equal(b.metrics.length, 1);
    const metric = b.metrics[0];
    assert.equal(metric.resultJsonBytes, 3500);
    assert.equal(metric.elapsedMs, 11);
    assert.equal(Object.hasOwn(metric, 'diagnostics'), false);
    assert.equal(JSON.stringify(metric).includes('private schedule evidence'), false);
    assert.equal(Object.hasOwn(metric.stageMilliseconds, 'browserHandoff'), false);
    const commit = b.downloads.commit;
    b.downloads.commit = id => { b.timers.advance(3); commit(id); };
    b.api.commit(dto.jobId);
    assert.equal(metric.stageMilliseconds.browserHandoff, 3);
    assert.equal(metric.browserHandoffSucceeded, true);
    assert.equal(b.clicks.length, 1);
    assert.throws(() => b.api.commit(dto.jobId), /no longer exists/);
    assert.equal(metric.stageMilliseconds.browserHandoff, 3);
    assert.equal(metric.browserHandoffSucceeded, true);
});

test('a failed browser handoff records elapsed attempt time and preserves the thrown error', async t => {
    const b = await browser(t);
    const dto = b.job('failed-handoff', b.select().source, 2, 'export');
    const { pending, worker } = await b.start(dto);
    await b.result(worker, dto, { downloadId: dto.jobId, filename: 'timed.zip' });
    await pending;
    b.downloads.commit = () => { b.timers.advance(5); throw new Error('Browser click failed'); };
    assert.throws(() => b.api.commit(dto.jobId), /Browser click failed/);
    assert.equal(b.metrics[0].stageMilliseconds.browserHandoff, 5);
    assert.equal(b.metrics[0].browserHandoffSucceeded, false);
    assert.equal(b.clicks.length, 0);
});

test('cancellation during processing and chunk transfer prevents late progress, Blob and commit', async t => {
    const b = await browser(t);
    const events = [];
    const dto = b.job('cancel-transfer', b.select().source, 3, 'export');
    const { pending, worker } = await b.start(dto, { invokeMethodAsync(method, progress) { events.push([method, progress]); return Promise.resolve(); } });
    await worker.emit({ kind: 'chunk', jobId: dto.jobId, sequence: 0, bytes: new Uint8Array([1]).buffer });
    b.api.cancel(dto.jobId);
    b.api.cancel(dto.jobId);
    await worker.emit({ kind: 'progress', jobId: dto.jobId, progress: { percent: 90 } });
    await worker.emit({ kind: 'chunk', jobId: dto.jobId, sequence: 1, bytes: new Uint8Array([2]).buffer });
    await b.result(worker, dto, { downloadId: dto.jobId, filename: 'cancelled.zip' });
    await assert.rejects(pending, /XER_CANCELLED/);
    assert.equal(worker.messages.filter(message => message.kind === 'cancel').length, 1);
    assert.equal(worker.messages.filter(message => message.kind === 'ack').length, 1);
    assert.equal(events.length, 0);
    assert.equal(b.blobs.length, 0);
    assert.throws(() => b.api.commit(dto.jobId), /no longer exists/);
});

test('cancellation while preparing the ZIP stream prevents a late Blob even after the forced restart deadline', async t => {
    const b = await browser(t);
    const dto = b.job('cancel-prepare', b.select().source, 2, 'export');
    const { pending, worker } = await b.start(dto);
    await worker.emit({ kind: 'chunk', jobId: dto.jobId, sequence: 0, bytes: new Uint8Array([8, 9]).buffer });
    let release;
    const gate = new Promise(resolve => { release = resolve; });
    const originalPrepare = b.downloads.prepare;
    b.downloads.prepare = (id, filename, type, reference) => originalPrepare(id, filename, type,
        { stream: async () => { await gate; return reference.stream(); } });
    const transfer = b.result(worker, dto, { downloadId: dto.jobId, filename: 'cancelled.zip' });
    await flush();
    b.api.cancel(dto.jobId);
    b.timers.advance(250);
    await assert.rejects(pending, /XER_CANCELLED/);
    release();
    await transfer;
    assert.equal(worker.terminated, true);
    assert.equal(b.blobs.length, 0);
    assert.equal(b.clicks.length, 0);
    assert.equal(b.streams[0].locked, false);
    assert.equal(b.metrics.length, 0);
    assert.throws(() => b.api.commit(dto.jobId), /no longer exists/);
});

test('stale job progress is ignored and stale result identity or generation is rejected', async t => {
    for (const extra of [{ jobId: 'old-job' }, { generation: 6 }]) {
        const b = await browser(t);
        const dto = b.job('current-job', b.select().source, 7);
        const events = [];
        const { pending, worker } = await b.start(dto, { invokeMethodAsync(method, progress) { events.push([method, progress]); return Promise.resolve(); } });
        await worker.emit({ kind: 'progress', jobId: 'old-job', progress: { percent: 50 } });
        await worker.emit({ kind: 'result', jobId: 'old-job', result: { jobId: 'old-job', generation: 6 } });
        await worker.emit({ kind: 'progress', jobId: dto.jobId, progress: { percent: 10 } });
        assert.deepEqual(events, [['OnProgress', { percent: 10 }]]);
        await b.result(worker, dto, extra);
        await assert.rejects(pending, /A stale browser processing result was rejected/);
        assert.equal(b.blobs.length, 0);
    }
});

test('forced worker restart retains original File handles and ignores messages from the old instance', async t => {
    const b = await browser(t);
    const selected = b.select();
    const first = await b.start(b.job('hung', selected.source));
    b.input.files = [Object.freeze({ name: 'replacement.xer', size: 100 })];
    b.api.cancel('hung');
    b.timers.advance(250);
    await assert.rejects(first.pending, /XER_CANCELLED/);
    const dto = b.job('restart', selected.source, 2);
    const events = [];
    const next = await b.start(dto, { invokeMethodAsync(method, value) { events.push([method, value]); return Promise.resolve(); } });
    assert.equal(next.worker.messages[0].files[0].file, selected.file);
    await first.worker.emit({ kind: 'progress', jobId: dto.jobId, progress: { percent: 99 } });
    await b.result(first.worker, dto, { downloadId: dto.jobId, filename: 'late.zip' });
    assert.equal(events.length, 0);
    assert.equal(b.blobs.length, 0);
    await b.result(next.worker, dto);
    await next.pending;
    const read = await b.api.readBytes(selected.source.sourceToken);
    read[0] = 99;
    assert.deepEqual([...await b.api.readBytes(selected.source.sourceToken)], [0, 1, 255]);
});

test('remove, invalidate, clear and dispose release handles and prevent stale outputs from resurfacing', async t => {
    const b = await browser(t);
    const selected = b.select();
    const dto = b.job('before-clear', selected.source, 1, 'export');
    const { pending, worker } = await b.start(dto);
    b.api.invalidate();
    assert.deepEqual(worker.messages.at(-1), { kind: 'invalidate' });
    b.api.remove(selected.source.sourceToken);
    assert.deepEqual(worker.messages.at(-1), { kind: 'remove', token: selected.source.sourceToken });
    await assert.rejects(b.api.readBytes(selected.source.sourceToken), /no longer registered/);
    b.api.clear();
    await assert.rejects(pending, /XER_CANCELLED/);
    assert.equal(worker.terminated, true);
    await b.result(worker, dto, { downloadId: dto.jobId, filename: 'late.zip' });
    assert.equal(b.blobs.length, 0);
    b.select();
    b.api.dispose();
    await assert.rejects(b.api.readBytes(selected.source.sourceToken), /no longer registered/);
    assert.throws(() => b.select('new-token'), /disposed/);
    await assert.rejects(b.run(b.job('disposed')), /disposed/);
});

test('metadata jobs reuse one worker and bound scalar history without transfer artefacts', async t => {
    const b = await browser(t);
    const selected = b.select();
    for (let index = 0; index < 70; index++) {
        const dto = b.job(`metadata-${index}`, selected.source, index);
        const pending = b.run(dto);
        if (index === 0) await b.workers[0].ready();
        await flush();
        const message = b.workers[0].messages.at(-1);
        assert.equal(message.kind, 'run');
        assert.equal(message.job.jobId, dto.jobId);
        assert.equal(message.files[0].file, selected.file);
        await b.result(b.workers[0], dto, { dataDate: '2026-08-31', sourceSha256: 'sha' });
        assert.equal((await pending).sourceSha256, 'sha');
    }
    assert.equal(b.workers.length, 1);
    assert.equal(selected.reads(), 0);
    assert.equal(b.blobs.length, 0);
    assert.equal(b.clicks.length, 0);
    assert.equal(b.streams.length, 0);
    assert.equal(b.timers.delays().length, 0);
    assert.equal(b.metrics.length, 64);
});
