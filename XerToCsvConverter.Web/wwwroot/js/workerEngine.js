// File objects remain immutable in this page/session. Only descriptors and
// completed ZIP chunks cross runtimes; input schedules never leave the browser.
const files = new Map();
let worker;
let ready;
let active;
let startupFailure;
let disposed = false;
let instance = 0;
let abortReady;

function version() {
    return document.querySelector('meta[name="application-version"]')?.content ?? '__APP_VERSION__';
}

export function registerSelection(token, selectionIndex, name, size) {
    if (disposed) throw new Error('The browser processing session is disposed.');
    if (files.has(token)) throw new Error('The source occurrence is already registered.');
    const file = document.getElementById('xer-file-input')?.files?.[selectionIndex];
    if (!file || file.name !== name || file.size !== size) throw new Error('The selected browser file is no longer available.');
    files.set(token, file);
}

export async function readBytes(token) {
    const file = files.get(token);
    if (!file) throw new Error('This browser source is no longer registered.');
    return new Uint8Array(await file.arrayBuffer());
}

function stopWorker(error = new Error('XER_CANCELLED')) {
    abortReady?.(error);
    abortReady = undefined;
    worker?.terminate();
    worker = undefined;
    ready = undefined;
    instance++;
}

function settle(job, error, result) {
    if (active !== job) return;
    clearTimeout(job.cancelTimer);
    active = undefined;
    job.chunks.length = 0;
    if (error) { globalThis.xerDownloads.discard(job.id); job.reject(error); }
    else {
        const metrics = globalThis.xerProcessingMetrics ??= [];
        metrics.push({ kind: job.kind, elapsedMs: performance.now() - job.started, warningCount: result.warningCount,
            fileCount: result.fileCount, stageMilliseconds: result.stageMilliseconds,
            downloadId: result.downloadId, resultJsonBytes: job.resultJsonBytes,
            retainedCacheBytes: result.retainedCacheBytes, cacheBudgetBytes: result.cacheBudgetBytes,
            cacheHits: result.cacheHits, parsedSources: result.parsedSources,
            allocatedBytes: result.allocatedBytes, managedHeapBytes: result.managedHeapBytes,
            sampledManagedHeapHighWaterBytes: result.sampledManagedHeapHighWaterBytes,
            zipBytes: result.zipBytes, zipBufferCapacityBytes: result.zipBufferCapacityBytes,
            retainedSourceCount: result.retainedSourceCount,
            diagnosticSha256: result.diagnosticSha256, csvSha256ByFile: result.csvSha256ByFile });
        if (metrics.length > 64) metrics.shift();
        job.resolve(result);
    }
}

async function ensureReady() {
    if (disposed) throw new Error('The browser processing session is disposed.');
    if (startupFailure) throw new Error(`XER_WORKER_STARTUP: ${startupFailure}`);
    if (ready) return ready;
    const serial = ++instance;
    ready = new Promise((resolve, reject) => {
        let timer;
        let initialized = false;
        abortReady = error => { clearTimeout(timer); reject(error); };
        const fail = message => {
            if (serial !== instance) return;
            clearTimeout(timer);
            startupFailure = message;
            stopWorker(new Error(`XER_WORKER_STARTUP: ${message}`));
        };
        try {
            worker = new Worker(new URL(`worker/main.js?v=${encodeURIComponent(version())}`, document.baseURI), { type: 'module', name: 'XER processing' });
            timer = setTimeout(() => fail('Worker initialisation timed out.'), 30000);
            worker.onerror = event => {
                if (serial !== instance) return;
                if (initialized && active) { const job = active; stopWorker(); settle(job, new Error(event.message ?? 'Browser worker stopped unexpectedly.')); }
                else fail(event.message ?? 'Worker initialisation failed.');
            };
            worker.onmessage = async ({ data }) => {
                if (serial !== instance) return;
                if (data.kind === 'ready') {
                    if (data.protocol !== 1 || data.version !== version()) { fail('UI and worker versions differ. Reload this page.'); return; }
                    clearTimeout(timer);
                    initialized = true;
                    abortReady = undefined;
                    resolve();
                    return;
                }
                if (data.kind === 'startup-error') { fail(data.message); return; }
                const job = active;
                if (!job || job.id !== data.jobId) return;
                if (job.cancelled) {
                    if (data.kind === 'result' || data.kind === 'error') settle(job, new Error('XER_CANCELLED'));
                    return;
                }
                if (data.kind === 'result-metrics') {
                    if (Number.isFinite(data.resultSerializationMs) && data.resultSerializationMs >= 0
                        && Number.isSafeInteger(data.resultJsonBytes) && data.resultJsonBytes >= 0) {
                        job.resultSerializationMs = data.resultSerializationMs;
                        job.resultJsonBytes = data.resultJsonBytes;
                        if (Number.isFinite(data.browserFileReadMs) && data.browserFileReadMs >= 0
                            && Number.isFinite(data.webCryptoSha256Ms) && data.webCryptoSha256Ms >= 0) {
                            job.browserFileReadMs = data.browserFileReadMs;
                            job.webCryptoSha256Ms = data.webCryptoSha256Ms;
                        }
                    }
                } else if (data.kind === 'progress') {
                    job.callback?.invokeMethodAsync('OnProgress', data.progress)?.catch(() => {});
                } else if (data.kind === 'chunk') {
                    if (data.sequence !== job.chunks.length || !(data.bytes instanceof ArrayBuffer)) {
                        stopWorker(); settle(job, new Error('ZIP transfer order is invalid.')); return;
                    }
                    job.chunks.push(new Uint8Array(data.bytes));
                    worker.postMessage({ kind: 'ack', jobId: job.id, sequence: data.sequence });
                } else if (data.kind === 'error') {
                    settle(job, new Error(data.message));
                } else if (data.kind === 'result') {
                    try {
                        if (data.result.jobId !== job.id || data.result.generation !== job.generation)
                            throw new Error('A stale browser processing result was rejected.');
                        const stages = data.result.stageMilliseconds ??= {};
                        if (job.resultSerializationMs !== undefined) stages.resultSerialization = job.resultSerializationMs;
                        if (job.webCryptoSha256Ms !== undefined) {
                            stages.browserFileRead = job.browserFileReadMs;
                            stages.webCryptoSha256 = job.webCryptoSha256Ms;
                            // The C# FileReading scope wraps FillFile, which includes
                            // native hashing. Attribute that measured time to SHA.
                            if (Number.isFinite(stages.FileReading)) stages.FileReading = Math.max(0, stages.FileReading - job.webCryptoSha256Ms);
                            if (Number.isFinite(stages.SourceSha256)) stages.SourceSha256 += job.webCryptoSha256Ms;
                        }
                        if (data.result.downloadId) {
                            const preparationStarted = performance.now();
                            let index = 0;
                            const chunks = job.chunks;
                            await globalThis.xerDownloads.prepare(job.id, data.result.filename, 'application/zip', {
                                stream: async () => new ReadableStream({
                                    pull(controller) {
                                        if (job.cancelled) { controller.error(new Error('XER_CANCELLED')); return; }
                                        if (index === chunks.length) controller.close();
                                        else controller.enqueue(chunks[index++]);
                                    }
                                })
                            });
                            stages.downloadPreparation = performance.now() - preparationStarted;
                        }
                        if (job.cancelled || active !== job) throw new Error('XER_CANCELLED');
                        settle(job, undefined, data.result);
                    } catch (error) { settle(job, error); }
                }
            };
        } catch (error) { fail(String(error.message ?? error)); }
    });
    return ready;
}

export function run(job, callback) {
    if (active) throw new Error('Another browser processing operation is active.');
    const sourceFiles = job.sources.map(source => {
        const file = files.get(source.sourceToken);
        if (!file) throw new Error('This browser source is no longer registered.');
        return { token: source.sourceToken, file };
    });
    return new Promise((resolve, reject) => {
        const pending = { id: job.jobId, generation: job.generation, resolve, reject, callback, chunks: [], cancelled: false,
            kind: job.kind, started: performance.now() };
        active = pending;
        ensureReady().then(() => {
            if (active === pending && !pending.cancelled) worker.postMessage({ kind: 'run', job, files: sourceFiles });
        }).catch(error => settle(pending, error));
    });
}

export function cancel(jobId) {
    const job = active;
    if (!job || job.id !== jobId || job.cancelled) return;
    job.cancelled = true;
    globalThis.xerDownloads.discard(job.id);
    worker?.postMessage({ kind: 'cancel', jobId });
    job.cancelTimer = setTimeout(() => {
        if (active !== job) return;
        stopWorker();
        settle(job, new Error('XER_CANCELLED'));
    }, 250);
}

export function remove(token) {
    files.delete(token);
    worker?.postMessage({ kind: 'remove', token });
}

export function invalidate() { worker?.postMessage({ kind: 'invalidate' }); }

export function clear() {
    const job = active;
    if (job) { stopWorker(); settle(job, new Error('XER_CANCELLED')); }
    else stopWorker();
    files.clear();
    startupFailure = undefined;
}

export function discard(downloadId) { globalThis.xerDownloads.discard(downloadId); }
export function commit(downloadId) {
    const metric = globalThis.xerProcessingMetrics?.findLast(entry => entry.downloadId === downloadId);
    const started = performance.now();
    let succeeded = false;
    try {
        globalThis.xerDownloads.commit(downloadId);
        succeeded = true;
    } finally {
        // The prepared result is recorded before this irreversible browser handoff.
        // Update only that bounded entry, and preserve a completed handoff on duplicate calls.
        if (metric && !metric.browserHandoffSucceeded) {
            metric.stageMilliseconds.browserHandoff = performance.now() - started;
            metric.browserHandoffSucceeded = succeeded;
        }
    }
}
export function dispose() { clear(); disposed = true; }
