// Developer lifecycle checks against the packaged site, using actual File, Worker,
// WebAssembly, stream and Blob implementations. No build or deployment is performed.
// node review/parser-performance-20260930/BrowserLifecycle.cjs --url URL --channel chrome --probe true
// Full workload: --bulk-rows 30000 --cycles 10 --memory-fixture "XER Sample/2608-EBA_PAA_8.0 (draft).xer"
// Use channel msedge for the same checks in installed Edge; results are browser-
// specific. Timing checks use scheduling tolerance around the 250 ms cancel timer.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { dependency } = require('./browser-dependencies.cjs');
const { chromium } = dependency('playwright');
const crypto = require('node:crypto');
const { execFileSync } = require('node:child_process');

const args = Object.fromEntries(Array.from({ length: Math.floor((process.argv.length - 2) / 2) }, (_, index) =>
    [process.argv[2 + index * 2].replace(/^--/, ''), process.argv[3 + index * 2]]));
const repo = path.resolve(__dirname, '../..');
const url = args.url || 'http://127.0.0.1:8767/XER-to-CSV/';
const channel = args.channel || 'chrome';
const output = path.resolve(args.output || path.join(repo, 'artifacts/parser-performance-20260930', `browser-lifecycle-${channel}.json`));
const fixturePath = path.resolve(args.fixture || path.join(repo, 'review/fixtures/J5001_C_BL01_2026-01-31.xer'));
const fixture = fs.readFileSync(fixturePath, 'utf8');
const memoryFixturePath = path.resolve(args['memory-fixture'] || fixturePath);
const memoryFixtureBytes = fs.readFileSync(memoryFixturePath);
const report = { url, channel, timestamp: new Date().toISOString(), checks: [], console: [], errors: [], trace: [] };
let browser, context, page;
function save() { fs.mkdirSync(path.dirname(output), { recursive: true }); fs.writeFileSync(output, JSON.stringify(report, null, 2)); }
async function check(name, operation) {
    const start = Date.now();
    try { const evidence = await operation(); report.checks.push({ name, passed: true, elapsedMs: Date.now() - start, evidence }); }
    catch (error) { report.checks.push({ name, passed: false, elapsedMs: Date.now() - start, error: String(error.stack || error) }); throw error; }
    finally { save(); }
}

async function initialisePage() {
    // Keep the UI runtime out of direct worker checks, so only the real processing
    // worker owns a .NET heap. A separate page below verifies UI fallback.
    await page.route('**/_framework/blazor.webassembly.js*', route => route.fulfill({ contentType: 'application/javascript', body: '' }));
    await page.addInitScript(() => {
        const NativeWorker = globalThis.Worker;
        globalThis.__life = { workers: [], events: [], blobs: [], revoked: [], progress: [], dropAcknowledgements: false };
        const create = URL.createObjectURL.bind(URL), revoke = URL.revokeObjectURL.bind(URL);
        URL.createObjectURL = blob => { const url = create(blob); __life.blobs.push({ url, size: blob.size, type: blob.type }); return url; };
        URL.revokeObjectURL = url => { __life.revoked.push(url); revoke(url); };
        globalThis.Worker = class extends NativeWorker {
            constructor(...args) {
                super(...args);
                this.lifecycleIndex = __life.workers.length;
                __life.workers.push({ url: String(args[0]), terminated: false });
                this.addEventListener('message', ({ data }) => {
                    __life.events.push({ worker: this.lifecycleIndex, kind: data.kind, jobId: data.jobId,
                        sequence: data.sequence, bytes: data.bytes?.byteLength, message: data.message, time: performance.now() });
                    if (data.kind === 'chunk' && __life.cancelOnChunk === data.jobId) {
                        __life.cancelledAt = performance.now(); __life.api.cancel(data.jobId);
                    }
                });
                this.addEventListener('error', event => __life.events.push({ worker: this.lifecycleIndex, kind: 'fatal', message: event.message }));
            }
            postMessage(message, ...rest) {
                if (message.kind === 'ack' && __life.dropAcknowledgements) return;
                return super.postMessage(message, ...rest);
            }
            terminate() { __life.workers[this.lifecycleIndex].terminated = true; return super.terminate(); }
        };
    });
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await page.evaluate(async () => {
        const input = document.createElement('input'); input.type = 'file'; input.id = 'xer-file-input'; document.body.append(input);
        __life.api = await import(new URL('js/workerEngine.js', document.baseURI));
        __life.counter = 0;
        __life.registerSelected = token => {
            const file = document.getElementById('xer-file-input').files[0];
            __life.api.registerSelection(token, 0, file.name, file.size);
            return { sourceToken: token, name: file.name, size: file.size };
        };
        __life.register = (token, name, text) => {
            const file = new File([text], name, { type: 'application/octet-stream' });
            const transfer = new DataTransfer(); transfer.items.add(file);
            document.getElementById('xer-file-input').files = transfer.files;
            __life.api.registerSelection(token, 0, name, file.size);
            return { sourceToken: token, name, size: file.size };
        };
        __life.run = async (kind, sources, extra = {}) => {
            const job = { protocol: 1, jobId: `lifecycle-${++__life.counter}`, generation: 1, kind, sources, tables: [], ...extra };
            __life.lastJob = job;
            return __life.api.run(job, { invokeMethodAsync(method, progress) {
                __life.progress.push({ method, ...progress });
                if (__life.cancelOnProgress === job.jobId && progress.percent < 70) {
                    __life.cancelledAt = performance.now(); __life.api.cancel(job.jobId);
                }
                return Promise.resolve();
            } });
        };
    });
}

function bulkFixture(count) {
    const lines = fixture.split(/\r?\n/);
    const begin = lines.findIndex(line => line === '%T\tTASK');
    const end = lines.findIndex((line, index) => index > begin && line.startsWith('%T\t'));
    const headers = lines[begin + 1].split('\t').slice(1);
    const template = lines[begin + 2].split('\t').slice(1);
    const rows = [];
    for (let index = 0; index < count; index++) {
        const row = template.slice();
        row[headers.indexOf('task_id')] = `B${index}`;
        row[headers.indexOf('task_code')] = `B${index}`;
        row[headers.indexOf('task_name')] = crypto.createHash('sha256').update(`lifecycle task ${index}`).digest('hex');
        rows.push('%R\t' + row.join('\t'));
    }
    return [...lines.slice(0, begin + 2), ...rows, ...lines.slice(end)].join('\r\n');
}

async function browserProcessMemory(cdp) {
    try {
        const { processInfo } = await cdp.send('SystemInfo.getProcessInfo');
        const ids = processInfo.map(process => Number(process.id)).filter(id => Number.isSafeInteger(id) && id > 0);
        const command = `@(Get-Process -Id ${ids.join(',')} -ErrorAction SilentlyContinue | ForEach-Object { `
            + '[PSCustomObject]@{ id=$_.Id; workingSetBytes=$_.WorkingSet64; privateBytes=$_.PrivateMemorySize64 } }) | ConvertTo-Json -Compress';
        const parsed = JSON.parse(execFileSync('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', command],
            { encoding: 'utf8', windowsHide: true }));
        const processes = Array.isArray(parsed) ? parsed : [parsed];
        return { browserProcessWorkingSetBytes: processes.reduce((sum, process) => sum + process.workingSetBytes, 0),
            browserProcessPrivateBytes: processes.reduce((sum, process) => sum + process.privateBytes, 0), processes };
    } catch (error) { return { browserProcessMemoryUnavailable: String(error.message) }; }
}

async function fallbackUi() {
    const uiContext = await browser.newContext({ acceptDownloads: true });
    const ui = await uiContext.newPage();
    let startupRequests = 0, downloads = 0;
    ui.on('download', () => downloads++);
    await uiContext.route('**/worker/main.js*', route => { startupRequests++; return route.abort('failed'); });
    try {
        await ui.goto(url, { waitUntil: 'domcontentloaded' });
        await ui.locator('#xer-file-input').waitFor();
        await ui.evaluate(() => { document.getElementById('xer-file-input').dataset.lifecyclePicker = 'before-upload'; });
        await ui.locator('#xer-file-input').setInputFiles({ name: '2601 fallback.xer', mimeType: 'application/octet-stream', buffer: Buffer.from(fixture) });
        await ui.waitForFunction(() => document.querySelector('.file-status')?.textContent?.trim().toLowerCase() === 'ready');
        await ui.waitForFunction(() => {
            const input = document.getElementById('xer-file-input');
            return input.dataset.lifecyclePicker !== 'before-upload' && input.files.length === 0;
        });
        const inputAfterUpload = await ui.evaluate(() => {
            const input = document.getElementById('xer-file-input');
            return { nativeFiles: input.files.length, frameworkFiles: Object.keys(input._blazorFilesById || {}).length };
        });
        assert.deepEqual(inputAfterUpload, { nativeFiles: 0, frameworkFiles: 0 });
        const parse = ui.getByRole('button', { name: 'Parse and load tables from uploaded XER files' });
        await parse.click();
        await ui.getByRole('button', { name: 'Export All as ZIP', exact: true }).waitFor();
        await ui.waitForFunction(() => !document.querySelector('[aria-label="Parse and load tables from uploaded XER files"]')?.disabled);
        assert.equal(startupRequests, 1);
        const event = ui.waitForEvent('download');
        await ui.getByRole('button', { name: 'Export All as ZIP', exact: true }).click();
        const download = await event;
        assert.equal(await download.failure(), null);
        assert.equal(downloads, 1);
        assert.equal(startupRequests, 1);
        await ui.evaluate(() => { document.getElementById('xer-file-input').dataset.lifecyclePicker = 'before-remove'; });
        await ui.getByRole('button', { name: 'Remove 2601 fallback.xer', exact: true }).click();
        await ui.waitForFunction(() => !document.querySelector('.file-item')
            && document.getElementById('xer-file-input').dataset.lifecyclePicker !== 'before-remove');
        const inputAfterRemove = await ui.evaluate(() => {
            const input = document.getElementById('xer-file-input');
            return { nativeFiles: input.files.length, frameworkFiles: Object.keys(input._blazorFilesById || {}).length };
        });
        assert.deepEqual(inputAfterRemove, { nativeFiles: 0, frameworkFiles: 0 });
        await ui.locator('#xer-file-input').setInputFiles({ name: '2601 fallback.xer', mimeType: 'application/octet-stream', buffer: Buffer.from(fixture) });
        await ui.waitForFunction(() => document.querySelector('.file-status')?.textContent?.trim().toLowerCase() === 'ready');
        await ui.evaluate(() => { document.getElementById('xer-file-input').dataset.lifecyclePicker = 'before-clear'; });
        await ui.getByRole('button', { name: 'Clear all uploaded files' }).click();
        await ui.waitForFunction(() => !document.querySelector('.file-item')
            && document.getElementById('xer-file-input').dataset.lifecyclePicker !== 'before-clear');
        assert.equal(await ui.locator('.file-item').count(), 0);
        const inputAfterClear = await ui.evaluate(() => {
            const input = document.getElementById('xer-file-input');
            return { nativeFiles: input.files.length, frameworkFiles: Object.keys(input._blazorFilesById || {}).length };
        });
        assert.deepEqual(inputAfterClear, { nativeFiles: 0, frameworkFiles: 0 });
        return { forcedStartupFailures: startupRequests, successfulFallbackDownloads: downloads,
            filename: download.suggestedFilename(), inputAfterUpload, inputAfterRemove, inputAfterClear };
    } finally { await uiContext.close(); }
}

(async () => {
    try {
        browser = await chromium.launch({ channel, headless: true, args: ['--enable-precise-memory-info'] });
        report.browserVersion = browser.version();
        context = await browser.newContext({ acceptDownloads: false });
        page = await context.newPage();
        page.setDefaultTimeout(60000);
        page.on('console', message => report.console.push({ type: message.type(), text: message.text() }));
        page.on('pageerror', error => report.errors.push(String(error)));
        await initialisePage();
        report.applicationVersion = await page.locator('meta[name="application-version"]').getAttribute('content');
        report.workerAssetHashes = await page.evaluate(async () => {
            const result = {};
            for (const file of ['js/workerEngine.js', 'worker/main.js', 'worker/_framework/blazor.boot.json']) {
                const bytes = await (await fetch(new URL(file, document.baseURI))).arrayBuffer();
                result[file] = [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))]
                    .map(byte => byte.toString(16).padStart(2, '0')).join('');
            }
            return result;
        });
        await check('real-worker-startup-and-metadata', async () => {
            await page.evaluate(text => { __life.a = __life.register('a', '2601 first.xer', text); }, fixture);
            const pending = page.evaluate(() => __life.run('metadata', [__life.a]));
            const guard = setTimeout(async () => {
                report.trace = await page.evaluate(() => ({ events: __life.events, workers: __life.workers })).catch(() => []);
                save();
            }, 15000);
            let deadline;
            try {
                const result = await Promise.race([pending, new Promise((_, reject) => { deadline = setTimeout(() => reject(new Error('Metadata worker probe exceeded 40 seconds.')), 40000); })]);
                assert.equal(result.dataDate, '2026-01-30'); assert.match(result.sourceSha256, /^[a-f0-9]{64}$/);
                assert.equal(result.parsedSources, 0);
                return result;
            } finally { clearTimeout(guard); clearTimeout(deadline); }
        });
        if (args.probe === 'true') return;
        await check('cache-hit-lru-eviction-and-zero-budget-reread', async () => {
            const evidence = await page.evaluate(async text => {
                __life.b = __life.register('b', '2601 second.xer', text.replaceAll('Synthetic A100', 'Synthetic B100'));
                __life.c = __life.register('c', '2601 third.xer', text.replaceAll('Synthetic A100', 'Synthetic C100'));
                const first = await __life.run('parse', [__life.a]);
                const second = await __life.run('parse', [__life.b], { cacheBudgetBytes: first.retainedCacheBytes * 2 });
                const touched = await __life.run('parse', [__life.a]);
                const third = await __life.run('parse', [__life.c]);
                const retained = await __life.run('parse', [__life.a]);
                const evicted = await __life.run('parse', [__life.b]);
                const zero = await __life.run('parse', [__life.a], { cacheBudgetBytes: 0 });
                const reread = await __life.run('parse', [__life.a]);
                return { first, second, touched, third, retained, evicted, zero, reread };
            }, fixture);
            assert.equal(evidence.first.parsedSources, 1);
            assert.equal(evidence.touched.cacheHits, 1);
            assert.equal(evidence.retained.cacheHits, 1);
            assert.equal(evidence.evicted.cacheHits, 0);
            assert.equal(evidence.evicted.parsedSources, 1);
            assert.equal(evidence.zero.retainedCacheBytes, 0);
            assert.equal(evidence.reread.cacheHits, 0);
            assert.equal(evidence.reread.parsedSources, 1);
            for (const result of Object.values(evidence)) assert.ok(result.retainedCacheBytes <= result.cacheBudgetBytes);
            return evidence;
        });
        await check('pending-startup-cancel-and-original-file-after-forced-restart', async () => {
            await page.evaluate(text => {
                __life.api.clear();
                __life.original = __life.register('original', '2601 original.xer', text);
                const replacement = new DataTransfer(); replacement.items.add(new File(['different content'], 'other.xer'));
                document.getElementById('xer-file-input').files = replacement.files;
            }, fixture);
            let intercepted;
            const holdStartup = route => { intercepted = route; };
            await context.route('**/worker/main.js*', holdStartup);
            const cancellation = await page.evaluate(async () => {
                const jobId = `lifecycle-${__life.counter + 1}`;
                const promise = __life.run('metadata', [__life.original]);
                let cancelledAt;
                setTimeout(() => { cancelledAt = performance.now(); __life.api.cancel(jobId); }, 50);
                try { await promise; return { failed: false }; }
                catch (error) { return { failed: true, message: error.message, settlementMs: performance.now() - cancelledAt }; }
            });
            if (intercepted) await intercepted.abort().catch(() => {});
            await context.unroute('**/worker/main.js*', holdStartup);
            assert.equal(cancellation.failed, true); assert.match(cancellation.message, /XER_CANCELLED/);
            assert.ok(cancellation.settlementMs < 1000, 'Real browser cancellation must settle promptly around the 250 ms timer.');
            const metadata = await page.evaluate(() => __life.run('metadata', [__life.original]));
            assert.equal(metadata.sourceSha256, crypto.createHash('sha256').update(fixture).digest('hex'));
            const handles = await page.evaluate(() => ({ workers: __life.workers, visibleInput: document.getElementById('xer-file-input').files[0].name }));
            assert.equal(handles.visibleInput, 'other.xer');
            assert.ok(handles.workers.some(worker => worker.terminated));
            return { cancellation, sourceSha256: metadata.sourceSha256, handles };
        });
        const bulkRows = Number(args['bulk-rows'] || 0);
        if (bulkRows > 0) {
            const text = bulkFixture(bulkRows);
            await page.evaluate(value => { __life.large = __life.register('large', '2601 large.xer', value); }, text);
            await check('cancel-during-real-parsing-prevents-output', async () => {
                const result = await page.evaluate(async () => {
                    __life.cancelOnProgress = `lifecycle-${__life.counter + 1}`;
                    const start = performance.now(), prior = __life.blobs.length;
                    try { await __life.run('parse', [__life.large]); return { cancelled: false }; }
                    catch (error) { return { cancelled: true, message: error.message, elapsedMs: performance.now() - start,
                        cancelSettlementMs: performance.now() - __life.cancelledAt,
                        createdBlobs: __life.blobs.length - prior, progress: __life.progress.filter(value => value.percent < 70).at(-1) }; }
                    finally { __life.cancelOnProgress = undefined; }
                });
                assert.equal(result.cancelled, true); assert.match(result.message, /XER_CANCELLED/);
                assert.equal(result.createdBlobs, 0); assert.ok(result.progress);
                return { bulkRows, ...result };
            });
            await check('cancel-during-real-zip-transfer-prevents-late-blob-and-commit', async () => {
                const result = await page.evaluate(async () => {
                    __life.dropAcknowledgements = true;
                    __life.cancelOnChunk = `lifecycle-${__life.counter + 1}`;
                    const id = __life.cancelOnChunk, prior = __life.blobs.length, start = performance.now();
                    let outcome;
                    try { await __life.run('export', [__life.large], { tables: ['TASK'] }); outcome = { cancelled: false }; }
                    catch (error) { outcome = { cancelled: true, message: error.message, cancelSettlementMs: performance.now() - __life.cancelledAt }; }
                    finally { __life.dropAcknowledgements = false; __life.cancelOnChunk = undefined; }
                    await new Promise(resolve => setTimeout(resolve, 300));
                    try { __life.api.commit(id); outcome.committed = true; }
                    catch (error) { outcome.committed = false; outcome.commitMessage = error.message; }
                    return { ...outcome, elapsedMs: performance.now() - start, createdBlobs: __life.blobs.length - prior,
                        chunks: __life.events.filter(event => event.jobId === id && event.kind === 'chunk'), workers: __life.workers };
                });
                assert.equal(result.cancelled, true); assert.match(result.message, /XER_CANCELLED/);
                assert.equal(result.committed, false); assert.equal(result.createdBlobs, 0);
                assert.ok(result.chunks.length > 0); assert.ok(result.workers.at(-1).terminated);
                const retry = await page.evaluate(() => __life.run('metadata', [__life.original]));
                assert.equal(retry.sourceSha256, crypto.createHash('sha256').update(fixture).digest('hex'));
                return { bulkRows, ...result, retainedOriginalHashAfterRestart: retry.sourceSha256 };
            });
        }
        await check('cancel-during-real-blob-preparation-prevents-late-url', async () => {
            const evidence = await page.evaluate(async () => {
                const prior = __life.blobs.length, original = xerDownloads.prepare;
                let release, prepared = false, cancelledAt;
                const gate = new Promise(resolve => { release = resolve; });
                const id = `lifecycle-${__life.counter + 1}`;
                xerDownloads.prepare = (downloadId, name, type, reference) => {
                    prepared = true;
                    setTimeout(() => { cancelledAt = performance.now(); __life.api.cancel(id); }, 10);
                    return original(downloadId, name, type, { stream: async () => { await gate; return reference.stream(); } });
                };
                let outcome;
                try { await __life.run('export', [__life.original], { tables: ['TASK'] }); outcome = { cancelled: false }; }
                catch (error) { outcome = { cancelled: true, message: error.message, settlementMs: performance.now() - cancelledAt }; }
                finally { release(); xerDownloads.prepare = original; }
                await new Promise(resolve => setTimeout(resolve, 100));
                try { __life.api.commit(id); outcome.committed = true; }
                catch (error) { outcome.committed = false; outcome.commitMessage = error.message; }
                return { ...outcome, prepared, createdBlobs: __life.blobs.length - prior };
            });
            assert.equal(evidence.prepared, true); assert.equal(evidence.cancelled, true);
            assert.match(evidence.message, /XER_CANCELLED/);
            assert.equal(evidence.committed, false); assert.equal(evidence.createdBlobs, 0);
            return evidence;
        });
        await check('real-stream-blob-discard-and-ten-teardown-cycles-memory', async () => {
            const cdp = await context.newCDPSession(page);
            await cdp.send('Performance.enable');
            const cycles = [];
            for (let index = 0; index < Number(args.cycles || 10); index++) {
                await page.locator('#xer-file-input').setInputFiles(memoryFixturePath);
                const cycle = await page.evaluate(async index => {
                    __life.api.clear();
                    const source = __life.registerSelected(`cycle-${index}`);
                    const metadata = await __life.run('metadata', [source]);
                    const parsed = await __life.run('parse', [source], { cacheBudgetBytes: 268435456 });
                    const exported = await __life.run('export', [source], { tables: ['TASK'] });
                    __life.api.discard(exported.downloadId);
                    const cleared = await __life.run('clear', []);
                    __life.api.clear();
                    document.getElementById('xer-file-input').value = '';
                    return { metadataHash: metadata.sourceSha256, parsedSources: parsed.parsedSources,
                        retainedCacheBytes: parsed.retainedCacheBytes, cacheBudgetBytes: parsed.cacheBudgetBytes,
                        retainedSourceCount: parsed.retainedSourceCount, managedHeapBytes: exported.managedHeapBytes,
                        sampledManagedHeapHighWaterBytes: exported.sampledManagedHeapHighWaterBytes,
                        zipBytes: __life.blobs.at(-1)?.size, zipBufferCapacityBytes: exported.zipBufferCapacityBytes,
                        cacheAfterClear: cleared.retainedCacheBytes, sourceCountAfterClear: cleared.retainedSourceCount,
                        liveWorkers: __life.workers.filter(worker => !worker.terminated).length,
                        preparedUrls: __life.blobs.length - __life.revoked.length };
                }, index);
                await cdp.send('HeapProfiler.collectGarbage');
                const metrics = await cdp.send('Performance.getMetrics');
                cycle.mainJsHeapUsedBytes = metrics.metrics.find(metric => metric.name === 'JSHeapUsedSize')?.value;
                assert.equal(cycle.parsedSources, 1); assert.equal(cycle.cacheAfterClear, 0);
                assert.equal(cycle.retainedSourceCount, 1); assert.equal(cycle.sourceCountAfterClear, 0);
                assert.equal(cycle.liveWorkers, 0); assert.equal(cycle.preparedUrls, 0);
                assert.ok(cycle.zipBytes > 0); assert.ok(cycle.retainedCacheBytes <= cycle.cacheBudgetBytes);
                cycles.push(cycle);
            }
            await cdp.detach();
            return { inputBytes: memoryFixtureBytes.length, inputSha256: crypto.createHash('sha256').update(memoryFixtureBytes).digest('hex'), cycles,
                mainHeapGrowthBytes: cycles.at(-1).mainJsHeapUsedBytes - cycles[0].mainJsHeapUsedBytes,
                memoryScope: 'Main JS heap is collected via CDP; managedHeapBytes is the worker result before teardown. These are not a total process peak-memory measurement.' };
        });
        await check('hot-worker-ten-update-export-remove-cycles-memory', async () => {
            const cdp = await context.newCDPSession(page), browserCdp = await browser.newBrowserCDPSession();
            await cdp.send('Performance.enable');
            await page.locator('#xer-file-input').setInputFiles(memoryFixturePath);
            await page.evaluate(async () => {
                __life.api.clear();
                __life.hotBaseline = __life.registerSelected('hot-baseline');
                await __life.run('metadata', [__life.hotBaseline]);
                __life.hotInitial = await __life.run('parse', [__life.hotBaseline], { cacheBudgetBytes: 268435456 });
                __life.hotWorkerCount = __life.workers.length;
            });
            const cycles = [];
            for (let index = 0; index < Number(args.cycles || 10); index++) {
                const cycle = await page.evaluate(async ({ index, text }) => {
                    if (__life.hotUpdate) __life.api.remove(__life.hotUpdate.sourceToken);
                    const update = __life.register(`hot-update-${index}`, `2602 update-${index}.xer`, text.replaceAll('Synthetic A100', `Synthetic update ${index}`));
                    __life.hotUpdate = update;
                    const metadata = await __life.run('metadata', [update]);
                    const parsed = await __life.run('parse', [__life.hotBaseline, update]);
                    const exported = await __life.run('export', [__life.hotBaseline, update], {
                        tables: ['01_XER_TASK', '06_XER_PREDECESSOR', '15_XER_RESOURCE_DISTRIBUTION'] });
                    __life.api.discard(exported.downloadId);
                    return { parsedSources: parsed.parsedSources, cacheHits: parsed.cacheHits,
                        retainedCacheBytes: parsed.retainedCacheBytes, cacheBudgetBytes: parsed.cacheBudgetBytes,
                        retainedSourceCount: parsed.retainedSourceCount, managedHeapBytes: exported.managedHeapBytes,
                        sampledManagedHeapHighWaterBytes: exported.sampledManagedHeapHighWaterBytes,
                        zipBytes: __life.blobs.at(-1)?.size, zipBufferCapacityBytes: exported.zipBufferCapacityBytes,
                        allocatedBytes: metadata.allocatedBytes + parsed.allocatedBytes + exported.allocatedBytes,
                        liveWorkers: __life.workers.filter(worker => !worker.terminated).length,
                        createdWorkers: __life.workers.length - __life.hotWorkerCount,
                        preparedUrls: __life.blobs.length - __life.revoked.length };
                }, { index, text: fixture });
                await cdp.send('HeapProfiler.collectGarbage');
                const metrics = await cdp.send('Performance.getMetrics');
                cycle.mainJsHeapUsedBytes = metrics.metrics.find(metric => metric.name === 'JSHeapUsedSize')?.value;
                Object.assign(cycle, await browserProcessMemory(browserCdp));
                cycle.cumulativeAllocatedBytes = (cycles.at(-1)?.cumulativeAllocatedBytes || 0) + cycle.allocatedBytes;
                assert.equal(cycle.parsedSources, 1); assert.equal(cycle.cacheHits, 1);
                assert.equal(cycle.retainedSourceCount, 2);
                assert.equal(cycle.liveWorkers, 1); assert.equal(cycle.createdWorkers, 0); assert.equal(cycle.preparedUrls, 0);
                assert.ok(cycle.zipBytes > 0); assert.ok(cycle.retainedCacheBytes <= cycle.cacheBudgetBytes);
                cycles.push(cycle);
            }
            const final = await page.evaluate(async () => { const cleared = await __life.run('clear', []); __life.api.clear(); return cleared.retainedCacheBytes; });
            assert.equal(final, 0);
            // The real baseline above exercises three expensive Enhanced tables.
            // Small complete review bundles below cover each profile's distinct
            // governance/projection/diagnostic lifetime without repeating large
            // histories. Each profile keeps one worker throughout its ten cycles.
            const reviewProfiles = {};
            for (const profile of ['programme', 'tender']) {
                const initial = await page.evaluate(async ({ profile, text }) => {
                    __life.api.clear();
                    __life.reviewBase = __life.register(`review-${profile}-base`, '2601 review-baseline.xer', text);
                    __life.reviewUpdate = undefined;
                    __life.reviewRequest = (profile, update, date, index) => {
                        const endOfMonth = value => {
                            const [year, month] = value.split('-').map(Number);
                            return new Date(Date.UTC(year, month, 0)).toISOString().slice(0, 10);
                        };
                        const project = { projectCode: 'LIFECYCLE', projectName: `Lifecycle ${profile} metadata ${index}`,
                            exportedAtUtc: '2026-12-31T00:00:00Z' };
                        if (profile === 'programme') return { programmeRequest: { ...project, programmeType: 'C', snapshots: [
                            { originalXerFilename: __life.reviewBase.name, snapshotKind: 0, snapshotTag: 'BL01',
                                monthUpdate: '2026-01-31', dataDate: '2026-01-30' },
                            ...(update ? [{ originalXerFilename: update.name, snapshotKind: 1,
                                snapshotTag: date.slice(2, 7).replace('-', ''), monthUpdate: endOfMonth(date), dataDate: date }] : [])
                        ] } };
                        return { tenderRequest: { ...project, state: 'NSW', sources: [
                            { sourceToken: __life.reviewBase.sourceToken, originalXerFilename: __life.reviewBase.name, statusDate: '2026-01-31' },
                            ...(update ? [{ sourceToken: update.sourceToken, originalXerFilename: update.name, statusDate: endOfMonth(date) }] : [])
                        ] } };
                    };
                    await __life.run('metadata', [__life.reviewBase]);
                    const seed = await __life.run(profile, [__life.reviewBase],
                        { cacheBudgetBytes: 268435456, ...__life.reviewRequest(profile, undefined, undefined, -1) });
                    __life.api.discard(seed.downloadId);
                    __life.reviewWorkerCount = __life.workers.length;
                    return { parsedSources: seed.parsedSources, retainedSourceCount: seed.retainedSourceCount, fileCount: seed.fileCount };
                }, { profile, text: fixture });
                assert.equal(initial.parsedSources, 1); assert.equal(initial.retainedSourceCount, 1); assert.ok(initial.fileCount > 5);
                const profileCycles = [];
                for (let index = 0; index < Number(args.cycles || 10); index++) {
                    const cycle = await page.evaluate(async ({ profile, index, text }) => {
                        if (__life.reviewUpdate) __life.api.remove(__life.reviewUpdate.sourceToken);
                        const date = new Date(Date.UTC(2026, index + 1, 27)).toISOString().slice(0, 10);
                        const update = __life.register(`review-${profile}-update-${index}`,
                            `${date.slice(2, 7).replace('-', '')} ${profile}-update-${index}.xer`,
                            text.replaceAll('2026-01-30', date).replaceAll('Synthetic A100', `Synthetic ${profile} update ${index}`));
                        __life.reviewUpdate = update;
                        const metadata = await __life.run('metadata', [update]);
                        const exported = await __life.run(profile, [__life.reviewBase, update], __life.reviewRequest(profile, update, date, index));
                        __life.api.discard(exported.downloadId);
                        return { parsedSources: exported.parsedSources, cacheHits: exported.cacheHits,
                            retainedCacheBytes: exported.retainedCacheBytes, cacheBudgetBytes: exported.cacheBudgetBytes,
                            retainedSourceCount: exported.retainedSourceCount, managedHeapBytes: exported.managedHeapBytes,
                            sampledManagedHeapHighWaterBytes: exported.sampledManagedHeapHighWaterBytes,
                            zipBytes: __life.blobs.at(-1)?.size, zipBufferCapacityBytes: exported.zipBufferCapacityBytes,
                            allocatedBytes: metadata.allocatedBytes + exported.allocatedBytes, fileCount: exported.fileCount,
                            diagnosticSha256: exported.diagnosticSha256,
                            liveWorkers: __life.workers.filter(worker => !worker.terminated).length,
                            createdWorkers: __life.workers.length - __life.reviewWorkerCount,
                            preparedUrls: __life.blobs.length - __life.revoked.length };
                    }, { profile, index, text: fixture });
                    await cdp.send('HeapProfiler.collectGarbage');
                    const metrics = await cdp.send('Performance.getMetrics');
                    cycle.mainJsHeapUsedBytes = metrics.metrics.find(metric => metric.name === 'JSHeapUsedSize')?.value;
                    Object.assign(cycle, await browserProcessMemory(browserCdp));
                    cycle.cumulativeAllocatedBytes = (profileCycles.at(-1)?.cumulativeAllocatedBytes || 0) + cycle.allocatedBytes;
                    assert.equal(cycle.parsedSources, 1); assert.equal(cycle.cacheHits, 1); assert.equal(cycle.retainedSourceCount, 2);
                    assert.equal(cycle.liveWorkers, 1); assert.equal(cycle.createdWorkers, 0); assert.equal(cycle.preparedUrls, 0);
                    assert.equal(cycle.fileCount, initial.fileCount); assert.match(cycle.diagnosticSha256, /^[a-f0-9]{64}$/);
                    assert.ok(cycle.zipBytes > 0); assert.ok(cycle.retainedCacheBytes <= cycle.cacheBudgetBytes);
                    profileCycles.push(cycle);
                }
                const cleared = await page.evaluate(async () => {
                    const result = await __life.run('clear', []); __life.api.clear();
                    document.getElementById('xer-file-input').value = '';
                    return { retainedCacheBytes: result.retainedCacheBytes, retainedSourceCount: result.retainedSourceCount };
                });
                assert.deepEqual(cleared, { retainedCacheBytes: 0, retainedSourceCount: 0 });
                const warmedProfile = profileCycles.slice(Math.min(2, profileCycles.length - 1));
                reviewProfiles[profile] = { baselineInputBytes: Buffer.byteLength(fixture), updateInput: 'Synthetic fixture with changing project Data Date and activity name',
                    initial, cycles: profileCycles, afterClear: cleared,
                    retainedCacheGrowthAfterWarmup: warmedProfile.at(-1).retainedCacheBytes - warmedProfile[0].retainedCacheBytes,
                    mainHeapGrowthAfterWarmup: warmedProfile.at(-1).mainJsHeapUsedBytes - warmedProfile[0].mainJsHeapUsedBytes,
                    managedHeapChangeAfterWarmup: warmedProfile.at(-1).managedHeapBytes - warmedProfile[0].managedHeapBytes };
            }
            await cdp.detach(); await browserCdp.detach();
            const warmed = cycles.slice(Math.min(2, cycles.length - 1));
            return { baselineInputBytes: memoryFixtureBytes.length,
                baselineInputSha256: crypto.createHash('sha256').update(memoryFixtureBytes).digest('hex'), updateInputBytes: Buffer.byteLength(fixture),
                enhancedTables: ['01_XER_TASK', '06_XER_PREDECESSOR', '15_XER_RESOURCE_DISTRIBUTION'],
                cycles, reviewProfiles, cacheAfterClear: final, retainedCacheGrowthAfterWarmup: warmed.at(-1).retainedCacheBytes - warmed[0].retainedCacheBytes,
                mainHeapGrowthAfterWarmup: warmed.at(-1).mainJsHeapUsedBytes - warmed[0].mainJsHeapUsedBytes,
                managedHeapChangeAfterWarmup: warmed.at(-1).managedHeapBytes - warmed[0].managedHeapBytes,
                memoryScope: 'One continuously retained worker per profile. Real baseline cycles select three Enhanced tables; both review profiles export complete bundles using small synthetic sources. This covers logical retention growth, not real-history review peak memory or all-fourteen-table Enhanced peak memory. Cache values are retained estimates, worker managed heap is sampled before output discard, collected main JS heap excludes workers/native Blobs, and isolated browser-process working/private sets are point samples, not peak memory.' };
        });
        if (args['skip-ui'] !== 'true') await check('worker-startup-failure-ui-fallback-parses-and-downloads', fallbackUi);
    } catch (error) {
        report.error = String(error.stack || error);
        process.exitCode = 1;
    } finally {
        if (page) report.trace = await page.evaluate(() => ({ events: __life?.events, workers: __life?.workers })).catch(() => report.trace);
        save();
        if (context) await context.close();
        if (browser) await browser.close();
        console.log(JSON.stringify({ output, passed: report.checks.filter(check => check.passed).length,
            failed: report.checks.filter(check => !check.passed).length, error: report.error }));
    }
})();
