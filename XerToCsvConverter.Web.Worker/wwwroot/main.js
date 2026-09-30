const version = '__APP_VERSION__';
const files = new Map();
const acknowledgements = new Map();
let host;
let activeJob;
let inputTimings;

try {
    const { dotnet } = await import('./_framework/dotnet.js?v=' + encodeURIComponent(version));
    const { readImmutableFile } = await import('./fileInput.js?v=' + encodeURIComponent(version));
    const runtime = await dotnet.withDiagnosticTracing(false).create();
    runtime.setModuleImports('worker-bridge', {
        async fillFile(token, destination) {
            const file = files.get(token);
            if (!file) throw new Error('This browser source is no longer registered.');
            try {
                const measured = await readImmutableFile(file, destination);
                inputTimings.browserFileReadMs += measured.browserFileReadMs;
                inputTimings.webCryptoSha256Ms += measured.webCryptoSha256Ms;
                return measured.hash;
            } finally {
                destination.dispose();
            }
        },
        progress(jobId, json) {
            self.postMessage({ kind: 'progress', jobId, progress: JSON.parse(json) });
        },
        resultMetrics(jobId, resultSerializationMs, resultJsonBytes) {
            self.postMessage({ kind: 'result-metrics', jobId, resultSerializationMs, resultJsonBytes, ...inputTimings });
        },
        chunk(jobId, sequence, view) {
            const bytes = view.slice();
            self.postMessage({ kind: 'chunk', jobId, sequence, bytes: bytes.buffer }, [bytes.buffer]);
        },
        acknowledgement(jobId, sequence) {
            return new Promise((resolve, reject) => acknowledgements.set(`${jobId}:${sequence}`, { resolve, reject }));
        }
    });
    const exports = await runtime.getAssemblyExports(runtime.getConfig().mainAssemblyName);
    host = exports.XerToCsvConverter.Web.Worker.WorkerHost;
    await runtime.runMain(runtime.getConfig().mainAssemblyName, []);
    self.onmessage = async ({ data }) => {
        if (data.kind === 'ack') {
            const key = `${data.jobId}:${data.sequence}`;
            acknowledgements.get(key)?.resolve();
            acknowledgements.delete(key);
            return;
        }
        if (data.kind === 'cancel') { if (activeJob === data.jobId) host.Cancel(); return; }
        if (data.kind === 'remove') { files.delete(data.token); host.Remove(data.token); return; }
        if (data.kind === 'invalidate') { host.Invalidate(); return; }
        if (data.kind !== 'run') return;
        if (activeJob) {
            self.postMessage({ kind: 'error', jobId: data.job.jobId, message: 'Another worker request is active.' });
            return;
        }
        activeJob = data.job.jobId;
        inputTimings = { browserFileReadMs: 0, webCryptoSha256Ms: 0 };
        try {
            const retained = new Set(data.job.sources.map(source => source.sourceToken));
            for (const token of files.keys()) if (!retained.has(token)) files.delete(token);
            for (const { token, file } of data.files ?? []) files.set(token, file);
            const json = await host.Run(JSON.stringify(data.job));
            self.postMessage({ kind: 'result', jobId: activeJob, result: JSON.parse(json) });
        } catch (error) {
            self.postMessage({ kind: 'error', jobId: activeJob, message: String(error?.message ?? error) });
        } finally {
            for (const [key, pending] of acknowledgements) {
                if (key.startsWith(`${activeJob}:`)) { pending.reject(new Error('Processing ended.')); acknowledgements.delete(key); }
            }
            activeJob = undefined;
            inputTimings = undefined;
        }
    };
    self.postMessage({ kind: 'ready', protocol: 1, version });
} catch (error) {
    self.postMessage({ kind: 'startup-error', message: String(error?.message ?? error) });
}
