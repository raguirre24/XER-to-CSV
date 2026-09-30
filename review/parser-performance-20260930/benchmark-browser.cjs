const { chromium } = require('./browser-dependencies.cjs').dependency('playwright');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve('artifacts/parser-performance-20260930');
const primary = path.resolve('XER Sample/2608-EBA_PAA_8.0 (draft).xer');
const second = 'C:/Users/ricar/Downloads/2607 C5064-C-2607.xer';
const modes = ['legacy', 'programme', 'tender'];
const channels = (process.env.BROWSERS || 'chrome,msedge').split(',');
const iterations = Number(process.env.PAIRS || 5);
const selectedModes = (process.env.MODES || modes.join(',')).split(',');
const variants = (process.env.VARIANTS || 'baseline,candidate').split(',');
const reportFile = path.join(root, process.env.REPORT_FILE || 'browser-benchmark.json');
const report = fs.existsSync(reportFile) && process.env.APPEND ? JSON.parse(fs.readFileSync(reportFile)) : { createdAt: new Date().toISOString(), baseline: '90b98d3', runtime: '.NET SDK 8.0.421 / WASM 8.0.27 / Release interpreter, identical compression', runs: [] };
function save() { fs.writeFileSync(reportFile, JSON.stringify(report, null, 2)); }
async function prepare(browser, variant, mode) {
  const page = await browser.newPage({ acceptDownloads: true });
  page.setDefaultTimeout(120000);
  const errors = [];
  page.on('pageerror', e => errors.push(e.message));
  page.on('console', m => { if (m.type() === 'error' && !m.text().includes('404')) errors.push(m.text()); });
  await page.addInitScript(() => {
    window.__jobs = [];
    const NativeWorker = window.Worker;
    window.Worker = class extends NativeWorker {
      postMessage(value, ...rest) {
        // Each iteration removes its previous sources and registers new occurrence
        // tokens. The runtime is warm; the normal 256 MiB cache still has to admit
        // this fresh parse, and cannot supply a hit for the new occurrence.
        if (value.kind === 'run') {
          window.__jobs.push(JSON.parse(JSON.stringify(value.job)));
        }
        return super.postMessage(value, ...rest);
      }
    };
    window.__gaps = [];
    let previous = performance.now();
    setInterval(() => { const now = performance.now(); window.__gaps.push(now - previous); previous = now; if (window.__gaps.length > 20000) window.__gaps.shift(); }, 50);
  });
  const startup = performance.now();
  await page.goto(variant === 'baseline' && process.env.BASELINE_URL ? process.env.BASELINE_URL
    : `http://127.0.0.1:${variant === 'baseline' ? 8766 : 8767}/XER-to-CSV/`);
  const fileInput = page.locator('input[type=file]');
  await fileInput.waitFor();
  const uiStartupMs = performance.now() - startup;
  if (mode !== 'legacy') {
    await page.locator('.profile-option').filter({ hasText: mode === 'programme' ? 'Programme Review' : 'Tender Review' }).locator('input').check();
    await page.getByPlaceholder(mode === 'programme' ? 'Example: QAC000623-01-02' : 'Example: QAC000623', { exact: true }).fill('PERFORMANCE');
    await page.getByPlaceholder('Governed project name').fill('Parser performance verification');
    if (mode === 'programme') await page.locator('.form-grid select').selectOption('C');
    await page.locator('body').click({ position: { x: 5, y: 5 } });
  } else await page.locator('#chkPowerBi').check();
  return { page, variant, mode, errors, uiStartupMs, fileInput };
}
async function workload(state, paths, iteration, warmup = false) {
  const { page, variant, mode } = state;
  // Remove preserves the initialized worker/runtime. Explicit reset is tested separately.
  while (await page.locator('.file-item').count()) await page.locator('.btn-remove').first().click();
  await state.fileInput.waitFor({ state: 'attached' });
  await page.waitForFunction(() => !document.querySelector('input[type=file]').disabled);
  await page.evaluate(() => { window.__gaps.length = 0; window.xerProcessingMetrics = []; window.__frozenParserPhaseRuns = []; window.__jobs.length = 0; });
  const started = performance.now();
  await state.fileInput.evaluate(input => { input.value = ''; });
  await state.fileInput.setInputFiles(paths);
  try { await page.waitForFunction(count => document.querySelectorAll('.file-item').length === count && !document.querySelector('input[type=file]').disabled, paths.length); }
  catch (error) { console.log('UPLOAD FAILURE', variant, mode, await page.locator('body').innerText(), state.errors); throw error; }
  const uploadMs = performance.now() - started;
  if (mode === 'programme') {
    const rows = page.locator('.snapshot-table tbody tr:has(input)');
    for (let i = 0; i < paths.length; i++) {
      const row = rows.nth(i);
      await row.locator('select').selectOption(i === 0 ? 'Baseline' : 'Update');
      await row.locator('input').nth(0).fill(i === 0 ? 'BL01' : '2608');
      const monthInput = row.locator('input').nth(1);
      const monthDate = i === 0 ? '2026-07-01' : '2026-08-01';
      await monthInput.fill(await monthInput.getAttribute('type') === 'month' ? monthDate.slice(0, 7) : monthDate);
      await row.locator('input').nth(2).fill(i === 0 && paths.length > 1 ? '2026-07-31' : '2026-08-25');
    }
    await page.locator('body').click({ position: { x: 5, y: 5 } });
  }
  if (mode === 'tender') {
    const inputs = page.locator('.tender-stage-table tbody input');
    for (let i = 0; i < paths.length; i++) await inputs.nth(i).fill(i === 0 ? '2026-07-31' : '2026-08-31');
    await page.locator('body').click({ position: { x: 5, y: 5 } });
  }
  let parseMs = 0;
  const processingStart = performance.now();
  if (mode === 'legacy') {
    await page.getByRole('button', { name: 'Parse and load tables from uploaded XER files', exact: true }).click();
    await page.getByRole('button', { name: 'Export Power BI as ZIP', exact: true }).waitFor();
    await page.waitForFunction(() => ![...document.querySelectorAll('button')].find(b => b.innerText === 'Export Power BI as ZIP')?.disabled);
    parseMs = performance.now() - processingStart;
  }
  const downloadPromise = page.waitForEvent('download');
  await page.getByRole('button', { name: mode === 'legacy' ? 'Export Power BI as ZIP' : `Validate and download ${mode === 'programme' ? 'Programme' : 'Tender'} Review bundle`, exact: true }).click();
  const download = await downloadPromise;
  const processingMs = performance.now() - processingStart;
  const totalMs = performance.now() - started;
  const evidence = await page.evaluate(() => ({ metrics: window.xerProcessingMetrics || [], observerPhases: window.__frozenParserPhaseRuns || [], gaps: window.__gaps, jobs: window.__jobs }));
  if (variant === 'candidate') {
    const processing = evidence.metrics.filter(metric => metric.kind !== 'metadata');
    if (!processing.length || processing.some(metric => metric.cacheHits !== 0)
      || processing.reduce((total, metric) => total + metric.parsedSources, 0) !== paths.length)
      throw new Error('A primary fresh-parse workload did not parse every new source occurrence.');
    if (processing.some(metric => metric.cacheBudgetBytes !== 256 * 1024 * 1024))
      throw new Error('A primary workload did not use the normal retained-cache budget.');
  }
  const output = path.join(root, process.env.ZIP_PREFIX ? 'browser-zips-' + process.env.ZIP_PREFIX : 'browser-zips',
    state.channel, mode, `${variant}-${warmup ? 'warmup' : iteration}.zip`);
  fs.mkdirSync(path.dirname(output), { recursive: true });
  await download.saveAs(output);
  const run = { browser: state.channel, browserVersion: state.browserVersion, variant, mode, iteration, warmup, files: paths.map(p => path.basename(p)), uiStartupMs: state.uiStartupMs,
    uploadMs, parseMs, processingMs, totalMs, maxPageTimerGapMs: Math.max(...evidence.gaps), workerMetrics: evidence.metrics,
    observerPhases: evidence.observerPhases, errors: state.errors.slice(), zip: output };
  console.log(JSON.stringify({ browser: run.browser, variant, mode, iteration, warmup, upload: Math.round(uploadMs), processing: Math.round(processingMs), total: Math.round(totalMs), maxGap: Math.round(run.maxPageTimerGapMs), errors: run.errors }));
  report.runs.push(run); save();
  await page.waitForFunction(() => !document.querySelector('input[type=file]').disabled);
  return run;
}
(async () => {
  for (const channel of channels) {
    const browser = await chromium.launch({ channel, headless: true });
    try {
      for (const mode of selectedModes) {
        const states = {};
        for (const variant of variants) {
          states[variant] = await prepare(browser, variant, mode);
          Object.assign(states[variant], { channel, browserVersion: browser.version() });
          await workload(states[variant], [primary], -1, true);
        }
        for (let pair = 0; pair < iterations; pair++) {
          for (const variant of (pair % 2 ? ['candidate', 'baseline'] : ['baseline', 'candidate']).filter(variant => variants.includes(variant))) await workload(states[variant], [primary], pair);
        }
        if (process.env.HISTORY === '1') {
          for (const variant of variants) await workload(states[variant], mode === 'legacy' ? [second] : [second, primary], 'second-history');
        }
        for (const state of Object.values(states)) await state.page.close();
      }
    } finally { await browser.close(); }
  }
})().catch(e => { console.error(e); save(); process.exit(1); });
