// The observer baseline is a separate package used only for phase attribution.
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(process.argv[2] || 'artifacts/parser-performance-20260930');
const mime = { '.html': 'text/html', '.js': 'text/javascript', '.json': 'application/json', '.wasm': 'application/wasm', '.css': 'text/css', '.svg': 'image/svg+xml', '.dat': 'application/octet-stream', '.dll': 'application/octet-stream', '.woff2': 'font/woff2' };
for (const [variant, port] of [['baseline', 8766], ['candidate', 8767], ['baseline-observer', 8768]]) {
  if (process.argv[3] && variant !== process.argv[3]) continue;
  const web = path.join(root, variant + '-web', 'wwwroot');
  http.createServer((req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const prefix = '/XER-to-CSV/';
    if (!url.pathname.startsWith(prefix)) { res.writeHead(404); return res.end(); }
    const local = decodeURIComponent(url.pathname.slice(prefix.length)) || 'index.html';
    const file = path.resolve(web, local);
    if (!file.startsWith(web + path.sep) || !fs.existsSync(file) || !fs.statSync(file).isFile()) { res.writeHead(404); return res.end(); }
    res.setHeader('Content-Type', mime[path.extname(file)] || 'application/octet-stream');
    res.setHeader('Cache-Control', 'no-store');
    if (local === 'index.html' || local === 'worker/main.js') {
      let text = fs.readFileSync(file, 'utf8').replaceAll('__APP_VERSION__', 'performance-' + variant);
      if (local === 'index.html') text = text.replace('<base href="/" />', '<base href="/XER-to-CSV/" />');
      res.end(text);
    } else fs.createReadStream(file).pipe(res);
  }).listen(port, '127.0.0.1', () => console.log(`${variant}: http://127.0.0.1:${port}/XER-to-CSV/`));
}
