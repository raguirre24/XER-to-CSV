// Runs the shipped helper without creating downloads on this machine.
// node --test XerToCsvConverter.Web/tests/downloads.test.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

function browser(applicationVersion = 'release-a') {
    const created = [], clicks = [], revoked = [], timers = [], appended = [], removed = [];
    const meta = applicationVersion === null ? null : { content: applicationVersion };
    const context = {
        window: {}, Blob,
        URL: {
            createObjectURL(blob) { created.push(blob); return 'blob:review-' + created.length; },
            revokeObjectURL: url => revoked.push(url)
        },
        document: {
            querySelector: selector => selector === 'meta[name="application-version"]' ? meta : null,
            body: { appendChild: link => appended.push(link) },
            createElement: () => ({
                click() { clicks.push({ filename: this.download, url: this.href }); },
                remove() { removed.push(this); }
            })
        },
        setTimeout: callback => timers.push(callback)
    };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../wwwroot/js/downloads.js'), 'utf8'), context);
    return { api: context.window.xerDownloads, meta, created, clicks, revoked, timers, appended, removed };
}

function reference(...chunks) {
    const readable = new ReadableStream({ start(controller) {
        for (const chunk of chunks) controller.enqueue(new Uint8Array(chunk));
        controller.close();
    } });
    return { readable, stream: async () => readable };
}

test('application version follows the current page metadata and supports unstamped local previews', () => {
    const b = browser('release-a');
    assert.equal(b.api.applicationVersion(), 'release-a');
    b.meta.content = 'release-b';
    assert.equal(b.api.applicationVersion(), 'release-b');
    assert.equal(browser(null).api.applicationVersion(), '__APP_VERSION__');
});

test('chunked preparation preserves bytes and hands off once before delayed URL revocation', async () => {
    const b = browser();
    const source = reference([0, 1, 2], [3, 255]);
    await b.api.prepare('one', 'bundle.zip', 'application/zip', source);
    assert.equal(source.readable.locked, false);
    assert.deepEqual(Array.from(new Uint8Array(await b.created[0].arrayBuffer())), [0, 1, 2, 3, 255]);
    assert.equal(b.created[0].type, 'application/zip');
    assert.equal(b.clicks.length, 0);
    b.api.commit('one');
    assert.deepEqual(b.clicks, [{ filename: 'bundle.zip', url: 'blob:review-1' }]);
    assert.equal(b.appended.length, 1);
    assert.equal(b.removed.length, 1);
    assert.equal(b.revoked.length, 0);
    b.api.discard('one');
    assert.throws(() => b.api.commit('one'), /no longer exists/);
    b.timers.forEach(callback => callback());
    assert.deepEqual(b.revoked, ['blob:review-1']);
});

test('discard after preparation revokes its URL once and prevents browser handoff', async () => {
    const b = browser();
    await b.api.prepare('one', 'bundle.zip', 'application/zip', reference());
    b.api.discard('one');
    b.api.discard('one');
    assert.deepEqual(b.revoked, ['blob:review-1']);
    assert.equal(b.clicks.length, 0);
    assert.throws(() => b.api.commit('one'), /no longer exists/);
});

test('opening a failed stream creates neither a URL nor a browser download', async () => {
    const b = browser();
    await assert.rejects(b.api.prepare('one', 'bundle.zip', 'application/zip', {
        stream: async () => { throw new Error('Stream failed'); }
    }), /Stream failed/);
    b.api.discard('one');
    assert.equal(b.created.length, 0);
    assert.equal(b.clicks.length, 0);
    assert.equal(b.revoked.length, 0);
    assert.throws(() => b.api.commit('one'), /no longer exists/);
});

test('a failed read cleans up and releases its reader lock', async () => {
    const b = browser();
    const readable = new ReadableStream({ start(controller) { controller.error(new Error('Read failed')); } });
    await assert.rejects(b.api.prepare('one', 'bundle.zip', 'application/zip', { stream: async () => readable }), /Read failed/);
    assert.equal(readable.locked, false);
    assert.equal(b.created.length, 0);
    assert.equal(b.clicks.length, 0);
    assert.throws(() => b.api.commit('one'), /no longer exists/);
});

test('discard while reading drains and unlocks without retaining a late URL or cancelling the transfer', async () => {
    const b = browser();
    let controller;
    let cancelled = false;
    const readable = new ReadableStream({
        start(value) { controller = value; controller.enqueue(new Uint8Array([1])); },
        cancel() { cancelled = true; }
    });
    const preparing = b.api.prepare('one', 'bundle.zip', 'application/zip', { stream: async () => readable });
    await new Promise(resolve => setImmediate(resolve));
    b.api.discard('one');
    controller.enqueue(new Uint8Array([2, 3]));
    controller.close();
    await preparing;
    assert.equal(cancelled, false);
    assert.equal(readable.locked, false);
    assert.equal(b.created.length, 0);
    assert.equal(b.clicks.length, 0);
    assert.throws(() => b.api.commit('one'), /no longer exists/);
});

test('pending preparation reserves its identity and cannot be handed off before ready', async () => {
    const b = browser();
    let controller;
    const readable = new ReadableStream({ start(value) { controller = value; } });
    const preparing = b.api.prepare('one', 'bundle.zip', 'application/zip', { stream: async () => readable });
    await assert.rejects(b.api.prepare('one', 'another.zip', 'application/zip', reference()), /already prepared/);
    assert.throws(() => b.api.commit('one'), /not ready/);
    controller.close();
    await preparing;
    b.api.commit('one');
    assert.equal(b.clicks.length, 1);
    b.timers.forEach(callback => callback());
    assert.equal(b.revoked.length, 1);
});

test('discard before opening a stream prevents its late completion from replacing a newer preparation', async () => {
    const b = browser();
    let open;
    const opening = new Promise(resolve => { open = resolve; });
    const preparing = b.api.prepare('one', 'old.zip', 'application/zip', { stream: () => opening });
    b.api.discard('one');
    await b.api.prepare('one', 'new.zip', 'application/zip', reference([9]));
    open(reference([1, 2]).readable);
    await preparing;
    assert.equal(b.created.length, 1);
    assert.deepEqual(Array.from(new Uint8Array(await b.created[0].arrayBuffer())), [9]);
    b.api.commit('one');
    assert.deepEqual(b.clicks, [{ filename: 'new.zip', url: 'blob:review-1' }]);
});
