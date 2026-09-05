// Runs the shipped helper, with browser download surfaces isolated from the machine.
// node --test XerToCsvConverter.Web/tests/downloads.test.cjs
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");

function browser() {
    const clicks = [], revoked = [], timers = [], appended = [], removed = [];
    let nextUrl = 0;
    const context = {
        window: {}, Blob,
        URL: {
            createObjectURL: () => `blob:review-${++nextUrl}`,
            revokeObjectURL: url => revoked.push(url)
        },
        document: {
            body: { appendChild: link => appended.push(link) },
            createElement: () => ({
                click() { clicks.push({ filename: this.download, url: this.href }); },
                remove() { removed.push(this); }
            })
        },
        setTimeout: callback => timers.push(callback)
    };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, "../wwwroot/js/downloads.js"), "utf8"), context);
    return { api: context.window.xerDownloads, clicks, revoked, timers, appended, removed };
}

test("preparing a stream never starts a download; cancellation discards the prepared URL", async () => {
    const b = browser();
    await b.api.prepare("one", "bundle.zip", "application/zip", { arrayBuffer: async () => new Uint8Array([1, 2]).buffer });
    assert.equal(b.clicks.length, 0);
    b.api.discard("one");
    assert.deepEqual(b.revoked, ["blob:review-1"]);
    assert.throws(() => b.api.commit("one"), /no longer exists/);
    b.api.discard("one");
    assert.equal(b.revoked.length, 1);
});

test("handoff is once only and keeps URL alive until the queued browser navigation", async () => {
    const b = browser();
    await b.api.prepare("one", "bundle.zip", "application/zip", { arrayBuffer: async () => new ArrayBuffer(0) });
    b.api.commit("one");
    assert.deepEqual(b.clicks, [{ filename: "bundle.zip", url: "blob:review-1" }]);
    assert.equal(b.appended.length, 1);
    assert.equal(b.removed.length, 1);
    b.api.discard("one");
    assert.equal(b.revoked.length, 0);
    assert.throws(() => b.api.commit("one"), /no longer exists/);
    b.timers.forEach(callback => callback());
    assert.deepEqual(b.revoked, ["blob:review-1"]);
});

test("stream preparation failure creates neither a URL nor a browser download", async () => {
    const b = browser();
    await assert.rejects(b.api.prepare("one", "bundle.zip", "application/zip", {
        arrayBuffer: async () => { throw new Error("Stream failed"); }
    }), /Stream failed/);
    b.api.discard("one");
    assert.equal(b.clicks.length, 0);
    assert.equal(b.revoked.length, 0);
});

test("cleanup during a pending stream prevents a late URL or download from reappearing", async () => {
    const b = browser();
    let finishRead;
    const reading = new Promise(resolve => { finishRead = resolve; });
    const preparing = b.api.prepare("one", "bundle.zip", "application/zip", { arrayBuffer: () => reading });
    b.api.discard("one");
    finishRead(new ArrayBuffer(0));
    await preparing;
    assert.throws(() => b.api.commit("one"), /no longer exists/);
    assert.equal(b.clicks.length, 0);
    assert.equal(b.revoked.length, 0);
});

test("pending preparation reserves its identity and cannot be handed off before ready", async () => {
    const b = browser();
    let finishRead;
    const reading = new Promise(resolve => { finishRead = resolve; });
    const preparing = b.api.prepare("one", "bundle.zip", "application/zip", { arrayBuffer: () => reading });
    await assert.rejects(b.api.prepare("one", "another.zip", "application/zip", { arrayBuffer: async () => new ArrayBuffer(0) }), /already prepared/);
    assert.throws(() => b.api.commit("one"), /not ready/);
    finishRead(new ArrayBuffer(0));
    await preparing;
    b.api.commit("one");
    assert.equal(b.clicks.length, 1);
    b.timers.forEach(callback => callback());
    assert.equal(b.revoked.length, 1);
});
