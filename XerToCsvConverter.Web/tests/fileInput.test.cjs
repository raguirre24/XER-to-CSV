const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createHash, webcrypto } = require('node:crypto');
const { test } = require('node:test');

const source = fs.readFileSync(path.join(__dirname, '../../XerToCsvConverter.Web.Worker/wwwroot/fileInput.js'), 'utf8');
const helper = import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);

test('the immutable file is read once, and the exact copied ArrayBuffer is hashed with separate read and SHA timings', async () => {
    const { readImmutableFile } = await helper;
    const bytes = Uint8Array.from([0, 1, 3, 128, 254, 255]);
    const destination = new Uint8Array(bytes.length);
    let now = 0, reads = 0, digests = 0;
    const file = Object.freeze({ async arrayBuffer() { reads++; now += 12; return bytes.buffer; } });
    const crypto = { subtle: { async digest(algorithm, buffer) {
        assert.equal(algorithm, 'SHA-256');
        assert.equal(buffer, bytes.buffer);
        assert.deepEqual(destination, bytes);
        digests++;
        now += 3;
        return webcrypto.subtle.digest(algorithm, buffer);
    } } };
    const result = await readImmutableFile(file, destination, crypto, () => now);
    assert.deepEqual(result, { hash: createHash('sha256').update(bytes).digest('hex'), browserFileReadMs: 12, webCryptoSha256Ms: 3 });
    assert.equal(reads, 1);
    assert.equal(digests, 1);
    assert.deepEqual(destination, bytes);
    assert.equal(Object.values(result).includes(bytes.buffer), false);
});

test('missing or rejected WebCrypto returns a null digest while preserving the copied bytes for managed fallback', async () => {
    const { readImmutableFile } = await helper;
    const bytes = Uint8Array.from([9, 8, 7]);
    for (const crypto of [null, {}, { subtle: { async digest() { throw new Error('Crypto unavailable'); } } }]) {
        const destination = new Uint8Array(bytes.length);
        const result = await readImmutableFile({ arrayBuffer: async () => bytes.buffer }, destination, crypto);
        assert.equal(result.hash, null);
        assert.deepEqual(destination, bytes);
        assert.equal(Number.isFinite(result.browserFileReadMs), true);
        assert.equal(Number.isFinite(result.webCryptoSha256Ms), true);
    }
});

test('read and length failures do not start hashing or return a partial digest', async () => {
    const { readImmutableFile } = await helper;
    let digests = 0;
    const crypto = { subtle: { digest() { digests++; throw new Error('Unexpected hash'); } } };
    await assert.rejects(readImmutableFile({ arrayBuffer: async () => { throw new Error('Read failed'); } }, new Uint8Array(1), crypto), /Read failed/);
    await assert.rejects(readImmutableFile({ arrayBuffer: async () => new ArrayBuffer(2) }, new Uint8Array(1), crypto), /length changed/);
    assert.equal(digests, 0);
});
