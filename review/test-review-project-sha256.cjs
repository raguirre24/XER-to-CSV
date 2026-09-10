/*
 * Independent arithmetic check for the review loaders' small pure-M SHA-256.
 * Constants and byte-order lists are read from each real helper. The translated
 * Number arithmetic is compared with Node's native cryptographic implementation.
 * This is NOT execution of M or proof of Power BI refresh compatibility.
 * Usage: node review/test-review-project-sha256.cjs [helper-or-expressions ...]
 */
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const defaultPaths = [
    'review/ReviewProjectFileToken.pq',
    'C:/Users/ricar/Documents/Code/Programme Review/Programme-Review/Project Review - Programme (datalake).SemanticModel/definition/expressions.tmdl',
    'C:/Users/ricar/Documents/Code/Tender-Review/Project Review - Tender Programme (datalake).SemanticModel/definition/expressions.tmdl',
];
const targets = process.argv.slice(2).length ? process.argv.slice(2) : defaultPaths;
const vectors = [
    ['', 'empty'], ['abc', 'abc'],
    ...[55, 56, 63, 64, 100, 101, 260].map(n => ['A'.repeat(n), `${n} ASCII bytes`]),
    ['QAC000623-01-02', 'reported project'],
    ['NORTH, SOUTH / P:01 | 100%', 'punctuation'],
    ['MĀORI 工程 🚧', 'Unicode mixed UTF-8'],
    ['é'.repeat(28), '56 bytes Unicode'],
    ['工程🚧'.repeat(26), '260 bytes Unicode'],
    ['NORTH  SOUTH', 'repeated internal spaces'],
    ['ßẞİı', 'Unicode casing inputs preserved by hash'],
];

function extractHelper(file) {
    const text = fs.readFileSync(file, 'utf8');
    const start = text.indexOf('fnXerCsvSha256 =');
    assert.ok(start >= 0, `${file}: SHA helper exists`);
    const end = text.indexOf('fnXerCsvProjectFileToken =', start);
    assert.ok(end > start, `${file}: file-token helper follows SHA`);
    const helper = text.slice(start, end);
    const list = pattern => {
        const match = helper.match(pattern);
        assert.ok(match, `${file}: expected source list ${pattern}`);
        return match[1].split(',').map(x => Number(x.trim()));
    };
    const constants = list(/Constants\s*=\s*List\.Buffer\(\{([\s\S]*?)\}\)/);
    const initial = list(/Initial\s*=\s*\{([^}]+)\}/);
    const lengthOrder = list(/LengthBytes\s*=\s*List\.Transform\(\{([^}]+)\}/);
    const digestOrder = list(/DigestBytes\s*=.*List\.Transform\(\{([^}]+)\}/);
    assert.equal(constants.length, 64);
    assert.equal(initial.length, 8);
    assert.deepEqual(lengthOrder, [7, 6, 5, 4, 3, 2, 1, 0], `${file}: explicit length byte order`);
    assert.deepEqual(digestOrder, [3, 2, 1, 0], `${file}: explicit digest byte order`);
    assert.ok(!/\{\s*(7|3)\s*\.\.\s*0\s*\}/.test(helper), `${file}: no descending-range dependence`);
    for (const formula of [
        'Number.Mod(56 - Number.Mod(List.Count(Bytes) + 1, 64) + 64, 64)',
        '4294967295 - h{4}',
        'Small1(w{i - 2}) + w{i - 7} + Small0(w{i - 15}) + w{i - 16}',
        'Mod32(hash{_} + Compressed{_})',
    ]) assert.ok(helper.includes(formula), `${file}: reviewed arithmetic formula retained: ${formula}`);
    return { constants, initial, lengthOrder, digestOrder };
}

function translatedArithmetic(value, { constants, initial, lengthOrder, digestOrder }) {
    const bytes = [...Buffer.from(value, 'utf8')];
    const bitLength = bytes.length * 8;
    const padding = (56 - ((bytes.length + 1) % 64) + 64) % 64;
    const message = [...bytes, 128, ...Array(padding).fill(0),
        ...lengthOrder.map(i => Math.floor(bitLength / 256 ** i) % 256)];
    const mod32 = n => n % 4294967296;
    const rotate = (n, bits) => Math.floor(n / 2 ** bits) + (n % 2 ** bits) * 2 ** (32 - bits);
    // BigInt bitwise operations emulate M's nonnegative integer words without
    // JavaScript's signed 32-bit coercion.
    const xor = (a, b) => Number(BigInt(a) ^ BigInt(b));
    const and = (a, b) => Number(BigInt(a) & BigInt(b));
    const xor3 = (a, b, c) => xor(xor(a, b), c);
    const small0 = n => xor3(rotate(n, 7), rotate(n, 18), Math.floor(n / 8));
    const small1 = n => xor3(rotate(n, 17), rotate(n, 19), Math.floor(n / 1024));
    const large0 = n => xor3(rotate(n, 2), rotate(n, 13), rotate(n, 22));
    const large1 = n => xor3(rotate(n, 6), rotate(n, 11), rotate(n, 25));
    let hash = [...initial];
    for (let block = 0; block < message.length / 64; ++block) {
        const words = Array.from({ length: 16 }, (_, i) =>
            [0, 1, 2, 3].reduce((sum, j) => sum + message[block * 64 + i * 4 + j] * 256 ** (3 - j), 0));
        for (let i = 16; i < 64; ++i)
            words.push(mod32(small1(words[i - 2]) + words[i - 7] + small0(words[i - 15]) + words[i - 16]));
        let h = [...hash];
        for (let i = 0; i < 64; ++i) {
            const choose = xor(and(h[4], h[5]), and(4294967295 - h[4], h[6]));
            const majority = xor3(and(h[0], h[1]), and(h[0], h[2]), and(h[1], h[2]));
            const t1 = mod32(h[7] + large1(h[4]) + choose + constants[i] + words[i]);
            const t2 = mod32(large0(h[0]) + majority);
            h = [mod32(t1 + t2), h[0], h[1], h[2], mod32(h[3] + t1), h[4], h[5], h[6]];
        }
        hash = hash.map((n, i) => mod32(n + h[i]));
    }
    return Buffer.from(hash.flatMap(word => digestOrder.map(i => Math.floor(word / 256 ** i) % 256))).toString('hex').toUpperCase();
}

for (const target of targets) {
    const helper = extractHelper(target);
    for (const [value, name] of vectors) {
        const expected = crypto.createHash('sha256').update(value, 'utf8').digest('hex').toUpperCase();
        assert.equal(translatedArithmetic(value, helper), expected, `${target}: ${name}`);
    }
    console.log(`PASS ${vectors.length} native-SHA256 arithmetic vectors: ${path.resolve(target)}`);
}
console.log('Scope: constants/byte order and translated Number arithmetic checked; native M/Power BI execution was not performed.');
