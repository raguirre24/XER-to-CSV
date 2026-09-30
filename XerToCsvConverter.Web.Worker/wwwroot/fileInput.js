// This buffer exists only while one immutable File is copied and hashed. The
// caller keeps the digest and timings; no JavaScript input buffer is cached.
export async function readImmutableFile(file, destination, cryptoProvider = globalThis.crypto, now = () => performance.now()) {
    const readingStarted = now();
    const bytes = await file.arrayBuffer();
    if (bytes.byteLength !== destination.length) throw new Error('Browser file length changed during reading.');
    destination.set(new Uint8Array(bytes), 0);
    const browserFileReadMs = now() - readingStarted;
    let hash = null;
    let webCryptoSha256Ms = 0;
    if (cryptoProvider?.subtle?.digest) {
        const hashStarted = now();
        try {
            const digest = new Uint8Array(await cryptoProvider.subtle.digest('SHA-256', bytes));
            hash = Array.from(digest, value => value.toString(16).padStart(2, '0')).join('');
        } catch {
            // Unsupported/blocked WebCrypto falls back to the existing managed SHA.
        } finally {
            webCryptoSha256Ms = now() - hashStarted;
        }
    }
    return { hash, browserFileReadMs, webCryptoSha256Ms };
}
