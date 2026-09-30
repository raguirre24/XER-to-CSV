(function () {
    "use strict";
    const prepared = new Map();

    function discard(id) {
        const item = prepared.get(id);
        if (!item) return;
        prepared.delete(id);
        item.chunks.length = 0;
        if (item.url) URL.revokeObjectURL(item.url);
    }

    window.xerDownloads = {
        applicationVersion() {
            return document.querySelector('meta[name="application-version"]')?.content ?? '__APP_VERSION__';
        },
        async prepare(id, filename, contentType, contentStreamReference) {
            if (prepared.has(id)) throw new Error("Download identity is already prepared.");
            const item = { filename, url: null, chunks: [] };
            prepared.set(id, item);
            let reader;
            try {
                // Avoid an additional contiguous JavaScript arrayBuffer allocation while
                // reading the .NET ZIP. The complete compressed download is still buffered.
                const readable = await contentStreamReference.stream();
                reader = readable.getReader();
                for (;;) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    // Keep draining after discard so .NET can complete its stream transfer,
                    // but stop retaining chunks for a preparation that can no longer commit.
                    if (prepared.get(id) === item) item.chunks.push(value);
                }
                // Interop can time out/disconnect while JS is still reading. A finally/discard
                // must invalidate that pending read rather than let it retain a late URL.
                if (prepared.get(id) !== item) return;
                const blob = new Blob(item.chunks, { type: contentType });
                item.chunks.length = 0;
                item.url = URL.createObjectURL(blob);
            } catch (error) {
                if (prepared.get(id) === item) discard(id);
                throw error;
            } finally {
                if (reader) reader.releaseLock();
            }
        },
        commit(id) {
            const item = prepared.get(id);
            if (!item) throw new Error("The prepared download no longer exists.");
            if (!item.url) throw new Error("The download stream is not ready.");
            prepared.delete(id);
            const link = document.createElement("a");
            try {
                link.href = item.url;
                link.download = item.filename;
                document.body.appendChild(link);
                link.click();
            } finally {
                link.remove();
                // Do not revoke before the browser has consumed the click navigation.
                setTimeout(() => URL.revokeObjectURL(item.url), 1000);
            }
        },
        discard
    };
})();
