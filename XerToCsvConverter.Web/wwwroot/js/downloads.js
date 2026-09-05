(function () {
    "use strict";
    const prepared = new Map();

    function discard(id) {
        const item = prepared.get(id);
        if (!item) return;
        prepared.delete(id);
        if (item.url) URL.revokeObjectURL(item.url);
    }

    window.xerDownloads = {
        async prepare(id, filename, contentType, contentStreamReference) {
            if (prepared.has(id)) throw new Error("Download identity is already prepared.");
            const item = { filename, url: null };
            prepared.set(id, item);
            try {
                const arrayBuffer = await contentStreamReference.arrayBuffer();
                // Interop can time out/disconnect while JS is still reading. A finally/discard
                // must invalidate that pending read rather than let it retain a late URL.
                if (prepared.get(id) !== item) return;
                const blob = new Blob([arrayBuffer], { type: contentType });
                item.url = URL.createObjectURL(blob);
            } catch (error) {
                if (prepared.get(id) === item) discard(id);
                throw error;
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
