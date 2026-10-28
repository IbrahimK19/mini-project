let fileChunks = new Map();
let totalChunksExpected = 0;
let totalFileSize = 0;

self.addEventListener('install', (e) => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

self.addEventListener('message', (event) => {
    const msg = event.data;
    if (msg.type === 'INIT') {
        fileChunks.clear();
        totalChunksExpected = msg.totalChunks;
        totalFileSize = msg.totalSize || (totalChunksExpected * 64 * 1024);
    } else if (msg.type === 'CHUNK') {
        fileChunks.set(msg.index, msg.chunk);
    }
});

self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);
    if (url.pathname === '/stream') {
        const rangeHeader = event.request.headers.get('range');
        
        event.respondWith(new Promise(async (resolve) => {
            // Wait lazily until chunk 0 arrives from the swarm
            while (!fileChunks.has(0) && totalChunksExpected > 0) {
                await new Promise(r => setTimeout(r, 50));
            }

            let start = 0;
            let end = totalFileSize - 1;

            if (rangeHeader) {
                const parts = rangeHeader.replace(/bytes=/, "").split("-");
                start = parseInt(parts[0], 10);
                if (parts[1]) {
                    end = parseInt(parts[1], 10);
                }
            }

            const CHUNK_SIZE = 64 * 1024;
            const stream = new ReadableStream({
                async start(controller) {
                    let currentByte = start;
                    while (currentByte <= end) {
                        const chunkIndex = Math.floor(currentByte / CHUNK_SIZE);
                        const byteOffsetInChunk = currentByte % CHUNK_SIZE;

                        while (!fileChunks.has(chunkIndex)) {
                            if (totalChunksExpected > 0 && chunkIndex >= totalChunksExpected) break;
                            await new Promise(r => setTimeout(r, 50));
                        }

                        if (fileChunks.has(chunkIndex)) {
                            const chunkBuffer = fileChunks.get(chunkIndex);
                            const view = new Uint8Array(chunkBuffer, byteOffsetInChunk);
                            controller.enqueue(view);
                            currentByte += view.byteLength;
                        } else {
                            break;
                        }
                    }
                    controller.close();
                }
            });

            const headers = {
                'Content-Type': 'video/mp4',
                'Accept-Ranges': 'bytes',
                'Content-Range': `bytes ${start}-${end}/${totalFileSize}`,
                'Content-Length': (end - start + 1)
            };

            resolve(new Response(stream, {
                status: rangeHeader ? 206 : 200,
                headers: headers
            }));
        }));
    }
});