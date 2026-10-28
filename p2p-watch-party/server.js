const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Serve static files from the public folder (includes index.html, app.js, sw.js)
app.use(express.static(path.join(__dirname, 'public')));

let clients = new Set();

wss.on('connection', (ws) => {
    const clientId = Math.random().toString(36).substring(2, 9);
    ws.clientId = clientId;
    clients.add(ws);

    // Send client their unique ID
    ws.send(JSON.stringify({ type: 'WELCOME', clientId }));

    // Broadcast updated peer list to the swarm
    broadcastPeerList();

    ws.on('message', (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch (e) {
            return;
        }

        // Forward WebRTC signaling offers/answers/ICE candidates to the target peer
        if (data.target && data.payload) {
            for (const client of clients) {
                if (client.clientId === data.target && client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify({ from: clientId, payload: data.payload }));
                    break;
                }
            }
        }
    });

    ws.on('close', () => {
        clients.delete(ws);
        broadcastPeerList();
    });
});

function broadcastPeerList() {
    const peers = Array.from(clients).map(c => c.clientId);
    const msg = JSON.stringify({ type: 'PEER_LIST', peers });
    for (const client of clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(msg);
        }
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`P2P Watch Party Server running on http://localhost:${PORT}`);
});