const protocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
const socket = new WebSocket(`${protocol}${window.location.host}`);

const videoElement = document.getElementById('localVideo');
const loaderDiv = document.getElementById('videoLoader');
const loaderText = document.getElementById('loaderText');
const roleSelectionDiv = document.getElementById('roleSelection');
const hostControlsDiv = document.getElementById('hostControls');
const guestControlsDiv = document.getElementById('guestControls');
const peerCountSpan = document.getElementById('peerCount');
const guestStatusLabel = document.getElementById('guestStatusLabel');

const CHUNK_SIZE = 64 * 1024; // 64 KB chunks
const iceServers = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        {
            urls: 'turn:openrelay.metered.ca:80',
            username: 'openrelayproject',
            credential: 'openrelayproject'
        }
    ]
};

let myId = null;
let isHost = false;
let fileChunks = []; 
let totalChunksExpected = 0;
let fileMetadata = null;

const swarmPeers = new Map();
const myChunks = new Set();

// Register Service Worker with root scope
if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js', { scope: '/' })
        .then(() => console.log('Service Worker registered successfully.'))
        .catch(err => console.error('Service Worker registration failed:', err));
}

// Fixed: Safely capture resolved registration from ready promise
async function sendToSW(message) {
    const registration = await navigator.serviceWorker.ready;
    if (navigator.serviceWorker.controller) {
        navigator.serviceWorker.controller.postMessage(message);
    } else if (registration && registration.active) {
        registration.active.postMessage(message);
    }
}

socket.onopen = () => console.log('Connected to swarm signaling server.');

socket.onmessage = async (event) => {
    let msg;
    try {
        msg = JSON.parse(event.data);
    } catch (e) {
        return;
    }

    if (msg.type === 'WELCOME') {
        myId = msg.clientId;
    } 
    else if (msg.type === 'PEER_LIST') {
        for (const peerId of msg.peers) {
            if (peerId !== myId && !swarmPeers.has(peerId)) {
                if (myId < peerId) {
                    initiatePeerConnection(peerId, true);
                }
            }
        }
    } 
    else if (msg.from && msg.payload) {
        let peer = swarmPeers.get(msg.from);
        if (!peer) {
            peer = initiatePeerConnection(msg.from, false);
        }

        if (msg.payload.offer) {
            await peer.pc.setRemoteDescription(new RTCSessionDescription(msg.payload.offer));
            const answer = await peer.pc.createAnswer();
            await peer.pc.setLocalDescription(answer);
            socket.send(JSON.stringify({ target: msg.from, payload: { answer: peer.pc.localDescription } }));
        } 
        else if (msg.payload.answer) {
            await peer.pc.setRemoteDescription(new RTCSessionDescription(msg.payload.answer));
        } 
        else if (msg.payload.ice) {
            await peer.pc.addIceCandidate(new RTCIceCandidate(msg.payload.ice));
        }
    }
};

function initiatePeerConnection(peerId, isInitiator) {
    const pc = new RTCPeerConnection(iceServers);
    const peerData = { pc, dataChannel: null, hasChunks: new Set(), inflightRequests: new Set() };
    swarmPeers.set(peerId, peerData);
    updatePeerCount();

    pc.onicecandidate = (event) => {
        if (event.candidate) {
            socket.send(JSON.stringify({ target: peerId, payload: { ice: event.candidate } }));
        }
    };

    if (isInitiator) {
        const dataChannel = pc.createDataChannel('swarm-stream', { ordered: true });
        dataChannel.binaryType = 'arraybuffer';
        setupDataChannelEvents(peerId, dataChannel);
        peerData.dataChannel = dataChannel;

        pc.createOffer().then(offer => {
            pc.setLocalDescription(offer);
            socket.send(JSON.stringify({ target: peerId, payload: { offer } }));
        });
    } else {
        pc.ondatachannel = (event) => {
            const dataChannel = event.channel;
            dataChannel.binaryType = 'arraybuffer';
            setupDataChannelEvents(peerId, dataChannel);
            peerData.dataChannel = dataChannel;
        };
    }

    return peerData;
}

function setupDataChannelEvents(peerId, dataChannel) {
    dataChannel.onopen = () => {
        dataChannel.send(JSON.stringify({ type: 'HAVE_CHUNKS', chunks: Array.from(myChunks) }));
        if (fileMetadata) {
            dataChannel.send(JSON.stringify({ type: 'METADATA', metadata: fileMetadata }));
        }
        triggerSwarmScheduler();
    };

    dataChannel.onmessage = async (event) => {
        if (typeof event.data === 'string') {
            const msg = JSON.parse(event.data);
            
            if (msg.type === 'METADATA') {
                if (!fileMetadata) {
                    fileMetadata = msg.metadata;
                    totalChunksExpected = fileMetadata.totalChunks;
                    
                    await sendToSW({ 
                        type: 'INIT', 
                        totalChunks: totalChunksExpected,
                        totalSize: fileMetadata.size 
                    });

                    videoElement.src = '/stream';
                    videoElement.load();

                    if (guestStatusLabel) guestStatusLabel.innerText = 'Status: Connected! Swarm downloading in background (0%)';
                    triggerSwarmScheduler();
                }
            } 
            else if (msg.type === 'HAVE_CHUNKS') {
                const peer = swarmPeers.get(peerId);
                if (peer) {
                    peer.hasChunks = new Set(msg.chunks);
                    triggerSwarmScheduler();
                }
            } 
            else if (msg.type === 'REQUEST_CHUNK') {
                const chunkIndex = msg.index;
                if (myChunks.has(chunkIndex) && fileChunks[chunkIndex]) {
                    dataChannel.send(JSON.stringify({ type: 'CHUNK_HEADER', index: chunkIndex }));
                    dataChannel.send(fileChunks[chunkIndex]);
                }
            }
            else if (msg.type === 'CHUNK_HEADER') {
                dataChannel.expectedChunkIndex = msg.index;
            }
            else if (msg.type === 'CONTROL') {
                handleRemoteControl(msg);
            }
        } 
        else {
            const peer = swarmPeers.get(peerId);
            if (peer && peer.dataChannel.expectedChunkIndex !== undefined) {
                const chunkIndex = peer.dataChannel.expectedChunkIndex;
                peer.dataChannel.expectedChunkIndex = undefined;
                peer.inflightRequests.delete(chunkIndex);

                if (!myChunks.has(chunkIndex)) {
                    fileChunks[chunkIndex] = event.data;
                    myChunks.add(chunkIndex);

                    await sendToSW({ type: 'CHUNK', index: chunkIndex, chunk: event.data });

                    updateProgress();
                    broadcastHaveChunks();
                    triggerSwarmScheduler();
                }
            }
        }
    };

    dataChannel.onclose = () => {
        swarmPeers.delete(peerId);
        updatePeerCount();
    };
}

document.getElementById('hostRoleBtn').onclick = () => {
    isHost = true;
    roleSelectionDiv.classList.add('hidden');
    hostControlsDiv.classList.remove('hidden');
    if (loaderText) loaderText.innerText = 'Host mode active. Select a video file below.';
};

document.getElementById('guestRoleBtn').onclick = () => {
    isHost = false;
    roleSelectionDiv.classList.add('hidden');
    guestControlsDiv.classList.remove('hidden');
    if (guestStatusLabel) guestStatusLabel.innerText = 'Status: Joined Swarm Mesh. Waiting for file...';
    if (loaderText) loaderText.innerText = 'Connected to Swarm. Waiting for host or peer stream...';
};

document.getElementById('videoFilePicker').onchange = async (e) => {
    const file = e.target.files[0];
    const arrayBuffer = await file.arrayBuffer();

    fileMetadata = {
        size: arrayBuffer.byteLength,
        totalChunks: Math.ceil(arrayBuffer.byteLength / CHUNK_SIZE),
        type: file.type || 'video/mp4'
    };
    totalChunksExpected = fileMetadata.totalChunks;

    await sendToSW({ 
        type: 'INIT', 
        totalChunks: totalChunksExpected,
        totalSize: fileMetadata.size 
    });

    if (loaderDiv) {
        loaderDiv.style.opacity = '0';
        setTimeout(() => { loaderDiv.style.display = 'none'; }, 500);
    }

    for (let i = 0; i < totalChunksExpected; i++) {
        const chunk = arrayBuffer.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
        fileChunks[i] = chunk;
        myChunks.add(i);

        await sendToSW({ type: 'CHUNK', index: i, chunk });
    }

    videoElement.src = '/stream';
    videoElement.load();

    broadcastMetadata();
    broadcastHaveChunks();
    updateStatus(`Host loaded file: ${(arrayBuffer.byteLength / (1024*1024)).toFixed(1)} MB (${totalChunksExpected} chunks)`);
};

function broadcastMetadata() {
    const msg = JSON.stringify({ type: 'METADATA', metadata: fileMetadata });
    for (const [, peer] of swarmPeers) {
        if (peer.dataChannel && peer.dataChannel.readyState === 'open') {
            peer.dataChannel.send(msg);
        }
    }
}

function broadcastHaveChunks() {
    const msg = JSON.stringify({ type: 'HAVE_CHUNKS', chunks: Array.from(myChunks) });
    for (const [, peer] of swarmPeers) {
        if (peer.dataChannel && peer.dataChannel.readyState === 'open') {
            peer.dataChannel.send(msg);
        }
    }
}

// --- PARALLEL SWARM SCHEDULER (PIPELINING) ---
function triggerSwarmScheduler() {
    if (!totalChunksExpected || myChunks.size >= totalChunksExpected) return;

    const MAX_INFLIGHT_PER_PEER = 4;

    for (const [peerId, peer] of swarmPeers) {
        if (!peer.dataChannel || peer.dataChannel.readyState !== 'open') continue;

        while (peer.inflightRequests.size < MAX_INFLIGHT_PER_PEER) {
            let targetChunk = -1;
            for (let i = 0; i < totalChunksExpected; i++) {
                if (!myChunks.has(i)) {
                    let alreadyRequestedAnywhere = false;
                    for (const [, p] of swarmPeers) {
                        if (p.inflightRequests.has(i)) {
                            alreadyRequestedAnywhere = true;
                            break;
                        }
                    }

                    if (!alreadyRequestedAnywhere && peer.hasChunks.has(i)) {
                        targetChunk = i;
                        break;
                    }
                }
            }

            if (targetChunk === -1) break;

            peer.inflightRequests.add(targetChunk);
            peer.dataChannel.send(JSON.stringify({ type: 'REQUEST_CHUNK', index: targetChunk }));
        }
    }
}

function updateProgress() {
    const percent = Math.round((myChunks.size / totalChunksExpected) * 100);
    const isComplete = myChunks.size >= totalChunksExpected;

    if (myChunks.size > 5 && loaderDiv && loaderDiv.style.display !== 'none') {
        loaderDiv.style.opacity = '0';
        setTimeout(() => { loaderDiv.style.display = 'none'; }, 500);
    }

    if (guestStatusLabel) {
        guestStatusLabel.innerText = isComplete 
            ? 'Status: Full Swarm Download Complete!' 
            : `Status: Downloading in background... ${percent}% (${myChunks.size}/${totalChunksExpected} chunks)`;
    }
}

videoElement.onplay = () => { if (isHost) broadcastControl('play', videoElement.currentTime); };
videoElement.onpause = () => { if (isHost) broadcastControl('pause', videoElement.currentTime); };
videoElement.onseeked = () => { if (isHost) broadcastControl('seek', videoElement.currentTime); };

function broadcastControl(action, currentTime) {
    const packet = JSON.stringify({ type: 'CONTROL', action, currentTime });
    for (const [, peer] of swarmPeers) {
        if (peer.dataChannel && peer.dataChannel.readyState === 'open') {
            peer.dataChannel.send(packet);
        }
    }
}

function handleRemoteControl(msg) {
    if (msg.action === 'play') {
        videoElement.currentTime = msg.currentTime;
        videoElement.play().catch(() => {});
    } else if (msg.action === 'pause') {
        videoElement.currentTime = msg.currentTime;
        videoElement.pause();
    } else if (msg.action === 'seek') {
        videoElement.currentTime = msg.currentTime;
    }
}

function updatePeerCount() {
    if (peerCountSpan) {
        peerCountSpan.innerText = `Connected Swarm Peers: ${swarmPeers.size}`;
    }
}

function updateStatus(text) {
    console.log(text);
    if (guestStatusLabel) guestStatusLabel.innerText = text;
}