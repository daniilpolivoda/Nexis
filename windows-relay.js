const crypto = require('crypto');

const MAX_ROOM_CLIENTS = 16;
const MAX_MESSAGE_BYTES = 700000;
const MAX_SIGNAL_BYTES = 128 * 1024;
const MAX_BUFFERED_BYTES = 1024 * 1024;
const POSITION_TTL_MS = 5000;

// Coordinates are client-reported. A private room key controls access to a broadcast.
module.exports = function attachWindowsRelay(wss, { now = Date.now, allowsPartyRoom = () => false } = {}) {
  const sockets = new Set();
  const clients = new Map();
  const clientsById = new Map();
  const links = new Map();
  const handleServerError = error => {
    console.error('[Windows Relay] WebSocket server error:', error.code || error.name || 'unknown');
  };
  wss.on('error', handleServerError);

  function send(ws, data) {
    if (ws.readyState !== 1) return false;
    if (ws.bufferedAmount >= MAX_BUFFERED_BYTES) {
      ws.close(1013, 'Slow connection; reconnect');
      return false;
    }
    try {
      ws.send(JSON.stringify(data));
      return true;
    } catch {
      ws.close(1011, 'Send failed');
      return false;
    }
  }

  function nearby(a, b) {
    const partyRoom = a.room.startsWith('party:');
    return a !== b && a.room === b.room && a.server && a.server === b.server
      && a.dimension && a.dimension === b.dimension
      && a.position && b.position
      && now() - a.positionAt <= POSITION_TTL_MS && now() - b.positionAt <= POSITION_TTL_MS
      && (!partyRoom || allowsPartyRoom(a.room))
      && (partyRoom || a.position.reduce((sum, value, i) => sum + (value - b.position[i]) ** 2, 0) <= 10000);
  }

  function removeLink(key, link) {
    links.delete(key);
    send(link.viewer.ws, { type: 'leave', id: link.sender.id });
    send(link.sender.ws, { type: 'viewer-left', id: link.viewer.id });
  }

  function reconcile() {
    for (const [key, link] of links) {
      if (!clientsById.has(link.sender.id) || !clientsById.has(link.viewer.id)
        || !link.sender.active || !link.viewer.receive || !nearby(link.sender, link.viewer)) {
        removeLink(key, link);
      }
    }
    for (const sender of clients.values()) {
      if (!sender.active) continue;
      for (const viewer of clients.values()) {
        if (!viewer.receive || !nearby(sender, viewer)) continue;
        const key = `${sender.id}/${viewer.id}`;
        const link = links.get(key);
        if (link && link.version === sender.version) continue;
        if (!send(viewer.ws, { type: 'peer', id: sender.id, plane: sender.plane, audio: sender.audio })) continue;
        if (!link) {
          if (!send(sender.ws, { type: 'viewer', id: viewer.id })) continue;
          links.set(key, { sender, viewer, version: sender.version });
        } else {
          link.version = sender.version;
        }
      }
    }
  }

  function validPlane(plane) {
    if (!Array.isArray(plane) || plane.length !== 11 || !plane.every(Number.isFinite)) return false;
    const rightLength = Math.hypot(...plane.slice(3, 6));
    const upLength = Math.hypot(...plane.slice(6, 9));
    return plane.slice(0, 3).every(value => Math.abs(value) <= 3.2e7)
      && rightLength > 0.99 && rightLength < 1.01 && upLength > 0.99 && upLength < 1.01
      && Math.abs(plane[3] * plane[6] + plane[4] * plane[7] + plane[5] * plane[8]) < 0.01
      && plane[9] >= 0.4 && plane[9] <= 25 && plane[10] >= 0.2 && plane[10] <= 25;
  }

  function signal(client, message, length) {
    if (length > MAX_SIGNAL_BYTES || typeof message.to !== 'string') return;
    const target = clientsById.get(message.to);
    if (!target || !nearby(client, target)) return;
    const outgoing = links.has(`${client.id}/${target.id}`);
    const incoming = links.has(`${target.id}/${client.id}`);
    if (!outgoing && !incoming) return;
    // One pair can carry two independent broadcasts. ICE must identify its owner.
    let stream = message.stream;
    if (stream === undefined && outgoing !== incoming) stream = outgoing ? client.id : target.id;
    const ownsStream = stream === client.id && outgoing;
    const viewsStream = stream === target.id && incoming;
    if (!ownsStream && !viewsStream) return;
    const data = message.data;
    if (!data || typeof data !== 'object' || Array.isArray(data)) return;
    if (data.description) {
      const { type, sdp } = data.description;
      if (typeof sdp !== 'string' || sdp.length > 112000) return;
      if (!(type === 'offer' && ownsStream) && !(type === 'answer' && viewsStream)) return;
      send(target.ws, { type: 'signal', from: client.id, stream, data: { description: { type, sdp } } });
    } else if (Object.hasOwn(data, 'candidate')) {
      const candidate = data.candidate;
      if (candidate !== null && (typeof candidate !== 'object' || Array.isArray(candidate)
        || typeof candidate.candidate !== 'string' || candidate.candidate.length > 8192)) return;
      send(target.ws, { type: 'signal', from: client.id, stream, data: { candidate } });
    }
  }

  function disconnect(ws) {
    sockets.delete(ws);
    const client = clients.get(ws);
    clients.delete(ws);
    if (!client) return;
    clientsById.delete(client.id);
    reconcile();
    for (const peer of clients.values()) if (peer.room === client.room) send(peer.ws, { type: 'stop', id: client.id });
  }

  wss.on('connection', (ws, req) => {
    if (req.url !== '/windows') return;
    sockets.add(ws);
    ws.windowsAlive = true;
    ws.windowsOpenedAt = now();
    ws.on('pong', () => { ws.windowsAlive = true; });
    ws.on('error', () => { disconnect(ws); });
    ws.on('close', () => { disconnect(ws); });
    ws.on('message', (raw, binary) => {
      if (binary || raw.length > MAX_MESSAGE_BYTES) { ws.close(1009, 'Message too large'); return; }
      let message;
      try { message = JSON.parse(raw.toString()); } catch { return; }
      if (!message || typeof message !== 'object' || Array.isArray(message)) return;
      let client = clients.get(ws);
      if (message.type === 'hello' && !client) {
        if (typeof message.room !== 'string' || message.room.length < 16 || message.room.length > 128) {
          ws.close(1008, 'Room key must contain 16 to 128 characters'); return;
        }
        if (message.room.startsWith('party:') && !allowsPartyRoom(message.room)) {
          ws.close(1008, 'Party session expired'); return;
        }
        if ([...clients.values()].filter(peer => peer.room === message.room).length >= MAX_ROOM_CLIENTS) {
          ws.close(1008, 'Room is full'); return;
        }
        client = { id: crypto.randomUUID(), ws, room: message.room, position: null, positionAt: 0,
          server: '', dimension: '', receive: true, active: false, version: 0, frameAt: -Infinity,
          diagnosticsAt: -Infinity, invalidPlane: false };
        clients.set(ws, client);
        clientsById.set(client.id, client);
        send(ws, { type: 'hello', id: client.id, protocol: 2 });
        return;
      }
      if (!client) return;
      if (message.type !== 'frame' && raw.length > MAX_SIGNAL_BYTES) { ws.close(1009); return; }
      if (message.type === 'position') {
        if (!Array.isArray(message.position) || message.position.length !== 3
          || !message.position.every(value => Number.isFinite(value) && Math.abs(value) <= 3.2e7)
          || typeof message.server !== 'string' || message.server.length > 256
          || typeof message.dimension !== 'string' || message.dimension.length > 128) return;
        client.position = message.position;
        client.positionAt = now();
        client.server = message.server;
        client.dimension = message.dimension;
        client.receive = message.receive !== false;
        reconcile();
      } else if (message.type === 'diagnostics') {
        const time = now();
        if (time - client.diagnosticsAt < 1500) return;
        client.diagnosticsAt = time;
        const status = { type: 'diagnostics', active: client.active, invalidPlane: client.invalidPlane,
          ownPosition: !!client.position && time - client.positionAt <= POSITION_TTL_MS,
          roomPeers: 0, receiving: 0, sameServer: 0, sameDimension: 0, freshPosition: 0 };
        for (const peer of clients.values()) {
          if (peer === client || peer.room !== client.room) continue;
          status.roomPeers++;
          if (!peer.receive) continue;
          status.receiving++;
          if (!client.server || client.server !== peer.server) continue;
          status.sameServer++;
          if (!client.dimension || client.dimension !== peer.dimension) continue;
          status.sameDimension++;
          if (peer.position && time - peer.positionAt <= POSITION_TTL_MS) status.freshPosition++;
        }
        send(ws, status);
      } else if (message.type === 'stream') {
        client.invalidPlane = message.active === true && !validPlane(message.plane);
        if (client.invalidPlane) return;
        const active = message.active === true;
        const audio = message.audio === true;
        if (active !== client.active || audio !== client.audio || JSON.stringify(message.plane) !== JSON.stringify(client.plane)) {
          client.version++;
        }
        client.active = active;
        client.audio = audio;
        client.plane = active ? message.plane : null;
        reconcile();
      } else if (message.type === 'signal') {
        signal(client, message, raw.length);
      } else if (message.type === 'frame' && now() - client.frameAt >= 100) {
        if (!client.position || typeof message.image !== 'string' || message.image.length > 680000 || !validPlane(message.plane)) return;
        client.frameAt = now();
        for (const peer of clients.values()) {
          if (peer.receive && nearby(client, peer)) send(peer.ws, {
            type: 'frame', id: client.id, image: message.image, plane: message.plane,
          });
        }
      } else if (message.type === 'stop') {
        client.active = false;
        reconcile();
        for (const peer of clients.values()) if (peer.room === client.room) send(peer.ws, { type: 'stop', id: client.id });
      }
    });
  });

  const sweep = () => {
    for (const ws of sockets) if (!clients.has(ws) && now() - ws.windowsOpenedAt > 10000) ws.close(1008, 'Hello timeout');
    reconcile();
  };
  const sweeper = setInterval(sweep, 1000);
  const heartbeat = setInterval(() => {
    for (const ws of sockets) {
      if (!ws.windowsAlive) { disconnect(ws); ws.terminate(); continue; }
      ws.windowsAlive = false;
      try { ws.ping(); } catch { disconnect(ws); ws.terminate(); }
    }
  }, 15000);
  sweeper.unref();
  heartbeat.unref();
  const close = () => {
    clearInterval(sweeper);
    clearInterval(heartbeat);
    wss.off('error', handleServerError);
  };
  wss.once('close', close);
  return { close, sweep };
};
