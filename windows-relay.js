const crypto = require('crypto');

// Windows images are routed only within a shared room, world and 100-block radius.
module.exports = function attachWindowsRelay(wss) {
  const clients = new Map();
  const send = (ws, data) => {
    if (ws.readyState === 1 && ws.bufferedAmount < 1024 * 1024) ws.send(JSON.stringify(data));
  };
  const nearby = (a, b) => a.room === b.room && a.server === b.server && a.dimension === b.dimension
    && a.position.every(Number.isFinite) && b.position.every(Number.isFinite)
    && a.position.reduce((sum, value, i) => sum + (value - b.position[i]) ** 2, 0) <= 10000;

  wss.on('connection', (ws, req) => {
    if (req.url !== '/windows') return;
    let client;
    let previousFrame = 0;
    ws.on('message', (raw, binary) => {
      if (binary || raw.length > 700000) { ws.close(1009); return; }
      let message;
      try { message = JSON.parse(raw.toString()); } catch { return; }
      if (message.type === 'hello' && !client) {
        if (typeof message.room !== 'string' || message.room.length < 16 || message.room.length > 128) return;
        client = { id: crypto.randomUUID(), ws, room: message.room, position: [], server: '', dimension: '' };
        clients.set(ws, client);
        send(ws, { type: 'hello', id: client.id });
      }
      if (!client) return;
      if (message.type === 'position') {
        if (!Array.isArray(message.position) || message.position.length !== 3
          || !message.position.every(Number.isFinite)) return;
        client.position = message.position;
        client.server = String(message.server || '').slice(0, 256);
        client.dimension = String(message.dimension || '').slice(0, 128);
      }
      if (message.type === 'frame' && Date.now() - previousFrame >= 100) {
        if (!client.server || !client.dimension || client.position.length !== 3
          || typeof message.image !== 'string' || message.image.length > 680000
          || !Array.isArray(message.plane) || message.plane.length !== 11
          || !message.plane.every(Number.isFinite)) return;
        previousFrame = Date.now();
        for (const peer of clients.values()) {
          if (peer !== client && nearby(client, peer)) send(peer.ws, {
            type: 'frame', id: client.id, image: message.image, plane: message.plane,
          });
        }
      }
      if (message.type === 'stop') {
        for (const peer of clients.values()) if (peer.room === client.room) send(peer.ws, { type: 'stop', id: client.id });
      }
    });
    ws.on('close', () => {
      clients.delete(ws);
      if (client) for (const peer of clients.values()) {
        if (peer.room === client.room) send(peer.ws, { type: 'stop', id: client.id });
      }
    });
  });
};
