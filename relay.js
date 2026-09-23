const { createServer } = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_MEMBERS = 10;
const INVITE_TTL_MS = 60_000;

const server = createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Nexis Party Relay is running');
});

const wss = new WebSocketServer({ server });
const clients = new Map();
const sockets = new Map();
const parties = new Map();
const invites = new Map();
const portalSockets = new Map();
const portalRooms = new Map();

function safeName(value, fallback = 'Player') {
  const text = String(value || '').trim();
  return text.slice(0, 32) || fallback;
}

function send(ws, data) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function sendError(ws, message) {
  send(ws, { type: 'error', message });
}

function findClientByName(name) {
  const lowered = safeName(name, '').toLowerCase();
  for (const client of clients.values()) {
    if (client.name.toLowerCase() === lowered) {
      return client;
    }
  }
  return null;
}

function partyOf(client) {
  return client.partyName ? parties.get(client.partyName) : null;
}

function snapshot(party) {
  if (!party) {
    return null;
  }

  return {
    name: party.name,
    leader: party.leader,
    max: MAX_MEMBERS,
    members: party.members.map((id) => {
      const client = clients.get(id);
      return {
        name: client?.name || 'Offline',
        leader: client?.name === party.leader,
        online: Boolean(client),
        world: '',
        x: 0,
        y: 0,
        z: 0,
        hp: 20,
      };
    }),
  };
}

function broadcastParty(party, extra = {}) {
  if (!party) {
    return;
  }
  const packet = { type: 'party', party: snapshot(party), ...extra };
  for (const id of party.members) {
    const client = clients.get(id);
    if (client) {
      send(client.ws, packet);
    }
  }
}

function broadcastToParty(client, data, includeSelf = true) {
  const party = partyOf(client);
  if (!party) {
    sendError(client.ws, 'Вы не в Party.');
    return false;
  }

  for (const id of party.members) {
    if (!includeSelf && id === client.id) {
      continue;
    }
    const target = clients.get(id);
    if (target) {
      send(target.ws, data);
    }
  }
  return true;
}

function leaveParty(client, silent = false) {
  const party = partyOf(client);
  if (!party) {
    return;
  }

  party.members = party.members.filter((id) => id !== client.id);
  client.partyName = null;

  if (party.members.length === 0) {
    parties.delete(party.name);
    return;
  }

  if (party.leader === client.name) {
    const nextLeader = clients.get(party.members[0]);
    party.leader = nextLeader?.name || party.leader;
  }

  if (!silent) {
    broadcastParty(party, { level: 'info', message: `${client.name} вышел из Party.` });
  } else {
    broadcastParty(party);
  }
}

function createParty(client, name) {
  const partyName = safeName(name, `${client.name}'s Party`);
  if (client.partyName) {
    sendError(client.ws, 'Вы уже в Party.');
    return;
  }
  if (parties.has(partyName)) {
    sendError(client.ws, 'Party с таким названием уже есть.');
    return;
  }

  const party = {
    name: partyName,
    leader: client.name,
    members: [client.id],
  };
  parties.set(partyName, party);
  client.partyName = partyName;
  send(client.ws, { type: 'notice', level: 'success', message: `Party "${partyName}" создана.` });
  broadcastParty(party, { info: true });
}

function inviteUser(client, targetName) {
  const party = partyOf(client);
  if (!party) {
    sendError(client.ws, 'Сначала создайте Party.');
    return;
  }
  if (party.leader !== client.name) {
    sendError(client.ws, 'Инвайтить может только лидер.');
    return;
  }
  if (party.members.length >= MAX_MEMBERS) {
    sendError(client.ws, 'Party заполнена.');
    return;
  }

  const target = findClientByName(targetName);
  if (!target) {
    sendError(client.ws, 'Игрок не онлайн или Party модуль у него выключен.');
    return;
  }
  if (target.partyName) {
    sendError(client.ws, 'Игрок уже в Party.');
    return;
  }

  const invite = crypto.randomUUID().slice(0, 8);
  invites.set(invite, {
    id: invite,
    from: client.id,
    target: target.id,
    party: party.name,
    expiresAt: Date.now() + INVITE_TTL_MS,
  });

  send(target.ws, {
    type: 'invite',
    invite,
    from: client.name,
    party: party.name,
    expires: INVITE_TTL_MS,
  });
  send(client.ws, { type: 'notice', level: 'info', message: `Инвайт отправлен ${target.name}.` });
}

function respondInvite(client, inviteId, accept) {
  const invite = invites.get(String(inviteId || ''));
  if (!invite || invite.target !== client.id || invite.expiresAt < Date.now()) {
    sendError(client.ws, 'Инвайт не найден или устарел.');
    return;
  }
  invites.delete(invite.id);

  const party = parties.get(invite.party);
  if (!accept) {
    const from = clients.get(invite.from);
    if (from) {
      send(from.ws, { type: 'notice', level: 'info', message: `${client.name} отклонил инвайт.` });
    }
    return;
  }
  if (!party || party.members.length >= MAX_MEMBERS || client.partyName) {
    sendError(client.ws, 'Нельзя войти в Party.');
    return;
  }

  party.members.push(client.id);
  client.partyName = party.name;
  broadcastParty(party, { info: true });
}

function removeClient(ws) {
  const client = sockets.get(ws);
  if (!client) {
    return;
  }
  leaveParty(client, true);
  sockets.delete(ws);
  clients.delete(client.id);
}

wss.on('connection', (ws, req) => {
  const isPortal = req.url === '/portal';

  ws.on('message', (raw, isBinary) => {
    if (isPortal) {
      handlePortalMessage(ws, raw, isBinary);
      return;
    }

    let client = sockets.get(ws);

    if (isBinary) {
      if (!client || !client.partyName) {
        return;
      }
      const name = Buffer.from(client.name, 'utf8');
      if (name.length > 255) {
        return;
      }
      const payload = Buffer.concat([Buffer.from([name.length]), name, Buffer.from(raw)]);
      const party = partyOf(client);
      for (const id of party.members) {
        if (id === client.id) {
          continue;
        }
        const target = clients.get(id);
        if (target && target.ws.readyState === target.ws.OPEN) {
          target.ws.send(payload, { binary: true });
        }
      }
      return;
    }

    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === 'hello') {
      const baseId = String(msg.id || crypto.randomUUID());
      const oldClient = clients.get(baseId);
      const id = oldClient && oldClient.ws !== ws ? `${baseId}:${crypto.randomUUID().slice(0, 8)}` : baseId;
      client = clients.get(id) || { id, ws, name: 'Player', partyName: null, color: 0, rainbow: false };
      client.ws = ws;
      client.name = safeName(msg.name, client.name);
      clients.set(id, client);
      sockets.set(ws, client);
      send(ws, { type: 'welcome', party: snapshot(partyOf(client)) });
      return;
    }

    if (!client) {
      sendError(ws, 'Сначала нужен hello.');
      return;
    }

    switch (msg.type) {
      case 'create':
        createParty(client, msg.name);
        break;
      case 'invite':
        inviteUser(client, msg.target);
        break;
      case 'invite_response':
        respondInvite(client, msg.invite, Boolean(msg.accept));
        break;
      case 'chat':
        broadcastToParty(client, { type: 'chat', from: client.name, text: String(msg.text || '').slice(0, 256) });
        break;
      case 'info':
        send(client.ws, { type: 'party', party: snapshot(partyOf(client)), info: true });
        break;
      case 'leave':
        leaveParty(client);
        send(client.ws, { type: 'party', party: null, info: true });
        break;
      case 'disband': {
        const party = partyOf(client);
        if (!party || party.leader !== client.name) {
          sendError(client.ws, 'Распустить Party может только лидер.');
          break;
        }
        for (const id of party.members) {
          const member = clients.get(id);
          if (member) {
            member.partyName = null;
            send(member.ws, { type: 'party', party: null, info: true });
            send(member.ws, { type: 'notice', level: 'info', message: 'Party распущена.' });
          }
        }
        parties.delete(party.name);
        break;
      }
      case 'kick': {
        const party = partyOf(client);
        const target = findClientByName(msg.target);
        if (!party || party.leader !== client.name || !target || target.partyName !== party.name) {
          sendError(client.ws, 'Не удалось кикнуть игрока.');
          break;
        }
        leaveParty(target, true);
        send(target.ws, { type: 'party', party: null, info: true });
        send(target.ws, { type: 'notice', level: 'info', message: 'Вас кикнули из Party.' });
        broadcastParty(party, { info: true });
        break;
      }
      case 'marker':
        broadcastToParty(client, {
          type: 'marker',
          id: crypto.randomUUID().slice(0, 8),
          from: client.name,
          x: Number(msg.x) || 0,
          y: Number(msg.y) || 0,
          z: Number(msg.z) || 0,
          dim: String(msg.dim || ''),
          ttl: Number(msg.ttl) || 8000,
          color: client.color || Number(msg.color) || 0,
          rainbow: client.rainbow || Boolean(msg.rainbow),
        });
        break;
      case 'spit':
        broadcastToParty(client, {
          type: 'spit',
          from: client.name,
          x: Number(msg.x) || 0,
          y: Number(msg.y) || 0,
          z: Number(msg.z) || 0,
          dx: Number(msg.dx) || 0,
          dy: Number(msg.dy) || 0,
          dz: Number(msg.dz) || 0,
          dim: String(msg.dim || ''),
        }, false);
        break;
      case 'color':
        client.color = Number(msg.color) || 0;
        client.rainbow = Boolean(msg.rainbow);
        broadcastParty(partyOf(client));
        break;
    }
  });

  ws.on('close', () => isPortal ? removePortalClient(ws) : removeClient(ws));
  ws.on('error', () => isPortal ? removePortalClient(ws) : removeClient(ws));
});

function portalSend(ws, data) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function portalPeers(client) {
  const room = portalRooms.get(client.room);
  if (!room) {
    return [];
  }
  return [...room].map((peer) => portalSockets.get(peer)).filter((peer) => peer && peer.ws !== client.ws);
}

function handlePortalMessage(ws, raw, isBinary) {
  let client = portalSockets.get(ws);

  if (isBinary) {
    if (!client || !client.room) {
      return;
    }
    if (raw.length < 2 || raw.length > 524288) {
      return;
    }
    for (const peer of portalPeers(client)) {
      if (peer.ws.readyState === peer.ws.OPEN) {
        peer.ws.send(raw, { binary: true });
      }
    }
    return;
  }

  let msg;
  try {
    msg = JSON.parse(raw.toString());
  } catch {
    return;
  }

  if (msg.t === 'j') {
    removePortalClient(ws, true);
    const room = String(msg.r || '').trim().slice(0, 24);
    const name = String(msg.n || 'Player').trim().slice(0, 32) || 'Player';
    if (!room) {
      return;
    }
    client = { ws, room, name, active: false };
    portalSockets.set(ws, client);
    if (!portalRooms.has(room)) {
      portalRooms.set(room, new Set());
    }
    portalRooms.get(room).add(ws);

    for (const peer of portalPeers(client)) {
      portalSend(peer.ws, { t: 'a', v: peer.active ? 1 : 0, n: peer.name });
      portalSend(ws, { t: 'a', v: peer.active ? 1 : 0, n: peer.name });
    }
    return;
  }

  if (!client) {
    return;
  }

  if (msg.t === 'a') {
    client.active = Number(msg.v) > 0;
    for (const peer of portalPeers(client)) {
      portalSend(peer.ws, { t: 'a', v: client.active ? 1 : 0, n: client.name });
    }
  }
}

function removePortalClient(ws, keepOpen = false) {
  const client = portalSockets.get(ws);
  if (!client) {
    return;
  }

  const room = portalRooms.get(client.room);
  if (room) {
    room.delete(ws);
    if (room.size === 0) {
      portalRooms.delete(client.room);
    }
  }
  portalSockets.delete(ws);

  if (!keepOpen) {
    for (const peer of portalPeers(client)) {
      portalSend(peer.ws, { t: 'l', n: client.name });
    }
  }
}

setInterval(() => {
  const now = Date.now();
  for (const [id, invite] of invites) {
    if (invite.expiresAt < now) {
      invites.delete(id);
    }
  }
}, 30_000).unref();

server.listen(PORT, () => {
  console.log(`[Nexis Party Relay] running on port ${PORT}`);
});
