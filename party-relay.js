const crypto = require('crypto');
const { promisify } = require('util');
const scrypt = promisify(crypto.scrypt);

const MAX_MEMBERS = 10;
const MAX_BUFFER = 256 * 1024;
const validPartyName = value => typeof value === 'string' && /^[A-Za-z0-9_.+-]{1,128}$/.test(value);
const validPassword = value => typeof value === 'string' && value.length <= 128 && !/[\p{Cc}\p{Cs}]/u.test(value);

/** Independent protocol for Dimasik's .party command, alongside the original Kimiko relay. */
module.exports = function attachPartyRelay(wss, { now = Date.now } = {}) {
  const sockets = new Set();
  const clients = new Map();
  const names = new Map();
  const parties = new Map();
  const mediaRooms = new Set();

  function send(ws, value) {
    if (ws.readyState !== 1) return;
    if (ws.bufferedAmount > MAX_BUFFER) { ws.close(1013, 'Slow connection'); return; }
    try { ws.send(JSON.stringify(value)); } catch { ws.close(1011); }
  }
  const error = (client, message) => send(client.ws, { type: 'error', message });

  function snapshot(party) {
    if (!party) return null;
    return { name: party.name, leader: party.leader.name,
      members: [...party.members].map(member => ({ id: member.id, name: member.name })),
      mediaRoom: party.mediaRoom, session: party.session };
  }

  function broadcast(party) {
    for (const member of party.members) send(member.ws, { type: 'party', party: snapshot(party) });
  }

  function rotateRoom(party) {
    mediaRooms.delete(party.mediaRoom);
    party.mediaRoom = 'party:' + crypto.randomBytes(32).toString('hex');
    mediaRooms.add(party.mediaRoom);
  }

  function leave(client, notify = true) {
    const party = client.party;
    client.party = null;
    if (notify) send(client.ws, { type: 'party', party: null });
    if (!party) return;
    party.members.delete(client);
    if (!party.members.size) {
      parties.delete(party.name.toLowerCase());
      mediaRooms.delete(party.mediaRoom);
      return;
    }
    if (party.leader === client) party.leader = party.members.values().next().value;
    rotateRoom(party);
    broadcast(party);
  }

  function disconnect(ws) {
    sockets.delete(ws);
    const client = clients.get(ws);
    clients.delete(ws);
    if (!client) return;
    names.delete(client.name.toLowerCase());
    leave(client, false);
  }

  async function operation(client, message) {
    if (client.busy) { error(client, 'Дождитесь ответа на предыдущую команду пати.'); return; }
    const type = message.type;
    if (type === 'create' || type === 'join') {
      if (client.party) { error(client, 'Сначала выйдите из текущей пати.'); return; }
      const password = message.password ?? '';
      if (!validPartyName(message.name) || !validPassword(password)) {
        error(client, 'Название пати: 1–128 символов латиницы, цифр, _, -, + или точки. Пароль: до 128 символов.'); return;
      }
      const key = message.name.toLowerCase();
      if (type === 'create' && parties.has(key)) { error(client, 'Пати с таким названием уже существует.'); return; }
      const existing = parties.get(key);
      if (type === 'join' && !existing) { error(client, 'Пати не найдена.'); return; }
      client.busy = true;
      try {
        if (type === 'create') {
          const salt = password ? crypto.randomBytes(16) : null;
          const hash = password ? await scrypt(password, salt, 32) : null;
          if (clients.get(client.ws) !== client) return;
          if (parties.has(key)) { error(client, 'Пати с таким названием уже существует.'); return; }
          const party = { name: message.name, leader: client, members: new Set([client]),
            session: crypto.randomUUID(), mediaRoom: '', salt, hash };
          rotateRoom(party);
          parties.set(key, party);
          client.party = party;
          broadcast(party);
        } else {
          if (existing.hash) {
            const hash = await scrypt(password, existing.salt, 32);
            if (!crypto.timingSafeEqual(existing.hash, hash)) { error(client, 'Неверный пароль пати.'); return; }
          }
          if (clients.get(client.ws) !== client) return;
          if (parties.get(key) !== existing) { error(client, 'Пати уже распущена.'); return; }
          if (existing.members.size >= MAX_MEMBERS) { error(client, 'В пати уже 10 участников.'); return; }
          existing.members.add(client);
          client.party = existing;
          broadcast(existing);
        }
      } catch {
        error(client, 'Не удалось выполнить команду пати. Повторите позже.');
      } finally { client.busy = false; }
      return;
    }
    if (type === 'info' || type === 'list') { send(client.ws, { type: 'party', party: snapshot(client.party), info: true }); return; }
    if (type === 'leave') { leave(client); return; }
    const party = client.party;
    if (!party) { error(client, 'Вы не состоите в пати.'); return; }
    if (type === 'disband') {
      if (party.leader !== client) { error(client, 'Распустить пати может только лидер.'); return; }
      mediaRooms.delete(party.mediaRoom);
      parties.delete(party.name.toLowerCase());
      for (const member of party.members) {
        member.party = null;
        send(member.ws, { type: 'party', party: null });
        send(member.ws, { type: 'notice', message: 'Пати распущена.' });
      }
      return;
    }
    if (type === 'kick') {
      if (party.leader !== client) { error(client, 'Удалять участников может только лидер.'); return; }
      const member = [...party.members].find(value => value.name.toLowerCase() === String(message.target).toLowerCase());
      if (!member || member === client) { error(client, 'Участник не найден.'); return; }
      leave(member);
      send(member.ws, { type: 'notice', message: 'Вы исключены из пати.' });
      return;
    }
    if (type === 'chat') {
      if (typeof message.text !== 'string' || !message.text.trim() || message.text.length > 512 || /[\p{Cc}\p{Cs}§]/u.test(message.text)) {
        error(client, 'Сообщение пати: 1–512 символов без управляющих кодов.'); return;
      }
      for (const member of party.members) send(member.ws, { type: 'chat', from: client.name, text: message.text });
    }
  }

  wss.on('connection', (ws, req) => {
    if (req.url !== '/party') return;
    sockets.add(ws);
    ws.partyAlive = true;
    ws.partyOpenedAt = now();
    ws.on('pong', () => { ws.partyAlive = true; });
    ws.on('close', () => disconnect(ws));
    ws.on('error', () => disconnect(ws));
    ws.on('message', raw => {
      if (raw.length > 131072) { ws.close(1009); return; }
      let message;
      try { message = JSON.parse(raw.toString()); } catch { return; }
      if (!message || typeof message !== 'object' || Array.isArray(message)) return;
      if (message.type !== 'team' && raw.length > 8192) { ws.close(1009); return; }
      let client = clients.get(ws);
      if (message.type === 'hello' && !client) {
        if (typeof message.id !== 'string' || !/^[A-Za-z0-9:_-]{1,128}$/.test(message.id)
          || typeof message.name !== 'string' || !/^[A-Za-z0-9_]{1,16}$/.test(message.name)) { ws.close(1008, 'Invalid identity'); return; }
        if (names.has(message.name.toLowerCase())) { send(ws, { type: 'error', message: 'Этот ник уже подключён к пати-серверу.' }); ws.close(1008); return; }
        client = { ws, id: message.id, name: message.name, party: null, busy: false, window: now(), messages: 0 };
        clients.set(ws, client); names.set(message.name.toLowerCase(), client);
        send(ws, { type: 'welcome', protocol: 3, features: ['team'], party: null });
        return;
      }
      if (!client) return;
      if (message.type === 'team') {
        if (!client.party) return;
        const clock = now();
        if (message.kind === 'pose') {
          if (raw.length > 4096) return;
          const state = message.state;
          if (!state || typeof state !== 'object' || Array.isArray(state)
            || typeof state.id !== 'string' || !/^[a-f0-9-]{36}$/i.test(state.id)
            || typeof state.server !== 'string' || state.server.length > 255
            || typeof state.dimension !== 'string' || state.dimension.length > 128
            || !['x', 'y', 'z', 'health', 'maxHealth', 'absorption', 'food', 'armor', 'level', 'xp', 'ping', 'slot'].every(key => Number.isFinite(state[key]))
            || ['x', 'y', 'z'].some(key => Math.abs(state[key]) > 3.2e7)) return;
          if (client.lastPoseAt !== undefined && clock - client.lastPoseAt < 75) return;
          client.lastPoseAt = clock;
          for (const member of client.party.members) if (member !== client)
            send(member.ws, { type: 'team', kind: 'pose', from: client.name, state });
        } else if (message.kind === 'inventory') {
          if (typeof message.id !== 'string' || !/^[a-f0-9-]{36}$/i.test(message.id)
            || typeof message.server !== 'string' || message.server.length > 255
            || typeof message.data !== 'string' || message.data.length > 90000 || !/^[A-Za-z0-9+/=]+$/.test(message.data)) return;
          if (client.lastInventoryAt !== undefined && clock - client.lastInventoryAt < 400) return;
          client.lastInventoryAt = clock;
          for (const member of client.party.members) if (member !== client)
            send(member.ws, { type: 'team', kind: 'inventory', from: client.name, id: message.id, server: message.server, data: message.data });
        }
        return;
      }
      if (now() - client.window >= 60000) { client.window = now(); client.messages = 0; }
      if (++client.messages > 100) { ws.close(1008, 'Rate limit'); return; }
      operation(client, message);
    });
  });
  const heartbeat = setInterval(() => {
    for (const ws of sockets) {
      if (!clients.has(ws) && now() - ws.partyOpenedAt > 10000) { ws.close(1008); continue; }
      if (!ws.partyAlive) { disconnect(ws); ws.terminate(); continue; }
      ws.partyAlive = false;
      try { ws.ping(); } catch { disconnect(ws); ws.terminate(); }
    }
  }, 15000);
  heartbeat.unref();
  const close = () => clearInterval(heartbeat);
  wss.once('close', close);
  return { allowsPartyRoom: room => mediaRooms.has(room), close };
};
