const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const attachParty = require('./party-relay');
const attachWindows = require('./windows-relay');

class Socket extends EventEmitter {
  readyState = 1;
  bufferedAmount = 0;
  sent = [];
  send(text) { this.sent.push(JSON.parse(text)); }
  ping() { this.emit('pong'); }
  close(code) { this.code = code; this.readyState = 3; this.emit('close'); }
  terminate() { this.close(1006); }
  message(value) { this.emit('message', Buffer.from(JSON.stringify(value)), false); }
  take(type) { return this.sent.filter(value => value.type === type); }
  party() { return this.take('party').at(-1)?.party; }
}

async function waitFor(predicate) {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  throw new Error('Timed out waiting for party operation');
}

function setup(t) {
  const server = new EventEmitter();
  const party = attachParty(server);
  const windows = attachWindows(server, { allowsPartyRoom: party.allowsPartyRoom });
  t.after(() => { party.close(); windows.close(); });
  function join(name) {
    const ws = new Socket();
    server.emit('connection', ws, { url: '/party' });
    ws.message({ type: 'hello', id: 'id-' + name, name });
    assert.equal(ws.take('welcome')[0]?.protocol, 3);
    return ws;
  }
  function monitor(room, x) {
    const ws = new Socket();
    server.emit('connection', ws, { url: '/windows' });
    ws.message({ type: 'hello', room });
    ws.message({ type: 'position', server: 'test.example', dimension: 'overworld', position: [x, 64, 0], receive: true });
    return ws;
  }
  return { server, party, windows, join, monitor };
}

test('creates a party and joins by password without exposing credentials', async t => {
  const { join, party } = setup(t);
  const leader = join('Leader'), guest = join('Guest');
  leader.message({ type: 'create', name: 'TestParty', password: 'private-password' });
  await waitFor(() => leader.party());
  guest.message({ type: 'join', name: 'testparty', password: 'wrong' });
  await waitFor(() => guest.take('error').length);
  assert.equal(guest.party(), undefined);
  guest.message({ type: 'join', name: 'TESTPARTY', password: 'private-password' });
  await waitFor(() => guest.party());
  assert.deepEqual(guest.party().members.map(value => value.name), ['Leader', 'Guest']);
  assert.equal(guest.party().leader, 'Leader');
  assert.match(guest.party().mediaRoom, /^party:[a-f0-9]{64}$/);
  assert(party.allowsPartyRoom(guest.party().mediaRoom));
  assert(!JSON.stringify(guest.sent).includes('private-password'));
  assert(!Object.hasOwn(guest.party(), 'hash'));
  guest.message({ type: 'chat', text: 'hello party' });
  assert.deepEqual(leader.take('chat')[0], { type: 'chat', from: 'Guest', text: 'hello party' });
});

test('leaving and disconnecting rotate access and transfer leadership', async t => {
  const { join, party } = setup(t);
  const leader = join('Leader'), guest = join('Guest');
  leader.message({ type: 'create', name: 'Friends' });
  guest.message({ type: 'join', name: 'Friends' });
  const old = guest.party().mediaRoom;
  leader.close(1000);
  assert.equal(guest.party().leader, 'Guest');
  assert.notEqual(guest.party().mediaRoom, old);
  assert(!party.allowsPartyRoom(old));
  const last = guest.party().mediaRoom;
  guest.message({ type: 'leave' });
  assert.equal(guest.party(), null);
  assert(!party.allowsPartyRoom(last));
});

test('disband requires leadership and revokes every media room', async t => {
  const { join, party } = setup(t);
  const leader = join('Leader'), guest = join('Guest');
  leader.message({ type: 'create', name: 'Friends' });
  guest.message({ type: 'join', name: 'Friends' });
  const room = guest.party().mediaRoom;
  guest.message({ type: 'disband' });
  assert.equal(guest.take('error').length, 1);
  assert(guest.party());
  leader.message({ type: 'disband' });
  assert.equal(leader.party(), null);
  assert.equal(guest.party(), null);
  assert(!party.allowsPartyRoom(room));
});

test('kick rotates token and removes old Windows eligibility on sweep', async t => {
  const { join, monitor, windows, party } = setup(t);
  const leader = join('Leader'), guest = join('Guest');
  leader.message({ type: 'create', name: 'Friends' });
  guest.message({ type: 'join', name: 'Friends' });
  const room = guest.party().mediaRoom;
  const sender = monitor(room, 0), viewer = monitor(room, 1000);
  sender.message({ type: 'stream', active: true, plane: [0, 65, 0, 1, 0, 0, 0, 1, 0, 4.8, 2.7], audio: true });
  assert.equal(viewer.take('peer').length, 1, 'party members receive beyond 100 blocks');
  leader.message({ type: 'kick', target: 'Guest' });
  assert.equal(guest.party(), null);
  assert(!party.allowsPartyRoom(room));
  windows.sweep();
  assert.equal(viewer.take('leave').length, 1);
  const denied = monitor(room, 0);
  assert.equal(denied.code, 1008, 'revoked party token cannot reconnect');
});

test('rejects malformed identities, duplicate names and oversized messages', t => {
  const { server, join } = setup(t);
  join('Player');
  const duplicate = new Socket();
  server.emit('connection', duplicate, { url: '/party' });
  duplicate.message({ type: 'hello', id: 'other', name: 'player' });
  assert.equal(duplicate.code, 1008);
  const malformed = new Socket();
  server.emit('connection', malformed, { url: '/party' });
  malformed.message({ type: 'hello', id: 'id', name: '../BadName' });
  assert.equal(malformed.code, 1008);
  const oversized = new Socket();
  server.emit('connection', oversized, { url: '/party' });
  oversized.message({ type: 'hello', id: 'id', name: 'a'.repeat(9000) });
  assert.equal(oversized.code, 1009);
});

test('blocks unauthorized party room keys and preserves ordinary room distance', t => {
  const { monitor } = setup(t);
  const unknown = monitor('party:' + 'f'.repeat(64), 0);
  assert.equal(unknown.code, 1008);
  const sender = monitor('ordinary-room-12345678', 0), viewer = monitor('ordinary-room-12345678', 1000);
  sender.message({ type: 'stream', active: true, plane: [0, 65, 0, 1, 0, 0, 0, 1, 0, 4.8, 2.7] });
  assert.equal(viewer.take('peer').length, 0);
});
