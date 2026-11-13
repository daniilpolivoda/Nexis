const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const attach = require('./windows-relay');

const PLANE = [0, 65, 0, 1, 0, 0, 0, 1, 0, 4.8, 2.7];
const ROOM = 'shared-room-123456789';

class Socket extends EventEmitter {
  readyState = 1;
  bufferedAmount = 0;
  sent = [];
  send(text) { this.sent.push(JSON.parse(text)); }
  ping() { this.emit('pong'); }
  close(code) { this.code = code; this.readyState = 3; this.emit('close'); }
  terminate() { this.close(1006); }
  message(data) { this.emit('message', Buffer.from(JSON.stringify(data)), false); }
  take(type) { return this.sent.filter(message => message.type === type); }
}

function setup(t) {
  const server = new EventEmitter();
  let clock = 10000;
  const relay = attach(server, { now: () => clock });
  t.after(() => relay.close());
  function join({ room = ROOM, position = [0, 64, 0], serverName = 'test.example', dimension = 'overworld', receive = true } = {}) {
    const socket = new Socket();
    server.emit('connection', socket, { url: '/windows' });
    socket.message({ type: 'hello', room });
    socket.id = socket.take('hello')[0]?.id;
    socket.message({ type: 'position', position, server: serverName, dimension, receive });
    return socket;
  }
  return { join, relay, advance: ms => { clock += ms; }, position: (socket, point, receive = true) => socket.message({ type: 'position', position: point, server: 'test.example', dimension: 'overworld', receive }) };
}

test('discovers eligible viewers at the 100-block boundary and forwards offer/answer/candidates', t => {
  const { join } = setup(t);
  const sender = join();
  const viewer = join({ position: [100, 64, 0] });
  sender.message({ type: 'stream', active: true, plane: PLANE, audio: true });
  assert.equal(sender.take('hello')[0].protocol, 2);
  assert.deepEqual(viewer.take('peer')[0], { type: 'peer', id: sender.id, plane: PLANE, audio: true });
  assert.equal(sender.take('viewer')[0].id, viewer.id);
  sender.message({ type: 'signal', to: viewer.id, data: { description: { type: 'offer', sdp: 'offer-sdp' } } });
  assert.equal(viewer.take('signal')[0].data.description.sdp, 'offer-sdp');
  viewer.message({ type: 'signal', to: sender.id, data: { description: { type: 'answer', sdp: 'answer-sdp' } } });
  viewer.message({ type: 'signal', to: sender.id, data: { candidate: { candidate: 'candidate:test' } } });
  assert.equal(sender.take('signal').length, 2);
});

test('isolates rooms, dimensions, servers, disabled reception and distance', t => {
  const { join } = setup(t);
  const sender = join();
  const rejected = [join({ room: 'another-room-123456789' }), join({ dimension: 'nether' }),
    join({ serverName: 'other.example' }), join({ receive: false }), join({ position: [100.01, 64, 0] })];
  sender.message({ type: 'stream', active: true, plane: PLANE });
  for (const viewer of rejected) {
    assert.equal(viewer.take('peer').length, 0);
    sender.message({ type: 'signal', to: viewer.id, data: { description: { type: 'offer', sdp: 'hidden' } } });
    assert.equal(viewer.take('signal').length, 0);
  }
});

test('position changes remove peers immediately and returning viewers renegotiate', t => {
  const { join, position } = setup(t);
  const sender = join(), viewer = join();
  sender.message({ type: 'stream', active: true, plane: PLANE });
  position(viewer, [101, 64, 0]);
  assert.equal(viewer.take('leave')[0].id, sender.id);
  assert.equal(sender.take('viewer-left')[0].id, viewer.id);
  position(viewer, [50, 64, 0]);
  assert.equal(viewer.take('peer').length, 2);
  assert.equal(sender.take('viewer').length, 2);
  position(viewer, [50, 64, 0], false);
  assert.equal(viewer.take('leave').length, 2);
});

test('stale positions, stop and disconnect close eligibility', t => {
  const { join, advance, relay, position } = setup(t);
  const sender = join(), viewer = join();
  sender.message({ type: 'stream', active: true, plane: PLANE });
  advance(5001); relay.sweep();
  assert.equal(viewer.take('leave').length, 1);
  position(sender, [0, 64, 0]); position(viewer, [0, 64, 0]);
  sender.message({ type: 'stop' });
  assert.equal(viewer.take('leave').length, 2);
  sender.message({ type: 'stream', active: true, plane: PLANE });
  sender.close(1000);
  assert.equal(viewer.take('leave').length, 3);
});

test('preserves legacy JPEG frame delivery with radius filtering', t => {
  const { join } = setup(t);
  const sender = join(), viewer = join(), distant = join({ position: [101, 64, 0] });
  sender.message({ type: 'frame', plane: PLANE, image: 'jpeg-base64' });
  assert.equal(viewer.take('frame')[0].image, 'jpeg-base64');
  assert.equal(distant.take('frame').length, 0);
});

test('validates plane geometry and offer direction, and terminates slow consumers', t => {
  const { join } = setup(t);
  const sender = join(), viewer = join();
  sender.message({ type: 'stream', active: true, plane: PLANE.map(() => 0) });
  assert.equal(viewer.take('peer').length, 0);
  sender.message({ type: 'stream', active: true, plane: PLANE });
  viewer.message({ type: 'signal', to: sender.id, data: { description: { type: 'offer', sdp: 'wrong-direction' } } });
  assert.equal(sender.take('signal').length, 0);
  viewer.bufferedAmount = 1024 * 1024;
  sender.message({ type: 'signal', to: viewer.id, data: { candidate: null } });
  assert.equal(viewer.code, 1013);
});

test('metadata updates preserve negotiated connections and stop closes them', t => {
  const { join } = setup(t);
  const sender = join(), viewer = join();
  sender.message({ type: 'stream', active: true, plane: PLANE, audio: false });
  sender.message({ type: 'stream', active: true, plane: PLANE, audio: false });
  assert.equal(viewer.take('peer').length, 1);
  const moved = [...PLANE]; moved[0] = 3;
  sender.message({ type: 'stream', active: true, plane: moved, audio: true });
  assert.equal(viewer.take('peer').length, 2);
  assert.equal(sender.take('viewer').length, 1);
  sender.message({ type: 'stream', active: false });
  assert.equal(viewer.take('leave').length, 1);
});

test('bounds room membership and rejects short keys', t => {
  const { join } = setup(t);
  const invalid = join({ room: 'short' });
  assert.equal(invalid.code, 1008);
  for (let count = 0; count < 16; count++) assert.ok(join().id);
  assert.equal(join().code, 1008);
});

test('bidirectional broadcasts route ICE and SDP by stream owner', t => {
  const { join } = setup(t);
  const a = join(), b = join();
  a.message({ type: 'stream', active: true, plane: PLANE, audio: true });
  b.message({ type: 'stream', active: true, plane: PLANE, audio: true });
  // Ambiguous legacy traffic cannot accidentally reach the wrong peer connection.
  a.message({ type: 'signal', to: b.id, data: { candidate: { candidate: 'ambiguous' } } });
  a.message({ type: 'signal', to: b.id, stream: 'unknown', data: { candidate: null } });
  assert.equal(b.take('signal').length, 0);

  for (const [sender, viewer] of [[a, b], [b, a]]) {
    sender.message({ type: 'signal', to: viewer.id, stream: sender.id,
      data: { description: { type: 'offer', sdp: `offer-${sender.id}` } } });
    viewer.message({ type: 'signal', to: sender.id, stream: sender.id,
      data: { description: { type: 'answer', sdp: `answer-${sender.id}` } } });
    sender.message({ type: 'signal', to: viewer.id, stream: sender.id,
      data: { candidate: { candidate: `sender-ice-${sender.id}` } } });
    viewer.message({ type: 'signal', to: sender.id, stream: sender.id,
      data: { candidate: { candidate: `viewer-ice-${sender.id}` } } });
    const offered = viewer.take('signal').find(message => message.data.description?.sdp === `offer-${sender.id}`);
    assert.equal(offered.stream, sender.id);
    assert.equal(offered.from, sender.id);
    const answered = sender.take('signal').find(message => message.data.description?.sdp === `answer-${sender.id}`);
    assert.equal(answered.stream, sender.id);
    assert.equal(answered.from, viewer.id);
    assert.equal(viewer.take('signal').find(message => message.data.candidate?.candidate === `sender-ice-${sender.id}`).stream, sender.id);
    assert.equal(sender.take('signal').find(message => message.data.candidate?.candidate === `viewer-ice-${sender.id}`).stream, sender.id);
  }
  const previous = b.take('signal').length;
  a.message({ type: 'signal', to: b.id, stream: b.id,
    data: { description: { type: 'offer', sdp: 'viewer-cannot-offer' } } });
  a.message({ type: 'signal', to: b.id, stream: a.id,
    data: { description: { type: 'answer', sdp: 'owner-cannot-answer' } } });
  assert.equal(b.take('signal').length, previous);
});
