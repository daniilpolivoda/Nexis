const { createServer } = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

// 1. Создаем обычный HTTP сервер для Render
const server = createServer((req, res) => {
    // Небольшой ответ для браузера и Health Check Render'а
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Nexis Globals Relay is Running');
});

// 2. Привязываем WebSocketServer к HTTP серверу
const wss = new WebSocketServer({ server });

const clients = new Map();
const parties = new Map();
let nextUserId = 100;

function genCode() {
    return Math.random().toString(36).substring(2, 6).toUpperCase();
}

function send(ws, data) {
    if (ws.readyState === 1) ws.send(JSON.stringify(data));
}

function buildPartyJson(party) {
    if (!party) return null;
    return {
        code: party.code,
        owner_id: party.ownerId,
        owner_username: party.ownerUsername,
        members: party.members.map(m => ({
            user_id: m.userId,
            username: m.username,
            minecraft_name: m.minecraftName || '',
            world_key: m.worldKey || '',
            server_address: m.serverAddress || '',
            online: true,
            health: m.health || 20,
            x: m.x || 0,
            y: m.y || 0,
            z: m.z || 0,
            custom_texture: m.customTexture || '',
            inventory: m.inventory || []
        }))
    };
}

function broadcastToParty(sender, data) {
    const party = parties.get(sender.partyCode);
    if (!party) return;
    for (const m of party.members) {
        const c = clients.get(m.userId);
        if (c && c.ws.readyState === 1) {
            c.ws.send(JSON.stringify(data));
        }
    }
}

wss.on('connection', (ws, req) => {
    const url = new URL(req.url, 'http://localhost');
    const token = url.searchParams.get('token') || 'dev-token';
    const userId = ++nextUserId;
    const clientObj = { ws, userId, token, username: 'Player' + userId, minecraftName: '', partyCode: null };
    clients.set(userId, clientObj);

    ws.on('message', (raw) => {
        let msg;
        try {
            msg = JSON.parse(raw.toString());
        } catch {
            return;
        }
        const type = msg.type || '';
        const currentClient = clients.get(userId);
        if (!currentClient) return;

        switch (type) {
            case 'globals.heartbeat': {
                currentClient.minecraftName = msg.minecraft_name || currentClient.minecraftName;
                if (msg.minecraft_name) currentClient.username = msg.minecraft_name;
                currentClient.serverAddress = msg.server_address || '';
                currentClient.worldKey = msg.world_key || '';
                currentClient.health = msg.health || 20;
                currentClient.x = msg.x || 0;
                currentClient.y = msg.y || 0;
                currentClient.z = msg.z || 0;
                currentClient.customTexture = msg.custom_texture || '';
                currentClient.lastSeen = Date.now();
                if (currentClient.partyCode) {
                    broadcastToParty(currentClient, {
                        type: 'globals.party.update',
                        party: buildPartyJson(parties.get(currentClient.partyCode))
                    });
                }
                break;
            }
            case 'globals.inventory.update': {
                if (!currentClient.partyCode) break;
                msg.user_id = currentClient.userId;
                broadcastToParty(currentClient, msg);
                break;
            }
            case 'globals.point.create': {
                if (!currentClient.partyCode) break;
                msg.user_id = currentClient.userId;
                msg.username = currentClient.username;
                msg.created_at_ms = Date.now();
                broadcastToParty(currentClient, { type: 'globals.point', ...msg });
                break;
            }
            case 'globals.party.create': {
                if (currentClient.partyCode) {
                    send(ws, { type: 'globals.error', error: 'ты уже в группе' });
                    break;
                }
                if (msg.minecraft_name) currentClient.username = msg.minecraft_name;
                const code = genCode();
                const party = {
                    code,
                    ownerId: userId,
                    ownerUsername: currentClient.username,
                    members: [currentClient]
                };
                parties.set(code, party);
                currentClient.partyCode = code;
                send(ws, { type: 'globals.party.created', party: buildPartyJson(party) });
                break;
            }
            case 'globals.party.join': {
                const code = (msg.code || '').toUpperCase();
                const party = parties.get(code);
                if (!party) {
                    send(ws, { type: 'globals.error', error: 'party not found' });
                    break;
                }
                if (msg.minecraft_name) currentClient.username = msg.minecraft_name;
                if (party.members.some(m => m.userId === userId)) {
                    send(ws, { type: 'globals.party.accepted', party: buildPartyJson(party) });
                    break;
                }
                party.members.push(currentClient);
                currentClient.partyCode = code;
                send(ws, { type: 'globals.party.accepted', party: buildPartyJson(party) });
                broadcastToParty(currentClient, {
                    type: 'globals.party.update',
                    party: buildPartyJson(party)
                });
                break;
            }
            case 'globals.party.list': {
                if (!currentClient.partyCode) {
                    send(ws, { type: 'globals.error', error: 'party not found' });
                    break;
                }
                const party = parties.get(currentClient.partyCode);
                if (party) {
                    send(ws, { type: 'globals.party.list', party: buildPartyJson(party) });
                }
                break;
            }
        }
    });

    ws.on('close', () => {
        const partyCode = clientObj.partyCode;
        if (partyCode) {
            const party = parties.get(partyCode);
            if (party) {
                party.members = party.members.filter(m => m.userId !== userId);
                if (party.members.length === 0) {
                    parties.delete(partyCode);
                } else if (party.ownerId === userId) {
                    party.ownerId = party.members[0].userId;
                    party.ownerUsername = party.members[0].username;
                }
                broadcastToParty(clientObj, {
                    type: 'globals.party.update',
                    party: buildPartyJson(party)
                });
            }
        }
        clients.delete(userId);
    });

    ws.on('error', () => {});
});

// 3. Запускаем HTTP сервер вместо напрямую WebSocket
server.listen(PORT, () => {
    console.log(`[Nexis Globals Relay] running on port ${PORT}`);
});
