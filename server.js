// 反向貪食蛇：網路對戰伺服器
// 1. 提供遊戲網頁（public/index.html）
// 2. 在 /ws 提供 WebSocket：建立房間、加入房間、在同一房間的兩位玩家之間轉送訊息
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');
const ROOM_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // 去掉容易看錯的 I、L、O、0、1
const MAX_PER_ROOM = 2;
const MAX_MSG_PER_SEC = 120;

/* ---------- 靜態網頁 ---------- */
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon' };
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  const rel = url.pathname === '/' ? 'index.html' : decodeURIComponent(url.pathname).replace(/^\/+/, '');
  const file = path.normalize(path.join(PUBLIC_DIR, rel));
  if (!file.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('找不到這個頁面'); }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

/* ---------- 房間 ---------- */
const rooms = new Map(); // 房號 -> { host, guest }

function newRoomCode() {
  for (let tries = 0; tries < 1000; tries++) {
    let code = '';
    for (let i = 0; i < 4; i++) code += ROOM_CHARS[Math.floor(Math.random() * ROOM_CHARS.length)];
    if (!rooms.has(code)) return code;
  }
  throw new Error('no room code available');
}
function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}
function other(ws) {
  const room = ws.room && rooms.get(ws.room);
  if (!room) return null;
  return ws === room.host ? room.guest : room.host;
}
function leaveRoom(ws) {
  const code = ws.room;
  const room = code && rooms.get(code);
  ws.room = null;
  if (!room) return;
  if (room.host === ws) {
    // 房主離開：房間關閉，訪客回到大廳
    if (room.guest) { room.guest.room = null; send(room.guest, { t: 'room-closed' }); }
    rooms.delete(code);
  } else if (room.guest === ws) {
    room.guest = null;
    send(room.host, { t: 'peer-left' });
  }
}

/* ---------- WebSocket ---------- */
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.room = null;
  ws.bucket = { start: Date.now(), count: 0 };
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    // 簡單的流量限制：每秒最多 MAX_MSG_PER_SEC 則
    const now = Date.now();
    if (now - ws.bucket.start > 1000) { ws.bucket.start = now; ws.bucket.count = 0; }
    if (++ws.bucket.count > MAX_MSG_PER_SEC) return;

    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;

    switch (m.t) {
      case 'create': {
        leaveRoom(ws);
        const code = newRoomCode();
        rooms.set(code, { host: ws, guest: null });
        ws.room = code;
        send(ws, { t: 'created', room: code });
        break;
      }
      case 'join': {
        const code = String(m.room || '').toUpperCase().trim();
        const room = rooms.get(code);
        if (!/^[A-Z0-9]{4}$/.test(code) || !room) return send(ws, { t: 'error', msg: `找不到房間「${code || '（空白）'}」` });
        if (room.host === ws) return send(ws, { t: 'error', msg: '這是你自己的房間' });
        if (room.guest && room.guest !== ws) return send(ws, { t: 'error', msg: '這個房間已經滿了' });
        leaveRoom(ws);
        room.guest = ws;
        ws.room = code;
        send(ws, { t: 'joined', room: code });
        send(room.host, { t: 'peer-joined' });
        break;
      }
      case 'relay': {
        // 遊戲內容伺服器不解讀，只轉給同房間的另一位玩家
        const peer = other(ws);
        if (peer) send(peer, { t: 'relay', d: m.d });
        break;
      }
      case 'leave':
        leaveRoom(ws);
        send(ws, { t: 'left' });
        break;
    }
  });

  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => {});
});

// 心跳：每 15 秒 ping 一次，沒回應的連線就關掉（例如手機直接關掉瀏覽器）
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 15000);
wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`反向貪食蛇伺服器已啟動：http://localhost:${PORT}`);
});
