/**
 * StormWatch — Blitzortung -> JSON proxy  (Node.js, runs on Render.com)
 * ====================================================================
 * WHY THIS, NOT CLOUDFLARE:
 *   Blitzortung's live feed is a WebSocket on PORT 3000. Cloudflare Workers
 *   cannot open port 3000 (proven: "cannot connect to the specified address").
 *   A normal Node server CAN, so this runs on Render instead. It holds the
 *   Blitzortung connection, keeps a rolling 30-minute buffer of strikes, and
 *   serves the exact JSON the StormWatch app already expects.
 *
 * ENDPOINTS:
 *   GET /?lat=48.85&lon=2.35&range=30  ->  { strikes:[ {lat,lon,time(ms)} ], ... }
 *   GET /health                         ->  { ok, connected, buffered, lastMessageAgoMs }
 *
 * Dependency: ws  (declared in package.json — Render installs it automatically).
 */

const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const HOSTS = [
  'wss://ws1.blitzortung.org/',        // port 443 (current standard) — try first
  'wss://ws7.blitzortung.org/',
  'wss://ws8.blitzortung.org/',
  'wss://ws1.blitzortung.org:3000/',   // legacy port 3000 fallback
  'wss://ws7.blitzortung.org:3000/',
];
const RETAIN_MS = 30 * 60 * 1000;
const MAX_BUFFER = 6000;

let buffer = [];            // { lat, lon, time(ms) }
let ws = null;
let hostIdx = 0;
let lastMessageAt = 0;
let reconnectTimer = null;
const diagnostics = { attempts: 0, connectedEver: false, currentUrl: null, lastError: null, lastErrorCode: null, lastHttpStatus: null, lastOpenAt: null, lastCloseAt: null, urlStatus: {} };

function addStrike(s) {
  buffer.push(s);
  if (buffer.length > MAX_BUFFER) buffer = buffer.slice(-MAX_BUFFER);
}
function prune() {
  const cut = Date.now() - RETAIN_MS;
  buffer = buffer.filter((s) => s.time > cut);
}
setInterval(prune, 30000);

// Blitzortung payload decompressor (LZW variant used by their web map).
function decode(input) {
  const s = '' + input;
  if (s.charAt(0) === '{') return s;
  const data = s.split('');
  const dict = {};
  let currChar = data[0];
  let oldPhrase = currChar;
  const out = [currChar];
  let code = 256;
  let phrase;
  for (let i = 1; i < data.length; i++) {
    const currCode = data[i].charCodeAt(0);
    if (currCode < 256) phrase = data[i];
    else phrase = dict[currCode] ? dict[currCode] : oldPhrase + currChar;
    out.push(phrase);
    currChar = phrase.charAt(0);
    dict[code] = oldPhrase + currChar;
    code++;
    oldPhrase = phrase;
  }
  return out.join('');
}

function connect() {
  const url = HOSTS[hostIdx % HOSTS.length];
  hostIdx++;
  diagnostics.attempts++;
  diagnostics.currentUrl = url;
  diagnostics.urlStatus[url] = 'connecting';
  console.log('[bz] connecting', url);
  ws = new WebSocket(url, {
    handshakeTimeout: 12000,
    rejectUnauthorized: false, // tolerate cert host/SNI mismatch on :3000
    origin: 'https://map.blitzortung.org',
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
      'Origin': 'https://map.blitzortung.org',
    },
  });

  ws.on('open', () => {
    console.log('[bz] open');
    diagnostics.connectedEver = true;
    diagnostics.lastOpenAt = new Date().toISOString();
    diagnostics.urlStatus[url] = 'OPEN';
    diagnostics.lastError = null;
    diagnostics.lastErrorCode = null;
    diagnostics.lastHttpStatus = null;
    lastMessageAt = Date.now();
    try { ws.send(JSON.stringify({ a: 111 })); } catch (e) {}
  });
  ws.on('message', (raw) => {
    lastMessageAt = Date.now();
    diagnostics.messages = (diagnostics.messages || 0) + 1;
    const str = raw.toString();
    let dec;
    try { dec = decode(str); }
    catch (e) {
      diagnostics.decodeErr = String(e && e.message || e);
      if (!diagnostics.sampleRaw) diagnostics.sampleRaw = str.slice(0, 220);
      return;
    }
    let obj;
    try { obj = JSON.parse(dec); }
    catch (e) {
      diagnostics.parseErr = String(e && e.message || e);
      if (!diagnostics.sampleDecoded) { diagnostics.sampleRaw = str.slice(0, 160); diagnostics.sampleDecoded = String(dec).slice(0, 220); }
      return;
    }
    diagnostics.decoded = (diagnostics.decoded || 0) + 1;
    if (!diagnostics.sampleKeys) { diagnostics.sampleKeys = Object.keys(obj); diagnostics.sampleObj = JSON.stringify(obj).slice(0, 300); }
    if (obj && typeof obj.lat === 'number' && typeof obj.lon === 'number') {
      diagnostics.withLatLon = (diagnostics.withLatLon || 0) + 1;
      let t = obj.time ? Math.floor(obj.time / 1e6) : Date.now();
      if (t < 1e12) t = Date.now();
      addStrike({ lat: obj.lat, lon: obj.lon, time: t });
    }
  });
  // Fired when the server answers the upgrade with a normal HTTP response (e.g. 403/404).
  ws.on('unexpected-response', (req, res) => {
    diagnostics.lastHttpStatus = res.statusCode;
    diagnostics.lastError = 'unexpected HTTP ' + res.statusCode;
    diagnostics.urlStatus[url] = 'HTTP ' + res.statusCode;
    console.log('[bz] unexpected-response', res.statusCode);
    try { ws.close(); } catch (e) {}
    scheduleReconnect();
  });
  ws.on('close', (code) => { diagnostics.lastCloseAt = new Date().toISOString(); console.log('[bz] close', code); scheduleReconnect(); });
  ws.on('error', (e) => {
    diagnostics.lastError = String(e && e.message || e);
    diagnostics.lastErrorCode = (e && e.code) || null;
    if (diagnostics.urlStatus[url] !== 'OPEN') diagnostics.urlStatus[url] = (e && e.code) || String(e && e.message || e);
    console.log('[bz] error', diagnostics.lastError);
    try { ws.close(); } catch (_) {}
  });
}
function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => { reconnectTimer = null; connect(); }, 3000);
}
// Watchdog: cycle the connection if it goes quiet for 60s.
setInterval(() => {
  if (ws && Date.now() - lastMessageAt > 60000) { try { ws.close(); } catch (e) {} }
}, 20000);
connect();

function haversine(a, b, c, d) {
  const R = 3958.8, toR = Math.PI / 180;
  const dLat = (c - a) * toR, dLon = (d - b) * toR;
  const x = Math.sin(dLat / 2) ** 2 + Math.cos(a * toR) * Math.cos(c * toR) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(x)));
}

const server = http.createServer((req, res) => {
  const cors = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': '*',
    'Content-Type': 'application/json',
  };
  if (req.method === 'OPTIONS') { res.writeHead(204, cors); res.end(); return; }

  const u = new URL(req.url, 'http://x');
  if (u.pathname === '/health') {
    res.writeHead(200, cors);
    res.end(JSON.stringify({
      ok: true,
      connected: !!(ws && ws.readyState === 1),
      buffered: buffer.length,
      lastMessageAgoMs: lastMessageAt ? Date.now() - lastMessageAt : null,
      diagnostics,
    }));
    return;
  }

  const lat = parseFloat(u.searchParams.get('lat'));
  const lon = parseFloat(u.searchParams.get('lon'));
  const range = parseFloat(u.searchParams.get('range') || '30');
  if (Number.isNaN(lat) || Number.isNaN(lon)) {
    res.writeHead(400, cors);
    res.end(JSON.stringify({ error: 'lat and lon required', connected: !!(ws && ws.readyState === 1), buffered: buffer.length, strikes: [] }));
    return;
  }

  prune();
  const strikes = buffer
    .map((s) => ({ lat: s.lat, lon: s.lon, time: s.time, dist: haversine(lat, lon, s.lat, s.lon) }))
    .filter((s) => s.dist <= range)
    .sort((a, b) => b.time - a.time)
    .slice(0, 500);

  res.writeHead(200, cors);
  res.end(JSON.stringify({ connected: !!(ws && ws.readyState === 1), count: strikes.length, strikes }));
});

server.listen(PORT, () => console.log('[http] listening on :' + PORT));
