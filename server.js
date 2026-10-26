const http = require('http');
const { WebSocket, WebSocketServer } = require('ws');
const PORT = Number(process.env.PORT || 10000);
const DERIV_ENDPOINTS = ['wss://ws.derivws.com/websockets/v3', 'wss://ws.binaryws.com/websockets/v3'];

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200);
    return res.end(JSON.stringify({ ok: true, service: 'analyzer-deriv-bridge', websocketPath: '/feed', uptimeSeconds: Math.round(process.uptime()), timestamp: new Date().toISOString() }));
  }
  res.writeHead(404);
  res.end(JSON.stringify({ ok: false, error: 'Not found' }));
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 1024 * 1024 });
server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://localhost');
  console.log('[browser] websocket upgrade requested path=' + url.pathname + ' app_id_present=' + Boolean(url.searchParams.get('app_id')));
  if (url.pathname !== '/feed') {
    console.warn('[browser] rejected unexpected path ' + url.pathname);
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }
  const appId = url.searchParams.get('app_id') || '';
  if (!/^[A-Za-z0-9_-]{1,128}$/.test(appId)) {
    console.warn('[browser] rejected invalid/missing app id');
    socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, client => {
    client.appId = appId;
    wss.emit('connection', client, req);
  });
});

wss.on('connection', client => {
  console.log('[browser] websocket connected; starting Deriv upstream');
  let upstream = null, keepAlive = null, retryTimer = null, connectTimer = null;
  let retryIndex = 0, closed = false, everConnected = false;
  const sendClient = obj => {
    if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(obj));
  };
  const clearTimers = () => {
    if (keepAlive) clearInterval(keepAlive);
    if (retryTimer) clearTimeout(retryTimer);
    if (connectTimer) clearTimeout(connectTimer);
    keepAlive = retryTimer = connectTimer = null;
  };
  const cleanup = () => {
    clearTimers();
    if (upstream) {
      const u = upstream;
      upstream = null;
      try { u.removeAllListeners(); u.terminate(); } catch (_) {}
    }
  };
  const connectUpstream = () => {
    if (closed || client.readyState !== WebSocket.OPEN) return;
    const endpoint = DERIV_ENDPOINTS[retryIndex];
    console.log('[deriv] connecting endpoint=' + endpoint + ' attempt=' + (retryIndex + 1));
    const u = new WebSocket(endpoint + '?app_id=' + encodeURIComponent(client.appId), { handshakeTimeout: 10000 });
    upstream = u;
    let opened = false;
    connectTimer = setTimeout(() => {
      if (upstream === u && u.readyState !== WebSocket.OPEN) {
        console.warn('[deriv] upstream connection timed out endpoint=' + endpoint);
        u.terminate();
      }
    }, 11000);
    u.on('open', () => {
      if (upstream !== u) return;
      opened = true;
      everConnected = true;
      if (connectTimer) clearTimeout(connectTimer);
      connectTimer = null;
      console.log('[deriv] upstream connected endpoint=' + endpoint);
      sendClient({ msg_type: 'bridge_status', status: 'upstream_connected', endpoint, at: new Date().toISOString() });
      keepAlive = setInterval(() => {
        if (upstream === u && u.readyState === WebSocket.OPEN) {
          try { u.send(JSON.stringify({ ping: 1 })); } catch (err) { console.warn('[deriv] ping failed: ' + err.message); }
        }
      }, 25000);
    });
    u.on('message', data => {
      if (upstream === u && client.readyState === WebSocket.OPEN) client.send(data.toString());
    });
    u.on('error', err => {
      console.error('[deriv] upstream error endpoint=' + endpoint + ' message=' + String(err.message || err));
      sendClient({ msg_type: 'bridge_status', status: 'upstream_error', message: String(err.message || err), endpoint });
    });
    u.on('close', (code, reason) => {
      if (upstream !== u) return;
      if (keepAlive) clearInterval(keepAlive);
      if (connectTimer) clearTimeout(connectTimer);
      keepAlive = connectTimer = null;
      upstream = null;
      console.warn('[deriv] upstream closed endpoint=' + endpoint + ' code=' + code + ' reason=' + reason.toString());
      sendClient({ msg_type: 'bridge_status', status: 'upstream_closed', code, reason: reason.toString(), endpoint, wasConnected: opened });
      if (!closed && client.readyState === WebSocket.OPEN && retryIndex < DERIV_ENDPOINTS.length - 1) {
        retryIndex++;
        retryTimer = setTimeout(connectUpstream, 800);
      } else if (!closed && client.readyState === WebSocket.OPEN) {
        sendClient({ msg_type: 'bridge_status', status: 'upstream_failed', message: 'Both Deriv WebSocket endpoints failed. See bridge service logs for the exact reason.', everConnected });
      }
    });
  };

  client.on('message', data => {
    if (!upstream || upstream.readyState !== WebSocket.OPEN) {
      console.warn('[browser] message arrived before upstream ready; buffering briefly');
      // Avoid silently dropping the initial ticks_history request during connection startup.
      const payload = data.toString();
      const started = Date.now();
      const waitForUpstream = () => {
        if (closed || client.readyState !== WebSocket.OPEN) return;
        if (upstream && upstream.readyState === WebSocket.OPEN) {
          try { upstream.send(payload); } catch (err) { sendClient({ msg_type: 'bridge_status', status: 'forward_error', message: String(err.message || err) }); }
          return;
        }
        if (Date.now() - started < 12000) return setTimeout(waitForUpstream, 100);
        sendClient({ msg_type: 'bridge_status', status: 'upstream_not_ready', message: 'Deriv upstream did not open within 12 seconds.' });
      };
      waitForUpstream();
      return;
    }
    try { upstream.send(data.toString()); }
    catch (err) {
      console.error('[browser] forward error: ' + String(err.message || err));
      sendClient({ msg_type: 'bridge_status', status: 'forward_error', message: String(err.message || err) });
    }
  });
  client.on('close', (code, reason) => {
    console.log('[browser] websocket closed code=' + code + ' reason=' + reason.toString());
    closed = true;
    cleanup();
  });
  client.on('error', err => {
    console.error('[browser] websocket error: ' + String(err.message || err));
    closed = true;
    cleanup();
  });
  connectUpstream();
});

server.listen(PORT, '0.0.0.0', () => console.log('Analyzer Deriv bridge listening on ' + PORT));
