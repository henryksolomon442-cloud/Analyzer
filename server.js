const http = require('http');
const { WebSocket, WebSocketServer } = require('ws');
const PORT = Number(process.env.PORT || 10000);
const DERIV_ENDPOINTS = ['wss://ws.derivws.com/websockets/v3', 'wss://ws.binaryws.com/websockets/v3'];
const server = http.createServer((req,res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200);
    return res.end(JSON.stringify({ok:true, service:'analyzer-deriv-bridge', websocketPath:'/feed', uptimeSeconds:Math.round(process.uptime()), timestamp:new Date().toISOString()}));
  }
  res.writeHead(404); res.end(JSON.stringify({ok:false,error:'Not found'}));
});
const wss = new WebSocketServer({ noServer:true, maxPayload:1024*1024 });
server.on('upgrade', (req,socket,head) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname !== '/feed') { socket.write('HTTP/1.1 404 Not Found\r\n\r\n'); socket.destroy(); return; }
  const appId = url.searchParams.get('app_id') || '';
  if (!/^[A-Za-z0-9_-]{1,128}$/.test(appId)) { socket.write('HTTP/1.1 400 Bad Request\r\n\r\n'); socket.destroy(); return; }
  wss.handleUpgrade(req,socket,head,client => {
    client.appId = appId;
    wss.emit('connection',client,req);
  });
});
wss.on('connection', client => {
  let upstream = null, keepAlive = null, retryIndex = 0, closed = false, connected = false;
  const cleanup = () => { if (keepAlive) clearInterval(keepAlive); keepAlive=null; if (upstream) { const u=upstream; upstream=null; try { u.removeAllListeners(); u.close(); } catch(e) {} } };
  const sendClient = obj => { if(client.readyState===WebSocket.OPEN) client.send(JSON.stringify(obj)); };
  const connectUpstream = () => {
    if(closed || client.readyState!==WebSocket.OPEN) return;
    const endpoint = DERIV_ENDPOINTS[Math.min(retryIndex,DERIV_ENDPOINTS.length-1)];
    const u = new WebSocket(endpoint+'?app_id='+encodeURIComponent(client.appId), { handshakeTimeout:12000 });
    upstream=u;
    u.on('open',()=> {
      connected=true; retryIndex=0;
      sendClient({msg_type:'bridge_status',status:'upstream_connected',endpoint:endpoint,at:new Date().toISOString()});
      keepAlive=setInterval(()=>{ if(upstream===u && u.readyState===WebSocket.OPEN) u.send(JSON.stringify({ping:1})); },30000);
    });
    u.on('message',data=> { if(upstream===u && client.readyState===WebSocket.OPEN) client.send(data.toString()); });
    u.on('error',err=>sendClient({msg_type:'bridge_status',status:'upstream_error',message:String(err.message||err),endpoint:endpoint}));
    u.on('close',(code,reason)=> {
      if(upstream!==u) return;
      if(keepAlive) clearInterval(keepAlive); keepAlive=null; upstream=null;
      sendClient({msg_type:'bridge_status',status:'upstream_closed',code:code,reason:reason.toString(),endpoint:endpoint,wasConnected:connected});
      if(!closed && client.readyState===WebSocket.OPEN && retryIndex<DERIV_ENDPOINTS.length-1) {
        retryIndex++;
        setTimeout(()=>{if(!closed && client.readyState===WebSocket.OPEN)connectUpstream();},1200);
      }
    });
  };
  client.on('message',data=> {
    if(!upstream || upstream.readyState!==WebSocket.OPEN) {
      sendClient({msg_type:'bridge_status',status:'upstream_not_ready',message:'Bridge is waiting for Deriv upstream connection.'});
      return;
    }
    try { upstream.send(data.toString()); } catch(e) { sendClient({msg_type:'bridge_status',status:'forward_error',message:String(e.message||e)}); }
  });
  client.on('close',()=>{closed=true;cleanup();});
  client.on('error',()=>{closed=true;cleanup();});
  connectUpstream();
});
server.listen(PORT,'0.0.0.0',()=>console.log('Analyzer Deriv bridge listening on '+PORT));
