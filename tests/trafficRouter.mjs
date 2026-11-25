// Disposable test router. Production should use the infrastructure load balancer;
// this exercises a stable client endpoint without installing a host service.
import { createServer, request as forward,Agent } from 'node:http';

export async function trafficRouter(backends, {port=0}={}) {
  for (const base of backends) {
    if (new URL(base).hostname !== '127.0.0.1') throw new Error('Test router is loopback-only');
  }
  let stopped = false, timer;
  const healthy = backends.map(() => false), failures = backends.map(() => 0);
  const sockets = new Set();
  // Node's global Agent has timeout=5000. On reuse it can retain the shorter
  // server keep-alive idle timeout because req.timeout equals agent.timeout.
  // A private Agent with timeout=0 makes Node reset the explicit 5s ACTIVE
  // request timeout on every reuse. No retries or global transport changes.
  const agent=new Agent({keepAlive:true,timeout:0,maxSockets:64,maxFreeSockets:4});
  async function probe() {
    await Promise.all(backends.map(async (base, index) => {
      let ready = false;
      try {
        const response = await fetch(base + '/readyz', { signal: AbortSignal.timeout(2000) });
        await response.arrayBuffer(); ready = response.status === 200;
      } catch { /* Two failed probes remove a backend; requests are never replayed. */ }
      if(backends[index]!==base)return; // An old probe cannot qualify a replacement listener.
      if (ready) { failures[index] = 0; healthy[index] = true; }
      else if (++failures[index] >= 2) healthy[index] = false;
    }));
    if (!stopped) timer = setTimeout(probe, 250);
  }
  const server = createServer((req, res) => {
    const started=performance.now();let timedOut=false;
    const index = healthy.findIndex(Boolean);
    if (index === -1) { res.writeHead(503).end('No active backend'); return; }
    const upstream = forward(new URL(req.url, backends[index]), {
      method: req.method, headers: req.headers, timeout: 5000,agent,
    }, incoming => { res.writeHead(incoming.statusCode, incoming.headers); incoming.pipe(res); });
    upstream.on('timeout', () => {timedOut=true;upstream.destroy(new Error('Backend timed out'));});
    upstream.on('error', error => {
      console.error('TEST_ROUTER_BACKEND_ERROR '+JSON.stringify({path:new URL(req.url,'http://test.invalid').pathname,code:error.code||error.name,timedOut,elapsedMs:Math.round(performance.now()-started)}));
      if (res.headersSent) res.destroy(); else res.writeHead(503).end('Backend failed; request was not replayed');
    });
    req.on('aborted', () => upstream.destroy());
    req.pipe(upstream);
  });
  server.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  await new Promise((resolve,reject) => { server.once('error',reject);server.listen(port, '127.0.0.1', resolve); });
  await probe();
  return {
    base: `http://127.0.0.1:${server.address().port}`,
    setBackend(index,base){
      if(stopped||!Number.isInteger(index)||index<0||index>=backends.length||new URL(base).hostname!=='127.0.0.1')throw Error('Invalid owned router backend replacement');
      backends[index]=base;healthy[index]=false;failures[index]=0;
    },
    async close() {
      stopped = true; clearTimeout(timer);
      agent.destroy();
      for (const socket of sockets) socket.destroy();
      await new Promise(resolve => server.close(resolve));
    },
  };
}
