// Node/Next request boundary. The load balancer checks /readyz, but a stale
// routing decision must still be rejected here. Do not retry requests.
import {timingSafeEqual} from 'node:crypto';
export function fencedHttpHandler(runtime, handle, {statusToken}={}) {
  return async (req, res) => {
    if(['/livez','/readyz','/ha/status'].includes(req.url)&&!['GET','HEAD'].includes(req.method)) {
      res.writeHead(405,{'allow':'GET, HEAD','cache-control':'no-store'}).end();return;
    }
    if(req.url==='/livez') {
      res.writeHead(200,{'cache-control':'no-store'}).end();return;
    }
    if(req.url==='/ha/status') {
      const expected=Buffer.from(typeof statusToken==='string'?'Bearer '+statusToken:''),actual=Buffer.from(req.headers.authorization||'');
      if(!statusToken||typeof runtime.status!=='function'||actual.length!==expected.length||!timingSafeEqual(actual,expected)) {
        res.writeHead(403,{'cache-control':'no-store'}).end();return;
      }
      try {const body=JSON.stringify(runtime.status());res.writeHead(200,{'cache-control':'no-store','content-type':'application/json'}).end(req.method==='HEAD'?undefined:body);}
      catch {res.writeHead(503,{'cache-control':'no-store'}).end();}
      return;
    }
    if (req.url === '/readyz') {
      res.writeHead(runtime.ready() ? 200 : 503, { 'cache-control': 'no-store' }).end();
      return;
    }
    if (!runtime.ready()) {
      res.writeHead(503, { 'cache-control': 'no-store' }).end('No active application instance');
      return;
    }
    // The original dashboard updater can replace a single host's pinned source
    // or packages without coordinating its peer, overlay or schema contract.
    // HA releases must use the documented stopped/quiescent deployment path.
    if(req.method==='POST') {
      let pathname;
      try {pathname=decodeURIComponent(new URL(req.url,'http://ha.invalid').pathname).replace(/\/+$/,'');}
      catch {res.writeHead(400,{'cache-control':'no-store'}).end();return;}
      if(pathname==='/api/system/version') {
        res.writeHead(409,{'cache-control':'no-store','content-type':'application/json'}).end(JSON.stringify({error:'Use the coordinated HA release procedure; in-place dashboard updates are disabled.',code:'HA_MANAGED_RELEASE'}));return;
      }
    }
    // Header checks alone cannot fence an already-open SSE/stream response.
    // Revalidate each body write/end too. A dispatched mutation may already
    // have committed: disconnect instead of claiming success or replaying it.
    const writeHead = res.writeHead;
    res.writeHead = function (...args) {
      if (!runtime.ready()) { this.destroy(); return this; }
      return writeHead.apply(this, args);
    };
    const write=res.write,end=res.end;
    res.write=function(...args) {
      if(!runtime.ready()) {this.destroy();return false;}
      return write.apply(this,args);
    };
    res.end=function(...args) {
      if(!runtime.ready()) {this.destroy();return this;}
      return end.apply(this,args);
    };
    try { await handle(req, res); }
    catch {
      if (res.headersSent || res.destroyed) res.destroy();
      else res.writeHead(500, { 'cache-control': 'no-store' }).end('Application request failed');
    }
  };
}
