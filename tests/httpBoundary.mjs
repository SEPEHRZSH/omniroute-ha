import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { fencedHttpHandler } from '../adapter/http.mjs';
import {trafficRouter} from './trafficRouter.mjs';

let owned = false, dispatched = 0,sendLate;
const statusToken='synthetic-status-token-for-boundary-test';
const runtime = { ready: () => owned,status:()=>({active:owned,role:owned?'active':'standby',epoch:owned?7:null}) };
const server = createServer(fencedHttpHandler(runtime, (req, res) => {
  dispatched++;
  if(req.url.startsWith('/lose-after-headers-')) {
    res.setHeader('content-type','text/event-stream');res.write('first');
    sendLate=()=>{owned=false;if(req.url.endsWith('write'))res.write('forbidden');else res.end('forbidden');};
    return;
  }
  if (req.url === '/lose-before-response') owned = false;
  res.end('application response');
},{statusToken}));
try {
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const base = 'http://127.0.0.1:' + server.address().port;
  const options = { signal: AbortSignal.timeout(3000), method: 'POST' };
  assert.equal((await fetch(base+'/livez')).status,200,'Standby liveness must not claim write readiness');
  assert.equal((await fetch(base+'/ha/status')).status,403);
  assert.equal((await fetch(base+'/ha/status',{headers:{authorization:'Bearer wrong'}})).status,403);
  const status=await fetch(base+'/ha/status',{headers:{authorization:'Bearer '+statusToken}});
  assert.equal(status.status,200);assert.equal(status.headers.get('cache-control'),'no-store');assert.deepEqual(await status.json(),{active:false,role:'standby',epoch:null});
  assert.equal((await fetch(base+'/ha/status',{...options,headers:{authorization:'Bearer '+statusToken}})).status,405);
  assert.equal((await fetch(base+'/livez',options)).status,405);
  assert.equal(dispatched,0,'Native health and authenticated status cannot invoke application writes');
  console.log('PASS native health/status boundary: standby liveness differs from readiness, opaque bearer required, no secret/state leak without auth, GET/HEAD-only and no application dispatch');
  assert.equal((await fetch(base + '/readyz')).status, 503);
  assert.equal((await fetch(base + '/normal', options)).status, 503);
  assert.equal(dispatched, 0);
  owned = true;
  assert.equal((await fetch(base + '/readyz')).status, 200);
  for(const endpoint of ['/api/system/version','/api/system/version?channel=latest','/api/system/version/','/api/system/%76ersion']) {
    const blocked=await fetch(base+endpoint,options);assert.equal(blocked.status,409);assert.equal((await blocked.json()).code,'HA_MANAGED_RELEASE');
  }
  assert.equal(dispatched,0,'Pinned-release updater denial must precede any original handler or external side effect');
  console.log('PASS managed HA release: original dashboard update POST is denied before application dispatch, including query/trailing-slash/encoded variants; no package/source mutation or external updater execution');
  assert.equal(await (await fetch(base + '/normal', options)).text(), 'application response');
  await assert.rejects(fetch(base + '/lose-before-response', options), /fetch failed/);
  assert.equal(dispatched, 2);
  assert.equal((await fetch(base + '/readyz')).status, 503);
  console.log('PASS HTTP boundary: standby rejects before dispatch, active serves, lost ownership before response closes the connection without replay');
  for(const operation of ['write','end']) {
    owned=true;
    const response=await fetch(base+'/lose-after-headers-'+operation,{signal:AbortSignal.timeout(3000)});
    const reader=response.body.getReader();
    assert.equal(new TextDecoder().decode((await reader.read()).value),'first');
    sendLate();await assert.rejects(reader.read(),/terminated|aborted|fetch failed/i);
    assert.equal((await fetch(base+'/readyz')).status,503);
  }
  assert.equal(dispatched,4);
  console.log('PASS in-flight HTTP body fencing: ownership loss after SSE headers rejects both later write and end payloads without replay');
} finally {
  server.closeAllConnections();
  await new Promise(resolve => server.close(resolve));
}

// Reproduce the Node global-Agent idle-timeout inheritance which caused
// intermittent test-router 503s on otherwise healthy browser requests.
const peers=[];let router;
const backend=createServer((req,res)=>{
  if(req.url==='/readyz'){res.end('ready');return;}
  peers.push(req.socket.remotePort);
  if(req.url==='/slow')setTimeout(()=>res.end('completed inside 5s request timeout'),4500);
  else res.end('warm reused connection');
});
try{
  backend.listen(0,'127.0.0.1');await once(backend,'listening');
  router=await trafficRouter(['http://127.0.0.1:'+backend.address().port]);
  assert.equal(await(await fetch(router.base+'/warm')).text(),'warm reused connection');
  await new Promise(resolve=>setTimeout(resolve,20));
  const slow=await fetch(router.base+'/slow',{signal:AbortSignal.timeout(8000)});assert.equal(slow.status,200);assert.equal(await slow.text(),'completed inside 5s request timeout');
  assert.equal(peers[0],peers[1],'The test must exercise the same reused upstream socket');
  console.log('PASS isolated router keep-alive regression: same socket serves a 4.5s response within its unchanged 5s active-request deadline; idle timeout cannot become a false failure, no replay');
}finally{await router?.close();backend.closeAllConnections();await new Promise(resolve=>backend.close(resolve));}
