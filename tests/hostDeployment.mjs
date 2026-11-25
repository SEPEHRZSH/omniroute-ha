// End-to-end service acceptance with two processes on one host.
import assert from 'node:assert/strict';
import {fork,spawn,execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {once} from 'node:events';
import {createServer} from 'node:net';
import {mkdtempSync,mkdirSync,readFileSync,writeFileSync,rmSync,cpSync,symlinkSync} from 'node:fs';
import path from 'node:path';
import {parseEnv} from 'node:util';
import {testDatabase} from './mongoTestSupport.mjs';
import {startRedisHa,waitRedis} from './redisHa.mjs';
import {validateHostEnvironment,loadHostEnvironment,inspectHostInfrastructure,hostStatus} from '../scripts/host.mjs';
import {MongoAdapter} from '../adapter/mongoAdapter.mjs';
import {trafficRouter} from './trafficRouter.mjs';
const test=await testDatabase('hosts'),dir=mkdtempSync(path.resolve('.work/host-acceptance-')),children=[];
let database,redis,bootstrap,router;
const roots=[process.cwd(),process.cwd()],relocate=process.env.HA_TEST_RELOCATE==='1';
const haproxy=process.env.HA_TEST_HAPROXY_BIN;
async function loopbackPort(){
  const socket=createServer();
  await new Promise((resolve,reject)=>{socket.once('error',reject);socket.listen(0,'127.0.0.1',resolve);});
  const {port}=socket.address();
  await new Promise((resolve,reject)=>socket.close(error=>error?reject(error):resolve()));
  return port;
}
function saveEnv(file,env){writeFileSync(file,Object.entries(env).map(([key,value])=>{assert.equal(/[\r\n']/.test(value),false);return key+"='"+value+"'";}).join('\n'),{mode:0o600});}
const stop=async child=>{if(!child||child.exitCode!==null||child.signalCode)return;const exited=once(child,'exit');child.kill('SIGTERM');const timer=setTimeout(()=>child.kill('SIGKILL'),10000);await exited;clearTimeout(timer);};
try{
  const work=path.join(dir,'bootstrap');mkdirSync(work,{mode:0o700});
  const builder=fork(new URL('../scripts/provision.mjs',import.meta.url),[],{execArgv:[],env:{...process.env,HA_MONGO_URI:test.uri,DATA_DIR:work},stdio:['ignore','ignore','inherit','ipc']});children.push(builder);
  let receipt;builder.on('message',message=>receipt=message);assert.equal((await once(builder,'exit'))[0],0);assert.ok(receipt?.database);database=receipt.database;
  redis=await startRedisHa();
  if(relocate){
    roots[1]=path.join(dir,'relocated');mkdirSync(roots[1],{mode:0o700});
    for(const name of ['adapter','scripts','package.json','package-lock.json'])cpSync(path.resolve(name),path.join(roots[1],name),{recursive:true});
    mkdirSync(path.join(roots[1],'.work'),{mode:0o700});
    cpSync(path.resolve('.work/backend'),path.join(roots[1],'.work/backend'),{recursive:true,verbatimSymlinks:true,filter:source=>!source.includes('/.next/cache')&&!/\/\.next\/trace(?:-|$)/.test(source)&&!/^.*\/backend\/(?:data|cache)(?:\/|$)/.test(source)});
    symlinkSync(path.resolve('node_modules'),path.join(roots[1],'node_modules'));
    mkdirSync(path.join(roots[1],'upstream'));symlinkSync(path.resolve('upstream/omniroute'),path.join(roots[1],'upstream/omniroute'));
    mkdirSync(path.join(roots[1],'tests'));mkdirSync(path.join(roots[1],'deploy'));
    // Use the same read-only checkout while resolving imports from the relocated root.
    writeFileSync(path.join(roots[1],'tests/readonly-source-hook.mjs'),`import{registerHooks}from'node:module';const alias=new URL('../upstream/omniroute/',import.meta.url).href;registerHooks({resolve(s,c,next){let url;if(s.startsWith('file:'))url=s;else if(s.startsWith('.')&&c.parentURL?.startsWith('file:'))url=new URL(s,c.parentURL).href;if(url?.startsWith(alias))return{url,shortCircuit:true};return next(s,c);}});`);
  }
  const template=parseEnv(readFileSync('deploy/host.env.example','utf8')),base={...template,...redis.env,HA_MONGO_URI:test.uri,HA_MONGO_DATABASE:database,HA_NEXT_DIR:path.resolve('.work/backend'),OMNIROUTE_HA_IDENTITY:'synthetic-two-host-entry-test',JWT_SECRET:'synthetic-host-jwt-secret-for-tests-only',API_KEY_SECRET:'synthetic-host-api-secret-for-tests-only',STORAGE_ENCRYPTION_KEY:'synthetic-host-storage-secret-for-tests-only',HA_STATUS_TOKEN:'synthetic-host-status-token-for-tests-only',NODE_OPTIONS:'',NODE_PATH:'',LD_PRELOAD:''};
  const envs=[],files=[],origins=[];
  for(let i=0;i<2;i++){const local=path.join(i===1&&relocate?path.join(roots[1],'.work'):dir,'host-'+i);mkdirSync(local,{mode:0o700});const port=await loopbackPort();
    const env={...base,HA_NEXT_DIR:path.join(roots[i],'.work/backend'),HOST:'127.0.0.1',PORT:String(port),DATA_DIR:local,TMPDIR:local,OMNIROUTE_PLUGINS_DIR:path.join(local,'plugins')};
    const file=path.join(local,'host.env');saveEnv(file,env);envs.push(env);files.push(file);origins.push('http://127.0.0.1:'+port);
  }
  const fingerprint=validateHostEnvironment(envs[0]).sharedFingerprint;assert.equal(validateHostEnvironment(envs[1]).sharedFingerprint,fingerprint);
  assert.notEqual(validateHostEnvironment({...envs[1],JWT_SECRET:base.JWT_SECRET+'changed'}).sharedFingerprint,fingerprint);
  for(const edit of [{HA_STATUS_TOKEN:''},{HA_REDIS_MODE:'disabled'},{HA_MONGO_DATABASE:'production'},{PORT:'0'},{NODE_OPTIONS:'--require=untrusted'}])assert.throws(()=>validateHostEnvironment({...envs[0],...edit}));
  assert.deepEqual(loadHostEnvironment(files[0]),envs[0]);
  console.log('PASS independent-host configuration: private dotenv files, path/profile/secret/namespace guards, per-host local paths and shared-secret fingerprint mismatch detection');
  for(let i=0;i<2;i++){envs[i].INITIAL_PASSWORD='Synthetic-host-test-password!';saveEnv(files[i],envs[i]);}
  Object.assign(process.env,envs[0]);delete process.env.INITIAL_PASSWORD;await import('../adapter/register.mjs');bootstrap=new MongoAdapter({uri:test.uri,database});globalThis.__omnirouteMongoAdapter=bootstrap;
  const settings=await import('../upstream/omniroute/src/lib/db/settings.ts');await settings.updateSettings({stickyRoundRobinLimit:23,cloudEnabled:false});
  const {createApiKey}=await import('../upstream/omniroute/src/lib/db/apiKeys.ts');const operator=await createApiKey('Synthetic host acceptance operator','synthetic-host-test',['manage']),revocable=await createApiKey('Synthetic host outage revocation','synthetic-host-test',['manage']);await bootstrap.close();bootstrap=undefined;delete globalThis.__omnirouteMongoAdapter;
  const preflight=await inspectHostInfrastructure(envs[0]);assert.equal(preflight.pass,true);assert.equal(preflight.redis.pubsub,true);assert.equal(preflight.mongo.migrations,222);
  console.log('PASS actual independent-host preflight: replica-set discovery, complete-schema provenance, read-only Mongo ownership and real authenticated Redis Pub/Sub');
  const start=index=>{const child=spawn(process.execPath,['--import',path.resolve('tests/networkGuard.mjs'),...(index===1&&relocate?['--import',path.join(roots[1],'tests/readonly-source-hook.mjs')]:[]),'scripts/host.mjs','start','--env',files[index]],{cwd:roots[index],env:{...process.env,NODE_OPTIONS:'',HA_TEST_REDIS_PORTS:JSON.stringify(redis.nodes.map(node=>node.port)),NODE_TEST_CONTEXT:'isolated-host-entry',LIBC:'glibc'},stdio:['ignore','pipe','pipe']});children.push(child);child.log='';for(const stream of [child.stdout,child.stderr])stream.on('data',data=>{child.log=(child.log+data.toString()).slice(-8000);});return child;};
  const first=start(0);await waitRedis(async()=>{assert.equal(first.exitCode,null,first.log);return(await hostStatus(origins[0],base.HA_STATUS_TOKEN)).active;},'first host-entry readiness',90000);
  const second=start(1);await waitRedis(async()=>{assert.equal(second.exitCode,null,second.log);return(await hostStatus(origins[1],base.HA_STATUS_TOKEN)).role==='standby';},'second independent service entry',60000);
  const request=(index,endpoint,init={})=>fetch(origins[index]+endpoint,{...init,headers:{authorization:'Bearer '+operator.key,'content-type':'application/json'},signal:AbortSignal.timeout(15000)});
  assert.equal((await fetch(origins[1]+'/livez')).status,200);assert.equal((await fetch(origins[1]+'/readyz')).status,503);assert.equal((await fetch(origins[1]+'/ha/status')).status,403);
  const update=await request(0,'/api/system/version?channel=latest',{method:'POST',body:'{}'});assert.equal(update.status,409);assert.equal((await update.json()).code,'HA_MANAGED_RELEASE');
  console.log('PASS actual managed release boundary: native app rejects the dashboard updater before original package/source mutation; no overlay rebuild or upstream modification');
  let response=await request(0,'/api/settings',{method:'PATCH',body:JSON.stringify({stickyRoundRobinLimit:29})});assert.equal(response.status,200,await response.text());
  for(let i=0;i<2;i++){envs[i].HA_ACCEPTANCE_API_KEY=operator.key;saveEnv(files[i],envs[i]);}
  if(haproxy){
    const port=await loopbackPort(),config=path.join(dir,'haproxy.cfg');
    writeFileSync(config,readFileSync('deploy/haproxy.cfg','utf8').replace('bind :8080','bind 127.0.0.1:'+port),{mode:0o600});
    const env={OMNI_PRIMARY:new URL(origins[0]).host,OMNI_STANDBY:new URL(origins[1]).host};
    await promisify(execFile)(haproxy,['-c','-f',config],{env,timeout:10000});
    const ingress=spawn(haproxy,['-db','-f',config],{env,stdio:['ignore','ignore','inherit']});
    children.push(ingress);
    await once(ingress,'spawn');
    router={base:'http://127.0.0.1:'+port,close:()=>stop(ingress)};
    await waitRedis(async()=>{
      const response=await fetch(router.base+'/api/settings',{headers:{authorization:'Bearer '+operator.key},signal:AbortSignal.timeout(2000)});
      await response.arrayBuffer();
      return response.status===200;
    },'HAProxy active readiness',15000);
  }else router=await trafficRouter(origins);
  const record=path.join(dir,'acceptance.json');
  const acceptance=async action=>{const result=await promisify(execFile)(process.execPath,['scripts/accept-hosts.mjs',action,'--env',files[0],'--record',record,...(action==='capture'?['--a',origins[0],'--b',origins[1],'--ingress',router.base]:[])],{timeout:30000,env:{...process.env,NODE_OPTIONS:''}});return result.stdout;};
  assert.equal(JSON.parse(await acceptance('capture')).pass,true);
  const watcher=spawn(process.execPath,['scripts/accept-hosts.mjs','watch-promotion','--env',files[0],'--record',record],{env:{...process.env,NODE_OPTIONS:''},stdio:['ignore','pipe','pipe']});children.push(watcher);let watchLog='';
  await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('Acceptance watcher arm timeout')),15000);watcher.stdout.on('data',chunk=>{watchLog+=chunk.toString();if(watchLog.includes('ARMED:')){clearTimeout(timer);resolve();}});watcher.stderr.on('data',chunk=>{watchLog+=chunk.toString();});watcher.once('exit',code=>{clearTimeout(timer);if(code!==0)reject(Error('Acceptance watcher exited before arm: '+watchLog));});});
  await redis.outage();response=await request(0,'/api/keys/'+revocable.id,{method:'DELETE'});assert.equal(response.status,200,await response.text());
  const old=await hostStatus(origins[0],base.HA_STATUS_TOKEN),failureAt=performance.now();await stop(first);
  const promoted=await waitRedis(async()=>{const state=await hostStatus(origins[1],base.HA_STATUS_TOKEN);return state.active&&state.epoch>old.epoch?state:false;},'promotion without shared parent coordination',90000);
  response=await request(1,'/api/settings');assert.equal(response.status,200);assert.equal((await response.json()).stickyRoundRobinLimit,29);
  if(watcher.exitCode===null && !watcher.signalCode)await once(watcher,'exit');assert.equal(watcher.exitCode,0,watchLog);
  const {combo}=JSON.parse(readFileSync(record,'utf8'));
  assert.notEqual(promoted.instanceId,old.instanceId);
  console.log('PASS Combo through '+(haproxy?'HAProxy':'shared fixture ingress')+' before and after app failover: '+JSON.stringify(combo));
  console.log('PASS observed local service promotion RTO: '+Math.round(performance.now()-failureAt)+' ms from controlled stop request to successful persisted Settings read; not a physical-host/network RTO');
  response=await fetch(origins[1]+'/api/settings',{headers:{authorization:'Bearer '+revocable.key}});assert.equal(response.status,403);
  const restarted=start(0);await waitRedis(async()=>{assert.equal(restarted.exitCode,null,restarted.log);return(await hostStatus(origins[0],base.HA_STATUS_TOKEN)).role==='standby';},'old host-entry rejoins',60000);
  await redis.recover();
  assert.equal(JSON.parse(await acceptance('verify-rejoin')).pass,true);assert.match(await acceptance('cleanup'),/^PASS/);
  console.log('PASS real operator acceptance commands: capture synthetic API Keys/Memory, watch ingress promotion with measured RTO, verify durable ownership/data/revocation and old-host rejoin, remove only owned synthetic records');
  console.log('PASS host launch during real Redis outage: revocation stays authoritative through promotion, old service starts/rejoins without Redis, original configured Sentinel fixture recovers; no cache fail-open or Redis write ownership');
  const topology=await promisify(execFile)(process.execPath,['scripts/host.mjs','topology','--env',files[0],'--url',origins[0],'--peer',origins[1]],{timeout:20000,env:{...process.env,NODE_OPTIONS:''}});assert.equal(JSON.parse(topology.stdout).pass,true);
  console.log('PASS actual host launcher and topology: two private environments/data dirs, shared Mongo/Redis, auth-only role endpoint, standby live/ready distinction, Settings HTTP persistence, promotion epoch '+old.epoch+' -> '+promoted.epoch+', safe old-host restart and durable-owner/native/shared-profile agreement (one OS, not two VMs)');
  if(relocate){if(process.env.HA_TEST_BROWSER_BIN){const {verifyBrowserHttp}=await import('./browser.mjs');await verifyBrowserHttp(router.base,'Synthetic-host-test-password!','relocated service entry');}console.log('PASS relocated application artifact: same compiled build at a different installation root, matching native/shared identities, actual HTTP ownership/Memory/keys and optional private Chrome; shared read-only source/dependencies only for this local fixture, not independent-host acceptance');}
}finally{
  await router?.close();for(const child of children.reverse())await stop(child);await bootstrap?.close();delete globalThis.__omnirouteMongoAdapter;await redis?.close();if(database)await test.client.db(database).dropDatabase();await test.cleanup();rmSync(dir,{recursive:true});
}
