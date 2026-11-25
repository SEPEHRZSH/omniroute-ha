// Independent-host staging operations. No shell evaluation, database reset,
// implicit infrastructure provisioning or production namespace enablement.
import assert from 'node:assert/strict';
import {readFileSync,lstatSync,mkdirSync} from 'node:fs';
import {parseEnv} from 'node:util';
import {createHmac,randomUUID} from 'node:crypto';
import {spawn,spawnSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';
import path from 'node:path';
import {MongoClient} from 'mongodb';
import {verifyBuildContract} from '../adapter/buildContract.mjs';
import {workspacePath} from '../adapter/workspacePath.mjs';
import {redisConfiguration,createRedisAccess} from '../adapter/redis.mjs';
import {nativeRevision} from '../adapter/runtimeIdentity.mjs';
const root=path.resolve(import.meta.dirname,'..');
const required=['HA_MONGO_URI','HA_MONGO_DATABASE','HA_NEXT_DIR','DATA_DIR','TMPDIR','OMNIROUTE_HA_IDENTITY','JWT_SECRET','API_KEY_SECRET','STORAGE_ENCRYPTION_KEY','HA_STATUS_TOKEN'];
export function validateHostEnvironment(env){
  assert.equal(process.platform,'linux','The pinned deployment runtime targets Linux');assert.equal(process.arch,'x64','The pinned deployment runtime targets x86-64');
  assert.equal(process.versions.node,'22.22.2','Use the pinned project-local Node 22.22.2');
  for(const key of required)assert.ok(typeof env[key]==='string'&&env[key].trim(),'Missing required setting: '+key);
  for(const key of ['JWT_SECRET','API_KEY_SECRET','STORAGE_ENCRYPTION_KEY','HA_STATUS_TOKEN'])assert.ok(env[key].length>=32,key+' must have at least 32 characters');
  assert.match(env.HA_MONGO_DATABASE,/^ha_test_[a-z0-9_]+$/,'Only an explicitly isolated staging namespace is supported');
  assert.match(env.HA_MONGO_URI,/^mongodb(?:\+srv)?:\/\//,'Invalid MongoDB URI');
  assert.ok(!/[\r\n]/.test(env.HA_MONGO_URI),'Invalid MongoDB URI');
  const parameters=new Map([...new URLSearchParams(env.HA_MONGO_URI.split('?')[1]||'')].map(([key,value])=>[key.toLowerCase(),value.toLowerCase()]));
  for(const key of ['directconnection','loadbalanced','tlsinsecure','tlsallowinvalidcertificates','tlsallowinvalidhostnames'])assert.notEqual(parameters.get(key),'true','HA/TLS bypass option is forbidden: '+key);
  const port=Number(env.PORT);assert.ok(Number.isInteger(port)&&port>=1&&port<=65535,'PORT must be 1..65535');
  assert.ok(env.HOST&&env.HOST!=='localhost','Set an explicit private bind address or 0.0.0.0');
  for(const key of ['OMNIROUTE_HEADLESS','OMNIROUTE_DISABLE_BACKGROUND_SERVICES','OMNIROUTE_DISABLE_CREDENTIAL_HEALTH_CHECK','OMNIROUTE_DISABLE_REDIS_AUTH_CACHE','OMNIROUTE_DISABLE_CLI_TOKEN','DISABLE_SQLITE_AUTO_BACKUP','REQUIRE_API_KEY'])assert.ok(['true','1'].includes(env[key]),'Validated HA profile requires '+key);
  for(const key of ['DATA_DIR','TMPDIR'])workspacePath(env[key],{directory:true,existing:false});
  const appDirectory=workspacePath(env.HA_NEXT_DIR,{directory:true});verifyBuildContract(appDirectory);
  const redis=redisConfiguration(env);assert.notEqual(redis.mode,'disabled','Two-host profile requires explicitly configured Redis');
  for(const key of ['NODE_OPTIONS','NODE_PATH','LD_PRELOAD'])assert.ok(!env[key],'Unsupported executable injection setting: '+key);
  assert.notEqual(env.NODE_TLS_REJECT_UNAUTHORIZED,'0','TLS certificate verification cannot be disabled');assert.notEqual(process.env.NODE_TLS_REJECT_UNAUTHORIZED,'0','Inherited TLS certificate verification cannot be disabled');
  const shared=Object.keys(env).filter(key=>key!=='OMNIROUTE_PLUGINS_DIR'&&/^(HA_MONGO_|HA_REDIS_|HA_QUOTA_|JWT_SECRET$|API_KEY_SECRET$|STORAGE_ENCRYPTION_KEY$|HA_STATUS_TOKEN$|OMNIROUTE_|QUOTA_|MEMORY_|FREE_PROXY_|TZ$|REDIS_KEY_PREFIX$|REDIS_URL$|REQUIRE_API_KEY$|INITIAL_PASSWORD$|PROXY_HEALTH_ENABLED$|NODE_ENV$|NEXT_PUBLIC_BASE_URL$)/.test(key)).sort();
  return{port,appDirectory,redisMode:redis.mode,sharedFingerprint:createHmac('sha256',env.STORAGE_ENCRYPTION_KEY).update(JSON.stringify(shared.map(key=>[key,env[key]]))).digest('hex')};
}
export function loadHostEnvironment(filename){
  const file=workspacePath(filename),stat=lstatSync(file);assert.equal(stat.uid,process.getuid(),'Environment file must belong to the service/operator user');assert.equal(stat.mode&0o077,0,'Environment file must be private (0600 or stricter)');
  const env=parseEnv(readFileSync(file,'utf8'));validateHostEnvironment(env);return env;
}
export async function inspectHostInfrastructure(env,{requireRedis=true}={}){
  const checked=validateHostEnvironment(env),client=new MongoClient(env.HA_MONGO_URI,{maxPoolSize:2,minPoolSize:0,timeoutMS:7000,serverSelectionTimeoutMS:5000,readConcern:{level:'majority'}}),redis=createRedisAccess(env);let subscriber;
  try{
    await client.connect();const hello=await client.db('admin').command({hello:1});assert.ok(hello.setName&&hello.isWritablePrimary,'MongoDB replica-set PRIMARY required');assert.ok(hello.hosts?.length>=3,'At least three discovered MongoDB data members required');
    const db=client.db(env.HA_MONGO_DATABASE),writer=await db.collection('_ha_serial').findOne({_id:'writer'});assert.ok(writer,'Provision this database once before host startup');assert.ok(!writer.ddl,'Database is gated for offline maintenance/reconciliation');
    const migrations=await db.collection('_omniroute_migrations').countDocuments();assert.equal(migrations,222,'Pinned complete schema/migration provenance required');
    let redisStatus;
    try{
    const publisher=await redis.get();assert.equal(await publisher.ping(),'PONG');subscriber=publisher.duplicate({lazyConnect:true});subscriber.on('error',()=>{});await subscriber.connect();
    const channel=(env.REDIS_KEY_PREFIX||'omniroute:')+'preflight:'+randomUUID();
    let timer;const received=new Promise((resolve,reject)=>{timer=setTimeout(()=>reject(Error('Redis preflight Pub/Sub timeout')),5000);subscriber.on('message',(name,message)=>{if(name===channel&&message==='synthetic-preflight')resolve();});});
    try{await subscriber.subscribe(channel);await publisher.publish(channel,'synthetic-preflight');await received;}finally{clearTimeout(timer);received.catch(()=>{});}
    redisStatus={mode:checked.redisMode,ping:true,pubsub:true,degraded:false};
    }catch{if(requireRedis)throw Error('Redis preflight unavailable');redisStatus={mode:checked.redisMode,ping:false,pubsub:false,degraded:true};}
    return{pass:true,buildId:JSON.parse(readFileSync(path.join(checked.appDirectory,'.next/ha-complete.json'))).buildId,nativeRevision:nativeRevision(),sharedFingerprint:checked.sharedFingerprint,mongo:{replicaSet:hello.setName,primary:hello.primary,discoveredDataMembers:hello.hosts.length,migrations,epoch:writer.epoch},redis:redisStatus};
  }finally{subscriber?.disconnect();redis.close();await client.close();}
}
export async function hostStatus(base,token,{timeoutMs=10000}={}){
  const url=new URL(base);assert.ok(['http:','https:'].includes(url.protocol)&&!url.username&&!url.password&&!url.search&&!url.hash&&url.pathname==='/','Expected private application origin');
  const response=await fetch(new URL('/ha/status',url),{headers:{authorization:'Bearer '+token},redirect:'error',signal:AbortSignal.timeout(timeoutMs)});assert.equal(response.status,200,'Authenticated host status unavailable');
  const state=await response.json();assert.equal(typeof state.active,'boolean','Invalid HA status payload');assert.ok(['active','standby','starting','promoting','retired'].includes(state.role));assert.equal(state.active,state.role==='active');assert.match(state.instanceId,/^[a-f0-9-]{36}$/);assert.ok(state.epoch===null||Number.isSafeInteger(state.epoch)&&state.epoch>0);return state;
}
async function main(){
  const [action,...args]=process.argv.slice(2);
  if(action==='service'){
    const operation=args[args.indexOf('--action')+1];assert.ok(['start','stop','restart','status'].includes(operation),'Use service --action start|stop|restart|status [--print]');
    const command=['--no-ask-password',operation,'omniroute-ha.service'];
    if(args.includes('--print')){console.log(JSON.stringify({command:['systemctl',...command],requiredWorkingDirectory:root,requiredEntry:path.join(root,'scripts/host.mjs')},null,2));return;}
    const read=property=>{const result=spawnSync('systemctl',['show','omniroute-ha.service','--property='+property,'--value'],{encoding:'utf8'});assert.ok(!result.error&&result.status===0,'Cannot inspect installed service; use the approved operator procedure');return result.stdout.trim();};
    assert.equal(read('WorkingDirectory'),root,'Refusing a service from another installation');assert.ok(read('ExecStart').includes(path.join(root,'scripts/host.mjs')),'Installed unit does not use this HA launcher');
    const result=spawnSync('systemctl',command,{stdio:'inherit'});assert.ok(!result.error&&result.status===0,'Service operation denied or failed; no sudo/permission bypass attempted');return;
  }
  const index=args.indexOf('--env');assert.ok(index>=0&&args[index+1],'Use host.mjs check|prepare|preflight|start|status|topology --env PRIVATE_ENV [--url ORIGIN] [--peer ORIGIN]');
  const env=loadHostEnvironment(args[index+1]);
  if(action==='check'){console.log(JSON.stringify({pass:true,...validateHostEnvironment(env)},null,2));return;}
  if(action==='prepare'){for(const key of ['DATA_DIR','TMPDIR','OMNIROUTE_PLUGINS_DIR'])if(env[key]){const target=workspacePath(env[key],{directory:true,existing:false});mkdirSync(target,{recursive:true,mode:0o700});}console.log('PREPARED private project-local runtime directories; no packages or system services installed');return;}
  if(action==='preflight'){console.log(JSON.stringify(await inspectHostInfrastructure(env),null,2));return;}
  if(action==='start'){
    const health=await inspectHostInfrastructure(env,{requireRedis:false});if(health.redis.degraded)console.warn('[HA] Redis is unavailable; MongoDB remains authoritative and native cache reconnect stays enabled');
    const child=spawn(process.execPath,[...process.execArgv,path.join(root,'adapter/serve.mjs')],{cwd:root,env:{...process.env,...env,HA_SHARED_FINGERPRINT:validateHostEnvironment(env).sharedFingerprint},stdio:'inherit'});
    for(const signal of ['SIGINT','SIGTERM'])process.on(signal,()=>child.kill(signal));child.on('error',()=>{process.exitCode=1;});child.on('exit',(code,signal)=>{process.exitCode=code??(signal?1:0);});return;
  }
  if(action==='status'||action==='topology'){
    const value=name=>{const i=args.indexOf(name);assert.ok(i>=0&&args[i+1],'Missing '+name);return args[i+1];};
    const a=await hostStatus(value('--url'),env.HA_STATUS_TOKEN);
    if(action==='status'){console.log(JSON.stringify(a,null,2));return;}
    const peer=value('--peer');assert.notEqual(new URL(peer).host,new URL(value('--url')).host,'Expected distinct application endpoints');const b=await hostStatus(peer,env.HA_STATUS_TOKEN);
    assert.equal(a.buildId,b.buildId,'Mixed application artifacts');assert.notEqual(a.instanceId,b.instanceId,'Both endpoints resolve to one process');assert.equal([a,b].filter(state=>state.active).length,1,'Exactly one Active required');
    assert.equal(a.buildId,JSON.parse(readFileSync(path.join(env.HA_NEXT_DIR,'.next/ha-complete.json'))).buildId,'Host artifact differs from the configured release');
    for(const state of [a,b]){assert.equal(state.nativeRevision,nativeRevision(),'Mixed native runtime releases');assert.equal(state.sharedFingerprint,validateHostEnvironment(env).sharedFingerprint,'Shared host configuration/secrets mismatch');}
    const client=new MongoClient(env.HA_MONGO_URI,{maxPoolSize:1,timeoutMS:7000});try{await client.connect();const owner=await client.db(env.HA_MONGO_DATABASE).collection('_ha_serial').findOne({_id:'writer'},{readConcern:{level:'majority'}});const active=[a,b].find(state=>state.active);assert.equal(active.instanceId,owner.ownerId);assert.equal(active.epoch,owner.epoch);console.log(JSON.stringify({pass:true,scope:'Endpoint/ownership validation; does not prove independent physical hosts',hosts:[a,b],durableEpoch:owner.epoch},null,2));}finally{await client.close();}return;
  }
  throw Error('Unknown host action');
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href)main().catch(error=>{console.error('HOST_CHECK_FAILED '+(error.code||error.name)+': '+(error.name==='AssertionError'?error.message:'Inspect private configuration, connectivity and service logs; secrets/URIs omitted'));process.exitCode=1;});
