import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {MongoAdapter} from '../adapter/mongoAdapter.mjs';
import {installBaseSchema,readBaseSchema} from '../adapter/baseSchema.mjs';
import {parse,splitStatements} from '../adapter/sql.mjs';
import {testDatabase} from './mongoTestSupport.mjs';
import {createServer} from 'node:http';
import {setTimeout as delay} from 'node:timers/promises';
import {createFreeProxySync} from '../adapter/freeProxySync.mjs';

Object.assign(process.env,{NODE_ENV:'test',DATA_DIR:path.resolve('.work/test-artifacts'),DISABLE_SQLITE_AUTO_BACKUP:'1',OMNIROUTE_DISABLE_BACKGROUND_SERVICES:'true',OMNIROUTE_DISABLE_REDIS_AUTH_CACHE:'true',REDIS_URL:''});
await import('../adapter/register.mjs');
const test=await testDatabase('freeproxy');
const db=new MongoAdapter({uri:test.uri,database:test.database}),sqlite=new DatabaseSync(':memory:');
const sql=name=>readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/'+name+'.sql',import.meta.url),'utf8');
const oracle={prepare:sqlite.prepare.bind(sqlite),exec:sqlite.exec.bind(sqlite),transaction(fn){return (...args)=>{sqlite.exec('BEGIN');try{const value=fn(...args);assert.ok(!(value instanceof Promise));sqlite.exec('COMMIT');return value;}catch(error){sqlite.exec('ROLLBACK');throw error;}};}};
const use=target=>{globalThis.__omnirouteMongoAdapter=target;};
const invoke=async(target,name,...args)=>{use(target);return api[name](...args);};
const record=row=>{const {createdAt,updatedAt,...data}=row;assert.ok(createdAt&&updatedAt);return data;};
let api,active,standby,syncServer,syncController;
try {
  for(const target of [db,oracle]) {
    if(target===db)installBaseSchema(target);else for(const statement of readBaseSchema().statements)target.exec(statement);
    target.exec(sql('004_proxy_registry').match(/CREATE TABLE IF NOT EXISTS proxy_registry \([\s\S]*?\);/)[0]);
    target.exec(sql('040_oneproxy_proxy_fields'));
    for(const statement of splitStatements(sql('068_free_proxies')))if(parse(statement).ast.type==='create')target.exec(statement);
    target.exec(sql('122_free_proxy_sync_errors'));
  }
  sqlite.exec(sql('072_free_proxies_fk'));
  api=await import('../upstream/omniroute/src/lib/db/freeProxies.ts');
  use(oracle);const expected=await api.getFreeProxyStats();
  use(db);assert.deepEqual(await api.getFreeProxyStats(),expected);
  db.installFreeProxyCleanup();
  db.installRequestLimits();
  console.log('PASS original Free Proxy empty stats through the real MongoDB-facing SDK');
  const ids=[];
  for(const [n,source,quality,latency] of [[1,'1proxy',80,40],[2,'1proxy',0,null],[3,'1proxy',null,10],[4,'proxifly',60,30],[5,'proxifly',60,null],[6,'iplocate',null,5]]) {
    const item={source,host:'198.51.100.'+n,port:8000+n,type:'http',countryCode:n%2?'US':'IR',qualityScore:quality,latencyMs:latency,lastValidated:'2099-01-0'+n+'T00:00:00.000Z'};
    const actual=await invoke(db,'upsertFreeProxy',item),expected=await invoke(oracle,'upsertFreeProxy',item);ids.push(actual.id);
    sqlite.prepare('UPDATE free_proxies SET id=? WHERE id=?').run(actual.id,expected.id);
    assert.equal(actual.action,'created');assert.equal(expected.action,'created');
    assert.deepEqual(record(await invoke(db,'getFreeProxyById',actual.id)),record(await invoke(oracle,'getFreeProxyById',actual.id)));
  }
  for(const filters of [{},{sortBy:'latency'},{sortBy:'recent',limit:3,offset:1},{sources:['1proxy'],country:'us',minQuality:50},{search:'100.4',onlyNotInPool:true}]) {
    assert.deepEqual((await invoke(db,'listFreeProxies',filters)).map(record),(await invoke(oracle,'listFreeProxies',filters)).map(record));
    assert.equal(await invoke(db,'countFreeProxies',filters),await invoke(oracle,'countFreeProxies',filters));
  }
  for(const statement of [
    'SELECT id,CASE WHEN quality_score > 50 THEN 1 WHEN quality_score IS NULL THEN 2 ELSE 0 END AS tier FROM free_proxies ORDER BY id',
    'SELECT id,CASE WHEN quality_score = NULL THEN 1 END AS unknown FROM free_proxies ORDER BY id',
    'SELECT source AS origin,COUNT(*) AS count FROM free_proxies GROUP BY source ORDER BY count DESC,origin ASC',
  ])assert.deepEqual(db.prepare(statement).all(),sqlite.prepare(statement).all().map(row=>({...row})));
  for(const statement of ['SELECT AVG(source) AS value FROM free_proxies',"SELECT SUM(CASE WHEN in_pool=0 THEN '10' ELSE '0' END) AS value FROM free_proxies"]){
    const rejected=new MongoAdapter({uri:test.uri,database:test.database});try{assert.throws(()=>rejected.prepare(statement).get(),{code:'HA_UNSUPPORTED_SQL'});}finally{await rejected.close();}
  }
  const updated={source:'1proxy',host:'198.51.100.1',port:8001,type:'https',qualityScore:92,latencyMs:99,countryCode:'US',lastValidated:'2099-02-01T00:00:00.000Z'};
  for(const target of [db,oracle])assert.deepEqual(await invoke(target,'upsertFreeProxy',updated),{id:ids[0],action:'updated'});
  assert.deepEqual(record(await invoke(db,'getFreeProxyById',ids[0])),record(await invoke(oracle,'getFreeProxyById',ids[0])));
  assert.deepEqual(await invoke(db,'getFreeProxyStats'),await invoke(oracle,'getFreeProxyStats'));
  for(const target of [db,oracle]){await invoke(target,'recordFreeProxySync','2099-02-01T00:00:00.000Z');await invoke(target,'recordFreeProxySyncErrors','1proxy',['synthetic source unavailable']);}
  assert.deepEqual(await invoke(db,'getFreeProxyStats'),await invoke(oracle,'getFreeProxyStats'));
  assert.deepEqual(await invoke(db,'getFreeProxySyncErrors'),await invoke(oracle,'getFreeProxySyncErrors'));
  for(const target of [db,oracle])await invoke(target,'markFreeProxyInPool',ids[0],'unattached-synthetic-pool-id');
  assert.deepEqual(record(await invoke(db,'getFreeProxyById',ids[0])),record(await invoke(oracle,'getFreeProxyById',ids[0])),'The original cleanup trigger must not invent an INSERT foreign key');
  const payload={name:'Synthetic promoted proxy',type:'http',host:'198.51.100.4',port:8004,source:'proxifly'};
  const original=db.prepare.bind(db),before=db.prepare('SELECT COUNT(*) AS n FROM proxy_registry').get().n;
  db.prepare=statement=>{const prepared=original(statement);if(statement.startsWith('UPDATE free_proxies SET in_pool = 1'))return {...prepared,run(){throw new Error('synthetic pool attachment failure');}};return prepared;};
  try{await assert.rejects(invoke(db,'promoteFreeProxyToPool',ids[3],payload),/synthetic pool attachment failure/);}finally{db.prepare=original;}
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM proxy_registry').get().n,before);assert.equal((await invoke(db,'getFreeProxyById',ids[3])).inPool,false);
  const registryId=await invoke(db,'promoteFreeProxyToPool',ids[3],payload),oracleRegistry=await invoke(oracle,'promoteFreeProxyToPool',ids[3],payload);
  sqlite.prepare('UPDATE proxy_registry SET id=? WHERE id=?').run(registryId,oracleRegistry);sqlite.prepare('UPDATE free_proxies SET pool_proxy_id=? WHERE id=?').run(registryId,ids[3]);
  assert.deepEqual(record(await invoke(db,'getFreeProxyById',ids[3])),record(await invoke(oracle,'getFreeProxyById',ids[3])));
  // REPLACE suppresses this DELETE trigger with default recursive_triggers=OFF.
  for(const target of [db,oracle])target.prepare("INSERT OR REPLACE INTO proxy_registry(id,name,type,host,port) VALUES (?,?,'http','198.51.100.4',8004)").run(registryId,'Replacement record');
  assert.equal((await invoke(db,'getFreeProxyById',ids[3])).inPool,true);assert.deepEqual(record(await invoke(db,'getFreeProxyById',ids[3])),record(await invoke(oracle,'getFreeProxyById',ids[3])));
  assert.throws(()=>db.transaction(()=>{db.prepare('DELETE FROM proxy_registry WHERE id=?').run(registryId);assert.equal((db.prepare('SELECT in_pool FROM free_proxies WHERE id=?').get(ids[3])).in_pool,0);throw new Error('rollback registry cleanup');})(),/rollback registry cleanup/);
  assert.equal((await invoke(db,'getFreeProxyById',ids[3])).inPool,true);
  for(const target of [db,oracle])target.prepare('DELETE FROM proxy_registry WHERE id=?').run(registryId);
  assert.deepEqual(record(await invoke(db,'getFreeProxyById',ids[3])),record(await invoke(oracle,'getFreeProxyById',ids[3])));
  assert.equal((await invoke(db,'getFreeProxyById',ids[3])).poolProxyId,null);
  for(const name of ['pruneStaleFreeProxies','clearFreeProxiesBySource']) {
    const args=name==='pruneStaleFreeProxies'?['1proxy',new Set(['198.51.100.3:8003'])]:['1proxy'];
    assert.equal(await invoke(db,name,...args),await invoke(oracle,name,...args));
    assert.deepEqual((await invoke(db,'listFreeProxies')).map(record),(await invoke(oracle,'listFreeProxies')).map(record));
    assert.equal((await invoke(db,'getFreeProxyById',ids[0])).inPool,true,'Cleanup must preserve already-pooled candidates');
  }
  for(const target of [db,oracle]){assert.equal(await invoke(target,'deleteFreeProxy',ids[5]),true);await invoke(target,'clearFreeProxySyncErrors','1proxy');}
  assert.equal(await invoke(db,'getFreeProxyById',ids[5]),null);assert.deepEqual(await invoke(db,'getFreeProxySyncErrors'),{});
  console.log('PASS original Free Proxy SDK: SQLite-differential writes, filters/null latency, AVG/CASE/group ordering, sync state, arbitrary pool IDs, atomic promotion/rollback and DELETE versus REPLACE cleanup');
  await test.db.collection('_ha_schema').updateOne({_id:'proxy_registry'},{$unset:{freeProxyCleanup:''}});
  const incomplete=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-free-incomplete'});
  try{assert.throws(()=>incomplete.verifyBaseSchema(),{code:'HA_SCHEMA_MISMATCH'});assert.equal(incomplete.canWrite(),false);}finally{await incomplete.close();}
  db.installFreeProxyCleanup();
  const verified=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-free-verified'});
  try{assert.deepEqual(verified.verifyBaseSchema(),{tables:17,indexes:22});}finally{await verified.close();}
  console.log('PASS Free Proxy readiness: incomplete native cleanup metadata refuses ownership readiness; offline installation and a fresh worker recover');
  const finalRegistry=await invoke(db,'promoteFreeProxyToPool',ids[3],payload);
  active=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-free-active'});standby=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-free-standby'});
  assert.ok(active.ownership('claim',10000));assert.equal(standby.ownership('claim',10000),null);
  assert.throws(()=>standby.prepare('DELETE FROM proxy_registry WHERE id=?').run(finalRegistry),{code:'HA_FENCED'});assert.equal((await invoke(db,'getFreeProxyById',ids[3])).inPool,true);
  active.ownership('release',10000);assert.ok(standby.ownership('claim',10000));
  assert.throws(()=>active.prepare('DELETE FROM proxy_registry WHERE id=?').run(finalRegistry),{code:'HA_FENCED'});
  standby.prepare('DELETE FROM proxy_registry WHERE id=?').run(finalRegistry);assert.equal((await invoke(db,'getFreeProxyById',ids[3])).poolProxyId,null);standby.ownership('release',10000);
  console.log('PASS Free Proxy fencing: standby and stale DELETE cannot clear references; promoted owner commits registry deletion and cleanup together');
  assert.ok(standby.ownership('claim',10000));use(standby);
  const runtime={db:standby,assertOwner(){assert.ok(standby.ownership('status'),'Active ownership required');},ready(){return !!standby.ownership('status');},retire(reason){throw new Error(reason);}};
  syncController=createFreeProxySync(runtime);globalThis.__omnirouteFreeProxySync=syncController;
  const {runFreeProxySyncCycle}=await import('../upstream/omniroute/src/lib/freeProxyProviders/syncCycle.ts');
  let requests=0,held,release;
  syncServer=createServer((req,res)=>{
    assert.equal(new URL(req.url,'http://localhost').pathname,'/synthetic-free-proxies');requests++;
    const port=requests===2?8082:8081;
    const finish=()=>res.end(JSON.stringify({proxies:[{ip:'93.184.216.34',port,protocol:'http',quality_score:77,country_code:'ZZ'}]}));
    res.setHeader('content-type','application/json');
    if(requests===2){held=res;release=finish;}else finish();
  });
  await new Promise(resolve=>syncServer.listen(0,'127.0.0.1',resolve));
  Object.assign(process.env,{FREE_PROXY_AUTO_SYNC_ENABLED:'true',FREE_PROXY_AUTO_SYNC_INTERVAL_MS:'300000',FREE_PROXY_1PROXY_API_URL:'http://127.0.0.1:'+syncServer.address().port+'/synthetic-free-proxies',FREE_PROXY_PROXIFLY_ENABLED:'false',FREE_PROXY_IPLOCATE_ENABLED:'false',FREE_PROXY_WEBSHARE_ENABLED:'false'});
  await api.recordFreeProxySync('2000-01-01T00:00:00.000Z');
  await syncController.start();assert.equal(syncController.state.started,true);
  const deadline=Date.now()+8000;
  while(syncController.state.runs<1&&Date.now()<deadline)await delay(25);
  assert.equal(syncController.state.runs,1);assert.equal(requests,1);
  assert.equal((await api.listFreeProxies({search:'93.184.216.34'}))[0].port,8081);
  assert.match(syncController.state.lastSyncAt,/^\d{4}-/);
  const first=runFreeProxySyncCycle();
  const holdDeadline=Date.now()+3000;while(!held&&Date.now()<holdDeadline)await delay(20);assert.ok(held);
  await assert.rejects(runFreeProxySyncCycle(),{code:'HA_FREE_PROXY_SYNC_BUSY'});assert.equal(requests,2);
  const previous=syncController.state.lastSyncAt;
  standby.ownership('release',10000);assert.ok(active.ownership('claim',10000));
  release();await assert.rejects(first);assert.equal(syncController.state.lastSyncAt,previous);
  assert.equal((await test.db.collection('free_proxies').findOne({host:'93.184.216.34'})).port,8081);
  assert.equal(await test.db.collection('free_proxies').countDocuments({host:'93.184.216.34',port:8082}),0,'Late source data must not enter the promoted database');
  await assert.rejects(runFreeProxySyncCycle());assert.equal(requests,2,'Stale owner must not make another source request');
  syncController.stop();assert.equal(syncController.state.started,false);
  assert.equal(globalThis.__freeProxyAutoSyncInterval,undefined);assert.equal(globalThis.__freeProxyAutoSyncStartupTimer,undefined,'Original independent scheduler must remain disabled');
  console.log('PASS owned Free Proxy auto-sync: actual original Oneproxy HTTP/parser/catalog and original cycle, delayed opt-in timer, manual/auto non-overlap, stale response/timestamp refusal and no independent source timers');
} finally {syncController?.stop();syncServer?.closeAllConnections();if(syncServer)await new Promise(resolve=>syncServer.close(resolve));delete globalThis.__omnirouteFreeProxySync;delete globalThis.__omnirouteMongoAdapter;await active?.close();await standby?.close();await db.close();sqlite.close();await test.cleanup();}
