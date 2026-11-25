import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdirSync} from 'node:fs';
import path from 'node:path';
import {MongoAdapter} from '../adapter/mongoAdapter.mjs';
import {testDatabase} from './mongoTestSupport.mjs';
import {usageRollupSql,teamRollupSql} from '../adapter/retentionQueries.mjs';
import {usageSpendSql,unpricedUsageSql,providerMetricsSql} from '../adapter/usageQueries.mjs';
import {checkKeyUsage} from '../adapter/keyUsage.mjs';
import {createRetentionRuntime} from '../adapter/retentionRuntime.mjs';
Object.assign(process.env,{DATA_DIR:path.resolve('.work/domain-data'),DISABLE_SQLITE_AUTO_BACKUP:'1',NODE_ENV:'test',STORAGE_ENCRYPTION_KEY:'synthetic-storage-key-for-ha-tests-only'});
mkdirSync(process.env.DATA_DIR,{recursive:true,mode:0o700});
await import('../adapter/register.mjs');
const test=await testDatabase('rollup'),db=new MongoAdapter({uri:test.uri,database:test.database}),oracle=new DatabaseSync(':memory:');
let primary,standby,verifier;
const sqlite={prepare:oracle.prepare.bind(oracle),exec:oracle.exec.bind(oracle),assertCompatible(){},verifyBaseSchema(){return {tables:17,indexes:22};},installRequestLimits(){},installMemoryTextIndex(){},installRequestDetailRetention(){},installFreeProxyCleanup(){},
  transaction(fn){return (...args)=>{oracle.exec('BEGIN');try{const value=fn(...args);assert.ok(!(value instanceof Promise));oracle.exec('COMMIT');return value;}catch(error){oracle.exec('ROLLBACK');throw error;}};}};
const sort=rows=>rows.map(row=>Object.fromEntries(Object.entries(row).sort(([a],[b])=>a.localeCompare(b)))).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
const retention=createRetentionRuntime(db,{assertOwner(){assert.equal(db.canWrite(),true);}});
function select(target){globalThis.__omnirouteMongoAdapter=target;globalThis.__omnirouteRetention=target===db?retention:{run:fn=>fn(),prices(){},finish(){}};}
const before='2026-01-02',columns=['api_key_id','api_key_name','billing_team_id','provider','model','timestamp','success','tokens_input','tokens_output','tokens_cache_read','tokens_cache_creation','tokens_reasoning','provider_cost_usd','service_tier','team_rollup_processed_at'];
const insert='INSERT INTO usage_history('+columns.join(',')+') VALUES ('+columns.map(()=>'?').join(',')+')';
function add(target,changes={}) {
  const row={api_key_id:'a',api_key_name:'A',billing_team_id:'team-a',provider:'P',model:'M',timestamp:'2026-01-01T10:00:00.000Z',success:1,tokens_input:2,tokens_output:1,tokens_cache_read:10,tokens_cache_creation:0,tokens_reasoning:0,provider_cost_usd:null,service_tier:'standard',team_rollup_processed_at:null,...changes};
  target.prepare(insert).run(...columns.map(key=>row[key]));
}
async function summaries(target) {
  const global=target.prepare('SELECT provider,model,date,total_requests,total_input_tokens,total_output_tokens,total_cost FROM daily_usage_summary').all();
  const team=target.prepare('SELECT * FROM daily_team_usage_summary').all().map(row=>({...row,
    token_shapes_json:row.token_shapes_json?Object.fromEntries(Object.entries(JSON.parse(row.token_shapes_json)).sort()):null,
    successful_token_shapes_json:row.successful_token_shapes_json?Object.fromEntries(Object.entries(JSON.parse(row.successful_token_shapes_json)).sort()):null}));
  return {global:sort(global),team:sort(team)};
}
try {
  const {provisionStartupSchema}=await import('../adapter/startupSchema.mjs');
  for(const target of [db,sqlite]) {
    select(target);
    const log=console.log;try{console.log=()=>{};await provisionStartupSchema(target);}finally{console.log=log;}
    target.prepare("INSERT INTO key_value(namespace,key,value) VALUES ('pricing','p',?)").run(JSON.stringify({m:{input:1000000,output:2000000,cached:100000}}));
    for(const id of ['team-a','team-b'])target.prepare('INSERT INTO teams(id,name,created_at,updated_at) VALUES (?,?,?,?)').run(id,id,'2025-01-01','2025-01-01');
    for(const changes of [{},{provider:'p',model:'m',success:0,api_key_name:'Z'},
      {tokens_input:10,tokens_cache_read:0},{tokens_input:100,provider_cost_usd:0},{provider_cost_usd:3.25},
      {service_tier:'',success:null,tokens_input:null,tokens_output:null},
      {api_key_id:'b',billing_team_id:'team-b',timestamp:'2025-12-31T23:30:00-03:00',provider:'ÖZ',model:'Model'},
      {provider:null,model:null,api_key_id:null,api_key_name:null},
      {team_rollup_processed_at:'2025-12-31'},{timestamp:'2026-01-03T00:00:00.000Z'}])add(target,changes);
    target.prepare('INSERT INTO daily_team_usage_summary(team_id,api_key_id,provider,model,date,total_requests,successful_requests,total_input_tokens,successful_input_tokens,token_shapes_json,successful_token_shapes_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)')
      .run('team-a','a','p','m','2026-01-01',3,2,3,2,'{"[1,0,0,0,0]":1,"[1,0,0,0,0]":2}','{"[1,0,0,0,0]":1,"[1,0,0,0,0]":1}');
    target.prepare('INSERT INTO daily_team_usage_summary(team_id,api_key_id,provider,model,date,total_requests,successful_requests) VALUES (?,?,?,?,?,?,?)')
      .run('team-a','unknown','__unknown_provider__','__unknown_model__','2026-01-01',5,1);
  }
  for(const target of [db,sqlite]){
    for(const[id,enabled]of [['report-a',1],['report-b',0]])target.prepare('INSERT INTO api_keys(id,name,key,created_at,usage_limit_enabled,daily_usage_limit_usd) VALUES (?,?,?,?,?,?)').run(id,id,'synthetic-'+id,'2026-01-04',enabled,10);
    for(const changes of [{api_key_id:'report-a'},{api_key_id:'report-a'},{api_key_id:'report-b'},{api_key_id:null},{api_key_id:'report-a',provider_cost_usd:0}])add(target,{provider:'REPORT',model:'Missing',timestamp:'2026-01-04T00:00:00.000Z',billing_team_id:null,...changes});
  }
  const {getApiKeyTotalsRows,getApiKeyMetadataRows}=await import('../upstream/omniroute/src/lib/db/usageAnalytics.ts');
  const keyQueries=[getApiKeyTotalsRows,getApiKeyMetadataRows],keyResults=[];
  for(const target of [db,sqlite]){
    select(target);
    const temporary=[];
    for(const [id,name]of [['','Legacy key'],[null,'Legacy key'],['a','Renamed key']]){
      const values={api_key_id:id,api_key_name:name,billing_team_id:null,provider:'ÖZ',model:'MODEL',timestamp:'2026-01-05',success:1,tokens_input:2,tokens_output:1,tokens_cache_read:0,tokens_cache_creation:0,tokens_reasoning:0,provider_cost_usd:0,service_tier:null,team_rollup_processed_at:null};
      temporary.push(target.prepare(insert).run(...columns.map(key=>values[key])).lastInsertRowid);
    }
    keyResults.push(keyQueries.flatMap(query=>[
      sort(query("WHERE ((api_key_id IS NOT NULL AND api_key_id != '') OR (api_key_name IS NOT NULL AND api_key_name != ''))",{})),
      sort(query('WHERE timestamp >= @sinceIso AND api_key_id = @keyId',{sinceIso:'2026-01-02',keyId:'a'})),
      sort(query("WHERE api_key_id = 'missing'",{})),
    ]));
    for(const id of temporary)target.prepare('DELETE FROM usage_history WHERE id=?').run(id);
  }
  select(db);assert.deepEqual(keyResults[0],keyResults[1]);
  console.log('PASS original API Manager analytics SQL vs SQLite: NULLIF/ASCII LOWER grouping, output aliases and derived grouped expressions, qualified CASE cost sums, zero/null costs, renamed/blank keys, named filters and empty input');
  const reportArgs={sinceIso:'2026-01-04T00:00:00.000Z'},affectedArgs={...reportArgs,labels:JSON.stringify(['report/missing','report/missing'])};
  for(const target of [db,sqlite]){
    target.prepare('INSERT INTO provider_connections(id,provider,created_at,updated_at) VALUES (?,?,?,?)').run('metrics-live','metrics-live','2026-01-01','2026-01-01');
    let n=0;
    for(const[provider,status,duration,error,type]of [
      ['metrics-live',500,10,'old failure',null],['metrics-live',null,null,null,null],['metrics-live',200,15,null,null],
      ['metrics-live',200,999,null,'upstream_http_attempt'],['metrics-deleted',500,10,'deleted',null],['duckduckgo-web',400,-2.5,'error',null],
      ['-',500,10,null,null],[null,500,10,null,null],
    ])target.prepare('INSERT INTO call_logs(id,provider,status,duration,error_summary,request_type,timestamp) VALUES (?,?,?,?,?,?,?)').run('metric-'+(++n),provider,status,duration,error,type,'2026-01-01T00:00:00.000Z');
  }
  assert.deepEqual(sort(db.prepare(providerMetricsSql[0]).all()),sort(oracle.prepare(providerMetricsSql[0]).all()));
  assert.deepEqual(sort(db.prepare(providerMetricsSql[1]).all({since:'2026-01-01'})),sort(oracle.prepare(providerMetricsSql[1]).all({since:'2026-01-01'})));
  for(const target of [db,sqlite]){target.prepare('DELETE FROM call_logs').run();target.prepare('DELETE FROM provider_connections WHERE id=?').run('metrics-live');}
  console.log('PASS original provider metrics SQL vs SQLite: live/deleted/no-auth providers, wire-attempt exclusion, status/error tie ordering, NULL duration/status and signed rounding');
  assert.deepEqual(sort(db.prepare(unpricedUsageSql[0]).all(reportArgs)),sort(oracle.prepare(unpricedUsageSql[0]).all(reportArgs)));
  assert.equal(db.prepare(unpricedUsageSql[1]).get(affectedArgs).affected,oracle.prepare(unpricedUsageSql[1]).get(affectedArgs).affected);
  assert.equal(db.prepare(unpricedUsageSql[1]).get(affectedArgs).affected,1);
  for(const target of [db,sqlite]){target.prepare('DELETE FROM usage_history WHERE timestamp>=?').run(reportArgs.sinceIso);for(const id of ['report-a','report-b'])target.prepare('DELETE FROM api_keys WHERE id=?').run(id);}
  console.log('PASS original unpriced usage report SQL: SQLite-differential case grouping, distinct/null keys, limited-key join, zero provider-cost exclusion and JSON label membership');
  verifier=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-archive-schema-verifier'});
  assert.deepEqual(verifier.verifyBaseSchema(),{tables:17,indexes:22});
  await test.db.collection('_ha_usage_history').dropIndex('ha_financial_history');
  try{assert.throws(()=>verifier.verifyBaseSchema(),{code:'HA_SCHEMA_MISMATCH'});}
  finally{await test.db.collection('_ha_usage_history').createIndex({api_key_id:1,timestamp:1},{name:'ha_financial_history',commitQuorum:'majority'});}
  assert.throws(()=>verifier.verifyBaseSchema(),{code:'HA_COMPATIBILITY_FAULT'});
  await verifier.close();verifier=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-archive-schema-restarted'});
  assert.deepEqual(verifier.verifyBaseSchema(),{tables:17,indexes:22});await verifier.close();verifier=undefined;
  console.log('PASS archive startup gate: missing index poisons the owned application; offline index repair plus a fresh process is required for schema verification');
  assert.deepEqual(sort(db.prepare(usageRollupSql).all(before)),sort(sqlite.prepare(usageRollupSql).all(before)));
  console.log('PASS usage rollup SELECT vs SQLite: exact request token shapes, stored/zero costs, failed requests, service tiers, ASCII case, nulls and UTC-offset days');
  const {rollupUsageHistoryBeforeDate}=await import('../upstream/omniroute/src/lib/usage/aggregateHistory.ts');
  const {invalidateDbCache}=await import('../upstream/omniroute/src/lib/db/readCache.ts');
  for(const target of [db,sqlite]) {
    select(target);invalidateDbCache();
    const result=await rollupUsageHistoryBeforeDate(before);assert.equal(result.errors,0);assert.ok(result.inserted>0);
  }
  assert.deepEqual(await summaries(db),await summaries(sqlite));
  const initial=await summaries(db);
  for(const target of [db,sqlite]){select(target);invalidateDbCache();assert.equal((await rollupUsageHistoryBeforeDate(before)).errors,0);}
  assert.deepEqual(await summaries(db),initial);assert.deepEqual(await summaries(db),await summaries(sqlite));
  console.log('PASS actual original usage rollup: source pricing, request-shape clamping, provider-reported costs, global/Team summaries, duplicate JSON count merge, legacy unknown shapes and repeat idempotence');
  for(const target of [db,sqlite]){add(target,{tokens_input:7,tokens_output:4,provider_cost_usd:2});select(target);invalidateDbCache();assert.equal((await rollupUsageHistoryBeforeDate(before)).errors,0);}
  assert.deepEqual(await summaries(db),await summaries(sqlite));
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM usage_history WHERE team_rollup_processed_at IS NULL AND timestamp < ?').get(before).n,0);
  console.log('PASS incremental Team rollup: new raw rows add once; prior processed rows are excluded and the marker commits with summaries');
  select(db);add(db,{tokens_input:9});const saved=await summaries(db),prepare=db.prepare.bind(db);
  db.prepare=sql=>sql===teamRollupSql?{run(){throw new Error('synthetic Team rollup failure');}}:prepare(sql);
  try{assert.equal((await rollupUsageHistoryBeforeDate(before)).errors,1);}finally{db.prepare=prepare;}
  assert.deepEqual(await summaries(db),saved);assert.equal(db.prepare('SELECT COUNT(*) AS n FROM usage_history WHERE team_rollup_processed_at IS NULL AND timestamp < ?').get(before).n,1);
  assert.equal(db.compatibilityFault,undefined);
  console.log('PASS actual rollup rollback: a failed Team write cannot leave global summaries or raw-row markers partially committed');
  for(const kind of ['raw','pricing']) {
    const previous=await summaries(db);let injected=false;
    db.prepare=sql=>sql===usageRollupSql?{all(...args){const rows=prepare(sql).all(...args);if(!injected){injected=true;if(kind==='raw')add(db,{tokens_input:11});else prepare("INSERT OR REPLACE INTO key_value(namespace,key,value) VALUES ('pricing','p',?)").run(JSON.stringify({m:{input:2000000,output:2000000}}));}return rows;}}:prepare(sql);
    try{assert.equal((await rollupUsageHistoryBeforeDate(before)).errors,1);}finally{db.prepare=prepare;}
    assert.equal(injected,true);assert.deepEqual(await summaries(db),previous);assert.equal(db.compatibilityFault,undefined);
  }
  assert.equal((await rollupUsageHistoryBeforeDate(before)).errors,0);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM usage_history WHERE team_rollup_processed_at IS NULL AND timestamp < ?').get(before).n,0);
  console.log('PASS original rollup snapshot CAS: raw or pricing writes after preview abort all summary/marker changes; explicit fresh retry succeeds without callback replay');
  let unblock;const gate=new Promise(resolve=>{unblock=resolve;});
  const running=retention.run(async()=>{await gate;return 'synthetic-complete';});
  await assert.rejects(retention.run(async()=>{}),{code:'HA_ROLLUP_BUSY'});unblock();assert.equal(await running,'synthetic-complete');
  console.log('PASS rollup admission: overlapping async rollups are refused instead of mixing previews');
  const unpricedBeforeArchive=sort(db.prepare(unpricedUsageSql[0]).all({sinceIso:'2025-01-01T00:00:00.000Z'}));
  const financialBefore=sort(db.prepare(usageSpendSql).all({apiKeyId:'a',sinceIso:'2025-01-01'}));
  const publishedBefore=await summaries(db),rawBefore=db.prepare('SELECT COUNT(*) AS n FROM usage_history').get().n;
  const finish=retention.finish;
  retention.finish=before=>{finish(before);throw new Error('synthetic failure after archive/delete');};
  try{assert.equal((await retention.purgeBefore(before)).errors,1);}finally{retention.finish=finish;}
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM usage_history').get().n,rawBefore);
  assert.equal(await test.db.collection('_ha_usage_history').countDocuments(),0);assert.deepEqual(await summaries(db),publishedBefore);
  const purged=await retention.purgeBefore(before);assert.equal(purged.errors,0);assert.equal(purged.deletedRows,rawBefore-1);assert.equal(purged.hasMore,false);
  assert.deepEqual(await summaries(db),publishedBefore);assert.deepEqual(sort(db.prepare(usageSpendSql).all({apiKeyId:'a',sinceIso:'2025-01-01'})),financialBefore);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM usage_history').get().n,1);
  assert.equal((await retention.purgeBefore(before)).deletedRows,0);
  const archivedId=(await test.db.collection('_ha_usage_history').findOne({})).id;
  assert.throws(()=>db.prepare('INSERT INTO usage_history(id,provider,model,timestamp) VALUES (?,?,?,?)').run(archivedId,'p','m','2026-01-01T00:00:00.000Z'),{code:'HA_ROLLUP_BASELINE'});
  console.log('PASS original rollup atomic purge: injected failure after financial archive/raw delete rolls back summaries, markers and both histories; committed purge preserves published totals and exact key-spend DTOs');
  // Historical baseline guards must still see financial history after raw deletion.
  await test.db.collection('api_keys').insertOne({id:'a',usage_limit_enabled:1,daily_usage_limit_usd:10});
  assert.deepEqual(sort(db.prepare(unpricedUsageSql[0]).all({sinceIso:'2025-01-01T00:00:00.000Z'}).map(({limitedApiKeys,...row})=>row)),sort(unpricedBeforeArchive.map(({limitedApiKeys,...row})=>row)));
  assert.equal(db.prepare(unpricedUsageSql[1]).get({sinceIso:'2025-01-01T00:00:00.000Z',labels:'["p/m"]'}).affected,1);
  console.log('PASS unpriced report archive consistency: raw plus retained financial history preserves request/key counts and affected limited keys deduplicate across both collections');
  await assert.rejects(checkKeyUsage(test.db,undefined,'a',Date.now(),()=>{},()=>{}),{code:'HA_QUOTA_POLICY'});
  add(db,{tokens_input:13,provider_cost_usd:4});
  assert.equal((await rollupUsageHistoryBeforeDate(before)).errors,0);
  const late=await summaries(db),old=publishedBefore.global.find(row=>row.provider==='p'),newer=late.global.find(row=>row.provider==='p');
  assert.equal(newer.total_requests,old.total_requests+1);assert.equal(newer.total_cost,old.total_cost+4);
  assert.equal((await retention.purgeBefore(before)).deletedRows,1);assert.deepEqual(await summaries(db),late);
  assert.equal((await rollupUsageHistoryBeforeDate(before)).errors,0);assert.deepEqual(await summaries(db),late);
  console.log('PASS archived late arrivals: regular rollup and subsequent purge add to durable cumulative totals without replacement/double counting; historical USD baseline cannot become empty');
  db.transaction(()=>{for(let n=0;n<503;n++)add(db,{provider:'Batch',model:'Bounded',tokens_input:1,provider_cost_usd:0.25,billing_team_id:null});})();
  const batch=await retention.purgeBefore(before);assert.equal(batch.errors,0);assert.equal(batch.deletedRows,500);assert.equal(batch.hasMore,true);
  const daily=db.prepare("SELECT * FROM daily_usage_summary WHERE provider='batch'").get();assert.equal(daily.total_requests,503);assert.equal(daily.total_cost,125.75);
  assert.equal((await retention.purgeBefore(before)).deletedRows,3);assert.deepEqual(db.prepare("SELECT * FROM daily_usage_summary WHERE provider='batch'").get(),daily);
  const archiveCount=await test.db.collection('_ha_usage_history').countDocuments();
  // A summary edited outside the archive protocol must never be guessed/rebased.
  add(db,{provider:'Batch',model:'Bounded',provider_cost_usd:1,billing_team_id:null});
  db.prepare("UPDATE daily_usage_summary SET total_cost=999 WHERE provider='batch'").run();
  assert.equal((await retention.purgeBefore(before)).errors,1);assert.equal(await test.db.collection('_ha_usage_history').countDocuments(),archiveCount);
  db.prepare("UPDATE daily_usage_summary SET total_cost=? WHERE provider='batch'").run(daily.total_cost);
  assert.equal((await retention.purgeBefore(before)).errors,0);
  console.log('PASS bounded archive: 503 same-day rows purge in 500/3 batches while all daily totals remain published; divergent summary refuses deletion and fresh repaired retry succeeds');
  primary=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-rollup-primary'});
  standby=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-rollup-standby'});
  const choose=actor=>{globalThis.__omnirouteMongoAdapter=actor;globalThis.__omnirouteRetention=createRetentionRuntime(actor,{assertOwner(){if(!actor.canWrite())throw Object.assign(new Error('Active rollup owner required'),{code:'HA_FENCED'});}});};
  assert.ok(primary.ownership('claim',60000));choose(standby);
  await assert.rejects(rollupUsageHistoryBeforeDate(before),{code:'HA_FENCED'});
  choose(primary);const ownPrepare=primary.prepare.bind(primary),acknowledged=await summaries(db);let switched=false;
  primary.prepare=sql=>sql===usageRollupSql?{all(...args){const rows=ownPrepare(sql).all(...args);assert.equal(primary.ownership('release'),true);assert.ok(standby.ownership('claim',60000));switched=true;return rows;}}:ownPrepare(sql);
  try{await assert.rejects(rollupUsageHistoryBeforeDate(before),{code:'HA_FENCED'});}finally{primary.prepare=ownPrepare;}
  assert.equal(switched,true);assert.deepEqual(await summaries(db),acknowledged);
  choose(standby);assert.equal((await rollupUsageHistoryBeforeDate(before)).errors,0);standby.ownership('release');
  console.log('PASS original rollup lifecycle: standby refuses before work, ownership loss during async pricing cannot commit, promoted owner completes a fresh fenced rollup');
} finally {delete globalThis.__omnirouteMongoAdapter;delete globalThis.__omnirouteRetention;await verifier?.close();await primary?.close();await standby?.close();await db.close();oracle.close();await test.cleanup();}
