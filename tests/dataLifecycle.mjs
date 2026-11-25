import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { Long, Decimal128 } from 'mongodb';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync,existsSync,readdirSync } from 'node:fs';
import {fork} from 'node:child_process';
import {once} from 'node:events';
import { createHash, randomUUID } from 'node:crypto';
import path from 'node:path';
import { backupDatabase, restoreDatabase, importSqlite, inspectArchive } from '../adapter/dataTransfer.mjs';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { readBaseSchema } from '../adapter/baseSchema.mjs';
import { testDatabase } from './mongoTestSupport.mjs';
import { credentialVersion } from '../adapter/credentialState.mjs';
import {workspacePath} from '../adapter/workspacePath.mjs';
import { verifyBuildContract } from '../adapter/buildContract.mjs';
import {providerRetirementTriggers} from '../adapter/providerRetirement.mjs';
import {migrationVersionSql} from '../adapter/usageQueries.mjs';

const fixtureEnv={NODE_ENV:'production',NEXT_RUNTIME:'nodejs',NEXT_TELEMETRY_DISABLED:'1',HOSTNAME:'127.0.0.1',REQUIRE_API_KEY:'true',OMNIROUTE_HEADLESS:'1',OMNIROUTE_DISABLE_BACKGROUND_SERVICES:'true',OMNIROUTE_DISABLE_CREDENTIAL_HEALTH_CHECK:'true',OMNIROUTE_DISABLE_TOKEN_HEALTHCHECK:'true',OMNIROUTE_DISABLE_REDIS_AUTH_CACHE:'true',OMNIROUTE_DISABLE_CLI_TOKEN:'true',DISABLE_SQLITE_AUTO_BACKUP:'true',PROXY_HEALTH_ENABLED:'false',VECTOR_STORE_DISABLE_VEC:'true',HA_REDIS_MODE:'disabled',REDIS_URL:'',QDRANT_HOST:'',MEMORY_TYPED_DECAY_ENABLED:'false',MEMORY_TYPED_DECAY_SWEEP_INTERVAL:'',JWT_SECRET:'synthetic-migration-jwt-secret-for-local-tests-only',API_KEY_SECRET:'synthetic-migration-api-secret-for-local-tests-only',STORAGE_ENCRYPTION_KEY:'synthetic-migration-storage-secret-for-local-tests-only',OMNIROUTE_HA_IDENTITY:'synthetic-migration-identity'};

if(process.argv[2]==='sqlite-source') {
  // Offline source fixture only. The real app below never loads SQLite.
  const file=workspacePath(process.argv[3],{existing:false});assert.equal(existsSync(file),false);
  Object.assign(process.env,fixtureEnv,{DATA_DIR:path.dirname(file)});
  const sqlite=new DatabaseSync(file);
  const facade={prepare:sqlite.prepare.bind(sqlite),exec:sqlite.exec.bind(sqlite),healthy(){return true;},canWrite(){return true;},assertCompatible(){},verifyBaseSchema(){return {tables:17,indexes:22};},installMemoryTextIndex(){},installRequestDetailRetention(){
    const sql=readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/006_detailed_request_logs.sql',import.meta.url),'utf8');
    assert.equal(createHash('sha256').update(sql).digest('hex'),'532e2853e760a2318e46b3cd29c50897b6b83953c410072afbb84a9f9e0d63e8');
    const start=sql.indexOf('CREATE TRIGGER IF NOT EXISTS trg_rdl_ring_buffer');sqlite.exec(sql.slice(start,sql.indexOf('END;',start)+4));
  },installFreeProxyCleanup(){sqlite.exec(readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/072_free_proxies_fk.sql',import.meta.url),'utf8'));},installRequestLimits(){},
    transaction(fn){const tx=(...args)=>{sqlite.exec('BEGIN');try{const result=fn(...args);assert.ok(!(result instanceof Promise));sqlite.exec('COMMIT');return result;}catch(error){sqlite.exec('ROLLBACK');throw error;}};tx.immediate=tx;return tx;}};
  globalThis.__omnirouteMongoAdapter=facade;await import('../adapter/register.mjs');
  try {
    const {provisionStartupSchema}=await import('../adapter/startupSchema.mjs');await provisionStartupSchema(facade);
    sqlite.prepare('INSERT INTO reasoning_cache(tool_call_id,provider,model,reasoning,char_count,expires_at) VALUES (?,?,?,?,?,?)')
      .run('synthetic-migrated-expiry','synthetic-migration','migration-model','Synthetic legacy reasoning preserved',34,'2099-01-01T00:00:00.000Z');
    // Restore the actual SQLite indexes normalized by fresh Mongo provisioning.
    // The import must handle real expression SQL and redundant physical aliases.
    sqlite.exec('DROP INDEX idx_uh_dedup');
    const {ensureUsageHistoryColumns}=await import('../upstream/omniroute/src/lib/db/schemaColumns.ts');
    ensureUsageHistoryColumns(facade);
    assert.match(sqlite.prepare("SELECT sql FROM sqlite_master WHERE name='idx_uh_dedup'").get().sql,/COALESCE/);
    sqlite.exec(readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/048_summary_indexes.sql',import.meta.url),'utf8'));
    sqlite.prepare('INSERT INTO daily_usage_summary(provider,model,date,total_requests,total_cost) VALUES (?,?,?,?,?)').run('synthetic','migration-model','2099-01-01',7,1.25);
    sqlite.prepare('INSERT INTO hourly_usage_summary(provider,model,date_hour,total_requests,total_cost) VALUES (?,?,?,?,?)').run('synthetic','migration-model','2099-01-01 00:00:00',7,1.25);
    const {updateSettings}=await import('../upstream/omniroute/src/lib/db/settings.ts');
    await updateSettings({requireLogin:true,stickyRoundRobinLimit:37,cloudEnabled:false,memoryEmbeddingSource:'remote',memoryEmbeddingProviderModel:null});
    const {createApiKey}=await import('../upstream/omniroute/src/lib/db/apiKeys.ts');const key=await createApiKey('Synthetic migrated operator','synthetic-migration-machine',['manage']);
    const {createProviderConnection}=await import('../upstream/omniroute/src/lib/db/providers.ts');
    const provider=await createProviderConnection({provider:'openai',authType:'apikey',name:'Synthetic migrated provider',apiKey:'synthetic-migration-provider-token'});
    sqlite.prepare('INSERT INTO quota_pools(id,connection_id,name) VALUES (?,?,?)').run('synthetic-empty-import-pool',provider.id,'Synthetic unknown zero-history pool');
    sqlite.prepare('INSERT INTO teams(id,name,max_budget_usd,budget_duration,created_at,updated_at) VALUES (?,?,?,?,?,?)').run('synthetic-empty-import-team','Synthetic unknown Team',null,null,'2026-01-01','2026-01-01');
    const free=await import('../upstream/omniroute/src/lib/db/freeProxies.ts');
    const freeProxy=await free.upsertFreeProxy({source:'1proxy',host:'198.51.100.88',port:8088,type:'http',qualityScore:50,lastValidated:'2099-01-01T00:00:00.000Z'});
    const freePool=await free.promoteFreeProxyToPool(freeProxy.id,{name:'Synthetic migration pool proxy',type:'http',host:'198.51.100.88',port:8088,source:'1proxy'});
    for(const name of ['164_retire_microsoft_designer_web','165_retire_felo_web','167_retire_qwen_web','168_retire_chatgpt_web','171_restore_chatgpt_web_cleanroom'])
      sqlite.exec(readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/'+name+'.sql',import.meta.url),'utf8'));
    sqlite.prepare('INSERT INTO provider_connections(id,provider,auth_type,is_active,created_at,updated_at) VALUES (?,?,?,?,?,?)').run('migration-retired-provider','felo','apikey',1,'2099','2099');
    // The real legacy triggers are redundant only when all declared cascades exist.
    const reasoning=readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/126_reasoning_routing_rules.sql',import.meta.url),'utf8');sqlite.exec(reasoning);
    const cascadeKey=await createApiKey('Synthetic cascade fixture','synthetic-migration-machine',[]);
    const cascadeProvider=await createProviderConnection({provider:'openai',authType:'apikey',name:'Synthetic cascade provider',apiKey:'synthetic-cascade-token'});
    const stamp='2099-01-01T00:00:00.000Z';
    for(const id of ['migration-scope-combo','migration-target-combo'])sqlite.prepare('INSERT INTO combos(id,name,data,created_at,updated_at) VALUES (?,?,?,?,?)').run(id,id,'{}',stamp,stamp);
    for(const [id,scope,column,parent] of [['migration-rule-key','apiKey','api_key_id',cascadeKey.id],['migration-rule-connection','connection','connection_id',cascadeProvider.id],['migration-rule-combo','combo','combo_id','migration-scope-combo'],['migration-rule-target','global','target_combo_id','migration-target-combo']])
      sqlite.prepare('INSERT INTO reasoning_routing_rules(id,name,scope,'+column+',target_kind,created_at,updated_at) VALUES (?,?,?,?,?,?,?)').run(id,id,scope,parent,column==='target_combo_id'?'combo':'keep',stamp,stamp);
    const cascadeParents=[['api_keys',cascadeKey.id],['provider_connections',cascadeProvider.id],['combos','migration-scope-combo'],['combos','migration-target-combo']];
    sqlite.exec('BEGIN');for(const [table,id] of cascadeParents)sqlite.prepare('DELETE FROM '+table+' WHERE id=?').run(id);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM reasoning_routing_rules').get().n,0);sqlite.exec('ROLLBACK');
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM reasoning_routing_rules').get().n,4);
    const {createMemory}=await import('../upstream/omniroute/src/lib/memory/store.ts');
    const memory=await createMemory({apiKeyId:key.id,sessionId:'migration-session',type:'factual',key:'migration-memory',content:'Migrated full profile memory',metadata:{synthetic:true},expiresAt:null});
    await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));
    if(!sqlite.prepare('PRAGMA table_info(memories)').all().some(column=>column.name==='memory_id'))sqlite.exec('ALTER TABLE memories ADD COLUMN memory_id INTEGER');
    sqlite.exec(readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/203_memory_fts_content_rowid.sql',import.meta.url),'utf8'));
    sqlite.close();process.send?.({keyId:key.id,key:key.key,providerId:provider.id,memoryId:memory.id,cascadeParents,freeProxyId:freeProxy.id,freePool});process.disconnect?.();
  } catch(error){sqlite.close();throw error;}
} else {

// Refuse stale/missing backend artifacts before allocating any database fixture.
// The source-only child deliberately does not require a compiled application.
verifyBuildContract(path.resolve('.work/backend'));
const test = await testDatabase('data');
mkdirSync('.work/test-artifacts', { recursive: true });
const directory = mkdtempSync(path.resolve('.work/test-artifacts/data-'));
const sqliteFile = path.join(directory, 'source.sqlite');
const archive = path.join(directory, 'snapshot.bson');
const restored = 'ha_test_restore_' + randomUUID().replaceAll('-', '');
const imported = 'ha_test_import_' + randomUUID().replaceAll('-', '');
const appImported='ha_test_migrated_app_'+randomUUID().replaceAll('-','');
const hash = file => createHash('sha256').update(readFileSync(file)).digest('hex');
let source, verify,application;
async function migratedHttp(fixture,{name,sticky,content,extended=false,database=appImported}) {
  const local=path.join(directory,'application-'+name);mkdirSync(local,{mode:0o700});
  application=fork(new URL('../adapter/serve.mjs',import.meta.url),[],{execArgv:['--permission','--allow-worker','--allow-child-process','--allow-addons','--allow-fs-read='+process.cwd(),'--allow-fs-write='+local,'--import','./adapter/register.mjs','--import','./tests/networkGuard.mjs'],env:{...process.env,...fixtureEnv,DATA_DIR:local,TMPDIR:local,OMNIROUTE_PLUGINS_DIR:path.join(local,'plugins'),HA_MONGO_URI:test.uri,HA_MONGO_DATABASE:database,HA_NEXT_DIR:path.resolve('.work/backend'),HOST:'127.0.0.1',PORT:'0'},stdio:['ignore','inherit','inherit','ipc']});
  const port=await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(new Error('Migrated application listener timeout')),60000);application.once('message',message=>{clearTimeout(timer);resolve(message.port);});application.once('exit',code=>{clearTimeout(timer);reject(new Error('Migrated application exited: '+code));});});
  const base='http://127.0.0.1:'+port,headers={authorization:'Bearer '+fixture.key,'content-type':'application/json'};
  const request=(endpoint,init={})=>fetch(base+endpoint,{...init,headers:{...headers,...init.headers},signal:AbortSignal.timeout(10000)});
  const deadline=Date.now()+90000;let ready=false;
  while(Date.now()<deadline){
    assert.ok(application.exitCode===null&&!application.signalCode,'Migrated application exited during readiness');
    try{const response=await fetch(base+'/readyz',{signal:AbortSignal.timeout(1500)});await response.arrayBuffer();if(response.status===200){ready=true;break;}}
    catch{/* Cold startup can outlast a probe; the overall deadline stays bounded. */}
    await new Promise(resolve=>setTimeout(resolve,100));
  }
  assert.equal(ready,true,'Migrated app readiness timeout');
  let response=await request('/api/settings'),body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.stickyRoundRobinLimit,sticky);
  response=await request('/api/providers/'+fixture.providerId);assert.equal(response.status,200,await response.text());
  if(extended) {
    response=await request('/api/cache/reasoning?provider=synthetic-migration&model=migration-model');body=await response.json();assert.equal(response.status,200,JSON.stringify(body));
    assert.ok(body.entries.some(row=>row.toolCallId==='synthetic-migrated-expiry'&&row.reasoning==='Synthetic legacy reasoning preserved'&&row.expiresAt==='2099-01-01T00:00:00.000Z'));
    console.log('PASS migrated legacy reasoning HTTP: original authenticated cache API reads the normalized SQLite expiry and preserved reasoning after real Mongo-backed startup');
    response=await request('/api/settings/free-proxies');body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.data.proxies.find(row=>row.id===fixture.freeProxyId).inPool,false);assert.equal(body.data.stats.avgQuality,50);
    console.log('PASS migrated Free Proxy HTTP: exact SQLite cleanup trigger is native, original registry deletion clears imported references and actual list/statistics read the preserved catalog');
    response=await request('/api/providers/migration-retired-provider');body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.connection.isActive,false);
    response=await request('/api/providers/migration-retired-provider',{method:'PUT',body:JSON.stringify({isActive:true})});body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.connection.isActive,false,'Original Provider HTTP writer must reload the native retirement tombstone');
    assert.equal((await test.client.db(database).collection('provider_connections').findOne({id:'migration-retired-provider'})).is_active,0);
    console.log('PASS migrated Provider retirement HTTP: exact legacy triggers map to native behavior; original GET/PUT cannot reactivate the retired connection');
  }
  response=await request('/api/memory/'+fixture.memoryId);body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.memory.content,content);
  if(fixture.artifactId) {
    response=await request('/api/usage/call-logs/'+fixture.artifactId);body=await response.json();assert.equal(response.status,200,JSON.stringify(body));
    assert.equal(body.detailState,'ready');assert.deepEqual(body.responseBody,{content:'Synthetic migrated artifact'});
  }
  response=await request('/api/keys',{method:'POST',body:JSON.stringify({name:'Synthetic post-migration write'})});body=await response.json();assert.equal(response.status,201,JSON.stringify(body));assert.ok(await test.client.db(database).collection('api_keys').findOne({id:body.id}));
  assert.equal(readdirSync(local,{recursive:true}).some(name=>/\.(?:sqlite|db)(?:-wal|-shm)?$/.test(name)),false);
  const exit=once(application,'exit');application.kill('SIGTERM');await exit;application=undefined;
}
try {
  source = new DatabaseSync(sqliteFile);
  for (const sql of readBaseSchema().statements) source.exec(sql);
  source.prepare('INSERT INTO key_value(namespace,key,value) VALUES (?,?,?)').run('settings', 'synthetic_migration_value', 'durable');
  source.prepare('INSERT INTO provider_connections(id,provider,auth_type,access_token,refresh_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?)')
    .run('synthetic-copied-oauth','openference','oauth','synthetic-access-copy','synthetic-refresh-copy','2026-01-01','2026-01-01');
  source.prepare('INSERT INTO provider_connections(id,provider,auth_type,access_token,refresh_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?)')
    .run('synthetic-copied-classic-github','github','oauth','synthetic-classic-copy-access',null,'2026-01-01','2026-01-01');
  source.exec('CREATE TABLE bytes_test(id INTEGER PRIMARY KEY AUTOINCREMENT,payload BLOB)');
  source.prepare('INSERT INTO bytes_test(id,payload) VALUES (?,?)').run(7, Buffer.from([0, 255, 3]));
  source.prepare('INSERT INTO bytes_test(id,payload) VALUES (?,?)').run(99, Buffer.from([1]));
  source.exec('DELETE FROM bytes_test WHERE id=99');
  source.exec('CREATE TABLE rowid_shadow(rowid TEXT, value TEXT)');
  source.prepare('INSERT INTO rowid_shadow(_rowid_,rowid,value) VALUES (?,?,?)').run(23, 'not-an-integer', 'preserved');
  source.exec('CREATE TABLE ordered_keys(a INTEGER,b TEXT,PRIMARY KEY(a,b)) WITHOUT ROWID');
  source.exec("INSERT INTO ordered_keys VALUES (3,'z'),(1,'a')");
  const migrationRoot = new URL('../upstream/omniroute/src/lib/db/migrations/', import.meta.url);
  source.exec(readFileSync(new URL('033_create_reasoning_cache.sql',migrationRoot),'utf8'));
  for(const [id,expiry] of [['legacy-iso','2099-01-01T00:00:00.000Z'],['legacy-fraction','2099-01-01 00:00:01.900+00:00'],['legacy-invalid','not-a-date']])
    source.prepare('INSERT INTO reasoning_cache(tool_call_id,provider,model,reasoning,expires_at) VALUES (?,?,?,?,?)').run(id,'synthetic','migration-model','Synthetic preserved '+id,expiry);
  source.exec(readFileSync(new URL('217_quota_recovery_tickets.sql',migrationRoot),'utf8'));
  for(const state of ['pending','running','completed'])source.prepare('INSERT INTO quota_recovery_tickets(id,api_key_id,turn_key,request_hash,endpoint,request_ciphertext,result_ciphertext,state,next_attempt_at,expires_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)')
    .run('migration-ticket-'+state,'synthetic-key',state,'synthetic-opaque-hash','chat/completions',state==='completed'?'':'synthetic-opaque-migration-payload',state==='completed'?'synthetic-opaque-result':null,state,'2026-01-01','2099-01-01','2026-01-01','2026-01-01');
  source.exec(readFileSync(new URL('015_create_memories.sql', migrationRoot), 'utf8'));
  source.exec('ALTER TABLE memories ADD COLUMN memory_id INTEGER; ALTER TABLE memories ADD COLUMN needs_reindex INTEGER NOT NULL DEFAULT 0');
  source.exec(readFileSync(new URL('203_memory_fts_content_rowid.sql', migrationRoot), 'utf8'));
  source.exec("INSERT INTO memories(id,api_key_id,type,content) VALUES ('memory-migration','synthetic-key','factual','Migration searchable content')");
  source.close(); source = undefined;
  const before = hash(sqliteFile);
  await assert.rejects(importSqlite(test.client, test.uri, sqliteFile, imported), /explicit migration mapping/);
  const migrated = await importSqlite(test.client, test.uri, sqliteFile, imported, { rebuildMemory: true });
  assert.ok(migrated.replacedDerivedObjects.includes('memory_fts_ai_assign'));
  assert.equal(migrated.counts.bytes_test, 1); assert.equal(hash(sqliteFile), before);
  assert.equal(migrated.normalizedReasoningExpiries,3);
  const expiries=await test.client.db(imported).collection('reasoning_cache').find({},{projection:{_id:0,tool_call_id:1,reasoning:1,expires_at:1}}).sort({tool_call_id:1}).toArray();
  assert.deepEqual(expiries,[{tool_call_id:'legacy-fraction',reasoning:'Synthetic preserved legacy-fraction',expires_at:4070908801},{tool_call_id:'legacy-invalid',reasoning:'Synthetic preserved legacy-invalid',expires_at:0},{tool_call_id:'legacy-iso',reasoning:'Synthetic preserved legacy-iso',expires_at:4070908800}]);
  console.log('PASS legacy reasoning expiry import: actual SQLite eligibility expression normalizes ISO/fractional/invalid dates, reports each conversion, preserves payloads and source hash, and verifies normalized target content');
  assert.equal(migrated.credentialGroupsRequiringVerification,1);
  assert.equal(await test.client.db(imported).collection('_ha_credential_lanes').findOne({_id:'github-app'}),null,'An access-only classic GitHub row must not create a one-time-grant copy gate');
  assert.equal(migrated.quotaRecoveriesRequiringConfirmation,2);
  for(const state of ['pending','running'])assert.equal((await test.client.db(imported).collection('quota_recovery_tickets').findOne({id:'migration-ticket-'+state})).state,'needs_confirmation');
  assert.equal((await test.client.db(imported).collection('quota_recovery_tickets').findOne({id:'migration-ticket-completed'})).result_ciphertext,'synthetic-opaque-result');
  assert.equal((await test.client.db(imported).collection('_ha_credential_lanes').findOne({_id:'openference'})).state,'verification_required');
  await assert.rejects(importSqlite(test.client, test.uri, sqliteFile, imported, { rebuildMemory: true }), /empty database/);
  verify = new MongoAdapter({ uri: test.uri, database: imported });
  assert.ok((await test.client.db(imported).collection('_ha_usage_history').listIndexes().toArray()).some(index=>index.name==='ha_financial_history'));
  assert.ok((await test.client.db(imported).collection('_ha_usage_archive_totals').listIndexes().toArray()).some(index=>index.name==='_id_'));
  assert.equal(verify.prepare("SELECT value FROM key_value WHERE key='synthetic_migration_value'").get().value, 'durable');
  assert.deepEqual(verify.prepare('SELECT payload FROM bytes_test').get().payload, Buffer.from([0,255,3]));
  assert.equal(verify.prepare('INSERT INTO bytes_test(payload) VALUES (?)').run(Buffer.from([2])).lastInsertRowid, 100);
  assert.equal((await test.client.db(imported).collection('rowid_shadow').findOne({ value: 'preserved' }))._ha_rowid, 23);
  assert.equal(verify.prepare('SELECT needs_reindex FROM memories WHERE id=?').get('memory-migration').needs_reindex, 1);
  assert.equal(await test.client.db(imported).collection('memories').countDocuments({ $text: { $search: 'searchable' } }), 1);
  await verify.close(); verify = undefined;
  for(const state of ['pending','running'])await test.client.db(imported).collection('quota_recovery_tickets').updateOne({id:'migration-ticket-'+state},{$set:{state}});
  // Model a historically completed, subsequently copied credential snapshot.
  // Restore must fence it again instead of trusting remote freshness from BSON.
  await test.client.db(imported).collection('_ha_credential_lanes').updateOne({_id:'openference'},{$set:{state:'completed'}});
  const importId=randomUUID();
  await test.client.db(imported).collection('_ha_credential_intents').insertOne({_id:importId,kind:'import',provider:'codex',lane:'openai-auth0',state:'reserved',epoch:1,importKey:'a'.repeat(64),payloadHash:'b'.repeat(64),identity:{email:'synthetic-unattached@example.invalid',workspaceId:null}});
  await test.client.db(imported).collection('_ha_credential_lanes').insertOne({_id:'openai-auth0',kind:'import',state:'import-reserved',intentId:importId,epoch:1});
  await test.client.db(imported).collection('data_probe').insertOne({ _id: 'types', large: Long.fromString('9223372036854775806'),
    decimal: Decimal128.fromString('0.1234567890123456789'), expires: new Date('2099-01-01'), content: 'synthetic backup text' });
  await test.client.db(imported).collection('data_probe').createIndex({ expires: 1 }, { expireAfterSeconds: 0 });
  await test.client.db(imported).collection('data_probe').createIndex({ content: 'text' });
  // Snapshot the actual imported database, with independent schema/index and
  // BSON type validation on restore. Cleanup is limited to generated databases.
  await backupDatabase(test.client, imported, archive);
  const inspected = inspectArchive(archive);
  assert.equal(Number(inspected.footer.counts.bytes_test), 2);
  const inspection=await restoreDatabase(test.client,archive,restored);
  assert.equal(inspection.activated,false);assert.equal(inspection.reconciliationRequired,true);
  assert.equal(typeof(await test.client.db(restored).collection('_ha_serial').findOne({_id:'writer'})).ddl,'string');
  const gatedOwner=new MongoAdapter({uri:test.uri,database:restored,ownerId:'synthetic-historical-target'});
  try{assert.equal(gatedOwner.ownership('claim',10000),null);}finally{await gatedOwner.close();}
  await test.client.db(restored).dropDatabase(); // Only this invocation's new, never-activated target.
  const changedSource=new MongoAdapter({uri:test.uri,database:imported,ownerId:'synthetic-post-backup-writer'});
  try{assert.ok(changedSource.ownership('claim',10000));changedSource.prepare("UPDATE key_value SET value=value WHERE key='synthetic_migration_value'").run();}
  finally{if(changedSource.open)changedSource.ownership('release');await changedSource.close();}
  await assert.rejects(restoreDatabase(test.client,archive,restored,{quiescentSource:imported}),/Source authority changed/);
  assert.equal((await test.client.db(imported).collection('_ha_serial').findOne({_id:'writer'})).ddl,undefined,'Rejected historical cutover must not retire the source');
  assert.equal(typeof(await test.client.db(restored).collection('_ha_serial').findOne({_id:'writer'})).ddl,'string');
  await test.client.db(restored).dropDatabase();
  const cutoverArchive=path.join(directory,'quiescent-cutover.bson');await backupDatabase(test.client,imported,cutoverArchive);
  const result = await restoreDatabase(test.client,cutoverArchive,restored,{quiescentSource:imported});
  assert.equal(result.activated,true);assert.equal(result.sourceRetired,imported);
  assert.equal(typeof(await test.client.db(imported).collection('_ha_serial').findOne({_id:'writer'})).ddl,'string');
  console.log('PASS restore authority safety: historical restore remains gated, actual intervening SQL write rejects activation, verified unchanged-source cutover activates only the target and permanently retires the old authority');
  assert.ok(result.collections >= 17);
  assert.equal(result.credentialGroupsRequiringVerification,2);
  assert.equal(await test.client.db(restored).collection('_ha_credential_lanes').findOne({_id:'github-app'}),null);
  assert.equal(result.quotaRecoveriesRequiringConfirmation,2);
  for(const state of ['pending','running']){
    const copied=await test.client.db(restored).collection('quota_recovery_tickets').findOne({id:'migration-ticket-'+state});
    assert.equal(copied.state,'needs_confirmation');assert.equal(copied.request_ciphertext,'synthetic-opaque-migration-payload');
  }
  assert.equal((await test.client.db(restored).collection('quota_recovery_tickets').findOne({id:'migration-ticket-completed'})).state,'completed');
  console.log('PASS copied quota recovery safety: original SQLite import and BSON restore fence pending/running tickets before activation, preserve opaque payloads and completed results, leave source unchanged and report required confirmation instead of replaying');
  verify = new MongoAdapter({ uri: test.uri, database: restored });
  assert.deepEqual(verify.verifyBaseSchema(), { tables: 17, indexes: 22 });
  assert.deepEqual(verify.prepare('SELECT payload FROM bytes_test WHERE id=7').get().payload, Buffer.from([0,255,3]));
  const restoredTypes = await test.client.db(restored).collection('data_probe').findOne({ _id: 'types' });
  assert.equal(restoredTypes.large.toString(), '9223372036854775806');
  assert.equal(restoredTypes.decimal.toString(), '0.1234567890123456789');
  assert.equal(restoredTypes.expires.toISOString(), '2099-01-01T00:00:00.000Z');
  const writer = await test.client.db(restored).collection('_ha_serial').findOne({ _id: 'writer' });
  assert.equal(writer.ownerId, null); assert.equal(writer.ddl, undefined); assert.ok(writer.epoch >= 2);
  assert.equal((await test.client.db(restored).collection('_ha_credential_lanes').findOne({_id:'openference'})).state,'verification_required');
  const recoveredOwner=new MongoAdapter({uri:test.uri,database:restored,ownerId:'synthetic-restored-credential-owner'});
  try {
    recoveredOwner.ownership('claim',10000);
    const row=await test.client.db(restored).collection('provider_connections').findOne({id:'synthetic-copied-oauth'});
    assert.throws(()=>recoveredOwner.credential('dispatch',{id:randomUUID(),connectionId:row.id,provider:row.provider,version:credentialVersion(row)}),{code:'HA_CREDENTIAL_UNCERTAIN'});
    assert.equal((await test.client.db(restored).collection('_ha_credential_lanes').findOne({_id:'openai-auth0'})).state,'verification_required');
    assert.equal((await test.client.db(restored).collection('_ha_credential_intents').findOne({_id:importId})).copyVerificationRequired,true);
    assert.throws(()=>recoveredOwner.credential('importStage',{id:randomUUID(),importKey:'a'.repeat(64),payloadHash:'b'.repeat(64),identity:{email:'synthetic-unattached@example.invalid',workspaceId:null}}),{code:'HA_CREDENTIAL_UNCERTAIN'});
  } finally {recoveredOwner.close();}
  await verify.close(); verify = undefined;
  await assert.rejects(restoreDatabase(test.client, archive, restored), /empty database/);
  const damaged = path.join(directory, 'corrupt.bson');
  const data = readFileSync(archive); data[Math.floor(data.length / 2)] ^= 1; writeFileSync(damaged, data, { mode: 0o600 });
  assert.throws(() => inspectArchive(damaged));
  const fullFile=path.join(directory,'full-profile.sqlite');
  const builder=fork(new URL(import.meta.url),['sqlite-source',fullFile],{execArgv:[],env:{...process.env,...fixtureEnv,DATA_DIR:directory},stdio:['ignore','inherit','inherit','ipc']});
  const fixture=await new Promise((resolve,reject)=>{let value;const timer=setTimeout(()=>{builder.kill('SIGTERM');reject(new Error('SQLite fixture builder timeout'));},60000);builder.on('message',message=>{value=message;});builder.once('error',reject);builder.once('exit',code=>{clearTimeout(timer);if(code===0&&value)resolve(value);else reject(new Error('SQLite fixture builder failed: '+code));});});
  // Deliberately malformed disposable input proves preflight fails before even
  // creating a target collection. Foreign-key enforcement stays enabled in MongoDB.
  const invalidFile=path.join(directory,'invalid-source.sqlite');
  let invalid=new DatabaseSync(invalidFile,{enableForeignKeyConstraints:false});
  invalid.exec('CREATE TABLE migration_parent(id INTEGER PRIMARY KEY); CREATE TABLE migration_child(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES migration_parent(id)); INSERT INTO migration_child VALUES (1,99)');invalid.close();
  const invalidHash=hash(invalidFile);
  await assert.rejects(importSqlite(test.client,test.uri,invalidFile,appImported),/foreign-key violations/);assert.equal(hash(invalidFile),invalidHash);
  assert.equal((await test.client.db(appImported).listCollections().toArray()).length,0);
  invalid=new DatabaseSync(invalidFile);invalid.exec('DELETE FROM migration_child; CREATE TABLE request_detail_logs(id TEXT PRIMARY KEY,timestamp TEXT NOT NULL); CREATE TRIGGER trg_rdl_ring_buffer AFTER INSERT ON request_detail_logs BEGIN DELETE FROM request_detail_logs; END');invalid.close();
  const changedTriggerHash=hash(invalidFile);
  await assert.rejects(importSqlite(test.client,test.uri,invalidFile,appImported),/Unrecognized request-detail retention trigger/);assert.equal(hash(invalidFile),changedTriggerHash);
  assert.equal((await test.client.db(appImported).listCollections().toArray()).length,0);
  console.log('PASS migration preflight: orphan foreign keys and altered known-trigger SQL are refused before target creation; input hashes remain unchanged');
  const originalDedup=readFileSync(new URL('../upstream/omniroute/src/lib/db/schemaColumns.ts',import.meta.url),'utf8').match(/`(CREATE INDEX IF NOT EXISTS idx_uh_dedup[\s\S]*?)`/)[1];
  for(const [unique,fallback] of [[false,'changed'],[false,'IF NOT EXISTS '],[true,'']]) {
    invalid=new DatabaseSync(invalidFile);
    invalid.exec("DROP TRIGGER IF EXISTS trg_rdl_ring_buffer; DROP TABLE IF EXISTS usage_history; CREATE TABLE usage_history(timestamp TEXT,provider TEXT,model TEXT,connection_id TEXT,api_key_id TEXT,tokens_input INTEGER,tokens_output INTEGER)");
    invalid.exec(originalDedup.replace('CREATE INDEX',unique?'CREATE UNIQUE INDEX':'CREATE INDEX').replace("COALESCE(provider, '')","COALESCE(provider, '"+fallback+"')"));invalid.close();
    const before=hash(invalidFile);
    await assert.rejects(importSqlite(test.client,test.uri,invalidFile,appImported),/Unrecognized usage dedup index/);
    assert.equal(hash(invalidFile),before);assert.equal((await test.client.db(appImported).listCollections().toArray()).length,0);
  }
  invalid=new DatabaseSync(invalidFile);invalid.exec('DROP TABLE usage_history; CREATE TABLE daily_usage_summary(provider TEXT,model TEXT,date TEXT); CREATE INDEX idx_daily_usage_provider_model_date_composite ON daily_usage_summary(provider,date,model)');invalid.close();
  const aliasHash=hash(invalidFile);
  await assert.rejects(importSqlite(test.client,test.uri,invalidFile,appImported),/Unrecognized summary index alias/);
  assert.equal(hash(invalidFile),aliasHash);assert.equal((await test.client.db(appImported).listCollections().toArray()).length,0);
  console.log('PASS migration index preflight: altered expressions, UNIQUE variants and same-name/different-key aliases fail before target creation');
  invalid=new DatabaseSync(invalidFile);
  const reasoningSql=readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/126_reasoning_routing_rules.sql',import.meta.url),'utf8');
  invalid.exec('CREATE TABLE api_keys(id TEXT PRIMARY KEY); CREATE TABLE reasoning_routing_rules(id TEXT,api_key_id TEXT)');
  invalid.exec(reasoningSql.match(/CREATE TRIGGER IF NOT EXISTS trg_reasoning_rules_api_key_delete[\s\S]*?END;/)[0]);invalid.close();
  const noCascadeHash=hash(invalidFile);
  await assert.rejects(importSqlite(test.client,test.uri,invalidFile,appImported),/requires declared CASCADE/);
  assert.equal(hash(invalidFile),noCascadeHash);assert.equal((await test.client.db(appImported).listCollections().toArray()).length,0);
  console.log('PASS reasoning migration preflight: exact trigger without its declared CASCADE is refused before target creation');
  invalid=new DatabaseSync(invalidFile);invalid.exec('DROP TRIGGER trg_reasoning_rules_api_key_delete; CREATE TABLE provider_connections(id TEXT,provider TEXT)');
  invalid.exec(providerRetirementTriggers().get('provider_connections_retire_felo_web_insert'));invalid.close();
  const incompleteRetirementHash=hash(invalidFile);
  await assert.rejects(importSqlite(test.client,test.uri,invalidFile,appImported),/retirement schema is incomplete/);
  assert.equal(hash(invalidFile),incompleteRetirementHash);assert.equal((await test.client.db(appImported).listCollections().toArray()).length,0);
  for(const [name,from,to,error] of [['provider_connections_retire_felo_web_insert',"'unavailable'","'available'",/Unrecognized provider retirement trigger/],['trg_free_proxies_clear_pool_proxy_id','in_pool = 0','in_pool = 1',/Unrecognized Free Proxy cleanup trigger/]]) {
    const altered=new DatabaseSync(fullFile),trigger=altered.prepare('SELECT sql FROM sqlite_master WHERE name=?').get(name).sql;
    altered.exec('DROP TRIGGER '+name);altered.exec(trigger.replace(from,to));altered.close();
    const alteredHash=hash(fullFile);
    await assert.rejects(importSqlite(test.client,test.uri,fullFile,appImported,{rebuildMemory:true}),error);
    assert.equal(hash(fullFile),alteredHash);assert.equal((await test.client.db(appImported).listCollections().toArray()).length,0);
    const repaired=new DatabaseSync(fullFile);repaired.exec('DROP TRIGGER '+name);repaired.exec(trigger);repaired.close();
  }
  console.log('PASS Provider retirement migration preflight: missing native columns and altered known-trigger SQL fail before target creation');
  const unsafeExpiryFile=path.join(directory,'unsafe-expiry.sqlite');let unsafeExpiry=new DatabaseSync(unsafeExpiryFile);
  unsafeExpiry.exec(readFileSync(new URL('033_create_reasoning_cache.sql',migrationRoot),'utf8'));
  unsafeExpiry.prepare('INSERT INTO reasoning_cache(tool_call_id,provider,model,reasoning,expires_at) VALUES (?,?,?,?,?)').run('unsafe','synthetic','model','Synthetic unsafe expiry',1e20);unsafeExpiry.close();
  const unsafeExpiryHash=hash(unsafeExpiryFile);await assert.rejects(importSqlite(test.client,test.uri,unsafeExpiryFile,appImported),/supported safe numeric profile/);
  assert.equal(hash(unsafeExpiryFile),unsafeExpiryHash);assert.equal((await test.client.db(appImported).listCollections().toArray()).length,0);
  unsafeExpiry=new DatabaseSync(unsafeExpiryFile);unsafeExpiry.exec('UPDATE reasoning_cache SET expires_at=0; ALTER TABLE reasoning_cache ADD COLUMN unreviewed TEXT');unsafeExpiry.close();
  const alteredExpiryHash=hash(unsafeExpiryFile);await assert.rejects(importSqlite(test.client,test.uri,unsafeExpiryFile,appImported),/exact original table/);
  assert.equal(hash(unsafeExpiryFile),alteredExpiryHash);assert.equal((await test.client.db(appImported).listCollections().toArray()).length,0);
  console.log('PASS legacy expiry migration preflight: unsafe numeric ranges and unreviewed table variants refuse before target creation without changing either SQLite source');
  const fullHash=hash(fullFile);const fullMigration=await importSqlite(test.client,test.uri,fullFile,appImported,{rebuildMemory:true});
  assert.equal(fullMigration.normalizedReasoningExpiries,1);
  assert.equal(fullMigration.sourceSha256,fullHash);
  assert.ok(fullMigration.quotaBaselinesRequiringReview.keys>=1);assert.equal(fullMigration.quotaBaselinesRequiringReview.pools,1);assert.equal(fullMigration.quotaBaselinesRequiringReview.teams,1);
  assert.equal((await test.client.db(appImported).collection('_ha_pool_profiles').findOne({_id:'synthetic-empty-import-pool'})).allWindows,false);
  assert.equal((await test.client.db(appImported).collection('_ha_team_profiles').findOne({_id:'synthetic-empty-import-team'})).complete,false);
  assert.equal((await test.client.db(appImported).collection('api_keys').findOne({id:fixture.keyId}))._ha_requires_key_baseline,true);
  assert.deepEqual(fullMigration.translatedTriggers.slice().sort(),['trg_free_proxies_clear_pool_proxy_id','trg_rdl_ring_buffer','trg_reasoning_rules_api_key_delete','trg_reasoning_rules_combo_delete','trg_reasoning_rules_connection_delete',...providerRetirementTriggers().keys()].sort());
  assert.deepEqual(fullMigration.translatedIndexes.map(index=>index.name).sort(),['idx_daily_usage_provider_model_date_composite','idx_hourly_usage_provider_model_date_composite','idx_uh_dedup']);
  const migratedAdapter=new MongoAdapter({uri:test.uri,database:appImported});
  try{
    assert.equal(migratedAdapter.prepare('SELECT in_pool FROM free_proxies WHERE id=?').get(fixture.freeProxyId).in_pool,1);
    migratedAdapter.prepare('DELETE FROM proxy_registry WHERE id=?').run(fixture.freePool);
    assert.equal(migratedAdapter.prepare('SELECT pool_proxy_id FROM free_proxies WHERE id=?').get(fixture.freeProxyId).pool_proxy_id,null);
    const deleteParents=()=>{for(const [table,id] of fixture.cascadeParents)migratedAdapter.prepare('DELETE FROM '+table+' WHERE id=?').run(id);};
    assert.throws(()=>migratedAdapter.transaction(()=>{deleteParents();assert.equal(migratedAdapter.prepare('SELECT COUNT(*) AS n FROM reasoning_routing_rules').get().n,0);throw new Error('rollback migrated cascades');})(),/rollback migrated cascades/);
    assert.equal(migratedAdapter.prepare('SELECT COUNT(*) AS n FROM reasoning_routing_rules').get().n,4);
    migratedAdapter.transaction(deleteParents)();assert.equal(migratedAdapter.prepare('SELECT COUNT(*) AS n FROM reasoning_routing_rules').get().n,0);
    for(const [table,dateColumn,date] of [['daily_usage_summary','date','2099-01-01'],['hourly_usage_summary','date_hour','2099-01-01 00:00:00']]) {
      const row=migratedAdapter.prepare('SELECT total_requests,total_cost FROM '+table+' WHERE provider=? AND model=? AND '+dateColumn+'=?').get('synthetic','migration-model',date);
      assert.deepEqual(row,{total_requests:7,total_cost:1.25});
      assert.throws(()=>migratedAdapter.prepare('INSERT INTO '+table+'(provider,model,'+dateColumn+') VALUES (?,?,?)').run('synthetic','migration-model',date),{code:'SQLITE_CONSTRAINT_UNIQUE'});
      const indexes=await test.client.db(appImported).collection(table).listIndexes().toArray();
      assert.ok(indexes.some(index=>index.name.includes('_usage_unique')&&index.unique===true));
      assert.equal(indexes.some(index=>index.name.endsWith('_date_composite')),false);
    }
    const dedup=await test.client.db(appImported).collection('usage_history').listIndexes().toArray();
    assert.deepEqual(dedup.find(index=>index.name==='idx_uh_dedup').key,{timestamp:1,provider:1,model:1,connection_id:1,api_key_id:1,tokens_input:1,tokens_output:1});
    migratedAdapter.transaction(()=>{const insert=migratedAdapter.prepare('INSERT INTO request_detail_logs(id,timestamp) VALUES (?,?)');for(let n=0;n<501;n++)insert.run('migration-detail-'+n,new Date(Date.UTC(2099,0,1)+n).toISOString());})();
    assert.equal(migratedAdapter.prepare('SELECT COUNT(*) AS n FROM request_detail_logs').get().n,500,'The real SQLite retention trigger must retain its behavior after migration');
  }finally{await migratedAdapter.close();}
  console.log('PASS real SQLite index migration: original expression optimization and duplicate aliases map explicitly; daily/hourly data and UNIQUE constraints survive');
  console.log('PASS real reasoning trigger migration: key, connection, scope-combo and target-combo cascades match SQLite; parent/rule deletion and rollback are atomic');
  await migratedHttp(fixture,{name:'profile',sticky:37,content:'Migrated full profile memory',extended:true});
  assert.equal(hash(fullFile),fullHash);
  console.log('PASS migrated real application: original Settings/API Key/Provider/Memory writers into a full pinned SQLite profile, read-only import, actual service startup/HTTP/auth and new MongoDB write without local SQLite');
  // Reuse the owned target only after the preceding service and adapter exited.
  await test.client.db(appImported).dropDatabase();
  const originalDirectory=path.join(directory,'original-source');mkdirSync(originalDirectory,{mode:0o700});
  const originalBuilder=fork(new URL('./sqliteSource.mjs',import.meta.url),[originalDirectory],{execArgv:['--permission','--allow-worker','--allow-child-process','--allow-addons','--allow-fs-read='+process.cwd(),'--allow-fs-write='+originalDirectory,'--import','./tests/networkGuard.mjs'],env:{...process.env,...fixtureEnv,DATA_DIR:originalDirectory,TMPDIR:originalDirectory,NODE_DISABLE_COMPILE_CACHE:'1'},stdio:['ignore','inherit','inherit','ipc']});
  const original=await new Promise((resolve,reject)=>{let value;const timer=setTimeout(()=>{originalBuilder.kill('SIGTERM');reject(new Error('Original SQLite source timeout'));},60000);originalBuilder.on('message',message=>{value=message;});originalBuilder.once('error',reject);originalBuilder.once('exit',code=>{clearTimeout(timer);if(code===0&&value)resolve(value);else reject(new Error('Original SQLite source failed: '+code));});});
  const originalHash=hash(original.file),complete=await importSqlite(test.client,test.uri,original.file,appImported,{rebuildMemory:true});
  assert.equal(complete.counts._omniroute_migrations,222);assert.deepEqual(Object.keys(complete.counts).sort(),original.tables);assert.equal(complete.sourceSha256,originalHash);assert.deepEqual(complete.addedRuntimeSchemas,['audit_log','ha_call_log_artifacts']);
  // The original SQLite source has no HA artifact table. Prove offline
  // provisioning permits a new shared write and the real compiled detail read,
  // then repeat that HTTP read after BSON recovery below.
  const artifactDb=new MongoAdapter({uri:test.uri,database:appImported});
  try {
    const sourceVersion=new DatabaseSync(original.file,{readOnly:true});
    try{assert.deepEqual(artifactDb.prepare(migrationVersionSql).get(),{...sourceVersion.prepare(migrationVersionSql).get()});}finally{sourceVersion.close();}
    original.artifactId='synthetic-migration-artifact';
    const timestamp=new Date().toISOString(),relPath=timestamp.slice(0,10)+'/synthetic-migration-artifact.json';
    const payload={schemaVersion:5,summary:{id:original.artifactId,timestamp},requestBody:{messages:[]},responseBody:{content:'Synthetic migrated artifact'},error:null};
    artifactDb.transaction(()=>{
      artifactDb.prepare('INSERT INTO ha_call_log_artifacts(path,payload,mtime_ms) VALUES (?,?,?)').run(relPath,JSON.stringify(payload),artifactDb.serverTime());
      artifactDb.prepare('INSERT INTO call_logs(id,timestamp,status,detail_state,artifact_relpath) VALUES (?,?,?,?,?)').run(original.artifactId,timestamp,200,'ready',relPath);
    })();
  } finally {await artifactDb.close();}
  await migratedHttp(original,{name:'upstream',sticky:43,content:'Original SQLite migration memory'});assert.equal(hash(original.file),originalHash);
  console.log('PASS complete original SQLite migration: real unoverlaid 222-migration source, every ordinary table imported and checked, preserved ledger/hash, actual MongoDB service startup/auth/Settings/Provider/Memory HTTP and new key write');
  const quiescentUntil=Date.now()+25000;
  while(!await test.client.db(appImported).collection('_ha_serial').findOne({_id:'writer',ddl:null,$expr:{$lte:['$leaseUntil','$$NOW']}})) {
    assert.ok(Date.now()<quiescentUntil,'Imported app did not become quiescent for backup');await new Promise(resolve=>setTimeout(resolve,100));
  }
  const completeArchive=path.join(directory,'complete-source.bson');await backupDatabase(test.client,appImported,completeArchive);
  // Reuse the earlier owned restore target after every prior worker has closed.
  await test.client.db(restored).dropDatabase();
  let activationReplyLost=false;
  const uncertainClient={db(database){
    const selected=test.client.db(database);if(database!==restored)return selected;
    return new Proxy(selected,{get(target,property){
      if(property==='collection')return collectionName=>{
        const collection=target.collection(collectionName);if(collectionName!=='_ha_serial')return collection;
        return new Proxy(collection,{get(target,property){
          if(property==='updateOne')return async(...args)=>{
            const result=await target.updateOne(...args);
            if(args[1]?.$unset?.ddl===''){activationReplyLost=true;throw new Error('synthetic-lost-activation-ack');}
            return result;
          };
          const value=Reflect.get(target,property);return typeof value==='function'?value.bind(target):value;
        }});
      };
      const value=Reflect.get(target,property);return typeof value==='function'?value.bind(target):value;
    }});
  }};
  await assert.rejects(restoreDatabase(uncertainClient,completeArchive,restored,{quiescentSource:appImported}),/synthetic-lost-activation-ack/);
  assert.equal(activationReplyLost,true);
  assert.equal(typeof(await test.client.db(appImported).collection('_ha_serial').findOne({_id:'writer'})).ddl,'string','An uncertain target activation must never reopen the source');
  assert.equal((await test.client.db(restored).collection('_ha_serial').findOne({_id:'writer'})).ddl,undefined,'The injected lost reply occurs after the target activation actually committed');
  console.log('PASS uncertain restore activation: actual target update commits before the synthetic lost reply; the caller receives uncertainty and the old authority stays fenced, preventing dual authorities');
  assert.equal(await test.client.db(restored).collection('_omniroute_migrations').countDocuments(),222);
  await migratedHttp(original,{name:'complete-restore',sticky:43,content:'Original SQLite migration memory',database:restored});assert.equal(hash(original.file),originalHash);
  console.log('PASS complete-schema recovery: full imported dataset/types/default metadata/indexes and ledger survive BSON backup/restore, then actual service startup/auth/Settings/Provider/Memory HTTP and another Mongo key write pass');
  console.log('PASS SQLite migration: real read-only source, full core schema/indexes, values/BLOBs, rowids, deleted AUTOINCREMENT high-water mark, source hash unchanged, existing target refused');
  console.log('PASS explicit Memory migration: pinned FTS definitions/triggers/shadows replaced by native text index, content retained, reindex marked; rowid shadow and WITHOUT ROWID ordering preserved');
  console.log('PASS backup/restore: quiescent fenced source, snapshot archive, BSON type/content/count checks, index reconstruction, new ownership epoch, corrupt archive and existing target refused');
  console.log('PASS copied OAuth safety: SQLite import fences one-time groups; even previously completed archive credentials require verification and cannot dispatch after restore');
  console.log('PASS copied unattached import safety: archived reserved receipt requires verification even without a provider row; restore cannot assume the original grant was never dispatched');
  console.log('ARTIFACT synthetic source/archive retained: ' + directory);
} finally {
  if(application&&application.exitCode===null&&!application.signalCode){const exit=once(application,'exit');application.kill('SIGTERM');await exit;}
  source?.close(); await verify?.close();
  for (const database of [imported, restored,appImported]) await test.client.db(database).dropDatabase(); // Names generated only by this test.
  await test.cleanup();
}
}
