import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {createServer} from 'node:http';
import {once} from 'node:events';
import {fork,execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdirSync,writeFileSync,unlinkSync} from 'node:fs';
import path from 'node:path';
import {randomUUID} from 'node:crypto';
import {MongoAdapter} from '../adapter/mongoAdapter.mjs';
import {providerIdentitySql,providerReconciliationSql} from '../adapter/providerQueries.mjs';
import {testDatabase} from './mongoTestSupport.mjs';

Object.assign(process.env,{NODE_ENV:'test',DATA_DIR:path.resolve('.work/domain-data'),DISABLE_SQLITE_AUTO_BACKUP:'1',
  STORAGE_ENCRYPTION_KEY:'synthetic-storage-key-for-ha-tests-only',CODEX_REFRESH_SPACING_MS:'0',
  HTTP_PROXY:'',HTTPS_PROXY:'',ALL_PROXY:'',REDIS_URL:'',OMNIROUTE_TOKEN_REFRESH_PASSIVE_PROVIDERS:''});
mkdirSync(process.env.DATA_DIR,{recursive:true,mode:0o700});
await import('../adapter/register.mjs');
const {createCredentialRuntime}=await import('../adapter/credentialRuntime.mjs');
const providers=await import('../upstream/omniroute/src/lib/db/providers.ts');
const {normalizeCodexImportRecord,preserveExistingCodexConnectionState}=await import('../upstream/omniroute/src/lib/oauth/services/codexImport.ts');
async function create(payload) {
  return providers.createProviderConnection(preserveExistingCodexConnectionState(payload,await providers.getProviderConnections({provider:'codex',authType:'oauth'})));
}
function install(db) {
  globalThis.__omnirouteMongoAdapter=db;
  const runtime={assertOwner(){assert.equal(db.canWrite(),true,'Current owner required');}};
  return globalThis.__omnirouteCredentialRuntime=createCredentialRuntime(db,runtime);
}
function payload(record) {const result=normalizeCodexImportRecord(record);assert.equal(result.ok,true);return result.payload;}
const record=(label,workspace=label)=>({access_token:'synthetic-import-old-'+label,refresh_token:'synthetic-import-seed-'+label,email:label+'@example.invalid',account_id:workspace});

if(process.argv.includes('--peer')) {
  const [message]=await once(process,'message');
  const db=new MongoAdapter({uri:message.uri,database:message.database,ownerId:'synthetic-import-peer'});
  try {
    assert.ok(db.ownership('claim',60000));
    globalThis.fetch=()=>{throw new Error('Resuming publication must not access the network');};
    const api=install(db),result=await api.importCodex(payload(message.record),create,message.record);
    process.send({id:result.id});db.ownership('release');
  } catch(error) {process.send({error:error.code||error.message});process.exitCode=1;}
  finally {await db.close();process.disconnect();}
} else {
  const test=await testDatabase('imports'),bootstrap=new MongoAdapter({uri:test.uri,database:test.database});
  let db,server,peer;const sqlite=new DatabaseSync(':memory:');let calls=0;
  try {
    const {provisionDomains}=await import('./provisionDomains.mjs');await provisionDomains(bootstrap);bootstrap.installRequestLimits();
    // Differential checks use precisely the four SELECTs extracted from the
    // pinned official writer, including JSON1's first-property/type behavior.
    sqlite.exec('CREATE TABLE provider_connections(id TEXT,provider TEXT,auth_type TEXT,email TEXT,provider_specific_data TEXT,created_at TEXT,updated_at TEXT)');
    const metadata=[null,'{}','{"workspaceId":42,"chatgptUserId":"u"}','{"workspaceId":null,"chatgptUserId":"u"}',
      '{"workspaceId":"w","chatgptUserId":"u"}','{"workspaceId":"w","workspaceId":"other","chatgptUserId":"u"}',
      '{"workspaceId":"","chatgptUserId":"u"}','{"workspaceId":[ 1, 2 ],"chatgptUserId":"u"}'];
    for(let index=0;index<metadata.length;index++) {
      const args=['synthetic-json-'+index,'codex','oauth','json@example.invalid',metadata[index],index%2?'2026-01-01':'2026-02-01','2026-01-01'];
      const sql='INSERT INTO provider_connections(id,provider,auth_type,email,provider_specific_data,created_at,updated_at) VALUES (?,?,?,?,?,?,?)';
      bootstrap.prepare(sql).run(...args);sqlite.prepare(sql).run(...args);
    }
    for(const [index,args] of [[0,['codex','w','u']],[1,['codex','u']],[2,['codex','w']],[2,['codex','[1,2]']],[3,['codex','w','json@example.invalid']]]) {
      const actual=bootstrap.prepare(providerIdentitySql[index]).all(...args).map(row=>row.id),expected=sqlite.prepare(providerIdentitySql[index]).all(...args).map(row=>row.id);
      assert.deepEqual(actual,expected);
    }
    bootstrap.prepare('DELETE FROM provider_connections').run();
    console.log('PASS Codex identity SQL: all four original queries match SQLite JSON1 null/types/duplicates/order');
    sqlite.exec('CREATE TABLE usage_history(id INTEGER PRIMARY KEY,connection_id TEXT,account_key TEXT,account_label TEXT,account_label_priority INTEGER,timestamp TEXT NOT NULL)');
    for(let index=0;index<5;index++) {
      const sql='INSERT INTO usage_history(id,connection_id,account_key,account_label,account_label_priority,timestamp) VALUES (?,?,?,?,?,?)';
      const args=[index+1,'synthetic-reconcile','old','Original '+index,[null,0,3,4,5][index],'2026-01-01'];
      bootstrap.prepare(sql).run(...args);sqlite.prepare(sql).run(...args);
    }
    const args={newAccountKey:'new',newLabelPriority:4,newLabel:'$literal-label',connectionId:'synthetic-reconcile',oldAccountKey:'old'};
    assert.equal(bootstrap.prepare(providerReconciliationSql).run(args).changes,Number(sqlite.prepare(providerReconciliationSql).run(args).changes));
    const query='SELECT id,account_key,account_label,account_label_priority FROM usage_history ORDER BY id';
    assert.deepEqual(bootstrap.prepare(query).all(),sqlite.prepare(query).all().map(row=>({...row})));
    console.log('PASS original Codex usage identity reconciliation vs SQLite: null/lower/equal/higher label priorities and literal binding');
    server=createServer((req,res)=>{let wire='';req.on('data',chunk=>{wire+=chunk;});req.on('end',()=>{
      calls++;assert.equal(req.method,'POST');assert.equal(req.url,'/oauth/token');
      const token=new URLSearchParams(wire).get('refresh_token');assert.ok(token.startsWith('synthetic-import-seed-'));
      res.setHeader('content-type','application/json');
      res.end(JSON.stringify({access_token:'synthetic-import-access-'+calls,refresh_token:'synthetic-import-rotated-'+calls,expires_in:3600}));
    });});server.listen(0,'127.0.0.1');await once(server,'listening');
    const {OAUTH_ENDPOINTS}=await import('../upstream/omniroute/open-sse/config/constants.ts');OAUTH_ENDPOINTS.openai.token='http://127.0.0.1:'+server.address().port+'/oauth/token';
    db=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-import-primary'});assert.ok(db.ownership('claim',60000));let api=install(db);
    const {credentialTokenHash}=await import('../adapter/credentialImports.mjs');
    const abandoned=randomUUID(),abandonedKey=credentialTokenHash('synthetic-abandoned-root-import');
    db.credential('importStage',{id:abandoned,importKey:abandonedKey,payloadHash:abandonedKey,identity:{email:'root-unknown@example.invalid',workspaceId:null}});
    db.credential('importDispatch',{id:abandoned});db.ownership('release');
    const cli=async(...args)=>JSON.parse((await promisify(execFile)(process.execPath,['scripts/credentials.mjs',...args],{env:{...process.env,HA_MONGO_URI:test.uri},timeout:20000,maxBuffer:262144})).stdout);
    const status=await cli('status',test.database,'codex');assert.equal(status.pendingImports[0].state,'dispatched');assert.equal(status.connections.length,0);
    const evidence=path.join(process.env.DATA_DIR,'import-recovery-'+randomUUID()+'.json');
    writeFileSync(evidence,JSON.stringify({id:randomUUID(),provider:'codex',laneVersion:status.laneVersion,previousGrantsRevoked:true,verifiedBy:'synthetic-test-operator',providerReference:'synthetic-revocation-receipt',verifiedAt:new Date().toISOString(),connections:[]}),{mode:0o600,flag:'wx'});
    try{assert.equal((await cli('recover',test.database,evidence,'--offline')).connections,0);}finally{unlinkSync(evidence);}
    assert.equal((await test.db.collection('_ha_credential_intents').findOne({_id:abandoned})).state,'reconciled');
    assert.ok(db.ownership('claim',60000));
    assert.throws(()=>db.credential('importStage',{id:randomUUID(),importKey:abandonedKey,payloadHash:abandonedKey,identity:{email:'root-unknown@example.invalid',workspaceId:null}}),{code:'HA_CREDENTIAL_UNCERTAIN'});
    const neverSent=randomUUID(),neverKey=credentialTokenHash('synthetic-never-sent');
    db.credential('importStage',{id:neverSent,importKey:neverKey,payloadHash:neverKey,identity:{email:'never-sent@example.invalid',workspaceId:null}});
    db.ownership('release');assert.ok(db.ownership('claim',60000));
    console.log('PASS actual credential CLI: private zero-member import recovery, durable audit/reconciliation and consumed-grant rejection');
    const first=record('first'),[one,two]=await Promise.all([1,2].map(()=>api.importCodex(payload(first),create,first)));
    assert.equal(one.id,two.id);assert.equal(calls,1);
    assert.equal((await test.db.collection('_ha_credential_intents').findOne({_id:neverSent})).state,'cancelled','Never-dispatched old-epoch receipt may safely release its lane');
    assert.equal((await api.importCodex(payload(first),create,first)).id,one.id);assert.equal(calls,1);
    await assert.rejects(api.importCodex(payload({...first,priority:9}),create,{...first,priority:9}),{code:'HA_CREDENTIAL_STALE'});
    let receipt=await test.db.collection('_ha_credential_intents').findOne({kind:'import',connectionId:one.id});assert.equal(receipt.state,'attached');assert.ok(receipt.fields.refresh_token.startsWith('enc:v1:'));
    assert.equal((await providers.getProviderConnectionById(one.id)).refreshToken,'synthetic-import-rotated-1');
    await providers.updateProviderConnection(one.id,{priority:7,providerSpecificData:{workspaceId:'first',chatgptUserId:'synthetic-user',codexFingerprintMode:'synthetic-preserved'}});
    const updated=record('first','first');updated.refresh_token='synthetic-import-seed-new';updated.priority=99;
    const upsert=await api.importCodex(payload(updated),create,updated);assert.equal(upsert.id,one.id);
    const stored=await providers.getProviderConnectionById(one.id);assert.equal(stored.priority,7);assert.equal(stored.providerSpecificData.codexFingerprintMode,'synthetic-preserved');assert.equal(stored.providerSpecificData.chatgptUserId,'synthetic-user');assert.equal(calls,2);
    console.log('PASS actual Codex source import: one POST, encrypted durable attachment, concurrent/repeated idempotency and original metadata/priority-preserving upsert');
    const resumed=record('resume');
    await assert.rejects(api.importCodex(payload(resumed),async()=>{throw new Error('synthetic creator interruption');},resumed),/synthetic creator interruption/);
    assert.equal(calls,3);assert.equal(await test.db.collection('provider_connections').countDocuments({email:resumed.email}),0);
    receipt=await test.db.collection('_ha_credential_intents').findOne({kind:'import',state:'published'});assert.ok(receipt.fields.refresh_token.startsWith('enc:v1:'));
    await assert.rejects(providers.createProviderConnection({provider:'openai',authType:'oauth',email:'sibling@example.invalid',accessToken:'synthetic',refreshToken:'synthetic'}),{code:'HA_CREDENTIAL_UNCERTAIN'});
    db.ownership('release');await db.close();
    peer=fork(new URL(import.meta.url),['--peer'],{execArgv:[],env:process.env,stdio:['ignore','inherit','inherit','ipc']});
    const answer=once(peer,'message'),exit=once(peer,'exit');peer.send({uri:test.uri,database:test.database,record:resumed});const [result]=await answer;assert.equal(result.error,undefined);assert.equal((await exit)[0],0);
    assert.ok(result.id);assert.equal(calls,3);assert.equal((await test.db.collection('_ha_credential_intents').findOne({_id:receipt._id})).state,'attached');
    console.log('PASS real fresh-process import recovery: published encrypted pair resumes through original SQL writer without another exchange or pre-created provider row');
    db=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-import-recovered'});assert.ok(db.ownership('claim',60000));api=install(db);
    const rejected=record('wrong');
    await assert.rejects(api.importCodex(payload(rejected),data=>create({...data,refreshToken:'synthetic-wrong-pair'}),rejected),{code:'HA_CREDENTIAL_STALE'});
    assert.equal(await test.db.collection('provider_connections').countDocuments({email:rejected.email}),0,'Attachment failure must roll back the original INSERT');
    assert.equal(await test.db.collection('_ha_credential_intents').countDocuments({kind:'import',state:'published'}),1);
    const corrected=await api.importCodex(payload(rejected),create,rejected);assert.ok(corrected.id);assert.equal(calls,4);
    const uncertain=record('unknown'),id=randomUUID();
    db.credential('importStage',{id,importKey:credentialTokenHash(uncertain.refresh_token),payloadHash:credentialTokenHash('synthetic-unknown'),identity:{email:uncertain.email,workspaceId:'unknown'}});
    assert.equal(db.credential('importDispatch',{id}).dispatched,true);
    db.ownership('release');await db.close();db=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-import-next'});assert.ok(db.ownership('claim',60000));api=install(db);
    await assert.rejects(api.importCodex(payload(record('blocked')),create,record('blocked')),{code:'HA_CREDENTIAL_UNCERTAIN'});assert.equal(calls,4);
    assert.throws(()=>db.credential('importPublish',{id,fields:receipt.fields}),{code:'HA_CREDENTIAL_STALE'});
    console.log('PASS import safety: wrong-pair attachment rolls back atomically; published retry avoids network; unknown exchange blocks new owner and stale publication');
    db.ownership('release');
  } finally {
    peer?.kill();delete globalThis.__omnirouteMongoAdapter;delete globalThis.__omnirouteCredentialRuntime;
    await db?.close();await bootstrap.close();sqlite.close();if(server){server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}await test.cleanup();
  }
}
