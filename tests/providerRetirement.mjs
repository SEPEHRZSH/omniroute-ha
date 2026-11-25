import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync,writeFileSync,mkdtempSync,existsSync } from 'node:fs';
import {spawnSync} from 'node:child_process';
import {createHash,randomUUID} from 'node:crypto';
import {BSON,Int32} from 'mongodb';
import path from 'node:path';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { installBaseSchema,readBaseSchema } from '../adapter/baseSchema.mjs';
import { testDatabase } from './mongoTestSupport.mjs';
import {backupDatabase,restoreDatabase,inspectArchive,repairProviderRetirement} from '../adapter/dataTransfer.mjs';

Object.assign(process.env,{NODE_ENV:'test',DATA_DIR:path.resolve('.work/test-artifacts'),DISABLE_SQLITE_AUTO_BACKUP:'1',OMNIROUTE_DISABLE_BACKGROUND_SERVICES:'true',OMNIROUTE_DISABLE_REDIS_AUTH_CACHE:'true',REDIS_URL:'',STORAGE_ENCRYPTION_KEY:'synthetic-retirement-storage-secret-for-tests'});
await import('../adapter/register.mjs');
const {ensureProviderConnectionsColumns}=await import('../upstream/omniroute/src/lib/db/schemaColumns.ts');
const test=await testDatabase('retirement');
const db=new MongoAdapter({uri:test.uri,database:test.database}),sqlite=new DatabaseSync(':memory:');
let active,standby;
const legacyTargets=['ha_test_retire_old_'+randomUUID().replaceAll('-',''),'ha_test_retire_fixed_'+randomUUID().replaceAll('-','')];
const migration=name=>readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/'+name+'.sql',import.meta.url),'utf8');
const stamp='2099-01-01T00:00:00.000Z';
const providerInsert='INSERT INTO provider_connections(id,provider,auth_type,is_active,created_at,updated_at) VALUES (?,?,?,?,?,?)';
function normalized(row){return Object.fromEntries(Object.entries(row).map(([key,value])=>[key,typeof value==='string'&&/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$/.test(value)?'<current SQLite datetime>':value]));}
function snapshot(target,id){return normalized(target.prepare('SELECT provider,is_active,test_status,error_code,last_error,last_error_type,last_error_source,last_error_at,updated_at FROM provider_connections WHERE id=?').get(id));}
function lease(target,id){return normalized(target.prepare('SELECT provider,connection_id,state,ended_at,end_reason FROM exclusive_connection_leases WHERE id=?').get(id));}
try {
  for(const target of [db,sqlite]) {
    if(target===db)installBaseSchema(target);else for(const sql of readBaseSchema().statements)target.exec(sql);
    ensureProviderConnectionsColumns(target);
    target.exec(migration('157_exclusive_connection_leases'));
  }
  db.installRequestLimits(); // Offline prerequisites, before any ownership epoch.
  for(const name of ['164_retire_microsoft_designer_web','165_retire_felo_web','167_retire_qwen_web','168_retire_chatgpt_web','171_restore_chatgpt_web_cleanroom'])sqlite.exec(migration(name));
  for(const [id,provider] of [['felo','\ufeff FELO\u3000'],['qwen','\u00a0QW\u2000'],['legacy-chat','CGPT-WEB'],['designer','\tMSDESIGNER\r'],['canonical-chat','chatgpt-web'],['ordinary','openai'],['ordinary-second','openai']]) {
    for(const target of [db,sqlite])target.prepare(providerInsert).run(id,provider,'apikey',1,stamp,stamp);
    assert.deepEqual(snapshot(db,id),snapshot(sqlite,id),'Original retirement trigger: '+id);
  }
  console.log('PASS native Provider retirement: SQLite-differential canonical restoration, legacy tombstones, exact Unicode/ASCII trimming and stored status/error behavior');
  const leaseInsert='INSERT INTO exclusive_connection_leases(id,lease_owner_hash,api_key_id,provider,connection_id,generation,state,acquired_at,renewed_at,expires_at,ended_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)';
  let leaseId=0;
  for(const [provider,connection,state] of [['openai','felo','ACTIVE'],['openai','qwen','ACTIVE'],['openai','legacy-chat','ACTIVE'],['openai','designer','ACTIVE'],['felo','qwen','ACTIVE'],['msdesigner','legacy-chat','ACTIVE'],['chatgpt-web','canonical-chat','ACTIVE'],['\u00a0MSDESIGNER\u00a0','ordinary','ACTIVE'],['felo','felo','RELEASED'],['openai','ordinary-second','ACTIVE']]) {
    const id=++leaseId,args=[id,id.toString(16).padStart(64,'0'),'synthetic-key',provider,connection,1,state,stamp,stamp,stamp,'previous-ended-at'];
    for(const target of [db,sqlite])target.prepare(leaseInsert).run(...args);
    assert.deepEqual(lease(db,id),lease(sqlite,id),'Original lease trigger: '+id);
  }
  for(const target of [db,sqlite])target.prepare('UPDATE exclusive_connection_leases SET provider=? WHERE id=?').run('qw',10);
  assert.deepEqual(lease(db,10),lease(sqlite,10));
  for(const id of ['felo','canonical-chat'])for(const sql of [
    'UPDATE provider_connections SET provider=? WHERE id=?',
    "INSERT OR REPLACE INTO provider_connections(provider,id,auth_type,created_at,updated_at) VALUES (?,?,'apikey','2099','2099')",
    "INSERT OR IGNORE INTO provider_connections(provider,id,auth_type,created_at,updated_at) VALUES (?,?,'apikey','2099','2099')",
    "INSERT INTO provider_connections(provider,id,auth_type,created_at,updated_at) VALUES (?,?,'apikey','2099','2099') ON CONFLICT(id) DO UPDATE SET is_active=1",
  ]) {
    const before=snapshot(db,id);
    for(const target of [db,sqlite])assert.throws(()=>target.prepare(sql).run('openai',id),error=>error.code==='SQLITE_CONSTRAINT_TRIGGER'||error.errcode===1811);
    assert.deepEqual(snapshot(db,id),before);assert.deepEqual(snapshot(db,id),snapshot(sqlite,id));
  }
  const returning=providerInsert+' RETURNING id,is_active,test_status';
  assert.deepEqual(db.prepare(returning).all('returning','felo','apikey',1,stamp,stamp),sqlite.prepare(returning).all('returning','felo','apikey',1,stamp,stamp).map(row=>({...row})));
  assert.equal(snapshot(db,'returning').is_active,0);
  for(const target of [db,sqlite])target.prepare('UPDATE provider_connections SET updated_at=? WHERE id=?').run('manual-unchanged-trigger-clock','felo');
  assert.deepEqual(snapshot(db,'felo'),snapshot(sqlite,'felo'));
  const upsert=providerInsert+' ON CONFLICT(id) DO UPDATE SET is_active=1 RETURNING id,is_active';
  assert.deepEqual(db.prepare(upsert).all('felo','felo','apikey',1,stamp,stamp),sqlite.prepare(upsert).all('felo','felo','apikey',1,stamp,stamp).map(row=>({...row})));
  assert.deepEqual(snapshot(db,'felo'),snapshot(sqlite,'felo'));
  const change="UPDATE provider_connections SET provider='qw',is_active=1 WHERE id='ordinary'";
  assert.throws(()=>db.transaction(()=>{db.exec(change);assert.equal(lease(db,8).state,'INVALIDATED');throw new Error('rollback retired provider');})(),/rollback retired provider/);
  sqlite.exec('BEGIN');sqlite.exec(change);assert.equal(lease(sqlite,8).state,'INVALIDATED');sqlite.exec('ROLLBACK');
  assert.deepEqual(snapshot(db,'ordinary'),snapshot(sqlite,'ordinary'));assert.deepEqual(lease(db,8),lease(sqlite,8));
  for(const target of [db,sqlite])target.exec(change);
  assert.deepEqual(snapshot(db,'ordinary'),snapshot(sqlite,'ordinary'));assert.deepEqual(lease(db,8),lease(sqlite,8));
  console.log('PASS native retirement lifecycle: SQLite-differential lease precedence/timestamps, identity UPDATE/REPLACE/IGNORE/UPSERT refusal, pre-trigger RETURNING, watched-column behavior and atomic provider/lease rollback');
  db.prepare(providerInsert).run('fenced-provider','openai','apikey',1,stamp,stamp);
  db.prepare(leaseInsert).run(11,'b'.repeat(64),'synthetic-key','openai','fenced-provider',1,'ACTIVE',stamp,stamp,stamp,null);
  active=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-retirement-active'});
  standby=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-retirement-standby'});
  assert.ok(active.ownership('claim',10000));assert.equal(standby.ownership('claim',10000),null);
  assert.throws(()=>standby.prepare("UPDATE provider_connections SET provider='felo' WHERE id='fenced-provider'").run(),{code:'HA_FENCED'});
  assert.equal(lease(db,11).state,'ACTIVE');
  active.prepare("UPDATE provider_connections SET provider='felo' WHERE id='fenced-provider'").run();
  assert.equal(snapshot(db,'fenced-provider').is_active,0);assert.equal(lease(db,11).state,'INVALIDATED');
  active.ownership('release',10000);assert.ok(standby.ownership('claim',10000));
  assert.throws(()=>active.prepare("UPDATE provider_connections SET is_active=1 WHERE id='fenced-provider'").run(),{code:'HA_FENCED'});
  standby.prepare("UPDATE provider_connections SET is_active=1 WHERE id='fenced-provider'").run();
  assert.equal(snapshot(db,'fenced-provider').is_active,0);assert.equal(lease(db,11).state,'INVALIDATED');
  standby.ownership('release',10000);
  console.log('PASS retired Provider fencing: standby cannot mutate, active atomically invalidates lease, promoted owner retains tombstone and stale owner is fenced');
  await active.close();await standby.close();active=undefined;standby=undefined;
  // Direct mutation is intentional test-only legacy state, not an app write path.
  await test.db.collection('provider_connections').updateOne({id:'felo'},{$set:{is_active:1}});
  await test.db.collection('exclusive_connection_leases').updateOne({id:1},{$set:{state:'ACTIVE'}});
  const invalidOwner=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-invalid-legacy-owner'});
  try{assert.throws(()=>invalidOwner.verifyBaseSchema(),{code:'HA_SCHEMA_MISMATCH'});assert.equal(invalidOwner.canWrite(),false);}finally{await invalidOwner.close();}
  const directory=mkdtempSync(path.resolve('.work/test-artifacts/retirement-')),archive=path.join(directory,'must-not-exist.bson');
  await assert.rejects(backupDatabase(test.client,test.database,archive),/offline repair-provider-retirement/);assert.equal(existsSync(archive),false);
  const repaired=await repairProviderRetirement(test.client,test.uri,test.database);
  assert.deepEqual(repaired.repaired,{providers:1,leases:1});assert.equal(snapshot(db,'felo').is_active,0);assert.equal(lease(db,1).state,'INVALIDATED');
  const repeated=spawnSync(process.execPath,['scripts/data.mjs','repair-provider-retirement',test.database],{encoding:'utf8',timeout:30000,env:{...process.env,HA_MONGO_URI:test.uri}});
  assert.equal(repeated.status,0,repeated.stderr);assert.deepEqual(JSON.parse(repeated.stdout.trim()).repaired,{providers:0,leases:0});
  const freshOwner=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-repaired-owner'});
  try{assert.deepEqual(freshOwner.verifyBaseSchema(),{tables:17,indexes:22});assert.ok(freshOwner.ownership('claim',10000));
    const refused=spawnSync(process.execPath,['scripts/data.mjs','repair-provider-retirement',test.database],{encoding:'utf8',timeout:30000,env:{...process.env,HA_MONGO_URI:test.uri}});
    assert.notEqual(refused.status,0);assert.match(refused.stderr,/Stop applications/);freshOwner.ownership('release',10000);
  }finally{await freshOwner.close();}
  console.log('PASS legacy retirement recovery: readiness and backup fail closed, quiescent native repair is idempotent through the actual CLI, fresh readiness recovers and live-owner repair is refused');
  const current=path.join(directory,'current.bson'),legacy=path.join(directory,'legacy.bson');
  await backupDatabase(test.client,test.database,current);
  // Construct an internally valid synthetic old-adapter archive. Never bypass
  // the current backup guard to create an unsafe snapshot in application code.
  const input=readFileSync(current),parts=[],archiveHash=createHash('sha256');let hashes;
  assert.ok(input.length<5*1024*1024,'Synthetic legacy archive must stay small');
  for(let offset=0;offset<input.length;){const size=input.readInt32LE(offset),frame=BSON.deserialize(input.subarray(offset,offset+size),{promoteValues:false});offset+=size;
    if(frame.kind==='header')hashes=Object.fromEntries(frame.collections.map(collection=>[collection.name,createHash('sha256')]));
    if(frame.kind==='document'){
      if(frame.collection==='provider_connections'&&frame.document.id==='felo')frame.document.is_active=new Int32(1);
      if(frame.collection==='exclusive_connection_leases'&&Number(frame.document.id)===1)frame.document.state='ACTIVE';
      hashes[frame.collection].update(BSON.serialize(frame.document));
    }
    if(frame.kind==='footer'){frame.sha256=archiveHash.digest('hex');frame.hashes=Object.fromEntries(Object.entries(hashes).map(([name,hash])=>[name,hash.digest('hex')]));}
    const bytes=BSON.serialize(frame);parts.push(bytes);if(frame.kind!=='footer')archiveHash.update(bytes);
  }
  writeFileSync(legacy,Buffer.concat(parts),{mode:0o600,flag:'wx'});inspectArchive(legacy);
  const legacyHash=createHash('sha256').update(readFileSync(legacy)).digest('hex');
  await assert.rejects(restoreDatabase(test.client,legacy,legacyTargets[0]),/offline repair-provider-retirement/);
  const held=await test.client.db(legacyTargets[0]).collection('_ha_serial').findOne({_id:'writer'});
  assert.ok(held.ddl,'Invalid restore must remain fenced');
  for(const [token,epoch] of [[randomUUID(),held.epoch],[held.ddl,held.epoch+1]]) {
    const invalidAdmin=new MongoAdapter({uri:test.uri,database:legacyTargets[0],maintenanceToken:token,maintenanceEpoch:epoch});
    try{assert.throws(()=>invalidAdmin.prepare('UPDATE provider_connections SET is_active=0 WHERE id=?').run('felo'),{code:'HA_FENCED'});}finally{await invalidAdmin.close();}
  }
  const restored=spawnSync(process.execPath,['scripts/data.mjs','restore',legacy,legacyTargets[1],'--repair-retirement','--quiescent-source='+test.database],{encoding:'utf8',timeout:60000,env:{...process.env,HA_MONGO_URI:test.uri}});
  assert.equal(restored.status,0,restored.stderr);assert.deepEqual(JSON.parse(restored.stdout.trim()).retirementRepairs,{providers:1,leases:1});
  assert.equal((await test.client.db(legacyTargets[1]).collection('provider_connections').findOne({id:'felo'})).is_active,0);
  assert.equal((await test.client.db(legacyTargets[1]).collection('exclusive_connection_leases').findOne({id:1})).state,'INVALIDATED');
  assert.equal((await test.client.db(legacyTargets[1]).collection('_ha_serial').findOne({_id:'writer'})).ddl,undefined);
  assert.equal(JSON.parse(restored.stdout.trim()).activated,true);
  assert.equal(typeof(await test.db.collection('_ha_serial').findOne({_id:'writer'})).ddl,'string','Verified CLI cutover must retain the old authority gate');
  assert.equal(createHash('sha256').update(readFileSync(legacy)).digest('hex'),legacyHash);
  console.log('PASS legacy retirement archive recovery: valid old-state BSON is refused/fenced by default; explicit real CLI restore repairs native constraints, reports counts, activates safely and preserves the input');
} finally {await active?.close();await standby?.close();await db.close();sqlite.close();try{for(const target of legacyTargets)await test.client.db(target).dropDatabase({maxTimeMS:15000,timeoutMS:20000});}finally{await test.cleanup();}}
