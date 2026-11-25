// Offline administrative tools only. node:sqlite is used solely to read an
// explicitly supplied, quiesced migration source; never by the application.
import assert from 'node:assert/strict';
import { BSON } from 'mongodb';
import { randomUUID, createHash } from 'node:crypto';
import { openSync, closeSync, readSync, writeSync, fsyncSync, existsSync, statSync, linkSync, unlinkSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { workspacePath } from './workspacePath.mjs';
import { MongoAdapter } from './mongoAdapter.mjs';
import { parse, splitStatements } from './sql.mjs';
import { compileTable,assertStoredSchema } from './schema.mjs';
import { readBaseSchema, checkBaseSchema } from './baseSchema.mjs';
import { fenceCopiedCredentials } from './credentialState.mjs';
import {providerRetirementTriggers,assertProviderRetirementSchema,inspectProviderRetirement,verifyProviderRetirement} from './providerRetirement.mjs';
import {freeProxyTrigger,assertFreeProxyCleanupSchema} from './freeProxyQueries.mjs';
import {provisionAuditSchema,provisionArtifactSchema} from './startupSchema.mjs';
import {reasoningExpirySql} from './retentionQueries.mjs';

const wc = { w: 'majority', j: true, wtimeoutMS: 5000 };
const digest = () => createHash('sha256');
function normalizeSchemaSql(sql) {
  // Normalize only the leading DDL clause and whitespace outside quoted values.
  // Never erase an IF NOT EXISTS phrase or collapse spaces inside a SQL literal.
  const ddl=sql.trim().replace(/^(CREATE (?:VIRTUAL )?(?:TABLE|TRIGGER)|CREATE (?:UNIQUE )?INDEX)\s+IF NOT EXISTS\s+/i,'$1 ');
  return ddl.match(/'(?:[^']|'')*'|"(?:[^"]|"")*"|[^'"]+/g)
    .map(part=>part.startsWith("'")||part.startsWith('"')?part:part.replace(/\s+/g,' ')).join('').replace(/;$/,'');
}
// These non-unique SQLite indexes are optimizations, not data constraints.
// Keep all UNIQUE indexes and query semantics; translate only exact pinned SQL.
function mapSqliteIndexes(objects) {
  const translated=[];
  const pinned=(file,hash)=>{
    const sql=readFileSync(new URL('../upstream/omniroute/src/lib/db/'+file,import.meta.url),'utf8');
    assert.equal(digest().update(sql).digest('hex'),hash,'Index mapping source changed: '+file);
    return sql;
  };
  const canonical=sql=>normalizeSchemaSql(sql).replace(/\(\s+/g,'(').replace(/\s+\)/g,')');
  const dedup=objects.find(object=>object.type==='index'&&object.name==='idx_uh_dedup');
  if(dedup) {
    const source=pinned('schemaColumns.ts','38e9783b645a60c966c0f996584473fdf0bf97c1a365a366e9c2620b1b09f73e');
    const expected=source.match(/`(CREATE INDEX IF NOT EXISTS idx_uh_dedup[\s\S]*?)`/)?.[1];
    assert.ok(expected,'Usage dedup index anchor changed');
    const replacement='CREATE INDEX IF NOT EXISTS idx_uh_dedup ON usage_history(timestamp,provider,model,connection_id,api_key_id,tokens_input,tokens_output)';
    if(canonical(dedup.sql)===canonical(expected)) {
      objects=objects.map(object=>object===dedup?{...object,sql:replacement}:object);
      translated.push({name:dedup.name,replacement:'raw-column optimization'});
    } else assert.equal(canonical(dedup.sql),canonical(replacement),'Unrecognized usage dedup index; explicit mapping required');
  }
  const aliases=objects.filter(object=>object.type==='index'&&/^idx_(daily|hourly)_usage_provider_model_date_composite$/.test(object.name));
  if(aliases.length) {
    const original=splitStatements(pinned('migrations/047_aggregation_tables.sql','7c8ca3483e8e8cd6e91ad995cf5956d52c5991f862215c8305cbe75c5fced9ae'));
    const repeated=splitStatements(pinned('migrations/048_summary_indexes.sql','1d51401f3dc18e6b6e6343969bf210b2b96f3f7a885dae45c416be8bbd0aa7fc'));
    for(const alias of aliases) {
      const baseName=alias.name.replace(/_composite$/,'');
      const definition=(statements,name)=>{
        const prefix='CREATE INDEX IF NOT EXISTS '+name;
        const sql=statements.find(value=>value.includes(prefix+'\n')||value.includes(prefix+' '));
        // splitStatements preserves leading migration comments; sqlite_master does not.
        return sql?.slice(sql.indexOf(prefix));
      };
      const expected=definition(repeated,alias.name);
      assert.ok(expected,'Summary index alias anchor changed');
      assert.equal(canonical(alias.sql),canonical(expected),'Unrecognized summary index alias; explicit mapping required');
      const base=objects.find(object=>object.type==='index'&&object.name===baseName);
      if(!base)continue; // Preserve a valid alias if it is the only physical index.
      const baseSql=definition(original,baseName);
      assert.ok(baseSql,'Original summary index anchor changed');
      assert.equal(canonical(base.sql),canonical(baseSql),'Unrecognized original summary index; explicit mapping required');
      objects=objects.filter(object=>object!==alias);
      translated.push({name:alias.name,aliasOf:baseName});
    }
  }
  return {objects,translated};
}
function fileHash(file) {
  const fd = openSync(file, 'r'), hash = digest(), bytes = Buffer.alloc(1024 * 1024);
  try { for (let count; (count = readSync(fd, bytes, 0, bytes.length, null));) hash.update(bytes.subarray(0, count)); }
  finally { closeSync(fd); }
  return hash.digest('hex');
}
async function fenceCopiedRecoveries(db,token) {
  assert.ok(await db.collection('_ha_serial').findOne({_id:'writer',ownerId:null,ddl:token}),'Ticket copy fence requires offline maintenance ownership');
  const spec=await db.collection('_ha_schema').findOne({_id:'quota_recovery_tickets'});
  if(!spec){assert.equal(await db.listCollections({name:'quota_recovery_tickets'}).hasNext(),false,'Unregistered recovery-ticket collection');return 0;}
  assertStoredSchema(spec);
  for(const name of ['id','state','updated_at'])assert.equal(spec.columns.find(column=>column.name===name)?.type,'TEXT','Incompatible recovery-ticket copy schema');
  assert.ok(spec.columns.find(column=>column.name==='state').allowed?.includes('needs_confirmation'),'Ticket confirmation state is not supported');
  // Even a pending historical snapshot may have dispatched after that snapshot
  // was taken. Never resume copied execution automatically on a fresh authority.
  const changed=await db.collection('quota_recovery_tickets').updateMany({state:{$in:['pending','running']}},{$set:{state:'needs_confirmation',updated_at:new Date().toISOString()}},{writeConcern:wc});
  return changed.modifiedCount;
}
async function fenceCopiedQuota(db,token,imported=false){
  assert.ok(await db.collection('_ha_serial').findOne({_id:'writer',ownerId:null,ddl:token}),'Quota copy fence requires offline maintenance ownership');
  const result={keys:0,teams:0,pools:0};
  for(const [table,kind,profiles] of [['api_keys','keys',null],['teams','teams','_ha_team_profiles'],['quota_pools','pools','_ha_pool_profiles']]){
    const spec=await db.collection('_ha_schema').findOne({_id:table});
    if(!spec){assert.equal(await db.listCollections({name:table}).hasNext(),false,'Unregistered quota collection');continue;}
    assertStoredSchema(spec);const id=spec.columns.find(column=>column.name==='id');
    assert.ok(id?.type==='TEXT'&&id.primary,'Quota copy fence requires original TEXT primary identities');
    if(kind==='keys'){
      // Empty best-effort history is not proof that a pre-cutover key spent
      // nothing. Public management/auth remain usable; monetary/token admission
      // waits for the existing offline reviewed key/scoped cutover.
      if(imported)await db.collection(table).updateMany({},{$set:{_ha_requires_key_baseline:true}},{writeConcern:wc});
      result.keys=await db.collection(table).countDocuments({_ha_requires_key_baseline:true});continue;
    }
    for await(const row of db.collection(table).find({},{projection:{id:1},batchSize:256})){
      assert.ok(typeof row.id==='string'&&row.id,'Invalid quota copy identity');
      const profile=await db.collection(profiles).findOne({_id:row.id});
      const complete=kind==='pools'?profile?.allWindows===true&&profile.trackingVersion===1:profile?.complete===true;
      if(imported||!complete){
        await db.collection(profiles).updateOne({_id:row.id},{$set:{...(kind==='pools'?{allWindows:false,trackingVersion:0}:{complete:false}),requiresReview:imported?'sqlite-import':'archive-restore'}},{upsert:true,writeConcern:wc});
        result[kind]++;
      }
    }
  }
  return result;
}
const name = value => { assert.match(value, /^ha_test_[a-z0-9_]+$/); return value; };
const localFile = (file,existing=true) => workspacePath(file,{existing});
async function gate(db, fresh = false) {
  const token = randomUUID();
  if (fresh) {
    assert.equal((await db.listCollections().toArray()).length, 0, 'Target must be a new, empty database');
    await db.collection('_ha_serial').insertOne({ _id: 'writer', epoch: 0, revision: 0, ownerId: null, leaseUntil: new Date(0), ddl: token }, { writeConcern: wc });
  } else {
    const acquired = await db.collection('_ha_serial').findOneAndUpdate({ _id: 'writer', ddl: null,
      $expr: { $lte: ['$leaseUntil', '$$NOW'] } }, [{ $set: { ddl: token, ownerId: null, leaseUntil: '$$NOW', epoch: { $add: ['$epoch', 1] } } }],
    { returnDocument: 'after', maxTimeMS: 3000, writeConcern: wc });
    assert.ok(acquired, 'Stop applications and wait for their leases/transactions before backup');
  }
  return token;
}
async function activate(db, token) {
  const result = await db.collection('_ha_serial').updateOne({ _id: 'writer', ddl: token, ownerId: null }, { $unset: { ddl: '' } }, { writeConcern: wc });
  assert.equal(result.matchedCount, 1, 'Maintenance gate changed; refusing activation');
}
function writeAll(fd, bytes) {
  for (let offset = 0; offset < bytes.length;) {
    const written = writeSync(fd, bytes, offset, bytes.length - offset);
    assert.ok(written > 0); offset += written;
  }
}
function* frames(file) {
  const fd = openSync(localFile(file), 'r');
  function readExact(buffer, allowEnd = false) {
    let offset = 0;
    while (offset < buffer.length) {
      const count = readSync(fd, buffer, offset, buffer.length - offset, null);
      if (!count && allowEnd && !offset) return false;
      assert.ok(count, 'Truncated archive'); offset += count;
    }
    return true;
  }
  try {
    for (;;) {
      const prefix = Buffer.alloc(4);
      if (!readExact(prefix, true)) return;
      const size = prefix.readInt32LE();
      assert.ok(size >= 5 && size <= 32 * 1024 * 1024, 'Invalid archive frame');
      const bytes = Buffer.alloc(size); prefix.copy(bytes); readExact(bytes.subarray(4));
      yield { bytes, value: BSON.deserialize(bytes, { promoteValues: false }) };
    }
  } finally { closeSync(fd); }
}
export function inspectArchive(file) {
  const hash = digest(), counts = {}, hashes = {};
  let header, footer;
  for (const { bytes, value } of frames(file)) {
    assert.ok(!footer, 'Unexpected data after archive footer');
    if (value.kind === 'footer') { footer = value; continue; }
    hash.update(bytes);
    if (!header) {
      assert.equal(value.kind, 'header'); assert.equal(Number(value.version), 1);
      name(value.database); header = value;
      assert.ok(Array.isArray(header.collections) && new Set(header.collections.map(c => c.name)).size === header.collections.length);
      for (const c of header.collections) { assert.match(c.name, /^[A-Za-z_][A-Za-z0-9_]*$/); counts[c.name] = 0; hashes[c.name] = digest(); }
    } else {
      assert.equal(value.kind, 'document'); assert.ok(Object.hasOwn(counts, value.collection));
      if(value.collection==='_ha_schema')assertStoredSchema(value.document);
      counts[value.collection]++; hashes[value.collection].update(BSON.serialize(value.document));
    }
  }
  assert.ok(header && footer, 'Incomplete archive');
  assert.equal(hash.digest('hex'), footer.sha256, 'Archive checksum mismatch');
  for (const c of header.collections) {
    assert.equal(counts[c.name], Number(footer.counts[c.name]));
    assert.equal(hashes[c.name].digest('hex'), footer.hashes[c.name]);
  }
  return { header, footer };
}
export async function backupDatabase(client, database, output) {
  const db = client.db(name(database)), file = localFile(output, false);
  assert.ok(!existsSync(file), 'Never overwrite an existing backup');
  const token = await gate(db), partial = file + '.' + randomUUID() + '.partial';
  const session = client.startSession();
  let fd;
  try {
    await verifyProviderRetirement(db);
    for await(const spec of db.collection('_ha_schema').find({}, {batchSize:50}))assertStoredSchema(spec);
    const collections = await db.listCollections().toArray();
    const metadata = [];
    for (const c of collections.sort((a, b) => a.name.localeCompare(b.name))) {
      assert.equal(c.type, 'collection', 'Views require an explicit backup integration');
      metadata.push({ name: c.name, options: c.options, indexes: await db.collection(c.name).listIndexes().toArray() });
    }
    fd = openSync(partial, 'wx', 0o600);
    const hash = digest(), counts = {}, hashes = {};
    let size = 0;
    const write = value => { const bytes = BSON.serialize(value); size += bytes.length; assert.ok(size <= 512 * 1024 * 1024, 'Archive exceeds bounded offline-tool size'); writeAll(fd, bytes); hash.update(bytes); };
    write({ kind: 'header', version: 1, upstream: '191dce8d534c', database, createdAt: new Date(), collections: metadata });
    session.startTransaction({ readConcern: { level: 'snapshot' }, readPreference: 'primary' });
    for (const c of metadata) {
      counts[c.name] = 0; hashes[c.name] = digest();
      for await (const document of db.collection(c.name).find({}, { session, batchSize: 50, promoteValues: false }).sort({ _id: 1 })) {
        write({ kind: 'document', collection: c.name, document });
        hashes[c.name].update(BSON.serialize(document)); counts[c.name]++;
      }
    }
    await session.abortTransaction(); // Read-only snapshot; nothing to commit.
    writeAll(fd, BSON.serialize({ kind: 'footer', sha256: hash.digest('hex'), counts,
      hashes: Object.fromEntries(Object.entries(hashes).map(([key, value]) => [key, value.digest('hex')])) }));
    fsyncSync(fd); closeSync(fd); fd = undefined;
    inspectArchive(partial);
    linkSync(partial, file); unlinkSync(partial); // No overwrite, even if another file appeared.
    return { file, counts };
  } finally {
    if (fd !== undefined) closeSync(fd);
    if (session.inTransaction()) await session.abortTransaction().catch(() => {});
    await session.endSession();
    await activate(db, token); // Incomplete .partial files are preserved for diagnosis.
  }
}
async function repairRetirementUnderGate(client,uri,database,token) {
  const db=client.db(name(database)),invalid=await inspectProviderRetirement(db);
  if(!invalid.providers.length&&!invalid.leases.length)return {providers:0,leases:0};
  const fence=await db.collection('_ha_serial').findOne({_id:'writer',ownerId:null,ddl:token});
  assert.ok(fence&&Number.isSafeInteger(fence.epoch)&&fence.epoch>=0,'Offline retirement gate changed');
  const adapter=new MongoAdapter({uri,database,maintenanceToken:token,maintenanceEpoch:fence.epoch,timeoutMs:15000});
  try {
    adapter.transaction(()=>{
      for(const id of invalid.providers)adapter.prepare('UPDATE provider_connections SET is_active=is_active WHERE id=?').run(id);
      for(const id of invalid.leases)adapter.prepare("UPDATE exclusive_connection_leases SET state=state WHERE id=? AND state='ACTIVE'").run(id);
    })();
    await verifyProviderRetirement(db);
    return {providers:invalid.providers.length,leases:invalid.leases.length};
  } finally {await adapter.close();}
}
export async function repairProviderRetirement(client,uri,database) {
  const db=client.db(name(database)),token=await gate(db);
  // Failure/uncertain commit leaves the target fenced for explicit diagnosis.
  const repaired=await repairRetirementUnderGate(client,uri,database,token);
  await activate(db,token);return {database,repaired};
}
export async function restoreDatabase(client, input, target, {repairRetirement=false,uri,quiescentSource}={}) {
  assert.ok(!repairRetirement||uri,'Explicit restore retirement repair requires a MongoDB URI');
  const { header, footer } = inspectArchive(input); // Verify before any database mutation.
  assert.equal(header.upstream, '191dce8d534c', 'Archive version requires an explicit upgrade');
  if(quiescentSource!==undefined){
    name(quiescentSource);assert.equal(quiescentSource,header.database,'Quiescent source must match the archive authority');
    assert.notEqual(quiescentSource,target,'Restore requires a different target authority');
  }
  const db = client.db(name(target)), token = await gate(db, true);
  let archivedWriter;
    // Leave a failed target fenced for inspection before any further operation.
  for (const c of header.collections) if (c.name !== '_ha_serial') await db.createCollection(c.name, c.options);
  for (const { value } of frames(input)) {
    if (value.kind !== 'document') continue;
    if (value.collection === '_ha_serial' && value.document._id === 'writer') {
      const epoch = Number(value.document.epoch);
      assert.ok(Number.isSafeInteger(epoch) && epoch >= 0 && epoch<Number.MAX_SAFE_INTEGER);
      assert.equal(archivedWriter,undefined,'Duplicate archived writer');
      archivedWriter={epoch,revision:Number(value.document.revision)};
      assert.ok(Number.isSafeInteger(archivedWriter.revision)&&archivedWriter.revision>=0&&value.document.ownerId===null&&typeof value.document.ddl==='string','Archive must contain a quiesced writer');
      await db.collection('_ha_serial').replaceOne({ _id: 'writer', ddl: token }, { ...value.document,
        ownerId: null, epoch: epoch + 1, leaseUntil: new Date(0), ddl: token }, { writeConcern: wc });
    } else await db.collection(value.collection).insertOne(value.document, { writeConcern: wc });
  }
  for (const c of header.collections) {
    const count = await db.collection(c.name).countDocuments();
    assert.equal(count, Number(footer.counts[c.name]), 'Restored count mismatch: ' + c.name);
    if (c.name !== '_ha_serial') {
      const hash = digest();
      for await (const row of db.collection(c.name).find({}, { promoteValues: false }).sort({ _id: 1 })) hash.update(BSON.serialize(row));
      assert.equal(hash.digest('hex'), footer.hashes[c.name], 'Restored content mismatch: ' + c.name);
    }
    // Offline majority index builds can exceed ordinary application read
    // deadlines. Keep them bounded without weakening the activation gate or
    // retrying an uncertain operation against a partially restored target.
    for (const { v, ns, ...index } of c.indexes) if (index.name !== '_id_') await db.collection(c.name).createIndexes([index], { commitQuorum: 'majority', maxTimeMS: 45000, timeoutMS: 60000 });
  }
  assert.ok(await db.collection('_ha_schema').findOne({ _id: 'api_keys' }), 'Missing application schema');
  const plan = readBaseSchema(), catalog = new Map();
  for (const table of plan.tables.keys()) catalog.set(table, await db.collection(table).listIndexes().toArray());
  checkBaseSchema(plan, await db.collection('_ha_schema').find({}, { promoteValues: true }).toArray(), catalog);
  const retirementRepairs=repairRetirement?await repairRetirementUnderGate(client,uri,target,token):undefined;
  await verifyProviderRetirement(db);
  const credentialGroupsRequiringVerification=await fenceCopiedCredentials(db,'restore',wc);
  const quotaRecoveriesRequiringConfirmation=await fenceCopiedRecoveries(db,token);
  const quotaBaselinesRequiringReview=await fenceCopiedQuota(db,token);
  assert.ok(archivedWriter,'Archive is missing its writer fence');
  const result={ database: target, collections: header.collections.length,credentialGroupsRequiringVerification,quotaRecoveriesRequiringConfirmation,quotaBaselinesRequiringReview,retirementRepairs };
  // A checksum proves snapshot integrity, not that later spend/revocations did
  // not happen. Historical recovery is inspection-only until reconciled. The
  // bounded activation path proves the explicitly selected original authority
  // has not advanced since this offline snapshot, then permanently retires it.
  if(quiescentSource===undefined)return {...result,activated:false,reconciliationRequired:true};
  const source=client.db(quiescentSource),sourceToken=await gate(source);
  let activationAttempted=false;
  try{
    const writer=await source.collection('_ha_serial').findOne({_id:'writer',ddl:sourceToken,ownerId:null});
    assert.ok(writer&&writer.epoch===archivedWriter.epoch+1&&writer.revision===archivedWriter.revision,
      'Source authority changed after this backup; historical recovery requires reconciliation');
    activationAttempted=true;
    await activate(db,token);
    // Do not release the old authority, including on an uncertain activation
    // acknowledgement. Two independently active databases are unsafe.
    return {...result,activated:true,sourceRetired:quiescentSource};
  }finally{
    if(!activationAttempted)await activate(source,sourceToken);
  }
}

export async function importSqlite(client, uri, input, target, { rebuildMemory = false } = {}) {
  // Administrative reader only. Backup/restore must not load SQLite at all.
  const { DatabaseSync } = await import('node:sqlite');
  const file = localFile(input);
  assert.ok(statSync(file).isFile(), 'Expected a regular SQLite file');
  const checkpointed = () => { for (const suffix of ['-wal', '-journal']) assert.ok(!existsSync(file + suffix) || statSync(file + suffix).size === 0,
    'Source must be stopped and checkpointed; do not ignore WAL/journal data'); };
  checkpointed();
  const sourceHash = fileHash(file);
  const source = new DatabaseSync(file, { readOnly: true, allowExtension: false });
  const db = client.db(name(target));
  let adapter;
  try {
    source.exec('BEGIN'); // One read snapshot for schema, rows and sequences.
    assert.equal(source.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
    assert.equal(source.prepare('PRAGMA foreign_key_check').get(),undefined,'SQLite source has foreign-key violations; target must not be created');
    let objects = source.prepare("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY name").all();
    const replaced = [];
    const translatedTriggers=[];
    const detailTrigger=objects.find(object=>object.type==='trigger'&&object.name==='trg_rdl_ring_buffer');
    if(detailTrigger) {
      const sql=readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/006_detailed_request_logs.sql',import.meta.url),'utf8');
      assert.equal(digest().update(sql).digest('hex'),'532e2853e760a2318e46b3cd29c50897b6b83953c410072afbb84a9f9e0d63e8');
      const start=sql.indexOf('CREATE TRIGGER IF NOT EXISTS trg_rdl_ring_buffer'),end=sql.indexOf('END;',start);
      assert.ok(start>=0&&end>start,'Request-detail trigger source changed');
      assert.equal(normalizeSchemaSql(detailTrigger.sql),normalizeSchemaSql(sql.slice(start,end+4)),'Unrecognized request-detail retention trigger; explicit mapping required');
      translatedTriggers.push(detailTrigger.name);objects=objects.filter(object=>object!==detailTrigger);
    }
    const reasoningLinks={
      trg_reasoning_rules_api_key_delete:[['api_key_id','api_keys']],
      trg_reasoning_rules_combo_delete:[['combo_id','combos'],['target_combo_id','combos']],
      trg_reasoning_rules_connection_delete:[['connection_id','provider_connections']],
    };
    const reasoningTriggers=objects.filter(object=>object.type==='trigger'&&Object.hasOwn(reasoningLinks,object.name));
    if(reasoningTriggers.length) {
      const sql=readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/126_reasoning_routing_rules.sql',import.meta.url),'utf8');
      assert.equal(digest().update(sql).digest('hex'),'20e88a5637d7a807d01db67416f9b91536e984ae20c7b96ad53d9375c2e11b6d');
      const definitions=[...sql.matchAll(/CREATE TRIGGER IF NOT EXISTS (\w+)[\s\S]*?END;/g)];
      const table=objects.find(object=>object.type==='table'&&object.name==='reasoning_routing_rules');
      assert.ok(table,'Reasoning cleanup mapping requires its policy table');
      const spec=compileTable(parse(table.sql).ast);
      for(const trigger of reasoningTriggers) {
        const definition=definitions.find(match=>match[1]===trigger.name);
        assert.ok(definition,'Reasoning cleanup trigger anchor changed');
        assert.equal(normalizeSchemaSql(trigger.sql),normalizeSchemaSql(definition[0]),'Unrecognized reasoning cleanup trigger; explicit mapping required');
        for(const [column,parent] of reasoningLinks[trigger.name])
          assert.ok(spec.foreignKeys.some(key=>key.columns.length===1&&key.columns[0]===column&&key.table===parent&&key.target.length===1&&key.target[0]==='id'&&key.onDelete==='CASCADE'),
            'Reasoning cleanup mapping requires declared CASCADE: '+column);
        objects=objects.filter(object=>object!==trigger);translatedTriggers.push(trigger.name);
      }
    }
    const retirement=providerRetirementTriggers();
    const retirementObjects=objects.filter(object=>object.type==='trigger'&&retirement.has(object.name));
    if(retirementObjects.length) {
      for(const name of ['provider_connections','exclusive_connection_leases']) {
        const table=objects.find(object=>object.type==='table'&&object.name===name);
        assert.ok(table,'Provider retirement mapping requires table: '+name);
        assertProviderRetirementSchema(compileTable(parse(table.sql).ast),name);
      }
      for(const trigger of retirementObjects) {
        assert.equal(normalizeSchemaSql(trigger.sql),normalizeSchemaSql(retirement.get(trigger.name)),'Unrecognized provider retirement trigger; explicit mapping required');
        objects=objects.filter(object=>object!==trigger);translatedTriggers.push(trigger.name);
      }
    }
    const freeCleanup=objects.find(object=>object.type==='trigger'&&object.name==='trg_free_proxies_clear_pool_proxy_id');
    if(freeCleanup) {
      assert.equal(normalizeSchemaSql(freeCleanup.sql),normalizeSchemaSql(freeProxyTrigger),'Unrecognized Free Proxy cleanup trigger; explicit mapping required');
      const registry=objects.find(object=>object.type==='table'&&object.name==='proxy_registry'),free=objects.find(object=>object.type==='table'&&object.name==='free_proxies');
      assert.ok(registry&&free,'Free Proxy cleanup mapping requires both tables');
      assertFreeProxyCleanupSchema(compileTable(parse(registry.sql).ast),compileTable(parse(free.sql).ast));
      objects=objects.filter(object=>object!==freeCleanup);translatedTriggers.push(freeCleanup.name);
    }
    if (rebuildMemory) {
      // Only the exact final pinned FTS definition/triggers are replaceable.
      // Unknown triggers, older variants and extensions still fail preflight.
      const migration = readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/203_memory_fts_content_rowid.sql', import.meta.url), 'utf8');
      assert.equal(digest().update(migration).digest('hex'), '5540381c6ef763c95e418cc04bac28c9b22b12bccb901c6368f354b39ceb56f5');
      const normalize = normalizeSchemaSql;
      const definitions = [migration.match(/CREATE VIRTUAL TABLE memory_fts[\s\S]*?;/)?.[0], ...migration.matchAll(/CREATE TRIGGER [\s\S]*?END;/g)].map(m => typeof m === 'string' ? m : m[0]);
      const allowed = new Set(definitions.filter(Boolean).map(normalize));
      const virtual = objects.find(o => o.name === 'memory_fts');
      if (virtual) {
        assert.ok(allowed.has(normalize(virtual.sql)), 'Unrecognized Memory FTS definition');
        const shadows = new Set(source.prepare('PRAGMA table_list').all().filter(t => t.schema === 'main' && t.type === 'shadow' && t.name.startsWith('memory_fts_')).map(t => t.name));
        objects = objects.filter(o => {
          if (o.name === 'memory_fts' || shadows.has(o.name) || o.type === 'trigger' && allowed.has(normalize(o.sql))) { replaced.push(o.name); return false; }
          return true;
        });
      }
    }
    const indexMapping=mapSqliteIndexes(objects);objects=indexMapping.objects;
    assert.ok(objects.every(o => o.type === 'table' || o.type === 'index'), 'Virtual tables, triggers and views need an explicit migration mapping; source retained unchanged');
    const tables = objects.filter(o => o.type === 'table').map(o => ({ ...o, spec: compileTable(parse(o.sql).ast) }));
    const reasoningTable=tables.find(table=>table.name==='reasoning_cache');
    if(reasoningTable){
      const ddl=readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/033_create_reasoning_cache.sql',import.meta.url),'utf8');
      assert.equal(digest().update(ddl).digest('hex'),'18f5668679d0673c0b40eeac8b101de9f90de8778412a9ed279eaebf4eb9bd06','Reasoning migration source changed');
      assert.equal(normalizeSchemaSql(reasoningTable.sql),normalizeSchemaSql(ddl.match(/CREATE TABLE[\s\S]*?;/)?.[0]),'Legacy reasoning expiry mapping requires the exact original table');
      assert.equal(source.prepare(`SELECT 1 AS outside FROM reasoning_cache WHERE (${reasoningExpirySql}) > ? OR (${reasoningExpirySql}) < ? LIMIT 1`).get(Number.MAX_SAFE_INTEGER,-Number.MAX_SAFE_INTEGER),undefined,'Reasoning expiry exceeds the supported safe numeric profile');
    }
    // Validate every DDL statement before creating the target. Unsupported SQL
    // remains a compatibility failure, never silently skipped.
    for (const o of objects) parse(o.sql);
    const token = await gate(db, true);
    adapter = new MongoAdapter({ uri, database: target, maintenanceToken: token, timeoutMs: 15000 });
    for (const t of tables) adapter.exec(t.sql);
    const pending = new Map(tables.map(t => [t.name, t]));
    const counts = {};let normalizedReasoningExpiries=0;
    while (pending.size) {
      const table = [...pending.values()].find(t => t.spec.foreignKeys.every(f => f.table === t.name || !pending.has(f.table)));
      assert.ok(table, 'Cyclic foreign keys require an explicit migration mapping');
      const columns = table.spec.columns.map(c => c.name);
      const quote = value => '"' + value.replaceAll('"', '""') + '"';
      const hasRowid = !table.spec.withoutRowid;
      const rowid = ['rowid', '_rowid_', 'oid'].find(alias => !columns.some(c => c.toLowerCase() === alias));
      assert.ok(!hasRowid || rowid, 'All SQLite rowid aliases are shadowed; explicit migration mapping required');
      assert.ok(!columns.includes('__source_rowid'), 'Reserved migration column');
      const order = hasRowid ? rowid : table.spec.primaryKey.map(quote).join(',');
      // Evaluate legacy expiry through the real SQLite source expression while
      // its read transaction is open. Preserve normal numeric storage; normalize
      // text/blob dates to the exact epoch used by upstream eligibility checks.
      const reasoning=table===reasoningTable;
      assert.ok(!reasoning||!columns.includes('__ha_reasoning_normalized'),'Reserved reasoning migration column');
      const projection=columns.map(column=>reasoning&&column==='expires_at'
        ? `CASE WHEN typeof(expires_at) IN ('integer','real') THEN expires_at ELSE ${reasoningExpirySql} END AS expires_at`:quote(column)).join(',');
      const sql = `SELECT ${hasRowid ? rowid + ' AS __source_rowid,' : ''}${projection}${reasoning?", typeof(expires_at) NOT IN ('integer','real') AS __ha_reasoning_normalized":''} FROM ${quote(table.name)} ORDER BY ${order}`;
      const read = source.prepare(sql); read.setReadBigInts(true);
      const insert = adapter.prepare(`INSERT INTO ${quote(table.name)}(${columns.map(quote).join(',')}) VALUES (${columns.map(() => '?').join(',')})`);
      const expected = digest(); let count = 0;
      for (const raw of read.iterate()) {
        if(reasoning&&raw.__ha_reasoning_normalized)normalizedReasoningExpiries++;
        const safe = value => { if (typeof value === 'bigint') { const n = Number(value); assert.ok(Number.isSafeInteger(n), '64-bit value exceeds the adapter ABI'); return n; } return value instanceof Uint8Array ? Buffer.from(value) : value; };
        const values = columns.map(c => safe(raw[c]));
        const result = insert.run(...values);
        if (hasRowid) await db.collection(table.name).updateOne({ _ha_rowid: result.lastInsertRowid }, { $set: { _ha_rowid: safe(raw.__source_rowid) } }, { writeConcern: wc });
        expected.update(BSON.serialize({ values })); count++;
      }
      const actual = digest();
      const sorted = hasRowid ? { _ha_rowid: 1 } : Object.fromEntries(table.spec.primaryKey.map(c => [c, 1]));
      for await (const row of db.collection(table.name).find({}, { promoteBuffers: true }).sort(sorted)) actual.update(BSON.serialize({ values: columns.map(c => row[c]) }));
      assert.equal(count, await db.collection(table.name).countDocuments());
      assert.equal(expected.digest('hex'), actual.digest('hex'), 'Imported content mismatch: ' + table.name);
      counts[table.name] = count; pending.delete(table.name);
    }
    for (const index of objects.filter(o => o.type === 'index')) adapter.exec(index.sql);
    if (source.prepare("SELECT name FROM sqlite_master WHERE name='sqlite_sequence'").get()) {
      for (const row of source.prepare('SELECT name,seq FROM sqlite_sequence').all()) {
        assert.ok(Number.isSafeInteger(row.seq));
        await db.collection('_ha_sequences').updateOne({ _id: row.name }, { $set: { value: row.seq } }, { upsert: true, writeConcern: wc });
      }
    }
    if (replaced.length) {
      adapter.installMemoryTextIndex();
      const fields = tables.find(t => t.name === 'memories')?.spec.columns.map(c => c.name) ?? [];
      if (fields.includes('needs_reindex')) adapter.prepare('UPDATE memories SET needs_reindex=1').run();
      // FTS is rebuilt immediately from authoritative content. Vectors require
      // the configured embedding provider and are never copied with stale IDs.
      if (tables.some(t => t.name === 'memory_vec_meta')) adapter.prepare('UPDATE memory_vec_meta SET vec_loaded=0').run();
    }
    const addedRuntimeSchemas=provisionAuditSchema(adapter)?['audit_log']:[];
    if(provisionArtifactSchema(adapter))addedRuntimeSchemas.push('ha_call_log_artifacts');
    adapter.verifyBaseSchema({runtime:true}); // Include HA artifact constraints before activation.
    // Imported SQL has no HA-private collections. Provision their indexes under
    // this fresh target's maintenance fence before releasing it for application use.
    adapter.installRequestLimits();
    if(translatedTriggers.includes('trg_rdl_ring_buffer'))adapter.installRequestDetailRetention();
    if(freeCleanup)adapter.installFreeProxyCleanup();
    checkpointed();
    assert.equal(fileHash(file), sourceHash, 'Migration source changed; target remains fenced');
    const credentialGroupsRequiringVerification=await fenceCopiedCredentials(db,'sqlite-import',wc);
    const quotaRecoveriesRequiringConfirmation=await fenceCopiedRecoveries(db,token);
    const quotaBaselinesRequiringReview=await fenceCopiedQuota(db,token,true);
    await activate(db, token);
    return { database: target, counts, addedRuntimeSchemas,replacedDerivedObjects: replaced,translatedTriggers,translatedIndexes:indexMapping.translated,normalizedReasoningExpiries, sourceSha256: sourceHash, sourceUnchanged: true,credentialGroupsRequiringVerification,quotaRecoveriesRequiringConfirmation,quotaBaselinesRequiringReview };
  } finally { await adapter?.close(); source.close(); }
}
