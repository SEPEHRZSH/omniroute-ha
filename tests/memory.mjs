import assert from 'node:assert/strict';
import { readFileSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { testDatabase } from './mongoTestSupport.mjs';
import { provisionDomains } from './provisionDomains.mjs';
import { MongoMemoryVectors } from '../adapter/memoryVectors.mjs';
import {memoryDecaySchedule,runMemoryDecay} from '../adapter/memoryDecay.mjs';

const test = await testDatabase('memory');
const dataDir = path.resolve('.work/memory-data'); mkdirSync(dataDir, { recursive: true });
Object.assign(process.env, {
  NODE_ENV: 'test', DATA_DIR: dataDir, DISABLE_SQLITE_AUTO_BACKUP: '1',
  OMNIROUTE_DISABLE_BACKGROUND_SERVICES: '1', OMNIROUTE_DISABLE_REDIS_AUTH_CACHE: '1',
  VECTOR_STORE_DISABLE_VEC: 'true', REDIS_URL: '', QDRANT_HOST: '',
  STORAGE_ENCRYPTION_KEY: 'synthetic-storage-key-for-ha-tests-only',
  API_KEY_SECRET: 'synthetic-api-key-secret-for-ha-tests-only', JWT_SECRET: 'synthetic-jwt-secret-for-ha-tests-only',
  MEMORY_TYPED_DECAY_ENABLED:'false',MEMORY_TYPED_DECAY_SWEEP_INTERVAL:'',
});
const db = new MongoAdapter({ uri: test.uri, database: test.database });
const peer = new MongoAdapter({ uri: test.uri, database: test.database });
globalThis.__omnirouteMongoAdapter = db;
let decayActive,decayStandby;
try {
  await provisionDomains(db);
  db.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/015_create_memories.sql', 'utf8'));
  db.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/111_memory_typed_decay.sql', 'utf8'));
  const settings = await import('../upstream/omniroute/src/lib/db/settings.ts');
  await settings.updateSettings({ memoryEmbeddingSource: 'remote', memoryEmbeddingProviderModel: null });
  const store = await import('../upstream/omniroute/src/lib/memory/store.ts');
  const { sqliteBackend: backend } = await import('../upstream/omniroute/src/lib/memory/sqliteBackend.ts');
  // The upstream SQLiteBackend name is retained, while its CRUD uses MongoDB.
  // Vector and text-search behavior is checked after these basic operations.
  const input = { apiKeyId: 'tenant-a', sessionId: 'session-1', type: 'factual', key: 'preference', content: 'Likes COFFEE', metadata: { category: 'drink' }, expiresAt: null };
  const first = await backend.create(input);
  assert.equal((await backend.get(first.id)).content, input.content);
  const replaced = await backend.create({ ...input, content: 'Likes tea' });
  assert.equal(replaced.id, first.id);
  assert.equal((await backend.get(first.id)).content, 'Likes tea');
  await backend.create({ ...input, apiKeyId: 'tenant-b', content: 'Other tenant coffee' });
  await backend.create({ ...input, key: 'second', type: 'episodic', content: 'COFFEE yesterday' });
  const all = await backend.list({ apiKeyId: 'tenant-a', limit: 1 });
  assert.equal(all.total, 2); assert.equal(all.data.length, 1);
  assert.deepEqual(all.byType, { factual: 1, episodic: 1 });
  const filtered = await backend.list({ apiKeyId: 'tenant-a', query: 'coffee', limit: 10 });
  assert.equal(filtered.total, 1); assert.equal(filtered.data[0].type, 'episodic');
  const emptyPage = await backend.list({ apiKeyId: 'tenant-a', limit: 10, offset: 10 });
  assert.equal(emptyPage.data.length, 0); assert.equal(emptyPage.total, 2);
  // Warm the upstream cache, then change MongoDB through a separate connection.
  await backend.get(first.id);
  peer.prepare('UPDATE memories SET content=? WHERE id=?').run('External update', first.id);
  assert.equal((await backend.get(first.id)).content, 'External update');
  store.recordMemoryAccess([first.id, first.id]);
  assert.equal((await backend.get(first.id)).accessCount, 1);
  assert.equal(await backend.update(first.id, { content: 'Changed again' }), true);
  assert.equal(await backend.delete(first.id), true);
  assert.equal(await backend.get(first.id), null);
  // Drain the real store's asynchronous embedding callbacks before asserting faults.
  await new Promise(resolve => setImmediate(resolve));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(db.compatibilityFault, undefined);
  assert.equal((await backend.health()).ok, true);
  assert.equal(await test.db.collection('memories').countDocuments({ api_key_id: 'tenant-a' }), 1);
  db.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/083_memory_vec.sql', 'utf8'));
  process.env.VECTOR_STORE_DISABLE_VEC = 'false';
  globalThis.__omnirouteMongoVectors = new MongoMemoryVectors(db);
  const { getVectorStore } = await import('../upstream/omniroute/src/lib/memory/vectorStore.ts');
  const vectors = getVectorStore();
  assert.equal((await vectors.stats()).rowCount, 0);
  assert.equal((await vectors.ensureReady({ signature: 'synthetic-embedding-3', dimensions: 3 })).ready, true);
  const points = [];
  for (const [key, apiKeyId, expiresAt, vector] of [
    ['vec-a', 'tenant-a', null, [1, 0, 0]],
    ['vec-b', 'tenant-a', null, [0, 1, 0]],
    ['vec-other', 'tenant-b', null, [1, 0, 0]],
    ['vec-expired', 'tenant-a', new Date(0), [1, 0, 0]],
  ]) {
    const memory = await backend.create({ ...input, key, apiKeyId, expiresAt });
    points.push(memory.id);
    assert.equal(await vectors.upsertVector(memory.id, new Float32Array(vector), { content: input.content, signature: 'synthetic-embedding-3' }), true);
  }
  db.installMemoryTextIndex();
  const vectorPeer = new MongoMemoryVectors(peer);
  const hits = await vectorPeer.searchVector(new Float32Array([1, 0, 0]), 10, 'tenant-a');
  assert.deepEqual(hits.map(hit => hit.memoryId), points.slice(0, 2));
  assert.equal(hits[0].distance, 0);
  assert.ok(Math.abs(hits[1].distance - Math.sqrt(2)) < 1e-6);
  const expected = { content: input.content, signature: 'synthetic-embedding-3' };
  await assert.rejects(vectors.upsertVector(points[0], new Float32Array([1, 2]), expected), /dimension/);
  assert.throws(() => db.transaction(() => { vectors.call('reset', { signature: 'rolled-back', dim: 2 }); throw new Error('rollback vectors'); })(), /rollback vectors/);
  assert.equal((await vectorPeer.stats()).activeDim, 3);
  assert.equal((await vectorPeer.stats()).rowCount, 4);
  const { retrieveMemories } = await import('../upstream/omniroute/src/lib/memory/retrieval.ts');
  const retrieved = await retrieveMemories('tenant-a', { retrievalStrategy: 'exact', query: 'coffee', scope: 'session', sessionId: 'session-1', maxTokens: 1000 });
  assert.ok(retrieved.some(memory => memory.id === points[0]));
  assert.ok(retrieved.every(memory => memory.apiKeyId === 'tenant-a' && memory.sessionId === 'session-1'));
  assert.ok(!retrieved.some(memory => memory.id === points[3]), 'expired memories must not reach prompt retrieval');
  const otherSession = await retrieveMemories('tenant-a', { retrievalStrategy: 'exact', scope: 'session', sessionId: 'absent-session' });
  assert.deepEqual(otherSession, []);
  const lexDoc = await backend.create({ ...input, key: 'native search', content: 'Blue comet', sessionId: 'lex' });
  const partial = await backend.create({ ...input, key: 'partial', content: 'Blue planet', sessionId: 'lex' });
  const lexical = vectorPeer.lexical('"blue" "comet"', 10, 'tenant-a', { sessionId: 'lex' });
  assert.deepEqual(lexical.map(row => row.id), [lexDoc.id], 'all sanitized terms must match');
  const hybrid = await vectors.searchHybrid(new Float32Array([1, 0, 0]), 'Blue comet', 10, 'tenant-a');
  assert.equal(hybrid.find(h => h.memoryId === lexDoc.id).ftsRank, 1);
  assert.equal(hybrid.find(h => h.memoryId === points[0]).vecRank, 1);
  assert.ok(!hybrid.some(h => h.memoryId === partial.id || h.memoryId === points[3]));
  const { retrievePreview, keywordEngineStatus } = await import('../upstream/omniroute/src/lib/memory/retrieval.ts');
  assert.equal(keywordEngineStatus(db).backend, 'MongoDB text');
  const preview = await retrievePreview('tenant-a', 'Blue comet', { strategy: 'semantic', maxTokens: 100, limit: 10 });
  assert.deepEqual(preview.items.map(item => [item.memory.id, item.tier]), [[lexDoc.id, 'mongodb-text']]);
  await backend.update(lexDoc.id, { content: 'no matching terms' });
  assert.equal(vectorPeer.lexical('"comet"', 10, 'tenant-a').length, 0);
  // An asynchronous embed started before an edit must not overwrite the new
  // content's vector, and the old vector must stop being searchable immediately.
  await backend.update(points[1], { content: 'A newer memory' });
  assert.equal((await vectorPeer.stats()).rowCount, 3);
  assert.equal(await vectors.upsertVector(points[1], new Float32Array([0, 1, 0]), expected), false);
  assert.equal((await backend.get(points[1])).content, 'A newer memory');
  assert.equal(await vectors.upsertVector(points[1], new Float32Array([0, 0, 1]), { ...expected, content: 'A newer memory' }), true);
  await backend.delete(points[0]);
  assert.equal((await vectorPeer.stats()).rowCount, 3);
  await vectors.resetForSignature('synthetic-embedding-2', 2);
  assert.equal((await vectorPeer.stats()).rowCount, 0);
  assert.equal((await vectorPeer.stats()).activeDim, 2);
  assert.equal(await vectors.upsertVector(points[1], new Float32Array([0, 0, 1]), { ...expected, content: 'A newer memory' }), false);
  assert.ok((await vectorPeer.stats()).needsReindex > 0);
  await new Promise(resolve => setImmediate(resolve));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(db.compatibilityFault, undefined);
  const { getMemorySettings } = await import('../upstream/omniroute/src/lib/memory/settings.ts');
  const { resolveEmbeddingSource, withMeasuredDimensions } = await import('../upstream/omniroute/src/lib/memory/embedding/index.ts');
  await settings.updateSettings({ memoryEmbeddingProviderModel: 'openai/text-embedding-3-small' });
  const model = resolveEmbeddingSource(await getMemorySettings());
  assert.equal((await vectors.ensureReady(model)).ready, true);
  const beforeRevisionChange = await vectors.stats();
  await settings.updateSettings({ memoryEmbeddingProviderModel: 'openai/text-embedding-3-large' });
  assert.equal((await vectors.ensureReady(model)).ready, false, 'an older embedding task must not revert the current model');
  assert.equal((await vectors.stats()).signature, beforeRevisionChange.signature);
  const measured = withMeasuredDimensions({ ...model, dimensions: null }, 3);
  assert.notEqual(measured.signature, { ...model, dimensions: null }.signature);
  await settings.updateSettings({memoryEmbeddingProviderModel:null,memoryEmbeddingCustomBaseUrl:'https://synthetic.invalid/v1',memoryEmbeddingCustomModelId:'synthetic-before'});
  const lazy=resolveEmbeddingSource(await getMemorySettings());assert.equal(lazy.dimensions,null);
  const measuredLazy=withMeasuredDimensions(lazy,3),readyLazy=await vectors.ensureReady(measuredLazy);assert.equal(readyLazy.ready,true);
  assert.equal(await vectors.upsertVector(points[1],new Float32Array([1,0,0]),{content:'A newer memory',signature:measuredLazy.signature}),true);
  assert.equal((await vectors.searchVector(new Float32Array([1,0,0]),10,'tenant-a',readyLazy.queryContext))[0].memoryId,points[1]);
  const sameLazy=await vectors.ensureReady(lazy);assert.equal(sameLazy.ready,true);
  assert.equal((await vectors.searchVector(new Float32Array([1,0,0]),10,'tenant-a',sameLazy.queryContext))[0].memoryId,points[1]);
  await settings.updateSettings({memoryEmbeddingCustomModelId:'synthetic-after'});
  await assert.rejects(vectors.searchVector(new Float32Array([1,0,0]),10,'tenant-a',readyLazy.queryContext),{code:'HA_MEMORY_CONFIGURATION_CHANGED'});
  await assert.rejects(vectors.searchHybrid(new Float32Array([1,0,0]),'A newer',10,'tenant-a',readyLazy.queryContext),{code:'HA_MEMORY_CONFIGURATION_CHANGED'});
  assert.equal((await vectors.ensureReady(resolveEmbeddingSource(await getMemorySettings()))).ready,false,'Unknown dimensions for a different model must not report old vectors ready');
  const readyAfter=await vectors.ensureReady(withMeasuredDimensions(resolveEmbeddingSource(await getMemorySettings()),3));assert.equal(readyAfter.ready,true);
  assert.equal((await vectors.searchVector(new Float32Array([1,0,0]),10,'tenant-a',readyAfter.queryContext)).length,0,'A new model must not reuse old vectors with the same dimensions');
  assert.equal(await vectors.upsertVector(points[1],new Float32Array([1,0,0]),{content:'A newer memory',signature:measuredLazy.signature}),false);
  console.log('PASS Memory query model binding: lazy dimensions preserve only the same model; stale vector/hybrid query contexts and late old-model upserts are refused across configuration changes');
  await settings.updateSettings({memoryEmbeddingProviderModel:null,memoryEmbeddingCustomBaseUrl:null,memoryEmbeddingCustomModelId:null});
  const schedule=await memoryDecaySchedule({MEMORY_TYPED_DECAY_ENABLED:'true',MEMORY_TYPED_DECAY_SWEEP_INTERVAL:'60'});
  assert.equal(schedule.interval,60000);
  assert.equal((await memoryDecaySchedule({})).interval,0);
  await assert.rejects(memoryDecaySchedule({MEMORY_TYPED_DECAY_ENABLED:'true',MEMORY_TYPED_DECAY_SWEEP_INTERVAL:'999999999'}),/timer range/);
  const old=new Date(Date.now()-31*86400000).toISOString(),decayRows={};
  for(const [key,type,count,last,tenant] of [['expired','episodic',0,null,'decay-a'],['factual','factual',0,null,'decay-a'],['access-immune','episodic',3,null,'decay-a'],['recent','episodic',0,new Date().toISOString(),'decay-a'],['other','episodic',0,null,'decay-b']]) {
    const memory=await backend.create({...input,apiKeyId:tenant,key:'synthetic-decay-'+key,type,content:'Synthetic decayed memory '+key});decayRows[key]=memory.id;
    db.prepare('UPDATE memories SET created_at=?,access_count=?,last_accessed_at=? WHERE id=?').run(old,count,last,memory.id);
  }
  const decayVectors=new MongoMemoryVectors(db);await decayVectors.resetForSignature('synthetic-decay-vectors',2);
  assert.equal(await decayVectors.upsertVector(decayRows.expired,new Float32Array([1,0]),{content:'Synthetic decayed memory expired',signature:'synthetic-decay-vectors'}),true);
  const candidates=store.listMemoriesForDecay({apiKeyId:'decay-a',limit:10});assert.ok(candidates.find(row=>row.id===decayRows.expired));
  store.recordMemoryAccess([decayRows.expired]);
  assert.equal(await decayVectors.deleteDecayedMemory(decayRows.expired,schedule.config,db.serverTime()),false,'A newly accessed memory must survive a stale decay candidate');
  assert.equal((await decayVectors.stats()).rowCount,1);
  db.prepare('UPDATE memories SET access_count=0,last_accessed_at=NULL WHERE id=?').run(decayRows.expired);
  assert.throws(()=>db.transaction(()=>{decayVectors.call('deleteDecayed',{memoryId:decayRows.expired,config:schedule.config,now:db.serverTime()});throw new Error('synthetic decay rollback');})(),/synthetic decay rollback/);
  assert.equal((await decayVectors.stats()).rowCount,1);assert.ok(await backend.get(decayRows.expired));
  decayActive=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-decay-active'});
  decayStandby=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-decay-standby'});assert.ok(decayActive.ownership('claim',60000));
  const runtime=adapter=>({db:adapter,assertOwner(){if(!adapter.canWrite())throw Object.assign(new Error('Memory decay requires the active owner'),{code:'HA_FENCED'});}});
  globalThis.__omnirouteMongoAdapter=decayActive;
  await settings.updateSettings({qdrantEnabled:true});
  await assert.rejects(new MongoMemoryVectors(decayActive).deleteDecayedMemory(decayRows.expired,schedule.config,decayActive.serverTime()),{code:'HA_MEMORY_DECAY_PROFILE'});
  assert.ok(await backend.get(decayRows.expired));await settings.updateSettings({qdrantEnabled:false});
  let decayNotifications=0;decayActive.onCommitted=tables=>{if(tables.includes('memories'))decayNotifications++;};
  assert.throws(()=>decayActive.transaction(()=>{new MongoMemoryVectors(decayActive).call('deleteDecayed',{memoryId:decayRows.expired,config:schedule.config,now:decayActive.serverTime()});throw new Error('synthetic owned rollback');})(),/synthetic owned rollback/);
  assert.equal(decayNotifications,0,'Aborted decay cannot publish cache invalidation');
  const decayPreview=await runMemoryDecay(runtime(decayActive),{config:schedule.config,dryRun:true,apiKeyId:'decay-a'});assert.equal(decayPreview.decayed,1);assert.equal(decayPreview.deletedIds.length,0);
  assert.equal((await runMemoryDecay(runtime(decayActive),{apiKeyId:'decay-a'})).skippedDisabled,true);
  const swept=await runMemoryDecay(runtime(decayActive),{config:schedule.config,apiKeyId:'decay-a'});assert.deepEqual(swept.deletedIds,[decayRows.expired]);
  assert.equal(decayNotifications,1,'Successful committed decay publishes one invalidation');
  for(const name of ['factual','access-immune','recent','other'])assert.ok(await backend.get(decayRows[name]));
  assert.equal((await decayVectors.stats()).rowCount,0);assert.equal(await backend.get(decayRows.expired),null);
  await assert.rejects(runMemoryDecay(runtime(decayStandby),{config:schedule.config}),{code:'HA_FENCED'});
  decayActive.ownership('release');assert.ok(decayStandby.ownership('claim',60000));globalThis.__omnirouteMongoAdapter=decayStandby;
  await assert.rejects(new MongoMemoryVectors(decayActive).deleteDecayedMemory(decayRows.other,schedule.config,decayStandby.serverTime()),{code:'HA_FENCED'});
  assert.deepEqual((await runMemoryDecay(runtime(decayStandby),{config:schedule.config,apiKeyId:'decay-b'})).deletedIds,[decayRows.other]);
  decayStandby.ownership('release');
  console.log('PASS owned Memory decay: original TTL/type/access predicates and candidate selection, opt-in/dry-run/tenant scope, current-access recheck, atomic vector rollback/deletion, standby/stale fencing and promoted continuation');
  console.log('PASS Memory decay safety: configured external Qdrant is refused without network/deletion; cache invalidation fires only after successful commit');
  console.log('PASS real OmniRoute Memory CRUD: upsert, tenant filters, pagination, grouped counts, access telemetry and cross-connection cache freshness');
  console.log('PASS native Memory vectors through upstream VectorStore: persistent exact L2, tenant/expiry isolation, dimension checks, transactional reset/rollback and reindex state');
  console.log('PASS MongoDB lexical retrieval, real upstream preview, all-term matching, immediate text index updates and RRF fusion');
  console.log('PASS Memory configuration race: stale task cannot reset the vector store after authoritative settings changed');
  console.log('PASS Memory embedding races: content edits invalidate vectors atomically; stale content/model results cannot overwrite current state');
  console.log('PASS actual Memory exact retrieval: schema discovery, query ranking, session and tenant boundaries, expiry and access telemetry');
} finally { await decayActive?.close();await decayStandby?.close();await db.close(); await peer.close(); delete globalThis.__omnirouteMongoAdapter; delete globalThis.__omnirouteMongoVectors; await test.cleanup(); }
