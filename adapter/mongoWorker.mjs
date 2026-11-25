import { randomUUID,randomBytes } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { parse, expression, predicate, asciiLower, noCaseKey, unsupported } from './sql.mjs';
import { checkIdentifier, literal, currentTimestamp, onlyKeys, columnDefinition, compileTable, compileIndex, tableInfo,assertStoredSchema } from './schema.mjs';
import { readBaseSchema, checkBaseSchema,artifactSchemaSql } from './baseSchema.mjs';
import { joinPipeline } from './join.mjs';
import { admitRequest, checkQuotaPolicy } from './requestLimits.mjs';
import { textIndex } from './memoryText.mjs';
import { executeMemoryVector } from './memoryVectorWorker.mjs';
import { executeConversationCandidates } from './conversationQuery.mjs';
import { executeLatestQuota } from './quotaQuery.mjs';
import { executeUsageQuery } from './usageQueries.mjs';
import { executeProviderQuery,executeProviderIdentity,executeProviderReconciliation } from './providerQueries.mjs';
import {deleteDecayedMemory} from './memoryDecay.mjs';
import {executeRetentionQuery,retentionDayMatch,assertRetentionMarkerSchema} from './retentionQueries.mjs';
import {archiveDay,recordArchivePrices,archiveGlobalUpsert,finishUsageArchive} from './usageArchive.mjs';
import { executeGamificationQuery } from './gamificationQueries.mjs';
import { reservationOperation } from './reservations.mjs';
import { credentialOperation,credentialVersion,protectCredentialWrite,recordCredentialWrite } from './credentialState.mjs';
import {protectProviderIdentity,applyProviderRetirement,verifyProviderRetirement} from './providerRetirement.mjs';
import {freeProxyCleanupSql,assertFreeProxyCleanupSchema} from './freeProxyQueries.mjs';

const transactionOptions = { readConcern: { level: 'snapshot' }, writeConcern: { w: 'majority', j: true, wtimeoutMS: 5000 }, readPreference: 'primary', maxCommitTimeMS: 5000 };
function error(code, message) { return Object.assign(new Error(message), { code }); }

export async function createHandler(config) {
  if (!config.uri || !/^ha_test_[a-z0-9_]+$/.test(config.database)) {
    // Keep production disabled until the outstanding release gates are validated.
    throw error('HA_TEST_ONLY', 'This vertical slice requires an explicitly named ha_test_* database');
  }
  if(config.maintenanceToken!==undefined&&(!/^[a-f0-9-]{36}$/.test(config.maintenanceToken)||config.ownerId)||
      config.maintenanceEpoch!==undefined&&(!config.maintenanceToken||!Number.isSafeInteger(config.maintenanceEpoch)||config.maintenanceEpoch<0))
    throw error('HA_INVALID_MAINTENANCE','Offline maintenance requires a private gate token and exact nonnegative epoch, without an app owner');
  const client = new MongoClient(config.uri, {
    ...(config.testProxyPort ? { proxyHost: '127.0.0.1', proxyPort: config.testProxyPort } : {}),
    serverSelectionTimeoutMS: 3000, connectTimeoutMS: 3000, socketTimeoutMS: 7000,
    retryWrites: false, readPreference: 'primary', readConcern: { level: 'majority' },
    maxPoolSize: 2, minPoolSize: 0, maxConnecting: 1,
    writeConcern: { w: 'majority', j: true, wtimeoutMS: 5000 }, promoteBuffers: true,
  });
  await client.connect();
  const db = client.db(config.database);
  const hello = await db.admin().command({ hello: 1 });
  if (!hello.setName) throw error('HA_REPLICA_SET_REQUIRED', 'MongoDB replica set is required');
  const initialized=await db.collection('_ha_serial').findOne({_id:'writer'},{projection:{_id:1},maxTimeMS:3000});
  if(!initialized) {
    if(config.ownerId) throw error('HA_SCHEMA_MISMATCH','Provision the HA writer record offline before application startup');
    await db.collection('_ha_serial').updateOne({ _id: 'writer' }, { $setOnInsert: {
      revision: 0, ownerId: null, epoch: 0, leaseUntil: new Date(0),
    } }, { upsert: true });
  }
  let session;
  let aborted = false;
  let lastInsertRowid = 0;
  let ownershipEpoch;
  const usageRollups=new Map();
  const ownerId = config.ownerId;
  async function rollupPreview(id,before) {
    const preview=usageRollups.get(id),current=await db.collection('_ha_serial').findOne({_id:'writer'},{session});
    if(!session||!preview||before!==undefined&&preview.before!==before||current.revision!==preview.revision+1||current.epoch!==preview.epoch||current.ownerId!==preview.ownerId)throw error('HA_ROLLUP_STALE','Usage or policy changed while pricing; no rollup writes may commit');
    return preview;
  }

  async function ownership(action, leaseMs = 5000) {
    if (!ownerId || typeof ownerId !== 'string') throw error('HA_OWNER_REQUIRED', 'A process-specific owner ID is required');
    if (session) throw error('HA_TRANSACTION_ACTIVE', 'Ownership operations cannot run inside application transactions');
    if (!Number.isSafeInteger(leaseMs) || leaseMs < 250 || leaseMs > 60000) throw error('HA_INVALID_LEASE', 'Lease must be 250..60000 ms');
    const owners = db.collection('_ha_serial');
    const matches = { _id: 'writer', ownerId, epoch: ownershipEpoch, $expr: { $gt: ['$leaseUntil', '$$NOW'] } };
    if (action === 'status') {
      // Redis/cache state is never consulted for authoritative ownership.
      return !!await owners.findOne(matches, { readConcern: { level: 'linearizable' }, maxTimeMS: 3000 });
    }
    if (action === 'claim') {
      const claim = await owners.findOneAndUpdate({ _id: 'writer', ddl: null, $expr: { $lte: ['$leaseUntil', '$$NOW'] } }, [{ $set: {
        ownerId: { $literal: ownerId }, epoch: { $add: ['$epoch', 1] }, leaseUntil: { $add: ['$$NOW', leaseMs] },
      } }], { returnDocument: 'after', maxTimeMS: 3000 });
      if (!claim) return null;
      ownershipEpoch = claim.epoch;
      return { epoch: ownershipEpoch, leaseUntil: claim.leaseUntil };
    }
    if (action === 'renew') {
      const renewed = await owners.findOneAndUpdate(matches, [{ $set: { leaseUntil: { $add: ['$$NOW', leaseMs] } } }], { returnDocument: 'after', maxTimeMS: 3000 });
      if (!renewed) throw error('HA_FENCED', 'Active ownership has expired or changed');
      return { epoch: renewed.epoch, leaseUntil: renewed.leaseUntil };
    }
    if (action === 'release') {
      const released = await owners.updateOne(matches, [{ $set: { ownerId: null, leaseUntil: '$$NOW' } }]);
      ownershipEpoch = undefined;
      return released.matchedCount === 1;
    }
    unsupported('ownership operation');
  }

  function writeFence() {
    return ownerId?{_id:'writer',ownerId,epoch:ownershipEpoch,$expr:{$gt:['$leaseUntil','$$NOW']}}:
      {_id:'writer',epoch:config.maintenanceToken?(config.maintenanceEpoch??0):0,ownerId:null,ddl:config.maintenanceToken??null,
        ...(config.maintenanceToken?{$expr:{$lte:['$leaseUntil','$$NOW']}}:{})};
  }
  async function begin(options = transactionOptions) {
    if (session) throw error('SQLITE_ERROR', 'Transaction already active');
    aborted = false;
    session = client.startSession();
    session.startTransaction(options);
    try {
      // Serialize all writers before snapshot reads. A concurrent writer gets a real
      // MongoDB write conflict, never a silently stale read-modify-write result.
      const fenced = await db.collection('_ha_serial').updateOne(writeFence(), { $inc: { revision: 1 } }, { session });
      if (fenced.matchedCount !== 1) throw error('HA_FENCED', 'This process does not own the active writer epoch');
    } catch (cause) { await session.endSession(); session = undefined; throw cause; }
  }
  async function end(commit) {
    if (!session) {
      if (!commit && aborted) { aborted = false; return; }
      throw error('SQLITE_ERROR', 'No active transaction');
    }
    const ending = session;
    let commitSent = false;
    let failure;
    try {
      if (commit) {
        // Recheck server time at commit as well: a suspended process must not
        // commit a transaction whose ownership lease expired while it was paused.
        if (ownerId||config.maintenanceToken) {
          const current = await db.collection('_ha_serial').updateOne(writeFence(), { $inc: { revision: 1 } }, { session: ending });
          if (!current.matchedCount) {
            await ending.abortTransaction();
            throw error('HA_FENCED', 'Ownership or maintenance gate changed before commit');
          }
        }
        commitSent = true;
        await ending.commitTransaction();
      }
      else await ending.abortTransaction();
    } catch (cause) {
      // Once commit is attempted, conservatively require reconciliation unless
      // its acknowledgement was received. Never replay the application callback.
      if (commitSent) cause.outcomeUnknown = true;
      failure = cause;
    } finally {
      session = undefined;
      // endSession may fail on the same broken socket. It must not overwrite
      // an unknown commit outcome, nor undo an acknowledged successful commit.
      try { await ending.endSession(); } catch { /* Preserve the transaction outcome. */ }
    }
    if (failure) throw failure;
  }
  async function buildIndex(collection, keys, options) {
    if (ownerId || session) unsupported('online index installation');
    const gate = randomUUID(), writers = db.collection('_ha_serial');
    const locked = await writers.updateOne({ _id: 'writer', epoch: 0, ownerId: null, ddl: config.maintenanceToken ?? null }, { $set: { ddl: gate } });
    if (locked.matchedCount !== 1) throw error('HA_FENCED', 'Index installation requires an unactivated database');
    try { return await collection.createIndex(keys, options); }
    finally {
      await writers.updateOne({ _id: 'writer', epoch: 0, ddl: gate }, config.maintenanceToken ? { $set: { ddl: config.maintenanceToken } } : { $unset: { ddl: '' } });
    }
  }
  async function schema(name) {
    const result = await db.collection('_ha_schema').findOne({ _id: name }, { session });
    if (!result) throw error('SQLITE_ERROR', `No such table: ${name}`);
    assertStoredSchema(result);
    return result;
  }
  function textBinding(node, bindings, column) {
    // The Node SQLite ABI binds JavaScript numbers as REAL. Preserve its exact
    // TEXT affinity for the bounded integer status values used by OmniRoute.
    // Do not guess SQLite's floating-point formatter or SQL-expression types.
    if (column?.type !== 'TEXT' || node?.type !== 'function' || node.name?.name?.[0]?.value?.toUpperCase() !== '__HA_BIND__') return;
    const value = bindings[node.args.value[0].value];
    if (Number.isSafeInteger(value) && Math.abs(value) < 1e14) return value.toFixed(1);
  }
  function validate(row, spec) {
    for (const column of spec.columns) {
      const value = row[column.name];
      if (value == null) {
        if (column.notNull) throw error('SQLITE_CONSTRAINT_NOTNULL', `NOT NULL constraint: ${spec._id}.${column.name}`);
        row[column.name] = null;
      } else if (column.type === 'TEXT' && typeof value !== 'string') unsupported(`TEXT affinity conversion: ${spec._id}.${column.name} (${typeof value})`);
      else if(column.type==='DATETIME'&&!(typeof value==='number'||Buffer.isBuffer(value)||typeof value==='string'&&!/^[\x09-\x0d ]*[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?[\x09-\x0d ]*$/i.test(value)))unsupported('DATETIME numeric-text affinity conversion');
      else if (['INTEGER', 'REAL', 'NUMERIC', 'BOOLEAN'].includes(column.type) && typeof value !== 'number') unsupported('numeric affinity conversion');
      else if (column.type === 'BLOB' && !Buffer.isBuffer(value)) unsupported('BLOB affinity conversion');
      if (value != null && column.numericCheck) {
        const { op, value: n } = column.numericCheck;
        const ok = { '>': value > n, '>=': value >= n, '<': value < n, '<=': value <= n, '!=': value !== n, '<>': value !== n }[op];
        if (!ok) throw error('SQLITE_CONSTRAINT_CHECK', 'CHECK constraint: ' + spec._id + '.' + column.name);
      }
      if (value != null && column.allowed && !column.allowed.includes(value) && !column.allowed.includes(null)) throw error('SQLITE_CONSTRAINT_CHECK', `CHECK constraint: ${spec._id}.${column.name}`);
    }
  }
  async function checkReferences(row, spec, before) {
    const checks = [...(spec.checks ?? []), ...spec.columns.map(c => c.checkExpression).filter(Boolean)];
    if (checks.length) {
      const cols = new Set(spec.columns.map(c => c.name));
      const projection = Object.fromEntries(checks.map((check,i) => ['check'+i, expression(check, [], cols)]));
      const [values] = await db.aggregate([{ $documents: { $literal: [row] } }, { $project: projection }], { session }).toArray();
      if (Object.entries(values).some(([key,value]) => key !== '_id' && value != null && !value)) throw error('SQLITE_CONSTRAINT_CHECK', 'Table CHECK constraint');
    }
    for (const fk of spec.foreignKeys ?? []) {
      if (fk.columns.some(k => row[k] == null)) continue;
      const parent = await schema(fk.table);
      if (!parent.unique.some(keys => JSON.stringify(keys) === JSON.stringify(fk.target))) unsupported('foreign key without unique parent');
      const filter = Object.fromEntries(fk.target.map((k,i) => [k, row[fk.columns[i]]]));
      if (fk.table === spec._id && fk.target.every((k,i) => row[k] === row[fk.columns[i]])) continue;
      if (!await db.collection(fk.table).findOne(filter, { session })) throw error('SQLITE_CONSTRAINT_FOREIGNKEY', 'Missing foreign key parent');
    }
    if (!before) return;
    const children = await db.collection('_ha_schema').find({ 'foreignKeys.table': spec._id }, { session }).toArray();
    for (const child of children) for (const fk of child.foreignKeys.filter(f => f.table === spec._id)) {
      if (fk.target.every(k => row[k] === before[k]) || fk.target.some(k => before[k] == null)) continue;
      const filter = Object.fromEntries(fk.columns.map((k,i) => [k, before[fk.target[i]]]));
      if (await db.collection(child._id).findOne(filter, { session })) throw error('SQLITE_CONSTRAINT_FOREIGNKEY', 'Referenced key update is restricted');
    }
  }
  async function deleteRows(spec, rows, visited = new Set(), depth = 0,credentialIntent,credentialSnapshot,credentialImport,fireDeleteTriggers=true) {
    if (depth > 32) unsupported('foreign key cascade depth');
    const children = await db.collection('_ha_schema').find({ 'foreignKeys.table': spec._id }, { session }).toArray();
    for (const row of rows) {
      if(spec._id==='provider_connections') await protectCredentialWrite(db,session,row,credentialIntent,true,credentialSnapshot,credentialImport);
      const identity = spec._id + ':' + row._id;
      if (visited.has(identity)) continue;
      visited.add(identity);
      for (const child of children) for (const fk of child.foreignKeys.filter(f => f.table === spec._id)) {
        if (fk.target.some(k => row[k] == null)) continue;
        const filter = Object.fromEntries(fk.columns.map((k,i) => [k, row[fk.target[i]]]));
        const matches = await db.collection(child._id).find(filter, { session }).toArray();
        if (matches.length && fk.onDelete !== 'CASCADE') throw error('SQLITE_CONSTRAINT_FOREIGNKEY', 'Referenced row delete is restricted');
        await deleteRows(child, matches, visited, depth + 1,credentialIntent,credentialSnapshot,credentialImport);
      }
      await db.collection(spec._id).deleteOne({ _id: row._id }, { session });
      if(fireDeleteTriggers&&spec.freeProxyCleanup===true) {
        assertFreeProxyCleanupSchema(spec,await schema('free_proxies'));
        await execute({sql:freeProxyCleanupSql,args:[row.id],mode:'run'});
      }
    }
  }
  async function createTable(ast) {
    if (session) unsupported('DDL inside transaction');
    if (ast.keyword !== 'table' || ast.temporary || ast.as || ast.query_expr) unsupported('CREATE form');
    const spec = compileTable(ast);
    const name = checkIdentifier(ast.table?.[0]?.table);
    const existing = await db.collection('_ha_schema').findOne({ _id: name });
    if (existing) {
      if (ast.if_not_exists) return { changes: 0, lastInsertRowid };
      throw error('SQLITE_ERROR', `Table already exists: ${name}`);
    }
    if (ownerId) unsupported('online schema creation; provision schema before enabling ownership');
    // MongoDB requires local read concern for explicit collection creation in a
    // transaction. The same writer fence serializes bootstrap metadata changes;
    // majority commit publishes the collection and its schema together.
    await begin({ ...transactionOptions, readConcern: { level: 'local' } });
    try {
      await db.createCollection(name, { session });
      await db.collection('_ha_schema').insertOne(spec, { session });
      await end(true);
    } catch (cause) {
      if (session) await end(false).catch(() => {});
      throw cause;
    }
    return { changes: 0, lastInsertRowid };
  }
  async function checkUnique(row, spec, exceptId) {
    if(spec._id==='usage_history'&&await db.collection('_ha_usage_history').findOne({_id:row.id},{session,projection:{_id:1}}))throw error('HA_ROLLUP_BASELINE','Archived financial usage IDs cannot be reused');
    for (const keys of spec.unique) {
      if (keys.some(key => row[key] == null)) continue;
      const filter = { $expr: { $and: keys.map(key => {
        const nocase = spec.columns.find(c => c.name === key)?.collation === 'NOCASE';
        return { $eq: [nocase ? noCaseKey('$' + key) : '$' + key, nocase ? noCaseKey({ $literal: row[key] }) : { $literal: row[key] }] };
      }) } };
      if (exceptId != null) filter._id = { $ne: exceptId };
      if (await db.collection(spec._id).findOne(filter, { session })) throw error('SQLITE_CONSTRAINT_UNIQUE', `UNIQUE constraint: ${spec._id}`);
    }
  }
  async function execute(request) {
    let retirementTime;
    const afterProviderWrite=(table,row,event,columns)=>applyProviderRetirement({db,session,table,row,event,columns,schema,
      now:async()=>{
        retirementTime??=db.aggregate([{$documents:[{}]},{$project:{now:'$$NOW'}}],{session}).toArray().then(([value])=>value.now.toISOString().slice(0,19).replace('T',' '));
        return retirementTime;
      },
      store:async(table,before,after)=>{
        const spec=await schema(table);validate(after,spec);await checkUnique(after,spec,before._id);await checkReferences(after,spec,before);
        await db.collection(table).replaceOne({_id:before._id},after,{session});
      },
    });
    const pragma = /^PRAGMA\s+table_info\(\s*([A-Za-z][A-Za-z0-9_]*|_omniroute_migrations)\s*\)$/i.exec(request.sql.trim());
    if (pragma) {
      const spec = await schema(pragma[1]);
      const rows = tableInfo(spec);
      return request.mode === 'get' ? rows[0] : rows;
    }
    const { ast, bindings } = request.parsed ?? parse(request.sql, request.args);
    if(['ha_reasoning_read','ha_reasoning_write'].includes(ast.type)) {
      if(ast.type==='ha_reasoning_read'?!['all','get'].includes(request.mode):request.mode!=='run')unsupported('reasoning cache query mode');
      const spec=await schema('reasoning_cache'),expiry=spec.columns.find(column=>column.name==='expires_at');
      if(expiry?.type!=='INTEGER'||!expiry.notNull)unsupported('reasoning expiry requires numeric INTEGER storage; legacy text dates require explicit migration');
      return execute({...request,sql:ast.sql,parsed:parse(ast.sql,ast.bindings),reasoningExpiry:true});
    }
    if(ast.type==='ha_audit_prune') {
      if(request.mode!=='run')unsupported('audit retention write mode');
      return {...await executeRetentionQuery(db,session,ast,schema,asciiLower,execute),lastInsertRowid};
    }
    if(['ha_usage_rollup_upsert','ha_usage_rollup_marker'].includes(ast.type)) {
      if(request.mode!=='run')unsupported('rollup write mode');
      const preview=await rollupPreview(request.usageRollup,ast.before);
      if(ast.type==='ha_usage_rollup_upsert')return archiveGlobalUpsert(db,session,ast.bindings,request.sql,execute);
      if(!preview.validated)throw error('HA_ROLLUP_STALE','Team rollup must precede its marker');
      if(preview.purge) {
        assertRetentionMarkerSchema(await schema('usage_history'));
        const result=await db.collection('usage_history').updateMany({timestamp:{$lt:preview.before},billing_team_id:{$nin:[null,'']},team_rollup_processed_at:null,...retentionDayMatch(preview.days)},{$set:{team_rollup_processed_at:ast.bindings[0]}},{session});
        return {changes:result.matchedCount,lastInsertRowid};
      }
      return execute({sql:'/* native rollup marker */ '+request.sql,args:ast.bindings,mode:'run'});
    }
    if(['ha_usage_rollup_read','ha_team_rollup_write'].includes(ast.type)) {
      if(ast.type==='ha_usage_rollup_read'?!['all','get'].includes(request.mode):request.mode!=='run')unsupported('usage rollup query mode');
      let snapshot;
      if(request.usageRollup) {
        if(!/^[a-f0-9-]{36}$/.test(request.usageRollup))throw error('HA_ROLLUP_STALE','Invalid usage rollup context');
        if(ast.type==='ha_usage_rollup_read') {
          if(session||usageRollups.size>=64||usageRollups.has(request.usageRollup))throw error('HA_ROLLUP_STALE','Usage preview is repeated or inside a transaction');
          snapshot=client.startSession({snapshot:true});
        } else {
          const preview=await rollupPreview(request.usageRollup,ast.before);
          if(preview.purge)ast.days=preview.days;
          preview.validated=true;
        }
      }
      let value;
      try {
        let selectedIds,days;
        if(snapshot&&request.usagePurge===true) {
          const selected=await db.collection('usage_history').find({timestamp:{$lt:ast.before}},{session:snapshot,projection:{timestamp:1}}).sort({timestamp:1,_ha_rowid:1}).limit(500).toArray();
          selectedIds=selected.map(row=>row._id);days=[...new Set(selected.map(row=>archiveDay(row.timestamp)))];ast.days=days;
        }
        value=await executeRetentionQuery(db,snapshot??session,ast,schema,asciiLower,execute);
        if(snapshot) {
          const writer=await db.collection('_ha_serial').findOne({_id:'writer'},{session:snapshot});
          if(!Number.isSafeInteger(writer.revision)||writer.revision>=Number.MAX_SAFE_INTEGER)throw error('HA_ROLLUP_STALE','Invalid usage preview revision');
          usageRollups.set(request.usageRollup,{before:ast.before,revision:writer.revision,epoch:writer.epoch,ownerId:writer.ownerId,rows:value,purge:request.usagePurge===true,selectedIds,days});
        }
      } finally {await snapshot?.endSession();}
      return ast.type==='ha_usage_rollup_read'?(request.mode==='get'?value[0]:value):{...value,lastInsertRowid};
    }
    if(['ha_game_count','ha_game_read'].includes(ast.type)) {
      if(ast.type==='ha_game_count' && request.mode!=='run' || ast.type==='ha_game_read' && !['get','all'].includes(request.mode))unsupported('Gamification query mode');
      const result=await executeGamificationQuery(db,ast,session,schema,execute);
      return ast.type==='ha_game_read'&&request.mode==='get'?result[0]:result;
    }
    if(ast.type==='ha_provider_identity_update') {
      if(request.mode!=='run')unsupported('provider usage identity write mode');
      return {...await executeProviderReconciliation(db,session,ast,schema),lastInsertRowid};
    }
    if(ast.type==='ha_provider_identity') {
      if(!['all','get'].includes(request.mode))unsupported('provider identity read mode');
      const rows=await executeProviderIdentity(db,session,ast,schema);return request.mode==='get'?rows[0]:rows;
    }
    if(ast.type==='ha_provider_recovery') {
      if(request.mode!=='run') unsupported('provider recovery write mode');
      const providerBefore=await db.collection('provider_connections').findOne({id:ast.bindings[1]},{session});
      await protectCredentialWrite(db,session,providerBefore,request.credentialIntent,false,request.credentialSnapshot,request.credentialImport);
      const result=await executeProviderQuery(db,session,ast,schema);
      if(result.changes) {
        const row=await db.collection('provider_connections').findOne({id:ast.bindings[1]},{session});
        const stored=await afterProviderWrite('provider_connections',row,'update',new Set(['test_status','last_error','last_error_type','last_error_source','last_error_at','error_code']));
        await recordCredentialWrite(db,session,stored,request.credentialIntent,request.credentialImport,providerBefore);
      }
      return {...result,lastInsertRowid,...(result.changes && request.credentialSnapshot?{_haCredentialVersion:credentialVersion(await db.collection('provider_connections').findOne({id:ast.bindings[1]},{session}))}:{})};
    }
    if (['ha_usage_spend','ha_codex_display','ha_team_raw','ha_team_summary','ha_unpriced_groups','ha_unpriced_affected','ha_provider_metrics','ha_migration_version'].includes(ast.type)) {
      if (!['all','get'].includes(request.mode)) unsupported('usage SELECT execution mode');
      const snapshot=(ast.type==='ha_usage_spend'||ast.type.startsWith('ha_unpriced_')||ast.type==='ha_provider_metrics')&&!session?client.startSession({snapshot:true}):null;
      let rows;try{rows=await executeUsageQuery(db,ast,snapshot??session,schema,asciiLower);}finally{await snapshot?.endSession();}
      return request.mode==='get'?rows[0]:rows;
    }
    if (ast.type === 'ha_quota_latest') {
      if (!['get', 'all'].includes(request.mode)) unsupported('quota SELECT execution mode');
      const spec = await schema('quota_snapshots');
      const rows = await executeLatestQuota(db, ast, spec.columns.map(c => c.name), session);
      return request.mode === 'get' ? rows[0] : rows;
    }
    if (ast.type === 'ha_conversation_candidates') {
      if (!['get', 'all'].includes(request.mode)) unsupported('conversation SELECT execution mode');
      const conversations = await schema('agentic_conversations');
      await schema('conversation_turn_nodes');
      const rows = await executeConversationCandidates(db, ast, conversations.columns.map(c => c.name), session);
      return request.mode === 'get' ? rows[0] : rows;
    }
    if (ast.type === 'create' && ast.keyword === 'index') {
      if (session) unsupported('index creation inside application transaction');
      const spec = await schema(checkIdentifier(ast.table.table));
      const { keys, partialFilterExpression, unique, name: indexName } = compileIndex(ast, spec);
      if(unique&&Object.keys(keys).some(key=>spec.columns.find(column=>column.name===key)?.collation))unsupported('standalone unique index on a collated column');
      if (ownerId) {
        const existing = (await db.collection(spec._id).listIndexes().toArray()).find(index => index.name === indexName);
        if (!ast.if_not_exists || !existing || JSON.stringify(existing.key) !== JSON.stringify(keys) ||
          JSON.stringify(existing.partialFilterExpression) !== JSON.stringify(partialFilterExpression) || !!existing.unique !== unique) unsupported('online index creation/change');
        return { changes: 0, lastInsertRowid };
      }
      await buildIndex(db.collection(spec._id), keys, { name: indexName, ...(unique ? { unique: true } : {}), ...(partialFilterExpression ? { partialFilterExpression } : {}) });
      if(unique&&!ast.where) {
      // Register UNIQUE indexes as UPSERT and foreign-key targets as well.
        await begin();
        try {await db.collection('_ha_schema').updateOne({_id:spec._id},{$addToSet:{unique:Object.keys(keys)}},{session});await end(true);}
        catch(cause){if(session)await end(false).catch(()=>{});throw cause;}
      }
      return { changes: 0, lastInsertRowid };
    }
    const keys = {
      create: ['type', 'keyword', 'temporary', 'if_not_exists', 'table', 'create_definitions', 'table_options'],
      alter: ['type', 'table', 'expr'],
      select: ['type', 'with', 'options', 'distinct', 'columns', 'from', 'where', 'groupby', 'having', 'orderby', 'limit', 'for_update'],
      insert: ['type', 'or', 'table', 'columns', 'values', 'partition', 'on_duplicate_update', 'returning', 'on_conflict'],
      update: ['type', 'table', 'set', 'where', 'returning', 'orderby', 'limit'],
      delete: ['type', 'table', 'from', 'where', 'returning', 'orderby', 'limit'],
    };
    onlyKeys(ast, keys[ast.type] ?? [], 'statement');
    if (ast.options || ast.partition) unsupported('statement options');
    if (ast.type === 'create') return createTable(ast);
    if (ast.type === 'alter') {
      if (ast.expr?.length !== 1 || ast.expr[0].action !== 'add' || ast.expr[0].resource !== 'column') unsupported('ALTER TABLE form');
      const spec = await schema(checkIdentifier(ast.table[0].table));
      const column = columnDefinition(ast.expr[0]);
      if(column.defaultKind)unsupported('ALTER column with generated default');
      if (spec.columns.some(c => c.name === column.name)) throw error('SQLITE_ERROR', 'Duplicate column');
      if (column.primary || ast.expr[0].unique) unsupported('ALTER unique column');
      const row = { [column.name]: column.default };
      validate(row, { ...spec, columns: [column] });
      await db.collection(spec._id).updateMany({}, { $set: row }, { session });
      await db.collection('_ha_schema').updateOne({ _id: spec._id }, { $push: { columns: column } }, { session });
      return { changes: 0, lastInsertRowid };
    }
    if (!['select', 'insert', 'update', 'delete'].includes(ast.type)) unsupported(ast.type);
    if (ast.type === 'select' && !ast.from) {
      if (ast.with || ast.distinct || ast.groupby || ast.having || ast.orderby || ast.limit || ast.for_update || request.mode === 'run') unsupported('constant SELECT clause');
      const projection = { _id: 0 };
      for (const column of ast.columns) {
        const label = column.as || String(column.expr.value);
        projection[label] = expression(column.expr, bindings, new Set());
      }
      const rows = await db.aggregate([{ $documents: [{}] }, { $match: predicate(ast.where, bindings, new Set()) }, { $project: projection }], { session, maxTimeMS: 3000 }).toArray();
      return request.mode === 'get' ? rows[0] : rows;
    }
    const refs = ast.type === 'select' || ast.type === 'delete' ? ast.from : ast.table;
    if (ast.type === 'select' && [2, 3].includes(refs?.length)) {
      const specs = [];
      for (const ref of refs) specs.push(await schema(checkIdentifier(ref.table)));
      const rows = await db.collection(specs[0]._id).aggregate(joinPipeline(ast, bindings, specs, request.mode), { session, ...(!session ? { readConcern: { level: 'snapshot' } } : {}), maxTimeMS: 5000 }).toArray();
      return request.mode === 'get' ? rows[0] : rows;
    }
    if (refs?.length !== 1 || refs[0].db || refs[0].as || refs[0].join) unsupported('table source/JOIN');
    const name = checkIdentifier(refs[0].table);
    if (name === 'sqlite_master') {
      // Support the upstream table-existence probe without synthesizing SQL
      // definitions, FTS tables, views, or unsupported indexes.
      const { left, right, operator } = ast.where ?? {};
      if (ast.type !== 'select' || request.mode === 'run' || ast.with || ast.distinct || ast.groupby ||
        ast.having || ast.orderby || ast.limit || ast.for_update || ast._next ||
        ast.columns?.length !== 1 || ast.columns[0].as ||
        !(ast.columns[0].expr.type === 'column_ref' && ast.columns[0].expr.column === 'name' && !ast.columns[0].expr.table ||
          ast.columns[0].expr.type === 'number' && ast.columns[0].expr.value === 1) || operator !== 'AND' ||
        left?.operator !== '=' || left.left?.type !== 'column_ref' || left.left.column !== 'type' || left.left.table ||
        right?.operator !== '=' || right.left?.type !== 'column_ref' || right.left.column !== 'name' || right.left.table ||
        literal(left.right, bindings) !== 'table') unsupported('SQLite catalog query');
      const table = literal(right.right, bindings);
      const exists = typeof table === 'string' && await db.collection('_ha_schema').findOne({ _id: table }, { session });
      const rows = exists ? [ast.columns[0].expr.type === 'number' ? { '1': 1 } : { name: table }] : [];
      return request.mode === 'get' ? rows[0] : rows;
    }
    const spec = await schema(name);
    const columns = new Set(spec.columns.map(c => c.name));
    columns.reasoningExpiry=request.reasoningExpiry===true&&name==='reasoning_cache';
    columns.withoutRowid = !!spec.withoutRowid;
    if (ast.with || ast.distinct || ast.having || ast._next || (ast.type === 'select' && ast.returning) || ast.for_update) unsupported('query clause');
    const collection = db.collection(name);
    async function resolveSubquery(node) {
      if (!node || typeof node !== 'object') return node;
      const scalar = node.type === 'binary_expr' && node.operator === '=' ? node.right?.ast : null;
      if (scalar) {
        if (ast.type !== 'update' || scalar.type !== 'select' || scalar.from?.length !== 1 || scalar.from[0].table !== name ||
          node.left?.type !== 'column_ref' || !columns.has(node.left.column) ||
          scalar.columns?.length !== 1 || scalar.columns[0].as || scalar.columns[0].expr?.type !== 'column_ref' ||
          scalar.columns[0].expr.column !== node.left.column || !scalar.limit || scalar.limit.seperator ||
          scalar.limit.value.length !== 1 || literal(scalar.limit.value[0], bindings) !== 1) unsupported('scalar update subquery');
        // Resolve the selected value in the same fenced snapshot as the update.
        // An empty scalar SELECT is SQL NULL, so equality matches no rows.
        const row = await execute({ sql: '', mode: 'get', parsed: { ast: scalar, bindings } });
        node.right = { type: 'function', name: { name: [{ value: '__ha_bind__' }] },
          args: { value: [{ type: 'number', value: bindings.push(row?.[node.left.column] ?? null) - 1 }] } };
        return node;
      }
      const sub = node.type === 'binary_expr' && node.operator === 'IN' && node.right?.type === 'expr_list' && node.right.value?.length === 1 ? node.right.value[0]?.ast : null;
      if (sub) {
        if (ast.type !== 'delete' || sub.type !== 'select' || sub.from?.length !== 1 || sub.from[0].table !== name ||
          sub.columns?.length !== 1 || sub.columns[0].expr?.type !== 'column_ref' || sub.columns[0].as ||
          !sub.limit || sub.limit.seperator || sub.limit.value.length !== 1) unsupported('unbounded or correlated subquery');
        const max = literal(sub.limit.value[0], bindings);
        if (!Number.isInteger(max) || max < 0 || max > 5000) unsupported('subquery limit');
        const result = await execute({ sql: '', mode: 'all', parsed: { ast: sub, bindings } });
        node.right.value = result.map(row => ({ type: 'function', name: { name: [{ value: '__ha_bind__' }] },
          args: { value: [{ type: 'number', value: bindings.push(row[sub.columns[0].expr.column]) - 1 }] } }));
      } else {
        for (const child of Object.values(node)) {
          if (Array.isArray(child)) for (const entry of child) await resolveSubquery(entry);
          else if (child && typeof child === 'object') await resolveSubquery(child);
        }
      }
      return node;
    }
    await resolveSubquery(ast.where);
    const filter = predicate(ast.where, bindings, columns);
    if (ast.type === 'select') {
      if (request.mode === 'run') unsupported('SELECT.run');
      const pipeline = [{ $match: filter }];
      const unwrapAggregate = node => node.type === 'function' &&
        ['COALESCE', 'IFNULL'].includes(node.name?.name?.[0]?.value?.toUpperCase()) &&
        node.args?.value?.length === 2 && node.args.value[0].type === 'aggr_func'
        ? { aggregate: node.args.value[0], fallback: node.args.value[1] } : { aggregate: node };
      if (ast.columns.some(c => unwrapAggregate(c.expr).aggregate.type === 'aggr_func')) {
        if (ast.limit || ast.orderby&&!ast.groupby) unsupported('aggregate ordering/limit');
        const group = { _id: null }, projection = { _id: 0 }, empty = {};
        const groupExpressions=new Map(),groupOutputs=[];
        // A single-table aggregate may qualify its own columns. Never strip a
        // foreign/correlated qualifier or broaden JOIN handling here.
        const localColumns=node=>{
          if(!node||typeof node!=='object')return;
          if(node.type==='column_ref'&&node.table){if(node.table!==name)unsupported('aggregate foreign qualifier');node.table=null;}
          for(const value of Object.values(node))if(Array.isArray(value))value.forEach(localColumns);else if(value&&typeof value==='object')localColumns(value);
        };
        ast.columns.forEach(column=>localColumns(column.expr));localColumns(ast.groupby);
        if (ast.groupby) {
          onlyKeys(ast.groupby, ['columns'], 'GROUP BY');
          group._id = {};
          for (const item of ast.groupby.columns) {
            // SQLite resolves a real input column before an output alias.
            // Bare positional GROUP BY and arbitrary ungrouped selections are
            // deliberately outside this profile.
            let selected=item;
            if(item.type==='column_ref'&&!columns.has(item.column)){
              const aliases=ast.columns.filter(column=>column.as===item.column);
              if(aliases.length!==1)unsupported('GROUP BY alias');selected=aliases[0].expr;
            }
            if(selected.type==='number'||selected.type==='aggr_func')unsupported('GROUP BY positional/aggregate expression');
            const value=expression(selected,bindings,columns),signature=JSON.stringify(value);
            if(!groupExpressions.has(signature)){
              const key='_key'+groupExpressions.size;group._id[key]=value;groupExpressions.set(signature,'$_id.'+key);
            }
          }
        }
        const groupedExpression=value=>{
          const grouped=groupExpressions.get(JSON.stringify(value));if(grouped)return grouped;
          if(Array.isArray(value))return value.map(groupedExpression);
          if(value&&typeof value==='object'){
            if(Object.hasOwn(value,'$literal'))return value;
            return Object.fromEntries(Object.entries(value).map(([key,part])=>[key,groupedExpression(part)]));
          }
          if(typeof value==='string'&&value.startsWith('$'))unsupported('non-grouped aggregate projection');
          return value;
        };
        let aggregateIndex = 0;
        for (const column of ast.columns) {
          const { aggregate, fallback } = unwrapAggregate(column.expr);
          if(aggregate.type!=='aggr_func'){
            const label=column.as||(aggregate.type==='column_ref'?aggregate.column:null);
            if(!label)unsupported('aggregate projection requires an alias');
            const value=expression(aggregate,bindings,columns);
            projection[label]=groupedExpression(value);groupOutputs.push(label);
            if(!ast.groupby)empty[label]=literal(aggregate,bindings);
            continue;
          }
          if (aggregate.type !== 'aggr_func' || !column.as || aggregate.over || aggregate.args.distinct) unsupported('aggregate form');
          const name = aggregate.name.toUpperCase();
          if (!['COUNT', 'MAX', 'MIN', 'SUM','AVG'].includes(name)) unsupported(`aggregate ${name}`);
          const argument = aggregate.args.expr;
          if(name==='AVG'&&(argument.type!=='column_ref'||!['INTEGER','REAL','NUMERIC'].includes(spec.columns.find(c=>c.name===argument.column)?.type)))unsupported('AVG requires a numeric column');
          if(name==='SUM'&&argument.type==='case') {
            const numeric=node=>['number','null'].includes(node?.type)||node?.type==='column_ref'&&['INTEGER','REAL','NUMERIC','BOOLEAN'].includes(spec.columns.find(c=>c.name===node.column)?.type)||
              node?.type==='function'&&node.name?.name?.[0]?.value==='__ha_bind__'&&(bindings[node.args.value[0].value]==null||typeof bindings[node.args.value[0].value]==='number');
            if(!argument.args.every(branch=>numeric(branch.result)))unsupported('SUM CASE requires numeric results');
          }
          if (argument.type === 'star' && name !== 'COUNT') unsupported('non-COUNT star aggregate');
          if (name === 'SUM' && argument.type === 'column_ref' &&
            !['INTEGER', 'REAL', 'NUMERIC'].includes(spec.columns.find(c => c.name === argument.column)?.type)) unsupported('SUM of nonnumeric column');
          const value = argument.type === 'star' ? null : expression(argument, bindings, columns);
          const field = `_value${aggregateIndex++}`;
          group[field] = name === 'COUNT'
            ? { $sum: argument.type === 'star' ? 1 : { $cond: [{ $eq: [value, null] }, 0, 1] } }
            : { [{ MAX: '$max', MIN: '$min', SUM: '$sum',AVG:'$avg' }[name]]: value };
          let projected = `$${field}`;
          if (name === 'SUM') {
            group[field + 'Count'] = { $sum: { $cond: [{ $eq: [value, null] }, 0, 1] } };
            projected = { $cond: [{ $eq: [`$${field}Count`, 0] }, null, projected] };
          }
          projection[column.as] = fallback ? { $ifNull: [projected, expression(fallback, bindings, new Set())] } : projected;
          empty[column.as] = name === 'COUNT' ? 0 : fallback ? literal(fallback, bindings) : null;
        }
        const grouped=[...pipeline,{$group:group},{$project:projection}];
        if(ast.orderby) {
          const sort={};
          for(const item of ast.orderby) {
            if(item.expr.type!=='column_ref'||item.expr.table||item.expr.collate||!Object.hasOwn(projection,item.expr.column)||item.expr.column==='_id')unsupported('aggregate ORDER BY output alias');
            sort[item.expr.column]=item.type==='DESC'?-1:1;
          }
          // SQLite's grouping b-tree gives stable selected-key ties; these keys
          // never reverse an explicitly requested ordering.
          for(const label of groupOutputs)sort[label]??=1;
          grouped.push({$sort:sort});
        }
        const rows = await collection.aggregate(grouped, { session, maxTimeMS: 5000 }).toArray();
        const result = rows.length || ast.groupby ? rows : [empty];
        return request.mode === 'get' ? result[0] : result;
      }
      if (ast.groupby) unsupported('GROUP BY without aggregate');
      if (ast.orderby) {
        const sort = {};
        for (const item of ast.orderby) {
          if(item.expr.type==='binary_expr'&&item.expr.operator==='IS'&&item.expr.right?.type==='null'&&item.expr.left?.type==='column_ref') {
            const field='_ha_sort'+Object.keys(sort).length;
            pipeline.push({$set:{[field]:expression(item.expr,bindings,columns)}});sort[field]=item.type==='DESC'?-1:1;continue;
          }
          if (item.expr.type === 'case') {
            const node = item.expr;
            onlyKeys(node,['type','expr','args'],'ORDER BY CASE');
            if (node.expr?.type !== 'column_ref' || node.expr.table || node.expr.collate ||
                spec.columns.find(c=>c.name===node.expr.column)?.type !== 'TEXT' || spec.columns.find(c=>c.name===node.expr.column)?.collation ||
                !Array.isArray(node.args) || node.args.length < 2 || node.args.length > 32 || node.args.at(-1).type !== 'else') unsupported('ORDER BY CASE form');
            const branches = [], number = entry => { const value = literal(entry,bindings); if (!Number.isSafeInteger(value)) unsupported('ORDER BY CASE result'); return {$literal:value}; };
            for (const branch of node.args.slice(0,-1)) {
              onlyKeys(branch,['type','cond','result'],'ORDER BY CASE branch');
              const value = literal(branch.cond,bindings);
              if (branch.type !== 'when' || typeof value !== 'string') unsupported('ORDER BY CASE condition');
              branches.push({case:{$eq:[expression(node.expr,bindings,columns),{$literal:value}]},then:number(branch.result)});
            }
            onlyKeys(node.args.at(-1),['type','result'],'ORDER BY CASE default');
            const field = '_ha_sort'+Object.keys(sort).length;
            pipeline.push({$set:{[field]:{$switch:{branches,default:number(node.args.at(-1).result)}}}});
            sort[field] = item.type === 'DESC' ? -1 : 1;
            continue;
          }
          if (item.expr.type !== 'column_ref' || (!columns.has(item.expr.column) && !(item.expr.column === 'rowid' && !spec.withoutRowid))) unsupported('ORDER BY expression');
          const col = item.expr.column === 'rowid' && !columns.has('rowid') ? '_ha_rowid' : item.expr.column, direction = item.type === 'DESC' ? -1 : 1;
          const collation = item.expr.collate?.collate?.name?.toUpperCase();
          if (collation === 'NOCASE') {
            if (spec.columns.find(c => c.name === col)?.type !== 'TEXT') unsupported('NOCASE on nontext column');
            const field = '_ha_sort' + Object.keys(sort).length, value = '$' + col;
            const nul = { $indexOfCP: [{ $ifNull: [value, ''] }, '\u0000'] };
            pipeline.push({ $set: {
              [field]: asciiLower({ $cond: [{ $gte: [nul, 0] }, { $substrCP: [value, 0, nul] }, value] }),
              [field + 'Bytes']: { $strLenBytes: { $ifNull: [value, ''] } },
            } });
            sort[field] = direction; sort[field + 'Bytes'] = direction;
          } else if (!item.expr.collate || collation === 'BINARY') sort[col] = direction;
          else unsupported('ORDER BY collation');
        }
        pipeline.push({ $sort: sort });
      }
      if (ast.limit) {
        if (ast.limit.seperator && ast.limit.seperator !== 'offset') unsupported('LIMIT comma form');
        if (ast.limit.value.length !== (ast.limit.seperator ? 2 : 1)) unsupported('LIMIT shape');
        const n = literal(ast.limit.value[0], bindings);
        const offset = ast.limit.seperator ? literal(ast.limit.value[1], bindings) : 0;
        if (!Number.isSafeInteger(n) || !Number.isSafeInteger(offset)) unsupported('LIMIT value');
        if (offset > 0) pipeline.push({ $skip: offset });
        if (n === 0) pipeline.push({ $match: { $expr: false } });
        else if (n > 0) pipeline.push({ $limit: n });
      }
      if (request.mode === 'get') pipeline.push({ $limit: 1 });
      const projection = { _id: 0 };
      for (const column of ast.columns) {
        if (column.expr.type === 'column_ref' && column.expr.column === '*') {
          for (const c of columns) projection[c] = { $ifNull: [`$${c}`, null] };
        } else {
          const label = column.as || column.expr.column || (ast.columns.length===1 && column.expr.type==='number' && Number(column.expr.value)===1 && /^\s*SELECT\s+1\s+FROM\b/i.test(request.sql)?'1':null);
          if (!label) unsupported('unnamed SELECT expression');
          projection[label] = expression(column.expr, bindings, columns);
        }
      }
      pipeline.push({ $project: projection });
      const rows = await collection.aggregate(pipeline, { session, maxTimeMS: 5000 }).toArray();
      return request.mode === 'get' ? rows[0] : rows;
    }
    if (request.mode !== 'run' && !ast.returning) unsupported('write result mode without RETURNING');
    async function writeResult(changes, rows) {
      if(name==='provider_connections' && request.credentialSnapshot && changes) {
        if(request.mode!=='run' || rows.length!==1 || rows[0].id!==request.credentialSnapshot.connectionId) unsupported('credential callback write shape');
        return {changes,lastInsertRowid,_haCredentialVersion:credentialVersion(rows[0])};
      }
      if(name==='teams' && changes) {
        const ids=rows.filter(row=>row.status!=='active' || !(row.max_budget_usd>0)).map(row=>row.id);
        // Disabled/archived Teams no longer track every call by receipt. Retain
        // reviewed historical debits, but require review before re-enablement.
        if(ids.length) await db.collection('_ha_team_profiles').updateMany({_id:{$in:ids}},{$set:{complete:false}},{session});
      }
      if (name==='quota_consumption' && ast.type!=='delete' && changes) {
        const ids=rows.filter(row=>/:usd:(hourly|5h|daily|weekly|monthly)$/.test(row.dimension_key)).map(row=>row._id);
        // SQL REAL writes cannot silently preserve a stale exact HA balance.
        // A zero/reset can restart tracking; positive legacy edits need proof.
        if (ids.length) await collection.updateMany({_id:{$in:ids}},{$unset:{_ha_usd_nano:''}},{session});
      }
      if (request.mode === 'run') return { changes, lastInsertRowid };
      if (ast.returning.type !== 'returning') unsupported('RETURNING form');
      const project = { _id: 0 };
      for (const item of ast.returning.columns) {
        if (item.expr.type === 'column_ref' && item.expr.column === '*') {
          for (const c of columns) project[c] = { $ifNull: ['$' + c, null] };
        } else {
          const label = item.as || item.expr.column;
          if (!label) unsupported('RETURNING expression label');
          project[label] = expression(item.expr, bindings, columns);
        }
      }
      if (!rows.length) return request.mode === 'get' ? undefined : [];
      const result = await db.aggregate([{ $documents: { $literal: rows } }, { $project: project }], { session }).toArray();
      return request.mode === 'get' ? result[0] : result;
    }
    if (ast.limit || ast.orderby || ast.on_duplicate_update) unsupported('write clause');
    if (ast.type === 'delete') {
      const rows = await collection.find(filter, { session }).toArray();
      await deleteRows(spec, rows,undefined,0,request.credentialIntent,request.credentialSnapshot,request.credentialImport);
      return writeResult(rows.length, rows);
    }
    if (ast.type === 'update') {
      const assignments = {};
      for (const item of ast.set) {
        if (!columns.has(item.column)) unsupported(`column ${item.column}`);
        const converted = textBinding(item.value, bindings, spec.columns.find(c => c.name === item.column));
        assignments[item.column] = converted === undefined ? expression(item.value, bindings, columns) : { $literal: converted };
      }
      const rows = await collection.aggregate([{ $match: filter }, { $set: assignments }], { session }).toArray();
      for (const row of rows) {
        if (name === 'memories' && ('content' in assignments || 'key' in assignments)) {
          delete row._ha_vector;
          if (columns.has('needs_reindex')) row.needs_reindex = 1;
        }
        const alias = !spec.withoutRowid && spec.columns.find(c => c.primary && c.type === 'INTEGER');
        if (alias) {
          if (!Number.isSafeInteger(row[alias.name])) unsupported('noninteger rowid update');
          row._ha_rowid = row[alias.name];
        }
        const before=await collection.findOne({ _id: row._id }, { session });
        if(name==='provider_connections')await protectProviderIdentity(db,session,row,before,'update',new Set(Object.keys(assignments)));
        validate(row, spec); await checkUnique(row, spec, row._id);
        await checkReferences(row, spec, before);
        if(name==='provider_connections') await protectCredentialWrite(db,session,before,request.credentialIntent,false,request.credentialSnapshot,request.credentialImport);
        await collection.replaceOne({ _id: row._id }, row, { session });
        const stored=await afterProviderWrite(name,row,'update',new Set(Object.keys(assignments)));
        if(name==='provider_connections') await recordCredentialWrite(db,session,stored,request.credentialIntent,request.credentialImport,before);
      }
      return writeResult(rows.length, rows);
    }
    const conflict = ast.or?.[1]?.value?.toUpperCase();
    if (conflict && !['REPLACE', 'IGNORE'].includes(conflict)) unsupported('INSERT conflict mode');
    if (ast.values?.type !== 'values' || !ast.columns) unsupported('INSERT source');
    const [{ now }] = await db.aggregate([{ $documents: [{}] }, { $project: { now: '$$NOW' } }], { session }).toArray();
    const datetime = now.toISOString().slice(0, 19).replace('T', ' ');
    let changes = 0; const returned = [];
    for (const values of ast.values.values) {
      if (values.value.length !== ast.columns.length) throw error('SQLITE_ERROR', 'INSERT column count');
      const row = Object.fromEntries(spec.columns.map(c => [c.name, ast.columns.includes(c.name)?c.default:c.defaultKind==='random-hex8'?randomBytes(8).toString('hex'):c.defaultKind==='utc-seconds'?now.toISOString().slice(0,19)+'Z':c.defaultKind === 'datetime-now' ? datetime : c.default]));
      ast.columns.forEach((column, i) => {
        if (!columns.has(column)) unsupported(`column ${column}`);
        const value = values.value[i];
        const nowFunction = value.type === 'function' && value.name?.name?.[0]?.value?.toUpperCase() === 'DATETIME' &&
          value.args?.value?.length === 1 && value.args.value[0].value === 'now';
        row[column] = nowFunction || currentTimestamp(value) ? datetime : textBinding(value, bindings, spec.columns.find(c => c.name === column)) ?? literal(value, bindings);
      });
      const largest = await collection.findOne({}, { session, sort: { _ha_rowid: -1 }, projection: { _ha_rowid: 1 } });
      const alias = !spec.withoutRowid && spec.columns.find(c => c.primary && c.type === 'INTEGER');
      let rowid = alias && row[alias.name] != null ? row[alias.name] : Math.max(0, largest?._ha_rowid ?? 0) + 1;
      if (alias?.autoIncrement) {
        const sequence = await db.collection('_ha_sequences').findOne({ _id: name }, { session });
        if (row[alias.name] == null) rowid = Math.max(rowid, (sequence?.value ?? 0) + 1);
        await db.collection('_ha_sequences').updateOne({ _id: name }, { $max: { value: Math.max(0, rowid) } }, { session, upsert: true });
      }
      if (!Number.isSafeInteger(rowid)) unsupported('64-bit rowid overflow or noninteger primary key');
      if (alias) row[alias.name] = rowid;
      if(name==='provider_connections')await protectProviderIdentity(db,session,row,undefined,'insert',new Set(ast.columns));
      validate(row, spec);
      if (ast.on_conflict) {
        const target = ast.on_conflict.columns;
        if (!spec.unique.some(keys => keys.length === target.length && keys.every(k => target.includes(k)))) unsupported('ON CONFLICT target without matching unique constraint');
        const existing = await collection.findOne(Object.fromEntries(target.map(k => [k, row[k]])), { session });
        if (existing && target.every(k => row[k] != null)) {
          if (ast.on_conflict.set === null) continue;
          const replaceExcluded = node => {
            if (node?.type === 'column_ref' && node.table === 'excluded') {
              if (!columns.has(node.column)) unsupported('excluded column');
              return { type: 'function', name: { name: [{ value: '__ha_bind__' }] }, args: { value: [{ type: 'number', value: bindings.push(row[node.column]) - 1 }] } };
            }
            if (Array.isArray(node)) return node.map(replaceExcluded);
            if (node && typeof node === 'object') return Object.fromEntries(Object.entries(node).map(([k, v]) => [k, replaceExcluded(v)]));
            return node;
          };
          const assignments = {};
          for (const item of ast.on_conflict.set) {
            if (!columns.has(item.column)) unsupported('UPSERT update column');
            assignments[item.column] = expression(replaceExcluded(item.value), bindings, columns);
          }
          const [updated] = await collection.aggregate([{ $match: { _id: existing._id } }, { $set: assignments }], { session }).toArray();
          if (name === 'memories' && ('content' in assignments || 'key' in assignments)) {
            delete updated._ha_vector;
            if (columns.has('needs_reindex')) updated.needs_reindex = 1;
          }
          if(name==='provider_connections')await protectProviderIdentity(db,session,updated,existing,'update',new Set(Object.keys(assignments)));
          validate(updated, spec); await checkUnique(updated, spec, existing._id); await checkReferences(updated, spec, existing);
          if(name==='provider_connections') await protectCredentialWrite(db,session,existing,request.credentialIntent,false,request.credentialSnapshot,request.credentialImport);
          await collection.replaceOne({ _id: existing._id }, updated, { session });
          const stored=await afterProviderWrite(name,updated,'update',new Set(Object.keys(assignments)));
          if(name==='provider_connections') await recordCredentialWrite(db,session,stored,request.credentialIntent,request.credentialImport,existing);
          returned.push(updated); changes++; continue;
        }
      }
      if (conflict === 'REPLACE') {
        for (const keys of spec.unique) {
          // Default SQLite recursive_triggers=OFF suppresses DELETE triggers on
          // REPLACE victims. FK cascades still execute their own DELETE triggers.
          if (keys.every(key => row[key] != null)) await deleteRows(spec, await collection.find(Object.fromEntries(keys.map(key => [key, row[key]])), { session }).toArray(),undefined,0,request.credentialIntent,request.credentialSnapshot,request.credentialImport,false);
        }
      }
      try { await checkUnique(row, spec); }
      catch (cause) { if (conflict === 'IGNORE' && cause.code === 'SQLITE_CONSTRAINT_UNIQUE') continue; throw cause; }
      row._ha_rowid = rowid;
      await checkReferences(row, spec);
      if(name==='provider_connections')await protectCredentialWrite(db,session,row,request.credentialIntent,false,request.credentialSnapshot,request.credentialImport);
      await collection.insertOne(row, { session });
      const stored=await afterProviderWrite(name,row,'insert',new Set(ast.columns));
      if(name==='provider_connections')await recordCredentialWrite(db,session,stored,request.credentialIntent,request.credentialImport);
      if (spec.requestDetailRetention === 500) {
        const overflow = await collection.find({}, { session, projection: { _id: 1 } })
          .sort({ timestamp: -1, _ha_rowid: -1 }).skip(500).toArray();
        if (overflow.length) await collection.deleteMany({ _id: { $in: overflow.map(r => r._id) } }, { session });
      }
      returned.push(row); changes++; if (!spec.withoutRowid) lastInsertRowid = rowid;
    }
    return writeResult(changes, returned);
  }
  return async request => {
    try {
      if(request.operation==='releaseUsageRollup'){usageRollups.delete(request.id);return;}
      if(request.operation==='recordUsagePrices')return recordArchivePrices(usageRollups.get(request.id),request.rows);
      if(request.operation==='finishUsageRollup') {
        try{return await finishUsageArchive(db,session,await rollupPreview(request.id,request.before),execute);}
        catch(cause){if(session){await end(false).catch(()=>{});aborted=true;}throw cause;}
      }
      if (request.operation === 'ping') return { ok: (await db.command({ ping: 1 })).ok };
      if(request.operation==='storageStatus'){
        if(session)throw error('HA_STORAGE_CONTEXT','Storage metadata cannot run inside an application transaction');
        const stats=await db.command({dbStats:1,scale:1,maxTimeMS:3000});
        for(const field of ['dataSize','storageSize','indexSize','collections','objects'])if(!Number.isSafeInteger(stats[field])||stats[field]<0)throw error('HA_STORAGE_STATUS','Invalid MongoDB storage metadata');
        return{driver:'mongodb',namespace:'mongodb:'+db.databaseName,totalSize:stats.storageSize+stats.indexSize,dataSize:stats.dataSize,
          collections:stats.collections,objects:stats.objects,pageSize:null,pageCount:null,cacheSize:null,tables:null,indexes:null,
          statisticsScope:'MongoDB dbStats; SQLite page/cache and per-table details are not applicable'};
      }
      if (request.operation === 'credential' || request.operation === 'reservation' || request.operation === 'admitRequest' || request.operation === 'checkQuotaPolicy') {
        const implicit = !session;
        if (implicit) await begin();
        try {
          const enabled = Object.keys(config.quotaBounds ?? {}).length > 0;
          const result = request.operation==='credential'?await credentialOperation(db,session,request,schema,config.allowCredentialRecovery===true):request.operation === 'reservation' ? await reservationOperation(db, session, request, execute, config.quotaBounds, schema, config.allowReconciliation === true)
            : request.operation === 'checkQuotaPolicy' ? await checkQuotaPolicy(db, session, request.keyId, enabled)
            : await admitRequest(db, session, request.keyId, request.rules, enabled);
          if (implicit) await end(true);
          return result;
        } catch (cause) {
          if (session) { await end(false).catch(() => {}); aborted = true; }
          throw cause;
        }
      }
      if (request.operation === 'installRequestLimits') {
        if (ownerId || session) unsupported('online limit-index installation');
        await buildIndex(db.collection('_ha_request_limits'), { expiresAt: 1 }, { name: 'ha_request_expiry', expireAfterSeconds: 0 });
        await buildIndex(db.collection('_ha_reservations'), { state: 1, keyId: 1, epoch: 1 }, { name: 'ha_reservation_state' });
        await buildIndex(db.collection('_ha_reservations'), { teamId: 1, state: 1, settledAt: 1 }, { name: 'ha_team_reservations' });
        await buildIndex(db.collection('_ha_credential_intents'), { connectionId: 1, state: 1 }, { name: 'ha_credential_state' });
        await buildIndex(db.collection('_ha_credential_intents'), { lane: 1 }, { name: 'ha_one_dispatched_rotation', unique:true,partialFilterExpression:{state:'dispatched'} });
        await buildIndex(db.collection('_ha_usage_history'), {api_key_id:1,timestamp:1}, {name:'ha_financial_history'});
        await buildIndex(db.collection('_ha_usage_archive_totals'), {_id:1}, {name:'_id_'});
        await buildIndex(db.collection('_ha_credential_intents'), { importKey: 1 }, { name: 'ha_import_identity',unique:true,partialFilterExpression:{kind:'import'} });return;
      }
      if (request.operation === 'installMemoryTextIndex') {
        if (ownerId || session) unsupported('online text index installation');
        await schema('memories');
        await buildIndex(db.collection('memories'), { content: 'text', key: 'text' }, { name: textIndex, default_language: 'none' });
        return;
      }
      if (request.operation === 'installRequestDetailRetention') {
        if (ownerId || session) unsupported('online trigger installation');
        await begin();
        try {
          const spec = await schema('request_detail_logs');
          if (!spec.columns.some(c => c.name === 'timestamp' && c.type === 'TEXT' && c.notNull) ||
              !spec.primaryKey?.includes('id')) unsupported('request-detail schema mismatch');
          await db.collection('_ha_schema').updateOne({ _id: spec._id }, { $set: { requestDetailRetention: 500 } }, { session });
          await end(true);
        } catch (cause) { if (session) await end(false).catch(() => {}); throw cause; }
        return;
      }
      if(request.operation==='installFreeProxyCleanup') {
        if(ownerId||session)unsupported('online Free Proxy cleanup installation');
        await begin();
        try {
          const registry=await schema('proxy_registry');assertFreeProxyCleanupSchema(registry,await schema('free_proxies'));
          await db.collection('_ha_schema').updateOne({_id:registry._id},{$set:{freeProxyCleanup:true}},{session});await end(true);
        }catch(cause){if(session)await end(false).catch(()=>{});throw cause;}
        return;
      }
      if (request.operation === 'verifyBaseSchema') {
        const plan = readBaseSchema();
        if(request.runtime)plan.tables.set('ha_call_log_artifacts',compileTable(parse(artifactSchemaSql).ast));
        const names=[...plan.tables.keys()];
        const options = { maxTimeMS: 3000 };
        const specs = await db.collection('_ha_schema').find({}, options).toArray();
        for(const spec of specs)assertStoredSchema(spec);
        const found = await db.listCollections({ name: { $in: names }, type: 'collection' }, options).toArray();
        const catalog = new Map();
        for (const { name } of found) catalog.set(name, await db.collection(name).listIndexes(options).toArray());
        const result=checkBaseSchema(plan, specs, catalog);
        if(ownerId) {
          // No implicit collection/index creation by a promoted application.
          // Archive prerequisites must exist before either host can become ready.
          for(const [name,indexName,keys] of [['_ha_usage_history','ha_financial_history',{api_key_id:1,timestamp:1}],['_ha_usage_archive_totals','_id_',{_id:1}]]) {
            let indexes;try{indexes=await db.collection(name).listIndexes(options).toArray();}catch{throw error('HA_SCHEMA_MISMATCH','Provision financial archive collections offline before application startup');}
            if(!indexes.some(index=>index.name===indexName&&JSON.stringify(index.key)===JSON.stringify(keys)&&index.expireAfterSeconds===undefined&&!index.partialFilterExpression))throw error('HA_SCHEMA_MISMATCH','Financial archive index contract mismatch');
          }
          const snapshot=session??client.startSession({snapshot:true});
          try{
            await verifyProviderRetirement(db,snapshot);
            const proxySpecs=await db.collection('_ha_schema').find({_id:{$in:['proxy_registry','free_proxies']}},{session:snapshot,maxTimeMS:3000}).toArray();
            const registry=proxySpecs.find(spec=>spec._id==='proxy_registry'),free=proxySpecs.find(spec=>spec._id==='free_proxies');
            if(free||registry?.freeProxyCleanup) {
              if(registry?.freeProxyCleanup!==true)throw error('HA_SCHEMA_MISMATCH','Install the pinned Free Proxy cleanup offline before startup');
              try{assertFreeProxyCleanupSchema(registry,free);}catch{throw error('HA_SCHEMA_MISMATCH','Free Proxy cleanup schema is incomplete');}
              const tables=await db.listCollections({name:{$in:['proxy_registry','free_proxies']},type:'collection'},options).toArray();
              if(tables.length!==2)throw error('HA_SCHEMA_MISMATCH','Free Proxy cleanup collections are missing');
            }
          }finally{if(snapshot!==session)await snapshot.endSession();}
        }
        return result;
      }
      if (request.operation === 'clock') {
        const [value] = await db.aggregate([{ $documents: [{}] }, { $project: { now: { $toLong: '$$NOW' } } }], { session, maxTimeMS: 3000 }).toArray();
        return value.now;
      }
      if (request.operation === 'ownership') return ownership(request.action, request.leaseMs);
      if (request.operation === 'begin') { await begin(); return; }
      if (request.operation === 'commit') { await end(true); return; }
      if (request.operation === 'rollback') { await end(false); return; }
      if (request.operation === 'memoryVector') {
        if (['search','hybrid'].includes(request.action) && !session) {
          const snapshot = client.startSession({ snapshot: true });
          try { return await executeMemoryVector(db, snapshot, request); }
          finally { await snapshot.endSession(); }
        }
        const write = ['configure', 'upsert', 'delete', 'reset','deleteDecayed'].includes(request.action);
        const implicit = write && !session;
        if (implicit) await begin();
        try {
          const value = request.action==='deleteDecayed'
            ?await deleteDecayedMemory(db,session,request,async id=>(await execute({sql:'DELETE FROM memories WHERE id=?',args:[id],mode:'run'})).changes>0)
            :await executeMemoryVector(db, session, request);
          if (implicit) await end(true);
          return value;
        } catch (cause) {
          if (session) { await end(false).catch(() => {}); aborted = true; }
          throw cause;
        }
      }
      if (request.operation !== 'statement') unsupported('worker operation');
      const isPragma = /^PRAGMA\s+table_info\(/i.test(request.sql.trim());
      if (!isPragma) request.parsed = parse(request.sql, request.args);
      const kind = isPragma ? 'pragma' : request.parsed.ast.type;
      const implicit = !session && !['select', 'ha_conversation_candidates', 'ha_quota_latest', 'ha_usage_spend','ha_codex_display','ha_team_raw','ha_team_summary','ha_unpriced_groups','ha_unpriced_affected','ha_provider_metrics','ha_migration_version','ha_game_read','ha_provider_identity','ha_usage_rollup_read','ha_reasoning_read','pragma', 'create'].includes(kind);
      if (implicit) await begin();
      try {
        const result = await execute(request);
        if (implicit) await end(true);
        return result;
      } catch (cause) {
        // Do not permit a caller to catch a statement failure and commit earlier
        // partial effects of that statement. Abort the actual server transaction.
        if (session) { await end(false).catch(() => {}); aborted = true; }
        throw cause;
      }
    } catch (cause) {
      if(cause.code===241&&cause.message?.includes('HA_SQLITE_TEXT_CAST_REQUIRES_INTEGER_OR_TEXT'))throw error('HA_UNSUPPORTED_SQL','Joined CAST TEXT received an uncertified floating/blob value');
      if(cause.code===241&&cause.message?.includes('HA_REASONING_EXPIRY_REQUIRES_NUMERIC'))throw error('HA_UNSUPPORTED_SQL','Reasoning expiry received uncertified nonnumeric or unsafe numeric storage');
      if (cause.code === 11000) cause.code = 'SQLITE_CONSTRAINT_UNIQUE';
      if (cause.code === 112 || cause.hasErrorLabel?.('TransientTransactionError')) cause.code = 'SQLITE_BUSY';
      throw cause;
    }
  };
}
