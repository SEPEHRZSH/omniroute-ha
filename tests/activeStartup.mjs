// Run upstream instrumentation in a child so the parent can clean up after a boot failure.
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import path from 'node:path';
import { Socket } from 'node:net';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { attachActiveRuntime } from '../adapter/activeRuntime.mjs';
import { testDatabase } from './mongoTestSupport.mjs';
import { provisionStartupSchema } from '../adapter/startupSchema.mjs';

if (process.argv[2] === 'peer') {
  // Database I/O uses a dedicated Mongo worker. Boot on synthetic data must
  // not contact external providers, Redis, a local agent, or update services.
  Socket.prototype.connect = function () { throw new Error('External connection disabled in HA startup test'); };
  const db = new MongoAdapter({ uri: process.env.HA_TEST_URI, database: process.env.HA_TEST_DATABASE, ownerId: 'startup-peer' });
  db.onUnsupported = sql => console.error('[unsupported]', sql);
  const runtime = attachActiveRuntime(db, { leaseMs: 30000, intervalMs: 500,
    recycle(reason) { console.error('[retired]', reason, runtime.owner.lastCause?.message?.split('\n')[0], db.compatibilityFault || ''); process.exit(1); },
  });
  const { register } = await import('../upstream/omniroute/src/instrumentation.ts');
  await register();
  for (let attempt = 0; attempt < 300 && !runtime.ready(); attempt++) await new Promise(resolve => setTimeout(resolve, 100));
  assert.equal(runtime.ready(), true);
  const { getServerLifecyclePhase } = await import('../upstream/omniroute/src/lib/serverLifecycle.ts');
  assert.equal(getServerLifecyclePhase(), 'ready');
  const {getDatabaseSettings,updateDatabaseSettings,runIntegrityCheck}=await import('../upstream/omniroute/src/lib/db/databaseSettings.ts');
  const storage=db.storageStatus(),databaseSettings=getDatabaseSettings();
  assert.equal(storage.driver,'mongodb');assert.ok(storage.collections>=17);assert.ok(storage.totalSize>0);
  assert.equal(databaseSettings.driver,'mongodb');assert.equal(databaseSettings.stats.pageCount,null);assert.equal(databaseSettings.stats.freelistCount,null);
  assert.equal(databaseSettings.location.databasePath,'mongodb:'+db.name);assert.ok(databaseSettings.stats.databaseSizeBytes>0);
  assert.throws(()=>updateDatabaseSettings({optimization:{pageSize:4096}}),{code:'HA_STORAGE_UNSUPPORTED'});
  assert.throws(()=>updateDatabaseSettings({backup:{autoBackupEnabled:true}}),{code:'HA_STORAGE_UNSUPPORTED'});
  assert.throws(()=>runIntegrityCheck(),{code:'HA_STORAGE_UNSUPPORTED'});
  assert.equal(runtime.ready(),true);assert.equal(db.compatibilityFault,undefined);
  console.log('MONGODB_STORAGE_METADATA_VERIFIED');
  assert.equal(runtime.quotaRefresh.started,true);
  assert.equal(runtime.providerLimits.started,true);
  assert.equal(runtime.quotaRecovery.started,true);
  assert.equal(runtime.reasoningCleanup.started,true);
  assert.equal(runtime.reasoningCleanup.runs,1,'Original numeric-expiry cleanup runs once during active startup');
  const limitsDeadline=Date.now()+10000;
  while(!runtime.providerLimits.runs&&Date.now()<limitsDeadline)await new Promise(resolve=>setTimeout(resolve,100));
  assert.deepEqual(runtime.providerLimits.lastRun,{total:0,succeeded:0,failed:0});
  assert.ok(runtime.providerLimits.runs>=1,'Natural Provider Limits startup timer must execute');
  assert.ok(runtime.quotaRecovery.runs>=1,'Owned quota recovery startup timer must execute after readiness');
  console.log('OWNED_QUOTA_RECOVERY_VERIFIED');
  const {getLastProviderLimitsAutoSyncTime}=await import('../upstream/omniroute/src/lib/usage/providerLimits.ts');
  assert.ok(Number.isFinite(Date.parse(await getLastProviderLimitsAutoSyncTime())));
  console.log('OWNED_PROVIDER_LIMITS_VERIFIED');
  const {getQuotaCacheState}=await import('../upstream/omniroute/src/domain/quotaCacheState.ts');
  const quotaState=getQuotaCacheState();assert.ok(quotaState.refreshTimer);assert.equal(quotaState.refreshTimer.hasRef(),false);
  await quotaState.refreshTimer._onTimeout(); // Execute the actual registered callback, without a minute-long sleep.
  assert.equal(db.prepare('SELECT id FROM quota_snapshots WHERE connection_id=?').get('synthetic-owned-sweep'),undefined);
  const {getBestVisionModel}=await import('../upstream/omniroute/src/lib/guardrails/visionBridgeRouter.ts');
  let calls=0;const invoke=db.bridge.call;
  db.bridge.call=function(request){assert.ok(++calls<=200,'Vision enumeration must not perform per-model remote lookups');return invoke.call(this,request);};
  try{assert.equal(await getBestVisionModel({fixedModel:'',selectionCacheTtlMs:0,noCandidateCacheTtlMs:0},{hasUsableCredentials:async()=>false,isBlockedNoAuthProvider:async()=>false}),null);}
  finally{db.bridge.call=invoke;}
  console.log('OWNED_QUOTA_AND_BULK_VISION_VERIFIED '+calls);
  assert.equal(db.compatibilityFault, undefined);
  // All assertions have passed. Retire owned timers before intentionally
  // closing the worker; otherwise a heartbeat can misclassify test cleanup as
  // an unexpected database outage and invoke the failure-only exit callback.
  runtime.recycle=()=>{};
  runtime.retire('Startup verification complete');
  await db.close();
  process.exit(0);
} else {
  const test = await testDatabase('activeboot');
  const data = mkdtempSync(path.resolve('.work/activeboot-'));
  const settings = { NODE_ENV: 'test', NEXT_RUNTIME: 'nodejs', NODE_TEST_CONTEXT: 'isolated-boot',
    DATA_DIR: data, NEXT_TELEMETRY_DISABLED: '1', OMNIROUTE_HEADLESS: '1', OMNIROUTE_DISABLE_BACKGROUND_SERVICES: 'true',
    DISABLE_SQLITE_AUTO_BACKUP: '1', OMNIROUTE_DISABLE_CREDENTIAL_HEALTH_CHECK: '1', PROXY_HEALTH_ENABLED: 'false',
    OMNIROUTE_DISABLE_REDIS_AUTH_CACHE: '1', OMNIROUTE_DISABLE_CLI_TOKEN: 'true', OMNIROUTE_EVENT_LOOP_STALL_THRESHOLD_MS: '0',
    OMNIROUTE_HA_IDENTITY: 'synthetic-startup-deployment', HOSTNAME: '127.0.0.1', API_PORT: '20128', DASHBOARD_PORT: '20128',
    API_KEY_SECRET: 'synthetic-api-key-secret-for-ha-tests-only', JWT_SECRET: 'synthetic-jwt-secret-for-ha-tests-only',
    STORAGE_ENCRYPTION_KEY: 'synthetic-storage-key-for-ha-tests-only', INITIAL_PASSWORD: 'synthetic-boot-password-only',
    REDIS_URL: '', QDRANT_HOST: '', VECTOR_STORE_DISABLE_VEC: 'true' };
  const bootstrap = new MongoAdapter({ uri: test.uri, database: test.database });
  let child, timer;
  try {
    Object.assign(process.env, settings);
    await provisionStartupSchema(bootstrap);
    bootstrap.prepare('INSERT INTO quota_snapshots(provider,connection_id,window_key,remaining_percentage,created_at) VALUES (?,?,?,?,?)')
      .run('synthetic-quota','synthetic-owned-sweep','daily',50,'2000-01-01T00:00:00.000Z');
    await test.db.collection('request_detail_logs').insertMany(Array.from({length:500},(_,i) => ({
      id: 'seed-' + i, timestamp: '2099-01-01T00:00:00.000Z', _ha_rowid: i+1,
    })));
    const insertDetail = bootstrap.prepare('INSERT INTO request_detail_logs(id,timestamp) VALUES (?,?)');
    assert.throws(() => bootstrap.transaction(() => { insertDetail.run('rollback-detail', '2099-01-02'); throw new Error('rollback retention'); })(), /rollback retention/);
    assert.equal(await test.db.collection('request_detail_logs').countDocuments({}), 500);
    insertDetail.run('new-detail', '2099-01-02');
    assert.equal(await test.db.collection('request_detail_logs').countDocuments({}), 500);
    assert.equal(await test.db.collection('request_detail_logs').findOne({id:'seed-0'}), null);
    await bootstrap.close();
    child = fork(new URL(import.meta.url), ['peer'], {
      execArgv: ['--permission', '--allow-worker', '--allow-child-process', '--allow-addons',
        '--allow-fs-read=' + process.cwd(), '--allow-fs-write=' + data, '--import', './adapter/register.mjs'],
      env: { ...settings, PATH: path.dirname(process.execPath), HA_TEST_URI: test.uri, HA_TEST_DATABASE: test.database, TMPDIR: data },
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    });
    let tail = '';
    for (const stream of [child.stdout, child.stderr]) stream.on('data', chunk => { tail = (tail + chunk).slice(-6000); });
    timer = setTimeout(() => child.kill('SIGKILL'), 55000);
    const [code, signal] = await once(child, 'exit');
    if (code !== 0) throw new Error(`Active startup failed (${code ?? signal}):\n${tail}`);
    assert.match(tail,/MONGODB_STORAGE_METADATA_VERIFIED/);
    assert.match(tail,/OWNED_QUOTA_AND_BULK_VISION_VERIFIED \d+/);
    assert.match(tail,/OWNED_PROVIDER_LIMITS_VERIFIED/);
    assert.match(tail,/OWNED_QUOTA_RECOVERY_VERIFIED/);
    console.log('PASS upstream active instrumentation reached ready with MongoDB, headless mode and optional background schedulers disabled');
    console.log('PASS owned original quota refresh: real registered timer removes expired MongoDB snapshots; vision fallback scans the original registry with a per-call bulk capability view and at most 200 bridge operations');
    console.log('PASS owned original Provider Limits synchronization: real active instrumentation registers the scheduler and the original empty-provider sync persists its last scheduled run in MongoDB');
    console.log('PASS owned original quota session recovery: actual instrumentation registers the scheduler; natural post-readiness tick runs the original expiration/stale/due-ticket functions on MongoDB');
    console.log('PASS real MongoDB storage metadata and explicit N/A SQLite fields; SQLite optimization, automatic backup and quick_check refuse without retiring the owner');
  } finally {
    clearTimeout(timer); await bootstrap.close(); await test.cleanup();
    rmSync(data, { recursive: true, force: true }); // This test's unique temporary directory only.
  }
}
