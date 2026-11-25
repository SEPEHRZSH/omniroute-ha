import { createRedisAccess } from './redis.mjs';
import { ActiveOwnership } from './ownership.mjs';
import { MongoMemoryVectors } from './memoryVectors.mjs';
import { createCacheBus } from './cacheBus.mjs';
import { installQuotaRuntime } from './quotaRuntime.mjs';
import { createCredentialRuntime } from './credentialRuntime.mjs';
import {memoryDecaySchedule,runMemoryDecay} from './memoryDecay.mjs';
import {createRetentionRuntime} from './retentionRuntime.mjs';
import {createFreeProxySync} from './freeProxySync.mjs';
import {rotationGroupFor} from './credentialState.mjs';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';

const limitsSource=new URL('../upstream/omniroute/src/lib/usage/providerLimits.ts',import.meta.url);
if(createHash('sha256').update(readFileSync(limitsSource)).digest('hex')!=='a834b98e6057aa03c8ae3669f40ad231956ac2bc157cd724230b6c35f27dd9a1')throw new Error('Provider limits synchronization contract changed');
const quotaRecoverySource=new URL('../upstream/omniroute/src/lib/quota/quotaSessionRecovery.ts',import.meta.url);
if(createHash('sha256').update(readFileSync(quotaRecoverySource)).digest('hex')!=='b59231d97fe86030b1d22af23f8ccca15b0773278f360779c3900f48e111897f')throw new Error('Quota session recovery contract changed');
if(createHash('sha256').update(readFileSync(new URL('../upstream/omniroute/src/lib/db/quotaRecoveryTickets.ts',import.meta.url))).digest('hex')!=='83eee4d2177d03c1aafbdcd136afdfc1b45257edefc2f4748af476d38e4b5c53')throw new Error('Quota recovery ticket claim/result contract changed');

export async function cleanupReasoning(runtime) {
  runtime.assertOwner();const state=runtime.reasoningCleanup;
  if(!state?.started)throw new Error('Owned reasoning cleanup is not started');
  if(state.running)return{skipped:true};state.running=true;
  try{
    const {cleanupExpiredReasoning}=await import('../upstream/omniroute/src/lib/db/reasoningCache.ts');runtime.assertOwner();
    for(const purge of globalThis.__omnirouteReasoningPurges??[])purge();
    const deleted=cleanupExpiredReasoning();runtime.assertOwner();
    state.runs++;state.lastDeleted=deleted;return{deleted};
  }finally{state.running=false;}
}

export async function runQuotaRecovery(runtime) {
  runtime.assertOwner();const state=runtime.quotaRecovery;
  if(!state?.started)throw new Error('Owned quota recovery scheduler is not started');
  if(state.running)return{skipped:true};state.running=true;
  try{
    const {runQuotaSessionRecoveryTick}=await import(quotaRecoverySource);runtime.assertOwner();
    await runQuotaSessionRecoveryTick(new Date(runtime.db.serverTime()));runtime.assertOwner();
    state.runs++;state.lastRunAt=new Date(runtime.db.serverTime()).toISOString();
    return{runs:state.runs};
  }finally{state.running=false;}
}

// Reuse the original selection, quota transport, persistence and spacing. All
// durable writes still cross the MongoDB epoch fence; rotating exchanges also
// cross their durable single-attempt guard. In-flight read-only provider polls
// can overlap a promotion, but cannot publish an old owner's result.
export async function syncProviderLimits(runtime) {
  runtime.assertOwner();
  const state=runtime.providerLimits;
  if(!state?.started)throw new Error('Owned provider limits scheduler is not started');
  if(state.running)return{skipped:true};
  state.running=true;
  try{
    const [{getProviderConnections},{syncAllProviderLimits}]=await Promise.all([
      import('../upstream/omniroute/src/lib/db/providers.ts'),import(limitsSource),
    ]);
    runtime.assertOwner();const rows=await getProviderConnections({isActive:true},1001);
    if(rows.length>1000)throw new Error('Provider limits sync exceeds the bounded provider profile');
    runtime.assertOwner();const result=await syncAllProviderLimits({source:'scheduled',concurrency:1});runtime.assertOwner();
    state.lastRun={total:result.total,succeeded:result.succeeded,failed:result.failed};state.runs++;
    return state.lastRun;
  }finally{state.running=false;}
}

// The upstream rotator probes host-local directories. MongoDB artifacts instead
// need bounded, fenced cleanup of both summaries and their shared payloads.
export async function maintainCallLogs(db) {
  const [{ expirePendingCallLogDetails }, { deleteCallLogsBefore, trimCallLogsToMaxRows }, { resolveCallLogRetentionDays }] = await Promise.all([
    import('../upstream/omniroute/src/lib/db/callLogDetails.ts'),
    import('../upstream/omniroute/src/lib/usage/callLogRotation.ts'),
    import('../upstream/omniroute/src/lib/usage/callLogRetention.ts'),
  ]);
  const now = db.serverTime();
  const cutoff = new Date(now - resolveCallLogRetentionDays() * 86400000).toISOString();
  return db.transaction(() => {
    expirePendingCallLogDetails(db, now);
    const expired = deleteCallLogsBefore(cutoff, 25);
    const overflow = trimCallLogsToMaxRows(undefined, 25);
    let orphans = 0;
    const candidates = db.prepare('SELECT path FROM ha_call_log_artifacts WHERE mtime_ms<? ORDER BY mtime_ms ASC LIMIT 25').all(now - 300000);
    for (const { path } of candidates) {
      if (!db.prepare('SELECT id FROM call_logs WHERE artifact_relpath=? LIMIT 1').get(path)) {
        orphans += db.prepare('DELETE FROM ha_call_log_artifacts WHERE path=?').run(path).changes;
      }
    }
    return { expired: expired.deletedRows, overflow: overflow.deletedRows, orphans };
  })();
}

// Reuse upstream recovery selection, but conditionally clear the exact snapshot
// through its original CAS update. A newly recorded error must survive a tick.
export async function recoverConnections(runtime) {
  runtime.assertOwner();
  const [{runConnectionRecoveryTick,toRecoverableConnectionInput},{getProviderConnections,clearConnectionErrorIfUnchanged}]=await Promise.all([
    import('../upstream/omniroute/src/lib/quota/connectionRecovery.ts'),
    import('../upstream/omniroute/src/lib/db/providers.ts'),
  ]);
  runtime.assertOwner();
  const rows=await getProviderConnections({},1001);
  if(rows.length>1000) throw new Error('Connection recovery exceeds bounded provider profile');
  runtime.assertOwner();
  const result=await runConnectionRecoveryTick({nowMs:runtime.db.serverTime(),loadConnections:async()=>{
    runtime.assertOwner(); return rows.map(toRecoverableConnectionInput);
  },clearConnectionError:async(id,current)=>{
    runtime.assertOwner();
    if(!await clearConnectionErrorIfUnchanged(id,current)) throw new Error('Connection cooldown changed; recovery skipped');
    runtime.assertOwner();
  }});
  runtime.assertOwner(); return result;
}

    // Run upstream health checks without the process-local scheduler.
    // Rotating providers use durable exchange intents; unsupported providers
    // are counted separately from completed checks.
export async function refreshCredentials(runtime) {
  runtime.assertOwner();
  const [{getProviderConnections},{checkConnection},{getEffectiveTokenExpiryMs}]=await Promise.all([
    import('../upstream/omniroute/src/lib/db/providers.ts'),
    import('../upstream/omniroute/src/lib/tokenHealthCheck.ts'),
    import('../upstream/omniroute/src/lib/tokenHealthCheckExpiry.ts'),
  ]);
  runtime.assertOwner();const rows=await getProviderConnections({authType:'oauth'},1001);
  if(rows.length>1000) throw new Error('Token health exceeds bounded provider profile');
  const result={checked:0,unsupported:0,skipped:0,failed:0};
  for(const row of rows) {
    runtime.assertOwner();
    if(!globalThis.__omnirouteCredentialRuntime.supportsProactive(row.provider)) {result.unsupported++;continue;}
    // Aliases share the native rotation lane even when the upstream interval
    // map omits them. Unknown expiry must not consume a one-time refresh grant.
    if(rotationGroupFor(row.provider)&&!getEffectiveTokenExpiryMs(row)) {result.skipped++;continue;}
    try {await checkConnection(row);result.checked++;}
    catch(error) {result.failed++;console.warn('[HA] Token health pass failed:',String(error.code||'HA_CREDENTIAL_CHECK_FAILED'));}
    runtime.assertOwner();
  }
  return result;
}

const proxyTables=new Set(['key_value','provider_connections','proxy_registry','proxy_assignments','proxy_scope_rotation']);
function invalidateProxyCaches(fallback) {
  const callbacks=new Set(globalThis.__omnirouteProxyInvalidators??[]);
  if(fallback)callbacks.add(fallback);
  for(const invalidate of callbacks)invalidate();
}
async function invalidateCaches() {
  const [cache, keys, memory, settings] = await Promise.all([
    import('../upstream/omniroute/src/lib/db/readCache.ts'),
    import('../upstream/omniroute/src/lib/db/apiKeys.ts'),
    import('../upstream/omniroute/src/lib/memory/settings.ts'),
    import('../upstream/omniroute/src/lib/db/settings.ts'),
  ]);
  cache.invalidateDbCache(); keys.clearApiKeyCaches(); memory.invalidateMemorySettingsCache();
  for(const invalidate of globalThis.__omnirouteReadInvalidators??[])invalidate();
  invalidateProxyCaches(settings.bumpProxyConfigGeneration);
  return cache;
}
async function reloadCaches() {
  const cache = await invalidateCaches();
  await cache.getCachedSettings(); await cache.getCachedProviderConnections();
}

// Install before Next prepares the application. The original instrumentation
// body is registered once, then invoked only on promotion. Several upstream
// jobs have no stop API, so a retired process must be replaced by a supervisor.
export class ActiveRuntime {
  constructor(db, { leaseMs = 10000, intervalMs = 1000, warm = reloadCaches,
    recycle = reason => { console.error('[HA] Retiring application process:', reason); process.exit(75); } } = {}) {
    if (!db?.requiresOwnership) throw new Error('HA runtime requires a fenced MongoDB connection');
    this.db = db; this.recycle = recycle; this.jobs = new Map();
    this.owner = new ActiveOwnership(db, {
      leaseMs, intervalMs,
      onPromote: async () => {
        const renew = () => {
          if (!this.retired) {
            if (!db.healthy()) throw new Error('MongoDB unavailable during promotion');
            db.ownership('renew', leaseMs);
          }
        };
        // Startup can yield for longer than a lease. Renew only the same
        // unexpired epoch; a pause or loss of majority must retire this process.
        const heartbeat = setInterval(() => {
          try { renew(); } catch (error) { this.retire(error.code || error.message); }
        }, intervalMs);
        heartbeat.unref();
        try {
          db.verifyBaseSchema({runtime:true}); renew();
          await warm(); this.assertOwner();
          await this.boot(); this.assertOwner();
          for (const start of this.jobs.keys()) { this.assertOwner(); await start(); }
        } finally { clearInterval(heartbeat); }
      },
      onDemote: () => this.retire(this.owner.lastError || 'Active ownership lost'),
    });
  }
  registerJob(start, stop = () => {}) {
    if (this.jobs.has(start)) return;
    this.jobs.set(start, stop);
    if (this.owner.state === 'active') {
      Promise.resolve().then(() => { this.assertOwner(); return start(); }).catch(error => this.retire(error.message));
    }
  }
  assertOwner() {
    if (this.retired || !this.db.healthy() || !this.db.canWrite()) throw new Error('MongoDB active ownership required for startup');
  }
  register(boot) {
    if (this.retired) throw new Error('Retired runtime cannot start');
    if (this.boot === boot) return;
    if (this.boot || typeof boot !== 'function') throw new Error('Invalid duplicate boot registration');
    this.db.verifyBaseSchema({runtime:true});
    this.boot = boot;
    this.owner.start();
  }
  ready() { return !this.retired && this.owner.ready(); }
  retire(reason) {
    if (this.retired) return;
    this.retired = true;
    this.owner.stopped = true;
    clearTimeout(this.owner.timer);
    // Invalidate this process's connection before retiring timers/jobs. Do not
    // release a lease early or replay an in-flight operation on a new process.
    for (const stop of this.jobs.values()) { try { Promise.resolve(stop()).catch(() => {}); } catch {} }
    this.cacheBus?.close(); this.redis?.close(); this.db.close();
    this.recycle(reason);
  }
}

export function attachActiveRuntime(db, options) {
  process.env.TZ ||= 'UTC'; // The same explicit deployment zone on independent hosts.
  if (globalThis.__omnirouteActiveRuntime || globalThis.__omnirouteMongoAdapter) throw new Error('HA runtime already installed');
  const runtime = new ActiveRuntime(db, options);
  runtime.redis = createRedisAccess(process.env);
  globalThis.__omnirouteRedis = runtime.redis;
  globalThis.__omnirouteMongoAdapter = db;
  globalThis.__omnirouteMongoVectors = new MongoMemoryVectors(db);
  globalThis.__omnirouteActiveRuntime = runtime;
  globalThis.__omnirouteHardQuota = installQuotaRuntime(db, runtime);
  globalThis.__omnirouteCredentialRuntime=createCredentialRuntime(db,runtime);
  globalThis.__omnirouteRetention=createRetentionRuntime(db,runtime);
  runtime.freeProxySync=createFreeProxySync(runtime);
  globalThis.__omnirouteFreeProxySync=runtime.freeProxySync;
  runtime.registerJob(()=>runtime.freeProxySync.start(),()=>runtime.freeProxySync.stop());
  let stopQuotaRefresh;
  runtime.quotaRefresh={started:false};
  runtime.registerJob(async()=>{
    const [quota,{getQuotaCacheState}]=await Promise.all([
      import('../upstream/omniroute/src/domain/quotaCache.ts'),
      import('../upstream/omniroute/src/domain/quotaCacheState.ts'),
    ]);
    runtime.assertOwner();const state=getQuotaCacheState();
    if(state.refreshTimer||state.tickRunning)throw new Error('Uncontrolled quota refresh already running');
    // The original source and webpack chunks share this state. Promotion must
    // not inherit a standby's cached exhaustion/healthy override observations.
    state.cache.clear();state.healthyUntil.clear();state.refreshingSet.clear();
    stopQuotaRefresh=quota.stopBackgroundRefresh;quota.startBackgroundRefresh();runtime.quotaRefresh.started=true;
  },()=>{stopQuotaRefresh?.();runtime.quotaRefresh.started=false;});
  let providerLimitsTimer;
  runtime.providerLimits={started:false,running:false,runs:0};
  runtime.registerJob(async()=>{
    if(['1','true','yes','on'].includes((process.env.OMNIROUTE_DISABLE_PROVIDER_LIMITS_SYNC||'').trim().toLowerCase()))return;
    const {getProviderLimitsSyncIntervalMs,getLastProviderLimitsAutoSyncTime}=await import(limitsSource);runtime.assertOwner();
    const interval=getProviderLimitsSyncIntervalMs();
    if(!Number.isSafeInteger(interval)||interval<1000||interval>2147483647)throw new Error('Invalid provider limits sync interval');
    const previous=Date.parse(await getLastProviderLimitsAutoSyncTime());runtime.assertOwner();
    const elapsed=db.serverTime()-previous;
    const delay=Number.isFinite(previous)&&elapsed>=0&&elapsed<interval?Math.max(5000,interval-elapsed):5000;
    runtime.providerLimits.started=true;
    const tick=async()=>{
      try{await syncProviderLimits(runtime);}
      catch(error){runtime.retire(error.code||'Provider limits sync failed');}
      if(!runtime.retired&&runtime.providerLimits.started){providerLimitsTimer=setTimeout(tick,interval);providerLimitsTimer.unref();}
    };
    providerLimitsTimer=setTimeout(tick,delay);providerLimitsTimer.unref();
  },()=>{runtime.providerLimits.started=false;clearTimeout(providerLimitsTimer);});
  let reasoningCleanupTimer;
  runtime.reasoningCleanup={started:false,running:false,runs:0};
  runtime.registerJob(async()=>{
    const raw=Number(process.env.OMNIROUTE_REASONING_CACHE_CLEANUP_INTERVAL_MS);
    const interval=Number.isFinite(raw)&&raw>=60000?raw:30*60*1000;
    if(interval>2147483647)throw new Error('Reasoning cleanup interval exceeds the supported timer range');
    runtime.reasoningCleanup.started=true;
    await cleanupReasoning(runtime);
    const tick=async()=>{
      try{await cleanupReasoning(runtime);}
      catch(error){runtime.retire(error.code||'Reasoning cleanup failed');return;}
      if(!runtime.retired&&runtime.reasoningCleanup.started){reasoningCleanupTimer=setTimeout(tick,interval);reasoningCleanupTimer.unref();}
    };
    reasoningCleanupTimer=setTimeout(tick,interval);reasoningCleanupTimer.unref();
  },()=>{runtime.reasoningCleanup.started=false;clearTimeout(reasoningCleanupTimer);});
  let quotaRecoveryTimer;
  runtime.quotaRecovery={started:false,running:false,runs:0};
  runtime.registerJob(async()=>{
    if(['1','true','yes','on'].includes((process.env.OMNIROUTE_DISABLE_QUOTA_SESSION_RECOVERY||'').trim().toLowerCase()))return;
    const {reconcileInterruptedQuotaRecoveries}=await import('../upstream/omniroute/src/lib/db/quotaRecoveryTickets.ts');runtime.assertOwner();
    // A previous owner's running dispatch is ambiguous, never a retry grant.
    reconcileInterruptedQuotaRecoveries();runtime.quotaRecovery.started=true;
    const tick=async()=>{
      // Do not route a ticket while promotion/Next preparation is incomplete.
      if(!runtime.ready()){
        if(!runtime.retired&&runtime.quotaRecovery.started){quotaRecoveryTimer=setTimeout(tick,1000);quotaRecoveryTimer.unref();}return;
      }
      try{await runQuotaRecovery(runtime);}
      catch(error){runtime.retire(error.code||'Quota session recovery failed');}
      if(!runtime.retired&&runtime.quotaRecovery.started){quotaRecoveryTimer=setTimeout(tick,60000);quotaRecoveryTimer.unref();}
    };
    quotaRecoveryTimer=setTimeout(tick,1000);quotaRecoveryTimer.unref();
  },()=>{runtime.quotaRecovery.started=false;clearTimeout(quotaRecoveryTimer);});
  runtime.cacheBus = createCacheBus({ env: process.env, access: runtime.redis,
    identity: db.name + ':' + (process.env.OMNIROUTE_HA_IDENTITY || ''), epoch: () => runtime.owner.epoch,
    owns: () => runtime.ready(), invalidate: invalidateCaches });
  db.onCommitted = tables => {
    if(tables.includes('key_value')) {
      try{for(const invalidate of globalThis.__omnirouteReadInvalidators??[])invalidate('pricing',undefined,{skipModelCatalog:true});}
      catch{runtime.retire('Shared read cache invalidation failed');return;}
    }
    if(tables.some(table=>proxyTables.has(table))) {
      try {invalidateProxyCaches();}catch {runtime.retire('Proxy cache invalidation failed');return;}
    }
    return runtime.cacheBus.publish(tables);
  };
  void runtime.cacheBus.start();
  let maintenanceTimer;
  runtime.registerJob(() => {
    runtime.maintenanceStarted = true;
    maintenanceTimer = setInterval(() => {
      if (runtime.ready()) void maintainCallLogs(db).catch(error => runtime.retire(error.code || 'Log maintenance failed'));
    }, 60000);
    maintenanceTimer.unref();
  }, () => { runtime.maintenanceStarted = false; clearInterval(maintenanceTimer); });
  let recoveryTimer;
  runtime.registerJob(async()=>{
    if(['1','true','yes','on'].includes((process.env.OMNIROUTE_DISABLE_CONNECTION_RECOVERY||'').trim().toLowerCase())) return;
    const {resolveConnectionRecoveryIntervalMs}=await import('../upstream/omniroute/src/lib/quota/connectionRecovery.ts');
    const interval=resolveConnectionRecoveryIntervalMs();
    const tick=async()=>{
      try {await recoverConnections(runtime);}
      catch(error){runtime.retire(error.code||'Connection recovery failed');}
      if(!runtime.retired) {recoveryTimer=setTimeout(tick,interval);recoveryTimer.unref();}
    };
    await recoverConnections(runtime); runtime.recoveryStarted=true;
    recoveryTimer=setTimeout(tick,interval); recoveryTimer.unref();
  },()=>{runtime.recoveryStarted=false;clearTimeout(recoveryTimer);});
  let credentialTimer;
  runtime.registerJob(()=>{
    if(['1','true','yes','on'].includes((process.env.OMNIROUTE_DISABLE_TOKEN_HEALTHCHECK||'').trim().toLowerCase())) return;
    const tick=async()=>{
      try {runtime.tokenHealth.lastRun=await refreshCredentials(runtime);}
      catch(error){runtime.retire(error.code||'Token health pass failed');}
      if(!runtime.retired) {credentialTimer=setTimeout(tick,60000);credentialTimer.unref();}
    };
    runtime.tokenHealth={started:true};
    // Preserve the original delayed first pass; promotion/readiness does not
    // wait for an external OAuth server. Each pass still checks fresh ownership.
    credentialTimer=setTimeout(tick,10000);credentialTimer.unref();
  },()=>{if(runtime.tokenHealth)runtime.tokenHealth.started=false;clearTimeout(credentialTimer);});
  let decayTimer;
  runtime.registerJob(async()=>{
    const {config,interval}=await memoryDecaySchedule();runtime.assertOwner();
    if(!config.enabled||!interval)return;
    runtime.memoryDecay={started:true,runs:0};
    const tick=async()=>{
      try {
        const result=await runMemoryDecay(runtime,{config});runtime.memoryDecay.lastRun=result;runtime.memoryDecay.runs++;
        console.info('[HA] Memory decay:',JSON.stringify({scanned:result.scanned,decayed:result.decayed,deleted:result.deletedIds.length,capped:result.capped}));
      }
      catch(error){runtime.retire(error.code||'Memory decay failed');}
      if(!runtime.retired){decayTimer=setTimeout(tick,interval);decayTimer.unref();}
    };
    decayTimer=setTimeout(tick,0);decayTimer.unref();
  },()=>{if(runtime.memoryDecay)runtime.memoryDecay.started=false;clearTimeout(decayTimer);});
  let retentionTimer;
  runtime.registerJob(()=>{
    runtime.usageRetention={started:true,runs:0};
    const tick=async()=>{
      let delay=6*3600000;
      try {
        runtime.assertOwner();
        const {getUserDatabaseSettings}=await import('../upstream/omniroute/src/lib/db/databaseSettings.ts');
        if(getUserDatabaseSettings().retention.autoCleanupEnabled) {
          const result=await globalThis.__omnirouteRetention.cleanupUsageHistory();runtime.assertOwner();
          runtime.usageRetention.lastRun=result;runtime.usageRetention.runs++;
          if(result.errors||result.hasMore)delay=60000;
        }
      } catch(error) {
        if(!runtime.ready())runtime.retire(error.code||'Usage retention ownership unavailable');
        else {delay=60000;console.warn('[HA] Usage retention deferred:',String(error.code||'HA_RETENTION_FAILED'));}
      }
      if(!runtime.retired){retentionTimer=setTimeout(tick,delay);retentionTimer.unref();}
    };
    retentionTimer=setTimeout(tick,30000);retentionTimer.unref();
  },()=>{if(runtime.usageRetention)runtime.usageRetention.started=false;clearTimeout(retentionTimer);});
  return runtime;
}
