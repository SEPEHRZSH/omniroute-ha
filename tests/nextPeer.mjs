// Real Next.js HTTP harness for selected, unmodified upstream route handlers.
// The default fixture mounts selected handlers. HA_TEST_FULL_BACKEND instead
// exercises the built official backend, its instrumentation and native routes.
import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { createRequire } from 'node:module';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { fencedHttpHandler } from '../adapter/http.mjs';
import { attachActiveRuntime, maintainCallLogs, recoverConnections } from '../adapter/activeRuntime.mjs';
import './networkGuard.mjs';
import { verifyBuildContract } from '../adapter/buildContract.mjs';
import {runMemoryDecay} from '../adapter/memoryDecay.mjs';

// Node 22's ESM/CJS cycle handling with registerHooks cannot import Next's
// CommonJS entry as ESM. Its supported CommonJS entry avoids that loader cycle.
const next = createRequire(import.meta.url)('next');
process.env.TZ ||= 'UTC'; // Match the supported entry before creating its worker.

const db = new MongoAdapter({ uri: process.env.HA_TEST_URI, database: process.env.HA_TEST_DATABASE, ownerId: randomUUID() });
const owner = attachActiveRuntime(db, { leaseMs: 10000, intervalMs: 400 });
db.onUnsupported = (sql, error) => console.error('[unsupported]', error?.message, sql);
const fullBackend = process.env.HA_TEST_FULL_BACKEND === '1';
const directory = path.resolve(fullBackend ? '.work/backend' : 'tests/nextApp');
if (fullBackend) verifyBuildContract(directory);
const app = next({ dev: false, dir: directory, quiet: true });
await app.prepare();
const keys = await import('../upstream/omniroute/src/lib/db/apiKeys.ts');
const settings = await import('../upstream/omniroute/src/lib/db/settings.ts');
const cache = await import('../upstream/omniroute/src/lib/db/readCache.ts');
const providers = await import('../upstream/omniroute/src/lib/db/providers.ts');
if (!fullBackend) {
const routes = {
  '/api/v1/chat/completions': await import('../upstream/omniroute/src/app/api/v1/chat/completions/route.ts'),
  '/api/settings': await import('../upstream/omniroute/src/app/api/settings/route.ts'),
  '/api/keys': await import('../upstream/omniroute/src/app/api/keys/route.ts'),
  '/api/quota/pools': await import('../upstream/omniroute/src/app/api/quota/pools/route.ts'),
  '/api/usage/budget': await import('../upstream/omniroute/src/app/api/usage/budget/route.ts'),
  '/api/usage/token-limits': await import('../upstream/omniroute/src/app/api/usage/token-limits/route.ts'),
  '/api/memory/engine-status': await import('../upstream/omniroute/src/app/api/memory/engine-status/route.ts'),
  '/api/memory/retrieve-preview': await import('../upstream/omniroute/src/app/api/memory/retrieve-preview/route.ts'),
  '/api/memory': await import('../upstream/omniroute/src/app/api/memory/route.ts'),
};
const keyRoute = await import('../upstream/omniroute/src/app/api/keys/[id]/route.ts');
const memoryRoute = await import('../upstream/omniroute/src/app/api/memory/[id]/route.ts');
const providerRoute = await import('../upstream/omniroute/src/app/api/providers/[id]/route.ts');
const planRoute = await import('../upstream/omniroute/src/app/api/quota/plans/[connectionId]/route.ts');
const poolUsageRoute = await import('../upstream/omniroute/src/app/api/quota/pools/[id]/usage/route.ts');
const { AUTHZ_TRUSTED_HEADERS, PEER_IP_HEADER, VIA_PROXY_HEADER, CLIENT_IP_HEADER } = await import('../upstream/omniroute/src/server/authz/headers.ts');
globalThis.__omnirouteTestProxy = (await import('../upstream/omniroute/src/proxy.ts')).proxy;
const { ensureDbReadyForBoot } = await import('../upstream/omniroute/src/instrumentation-node.ts');
await ensureDbReadyForBoot();
// Deliberately warm stale state on the standby so promotion must invalidate it.
await cache.getCachedSettings();
await cache.getCachedProviderConnections();
globalThis.__omnirouteRouteTestDispatch = async request => {
  if (!owner.ready()) return Response.json({ error: 'Standby or database unavailable' }, { status: 503 });
  // The proxy ran in Next before dispatch. Never forward client-supplied
  // trusted stamps through the fixture's additional request reconstruction.
  const headers = new Headers(request.headers);
  for (const header of [...AUTHZ_TRUSTED_HEADERS, PEER_IP_HEADER, VIA_PROXY_HEADER, CLIENT_IP_HEADER]) headers.delete(header);
  request = new Request(request, { headers });
  const pathname = new URL(request.url).pathname;
  const match = /^\/api\/(keys|memory|providers)\/([^/]+)$/.exec(pathname);
  const planMatch = /^\/api\/quota\/plans\/([^/]+)$/.exec(pathname);
  const poolMatch = /^\/api\/quota\/pools\/([^/]+)\/usage$/.exec(pathname);
  const route = routes[pathname] || (planMatch ? planRoute : poolMatch ? poolUsageRoute : match ? ({ keys: keyRoute, memory: memoryRoute, providers: providerRoute })[match[1]] : undefined);
  const handler = route?.[request.method];
  if (!handler) return Response.json({ error: 'Route not mounted in test harness' }, { status: 404 });
  const response = await handler(request, { params: Promise.resolve({ id: match?.[2] ?? poolMatch?.[1], connectionId: planMatch?.[1] }) });
  if (!owner.ready()) return Response.json({ error: db.compatibilityFault || 'Ownership or database unavailable' }, { status: 503 });
  return response;
};
}
const { getServerLifecyclePhase } = await import('../upstream/omniroute/src/lib/serverLifecycle.ts');
const handle = app.getRequestHandler();
const server = createServer(fencedHttpHandler(owner, handle));
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
if (!fullBackend) {
  const { register } = await import('../upstream/omniroute/src/instrumentation.ts');
  await register();
} else if (!owner.boot) throw new Error('Official backend failed to register HA startup');
process.send?.({ port: server.address().port });
process.on('message', async message => {
  try {
    let value;
    if (message.operation === 'state') value = { active: owner.ready(), epoch:owner.owner.epoch,redis:owner.cacheBus.status(),quotaRefresh:owner.quotaRefresh, phase: getServerLifecyclePhase(), fault: db.compatibilityFault, ownershipError:owner.owner.lastError, maintenanceStarted: owner.maintenanceStarted === true, recoveryStarted:owner.recoveryStarted===true,tokenHealth:owner.tokenHealth,memoryDecay:owner.memoryDecay,usageRetention:owner.usageRetention,freeProxySync:owner.freeProxySync.state, cachedSettings: await cache.getCachedSettings(), cachedProviders: await cache.getCachedProviderConnections() };
    else if(message.operation==='providerLimitsState')value=owner.providerLimits;
    else if(message.operation==='loseKiroPublicationReply'){
      owner.assertOwner();if(!fullBackend||process.env.HA_TEST_OAUTH_REDIRECT!=='1')throw new Error('Synthetic OAuth fixture required');
      const credential=db.credential.bind(db);
      db.credential=(action,values)=>{
        const result=credential(action,values);
        if(action==='kiroStage'&&values.stage==='registered'){
          db.credential=credential;
          throw Object.assign(new Error('Synthetic lost Kiro registration publication acknowledgement'),{code:'HA_DB_OUTCOME_UNKNOWN'});
        }
        return result;
      };value=true;
    }
    else if(message.operation==='providerLimitsSync')value=await(await import('../adapter/activeRuntime.mjs')).syncProviderLimits(owner);
    else if(message.operation==='quotaRecoveryState')value=owner.quotaRecovery;
    else if(message.operation==='reasoningCleanupState')value=owner.reasoningCleanup;
    else if(message.operation==='reasoningCleanup')value=await(await import('../adapter/activeRuntime.mjs')).cleanupReasoning(owner);
    else if(message.operation==='ageReasoning')value=db.prepare('UPDATE reasoning_cache SET expires_at=? WHERE tool_call_id=?').run(1,'promotion-reasoning');
    else if(message.operation==='quotaRecovery')value=await(await import('../adapter/activeRuntime.mjs')).runQuotaRecovery(owner);
    else if(message.operation==='ageQuotaRecovery')value=db.prepare("UPDATE quota_recovery_tickets SET next_attempt_at=? WHERE id=? AND state='pending'").run('2000-01-01T00:00:00.000Z',message.ticketId);
    else if(message.operation==='keyMetadata')value=(await keys.getApiKeyMetadata(message.key))!==null;
    else if(message.operation==='quotaRefreshStart')value=(await import('../upstream/omniroute/src/domain/quotaCache.ts')).startBackgroundRefresh();
    else if(message.operation==='ageQuotaCache'){
      const {getQuotaCacheState}=await import('../upstream/omniroute/src/domain/quotaCacheState.ts');const entry=getQuotaCacheState().cache.get(message.connectionId);
      if(!entry)throw new Error('Missing original shared quota cache entry');entry.fetchedAt=0;value=true;
    }
    else if(message.operation==='usageRetention')value=await globalThis.__omnirouteRetention.purgeBefore(message.before);
    else if (message.operation === 'maintenance') value = await maintainCallLogs(db);
    else if (message.operation === 'recovery') value=await recoverConnections(owner);
    else if(message.operation==='memoryDecay')value=await runMemoryDecay(owner);
    else if(message.operation==='ageMemory')value=db.prepare('UPDATE memories SET created_at=?,last_accessed_at=NULL,access_count=0 WHERE id=?').run('2000-01-01T00:00:00.000Z',message.memoryId);
    else if(message.operation==='ageFreeProxySync')value=await(await import('../upstream/omniroute/src/lib/db/freeProxies.ts')).recordFreeProxySync('2000-01-01T00:00:00.000Z');
    else if(message.operation==='pricing')value=await settings.getPricingForModel(message.providerId,message.model);
    else if(message.operation==='pricingUpdate')value=await settings.updatePricing({[message.providerId]:{[message.model]:message.value}});
    else if (message.operation === 'settings') value = await settings.updateSettings(message.value);
    else if (message.operation === 'revoke') value = await keys.revokeApiKey(message.keyId);
    else if (message.operation === 'provider') value = await providers.updateProviderConnection(message.providerId, message.value);
    else if(message.operation==='proxyConfig')value=await settings.setProxyForLevel('key',message.providerId,message.value);
    else if(message.operation==='resolveProxy')value=await settings.resolveProxyForConnection(message.providerId);
    else if (message.operation === 'write') value = db.prepare("INSERT OR REPLACE INTO key_value(namespace,key,value) VALUES ('ha_test','stale',?)").run(message.value);
    else throw new Error('Unknown test operation');
    process.send?.({ id: message.id, value });
  } catch (error) { process.send?.({ id: message.id, error: error.code || error.message }); }
});
async function stop() {
  server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  await app.close(); await db.close(); process.exit(0);
}
process.once('SIGTERM', () => void stop());
process.once('disconnect', () => void stop());
