import { createHash } from 'node:crypto';

export const overlayPins = Object.freeze({
  'open-sse/executors/github.ts':'56eeb18511b53349c70a6ce071d446bc4eff7e796e7eabfab13dca927ef6ca06',
  'open-sse/services/providerConnectionPool.ts':'8d3498057a0a3105a2505ac3298287239f5cbb066c63b8be8a305edb6a023c6b',
  'src/lib/memory/embedding/index.ts':'2bc00876be77d4c97f568629042b226edcd41911c0fe9ef52c0bdbd85a6af81c',
  'src/app/api/memory/rerank-providers/route.ts':'01b4a2d7aa73ee24dedd2cb2c0dd701d2daba746bf9cb22c154e609592e172c3',
  'src/lib/db/databaseSettings.ts':'cc0a6ad240dc5d6852871f119d879b9aebb82eef54317a6f3f370ce994c75ba7',
  'src/lib/db/stats.ts':'907e2cfddfa59fd2094340e582c7e7e53a813a6e4f2fa63d1ed14dce2b9d2b96',
  'src/app/api/storage/health/route.ts':'01ab837520582b4ec9dd38370702e949ebc5c426e33e551957e586abee080041',
  'src/app/(dashboard)/dashboard/settings/components/SystemStorageTab.tsx':'08d215d115d4d368403157a897ffcbd7a3ce2060e4f4321458fd32e64d3cf4ba',
  'src/lib/guardrails/visionBridgeRouter.ts':'6a211ab17054f2a4c380fa37d5cbfe1b1381a18b7ad767f3796c201f7bddf733',
  'src/domain/quotaCache.ts':'d45553311209580dbc48660a15df486381751cf86bce4f3c72bb80d4f28c034a',
  'open-sse/handlers/chatCore/streamingTail.ts':'c4ea166a6aa58ed38c87e02238367d973a54878e8d75c3c1dc324c3eb9583010',
  'src/lib/freeProxyProviders/syncCycle.ts':'4aaa42817c3eb3043568cd4831bec58c453a32a8a7efa550bbde0514f1876797',
  'src/lib/system/headless.ts':'126aec93ac7fa513abe2518e8d580b8613509e375b504b4864814086bfc55d8b',
  'src/lib/compliance/index.ts':'d8a2607d81db1d717286868a8114571af8308a4d6e0700e93f223d320aec0c30',
  'src/lib/db/cleanup.ts':'4d79e53865af0f3eb0104d143ee9b12b6d0c07ae88349420483637778be51cbf',
  'src/lib/usage/aggregateHistory.ts':'69551a1787e53c3964e7e290be7710c396e2e484ba1b017feae0878473ecad8e',
  'open-sse/services/tokenRefresh.ts':'fa9224a989431d304d0c62b2c7dc1935c5f9883a8b8017e31b53080bd5adab4c',
  'src/app/api/oauth/codex/import/route.ts':'12ab61f1a3a7b06947df9b12ab53ad98ff8a4a7a323829b4f3cb6191167c8e3c',
  'src/lib/db/settings.ts':'ac74c9b6cd397f7b3b569a039cc9b4166eb0215c602542a118d4bb0fe8ebd77b',
  'open-sse/services/tokenLimitCounter.ts': '5290dd27698a8a2915e8ad118581fc47fb50b7c19ebfc45390d9455a6e5a9d32',
  'src/lib/quota/enforce.ts': '0ff5e2a40e2f170dd0dd33e4ca26bd41ba5ef4d62d6d3ef76929ac8dcfc979ef',
  'open-sse/handlers/chatCore.ts': 'd51f6d7b5a35028307138188fe0661c3495769f42e57e7d494b114cf33e91542',
  'open-sse/utils/proxyFetch.ts': 'db3772ac98d1e08977b26bcae040ea3257f0f96e0c6cd5dd769e6545dae67b93',
  'open-sse/executors/dispatchPin.ts': '4e2a9309ef5fe2dd4da8caec9298e159e4aa22206f6a64d5c4816fde52cc1e26',
  'src/domain/costRules.ts': '804bdc9d42022d2069dd0ec27ecc22c62aff03dfd5d7617275fbce05f63603e3',
  'src/domain/keyQuota.ts': '960f81fe342e64cef01a46111e3195f3b2c0a7adeabec9a66258b6cb2b0887e6',
  'src/lib/gamification/events.ts': '09342b505798b148e1c7405b6788df2c5fa98c6459ce16b287b3ed76c7f68a20',
  'src/lib/db/gamification.ts':'c0322e8b697eb5987054cb984b8885c8ffd15618fc19f62c1e67a30c433867ee',
  'src/lib/usage/callLogArtifacts.ts': 'fdc1c9398238f093f075ba18cded38d14bc911e61375592381fd1701cf1da79b',
  'src/lib/usage/callLogArtifactWriter.ts': 'b1de2d15e51ee9d59d7b98e0098a556eee39c62931a5c8d39a41fb8ae5d9b63c',
  'src/lib/usage/callLogDetailResolver.ts': 'a77792b308fee539e7c9835fce02d832bc4fc7f4e841f34c44ed6669cfd0e7d7',
  'src/lib/spend/batchWriter.ts': 'c08567cee3278500d327f2437ef4fd1fba11436bc5c683bb371d048ae1f6f3d3',
  'src/lib/memory/settings.ts': '97c235b9c2a0f46ac7a0a70cea7ce3f79239653da023b100566411e823bdae06',
  'src/lib/db/readCache.ts': '97cb59fb5767005900c575e8f6db450c3b94abaa7a7058671dda1717da223448',
  'src/lib/semanticCache.ts': '618f21142cd0d8ff75ed5b295992ef9955864e76d5d9faedbf43dc2bb7c9102b',
  'src/shared/utils/rateLimiter.ts': 'e111652745454f820281c17c0279b3ea803606c190f9e08a219f5e383468ed23',
  'src/shared/utils/apiKeyPolicy.ts': 'ce87bf62fcb21fd4af64d5d7af83808432da7e6c7c8093ca396f1f5903d8274e',
  'src/lib/memory/retrieval.ts': '21de64d2e9d21066dea83da118946fcd20605ea2e0ea727d7f4a01dca764de1e',
  'src/shared/schemas/memory.ts': '69c459e69bd6a3ea8b813a61e0825b58f1900bbfb81708406982c9fe01a1e2df',
  'src/instrumentation-node.ts': 'c6b43fd71367ae499cc12ff33b880b06f4879cd24d474a17d73187b4d4f4a5f0',
  'src/lib/usage/migrations.ts': 'ac8b5d6b64a6ea3d35133559f2297dd6187c93719d8e2ceb6ecd2cebf031ca60',
  'src/lib/db/core.ts': '097ef3b46440d89f22ff2b483b4bc591df8165faea256f02e3aada22ef891ed4',
  'src/lib/db/apiKeys.ts': '4e9bc2be404d6c74e1fbf9fc1d62b03853a84eead453ce70e2fc8cadd33673eb',
  'src/lib/db/adapters/runtimeRequire.ts': '3c37b1e0d788aa53bf89979aed6c2c3c6810da67f558bf85cfa37a77bf38a8e7',
  'open-sse/services/reasoningCache.ts': '4cf1e22bd793444e46a6169bbb60838216462c12d6010a2a293072ea5a3d8b4e',
  'src/shared/utils/machineId.ts': 'f685ef78521ae8b967a2f948cc59681e831b442cf10bef8fbf7b5b6dad541d7e',
  'src/lib/memory/store.ts': '3a0d7889477d1db1f6205a4f6a9850d78e36cc320bcb4762fe9d129b4281ed40',
  'src/lib/db/providers.ts': '68eee6bd6d9938f9c6cead176e482cc987485782d4a74892e5c00ca0d5fb8f62',
  'src/lib/memory/vectorStore.ts': '61fb7a26dc8628cb034d16c2a5397dc29a0abd7f2247ab65322bb7eb00d9bb77',
  'src/lib/memory/reindex.ts': 'e8b0151d36ed0c87874764243f0a6b5f576457bd0e64fa86cea4bea3b169461e',
  'src/lib/db/providers/migrations.ts': 'da2cb0c1f1689807439b194e2dad586d5ce9798106e6aff37b3b64d31f405949',
  'src/lib/db/apiKeys/secretAtRest.ts': '82dce1b987cc5926934172878e4f30779cb063e1a10b610fbb006e734c779723',
  'src/lib/quota/storeFactory.ts': '8789d61c294b2a36cf50095de0933bd39af81f2d096d8a872b904fa69a3cf847',
  'src/lib/quota/sqliteQuotaStore.ts': '82e7efcd0e12676da1bdbfbce4326990b7f7998b0d4dc1f074e8aad8e3a0a7b1',
});
function replaceOnce(source, before, after) {
  if (source.split(before).length !== 2) throw new Error('Upstream overlay anchor mismatch');
  return source.replace(before, after);
}

// Applied in memory by the source runner; the same transform can be used by a build.
// No upstream file is written. Any upstream drift requires explicit revalidation.
export function overlay(relativePath, source) {
  if (!(relativePath in overlayPins)) return source;
  if (createHash('sha256').update(source).digest('hex') !== overlayPins[relativePath]) throw new Error(`Upstream digest mismatch: ${relativePath}`);
  if(relativePath==='open-sse/services/providerConnectionPool.ts') {
    source=replaceOnce(source,'export async function getProviderSearchPool(provider: string): Promise<string[]> {',
      'export async function getProviderSearchPool(provider: string, providerNodesSnapshot?: unknown[]): Promise<string[]> {');
    source=replaceOnce(source,'    const providerNodes = await getCachedProviderNodes();','    const providerNodes = providerNodesSnapshot ?? await getCachedProviderNodes();');
  }else if(relativePath==='src/lib/memory/embedding/index.ts') {
    source=replaceOnce(source,'export async function listEmbeddingProviders(): Promise<EmbeddingProviderListing[]> {',
      'export async function listEmbeddingProviders(): Promise<EmbeddingProviderListing[]> {\n  const haListingLookup = await globalThis.__omnirouteCredentialRuntime.listingLookup(getProviderCredentials);');
    source=replaceOnce(source,'await getProviderCredentials(providerId)','await haListingLookup(providerId)');
    source=replaceOnce(source,'await getProviderCredentials(id)','await haListingLookup(id)');
  }else if(relativePath==='src/app/api/memory/rerank-providers/route.ts') {
    source=replaceOnce(source,'    const curated = [];','    const haListingLookup = await globalThis.__omnirouteCredentialRuntime.listingLookup(getProviderCredentials);\n    const curated = [];');
    source=replaceOnce(source,'await getProviderCredentials(providerId)','await haListingLookup(providerId)');
  }else if(relativePath==='src/lib/db/stats.ts') {
    const start=source.indexOf('  const pageSize =',source.indexOf('export function getDatabaseStats('));
    if(start<0||!source.trimEnd().endsWith('}'))throw new Error('Database statistics overlay anchor mismatch');
    source=source.slice(0,start)+'  return db.storageStatus() as unknown as DatabaseStats;\n}\n';
  }else if(relativePath==='src/lib/db/databaseSettings.ts') {
    const start=source.indexOf('  const dbStats =',source.indexOf('export function getDatabaseSettings(')),end=source.indexOf('\nexport function updateDatabaseSettings(',start);
    if(start<0||end<0)throw new Error('Database settings overlay anchor mismatch');
    source=source.slice(0,start)+`  const storage = getDbInstance().storageStatus();
  return {
    ...getUserDatabaseSettings(), driver: "mongodb",
    location: { databasePath: storage.namespace, dataDir: DATA_DIR, walSizeBytes: null, schemaVersion: getSchemaVersion() },
    stats: { databaseSizeBytes: storage.totalSize, pageCount: null, freelistCount: null,
      lastVacuumAt: null, lastOptimizationAt: null, integrityCheck: null, lastIntegrityCheckAt: null,
      autoVacuumDrift: null, lastReclaimedPages: null },
    storage,
  } as unknown as DatabaseSettings;
}
`+source.slice(end);
    source=replaceOnce(source,'  const nextSettings = getUserDatabaseSettings();',
      '  if (updates.optimization !== undefined || updates.backup?.autoBackupEnabled === true || (updates.backup?.autoBackupFrequency && updates.backup.autoBackupFrequency !== "never")) throw Object.assign(new Error("SQLite optimization/automatic backup is not available on MongoDB; use the HA operations procedure"), {code:"HA_STORAGE_UNSUPPORTED"});\n  const nextSettings = getUserDatabaseSettings();');
    source=replaceOnce(source,'  let status: "ok" | "error";',
      '  throw Object.assign(new Error("SQLite quick_check is not applicable to MongoDB; use MongoDB validation and the recovery procedure"), {code:"HA_STORAGE_UNSUPPORTED"});\n  let status: "ok" | "error";');
    source=replaceOnce(source,'function getSchemaVersion(): number {','function getSchemaVersion(): number {\n  if (!getDbInstance().prepare("SELECT name FROM sqlite_master WHERE type=\'table\' AND name=?").get("_omniroute_migrations")) return 0;');
  }else if(relativePath==='src/app/api/storage/health/route.ts') {
    const start=source.indexOf('    const dbFilePath ='),end=source.indexOf('\n    return NextResponse.json({',start);
    if(start<0||end<0)throw new Error('Storage health overlay anchor mismatch');
    source=source.slice(0,start)+'    const storage = globalThis.__omnirouteMongoAdapter.storageStatus();\n    const displayPath = storage.namespace, sizeBytes = storage.totalSize;\n    const lastBackupAt = null, backupCount = null; // External MongoDB backup inventory is not inferred.\n    const homeDir = "";\n'+source.slice(end);
    source=replaceOnce(source,'      driver: "sqlite",','      driver: "mongodb",\n      backupManagedExternally: true,');
    source=replaceOnce(source,'dataDir: dataDir.startsWith(homeDir) ? "~" + dataDir.slice(homeDir.length) : dataDir,','dataDir,');
  }else if(relativePath==='src/app/(dashboard)/dashboard/settings/components/SystemStorageTab.tsx') {
    source=replaceOnce(source,'dbSettings.stats.pageCount.toLocaleString()','(dbSettings.stats.pageCount?.toLocaleString() ?? "N/A")');
    source=replaceOnce(source,'dbSettings.stats.freelistCount.toLocaleString()','(dbSettings.stats.freelistCount?.toLocaleString() ?? "N/A")');
    source=replaceOnce(source,'  const renderOptimizationSettings = () => {','  const renderOptimizationSettings = () => {\n    if (dbSettings?.driver === "mongodb") return <p className="mt-6 text-sm">MongoDB storage maintenance and backup are managed through the HA operations procedure. SQLite page, cache and VACUUM settings are not applicable.</p>;');
    source=replaceOnce(source,'            loading={manualVacuumLoading}','            disabled={dbSettings?.driver === "mongodb"}\n            loading={manualVacuumLoading}');
    source=replaceOnce(source,'{t("allDataLocal")}','{dbSettings?.driver === "mongodb" ? "Shared MongoDB persistence; each host has a private runtime directory." : t("allDataLocal")}');
    source=replaceOnce(source,'            Database backups are stored on this server in the db_backups folder under its data',
      '            {dbSettings?.driver === "mongodb" ? "MongoDB backup, restore and validation use the HA operations procedure. SQLite automatic backups and local backup-file controls do not protect this database." : <>Database backups are stored on this server in the db_backups folder under its data');
    source=replaceOnce(source,'            and can upload credentials to a configured remote destination.',
      '            and can upload credentials to a configured remote destination.</>}');
  }else if(relativePath==='src/lib/guardrails/visionBridgeRouter.ts') {
    source='import { createModelCapabilityResolutionSnapshot } from "@/lib/modelCapabilityResolutionSnapshot";\n'+source;
    // Reuse the original uncached, per-call bulk view. Thousands of synchronous
    // remote lookups otherwise block the request loop and ownership heartbeat.
    source=replaceOnce(source,'  const candidatesByProvider = await Promise.all(',
      '  const haCapabilitySnapshot = createModelCapabilityResolutionSnapshot();\n  const candidatesByProvider = await Promise.all(');
    source=replaceOnce(source,'getResolvedModelCapabilities(fullModelId).supportsVision === true',
      'getResolvedModelCapabilities(fullModelId, undefined, haCapabilitySnapshot).supportsVision === true');
  } else if(relativePath==='src/domain/quotaCache.ts') {
    source=replaceOnce(source,'async function refreshEntry(entry: QuotaCacheEntry) {',
      'async function refreshEntry(entry: QuotaCacheEntry) {\n  globalThis.__omnirouteActiveRuntime.assertOwner();');
    source=replaceOnce(source,'const usage = await runWithProxyContext(proxyInfo?.proxy || null, () => getUsage(connection));',
      'const usage = await runWithProxyContext(proxyInfo?.proxy || null, () => { globalThis.__omnirouteActiveRuntime.assertOwner(); return getUsage(connection); });\n    globalThis.__omnirouteActiveRuntime.assertOwner();');
    source=replaceOnce(source,'async function backgroundRefreshTick() {',
      'async function backgroundRefreshTick() {\n  if (!globalThis.__omnirouteActiveRuntime?.ready()) return;');
    source=replaceOnce(source,'export function startBackgroundRefresh() {',
      'export function startBackgroundRefresh() {\n  globalThis.__omnirouteActiveRuntime.assertOwner();');
  } else if(relativePath==='open-sse/handlers/chatCore/streamingTail.ts') {
    source=replaceOnce(source,'  const onStreamComplete = ({','  const onStreamComplete = globalThis.__omnirouteHardQuota.bind(({');
    source=replaceOnce(source,'  };\n\n  const streamFailureFinalizers =', '  });\n\n  const streamFailureFinalizers =');
  } else if(relativePath==='src/lib/freeProxyProviders/syncCycle.ts') {
    source=replaceOnce(source,'): Promise<FreeProxySyncCycleResult> {',
      '): Promise<FreeProxySyncCycleResult> {\n  return globalThis.__omnirouteFreeProxySync.run(providers, haOriginalSyncCycle);\n}\nasync function haOriginalSyncCycle(providers: FreeProxyProvider[]): Promise<FreeProxySyncCycleResult> {');
  } else if(relativePath==='src/lib/system/headless.ts') {
    // Keep optional boot jobs disabled, but preserve the actual UI. The original
    // proxy still applies the complete authentication/authorization pipeline.
    source=replaceOnce(source,'  if (!isHeadless(env) || !isHeadlessDisabledPath(pathname)) return null;\n  return Response.json({ error: HEADLESS_DASHBOARD_DISABLED_ERROR }, { status: 404 });',
      '  return null; // HA job ownership is independent of dashboard availability.');
  } else if(relativePath==='src/lib/compliance/index.ts') {
    source=replaceOnce(source,'export async function cleanupExpiredLogs() {','export async function cleanupExpiredLogs() {\n  globalThis.__omnirouteActiveRuntime.assertOwner();');
    source=replaceOnce(source,`  try {
    const r1 = db.prepare("DELETE FROM usage_history WHERE timestamp < ?").run(usageCutoff);
    deletedUsage = r1.changes;
  } catch {
    /* table may not exist */
  }`, `  try {
    const archive = await globalThis.__omnirouteRetention.purgeBefore(usageCutoff.slice(0, 10));
    if (archive.errors) throw new Error("Usage archive deferred; raw history retained");
    deletedUsage = archive.deletedRows;
  } catch (error) {
    console.warn("[HA] Startup usage retention deferred:", error?.code || "HA_RETENTION_FAILED");
  }`);
  } else if(relativePath==='src/lib/db/cleanup.ts') {
    source=replaceOnce(source,'export async function cleanupUsageHistory(): Promise<CleanupResult> {','export async function cleanupUsageHistory(): Promise<CleanupResult> {\n  return globalThis.__omnirouteRetention.cleanupUsageHistory();');
  } else if(relativePath==='src/lib/usage/aggregateHistory.ts') {
    source=replaceOnce(source,'export async function rollupUsageHistoryBeforeDate(beforeDate: string): Promise<AggregationResult> {',
      'export async function rollupUsageHistoryBeforeDate(beforeDate: string): Promise<AggregationResult> {\n  return globalThis.__omnirouteRetention.run(() => haRollupUsageHistoryBeforeDate(beforeDate));\n}\nasync function haRollupUsageHistoryBeforeDate(beforeDate: string): Promise<AggregationResult> {');
    source=replaceOnce(source,'    const archivedRows = Array.from(','    globalThis.__omnirouteRetention.prices(pricedRows);\n    const archivedRows = Array.from(');
    source=replaceOnce(source,'      return changes;','      globalThis.__omnirouteRetention.finish(beforeDate);\n      return changes;');
  } else if(relativePath==='open-sse/executors/github.ts') {
    // Model listing/reactive refresh also reaches this executor, bypassing the
    // central helper. Reuse that helper so an App grant is durable before the
    // optional Copilot sub-token request; never issue a second raw exchange.
    source=replaceOnce(source,'  async refreshGitHubToken(refreshToken, log) {',
      '  async refreshGitHubToken(refreshToken, log) {\n    globalThis.__omnirouteCredentialRuntime.requireExchange("github");');
    source=replaceOnce(source,'      const githubTokens = await this.refreshGitHubToken(credentials.refreshToken, log);',
      '      const { getAccessToken } = await import("../services/tokenRefresh.ts");\n      const githubTokens = await getAccessToken("github", credentials, log);');
  } else if(relativePath==='open-sse/services/tokenRefresh.ts') {
    for(const [name,file,provider] of [
      ['refreshOpenferenceToken','openference','openference'],['refreshClaudeOAuthToken','claudeOAuth','claude'],['refreshCodexToken','codex','codex'],
      ['refreshClineToken','cline','cline'],['refreshKimiCodingToken','kimiCoding','kimi-coding'],['refreshGitLabDuoToken','gitlabDuo','gitlab-duo'],['refreshKiroToken','kiro','kiro'],
      ['refreshGitHubToken','github','github'],
    ])source=replaceOnce(source,`import { ${name} } from "./tokenRefresh/providers/${file}.ts";`,
      `import { ${name} as rawHa_${name} } from "./tokenRefresh/providers/${file}.ts";\nconst ${name} = async (...args) => { globalThis.__omnirouteCredentialRuntime.requireExchange("${provider}"); return rawHa_${name}(...args); };`);
    source=replaceOnce(source,'export function refreshAccessToken(provider, token, creds, log, proxy: unknown = null) {',
      'export function refreshAccessToken(provider, token, creds, log, proxy: unknown = null) {\n  globalThis.__omnirouteCredentialRuntime.validateDirect(provider);');
    source=replaceOnce(source,'  if (isPassiveCodex(provider)) return resolvePassiveCodex(credentials);','  globalThis.__omnirouteCredentialRuntime.validateCall(provider, credentials);\n  if (isPassiveCodex(provider)) return resolvePassiveCodex(credentials);');
    source=replaceOnce(source,'    const work = (async () => {','    const work = globalThis.__omnirouteCredentialRuntime.run(provider, credentials, async () => {');
    source=replaceOnce(source,'    })();\n    let mutexTimer:','    });\n    let mutexTimer:');
    source=replaceOnce(source,'  const result = await _getAccessTokenInternal(provider, credentials, log, proxyConfig);','  const result = await globalThis.__omnirouteCredentialRuntime.exchange(provider, credentials, () => _getAccessTokenInternal(provider, credentials, log, proxyConfig));');
  } else if(relativePath==='src/app/api/oauth/codex/import/route.ts') {
    source=replaceOnce(source,`    const refreshError = await validateCodexRefreshToken(norm.payload);
    if (refreshError) {
      failed += 1;
      results.push({ index: i, ok: false, error: refreshError });
      continue;
    }
`, '');
    source=replaceOnce(source,'      const existing = await getProviderConnections({ provider: "codex", authType: "oauth" });',
      '      const conn = await globalThis.__omnirouteCredentialRuntime.importCodex(norm.payload, async (validatedPayload) => {\n      const existing = await getProviderConnections({ provider: "codex", authType: "oauth" });');
    source=replaceOnce(source,'        norm.payload,\n        existing as Array<Record<string, unknown>>',
      '        validatedPayload,\n        existing as Array<Record<string, unknown>>');
    source=replaceOnce(source,'      const conn = await createProviderConnection(payload as Record<string, unknown>);',
      '      return createProviderConnection(payload as Record<string, unknown>);\n      }, flat.records[i]);');
  } else if(relativePath==='src/lib/db/settings.ts') {
    source=replaceOnce(source,'  proxyResolutionCache.clear();\n}',
      '  proxyResolutionCache.clear();\n}\n// Both source-loaded and compiled module copies must observe committed writes.\n(globalThis.__omnirouteProxyInvalidators ||= new Set()).add(bumpProxyConfigGeneration);');
  } else if (relativePath === 'open-sse/services/tokenLimitCounter.ts') {
    source = replaceOnce(source, '  if (!apiKeyId) return;\n  const delta = Number.isFinite(tokens)',
      '  if (!apiKeyId || globalThis.__omnirouteHardQuota?.ownsKey(apiKeyId)) return;\n  const delta = Number.isFinite(tokens)');
  } else if (relativePath === 'open-sse/handlers/chatCore.ts') {
    source = replaceOnce(source, '  return withResilienceActionsContext([args], (forwarded) => handleChatCoreInner(forwarded));',
      '  try { return await globalThis.__omnirouteHardQuota.run(args, (bounded) => withResilienceActionsContext([bounded], (forwarded) => handleChatCoreInner(forwarded))); }\n  catch (error) { if (String(error?.code).startsWith("HA_QUOTA") || String(error?.code).startsWith("HA_RESERVATION")) return createErrorResult(error.code === "HA_QUOTA_EXCEEDED" ? 429 : 503, error.message, null, "quota_share_blocked"); throw error; }');
  } else if (relativePath === 'open-sse/executors/dispatchPin.ts') {
    source = replaceOnce(source, '  return fetch(url, { ...init, dispatcher: getGuardedDispatcher(guard) } as RequestInit);',
      '  const dispatcher = getGuardedDispatcher(guard);\n  globalThis.__omnirouteHardQuota?.allowDispatcher(dispatcher);\n  return fetch(url, { ...init, dispatcher } as RequestInit);');
  } else if (relativePath === 'open-sse/utils/proxyFetch.ts') {
    source = replaceOnce(source, '  // Drop invisible characters (ZWSP, BOM, stray controls) a pasted key can carry;',
      '  if (globalThis.__omnirouteHardQuota?.current()) {\n    if ((options.dispatcher && !globalThis.__omnirouteHardQuota.isAllowedDispatcher(options.dispatcher)) || isTlsFingerprintEnabled() || resolveProxyForRequest(getTargetUrl(input)).proxyUrl) throw Object.assign(new Error("Bounded quotas require direct egress without a custom dispatcher or TLS fingerprinting"), { code: "HA_RESERVATION_REQUIRED" });\n    return globalThis.__omnirouteHardQuota.fetch(input, options);\n  }\n  // Drop invisible characters (ZWSP, BOM, stray controls) a pasted key can carry;');
  } else if (relativePath === 'src/domain/costRules.ts') {
    source = 'import { getLoggedInputTokens, getLoggedOutputTokens, getReasoningTokens } from "@/lib/usage/tokenAccounting";\n' + source;
    source = replaceOnce(source, 'export function recordCost(apiKeyId: string, cost: number, details?: RecordCostDetails): void {',
      'export function recordCost(apiKeyId: string, cost: number, details?: RecordCostDetails): void {\n  if (globalThis.__omnirouteHardQuota?.ownsStreamCost(apiKeyId)) return; // Raw terminal receipt owns managed SSE accounting.');
    source = replaceOnce(source, '  if (!apiKeyInfo?.id) return;',
      '  if (!apiKeyInfo?.id) return;\n  if (globalThis.__omnirouteHardQuota?.observe(apiKeyInfo.id, estimatedCost, getLoggedInputTokens(context.tokens), getLoggedOutputTokens(context.tokens), context.tokens, success, unpriced, getReasoningTokens(context.tokens), context.requestId, context.serviceTier)) return;');
  } else if (relativePath === 'src/domain/keyQuota.ts') {
    source = replaceOnce(source, '  const nowMs = (deps.now ?? Date.now)();\n  try {\n    if (billableTokens > 0)',
      '  if (globalThis.__omnirouteHardQuota?.current()) return; // Durable settlement owns these counters.\n  const nowMs = (deps.now ?? Date.now)();\n  try {\n    if (billableTokens > 0)');
  } else if (relativePath === 'src/lib/gamification/events.ts') {
    source = replaceOnce(source, '  try {\n    // A verified Radar supporter',
      '  try {\n    globalThis.__omnirouteActiveRuntime.assertOwner();\n    // A verified Radar supporter');
  } else if(relativePath==='src/lib/db/gamification.ts') {
    source=replaceOnce(source,'export function addXp(apiKeyId: string, action: string, amount: number, metadata?: string): void {',
      'export function addXp(apiKeyId: string, action: string, amount: number, metadata?: string): void {\n  return getDbInstance().transaction(() => {');
    source=replaceOnce(source,'    .run(apiKeyId, amount, calculateLevel(amount));\n}',
      '    .run(apiKeyId, amount, calculateLevel(amount));\n  })();\n}');
  } else if (relativePath === 'src/lib/usage/callLogArtifactWriter.ts') {
    source = 'import { writeCallArtifact } from "./callLogArtifacts";\n' + source;
    source = replaceOnce(source, '): Promise<CallLogArtifactWriteResult | null> {',
      '): Promise<CallLogArtifactWriteResult | null> {\n  return Promise.resolve(writeCallArtifact(artifact));');
  } else if (relativePath === 'src/lib/usage/callLogDetailResolver.ts') {
    source = replaceOnce(source, '    const payload = JSON.parse(await io.readFile(path.join(baseDir, relPath))) as DetailPayload;',
      '    const stored = db.prepare("SELECT payload FROM ha_call_log_artifacts WHERE path=?").get(relPath);\n    if (!stored) throw Object.assign(new Error("Artifact missing"), { code: "ENOENT" });\n    const payload = JSON.parse(stored.payload) as DetailPayload;');
    source = replaceOnce(source, '  if (!baseDir || !entry.timestamp) return null;',
      '  // Historical host-local JSON is not authoritative in HA mode.\n  return null;\n  if (!baseDir || !entry.timestamp) return null;');
  } else if (relativePath === 'src/lib/usage/callLogArtifacts.ts') {
    const start = source.indexOf('export function writeCallArtifact(');
    if (start < 0) throw new Error('Call artifact overlay anchor changed');
    // Preserve upstream serialization, truncation and checksums. Only the
    // persistence boundary changes; no local artifact worker or file fallback.
    source = source.slice(0, start) + `
function artifactDb() {
  const db = globalThis.__omnirouteMongoAdapter;
  if (!db) throw new Error("MongoDB artifact adapter is not initialized");
  return db;
}
export function writeCallArtifact(artifact: CallLogArtifact,
  relativePath = buildArtifactRelativePath(artifact.summary.timestamp, artifact.summary.id)): CallLogArtifactWriteResult | null {
  if (isBuildPhase) return null;
  if (!/^\\d{4}-\\d{2}-\\d{2}\\/[^/\\\\\\x00]+\\.json$/.test(relativePath)) throw new Error("Invalid artifact key");
  const serialized = serializeArtifactForStorage(artifact);
  const db = artifactDb();
  db.prepare("INSERT OR REPLACE INTO ha_call_log_artifacts(path,payload,mtime_ms) VALUES (?,?,?)")
    .run(relativePath, serialized, db.serverTime());
  return { relPath: relativePath, sizeBytes: Buffer.byteLength(serialized), sha256: computeArtifactChecksum(serialized) };
}
export function readCallArtifact(relativePath: string | null): { artifact: CallLogArtifact | null; state: "ready" | "missing" | "corrupt" } {
  if (!relativePath) return { artifact: null, state: "missing" };
  const row = artifactDb().prepare("SELECT payload FROM ha_call_log_artifacts WHERE path=?").get(relativePath);
  if (!row) return { artifact: null, state: "missing" };
  try { return { artifact: JSON.parse(row.payload), state: "ready" }; }
  catch { return { artifact: null, state: "corrupt" }; }
}
export function deleteCallArtifact(relativePath: string | null, _baseDir: string | null = getCallLogsDir()): boolean {
  return !!relativePath && artifactDb().prepare("DELETE FROM ha_call_log_artifacts WHERE path=?").run(relativePath).changes > 0;
}
export function cleanupEmptyCallLogDirs(_baseDir: string | null = getCallLogsDir()) {
  // MongoDB has no artifact directories to prune.
}
export function listCallLogArtifactFiles(_baseDir: string | null = getCallLogsDir()) {
  return artifactDb().prepare("SELECT path,mtime_ms FROM ha_call_log_artifacts ORDER BY mtime_ms DESC").all()
    .map(row => ({ relativePath: row.path, absPath: "mongodb:artifact/" + encodeURIComponent(row.path), mtimeMs: row.mtime_ms }));
}
export function purgeCallLogArtifactDirectory(_baseDir: string | null = getCallLogsDir()): PurgeCallLogArtifactDirectoryResult {
  return { deletedArtifacts: artifactDb().prepare("DELETE FROM ha_call_log_artifacts").run().changes, errors: 0 };
}
`;
  } else if (relativePath === 'src/lib/spend/batchWriter.ts') {
    // An active process may disappear before the normal 60-second flush.
    // Keep the original normalization and SQL writer, but commit each charge
    // synchronously to MongoDB instead of acknowledging an in-memory buffer.
    const start = source.indexOf('  increment(apiKeyId: string, cost: number, timestamp = Date.now()) {');
    const end = source.indexOf('\n  getBufferedEntries(', start);
    if (start < 0 || end < start) throw new Error('Spend writer overlay anchor mismatch');
    source = source.slice(0, start) + `  increment(apiKeyId: string, cost: number, timestamp = Date.now()) {
    if (!globalThis.__omnirouteMongoAdapter) throw new Error("MongoDB spend adapter is not initialized");
    const entry = normalizeEntry({ apiKeyId, cost, timestamp });
    if (entry) batchSaveCostEntries([entry]);
  }
` + source.slice(end);
  } else if (relativePath === 'src/lib/db/core.ts') {
    const guard = '\n  if (!globalThis.__omnirouteMongoAdapter) throw new Error("MongoDB adapter is not initialized");\n  setDb(globalThis.__omnirouteMongoAdapter);\n  return globalThis.__omnirouteMongoAdapter;';
    source = replaceOnce(source, 'export function getDbInstance(): SqliteDatabase {', 'export function getDbInstance(): SqliteDatabase {' + guard);
    source = replaceOnce(source, 'export async function ensureDbInitialized(): Promise<void> {', 'export async function ensureDbInitialized(): Promise<void> {\n  if (!getDbInstance().healthy()) throw new Error("MongoDB initialization is unhealthy"); getDbInstance().verifyBaseSchema(); return;');
    source = replaceOnce(source, 'export async function shutdownDbInstance(): Promise<boolean> {',
      'export async function shutdownDbInstance(): Promise<boolean> {\n  const mongo = globalThis.__omnirouteMongoAdapter; if (!mongo) return false; await mongo.close(); setDb(null); return true;');
    source = replaceOnce(source, 'export function closeDbInstance(options?: { checkpointMode?: WalCheckpointMode | null }): boolean {',
      'export function closeDbInstance(options?: { checkpointMode?: WalCheckpointMode | null }): boolean {\n  const mongo = globalThis.__omnirouteMongoAdapter; if (!mongo) return false; void mongo.close(); setDb(null); return true;');
  } else if (relativePath === 'src/lib/semanticCache.ts') {
    source = 'import { isBackgroundServicesDisabled } from "@/lib/env/backgroundServices";\nimport { stopSemanticCachePruneJob } from "./jobs/semanticCachePruneJob";\n' + source;
    source = replaceOnce(source, '\nstartSemanticCachePruneJob();', '\nif (!isBackgroundServicesDisabled()) globalThis.__omnirouteActiveRuntime.registerJob(startSemanticCachePruneJob, stopSemanticCachePruneJob);');
  } else if (relativePath === 'src/shared/utils/rateLimiter.ts') {
    source = replaceOnce(source, '  console.warn("[REDIS] REDIS_URL is not set in production. Using in-memory rate limiting.");', '  // MongoDB is authoritative for hard request limits.');
    source = replaceOnce(source, 'return REDIS_URL.length > 0;', 'return globalThis.__omnirouteRedis?.enabled === true;');
    source = replaceOnce(source, 'export function getRedisClient(): Promise<Redis> {', 'export function getRedisClient(): Promise<Redis> {\n  if (!isRedisConfigured()) throw new Error("Redis cache is disabled");\n  return globalThis.__omnirouteRedis.get();');
    source = replaceOnce(source, '): Promise<RateLimitResult> {', '): Promise<RateLimitResult> {\n  return globalThis.__omnirouteMongoAdapter.admitRequest(keyId, rules);');
  } else if (relativePath === 'src/shared/utils/apiKeyPolicy.ts') {
    source = replaceOnce(source, '      id: apiKeyInfo.id,\n      usageLimitEnabled: apiKeyInfo.usageLimitEnabled,',
      '      id: apiKeyInfo.id,\n      allowedConnections: apiKeyInfo.allowedConnections,\n      usageLimitEnabled: apiKeyInfo.usageLimitEnabled,');
    source = replaceOnce(source, '  if (statusRejection) return { apiKey, apiKeyInfo, rejection: statusRejection };',
      '  if (statusRejection) return { apiKey, apiKeyInfo, rejection: statusRejection };\n  let haManagedQuota = false;\n  if (apiKeyInfo.id) {\n    try { haManagedQuota = globalThis.__omnirouteMongoAdapter.checkQuotaPolicy(apiKeyInfo.id).managed; if (haManagedQuota && !/\\/(?:api\\/)?v1\\/(?:chat\\/completions|responses)\\/?$/.test(new URL(request.url).pathname)) throw new Error("Unsupported bounded endpoint"); }\n    catch (error) { return { apiKey, apiKeyInfo, rejection: errorResponse(HTTP_STATUS.SERVICE_UNAVAILABLE, error?.code === "HA_RESERVATION_REQUIRED" ? "Configured hard quotas require durable reservation support, unavailable in this HA profile" : "Quota policy unavailable") }; }\n  }');
    source = replaceOnce(source, '  const keyQuotaRejection = validateKeyQuota(context);', '  const keyQuotaRejection = haManagedQuota ? null : validateKeyQuota(context);');
    source = replaceOnce(source, '  if (rules.length) {', '  if (true) { // MongoDB also checks authoritative per-key quota policies.');
    source = replaceOnce(source, '    const result = await checkRateLimit(apiKeyInfo.id, rules);',
      '    let result;\n    try { result = await checkRateLimit(apiKeyInfo.id, rules); } catch (error) { return errorResponse(HTTP_STATUS.SERVICE_UNAVAILABLE, error?.code === "HA_RESERVATION_REQUIRED" ? "Configured hard quotas require durable reservation support, unavailable in this HA profile" : "Request limit policy unavailable"); }');
  } else if (relativePath === 'src/instrumentation-node.ts') {
    source = replaceOnce(source, 'export async function registerNodejs(): Promise<void> {', `export async function registerNodejs(): Promise<void> {
  const runtime = globalThis.__omnirouteActiveRuntime;
  if (!runtime) throw new Error("Fenced HA runtime must be installed before OmniRoute startup");
  runtime.register(bootActiveOmniRoute);
}
async function bootActiveOmniRoute(): Promise<void> {
  globalThis.__omnirouteActiveRuntime.assertOwner();`);
    source = replaceOnce(source, '  markServerReady();', '  globalThis.__omnirouteActiveRuntime.assertOwner();\n  markServerReady();');
  } else if (relativePath === 'src/lib/usage/migrations.ts') {
    // Local JSON archives belong to an explicit offline import. Either HA host
    // may boot with unrelated files; it must not archive, rename or import them.
    source = replaceOnce(source, 'const LEGACY_DATA_DIR = isCloud ? null : getLegacyDotDataDir();', 'const LEGACY_DATA_DIR = null;');
    source = replaceOnce(source, '\nmigrateLegacyUsageFiles();\n\nif (shouldPersistToDisk) {', '\nif (false) { // Legacy file import is disabled in MongoDB HA mode.');
  } else if (relativePath === 'src/lib/db/apiKeys.ts') {
    source = replaceOnce(source, 'const CACHE_TTL = 60 * 1000;', 'const CACHE_TTL = 0;');
  } else if (relativePath === 'src/lib/db/providers/migrations.ts') {
    source = replaceOnce(source, 'export function ensureProviderSpecificDataSecretsEncrypted(db: DbLike): number {',
      'export function ensureProviderSpecificDataSecretsEncrypted(db: DbLike): number {\n  if (!db.canWrite()) return 0;');
  } else if (relativePath === 'src/lib/db/apiKeys/secretAtRest.ts') {
    source = replaceOnce(source, 'export function backfillApiKeySecretsAtRest(db: SecretAtRestDb): number {',
      'export function backfillApiKeySecretsAtRest(db: SecretAtRestDb): number {\n  if (!db.canWrite()) return 0;');
  } else if (relativePath === 'src/lib/quota/enforce.ts') {
    source = replaceOnce(source, 'export async function recordConsumption(input: RecordConsumptionInput): Promise<void> {',
      'export async function recordConsumption(input: RecordConsumptionInput): Promise<void> {\n  if (globalThis.__omnirouteHardQuota?.ownsKey(input.apiKeyId)) return; // Fenced receipt settlement owns these counters.');
  } else if (relativePath === 'src/lib/quota/storeFactory.ts') {
    // Redis may cache derived state, but cannot become the authoritative quota
    // database or replace MongoDB on an error. Reuse the upstream SQL store.
    source = replaceOnce(source, 'export async function getQuotaStore(): Promise<QuotaStore> {',
      'export async function getQuotaStore(): Promise<QuotaStore> {\n  if (!globalThis.__omnirouteMongoAdapter) throw new Error("MongoDB quota adapter is not initialized");\n  if (!_store) { const { getSqliteQuotaStore } = await import("./sqliteQuotaStore"); _store = getSqliteQuotaStore(); }\n  return _store;');
  } else if (relativePath === 'src/lib/quota/sqliteQuotaStore.ts') {
    // Independent application hosts can disagree on time. Use the same database
    // clock as ownership, rather than allocating quota buckets by host wall time.
    const anchor = 'const nowMs = Date.now();';
    if (source.split(anchor).length !== 7) throw new Error('Quota clock overlay anchor mismatch');
    source = source.replaceAll(anchor, 'const nowMs = globalThis.__omnirouteMongoAdapter.serverTime();');
  } else if (relativePath === 'src/lib/db/adapters/runtimeRequire.ts') {
    // Its sole public export is the optional driver loader. Omit native imports
    // from application bundles as well as forbidding runtime resolution.
    source = 'export function runtimeRequire(specifier: string): never { throw new Error("SQLite driver loading is forbidden in MongoDB mode: " + specifier); }';
  } else if (relativePath === 'open-sse/services/reasoningCache.ts') {
    source = replaceOnce(source, '\nstartAutoCleanup();', '\n// Source and compiled caches share one active-owned durable cleanup timer.\n(globalThis.__omnirouteReasoningPurges ||= new Set()).add(purgeExpiredMemory);');
  } else if (relativePath === 'src/shared/utils/machineId.ts') {
    source = replaceOnce(source, 'function getMachineIdRaw(): string {', 'function getMachineIdRaw(): string {\n  const identity = process.env.OMNIROUTE_HA_IDENTITY;\n  if (!identity) throw new Error("OMNIROUTE_HA_IDENTITY must be shared by both HA instances");\n  return identity;');
  } else if (relativePath === 'src/shared/schemas/memory.ts') {
    source = source.replaceAll('["FTS5", "none"]', '["FTS5", "MongoDB text", "none"]')
      .replaceAll('["fts5", "vector",', '["fts5", "mongodb-text", "exact", "vector",')
      .replaceAll('["sqlite-vec", "qdrant", "none"]', '["sqlite-vec", "mongodb-l2", "qdrant", "none"]');
  } else if (relativePath === 'src/lib/memory/retrieval.ts') {
    source=replaceOnce(source,'import { resolveEmbeddingSource, embed } from "./embedding";','import { resolveEmbeddingSource, embed, withMeasuredDimensions } from "./embedding";');
    if(source.split('await vec.ensureReady(resolution);').length!==4)throw new Error('Memory readiness anchors changed');
    source=source.replaceAll('await vec.ensureReady(resolution);','const haVectorReady = await vec.ensureReady(withMeasuredDimensions(resolution, embeddingResult.vector.length));\n                if (!haVectorReady.ready) throw new Error("Memory embedding configuration changed");');
    source = source.replaceAll('hasTable("memory_fts")', 'globalThis.__omnirouteMongoVectors.textReady()');
    // Both SQL-specific function bodies are pinned. Keep upstream query
    // sanitization, scope, budget, scoring and fallback selection intact.
    const start = source.indexOf('  const db = getDbInstance();', source.indexOf('function buildFtsRows('));
    const end = source.indexOf('\n}\n', start);
    source = source.slice(0, start) + '  return globalThis.__omnirouteMongoVectors.lexical(safeQuery, 100, apiKeyId, { sessionId: config.scope === "session" ? config.sessionId : undefined, retentionDays: config.retentionDays });' + source.slice(end);
    const preview = source.indexOf('        const ftsQueryStr = apiKeyId');
    const after = source.indexOf('\n      }\n', preview);
    source = source.slice(0, preview) + '        ftsRows = globalThis.__omnirouteMongoVectors.lexical(safeQuery, limit, apiKeyId);' + source.slice(after);
    const scope = '{ ...haVectorReady.queryContext, sessionId: normalizedConfig.scope === "session" ? config.sessionId : undefined, retentionDays: normalizedConfig.retentionDays }';
    source = replaceOnce(source, 'vec.searchVector(embeddingResult.vector, 100, apiKeyId)', 'vec.searchVector(embeddingResult.vector, 100, apiKeyId, ' + scope + ')');
    source = replaceOnce(source, '                  config.query,\n                  100,\n                  apiKeyId', '                  config.query,\n                  100,\n                  apiKeyId, ' + scope);
    if(source.split('                apiKeyId ?? undefined\n').length!==3)throw new Error('Memory preview query anchors changed');
    source=source.replaceAll('                apiKeyId ?? undefined\n','                apiKeyId ?? undefined,\n                haVectorReady.queryContext\n');
    source = replaceOnce(source, 'available = supportsFts5(db);', 'available = globalThis.__omnirouteMongoVectors.textReady();');
    source = source.replaceAll('SQLite build lacks FTS5 — keyword search unavailable', 'MongoDB text index unavailable')
      .replaceAll('FTS5 keyword search active', 'MongoDB native text index active')
      .replaceAll('FTS5 probe failed', 'MongoDB text index probe failed')
      .replaceAll('"FTS5"', '"MongoDB text"').replaceAll('sqlite-vec', 'mongodb-l2')
      .replaceAll('"fts5" | "vector"', '"exact" | "mongodb-text" | "vector"')
      .replaceAll('tier: "fts5" as const', 'tier: "exact" as const');
    source = replaceOnce(source, '    if (ftsRows.length === 0) {\n      baseQuery', '    const lexicalMatched = ftsRows.length > 0;\n    if (!lexicalMatched) {\n      baseQuery');
    source = source.replace('tier: "fts5", vecScore: null', 'tier: "exact", vecScore: null')
      .replace('tier: "fts5", vecScore: null', 'tier: lexicalMatched ? "mongodb-text" : "exact", vecScore: null');
  } else if (relativePath === 'src/lib/db/readCache.ts') {
    source = replaceOnce(source, '  get(key: string): T | undefined {', '  get(key: string): T | undefined {\n    if (this.ttlMs === 0) return undefined;');
    source = replaceOnce(source,'  load(key: string, loader: () => Promise<T>): Promise<T> {','  load(key: string, loader: () => Promise<T>): Promise<T> {\n    if (this.ttlMs === 0) return loader();');
    source = replaceOnce(source, 'const SETTINGS_TTL_MS = 5_000;', 'const SETTINGS_TTL_MS = 0;');
    source = replaceOnce(source, 'const CONNECTIONS_TTL_MS = 5_000;', 'const CONNECTIONS_TTL_MS = 0;');
    source+='\n// Source and compiled copies share commit/promotion invalidation.\n(globalThis.__omnirouteReadInvalidators ||= new Set()).add(invalidateDbCache);\n';
  } else if (relativePath === 'src/lib/memory/settings.ts') {
    source = replaceOnce(source, '  if (cachedMemorySettings !== null) {\n    return cachedMemorySettings;\n  }', '');
  } else if (relativePath === 'src/lib/memory/store.ts') {
    source = replaceOnce(source, 'const MEMORY_CACHE_TTL = 60_000;', 'const MEMORY_CACHE_TTL = 0;');
    source = replaceOnce(source,
      '      await vec.upsertVector(id, embeddingResult.vector);\n      safeMarkNeedsReindex(id, false);',
      '      await vec.upsertVector(id, embeddingResult.vector, { content, signature: withMeasuredDimensions(resolution, embeddingResult.vector.length).signature });');
  } else if (relativePath === 'src/lib/memory/reindex.ts') {
    source = replaceOnce(source,
      '    await vec.upsertVector(item.id, embeddingResult.vector);\n    markMemoryNeedsReindex(item.id, false);',
      '    const stored = await vec.upsertVector(item.id, embeddingResult.vector, { content: item.content, signature: withMeasuredDimensions(resolveEmbeddingSource(settings), embeddingResult.vector.length).signature });\n    if (!stored) return "error";');
  } else if (relativePath === 'src/lib/db/providers.ts') {
    source=replaceOnce(source,'  const db = getDbInstance() as unknown as DbLike;\n  const existing = db.prepare("SELECT * FROM provider_connections WHERE id = ?").get(id);',
      '  globalThis.__omnirouteCredentialRuntime?.validateUpdate(id, data);\n  const db = getDbInstance() as unknown as DbLike;\n  const existing = db.prepare("SELECT * FROM provider_connections WHERE id = ?").get(id);');
    // Credential validation yields. Reading first allows two concurrent calls
    // in the active process to overwrite each other's unrelated fields.
    source = replaceOnce(source,
      '  const existing = db.prepare("SELECT * FROM provider_connections WHERE id = ?").get(id);\n  if (!existing) return null;',
      '  let existing = db.prepare("SELECT * FROM provider_connections WHERE id = ?").get(id);\n  if (!existing) return null;');
    source = replaceOnce(source,
      '  await assertApiKeyIsNotManagementPassword(data.apiKey);\n\n  const existingCamel',
      '  await assertApiKeyIsNotManagementPassword(data.apiKey);\n  existing = db.prepare("SELECT * FROM provider_connections WHERE id = ?").get(id);\n  if (!existing) return null;\n\n  const existingCamel');
  } else if (relativePath === 'src/lib/memory/vectorStore.ts') {
    source = replaceOnce(source, 'export function getVectorStore(): VectorStore | null {',
      'export function getVectorStore(): VectorStore | null {\n  if (process.env.VECTOR_STORE_DISABLE_VEC === "true") return null;\n  if (!globalThis.__omnirouteMongoVectors) throw new Error("MongoDB vector store is not initialized");\n  return globalThis.__omnirouteMongoVectors;');
  }
  return source;
}
