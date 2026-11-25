import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { splitStatements, parse } from './sql.mjs';
import { installBaseSchema,artifactSchemaSql } from './baseSchema.mjs';

const root = new URL('../upstream/omniroute/src/lib/', import.meta.url);
function pinned(file, hash) {
  const sql = readFileSync(new URL(file, root), 'utf8');
  if (createHash('sha256').update(sql).digest('hex') !== hash) throw new Error('Startup schema source changed: ' + file);
  return sql;
}

export function provisionArtifactSchema(db) {
  if(db.requiresOwnership)throw new Error('Provision runtime schema offline');
  const exists=db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get('ha_call_log_artifacts');
  db.exec(artifactSchemaSql);
  return !exists;
}

// SQLite initializes this lazily at application startup. MongoDB readiness must
// provision it offline, including imports of valid databases not yet booted.
export function provisionAuditSchema(db) {
  if(db.requiresOwnership)throw new Error('Provision runtime schema offline');
  const exists=db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get('audit_log');
  const audit=pinned('compliance/index.ts','d8a2607d81db1d717286868a8114571af8308a4d6e0700e93f223d320aec0c30');
  const section=audit.slice(audit.indexOf('function ensureAuditLogSchema('),audit.indexOf('type AuditLogQuery'));
  const blocks=[...section.matchAll(/db\.exec\(`\s*(CREATE [^`]+)`\)/g)];
  if(blocks.length!==2)throw new Error('Audit schema extraction failed');
  for(const block of blocks)db.exec(block[1]);
  return !exists;
}

// Fresh-database profile for the headless startup path. Never mark unprocessed
// migrations as applied. This is not a general upgrade or data import tool.
export async function provisionStartupSchema(db) {
  if (db.requiresOwnership) throw new Error('Use an offline bootstrap connection');
  installBaseSchema(db);
  const { ensureApiKeyFallbackColumns } = await import('../upstream/omniroute/src/lib/db/apiKeyColumnFallbacks.ts');
  const { ensureProviderConnectionsColumns } = await import('../upstream/omniroute/src/lib/db/schemaColumns.ts');
  const { ensureProxyLogsColumns } = await import('../upstream/omniroute/src/lib/db/schemaColumns.ts');
  ensureApiKeyFallbackColumns(db); ensureProviderConnectionsColumns(db); ensureProxyLogsColumns(db);
  pinned('db/schemaColumns.ts', '38e9783b645a60c966c0f996584473fdf0bf97c1a365a366e9c2620b1b09f73e');
  const { ensureUsageHistoryColumns, ensureUsageHistoryAccountIndex, ensureCallLogsColumns } = await import('../upstream/omniroute/src/lib/db/schemaColumns.ts');
  // This non-unique expression index is a SQLite query optimization. Native
  // queries still evaluate COALESCE; index the raw timestamp/key prefix in
  // MongoDB instead. Do not claim support for UNIQUE expression indexes.
  const dedupIndex = "CREATE INDEX IF NOT EXISTS idx_uh_dedup ON usage_history(timestamp, COALESCE(provider, ''), COALESCE(model, ''), COALESCE(connection_id, ''), COALESCE(api_key_id, ''), tokens_input, tokens_output)";
  ensureUsageHistoryColumns({ prepare: db.prepare.bind(db), exec(sql) {
    const normalized = sql.replace(/\s+/g, ' ').replace(/\(\s+/g, '(').replace(/\s+\)/g, ')').trim();
    return db.exec(normalized === dedupIndex ? 'CREATE INDEX IF NOT EXISTS idx_uh_dedup ON usage_history(timestamp,provider,model,connection_id,api_key_id,tokens_input,tokens_output)' : sql);
  } });
  ensureUsageHistoryAccountIndex(db);
  ensureCallLogsColumns(db);
  const migration = (name, hash) => pinned('db/migrations/' + name + '.sql', hash);
  const reasoning = migration('126_reasoning_routing_rules', '20e88a5637d7a807d01db67416f9b91536e984ae20c7b96ad53d9375c2e11b6d');
  const triggerStart = reasoning.indexOf('-- OmniRoute historically');
  if (triggerStart < 0) throw new Error('Reasoning trigger replacement anchor changed');
  // The three legacy cleanup triggers duplicate the declared CASCADE foreign
  // keys. The translator enforces those cascades natively in the parent TX.
  db.exec(reasoning.slice(0, triggerStart));
  db.exec(migration('017_version_manager_upstream_proxy', '2cea43fc6bc62350a70b366116d2a744e5b0063cf73a1110fc14f5977f263c99'));
  const registry = migration('004_proxy_registry', 'f4ad1ed25f3aa67830f0dc2bb240951dbfb8fbf8ce8d4c74e772018df3bd2f98');
  for (const sql of splitStatements(registry)) {
    const { ast } = parse(sql);
    if ((ast.table?.[0]?.table ?? ast.table?.table) === 'proxy_registry') db.exec(sql);
  }
  // A fresh database uses the final pool schema; no legacy assignment rows
  // exist to rename/copy/drop. Never mark that data migration as executed.
  const pools = migration('117_proxy_pool_rotation', 'a19bdc4949edf675a23aae46c24ff972fa6d22f479f1370a4af77341155efbca');
  for (const sql of splitStatements(pools)) if (parse(sql).ast.type === 'create') db.exec(sql);
  db.exec(migration('040_oneproxy_proxy_fields', '7ff59b71450390962ac1083d24988c296ed85fad6d77461c63f5ea8c55064bc5'));
  const freeProxies=migration('068_free_proxies','9d9a5ceed90d19933b4d6189ee20cbafb81672b18986dc55175d0f4ac2d6f5ab');
  // Fresh bootstrap has no legacy oneproxy rows to copy. Existing-data import
  // preserves the final SQLite table; never pretend the INSERT SELECT ran here.
  for(const sql of splitStatements(freeProxies))if(parse(sql).ast.type==='create')db.exec(sql);
  db.exec(migration('122_free_proxy_sync_errors','13ed9096fac0d253a239b513aa371d1f9ecb51caaf56cc5e3af3885be8bcc25f'));
  db.installFreeProxyCleanup();
  db.exec(migration('099_proxy_family', 'c6f9471d9e3d2fc693dbeb9f11c4d0752ad41af572acd14ba9b0fb404e8b1ee1'));
  db.exec(migration('131_proxy_subscriptions', 'b631df5a8addb37a0bc19f25b0481b2c8dcb4cdcbb2dbfc0baf6349acded7024'));
  db.exec(migration('005_combo_agent_fields', '377e2275fd67b6bdfeb1b26068a417f32ff85bb58478cceaee5e348b51d68d45'));
  const mcp = migration('002_mcp_a2a_tables', '824787caafd2c048662a3cbc21c645d38c1c910f742454497dfd8755623e0723');
  for (const sql of splitStatements(mcp)) {
    const { ast } = parse(sql);
    if ((ast.table?.[0]?.table ?? ast.table?.table) === 'mcp_tool_audit') db.exec(sql);
  }
  provisionAuditSchema(db);
  const details = migration('006_detailed_request_logs', '532e2853e760a2318e46b3cd29c50897b6b83953c410072afbb84a9f9e0d63e8');
  const start = details.indexOf('CREATE TRIGGER IF NOT EXISTS trg_rdl_ring_buffer');
  const end = details.indexOf('END;', start);
  if (start < 0 || end < start) throw new Error('Request detail trigger extraction failed');
  db.exec(details.slice(0, start));
  db.installRequestDetailRetention();
  db.exec(details.slice(end + 4));
  for (const [file, hash] of [
    ['033_create_reasoning_cache','18f5668679d0673c0b40eeac8b101de9f90de8778412a9ed279eaebf4eb9bd06'],
    ['217_quota_recovery_tickets','8d8f52c78a7135840a87caec794a941275100641ac13668ca9b5185248ca7ef3'],
    ['113_provider_node_icon_url','0c91c250b478ae8fc6d19a1b7fb4bebd91a9f1e784b82ee241f39f5cbbb22b20'],
    ['172_provider_node_daily_quota_reset','68e39dc17a2cc3823e0eab5f8b99d13f29e01e9a1641fe2e794343081f7c8867'],
    ['119_model_capability_overrides','7b275fcdf7d8dcd7ffbd127e89f9923034428dea994cfe26132dd411078f6c0b'],
    ['110_model_context_overrides','a891af45947f52eaea96e13b3cb2e939d83c02c16220c97b503b97bde70c8114'],
    ['157_exclusive_connection_leases','887d4c33b43c6dc34036c34c04a08436155499b9440b80a762d2228036eae3af'],
    ['177_provider_connection_synced_models_at','ae3fe0db6d4604afec763e8cefef29e4eb64e2bc63ee83ce5b198ab00aac4d7d'],
    ['013_quota_snapshots','7426e6850d80d49b07aa35d789bb8fe8da2950a4ddf0d62bf87bf337240df631'],
    ['108_provider_quota_reset_events','1c5319fbc23b19978e0bf1a7a74b115cebd0eb4f5a8e32bb8e917b82c44e40de'],
    ['105_usage_history_endpoint','27ad685dc494d801dd70f71ab2c0152790c24bae70dfbac350edd0f62a42f866'],
    ['185_usage_history_cpa_auth_index','b71c5e5f74e9a60b26bc242e336470b3e2ae63430194caaf619d3271512b3c12'],
    ['212_usage_history_provider_reported_cost','5a285c1d134d8856d811ea87dc312c196eced314a64b149603ef8ec1ba6b227c'],
    ['116_call_logs_reasoning_source','654cd24d21be6b0a94e2cd0d3a8080068b40cc5bb6ff5aab8bee5e7de0340af2'],
    ['154_call_logs_response_id','2cdc304873bd466a583aca271790ec42fef9a88d6a668fc2ed56920f2de906b0'],
    ['158_call_logs_error_type','5ee3fab50bf4baa75fb85bcf8ddd14708e9708ec2d69fa4a677c2cb42f5913db'],
    ['210_call_logs_detail_pending','e6bb642ed7c68b710b1d3ef1d365ff4952a58fe94af42c6fe8c8beb9ed816932'],
    ['066_api_key_groups','472f2eaf3ee59256f56d189c2d30f279c8fe140306d3f95170052338df0e22d8'],
    ['076_create_plugins','a470c072670ac09f598dbd9793851d883f98b91a44b4125dc9346e34b7d9e75d'],
    ['016_create_skills','8da8120c9aacfbebb99c818094bbccad8fcbaa35b1470e6ee1ccb8797abc36db'],
    ['027_skill_mode_and_metadata','b13dec5a43cc1b8e29af5e3ec526fb8bd65b9e77f78d30eab18f514f9e33f0fb'],
    ['155_agentic_conversations','023068deb65541eb2f65e74f784188e1e946235e20bad21c6f9f5a0aa99854bc'],
    ['156_conversation_turn_nodes','e5e9734795490f93071847b7cb2f84604e51a8fac19e44ce0464ced46dd66ff5'],
    ['015_create_memories','74655e922e19e4abc01c528194a957459d440f23bef4c2f4aed11276b1b8dc1a'],
    ['078_quota_consumption','a22760383f409b8c0733d4b0129149e4966e13da6029bc6a9cdaa0a9ce7f80c4'],
    ['079_provider_plans','1c8451a6bcf32008d6d0a63a4f96128583adc9e3fec7e3ee88d35ef4a07faa41'],
    ['085_quota_pools','49fd4ac1b24362dc3990153470df61a7ec695e698cd58e89473c5a3bd2fddc5f'],
    ['088_quota_groups','1ae03edb2a9ffda3cf34873ffe3c2a368d91c176a1152a08ae48c04218f3a215'],
    ['106_quota_allocation_model_caps','dfd6c42aedbae9259a3e649eb9860af1c3ee22b525cb967de672e06bfc4c5195'],
    ['189_quota_schedules','613780d7c4016e7fa82342c4a19768a6ea7cfdc15f2912b7ab17695301e3f390'],
    ['083_memory_vec','5c7c51fc19d06c2f7b3235d7153eb82575aee2e69dada14aa5f69724f21ca4d9'],
    ['111_memory_typed_decay','3ceb3e16769e1785410b15b752c5dd382ae875f160c837c672e653e3230e4124'],
  ]) db.exec(migration(file, hash));
  const poolMembers = migration('087_quota_pool_connections', 'bc45e02b78a3c64d8ea2ae3fc49ed73f84d619dba08601dbb3447266ef6a8367');
  // This profile creates empty pools; there are no legacy memberships to copy.
  for (const sql of splitStatements(poolMembers)) if (parse(sql).ast.type === 'create') db.exec(sql);
  db.exec(migration('182_request_cost_ledger_and_key_quota', 'b272f4dfe9481ae242dcab4a85ef90b4ab086a0b581011ee148c8f713ad206a7'));
  const tokenSource = pinned('db/tokenLimits.ts', 'c7802f88513d0c0d1015c91d4adeb479b78c04ef5c62a1db33c1bbc08debf3b2');
  const tokenSql = tokenSource.slice(tokenSource.indexOf('function ensureSchema()')).match(/db\.exec\(`([\s\S]*?)`\)/)?.[1];
  if (!tokenSql || tokenSql.includes('${')) throw new Error('Token schema extraction failed');
  db.exec(tokenSql);
  const capabilities = pinned('modelsDevSync.ts', 'e3479a4d12bb51c1da74e6cccf3435df08d01e4097912fcd01df20d46688a65a');
  const capabilitiesSql = capabilities.slice(capabilities.indexOf('export function ensureCapabilitiesTable()')).match(/db\.exec\(`([\s\S]*?)`\)/)?.[1];
  if (!capabilitiesSql || capabilitiesSql.includes('${')) throw new Error('Capabilities schema extraction failed');
  db.exec(capabilitiesSql);
  const compression = pinned('db/compressionCombos.ts', '8f2f6b37bcb408b8faaf4df79307b64f4368214ebb578554812020a24a22871c');
  const compressionSql = compression.slice(compression.indexOf('function ensureCompressionComboTables()')).match(/db\.exec\(`([\s\S]*?)`\)/)?.[1];
  if (!compressionSql || compressionSql.includes('${')) throw new Error('Compression schema extraction failed');
  db.exec(compressionSql);
  db.exec(migration('038_compression_analytics', '71b7d06e954d2c3733919a395a662adcd1b8bb605a9adb35025c44dc6d71ac2e'));
  const analytics = pinned('db/compressionAnalytics.ts', '2989f4d16a331bd20624748a8624dba0065e0e70dee96e6cf3227449e9b28f66');
  const columnList = analytics.match(/const COMPRESSION_ANALYTICS_COLUMNS = (\[[\s\S]*?\]) as const;/)?.[1];
  if (!columnList) throw new Error('Compression analytics columns changed');
  // The pinned source contains a static array of identifier/type string pairs.
  // Parse JSON only; never evaluate application source as bootstrap code.
  const additions = JSON.parse(columnList.replace(/,\s*\]/g, ']'));
  for (const [name, type] of additions) {
    if (!/^[a-z_]+$/.test(name) || !/^(TEXT|REAL|INTEGER)( DEFAULT 0)?$/.test(type)) throw new Error('Unexpected analytics column');
    db.exec(`ALTER TABLE compression_analytics ADD COLUMN ${name} ${type}`);
  }
  const breakdownSql = analytics.slice(analytics.indexOf('function ensureCompressionEngineBreakdownTable()')).match(/db\.exec\(`([\s\S]*?)`\)/)?.[1];
  if (!breakdownSql || breakdownSql.includes('${')) throw new Error('Compression engine schema changed');
  db.exec(breakdownSql);
  const telemetry = pinned('db/compressionRunTelemetry.ts', '062b2e673764823b49a7888759068c7f86d0f75e7c989850313db601ce25a209');
  const telemetrySql = telemetry.slice(telemetry.indexOf('export function ensureCompressionRunTelemetryTable()')).match(/db\.exec\(`([\s\S]*?)`\)/)?.[1];
  if (!telemetrySql || telemetrySql.includes('${')) throw new Error('Compression telemetry schema changed');
  db.exec(telemetrySql); // Provision the supplied connection, never an unrelated global singleton.
  db.exec(migration('047_aggregation_tables','7c8ca3483e8e8cd6e91ad995cf5956d52c5991f862215c8305cbe75c5fced9ae'));
  const summaryIndexes=migration('048_summary_indexes','1d51401f3dc18e6b6e6343969bf210b2b96f3f7a885dae45c416be8bbd0aa7fc');
  // 048 repeats the two non-unique 047 compound keys under new names. MongoDB
  // rejects duplicate physical indexes; retain the originals and all unique keys.
  for(const sql of splitStatements(summaryIndexes)) {
    if(/CREATE INDEX IF NOT EXISTS idx_(?:daily|hourly)_usage_provider_model_date_composite\b/.test(sql))continue;
    db.exec(sql);
  }
  db.exec(migration('215_team_cost_centers', 'ca7deaf551f7060e2f18beeae3a9cd14ee184c9e5905895793565a44d3b9546e'));
  db.exec(migration('216_team_cost_shapes','af3a7f1ab0bdb79fc7d299a26e35a07cdf7db7ea0596d3dc441d919c8b05c59d'));
  db.exec(migration('060_create_gamification','7e0ed4425700665d86ef1b500a2262ed52ca2ef051fbb167912f7393a4af67be'));
  // Fresh profile only: the historical backfill is not executed or marked done.
  const actionCounts=migration('176_xp_action_counts','1fe66deccf6076b4909d57a47df1d5beb5ab6824d882277b9ace734710ab23c1');
  const backfill=actionCounts.indexOf('-- Backfill current lifetime totals');
  if(backfill<0)throw new Error('XP action count bootstrap boundary changed');
  db.exec(actionCounts.slice(0,backfill));
  db.exec(migration('010_model_combo_mappings', '816546065a91deeb81b9b18209611ae76d1b91bb16d4508250ec75a6617c4e95'));
  provisionArtifactSchema(db);
  db.installMemoryTextIndex();
  db.installRequestLimits();
  db.assertCompatible();
}
