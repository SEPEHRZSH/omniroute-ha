import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { installBaseSchema } from '../adapter/baseSchema.mjs';
import { splitStatements,parse } from '../adapter/sql.mjs';

// Install the complete pinned core baseline, then selected domain additions.
// This does not certify or mark the full upstream migration history as applied.
export async function provisionDomains(db) {
  assert.deepEqual(installBaseSchema(db), { tables: 17, indexes: 22 });
  const { ensureApiKeyFallbackColumns } = await import('../upstream/omniroute/src/lib/db/apiKeyColumnFallbacks.ts');
  const { ensureProviderConnectionsColumns } = await import('../upstream/omniroute/src/lib/db/schemaColumns.ts');
  ensureApiKeyFallbackColumns(db);
  ensureProviderConnectionsColumns(db);
  for(const name of ['113_provider_node_icon_url','172_provider_node_daily_quota_reset'])db.exec(readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/'+name+'.sql',import.meta.url),'utf8'));
  const proxyMigration = readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/017_version_manager_upstream_proxy.sql', import.meta.url), 'utf8');
  db.exec(proxyMigration.match(/CREATE TABLE IF NOT EXISTS upstream_proxy_config \([\s\S]*?\);/)[0]);
  const registry=readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/004_proxy_registry.sql',import.meta.url),'utf8');
  db.exec(registry.match(/CREATE TABLE IF NOT EXISTS proxy_registry \([\s\S]*?\);/)[0]);
  const pools=readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/117_proxy_pool_rotation.sql',import.meta.url),'utf8');
  for(const sql of splitStatements(pools)) if(parse(sql).ast.type==='create') db.exec(sql);
  db.exec(readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/099_proxy_family.sql',import.meta.url),'utf8'));
}
