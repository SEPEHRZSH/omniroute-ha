import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';

let latest;
export function latestQuotaSql() {
  if (!latest) {
    const source = readFileSync(new URL('../upstream/omniroute/src/lib/db/quotaSnapshots.ts', import.meta.url), 'utf8');
    if (createHash('sha256').update(source).digest('hex') !== '6b243f0935e36d9e7a99ca4e360613f3dfbfdf096d694a7df82a6c1ed4e91044') throw Object.assign(new Error('Upstream quota-query contract changed'), { code: 'HA_UNSUPPORTED_SQL' });
    latest = source.slice(source.indexOf('export function getLatestQuotaSnapshotsForConnection(')).match(/\.prepare\(\s*`([\s\S]*?)`\s*\)/)?.[1];
    if (!latest || latest.includes('${')) throw new Error('Quota-query extraction failed');
  }
  return latest;
}
export function parseLatestQuota(sql, bindings) {
  if (!/^\s*SELECT\s+\*\s+FROM\s*\(/i.test(sql)) return null;
  if (sql.replace(/\s+/g, ' ').trim() !== latestQuotaSql().replace(/\s+/g, ' ').trim()) return null;
  if (bindings.length !== 1 || typeof bindings[0] !== 'string') throw Object.assign(new Error('Unsupported quota-query binding'), { code: 'HA_UNSUPPORTED_SQL' });
  return { type: 'ha_quota_latest', connection: bindings[0] };
}
export async function executeLatestQuota(db, ast, columns, session) {
  // Equivalent to ROW_NUMBER() PARTITION BY window_key ORDER BY time/id DESC,
  // filtered to rn=1. A hot window must never evict another window's snapshot.
  return db.collection('quota_snapshots').aggregate([
    { $match: { connection_id: ast.connection } },
    { $sort: { created_at: -1, id: -1 } },
    { $group: { _id: '$window_key', row: { $first: '$$ROOT' } } },
    { $replaceRoot: { newRoot: '$row' } },
    { $project: { _id: 0, ...Object.fromEntries(columns.map(c => [c, { $ifNull: ['$' + c, null] }])), rn: { $literal: 1 } } },
  ], { session, ...(!session ? { readConcern: { level: 'snapshot' } } : {}), maxTimeMS: 5000 }).toArray();
}
