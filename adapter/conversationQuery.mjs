import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';

let template;
const canonical = sql => sql.replace(/\s+/g, ' ').trim();
function reject(message) { throw Object.assign(new Error(message), { code: 'HA_UNSUPPORTED_SQL' }); }

// Reuse the exact upstream SQL template as the compatibility contract. No
// generic correlated-subquery support or source execution is implied here.
export function conversationCandidatesSql(count) {
  if (!template) {
    const source = readFileSync(new URL('../upstream/omniroute/src/lib/db/agenticConversations.ts', import.meta.url), 'utf8');
    if (createHash('sha256').update(source).digest('hex') !== 'c5cd123fbf106c08e6f414fd1a186c9dab3ec7a3d1b331073c12a062be15a52d') reject('Upstream conversation-query contract changed');
    template = source.slice(source.indexOf('export function contentCandidatesSql(')).match(/return `([\s\S]*?)`;/)?.[1];
    if (!template || template.split('${placeholders}').length !== 2 || template.replace('${placeholders}', '').includes('${')) reject('Conversation-query template extraction failed');
  }
  return template.replace('${placeholders}', Array(count).fill('?').join(', '));
}

export function parseConversationCandidates(sql, bindings) {
  if (!/^\s*SELECT\s+c\.\*/i.test(sql)) return null;
  const count = bindings.length - 3;
  if (count < 1 || count > 512 || canonical(sql) !== canonical(conversationCandidatesSql(count))) return null;
  const hashes = bindings.slice(0, count), [fingerprint, scanLimit, limit] = bindings.slice(count);
  if (!hashes.every(h => typeof h === 'string' && h.length <= 4096) || typeof fingerprint !== 'string' ||
    !Number.isSafeInteger(scanLimit) || scanLimit < 0 || scanLimit > 5000 ||
    !Number.isSafeInteger(limit) || limit < 0 || limit > 5000) reject('Unsupported conversation query bindings or bounds');
  return { type: 'ha_conversation_candidates', hashes, fingerprint, scanLimit, limit };
}

export async function executeConversationCandidates(db, ast, columns, session) {
  if (!ast.scanLimit || !ast.limit) return [];
  const projection = Object.fromEntries(columns.map(column => [column, { $ifNull: ['$' + column, null] }]));
  return db.collection('agentic_conversations').aggregate([
    { $match: { fingerprint_hash: ast.fingerprint } },
    { $sort: { last_seen_at: -1 } }, { $limit: ast.scanLimit },
    { $lookup: { from: 'conversation_turn_nodes', let: { conversation: '$id' }, pipeline: [
      { $match: { content_hash: { $in: ast.hashes }, $expr: { $eq: ['$conversation_id', '$$conversation'] } } },
      { $group: { _id: '$content_hash' } },
    ], as: '_ha_matches' } },
    { $set: { hits: { $size: '$_ha_matches' } } },
    { $match: { hits: { $gt: 0 } } },
    { $sort: { hits: -1, last_seen_at: -1 } }, { $limit: ast.limit },
    { $project: { _id: 0, ...projection, hits: 1 } },
  ], { session, ...(!session ? { readConcern: { level: 'snapshot' } } : {}), maxTimeMS: 5000 }).toArray();
}
