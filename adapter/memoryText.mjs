// MongoDB's native text index is transactionally maintained on content/key.
// This implements OmniRoute's sanitized lexical query contract, not arbitrary
// FTS5 MATCH expressions or SQLite's BM25 scoring algorithm.
export const textIndex = 'ha_memory_text_v1';
export async function hasMemoryTextIndex(db) {
  const indexes = await db.collection('memories').listIndexes({ maxTimeMS: 3000 }).toArray();
  return indexes.some(i => i.name === textIndex && i.default_language === 'none' &&
    i.weights?.content === 1 && i.weights?.key === 1 && Object.keys(i.weights).length === 2);
}

export function scopeStages(request) {
  const { apiKeyId, sessionId, retentionDays = 0 } = request;
  for (const field of [apiKeyId, sessionId]) if (field != null && typeof field !== 'string') throw new TypeError('Invalid Memory scope');
  if (!Number.isFinite(retentionDays) || retentionDays < 0) throw new RangeError('Invalid retention period');
  const date = field => ({ $convert: { input: '$' + field, to: 'date', onError: null, onNull: null } });
  const stages = [{ $match: {
    ...(apiKeyId != null ? { api_key_id: apiKeyId } : {}), ...(sessionId != null ? { session_id: sessionId } : {}),
    $expr: { $or: [{ $eq: [{ $ifNull: ['$expires_at', null] }, null] }, { $gt: [date('expires_at'), '$$NOW'] }] },
  } }];
  if (retentionDays) stages.push({ $match: { $expr: { $gte: [date('created_at'), { $subtract: ['$$NOW', retentionDays * 86400000] }] } } });
  return stages;
}

export async function lexicalRows(db, session, request) {
  if (!Number.isInteger(request.topK) || request.topK < 1 || request.topK > 1000) throw new RangeError('Invalid lexical topK');
  if (request.query === '') return [];
  if (typeof request.query !== 'string' || request.query.length > 4200 ||
      !/^"[\w\u00C0-\u024F\u1E00-\u1EFF]+"(?: "[\w\u00C0-\u024F\u1E00-\u1EFF]+"){0,31}$/.test(request.query)) {
    throw new TypeError('Expected an upstream-sanitized Memory query');
  }
  return db.collection('memories').aggregate([
    { $match: { $text: { $search: request.query, $language: 'none' } } }, ...scopeStages(request),
    { $set: { _textScore: { $meta: 'textScore' } } }, { $sort: { _textScore: -1, id: 1 } }, { $limit: request.topK },
    { $project: { _id: 0, _ha_vector: 0, _ha_rowid: 0 } },
  ], { session, maxTimeMS: 5000 }).toArray();
}

export function fuseResults(vector, lexical, k, limit) {
  if (!Number.isFinite(k) || k <= 0) throw new RangeError('Invalid RRF constant');
  const merged = new Map();
  for (const [list, isVector] of [[vector, true], [lexical, false]]) {
    for (const [index, entry] of list.entries()) {
      const id = isVector ? entry.memoryId : entry.id;
      const hit = merged.get(id) ?? { memoryId: id, vecRank: null, ftsRank: null, rrfScore: 0, vecDistance: null, ftsScore: null };
      const rank = index + 1;
      if (isVector) { hit.vecRank = rank; hit.vecDistance = entry.distance; }
      else { hit.ftsRank = rank; hit.ftsScore = -entry._textScore; }
      hit.rrfScore += 1 / (k + rank); merged.set(id, hit);
    }
  }
  return [...merged.values()].sort((a, b) => b.rrfScore - a.rrfScore || (a.memoryId < b.memoryId ? -1 : 1)).slice(0, limit);
}
