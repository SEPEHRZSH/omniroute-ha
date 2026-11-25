// Native replacement for the SQLite vector extension at OmniRoute's VectorStore
// boundary. SQL/FTS expressions are never accepted or silently approximated here.
import { unsupported } from './sql.mjs';

import { hasMemoryTextIndex, scopeStages, lexicalRows, fuseResults } from './memoryText.mjs';

function dimensions(value) {
  if (!Number.isInteger(value) || value < 1 || value > 8192) throw new RangeError('Vector dimensions must be 1..8192');
}
function vectorValues(value, dim) {
  if (!(value instanceof Float32Array) && !Array.isArray(value)) throw new TypeError('Expected a float32 vector');
  const values = [...value].map(Math.fround);
  if (values.length !== dim || values.some(n => !Number.isFinite(n))) throw new RangeError('Vector dimension/value mismatch');
  return values;
}

export async function executeMemoryVector(db, session, request) {
  const memories = db.collection('memories');
  if (request.action === 'textStatus') return hasMemoryTextIndex(db);
  if (request.action === 'lexical') return lexicalRows(db, session, request);
  const metadata = db.collection('memory_vec_meta');
  const meta = await metadata.findOne({ id: 1 }, { session });
  if (!meta) throw new Error('Memory vector schema must be provisioned before startup');
  const options = { session, maxTimeMS: 5000 };
  const expectedRevision = request.action === 'upsert' ? request.expected?.configurationRevision : request.configurationRevision;
  const changed=()=>{throw Object.assign(new Error('Memory query embedding configuration changed'),{code:'HA_MEMORY_CONFIGURATION_CHANGED'});};
  if (expectedRevision != null) {
    const settings = await db.collection('key_value').findOne({ namespace: 'settings', key: '_settingsRevision' }, { session });
    const revision = settings?.value ? JSON.parse(settings.value) : 0;
    if (revision !== expectedRevision) {
      if(['search','hybrid'].includes(request.action))changed();
      return request.action === 'upsert' ? false : { ready: false, reason: 'Settings changed before vector configuration' };
    }
  }
  if (request.action === 'configure' || request.action === 'reset') {
    const { signature, dim } = request;
    if (dim == null && request.action === 'configure') return { ready: !!meta.vec_loaded&&meta.embedding_signature?.replace(/:[0-9]+$/,':null')===signature, signature:meta.embedding_signature, reason: 'No embedding dimensions available for this model' };
    dimensions(dim);
    if (typeof signature !== 'string' || !signature || signature.length > 512) throw new TypeError('Invalid embedding signature');
    if (request.action === 'reset' || meta.embedding_signature !== signature || meta.active_dim !== dim) {
      await memories.updateMany({}, { $unset: { _ha_vector: '' }, $set: { needs_reindex: 1 } }, options);
      await metadata.updateOne({ id: 1 }, [{ $set: {
        active_dim: dim, embedding_signature: { $literal: signature }, vec_loaded: 1,
        last_reset_at: { $dateToString: { date: '$$NOW', format: '%Y-%m-%dT%H:%M:%S.%LZ' } },
      } }], options);
    }
    return { ready: true, reason: 'MongoDB exact L2 vector search is ready' };
  }
  if (request.action === 'delete') {
    await memories.updateOne({ id: request.memoryId }, { $unset: { _ha_vector: '' } }, options); return;
  }
  if (request.action === 'stats') {
    return {
      rowCount: meta.vec_loaded ? await memories.countDocuments({ '_ha_vector.signature': meta.embedding_signature }, options) : 0,
      needsReindex: await memories.countDocuments({ needs_reindex: 1 }, options),
      activeDim: meta.active_dim, signature: meta.embedding_signature,
    };
  }
  if (!meta.vec_loaded) throw new Error('Memory vector store is not configured');
  if (request.action === 'upsert') {
    if (typeof request.expected?.content !== 'string' || typeof request.expected?.signature !== 'string') {
      unsupported('unguarded Memory vector write');
    }
    if (meta.embedding_signature !== request.expected.signature) return false;
    const values = vectorValues(request.vector, meta.active_dim);
    const result = await memories.updateOne({ id: request.memoryId, content: request.expected.content }, {
      $set: { _ha_vector: { signature: meta.embedding_signature, values }, needs_reindex: 0 },
    }, options);
    return result.matchedCount === 1;
  }
  if (request.action === 'search' || request.action === 'hybrid') {
    if(request.expectedSignature!=null&&request.expectedSignature!==meta.embedding_signature)changed();
    const vector = vectorValues(request.vector, meta.active_dim);
    if (!Number.isInteger(request.topK) || request.topK < 1 || request.topK > 1000) throw new RangeError('topK must be 1..1000');
    const filter = { '_ha_vector.signature': meta.embedding_signature };
    if (request.apiKeyId != null) filter.api_key_id = request.apiKeyId;
    const squared = { $map: { input: { $zip: { inputs: ['$_ha_vector.values', { $literal: vector }] } }, as: 'pair',
      in: { $pow: [{ $subtract: [{ $arrayElemAt: ['$$pair', 0] }, { $arrayElemAt: ['$$pair', 1] }] }, 2] } } };
    const hits = await memories.aggregate([
      { $match: filter },
      ...scopeStages(request),
      { $set: { distance: { $sqrt: { $sum: squared } } } },
      { $sort: { distance: 1, id: 1 } }, { $limit: request.topK },
      { $project: { _id: 0, memoryId: '$id', distance: 1, score: { $divide: [1, { $add: [1, '$distance'] }] } } },
    ], options).toArray();
    return request.action === 'search' ? hits : fuseResults(hits, await lexicalRows(db, session, request), request.rrfK, request.topK);
  }
  unsupported(`native Memory vector operation ${request.action}`);
}
