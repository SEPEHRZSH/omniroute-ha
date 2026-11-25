import { unsupported } from './sql.mjs';

/** Implements OmniRoute's VectorStore interface without loading sqlite-vec. */
export class MongoMemoryVectors {
  constructor(adapter) { this.adapter = adapter; }
  call(action, values = {}) {
    this.adapter.assertCompatible();
    try {
      const result=this.adapter.bridge.call({ operation: 'memoryVector', action, ...values });
      if(action==='deleteDecayed'&&result) {
        if(this.adapter.inTransaction)this.adapter.changedTables.add('memories');
        else this.adapter.notifyCommitted(['memories']);
      }
      return result;
    }
    catch (error) {
      if (error.code === 'HA_UNSUPPORTED_SQL') this.adapter.compatibilityFault = error.message;
      throw error;
    }
  }
  async ensureReady(resolution) {
    if (process.env.MEMORY_VEC_QUANTIZATION && process.env.MEMORY_VEC_QUANTIZATION !== 'none') {
      this.adapter.compatibilityFault = 'MongoDB vector quantization is not implemented';
      unsupported(this.adapter.compatibilityFault);
    }
    let revision;
    if (resolution.source) {
      const [{ getSettingsRevision }, { getMemorySettings }, { resolveEmbeddingSource, withMeasuredDimensions }] = await Promise.all([
        import('../upstream/omniroute/src/lib/db/settings.ts'),
        import('../upstream/omniroute/src/lib/memory/settings.ts'),
        import('../upstream/omniroute/src/lib/memory/embedding/index.ts'),
      ]);
      const before = await getSettingsRevision();
      const current = withMeasuredDimensions(resolveEmbeddingSource(await getMemorySettings()), resolution.dimensions);
      revision = await getSettingsRevision();
      if (before !== revision || current.signature !== resolution.signature) return { ready: false, reason: 'Embedding configuration changed; retry with fresh settings' };
    }
    const ready = this.call('configure', { signature: resolution.signature, dim: resolution.dimensions, configurationRevision: revision });
    if (ready.ready) this.configurationRevision = revision;
    return {...ready,...(ready.ready?{queryContext:{expectedSignature:ready.signature??resolution.signature,configurationRevision:revision}}:{})};
  }
  async resetForSignature(signature, dim) { this.call('reset', { signature, dim }); }
  async upsertVector(memoryId, vector, expected) {
    if (typeof expected?.content !== 'string' || typeof expected?.signature !== 'string') {
      return this.adapter.reject('Memory vector writes require the embedded content and model signature');
    }
    return this.call('upsert', { memoryId, vector, expected: { ...expected, configurationRevision: this.configurationRevision } });
  }
  async deleteVector(memoryId) { this.call('delete', { memoryId }); }
  async deleteDecayedMemory(memoryId,config,now) { return this.call('deleteDecayed',{memoryId,config,now}); }
  async searchVector(vector, topK, apiKeyId, scope = {}) { return this.call('search', { ...scope, vector, topK, apiKeyId }); }
  lexical(query, topK, apiKeyId, scope = {}) { return this.call('lexical', { ...scope, query, topK, apiKeyId }); }
  textReady() { return this.call('textStatus'); }
  async searchHybrid(vector, queryText, topK, apiKeyId, scope = {}) {
    const { sanitizeFts5Query } = await import('../upstream/omniroute/src/lib/memory/retrieval/scoring.ts');
    return this.call('hybrid', { ...scope, vector, query: sanitizeFts5Query(queryText), topK, apiKeyId, rrfK: Number(process.env.MEMORY_RRF_K ?? 60) });
  }
  async stats() { return this.call('stats'); }
}
