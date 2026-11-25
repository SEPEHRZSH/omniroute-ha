import { SyncBridge } from './syncBridge.mjs';
import { splitStatements, unsupported } from './sql.mjs';
import { quotaBounds } from './reservations.mjs';

/** Synchronous OmniRoute database interface backed by a MongoDB worker. */
export class MongoAdapter {
  constructor(config) {
    this.driver = 'mongodb';
    this.name = config.database;
    this.inTransaction = false;
    this.changedTables = new Set();
    this.requiresOwnership = !!config.ownerId;
    this.quotaBounds = quotaBounds(config.quotaBounds ?? process.env.HA_QUOTA_BOUNDS);
    this.bridge = new SyncBridge(new URL('./mongoWorker.mjs', import.meta.url), { config: { ...config, quotaBounds: this.quotaBounds }, timeoutMs: config.timeoutMs ?? 10000 });
    try { this.bridge.call({ operation: 'ping' }); }
    catch (error) { this.bridge.close(); throw error; }
    this.raw = Object.freeze({ loadExtension: () => this.reject('SQLite native extension; requires an explicit domain overlay') });
  }
  reject(feature) { this.compatibilityFault = feature; unsupported(feature); }
  assertCompatible() {
    if (this.compatibilityFault) throw Object.assign(new Error('MongoDB compatibility fault; restart after correcting the unsupported operation'), { code: 'HA_COMPATIBILITY_FAULT' });
  }
  get open() { return this.bridge.open; }
  notifyCommitted(tables) {
    if (!tables.length || !this.onCommitted) return;
    // Cache invalidation must precede the next same-turn read. Redis transport
    // can return a promise, but cannot delay local read-your-writes visibility.
    try { Promise.resolve(this.onCommitted(tables)).catch(() => {}); } catch {}
  }
  ownership(action, leaseMs) { return this.bridge.call({ operation: 'ownership', action, leaseMs }); }
  storageStatus() { this.assertCompatible(); return this.bridge.call({operation:'storageStatus'}); }
  admitRequest(keyId, rules) { this.assertCompatible(); return this.bridge.call({ operation: 'admitRequest', keyId, rules }); }
  checkQuotaPolicy(keyId) { this.assertCompatible(); return this.bridge.call({ operation: 'checkQuotaPolicy', keyId }); }
  reservation(action, values) {
    this.assertCompatible();
    try { return this.bridge.call({ operation: 'reservation', action, ...values }); }
    catch (error) { if (['HA_UNSUPPORTED_SQL', 'SQLITE_ERROR','HA_SCHEMA_MISMATCH'].includes(error.code)) this.compatibilityFault = error.message; throw error; }
  }
  credential(action,values) {
    this.assertCompatible();const result=this.bridge.call({operation:'credential',action,...values});
    const changed=action==='publish'&&result?.published===true ||
      action==='recover'&&result?.recovered===true&&!result.idempotent&&result.connections>0 ||
      action==='kiroStage'&&result?.stage==='registration_published';
    if(changed){
      if(this.inTransaction)this.changedTables.add('provider_connections');else this.notifyCommitted(['provider_connections']);
    }
    return result;
  }
  installRequestLimits() { this.assertCompatible(); return this.bridge.call({ operation: 'installRequestLimits' }); }
  installMemoryTextIndex() { this.assertCompatible(); return this.bridge.call({ operation: 'installMemoryTextIndex' }); }
  installRequestDetailRetention() { this.assertCompatible(); return this.bridge.call({ operation: 'installRequestDetailRetention' }); }
  installFreeProxyCleanup() { this.assertCompatible(); return this.bridge.call({operation:'installFreeProxyCleanup'}); }
  verifyBaseSchema({runtime=false}={}) {
    this.assertCompatible();
    try { return this.bridge.call({ operation: 'verifyBaseSchema',runtime }); }
    catch (error) { if (error.code === 'HA_SCHEMA_MISMATCH') this.compatibilityFault = error.message; throw error; }
  }
  serverTime() { this.assertCompatible(); return this.bridge.call({ operation: 'clock' }); }
  canWrite() {
    if (!this.open || this.compatibilityFault) return false;
    if (!this.requiresOwnership || this.inTransaction) return true;
    try { return this.ownership('status'); } catch { return false; }
  }
  healthy() {
    if (this.compatibilityFault || !this.open) return false;
    try { return this.bridge.call({ operation: 'ping' }).ok === 1; } catch { return false; }
  }
  statements(sql) {
    this.assertCompatible();
    try { return splitStatements(sql); }
    catch (error) { if (error.code === 'HA_UNSUPPORTED_SQL') this.compatibilityFault = error.message; throw error; }
  }
  prepare(sql) {
    if (this.statements(sql).length !== 1) this.reject('multiple prepared statements');
    const invoke = (mode, args) => {
      this.assertCompatible();
      try {
        const credential=this.credentialScope?.();
        const result = this.bridge.call({ operation: 'statement', sql, args, mode,
          usageRollup:this.usageRollupScope?.(),
          usagePurge:this.usagePurgeScope?.(),
          credentialIntent:typeof credential==='string'?credential:credential?.importId?undefined:credential?.id,
          credentialImport:credential?.importId,
          credentialSnapshot:credential && typeof credential==='object' && !credential.id?{connectionId:credential.connectionId,version:credential.version}:undefined });
        if(result?._haCredentialVersion && credential && typeof credential==='object') {credential.version=result._haCredentialVersion;delete result._haCredentialVersion;}
        const changed = mode === 'run' ? result?.changes > 0 : mode === 'all' ? result?.length > 0 : result != null;
        if (changed) {
          const table = /^\s*(?:INSERT(?: OR (?:IGNORE|REPLACE))?\s+INTO|UPDATE|DELETE\s+FROM)\s+"?([A-Za-z][A-Za-z0-9_]*)/i.exec(sql)?.[1];
          if (table) {
            if (this.inTransaction) this.changedTables.add(table);
            else this.notifyCommitted([table]);
          }
        }
        return result;
      }
      catch (error) {
        // Upstream sometimes catches DB errors. Preserve an out-of-band readiness
        // failure rather than letting those catches disguise missing compatibility.
        if (['HA_UNSUPPORTED_SQL','SQLITE_ERROR','HA_SCHEMA_MISMATCH'].includes(error.code)) {
          this.compatibilityFault = error.message;
          this.onUnsupported?.(sql, error);
        }
        throw error;
      }
    };
    return { run: (...args) => invoke('run', args), get: (...args) => invoke('get', args), all: (...args) => invoke('all', args) };
  }
  exec(sql) {
    // Rollback remains available after a compatibility fault to release a real
    // transaction; ordinary SQL cannot run until a fresh adapter is created.
    if (/^\s*ROLLBACK\s*;?\s*$/i.test(sql)) { this.rollback(); return; }
    if (/^\s*(COMMIT|END)\s*;?\s*$/i.test(sql)) { this.commit(); return; }
    for (const statement of this.statements(sql)) {
      const control = statement.trim().toUpperCase();
      if (/^BEGIN(?: (?:IMMEDIATE|DEFERRED|EXCLUSIVE))?(?: TRANSACTION)?$/.test(control)) this.begin();
      else if (control === 'COMMIT' || control === 'END') this.commit();
      else if (control === 'ROLLBACK') this.rollback();
      else this.prepare(statement).run();
    }
  }
  begin() {
    this.assertCompatible();
    if (this.inTransaction) this.reject('nested transaction; savepoint support is not implemented');
    this.bridge.call({ operation: 'begin' });
    this.changedTables.clear();
    this.inTransaction = true;
  }
  commit() {
    if (this.compatibilityFault) {
      if (this.inTransaction && this.open) this.rollback();
      this.assertCompatible();
    }
    const tables=[...this.changedTables];
    try { this.bridge.call({ operation: 'commit' }); }
    finally { this.inTransaction = false; this.changedTables.clear(); }
    this.notifyCommitted(tables);
  }
  rollback() {
    try { this.bridge.call({ operation: 'rollback' }); }
    finally { this.inTransaction = false; this.changedTables.clear(); }
  }
  transaction(fn) {
    const db = this;
    function tx(...args) {
      db.begin();
      try {
        const value = fn.apply(this, args);
        if (value && typeof value.then === 'function') db.reject('async transaction callback');
        db.commit();
        return value;
      } catch (error) {
        if (db.inTransaction && db.open) {
          try { db.rollback(); } catch { db.close(); }
        }
        throw error;
      }
    }
    tx.immediate = tx;
    tx.deferred = tx;
    tx.exclusive = tx;
    return tx;
  }
  immediate(fn) { return this.transaction(fn)(); }
  pragma(sql, options) {
    const rows = this.prepare(`PRAGMA ${sql}`).all();
    return options?.simple ? Object.values(rows[0] ?? {})[0] : rows;
  }
  backup() { return Promise.reject(Object.assign(new Error('Use MongoDB backup tooling'), { code: 'HA_UNSUPPORTED_BACKUP' })); }
  checkpoint() { this.reject('SQLite checkpoint on MongoDB'); }
  close() { this.inTransaction = false; return this.bridge.close(); }
}
