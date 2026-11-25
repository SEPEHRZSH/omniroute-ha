import assert from 'node:assert/strict';
import { ActiveRuntime } from '../adapter/activeRuntime.mjs';
import { register } from '../upstream/omniroute/src/instrumentation.ts';
import { getServerLifecyclePhase } from '../upstream/omniroute/src/lib/serverLifecycle.ts';
import { workspacePath } from '../adapter/workspacePath.mjs';
import {openOriginalSqlite} from '../adapter/originalSqlite.mjs';
import { mkdtempSync, mkdirSync, symlinkSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const pathFixture = mkdtempSync(path.resolve('.work/path-guard-'));
try {
  mkdirSync(path.join(pathFixture,'data'));
  writeFileSync(path.join(pathFixture,'evidence.json'),'{}');
  symlinkSync(path.resolve('upstream'),path.join(pathFixture,'link'));
  assert.equal(workspacePath(path.join(pathFixture,'data'),{directory:true}),path.join(pathFixture,'data'));
  assert.equal(workspacePath(path.join(pathFixture,'new.bson'),{existing:false}),path.join(pathFixture,'new.bson'));
  for (const invalid of [path.resolve('upstream'),path.resolve('..'),path.join(pathFixture,'link/anything')]) assert.throws(()=>workspacePath(invalid,{directory:true}),/workspace|upstream|Symlink/);
  assert.throws(()=>workspacePath(path.join(pathFixture,'evidence.json'),{directory:true}),/type/);
  await assert.rejects(openOriginalSqlite(pathFixture),/Never overwrite/);
  globalThis.__omnirouteMongoAdapter={};
  try{await assert.rejects(openOriginalSqlite(path.join(pathFixture,'data')),/cannot run in an HA application/);}
  finally{delete globalThis.__omnirouteMongoAdapter;}
} finally { rmSync(pathFixture,{recursive:true}); }
console.log('PASS workspace paths: private directories/files, upstream/outside/type rejection and no symlink traversal');
console.log('PASS offline original schema isolation: nonempty source refused before loading SQLite; an HA application cannot invoke generation');

process.env.NEXT_RUNTIME = 'nodejs';
const originalFetch = globalThis.fetch;
await assert.rejects(register(), /Fenced HA runtime/);
assert.equal(globalThis.fetch, originalFetch);

// Exercise the actual top-level registration paths. The clock/lease below
// is a lifecycle fixture only; this does not test MongoDB or a positive boot.
function lifecycleDb() {
  return {
    requiresOwnership: true, owner: false, valid: true, closed: false, renewals: 0,
    healthy() { return this.valid && !this.closed; },
    verifyBaseSchema() { if (!this.valid) throw new Error('Incomplete schema'); },
    canWrite() { return this.owner && this.healthy(); },
    close() { this.closed = true; },
    ownership(action) {
      if (action === 'claim') return this.owner ? { epoch: 1 } : null;
      if (action === 'status') return this.canWrite();
      if (action === 'renew') {
        if (!this.canWrite()) throw new Error('Lost epoch');
        this.renewals++; return;
      }
      throw new Error('Unexpected lease operation');
    },
  };
}
const db = lifecycleDb();
const reasons = [];
const options = { intervalMs: 50, leaseMs: 300, warm: async () => {}, recycle: reason => reasons.push(reason) };
const standby = new ActiveRuntime(db, options);
globalThis.__omnirouteActiveRuntime = standby;
try {
  await register(); await register();
  assert.equal(standby.ready(), false); assert.equal(getServerLifecyclePhase(), 'starting');
  assert.equal(globalThis.fetch, originalFetch, 'Standby must not install the global proxy fetch patch');
  db.valid = false;
  await assert.rejects(async () => new ActiveRuntime(db, options).register(() => {}), /Incomplete schema/);
} finally { standby.retire('test stop'); delete globalThis.__omnirouteActiveRuntime; }

const activeDb = lifecycleDb();
const active = new ActiveRuntime(activeDb, options);
let entries = 0, finish;
const blockedBoot = new Promise(resolve => { finish = resolve; });
active.register(async () => { entries++; await blockedBoot; });
activeDb.owner = true;
const promoting = active.owner.step();
await new Promise(resolve => setTimeout(resolve, 170));
assert.equal(entries, 1); assert.equal(active.ready(), false); assert.ok(activeDb.renewals >= 2);
finish(); await promoting;
assert.equal(active.ready(), true);
activeDb.owner = false;
assert.equal(active.ready(), false);
assert.ok(activeDb.closed); assert.ok(active.retired);
assert.throws(() => active.register(() => {}), /Retired/);
assert.equal(reasons.length, 2);
console.log('PASS actual upstream instrumentation: missing controller rejects boot; standby registers without global fetch patch or ready state');
console.log('PASS lifecycle unit: long promotion renews the lease, readiness waits for completion, ownership loss closes the adapter and retires the process');
