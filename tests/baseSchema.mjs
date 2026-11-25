import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readBaseSchema, checkBaseSchema } from '../adapter/baseSchema.mjs';
import { compileTable, tableInfo } from '../adapter/schema.mjs';
import { parse } from '../adapter/sql.mjs';
import {assertRetentionMarkerSchema} from '../adapter/retentionQueries.mjs';

const plan = readBaseSchema(), oracle = new DatabaseSync(':memory:');
try {
  for (const sql of plan.statements) oracle.exec(sql);
  assert.equal(plan.tables.size, 17); assert.equal(plan.indexes.length, 22);
  const catalog = new Map(), specs = [...plan.tables.values()];
  for (const [name, spec] of plan.tables) {
    const actual = oracle.prepare('SELECT * FROM pragma_table_info(?)').all(name).map(row => ({ ...row }));
    assert.deepEqual(tableInfo(spec), actual, name);
    const indexList = oracle.prepare('SELECT * FROM pragma_index_list(?)').all(name);
    const columns = idx => oracle.prepare('SELECT name, "desc" AS reverse FROM pragma_index_xinfo(?) WHERE "key" = 1 ORDER BY seqno').all(idx.name);
    for (const unique of indexList.filter(i => i.unique)) {
      assert.ok(spec.unique.some(key => JSON.stringify(key) === JSON.stringify(columns(unique).map(col => col.name))), unique.name);
    }
    catalog.set(name, indexList.filter(idx => idx.origin === 'c').map(idx => ({
      name: idx.name, key: Object.fromEntries(columns(idx).map(row => [row.name, row.reverse ? -1 : 1])),
    })));
  }
  assert.deepEqual(checkBaseSchema(plan, specs, catalog), { tables: 17, indexes: 22 });
  assert.throws(() => checkBaseSchema(plan, [], catalog), { code: 'HA_SCHEMA_MISMATCH' });
  const incomplete = new Map(catalog); incomplete.delete('api_keys');
  assert.throws(() => checkBaseSchema(plan, specs, incomplete), { code: 'HA_SCHEMA_MISMATCH' });
  const drift = structuredClone(specs);
  drift.find(s => s._id === 'api_keys').unique = [];
  assert.throws(() => checkBaseSchema(plan, drift, catalog), /unique key/);
  const index = catalog.get('domain_budget_reset_logs').find(i => i.name === 'idx_dbrl_key_reset');
  index.key.reset_at = 1;
  assert.throws(() => checkBaseSchema(plan, specs, catalog), /idx_dbrl_key_reset/);
  assert.throws(() => readBaseSchema('changed source'), /digest mismatch/);
  for (const ddl of [
    'CREATE TABLE timestamp_metadata(id INTEGER PRIMARY KEY, created TEXT DEFAULT CURRENT_TIMESTAMP)',
    'CREATE TABLE boolean_metadata(id TEXT PRIMARY KEY, enabled BOOLEAN DEFAULT 1)',
    'CREATE TABLE without_rowid_metadata(a TEXT,b TEXT,PRIMARY KEY(a,b)) WITHOUT ROWID',
  ]) {
    oracle.exec(ddl);
    const spec = compileTable(parse(ddl).ast);
    assert.deepEqual(tableInfo(spec), oracle.prepare('SELECT * FROM pragma_table_info(?)').all(spec._id).map(row => ({ ...row })));
  }
  console.log('PASS additional SQLite schema metadata: CURRENT_TIMESTAMP, BOOLEAN and WITHOUT ROWID');
  console.log('PASS upstream base schema: 17 tables and 22 indexes vs SQLite metadata; missing collections, key and index drift rejected');
} finally { oracle.close(); }

// Validate the fresh profile against SQLite before allocating MongoDB collections.
// Native replacement markers are counted here; integration tests verify behavior.
process.env.DATA_DIR = new URL('../.work/', import.meta.url).pathname;
process.env.DISABLE_SQLITE_AUTO_BACKUP = '1';
await import('../adapter/register.mjs');
const { provisionStartupSchema } = await import('../adapter/startupSchema.mjs');
const profile = new DatabaseSync(':memory:');
const native = [];
const facade = {
  prepare: profile.prepare.bind(profile), exec: profile.exec.bind(profile),
  verifyBaseSchema() { assert.equal(profile.prepare("SELECT count(*) AS n FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").get().n, 17); },
  installRequestDetailRetention() { native.push('retention'); },
  installFreeProxyCleanup() { native.push('free-proxy'); },
  installMemoryTextIndex() { native.push('text'); },
  installRequestLimits() { native.push('admission'); },
  assertCompatible() {},
};
globalThis.__omnirouteMongoAdapter = facade;
const log = console.log;
try {
  console.log = () => {}; // Column-addition chatter; exceptions still fail the test.
  await provisionStartupSchema(facade);
  const usageDdl=profile.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='usage_history'").get().sql;
  assertRetentionMarkerSchema(compileTable(parse(usageDdl).ast));
  assert.deepEqual(native, ['free-proxy','retention', 'text', 'admission']);
  for (const column of ['endpoint', 'cpa_auth_index', 'provider_credits', 'provider_cost_usd']) {
    assert.ok(profile.prepare('PRAGMA table_info(usage_history)').all().some(row => row.name === column));
  }
  for (const table of ['plugins', 'skills', 'quota_allocations', 'compression_combos']) {
    assert.ok(profile.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(table));
  }
} finally {
  console.log = log;
  delete globalThis.__omnirouteMongoAdapter;
  profile.close();
}
console.log('PASS fresh startup profile SQL against in-memory SQLite; native replacement behavior requires MongoDB integration tests');
const constrained=new DatabaseSync(':memory:');
try {
  for(const extra of ["team_rollup_processed_at TEXT CHECK(team_rollup_processed_at = 'allowed')","team_rollup_processed_at TEXT, other INTEGER CHECK(team_rollup_processed_at IS NULL)"]) {
    const ddl='CREATE TABLE marker_constraint(id INTEGER PRIMARY KEY,'+extra+')';
    constrained.exec(ddl);constrained.exec('INSERT INTO marker_constraint(id) VALUES (1)');
    assert.throws(()=>constrained.prepare('UPDATE marker_constraint SET team_rollup_processed_at=?').run('not-allowed'),/CHECK constraint/);
    assert.throws(()=>assertRetentionMarkerSchema(compileTable(parse(ddl).ast)),{code:'HA_UNSUPPORTED_SQL'});
    constrained.exec('DROP TABLE marker_constraint');
  }
} finally {constrained.close();}
console.log('PASS native rollup marker constraint gate: actual startup schema accepted; SQLite CHECKs on marker or another column cannot be bypassed by native update');
