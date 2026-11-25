import assert from 'node:assert/strict';
import { readFileSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { testDatabase } from './mongoTestSupport.mjs';
import { installBaseSchema } from '../adapter/baseSchema.mjs';

const test = await testDatabase('quota');
const dataDir = path.resolve('.work/quota-data'); mkdirSync(dataDir, { recursive: true });
Object.assign(process.env, {
  NODE_ENV: 'test', DATA_DIR: dataDir, DISABLE_SQLITE_AUTO_BACKUP: '1',
  OMNIROUTE_DISABLE_BACKGROUND_SERVICES: '1', OMNIROUTE_DISABLE_REDIS_AUTH_CACHE: '1',
  REDIS_URL: '', QDRANT_HOST: '', QUOTA_STORE_DRIVER: 'redis', QUOTA_STORE_REDIS_URL: '',
});
const config = { uri: test.uri, database: test.database, quotaBounds: {} };
const bootstrap = new MongoAdapter(config);
let active, standby;
const realNow = Date.now;
try {
  installBaseSchema(bootstrap);
  bootstrap.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/078_quota_consumption.sql', 'utf8'));
  bootstrap.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/182_request_cost_ledger_and_key_quota.sql', 'utf8'));
  bootstrap.installRequestLimits();
  await bootstrap.close();
  active = new MongoAdapter({ ...config, ownerId: 'synthetic-quota-primary' });
  standby = new MongoAdapter({ ...config, ownerId: 'synthetic-quota-standby' });
  globalThis.__omnirouteMongoAdapter = active;
  active.ownership('claim', 10000);
  const { getQuotaStore, getQuotaStoreSync } = await import('../upstream/omniroute/src/lib/quota/storeFactory.ts');
  const { SqliteQuotaStore } = await import('../upstream/omniroute/src/lib/quota/sqliteQuotaStore.ts');
  const { WINDOW_MS } = await import('../upstream/omniroute/src/lib/quota/dimensions.ts');
  const store = await getQuotaStore();
  assert.ok(store instanceof SqliteQuotaStore, 'The real SQL-facing quota implementation must be used');
  assert.equal(getQuotaStoreSync(), store);
  const dim = { poolId: 'synthetic-pool', unit: 'tokens', window: 'daily' };
  const started = active.serverTime();
  // Deliberate host-clock skew must not allocate counters to a different window.
  Date.now = () => realNow() + 365 * 86400000;
  const consumed = await Promise.all(Array.from({ length: 20 }, () => store.consume('tenant-a', dim, 1)));
  assert.deepEqual(consumed, Array.from({ length: 20 }, (_, i) => i + 1));
  await store.consume('tenant-b', dim, 7);
  assert.equal(await store.peek('tenant-a', dim), 20);
  assert.equal(await store.poolConsumedTotal(dim.poolId, dim), 27);
  Date.now = realNow;
  const { recordCost } = await import('../upstream/omniroute/src/domain/costRules.ts');
  const { spendBatchWriter } = await import('../upstream/omniroute/src/lib/spend/batchWriter.ts');
  recordCost('synthetic-id', 0.125);
  assert.equal(spendBatchWriter.getPendingCostTotal('synthetic-id'), 0);
  assert.equal((await test.db.collection('domain_cost_history').findOne({ api_key_id: 'synthetic-id' })).cost, 0.125);
  const rows = await test.db.collection('quota_consumption').find({}).toArray();
  const expectedBucket = Math.floor(started / WINDOW_MS.daily);
  assert.ok(rows.every(row => row.bucket_index === expectedBucket));
  assert.ok(rows.every(row => Math.abs(row.updated_at - realNow()) < 10000));
  const { checkRateLimit } = await import('../upstream/omniroute/src/shared/utils/rateLimiter.ts');
  active.prepare('INSERT INTO api_key_quota_limits(api_key_id,rpm_limit) VALUES (?,?)').run('rpm-key', 3);
  const rpmResults = await Promise.all(Array.from({ length: 12 }, () => checkRateLimit('rpm-key', [])));
  assert.equal(rpmResults.filter(result => result.allowed).length, 3, 'RPM admission must reserve before dispatch, even with no custom rate rules');
  active.prepare('INSERT INTO api_key_quota_limits(api_key_id,rpm_limit) VALUES (?,?)').run('rpm-history-key', 3);
  active.prepare('INSERT INTO api_key_quota_counters(api_key_id,dimension_key,bucket_index,consumed,updated_at) VALUES (?,?,?,?,?)')
    .run('rpm-history-key', 'key-quota:rpm', Math.floor(active.serverTime() / 60000), 2, active.serverTime());
  const withHistory = await Promise.all(Array.from({ length: 3 }, () => checkRateLimit('rpm-history-key', [])));
  assert.equal(withHistory.filter(result => result.allowed).length, 1, 'Existing completed usage must not be forgotten when enabling RPM admission');
  active.prepare('INSERT INTO api_key_quota_limits(api_key_id,rpm_limit) VALUES (?,?)').run('rpm-boundary-key', 2);
  const boundaryBucket = Math.floor(active.serverTime() / 60000);
  await test.db.collection('_ha_request_limits').insertOne({ _id: 'rpm-boundary-key', windows: [], rpmInitialized: true,
    rpm: [{ bucket: boundaryBucket - 1, count: 1 }], expiresAt: new Date(active.serverTime() + 120000) });
  active.prepare('INSERT INTO api_key_quota_counters(api_key_id,dimension_key,bucket_index,consumed,updated_at) VALUES (?,?,?,?,?)')
    .run('rpm-boundary-key', 'key-quota:rpm', boundaryBucket, 1, active.serverTime());
  assert.equal((await checkRateLimit('rpm-boundary-key', [])).allowed, true, 'A completion across a minute boundary must not double-charge durable admission');
  assert.equal((await checkRateLimit('rpm-boundary-key', [])).allowed, false);
  assert.equal((await checkRateLimit('rpm-enable-later', [])).allowed, true);
  active.prepare('INSERT INTO api_key_quota_limits(api_key_id,rpm_limit) VALUES (?,?)').run('rpm-enable-later', 1);
  assert.equal((await checkRateLimit('rpm-enable-later', [])).allowed, false, 'Enabling RPM must retain previously unlimited admissions');
  active.prepare('INSERT INTO api_key_quota_limits(api_key_id,tpm_limit) VALUES (?,?)').run('reservation-key', 100);
  await assert.rejects(async () => checkRateLimit('reservation-key', []), { code: 'HA_RESERVATION_REQUIRED' });
  assert.equal(await test.db.collection('_ha_request_limits').countDocuments({ _id: 'reservation-key' }), 0);
  const rules = [{ window: 86400, limit: 9 }, { window: 3600, limit: 3 }];
  const admissions = await Promise.all(Array.from({ length: 12 }, () => checkRateLimit('synthetic-id', rules)));
  assert.equal(admissions.filter(r => r.allowed).length, 3);
  const rejected = admissions.find(r => !r.allowed);
  assert.equal(rejected.failedWindow, 3600); assert.ok(rejected.resetAt > realNow());
  assert.deepEqual((await test.db.collection('_ha_request_limits').findOne({ _id: 'synthetic-id' })).windows.map(w => w.count), [3, 3]);
  globalThis.__omnirouteMongoAdapter = standby;
  recordCost('synthetic-id', 100);
  assert.equal(await test.db.collection('domain_cost_history').countDocuments({ api_key_id: 'synthetic-id' }), 1);
  await assert.rejects(store.consume('tenant-a', dim, 100), { code: 'HA_FENCED' });
  assert.equal(await store.peek('tenant-a', dim), 20);
  active.ownership('release'); standby.ownership('claim', 10000);
  assert.equal((await checkRateLimit('rpm-key', [])).allowed, false, 'RPM admission survives ownership promotion');
  recordCost('synthetic-id', 0.5);
  assert.equal(await test.db.collection('domain_cost_history').countDocuments({ api_key_id: 'synthetic-id' }), 2);
  assert.equal(spendBatchWriter.getPendingCostTotal('synthetic-id'), 0);
  assert.equal((await checkRateLimit('synthetic-id', rules)).allowed, false);
  assert.equal(await store.consume('tenant-a', dim, 2), 22);
  assert.equal(await store.poolConsumedTotal(dim.poolId, dim), 29);
  await store.clear('tenant-b', dim);
  assert.equal(await store.poolConsumedTotal(dim.poolId, dim), 22);
  globalThis.__omnirouteMongoAdapter = active;
  await assert.rejects(store.consume('tenant-a', dim, 100), { code: 'HA_FENCED' });
  assert.equal(active.compatibilityFault, undefined); assert.equal(standby.compatibilityFault, undefined);
  console.log('PASS actual QuotaStore: MongoDB authoritative even with Redis selected, concurrent consumption, pool totals, database-clock buckets, standby fencing and promotion');
  console.log('PASS hard per-key request admission: concurrent requests cannot exceed configured windows, rejection does not consume other windows, limits survive promotion');
  console.log('PASS hard RPM quota: atomic admission, historic seed, minute-boundary completion deduplication, enabling a limit after unlimited traffic and promotion');
  console.log('PASS real cost recording: synchronous majority-committed spend, no pending process buffer, standby fencing and promotion');
  console.log('PASS shared-pool counters; reservation policies fail closed without certified bounds (bounded admission is covered separately)');
} finally {
  Date.now = realNow;
  await bootstrap.close(); await active?.close(); await standby?.close();
  delete globalThis.__omnirouteMongoAdapter; await test.cleanup();
}
