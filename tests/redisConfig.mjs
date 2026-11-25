// Transport wiring only: no Redis endpoint or network is used by this test.
import assert from 'node:assert/strict';
import { Redis, Cluster } from 'ioredis';
import { createRedisAccess, redisConfiguration } from '../adapter/redis.mjs';
import { createCacheBus } from '../adapter/cacheBus.mjs';
import { EventEmitter } from 'node:events';
assert.equal(typeof Redis, 'function'); assert.equal(typeof Cluster, 'function');
let constructed = [];
class FakeClient {
  constructor(...args) { this.args = args; constructed.push(this); }
  on() {} async connect() {} disconnect() { this.disconnected = true; }
}
class FakeCluster extends FakeClient {}
const load = async () => ({ Redis: FakeClient, Cluster: FakeCluster });
const disabled = createRedisAccess({}, load);
assert.throws(() => disabled.get(), /disabled/); assert.equal(constructed.length, 0);
for (const mode of ['direct', 'sentinel', 'cluster']) {
  const env = { HA_REDIS_MODE: mode, HA_REDIS_NODES: '[{"host":"cache.invalid","port":26379}]',
    REDIS_URL: 'rediss://vip.invalid:6380', HA_REDIS_MASTER: 'ha-cache', HA_REDIS_TLS: 'true', HA_REDIS_SENTINEL_TLS: 'true' };
  const access = createRedisAccess(env, load);
  const [a,b] = await Promise.all([access.get(), access.get()]);
  assert.equal(a,b); assert.equal(access.enabled, true);
  if (mode === 'direct') assert.equal(a.args[0], env.REDIS_URL);
  if (mode === 'cluster') { assert.ok(a instanceof FakeCluster); assert.equal(a.args[1].redisOptions.keyPrefix, 'omniroute:'); }
  if (mode === 'sentinel') { assert.equal(a.args[0].role, 'master'); assert.ok(a.args[0].enableTLSForSentinelMode); }
  access.close(); assert.equal(a.disconnected, true); assert.throws(() => access.get(), /closed/);
}
assert.equal(constructed.length, 3);
for (const dataTls of [false, true]) for (const sentinelTls of [false, true]) {
  const config = redisConfiguration({ HA_REDIS_MODE: 'sentinel', HA_REDIS_NODES: '[{"host":"sentinel.invalid","port":26379}]',
    HA_REDIS_MASTER: 'synthetic', HA_REDIS_TLS: String(dataTls), HA_REDIS_SENTINEL_TLS: String(sentinelTls) });
  assert.equal(!!config.options.tls, dataTls);
  assert.equal(!!config.options.enableTLSForSentinelMode, dataTls, 'Data-server TLS must not depend on Sentinel discovery TLS');
  assert.equal(!!config.options.sentinelTLS, sentinelTls);
}
assert.throws(() => redisConfiguration({ HA_REDIS_MODE: 'cluster', HA_REDIS_NODES: '[]' }), /nodes/);
assert.throws(() => redisConfiguration({ HA_REDIS_MODE: 'sentinel', HA_REDIS_NODES: '[{"host":"x","port":1}]' }), /master/);
assert.throws(() => redisConfiguration({ HA_REDIS_MODE: 'direct', REDIS_URL: 'http://unexpected.invalid' }), /endpoint/);
console.log('PASS Redis client configuration and lazy lifecycle (mock transport): disabled, direct/VIP, Sentinel and Cluster; no live Redis validation');

const clients = [], sent = [];
class BusClient extends EventEmitter {
  constructor() { super(); clients.push(this); }
  async connect() { this.emit('ready'); }
  async subscribe(channel) { this.channel = channel; return 1; }
  async publish(channel, message) { sent.push(JSON.parse(message)); for (const client of clients) if (client.channel === channel) client.emit('message', channel, message); return 1; }
  disconnect() { this.emit('close'); }
}
const busLoad = async () => ({ Redis: BusClient, Cluster: BusClient });
const busEnv = { HA_REDIS_MODE: 'direct', REDIS_URL: 'redis://cache.invalid:6379', REDIS_KEY_PREFIX: 'isolated-test:' };
let owned = true, refreshed = 0;
const access = createRedisAccess(busEnv, busLoad);
const bus = createCacheBus({ env: busEnv, access, identity: 'synthetic-database', epoch: () => 3,
  owns: () => owned, invalidate: () => { refreshed++; }, load: busLoad });
await bus.start();
assert.ok(clients[0].channel.startsWith('isolated-test:ha:'));
await new Promise(resolve => setTimeout(resolve, 150));
assert.equal(refreshed, 1, 'Initial subscription invalidates possibly stale caches');
await bus.publish(['api_keys', 'provider_connections', 'not-watched']);
await new Promise(resolve => setTimeout(resolve, 150));
assert.equal(refreshed, 2); assert.deepEqual(sent[0], { v: 1, epoch: 3, tables: ['api_keys', 'provider_connections'] });
owned = false; await bus.publish(['api_keys']); assert.equal(sent.length, 1, 'Standby cannot publish state-change hints');
clients[0].emit('ready');
await new Promise(resolve => setTimeout(resolve, 150));
assert.equal(refreshed, 3, 'Reconnect invalidates after potentially missed messages');
clients[0].emit('message', clients[0].channel, 'not JSON');
assert.equal(sent.length, 1);
owned=true;await bus.publish(['proxy_assignments']);
await new Promise(resolve=>setTimeout(resolve,150));
assert.deepEqual(sent[1].tables,['proxy_assignments']);assert.equal(refreshed,4,'Proxy assignment hints invalidate the separate proxy configuration cache too');
bus.close(); access.close(); assert.equal(bus.status().connected, false);
console.log('PASS Redis invalidation bus (mock only): separate subscriber, credential-free messages, reconnect refresh, standby publish rejection and close');
