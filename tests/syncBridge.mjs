import assert from 'node:assert/strict';
import { SyncBridge } from '../adapter/syncBridge.mjs';

const fixture = new URL('./bridgeFixture.mjs', import.meta.url);
const bridge = new SyncBridge(fixture, { timeoutMs: 2000, responseBytes: 2048 });
try {
  const value = { id: 42n, data: Buffer.from([0, 255]), missing: undefined, empty: null };
  assert.deepEqual(bridge.call({ value }), value);
  const retained = Array.from({ length: 200 }, (_, id) => bridge.call({ value: { id, data: Buffer.alloc(128, id % 256) } }));
  for (let id = 0; id < retained.length; id++) assert.deepEqual(retained[id], { id, data: Buffer.alloc(128, id % 256) });
  assert.throws(() => bridge.call({ operation: 'error' }), { code: 'TEST_FAILURE' });
  assert.equal(bridge.call({ value: 'still usable after a known error' }), 'still usable after a known error');
  assert.throws(() => bridge.call({ value: 'x'.repeat(4096) }), { code: 'HA_RESULT_LIMIT', outcomeUnknown: true });
  assert.throws(() => bridge.call({ value: 1 }), { code: 'HA_BRIDGE_CLOSED' });
} finally {
  await bridge.close();
}
const slow = new SyncBridge(fixture, { timeoutMs: 500 });
try {
  slow.call({ value: 'ready' });
  assert.throws(() => slow.call({ operation: 'wait', ms: 2000 }), { code: 'HA_BRIDGE_TIMEOUT', outcomeUnknown: true });
  assert.throws(() => slow.call({ value: 1 }), { code: 'HA_BRIDGE_CLOSED' });
} finally {
  await slow.close();
}
const crash = new SyncBridge(fixture, { timeoutMs: 500 });
try {
  crash.call({ value: 'ready' });
  assert.throws(() => crash.call({ operation: 'crash' }), { code: 'HA_BRIDGE_TIMEOUT' });
} finally {
  await crash.close();
}
console.log('PASS sync bridge: typed results, errors, bounded failures, poisoned connections');
