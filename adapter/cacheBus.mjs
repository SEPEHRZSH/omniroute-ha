// Best-effort Redis invalidation hints, never authorization or write ownership.
// Messages contain table names/epochs only; no credentials or cached payloads.
import { createHash } from 'node:crypto';
import { createRedisAccess } from './redis.mjs';
const watched = new Set(['key_value', 'api_keys', 'provider_connections', 'provider_nodes', 'memories','proxy_registry','proxy_assignments','proxy_scope_rotation']);
export function createCacheBus({ env, access, identity, epoch, owns, invalidate, load }) {
  const channel = (env.REDIS_KEY_PREFIX?.trim() || 'omniroute:') + 'ha:' + createHash('sha256').update(identity).digest('hex').slice(0, 24);
  const subscriberAccess = createRedisAccess(env, load);
  let closed = false, retry, refresh, subscribeRetry, connected = false, starting = false, started = false;
  function stale() {
    if (closed || refresh) return;
    refresh = setTimeout(() => { refresh = undefined; if (!closed) Promise.resolve().then(invalidate).catch(() => {}); }, 100);
    refresh.unref?.();
  }
  async function start() {
    if (closed || !access.enabled || starting || started) return;
    starting = true;
    try {
      const sub = await subscriberAccess.get();
      if (closed) return;
      const subscribe = () => {
        clearTimeout(subscribeRetry);
        if (!closed) void sub.subscribe(channel).then(() => { connected = true; stale(); }).catch(() => {
          connected = false;
          if (!closed) { subscribeRetry = setTimeout(subscribe, 3000); subscribeRetry.unref?.(); }
        });
      };
      sub.on('ready', subscribe); // Reconnect invalidates after any missed messages.
      sub.on('close', () => { connected = false; });
      sub.on('message', (target, text) => {
        if (target !== channel || text.length > 2048) return;
        try {
          const message = JSON.parse(text);
          if (message.v === 1 && Number.isSafeInteger(message.epoch) && Array.isArray(message.tables) && message.tables.some(t => watched.has(t))) stale();
        } catch { /* Untrusted cache hints cannot mutate application state. */ }
      });
      started = true; subscribe();
    } catch {
      connected = false;
      if (!closed) { retry = setTimeout(start, 3000); retry.unref?.(); }
    } finally { starting = false; }
  }
  return {
    start,
    async publish(tables) {
      const selected = [...new Set(tables)].filter(t => watched.has(t));
      if (closed || !access.enabled || !selected.length || !owns()) return;
      try { const client = await access.get(); if (!closed && owns()) await client.publish(channel, JSON.stringify({ v: 1, epoch: epoch(), tables: selected })); }
      catch { /* Reads/promotion still validate against MongoDB. */ }
    },
    status() { return { enabled: access.enabled, connected }; },
    close() { closed = true; connected = false; clearTimeout(retry); clearTimeout(refresh); clearTimeout(subscribeRetry); subscriberAccess.close(); },
  };
}
