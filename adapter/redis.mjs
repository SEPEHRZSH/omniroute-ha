// Optional cache transport; never authorizes a request, a Mongo write or an HA lease.
export function redisConfiguration(env) {
  const mode = env.HA_REDIS_MODE?.trim() || 'disabled';
  if (mode === 'disabled') return { mode };
  if (!['direct', 'sentinel', 'cluster'].includes(mode)) throw new Error('Invalid HA_REDIS_MODE');
  const options = { keyPrefix: env.REDIS_KEY_PREFIX?.trim() || 'omniroute:', connectTimeout: 2000, commandTimeout: 2000,
    maxRetriesPerRequest: 1, enableOfflineQueue: false, lazyConnect: true,
    ...(env.HA_REDIS_USERNAME ? { username: env.HA_REDIS_USERNAME } : {}),
    ...(env.HA_REDIS_PASSWORD ? { password: env.HA_REDIS_PASSWORD } : {}) };
  function tls(flag) {
    if (flag == null || flag === 'false') return undefined;
    if (flag !== 'true') throw new Error('Redis TLS flags must be true or false');
    return {};
  }
  const dataTls = tls(env.HA_REDIS_TLS);
  if (dataTls) options.tls = dataTls;
  if (mode === 'direct') {
    const url = new URL(env.REDIS_URL);
    if (!['redis:', 'rediss:'].includes(url.protocol) || !url.hostname) throw new Error('Invalid Redis endpoint');
    return { mode, url: url.href, options };
  }
  let nodes;
  try { nodes = JSON.parse(env.HA_REDIS_NODES); } catch { throw new Error('Expected HA_REDIS_NODES JSON array'); }
  if (!Array.isArray(nodes) || !nodes.length || nodes.length > 64 || nodes.some(n =>
    typeof n?.host !== 'string' || !n.host.trim() || !Number.isInteger(n.port) || n.port < 1 || n.port > 65535)) throw new Error('Invalid Redis nodes');
  nodes = nodes.map(({ host, port }) => ({ host, port }));
  if (mode === 'cluster') return { mode, nodes, options: { redisOptions: options, lazyConnect: true, enableOfflineQueue: false } };
  if (!env.HA_REDIS_MASTER?.trim()) throw new Error('Sentinel master name is required');
  const sentinelTls = tls(env.HA_REDIS_SENTINEL_TLS);
  return { mode, options: { ...options, sentinels: nodes, name: env.HA_REDIS_MASTER, role: 'master',
    // ioredis uses this flag for the resolved DATA server; sentinelTLS governs
    // the separate Sentinel discovery connection. Neither may weaken the other.
    ...(dataTls ? { enableTLSForSentinelMode: true } : {}), ...(sentinelTls ? { sentinelTLS: sentinelTls } : {}),
    sentinelUsername: env.HA_REDIS_SENTINEL_USERNAME, sentinelPassword: env.HA_REDIS_SENTINEL_PASSWORD } };
}

export function createRedisAccess(env, load = () => import('ioredis')) {
  const config = redisConfiguration(env);
  let pending, client, closed = false;
  return {
    enabled: config.mode !== 'disabled',
    get() {
      if (closed) throw new Error('Redis cache is closed');
      if (config.mode === 'disabled') throw new Error('Redis cache is disabled');
      return pending ??= (async () => {
        const { Redis, Cluster } = await load();
        if (closed) throw new Error('Redis cache is closed');
        client = config.mode === 'cluster' ? new Cluster(config.nodes, config.options) :
          config.mode === 'sentinel' ? new Redis(config.options) : new Redis(config.url, config.options);
        client.on('error', () => {}); // Commands surface errors; do not log credentials or full URLs.
        let timer;
        try {
          await Promise.race([client.connect(), new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error('Redis cache connection timed out')), 3000);
          })]);
          if (closed) throw new Error('Redis cache is closed');
          return client;
        } finally { clearTimeout(timer); }
      })().catch(error => { client?.disconnect(); client = undefined; pending = undefined; throw error; });
    },
    close() { closed = true; client?.disconnect(); },
  };
}
