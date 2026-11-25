// Exact final upstream cleanup contract; arbitrary trigger SQL remains unsupported.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';

const source=readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/072_free_proxies_fk.sql',import.meta.url),'utf8');
assert.equal(createHash('sha256').update(source).digest('hex'),'50ad89c3aba5d3d389ff4fbec3e4f3b946fffb6dafe33acf15299ce5110a16da','Free Proxy cleanup source changed');
export const freeProxyTrigger=source.match(/CREATE TRIGGER trg_free_proxies_clear_pool_proxy_id[\s\S]*?END;/)?.[0];
assert.ok(freeProxyTrigger,'Free Proxy cleanup trigger anchor changed');
const body=freeProxyTrigger.match(/BEGIN\s*([\s\S]*?)\s*END;/)?.[1];
assert.equal((body.match(/OLD\.id/g)||[]).length,1,'Free Proxy cleanup binding changed');
export const freeProxyCleanupSql=body.replace('OLD.id','?');
export function assertFreeProxyCleanupSchema(registry,free) {
  assert.ok(registry?._id==='proxy_registry'&&registry.primaryKey?.includes('id'),'Free Proxy cleanup requires the registry primary key');
  for(const [name,type] of [['pool_proxy_id','TEXT'],['in_pool','INTEGER'],['updated_at','TEXT']])
    assert.ok(free?._id==='free_proxies'&&free.columns.some(column=>column.name===name&&column.type===type),'Free Proxy cleanup schema mismatch: '+name);
}
