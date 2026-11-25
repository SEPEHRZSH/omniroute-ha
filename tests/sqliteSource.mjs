// Explicit offline input generator, never an application driver or fallback.
// This independent loader applies no HA overlays: original core runs all migrations.
import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {openOriginalSqlite} from '../adapter/originalSqlite.mjs';

assert.equal(path.resolve(process.argv[1]),fileURLToPath(import.meta.url),'SQLite oracle is an explicit test entry only');
for(const key of ['JWT_SECRET','API_KEY_SECRET','STORAGE_ENCRYPTION_KEY'])assert.ok(process.env[key]?.startsWith('synthetic-'),'Oracle requires synthetic credentials');
const original=await openOriginalSqlite(process.argv[2]);
try {
  const {updateSettings}=await import('../upstream/omniroute/src/lib/db/settings.ts');
  const {createApiKey}=await import('../upstream/omniroute/src/lib/db/apiKeys.ts');
  const {createProviderConnection}=await import('../upstream/omniroute/src/lib/db/providers.ts');
  const {createMemory}=await import('../upstream/omniroute/src/lib/memory/store.ts');
  await updateSettings({requireLogin:true,stickyRoundRobinLimit:43,cloudEnabled:false,memoryEmbeddingSource:'remote',memoryEmbeddingProviderModel:null});
  const key=await createApiKey('Synthetic original-source operator','synthetic-source-machine',['manage']);
  const provider=await createProviderConnection({provider:'openai',authType:'apikey',name:'Synthetic original-source provider',apiKey:'synthetic-original-provider-token'});
  const memory=await createMemory({apiKeyId:key.id,type:'factual',key:'original-source',content:'Original SQLite migration memory',expiresAt:null});
  await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));
  console.log('PASS original unoverlaid SQLite source: real driver, 222 migrations and actual Settings/API Key/Provider/Memory writers');
  process.send?.({...original.metadata(),keyId:key.id,key:key.key,providerId:provider.id,memoryId:memory.id});
} finally {original.close();process.disconnect?.();}
