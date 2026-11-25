// Explicit offline schema/input generation only. Never an application driver.
// The original loader intentionally applies no HA overlays.
import assert from 'node:assert/strict';
import {registerHooks,createRequire} from 'node:module';
import {existsSync,readFileSync,readdirSync} from 'node:fs';
import {fileURLToPath,pathToFileURL} from 'node:url';
import path from 'node:path';
import {transformSync} from 'esbuild';
import {workspacePath} from './workspacePath.mjs';
import {readBaseSchema} from './baseSchema.mjs';

export async function openOriginalSqlite(output) {
  assert.ok(!globalThis.__omnirouteMongoAdapter&&!globalThis.__omnirouteActiveRuntime,'Offline source generation cannot run in an HA application');
  const directory=workspacePath(output,{directory:true});
  assert.equal(readdirSync(directory).length,0,'Never overwrite an existing source or backup');
  readBaseSchema(); // Verify the pinned official core before executing it.
  Object.assign(process.env,{DATA_DIR:directory,TMPDIR:directory,NODE_ENV:'test',NODE_TEST_CONTEXT:'explicit-original-sqlite-source',NEXT_PHASE:'',OMNIROUTE_BUILDING:'',
    NODE_DISABLE_COMPILE_CACHE:'1',DISABLE_SQLITE_AUTO_BACKUP:'true',OMNIROUTE_DISABLE_BACKGROUND_SERVICES:'true',VECTOR_STORE_DISABLE_VEC:'true',OMNIROUTE_EXTRA_MIGRATIONS_DIRS:'',REDIS_URL:''});
  const root=fileURLToPath(new URL('../upstream/omniroute/',import.meta.url)),prefix=pathToFileURL(root).href,projectRequire=createRequire(new URL('../package.json',import.meta.url));
  const candidate=base=>[base,base+'.ts',base+'.tsx',base+'.js',base+'/index.ts',base+'/index.js'].find(file=>/\.(?:[cm]?js|tsx?|json)$/.test(file)&&existsSync(file));
  registerHooks({
    resolve(specifier,context,next) {
      let base;
      if(specifier.startsWith('@/'))base=path.join(root,'src',specifier.slice(2));
      else if(specifier.startsWith('@omniroute/open-sse/'))base=path.join(root,'open-sse',specifier.slice('@omniroute/open-sse/'.length));
      else if(specifier.startsWith('.')&&context.parentURL?.startsWith(prefix))base=path.resolve(path.dirname(fileURLToPath(context.parentURL)),specifier);
      if(base){const file=candidate(base)||(/\.js$/.test(base)&&candidate(base.slice(0,-3)));if(file)return {url:pathToFileURL(file).href,shortCircuit:true};}
      try{return next(specifier,context);}catch(error){if(!specifier.startsWith('.')&&!specifier.includes(':'))return {url:pathToFileURL(projectRequire.resolve(specifier)).href,shortCircuit:true};throw error;}
    },
    load(url,context,next) {
      if(url.startsWith(prefix)&&url.endsWith('.json')){const data=JSON.parse(readFileSync(fileURLToPath(url),'utf8'));return {format:'module',shortCircuit:true,source:'const data='+JSON.stringify(data)+';export default data;'+(url===pathToFileURL(path.join(root,'package.json')).href?'export const version=data.version;':'')};}
      if(url.startsWith(prefix)&&/\.[jt]sx?$/.test(url))return {format:'module',shortCircuit:true,source:transformSync(readFileSync(fileURLToPath(url),'utf8'),{format:'esm',target:'node22',loader:url.endsWith('.tsx')?'tsx':url.endsWith('.ts')?'ts':'js'}).code};
      return next(url,context);
    },
  });
  const core=await import('../upstream/omniroute/src/lib/db/core.ts');
  try {
    const db=core.getDbInstance();
    assert.equal(db.driver,'node:sqlite','Offline bootstrap requires the pinned Node SQLite driver');
    return {db,close:()=>core.closeDbInstance({checkpointMode:'TRUNCATE'}),metadata() {
      const migrations=db.prepare('SELECT COUNT(*) AS n FROM _omniroute_migrations').get().n;
      assert.equal(migrations,222);assert.equal(db.prepare('PRAGMA foreign_key_check').all().length,0);
      const tables=db.prepare('PRAGMA table_list').all().filter(table=>table.schema==='main'&&table.type==='table'&&!table.name.startsWith('sqlite_')).map(table=>table.name).sort();
      return {file:path.join(directory,'storage.sqlite'),migrations,tables};
    }};
  } catch(error){core.closeDbInstance({checkpointMode:'TRUNCATE'});throw error;}
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
  assert.ok(process.send,'Offline schema entry requires its provisioning parent');
  const original=await openOriginalSqlite(process.argv[2]);
  try {
    for(const table of ['api_keys','provider_connections','memories'])assert.equal(original.db.prepare('SELECT COUNT(*) AS n FROM '+table).get().n,0,'Fresh schema must contain no application credentials/data');
    process.send({...original.metadata(),empty:true});
  } finally {original.close();process.disconnect?.();}
}
