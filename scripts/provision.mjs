// Creates a new isolated staging database. Never imports into, drops, or
// upgrades an existing database. Run with the project-local Node runtime.
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import {mkdtempSync,mkdirSync} from 'node:fs';
import {fork,spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import { MongoClient } from 'mongodb';
import {importSqlite} from '../adapter/dataTransfer.mjs';
import { workspacePath } from '../adapter/workspacePath.mjs';

if (!process.env.HA_MONGO_URI) throw new Error('HA_MONGO_URI is required');
workspacePath(process.env.DATA_DIR,{directory:true});
const root=fileURLToPath(new URL('../',import.meta.url));
function verify() {
  const result=spawnSync('bash',['scripts/verify-upstream.sh'],{cwd:root,env:{...process.env,GIT_OPTIONAL_LOCKS:'0'},stdio:'inherit'});
  if(result.error)throw new Error('Cannot start upstream verification ('+result.error.code+'); Bash and Git must be available in PATH');
  assert.equal(result.status,0,'Upstream integrity gate failed');
}
verify();
const database = 'ha_test_stage_' + randomUUID().replaceAll('-', '');
const client = new MongoClient(process.env.HA_MONGO_URI, { maxPoolSize: 2, minPoolSize: 0, maxConnecting: 1, serverSelectionTimeoutMS: 5000 });
let sourceDirectory;
try {
  await client.connect();
  const hello = await client.db('admin').command({ hello: 1 });
  if (!hello.setName || !hello.isWritablePrimary) throw new Error('Replica-set primary required');
  if ((await client.db(database).listCollections().toArray()).length) throw new Error('Refusing to change an existing database');
  const work=workspacePath(path.join(root,'.work'),{directory:true});
  const artifacts=path.join(work,'test-artifacts');mkdirSync(artifacts,{recursive:true,mode:0o700});
  workspacePath(artifacts,{directory:true});
  sourceDirectory=mkdtempSync(path.join(artifacts,'bootstrap-'));
  // Isolate schema generation from credentials, inherited Node hooks, and
  // network access. Allow writes only in the new private directory.
  const child=fork(new URL('../adapter/originalSqlite.mjs',import.meta.url),[sourceDirectory],{
    cwd:sourceDirectory,execArgv:['--permission','--allow-worker','--allow-child-process','--allow-addons','--allow-fs-read='+root,'--allow-fs-write='+sourceDirectory,'--require',path.join(root,'adapter/buildGuard.cjs')],
    env:{PATH:path.dirname(process.execPath),DATA_DIR:sourceDirectory,TMPDIR:sourceDirectory,NODE_DISABLE_COMPILE_CACHE:'1',
      JWT_SECRET:'synthetic-empty-schema-jwt',API_KEY_SECRET:'synthetic-empty-schema-api',STORAGE_ENCRYPTION_KEY:'synthetic-empty-schema-storage'},
    stdio:['ignore','pipe','pipe','ipc'],
  });
  const source=await new Promise((resolve,reject)=>{
    let value,output='',timedOut=false;
    const timer=setTimeout(()=>{timedOut=true;child.kill('SIGKILL');},60000);
    for(const stream of [child.stdout,child.stderr])stream.on('data',chunk=>{output=(output+chunk).slice(-6000);});
    child.on('message',message=>{value=message;});
    child.once('error',error=>{clearTimeout(timer);reject(error);});
    child.once('exit',code=>{clearTimeout(timer);if(!timedOut&&code===0&&value?.empty===true)resolve(value);else reject(new Error('Offline original schema generation failed'+(timedOut?' (timeout)':'')+': '+output));});
  });
  verify(); // A changed checkout cannot be imported after the child succeeded.
  const imported=await importSqlite(client,process.env.HA_MONGO_URI,source.file,database,{rebuildMemory:true});
  assert.equal(imported.counts._omniroute_migrations,source.migrations);
  assert.deepEqual(Object.keys(imported.counts).sort(),source.tables);
  console.log('Created isolated complete-schema database: ' + database);
  console.log('Original migration rows: '+source.migrations+'; ordinary source tables: '+source.tables.length);
  console.log('Retained offline schema source: '+source.file);
  console.log('Offline source SHA-256: '+imported.sourceSha256);
  console.log('Set HA_MONGO_DATABASE to this value on both staging hosts.');
  process.send?.({database,source:source.file,sourceSha256:imported.sourceSha256,migrations:source.migrations,tables:source.tables,addedRuntimeSchemas:imported.addedRuntimeSchemas});
} catch (error) {
  console.error('Provisioning failed; inspect only this new database: ' + database);
  if(sourceDirectory)console.error('Retained offline source directory: '+sourceDirectory);
  throw error;
} finally {await client.close();verify();}
