import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { transformSync } from 'esbuild';
import { overlay, overlayPins } from '../adapter/overlay.mjs';
import {spawnSync} from 'node:child_process';
import path from 'node:path';

for (const filename of Object.keys(overlayPins)) {
  const source = readFileSync(new URL('../upstream/omniroute/' + filename, import.meta.url), 'utf8');
  const transformed = overlay(filename, source);
  assert.notEqual(transformed, source);
  transformSync(transformed, { loader: filename.endsWith('.tsx')?'tsx':'ts', format: 'esm', target: 'node22' });
  assert.throws(() => overlay(filename, source + '\n'), /digest mismatch/);
}
console.log(`PASS upgrade gate: all ${Object.keys(overlayPins).length} source pins match, transforms compile, upstream drift is rejected`);
// A headless boot must not disable the authenticated dashboard in the HA build.
const headlessSource=overlay('src/lib/system/headless.ts',readFileSync(new URL('../upstream/omniroute/src/lib/system/headless.ts',import.meta.url),'utf8'));
const headlessCode=transformSync(headlessSource.replace('import { parseEnvBoolean } from "@/shared/utils/envBoolean";','const parseEnvBoolean=(value,fallback)=>value===undefined?fallback:["1","true","yes","on"].includes(value);'),{loader:'ts',format:'esm',target:'node22'}).code;
const headless=await import('data:text/javascript;base64,'+Buffer.from(headlessCode).toString('base64'));
assert.equal(headless.isHeadless({OMNIROUTE_HEADLESS:'1'}),true);
assert.equal(headless.skipInHeadless('synthetic',{OMNIROUTE_HEADLESS:'1'},()=>{}),true);
for(const pathname of ['/dashboard','/dashboard/settings','/home','/api/settings'])assert.equal(headless.headlessGateResponse(pathname,{OMNIROUTE_HEADLESS:'1'}),null);
console.log('PASS headless boot/UI separation: optional jobs remain gated and dashboard requests continue to the original authz pipeline');
// Load the transformed cache class to test overlay behavior.
const cacheSource=overlay('src/lib/db/readCache.ts',readFileSync(new URL('../upstream/omniroute/src/lib/db/readCache.ts',import.meta.url),'utf8'));
const boundary=cacheSource.indexOf('// Cache with 5s TTL');assert.ok(boundary>0);
const code=transformSync(cacheSource.slice(0,boundary)+'\nexport {TTLCache};',{loader:'ts',format:'esm',target:'node22'}).code;
const {TTLCache}=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
const disabled=new TTLCache(0);let releaseOld;
const old=disabled.load('critical',()=>new Promise(resolve=>{releaseOld=resolve;}));
const current=disabled.load('critical',()=>Promise.resolve('current'));releaseOld('previous');
assert.equal(await old,'previous');assert.equal(await current,'current','Zero-TTL reads must not join a pre-commit pending load');
const cached=new TTLCache(30000);let releaseCached,loads=0;
const first=cached.load('pricing',()=>{loads++;return new Promise(resolve=>{releaseCached=resolve;});});
assert.equal(cached.load('pricing',()=>{loads++;return Promise.resolve('unexpected');}),first);
assert.equal(loads,1);cached.invalidate();
assert.equal(await cached.load('pricing',()=>Promise.resolve('fresh')),'fresh');releaseCached('stale');await first;
assert.equal(cached.get('pricing'),'fresh','A late invalidated load must not repopulate the cache');
console.log('PASS transformed cache behavior: zero TTL bypasses pending reads; positive TTL retains coalescing and late invalidated loads cannot repopulate stale values');

// Reproduce the optional native compiler's eager-load failure without loading
// any native binding. Test the actual next-intl classes, not a copied extractor.
const lazyIntl=spawnSync(process.execPath,['--require','./adapter/buildGuard.cjs','--input-type=module','-e',String.raw`
import assert from 'node:assert/strict';import {registerHooks} from 'node:module';import {pathToFileURL} from 'node:url';import path from 'node:path';import fs from 'node:fs';import {promisify} from 'node:util';
const directory=path.resolve('upstream/omniroute/src/i18n/messages');
assert.equal(await promisify(fs.realpath)(directory),directory);
await assert.rejects(promisify(fs.realpath)('/home'),{code:'EACCES'});
assert.equal(process.permission.has('fs.read','/home'),false,'No filesystem permission may be widened');
let loads=0;
const compiler='export async function transform(){return {code:"compiled",map:null,output:JSON.stringify({results:JSON.stringify([{type:"extracted",id:"synthetic-id",message:"hello"}])})}}';
registerHooks({resolve(s,c,next){if(s==='@swc/core'){loads++;return {url:'data:text/javascript,'+encodeURIComponent(compiler),shortCircuit:true};}return next(s,c);}});
await import('next-intl/plugin');assert.equal(loads,0);
for(const flavor of ['development','production']) {
 const {default:Extractor}=await import(pathToFileURL(path.resolve('node_modules/next-intl/dist/esm',flavor,'extractor/extractor/MessageExtractor.js')).href);
 const parser=new Extractor({projectRoot:process.cwd()}),before=loads;
 assert.deepEqual(await parser.extract(path.resolve('synthetic-messages.tsx'),'const value = "plain";'),{messages:[],code:'const value = "plain";'});assert.equal(loads,before);
 const result=await parser.extract(path.resolve('synthetic-messages.tsx'),'const value = useExtracted("hello");');assert.equal(result.code,'compiled');assert.equal(result.messages[0].id,'synthetic-id');assert.ok(loads>0);
}
console.log('LAZY_INTL_CHILD_VERIFIED');
`],{cwd:process.cwd(),encoding:'utf8',env:{...process.env,NODE_OPTIONS:['--permission','--allow-child-process','--allow-addons','--allow-fs-read='+process.cwd(),'--allow-fs-write='+path.resolve('.work')].join(' ')}});
assert.equal(lazyIntl.error,undefined,'Build-integration subprocess must actually execute: '+lazyIntl.error?.code);
assert.equal(lazyIntl.signal,null);
assert.equal(lazyIntl.status,0,(lazyIntl.stdout+lazyIntl.stderr).slice(-4000));
assert.match(lazyIntl.stdout,/LAZY_INTL_CHILD_VERIFIED/);
console.log('PASS lazy next-intl compiler integration: normal plugin/catalogs avoid native loading; actual dev/prod extraction still dispatches (mock compiler), no security-check bypass');
const guardedCli=spawnSync(process.execPath,['--require','./adapter/buildGuard.cjs','node_modules/next/dist/bin/next','--version'],{cwd:process.cwd(),encoding:'utf8'});
assert.equal(guardedCli.error,undefined,'Next CLI subprocess must actually execute: '+guardedCli.error?.code);
assert.equal(guardedCli.signal,null);
assert.equal(guardedCli.status,0,(guardedCli.stdout+guardedCli.stderr).slice(-2000));
const {dependencies}=JSON.parse(readFileSync(new URL('../package.json',import.meta.url),'utf8'));
assert.equal(guardedCli.stdout.trim(),'Next.js v'+dependencies.next,'Build hooks must preserve real Next CLI CommonJS cycle loading');
console.log('PASS guarded Next CLI startup: synchronous loader hooks retain CommonJS interoperability');
const guardedConfig=spawnSync(process.execPath,['--require','./adapter/buildGuard.cjs','-e','require("next/dist/server/config");console.log("NEXT_CONFIG_GRAPH_VERIFIED")'],{cwd:process.cwd(),encoding:'utf8'});
assert.equal(guardedConfig.error,undefined);assert.equal(guardedConfig.signal,null);assert.equal(guardedConfig.status,0,(guardedConfig.stdout+guardedConfig.stderr).slice(-2000));
assert.match(guardedConfig.stdout,/NEXT_CONFIG_GRAPH_VERIFIED/);
console.log('PASS guarded Next CommonJS config graph: real cyclic dependencies load with resolve-only build integration');

// Exercise the logging and OAuth wrappers without a database or network.
Object.assign(process.env,{DATA_DIR:path.resolve('.work'),NODE_ENV:'test',NODE_TEST_CONTEXT:'isolated-capture-unit',OMNIROUTE_HEADLESS:'1',OMNIROUTE_DISABLE_BACKGROUND_SERVICES:'true',DISABLE_SQLITE_AUTO_BACKUP:'1',OMNIROUTE_DISABLE_REDIS_AUTH_CACHE:'1',REDIS_URL:''});
await import('../adapter/register.mjs');
const {createCredentialFetchGuard}=await import('../adapter/credentialRuntime.mjs');
const {runWithCapture}=await import('../upstream/omniroute/open-sse/utils/providerRequestLogging.ts');
const {AsyncLocalStorage}=await import('node:async_hooks');
const originalFetch=globalThis.fetch,credentialScope=new AsyncLocalStorage();let captured=0,sent=0,owns=true;
try{
  globalThis.fetch=async()=>{sent++;return Response.json({ok:true});};
  const install=createCredentialFetchGuard(credentialScope,{assertOwner(){if(!owns)throw Object.assign(new Error('Synthetic stale owner'),{code:'HA_FENCED'});}},globalThis.fetch);
  const capture={capture(){captured++;},body:fallback=>fallback};
  install();let wrapper,inner;
  for(let i=0;i<50;i++){
    await runWithCapture(capture,()=>globalThis.fetch('https://synthetic-provider.invalid/v1/chat/completions',{method:'POST',body:JSON.stringify({model:'synthetic',messages:[]})}));
    install();const state=globalThis[Symbol.for('omniroute.providerRequestCapture.state')];
    wrapper??=state.wrappedFetch;inner??=state.wrappedInnerFetch;
    assert.equal(globalThis.fetch,wrapper);assert.equal(state.wrappedInnerFetch,inner,'Repeated exchanges must not grow a wrapper chain');
    if(i%5===0){
      const state={id:'synthetic-'+i,endpoint:'https://synthetic-oauth.invalid/token'};
      await runWithCapture(capture,()=>credentialScope.run(state,()=>globalThis.fetch(state.endpoint,{method:'POST',body:JSON.stringify({grant_type:'refresh_token',refresh_token:'synthetic'})})));
      await assert.rejects(credentialScope.run(state,()=>globalThis.fetch(state.endpoint,{method:'POST'})),{code:'HA_CREDENTIAL_POLICY'});
    }
  }
  assert.equal(sent,60);assert.equal(captured,50,'OAuth bodies must not enter provider request capture');
  owns=false;await assert.rejects(credentialScope.run({id:'stale',endpoint:'https://synthetic-oauth.invalid/token'},()=>globalThis.fetch('https://synthetic-oauth.invalid/token',{method:'POST'})),{code:'HA_FENCED'});assert.equal(sent,60);
}finally{globalThis.fetch=originalFetch;}
console.log('PASS actual fetch-capture/OAuth composition: 50 provider dispatches interleaved with one-time exchanges, one capture/send, stable wrapper identity, no auth-body capture, replay/stale-owner rejection (unit transport)');
