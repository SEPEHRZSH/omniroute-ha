import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { fork,execFile } from 'node:child_process';
import {promisify} from 'node:util';
import { once } from 'node:events';
import { mkdirSync, readdirSync, mkdtempSync, rmSync,writeFileSync } from 'node:fs';
import path from 'node:path';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { testDatabase, fixtureSet } from './mongoTestSupport.mjs';
import { provisionStartupSchema } from '../adapter/startupSchema.mjs';
import { hashSync } from 'bcryptjs';
import { randomUUID } from 'node:crypto';
import { backupDatabase, restoreDatabase } from '../adapter/dataTransfer.mjs';
import { trafficRouter } from './trafficRouter.mjs';
import {startRedisHa,waitRedis} from './redisHa.mjs';
import {verifyBrowserHttp} from './browser.mjs';
import {windows as poolWindows} from '../adapter/poolQuota.mjs';

const test = await testDatabase('http');
const retentionExpiredRows=501;
const dataDir = mkdtempSync(path.resolve('.work/http-'));
const testEnv = {
  HA_TEST_FULL_BACKEND: process.env.HA_TEST_FULL_BACKEND || '',
  HA_TEST_VECTOR_HTTP:process.env.HA_TEST_VECTOR_HTTP||'',
  OMNIROUTE_TRACE:process.env.HA_TEST_TRACE==='1'?'true':'false',
  NEXT_PRIVATE_OUTPUT_TRACE_ROOT: process.cwd(),
  NEXT_RUNTIME: 'nodejs', NODE_TEST_CONTEXT: 'isolated-http', OMNIROUTE_HEADLESS: '1',
  OMNIROUTE_DISABLE_CREDENTIAL_HEALTH_CHECK: '1', PROXY_HEALTH_ENABLED: 'false', HOSTNAME: '127.0.0.1',
  API_PORT: '20128', DASHBOARD_PORT: '20128', OMNIROUTE_EVENT_LOOP_STALL_THRESHOLD_MS: '0',
  NODE_ENV: 'production', NEXT_TELEMETRY_DISABLED: '1', DATA_DIR: dataDir,
  DISABLE_SQLITE_AUTO_BACKUP: '1', OMNIROUTE_DISABLE_BACKGROUND_SERVICES: 'true', ENABLE_REQUEST_LOGS:'true',
  OMNIROUTE_DISABLE_REDIS_AUTH_CACHE: '1', OMNIROUTE_DISABLE_CLI_TOKEN: 'true',
  OMNIROUTE_HA_IDENTITY: 'synthetic-shared-ha-deployment',
  STORAGE_ENCRYPTION_KEY: 'synthetic-storage-key-for-ha-tests-only',
  API_KEY_SECRET: 'synthetic-api-key-secret-for-ha-tests-only', JWT_SECRET: 'synthetic-jwt-secret-for-ha-tests-only',
  REDIS_URL: '', HA_REDIS_MODE:'disabled',QDRANT_HOST: '',
  FREE_PROXY_AUTO_SYNC_ENABLED:process.env.HA_TEST_FULL_BACKEND==='1'?'true':'false',
  FREE_PROXY_AUTO_SYNC_INTERVAL_MS:'300000',FREE_PROXY_1PROXY_ENABLED:'true',FREE_PROXY_PROXIFLY_ENABLED:'false',FREE_PROXY_IPLOCATE_ENABLED:'false',FREE_PROXY_WEBSHARE_ENABLED:'false',
  OMNIROUTE_API_KEY_WEEKLY_WINDOW: process.env.HA_TEST_KEY_WEEKLY_MODE || 'provider',
  OMNIROUTE_API_KEY_WEEKLY_WINDOW_TIMEZONE: process.env.HA_TEST_KEY_WEEKLY_ZONE || '',
  VECTOR_STORE_DISABLE_VEC: process.env.HA_TEST_VECTOR_HTTP==='1'?'false':'true',
  MEMORY_TYPED_DECAY_ENABLED:'true',MEMORY_TYPED_DECAY_SWEEP_INTERVAL:'60',
  HA_QUOTA_BOUNDS: process.env.HA_TEST_RESERVATIONS === '1' ? JSON.stringify({ 'openai-compatible-ha-test/synthetic-model': { tokens: 5, nanoUsd: 3100000000,...(process.env.HA_TEST_EXTENDED_QUOTA==='1'?{tools:true,maxImages:2,maxAudioBytes:1024,mediaPricing:'provider'}:{}) },'openai-compatible-ha-legacy/model':{tokens:5,nanoUsd:3100000000} }) : '',
};
Object.assign(process.env, testEnv);
let bootstrap,provisionedDatabase;
const children = [];
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
let counter = 0;
let router, providerServer, providerCalls = 0, boundedCalls = 0;
let streamingCalls=0;
let heldStreamingResponse,releaseHeldStream,pendingLostStream,holdNextStreaming=false;
let expectedProviderToken = 'synthetic-provider';
let heldProviderResponse, releaseHeldProvider, pendingInference;
let oauthCalls=0,holdOAuth=false,heldOAuthResponse,releaseOAuth,pendingOAuth;
let knownOAuthCiphertext;
const pairedOAuthProviders=['claude','codex','cline','kimi-coding','gitlab-duo','kiro','amazon-q','github'];
const pairedOAuthCalls=Object.fromEntries(pairedOAuthProviders.map(provider=>[provider,0])),pairedOAuthCiphertext={};
let kiroRegistrations=0,kiroWireRefresh='synthetic-kiro-http-seed',kiroInterruptedId;
let importCalls=0,heldImport,releaseImport,pendingImport,importedProviderId;
const importRecord={access_token:'synthetic-import-old',refresh_token:'synthetic-import-seed-http',email:'synthetic-import@example.invalid',account_id:'synthetic-import-workspace'};
const unknownImport={access_token:'synthetic-import-old-unknown',refresh_token:'synthetic-import-seed-unknown',email:'synthetic-import-unknown@example.invalid',account_id:'synthetic-import-unknown'};
const oauthReceivedAt=[];
const providerRequests = [];
let failure;
let vectorMemoryId,embeddingCalls=0;
let freeProxyId,promotedFreeProxyId,freeProxyConnects=0;
let freeProxySyncCalls=0,holdFreeProxySync=false,heldFreeProxySync,releaseFreeProxySync,pendingFreeProxySync,freeProxyLastSync;
const freeProxyTunnels=new Set();
let serviceDatabase = test.database, recoveredDatabase;
let dashboardCookie, revokedDashboardCookie;
let redisFixture,redisRevokedKey;
let quotaPollCalls=0,quotaPollProvider;
let recoveryProviderCalls=0,recoveryMode='quota',heldRecoveryResponse,releaseRecovery,pendingRecovery;
let completedRecoveryId,uncertainRecoveryId,recoveryKey,enqueueRecovery;
let responsesKey,responsesStreamKey,responsesId;
let legacyHttpKey,legacyHttpPoolId,legacyHttpConnection,legacyHttpEvidence,legacyProviderCalls=0;
async function storedContinuationShape(responseId){
  const rows=await test.db.collection('call_logs').find({response_id:responseId}).sort({timestamp:-1}).limit(3).toArray(),result=[];
  for(const row of rows){
    const stored=row.artifact_relpath&&await test.db.collection('ha_call_log_artifacts').findOne({path:row.artifact_relpath});
    const artifact=stored?JSON.parse(stored.payload):null,pipeline=artifact?.pipeline,request=pipeline?.clientRawRequest,response=pipeline?.clientResponse;
    result.push({state:row.detail_state,keyMatches:row.api_key_id===responsesKey.id,video:row.video_content_removed,artifact:!!artifact,pipelineKeys:Object.keys(pipeline??{}),
      inputLength:Array.isArray(request?.body?.input)?request.body.input.length:null,effectiveLength:Array.isArray(request?.effectiveInput)?request.effectiveInput.length:null,
      outputLength:Array.isArray(response?.output)?response.output.length:null,summaryLength:Array.isArray(response?.summary?.output)?response.summary.output.length:null,
      responseKeys:Object.keys(response??{}),truncated:response?._truncated});
  }return result;
}
async function dashboardRequest(child,endpoint,cookie,init={}) {
  return fetch(child.base+endpoint,{...init,headers:{'content-type':'application/json',origin:child.base,...(cookie?{cookie}:{}),...init.headers},
    redirect:'manual',signal:AbortSignal.timeout(10000)});
}
async function enrollMfa(child,key,request) {
  const response=await request(child,'/api/auth/mfa/setup',key,{method:'POST',body:JSON.stringify({password:'synthetic-dashboard-password'})});
  assert.equal(response.status,200,'Original MFA setup');const enrollment=await response.json();
  assert.match(enrollment.secret,/^[A-Z2-7]+$/);assert.match(enrollment.qrCodeDataUri,/^data:image\/png/);
  const {generateHotpCode,totpCounter}=await import('../upstream/omniroute/src/lib/auth/totp.ts');
  const enabled=await request(child,'/api/auth/mfa/enable',key,{method:'POST',body:JSON.stringify({password:'synthetic-dashboard-password',secret:enrollment.secret,code:generateHotpCode(enrollment.secret,totpCounter())})});
  assert.equal(enabled.status,200,'Original MFA enrollment');const {backupCodes}=await enabled.json();assert.equal(backupCodes.length,10);
  const login=await dashboardRequest(child,'/api/auth/login',null,{method:'POST',body:JSON.stringify({password:'synthetic-dashboard-password'})});
  assert.equal(login.status,200);assert.equal((await login.json()).mfaRequired,true);
  assert.equal(login.headers.getSetCookie().some(value=>/^auth_token=[^;]/.test(value)),false,'Password alone must not issue a session');
  const pending=login.headers.getSetCookie().find(value=>value.startsWith('omniroute_mfa_pending='))?.split(';')[0];assert.ok(pending);
  assert.equal((await dashboardRequest(child,'/api/settings',pending)).status,401);
  return {secret:enrollment.secret,backupCodes,pending};
}
async function verifyPromotedMfa(child,key,mfa,request) {
  const {generateHotpCode,totpCounter}=await import('../upstream/omniroute/src/lib/auth/totp.ts');
  const code=generateHotpCode(mfa.secret,totpCounter());
  const verify=(value,cookie=mfa.pending)=>dashboardRequest(child,'/api/auth/mfa/verify',cookie,{method:'POST',body:JSON.stringify({code:value})});
  const accepted=await verify(code);assert.equal(accepted.status,200,'Promoted active must accept the pending login and shared factor');assert.equal((await accepted.json()).method,'totp');
  const session=accepted.headers.getSetCookie().find(value=>value.startsWith('auth_token='))?.split(';')[0];assert.ok(session);
  assert.equal((await dashboardRequest(child,'/api/settings',session)).status,200);
  const freshPending=async()=>{
    const response=await dashboardRequest(child,'/api/auth/login',null,{method:'POST',body:JSON.stringify({password:'synthetic-dashboard-password'})});assert.equal(response.status,200);
    assert.equal((await response.json()).mfaRequired,true);const cookie=response.headers.getSetCookie().find(value=>value.startsWith('omniroute_mfa_pending='))?.split(';')[0];assert.ok(cookie);return cookie;
  };
  const nextPending=await freshPending();assert.equal((await verify(code,nextPending)).status,401,'A used TOTP counter must not be replayed');
  const spent=await Promise.all([1,2].map(()=>verify(mfa.backupCodes[0],nextPending)));
  assert.equal(spent.filter(response=>response.status===200).length,1,'One recovery code can create only one session under concurrent requests');
  assert.ok(spent.filter(response=>response.status!==200).every(response=>[401,429].includes(response.status)));
  for(const response of spent)await response.arrayBuffer();
  const status=await request(child,'/api/auth/mfa/status',key);assert.equal(status.status,200);const state=await status.json();assert.equal(state.enabled,true);assert.equal(state.backupCodesRemaining,9);
  const pending=await freshPending();assert.equal((await verify(mfa.backupCodes[0],pending)).status,401,'Consumed recovery code must stay rejected');
  const disabled=await request(child,'/api/auth/mfa/disable',key,{method:'POST',body:JSON.stringify({password:'synthetic-dashboard-password',code:mfa.backupCodes[1]})});assert.equal(disabled.status,200,'Disable requires a different live factor');await disabled.arrayBuffer();
  console.log('PASS actual MFA promotion HTTP: original enrollment/QR, password-only denial, old-active pending cookie accepted by promoted owner, TOTP replay refusal and atomic single-use recovery codes; no factor bypass');
}
async function verifyReasoningHttp(child,phase,expected){
  const response=await dashboardRequest(child,'/api/cache/reasoning?provider=synthetic&model=synthetic-model&limit=10&offset=0',dashboardCookie),body=await response.json();
  assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.entries.length,expected);assert.ok(body.entries.some(row=>row.toolCallId==='retained-reasoning'));
  assert.equal(body.entries.some(row=>row.toolCallId==='old-reasoning'),false);assert.ok(body.stats);
  console.log('PASS actual reasoning cache HTTP '+phase+': authenticated stats/filter/pagination, persisted numeric expiry and active-owned cleanup');
}
async function verifyDashboardHttp(child, cookie, phase) {
  for (const endpoint of ['/login','/docs','/dashboard','/dashboard/settings','/dashboard/providers','/dashboard/memory']) {
    const response=await dashboardRequest(child,endpoint,cookie);
    const html=await response.text();
    assert.equal(response.status,200,phase+' '+endpoint+': '+html.slice(0,500));
    assert.match(response.headers.get('content-type'),/text\/html/);
    assert.match(html,/<html[ >]/);
    for(const secret of ['synthetic-dashboard-password','synthetic-storage-key-for-ha-tests-only','synthetic-token-before-promotion']) assert.equal(html.includes(secret),false,'UI must not serialize credentials');
    if(endpoint==='/dashboard') {
      const asset=html.match(/href="([^" ]*\/_next\/static\/[^" ]+\.css(?:\?[^" ]*)?)"/);
      assert.ok(asset,'Dashboard must include actual CSS');
      const stylesheet=await fetch(new URL(asset[1].replaceAll('&amp;','&'),child.base),{signal:AbortSignal.timeout(10000)});
      assert.equal(stylesheet.status,200);assert.match(stylesheet.headers.get('content-type'),/text\/css/);
      assert.ok((await stylesheet.text()).length>1000,'Dashboard stylesheet must be populated');
    }
  }
  const localized=await dashboardRequest(child,'/dashboard',cookie+'; NEXT_LOCALE=fa');
  const localizedHtml=await localized.text();assert.equal(localized.status,200,phase+' Persian dashboard');
  assert.match(localizedHtml,/<html[^>]*lang="fa"/);assert.match(localizedHtml,/<html[^>]*dir="rtl"/);
  const denied=await dashboardRequest(child,'/dashboard',null);
  assert.ok([302,303,307,308].includes(denied.status),'Unauthenticated dashboard must redirect');
  assert.match(denied.headers.get('location'),/\/login/);
  console.log('PASS actual dashboard/docs HTML and CSS '+phase+': login, docs, four authenticated pages, Persian RTL, credential non-disclosure and unauthenticated redirect');
}
function receive(child, id) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { cleanup(); reject(new Error('Peer response timeout')); }, 20000);
    function cleanup() { clearTimeout(timer); child.off('message', message); child.off('exit', exit); }
    function message(value) { if (value.id === id) { cleanup(); resolve(value); } }
    function exit(code) { cleanup(); reject(new Error(`Peer exited: ${code}`)); }
    child.on('message', message); child.on('exit', exit);
  });
}
async function call(child, request) {
  const id = ++counter, answer = receive(child, id);
  child.send({ ...request, id }); return answer;
}
async function spawn(service = false) {
  const local = path.join(dataDir, 'host-' + children.length);
  mkdirSync(local);
  const child = fork(new URL(service ? '../adapter/serve.mjs' : './nextPeer.mjs', import.meta.url), [], {
    execArgv: ['--permission', '--allow-worker', '--allow-child-process', '--allow-addons',
      '--allow-fs-read=' + process.cwd(), '--allow-fs-write=' + local, '--import', './adapter/register.mjs', '--import', './tests/networkGuard.mjs'],
    env: { ...testEnv, DATA_DIR: local, OMNIROUTE_PLUGINS_DIR: path.join(local, 'plugins'), TMPDIR: local, PATH: path.dirname(process.execPath), HA_TEST_URI: test.uri, HA_TEST_DATABASE: serviceDatabase,
      HA_MONGO_URI: test.uri, HA_MONGO_DATABASE: serviceDatabase, HA_NEXT_DIR: path.resolve('.work/backend'), HOST: '127.0.0.1', PORT: '0' },
    stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
  });
  children.push(child);
  const result = await receive(child, undefined);
  assert.ok(result.port); child.base = `http://127.0.0.1:${result.port}`;
  return child;
}
async function waitReady(child, expected) {
  // A SIGSTOP may catch the former owner holding a Mongo transaction lock.
  // Keep the lease/fence unchanged; allow the server's idle-TX expiry as well
  // as cold boot. This is a bounded acceptance wait, not a short-RTO claim.
  const deadline = Date.now() + 90000;
  while (Date.now() < deadline) {
    if (child.pid && (child.exitCode !== null || child.signalCode)) throw new Error('Application exited during readiness wait');
    try {
      const response = await fetch(child.base + '/readyz', { signal: AbortSignal.timeout(1500) });
      await response.arrayBuffer();
      if ((response.status === 200) === expected) return;
    } catch { /* A cold startup or election can outlast one probe; retry until the bounded deadline. */ }
    await delay(100);
  }
  throw new Error(`Readiness did not become ${expected}`);
}
try {
  if(process.env.HA_TEST_REDIS==='1'){
    redisFixture=await startRedisHa();Object.assign(testEnv,redisFixture.env,{HA_TEST_REDIS_PORTS:JSON.stringify(redisFixture.nodes.map(node=>node.port))});Object.assign(process.env,testEnv);
  }
  if(testEnv.HA_TEST_FULL_BACKEND==='1') {
    const builder=fork(new URL('../scripts/provision.mjs',import.meta.url),[],{execArgv:[],
      env:{...process.env,...testEnv,NODE_OPTIONS:'',HA_MONGO_URI:test.uri,DATA_DIR:dataDir,TMPDIR:dataDir},stdio:['ignore','pipe','pipe','ipc']});
    const provisioned=await new Promise((resolve,reject)=>{
      let value,output='',timedOut=false;
      const timer=setTimeout(()=>{timedOut=true;builder.kill('SIGKILL');},180000);
      for(const stream of [builder.stdout,builder.stderr])stream.on('data',chunk=>{output=(output+chunk).slice(-8000);});
      builder.on('message',message=>{
        if(/^ha_test_stage_[a-f0-9]{32}$/.test(message?.database)){provisionedDatabase=message.database;value=message;}
      });
      builder.once('error',error=>{clearTimeout(timer);reject(error);});
      builder.once('exit',code=>{clearTimeout(timer);if(!timedOut&&code===0&&value)resolve(value);else reject(new Error('Real provisioning CLI failed'+(timedOut?' (timeout)':'')+': '+output));});
    });
    assert.equal(provisioned.migrations,222);assert.ok(provisioned.tables.includes('_omniroute_migrations'));
    assert.ok(provisioned.addedRuntimeSchemas.includes('ha_call_log_artifacts'));
    test.database=serviceDatabase=provisioned.database;test.db=test.client.db(provisioned.database);
    assert.equal(await test.db.collection('_omniroute_migrations').countDocuments(),222);
    for(const table of ['api_keys','provider_connections','memories'])assert.equal(await test.db.collection(table).countDocuments(),0);
    console.log('PASS actual complete provisioning CLI: original 222 migrations, '+provisioned.tables.length+' ordinary tables, empty API keys/Providers/Memory; retained schema input '+provisioned.source);
  }
  bootstrap=new MongoAdapter({uri:test.uri,database:test.database});
  bootstrap.onUnsupported=(sql,error)=>console.error('[unsupported bootstrap]',error?.message,sql);
  globalThis.__omnirouteMongoAdapter=bootstrap;
  if(!provisionedDatabase)await provisionStartupSchema(bootstrap);
  const settings = await import('../upstream/omniroute/src/lib/db/settings.ts');
  const keys = await import('../upstream/omniroute/src/lib/db/apiKeys.ts');
  await settings.updateSettings({ requireLogin: true, setupComplete: true, password: hashSync('synthetic-dashboard-password', 10), stickyRoundRobinLimit: 7, cloudEnabled: false,proxyEnabled:true,
    memoryEmbeddingSource: 'remote', memoryEmbeddingProviderModel: null });
  providerServer = createServer((req, res) => {
    if(req.url==='/oauth/github-copilot'){
      assert.equal(req.method,'GET');res.setHeader('content-type','application/json');
      if(req.headers.authorization==='token synthetic-github-http-before')res.writeHead(401).end(JSON.stringify({error:'synthetic expired GitHub access token'}));
      else if(/^token synthetic-github-http-access-[12]$/.test(req.headers.authorization??''))res.end(JSON.stringify({token:'synthetic-copilot-subtoken',expires_at:Math.floor(Date.now()/1000)+3600}));
      else res.writeHead(401).end(JSON.stringify({error:'synthetic unknown GitHub access token'}));
      return;
    }
    if(req.url==='/legacy/v1/chat/completions'){
      assert.equal(req.headers.authorization,'Bearer synthetic-legacy-provider');let wire='';req.on('data',chunk=>{wire+=chunk;});req.on('end',()=>{
        const body=JSON.parse(wire);assert.equal(body.model,'model');assert.equal(body.stream,false);assert.equal(body.max_tokens,3);legacyProviderCalls++;
        res.writeHead(200,{'content-type':'application/json'}).end(JSON.stringify({id:'synthetic-legacy-response',object:'chat.completion',choices:[{message:{role:'assistant',content:'synthetic-legacy-pool-response'},finish_reason:'stop'}],usage:{prompt_tokens:2,completion_tokens:1,total_tokens:3}}));
      });return;
    }
    if(req.url==='/recovery/v1/chat/completions'){
      assert.equal(req.headers.authorization,'Bearer synthetic-recovery-provider');
      let wire='';req.on('data',chunk=>{wire+=chunk;});req.on('end',()=>{
        const payload=JSON.parse(wire);assert.equal(payload.stream,false);recoveryProviderCalls++;
        if(recoveryMode==='quota'){res.setHeader('retry-after','1');res.writeHead(402,{'content-type':'application/json'}).end(JSON.stringify({error:{message:'credits exhausted synthetic provider',code:'insufficient_quota',type:'insufficient_quota'}}));return;}
        const finish=()=>{res.setHeader('content-type','application/json');res.end(JSON.stringify({id:'synthetic-recovery',object:'chat.completion',choices:[{message:{role:'assistant',content:'synthetic-recovered-response'},finish_reason:'stop'}],usage:{prompt_tokens:2,completion_tokens:1,total_tokens:3}}));};
        if(recoveryMode==='held'){heldRecoveryResponse=res;releaseRecovery=finish;}else finish();
      });return;
    }
    if(req.url==='/synthetic-quota'){
      quotaPollCalls++;res.setHeader('content-type','application/json');res.end(JSON.stringify({used:quotaPollCalls*10,total:100,resetAt:'2099-01-01T00:00:00.000Z'}));return;
    }
    if(req.url.startsWith('/synthetic-free-proxies?')) {
      freeProxySyncCalls++;res.setHeader('content-type','application/json');
      const held=holdFreeProxySync;holdFreeProxySync=false;
      const finish=()=>res.end(JSON.stringify({proxies:[{ip:'93.184.216.34',port:held?8082:8081,protocol:'http',quality_score:77,country_code:'ZZ'}]}));
      if(held){heldFreeProxySync=res;releaseFreeProxySync=finish;}else finish();return;
    }
    if(req.url==='/v1/embeddings') {
      let wire='';req.on('data',chunk=>{wire+=chunk;});req.on('end',()=>{
        const body=JSON.parse(wire),input=Array.isArray(body.input)?body.input[0]:body.input;
        if(body.model!=='synthetic-memory-embedding'||typeof input!=='string'){res.writeHead(400).end();return;}
        embeddingCalls++;const vector=input.toLowerCase().includes('satellite')?[0,1,0]:[1,0,0];
        res.setHeader('content-type','application/json');res.end(JSON.stringify({model:body.model,data:[{object:'embedding',index:0,embedding:vector}],usage:{prompt_tokens:1,total_tokens:1}}));
      });return;
    }
    if(req.url==='/oauth/kiro-register'){
      let wire='';req.on('data',chunk=>{wire+=chunk;});req.on('end',()=>{
        const body=JSON.parse(wire);assert.equal(body.clientName,'kiro-oauth-client');assert.equal(body.clientType,'public');kiroRegistrations++;
        res.setHeader('content-type','application/json');res.end(JSON.stringify({clientId:'synthetic-http-registered-client',clientSecret:'synthetic-http-registered-secret',clientSecretExpiresAt:Math.floor(Date.now()/1000)+86400}));
      });return;
    }
    const paired=/^\/oauth\/(claude|codex|cline|kimi-coding|gitlab-duo|kiro|amazon-q|github)$/.exec(req.url);
    if(paired) {
      const provider=paired[1];let wire='';req.on('data',chunk=>{wire+=chunk;});req.on('end',()=>{
        if(provider==='cline') {
          const data=JSON.parse(wire),count=++pairedOAuthCalls.cline;
          assert.equal(data.grantType,'refresh_token');assert.equal(data.clientType,'extension');
          assert.equal(data.refreshToken,count===1?'synthetic-cline-http-seed':'synthetic-cline-http-refresh-'+(count-1));
          res.setHeader('content-type','application/json');res.end(JSON.stringify({data:{accessToken:'synthetic-cline-http-access-'+count,refreshToken:'synthetic-cline-http-refresh-'+count,expiresAt:new Date(Date.now()+3600000).toISOString()}}));return;
        }
        if(provider==='kiro'){
          const body=JSON.parse(wire),count=++pairedOAuthCalls.kiro;
          assert.equal(body.grantType,'refresh_token');assert.equal(body.refreshToken,kiroWireRefresh);
          if(body.clientId==='synthetic-http-old-client'){res.writeHead(401,{'content-type':'application/json'}).end(JSON.stringify({error:'invalid_client',__type:'InvalidClientException'}));return;}
          assert.equal(body.clientId,'synthetic-http-registered-client');assert.equal(body.clientSecret,'synthetic-http-registered-secret');
          kiroWireRefresh='synthetic-kiro-http-refresh-'+count;
          res.setHeader('content-type','application/json');res.end(JSON.stringify({accessToken:'synthetic-kiro-http-access-'+count,refreshToken:kiroWireRefresh,expiresIn:3600}));return;
        }
        const form=new URLSearchParams(wire);assert.equal(form.get('grant_type'),'refresh_token');assert.ok(form.get('client_id'));
        if(provider==='codex')assert.equal(form.has('scope'),false);
        if(provider==='claude')assert.equal(req.headers['anthropic-beta'],'oauth-2025-04-20');
        if(provider==='kimi-coding')assert.equal(req.headers['x-msh-device-id'],'synthetic-http-device');
        if(provider==='gitlab-duo')assert.equal(form.get('client_id'),'synthetic-gitlab-http-client');
        if(provider==='amazon-q'){assert.equal(form.get('client_id'),'synthetic-http-idp-client');assert.equal(form.has('client_secret'),false);assert.ok(form.get('scope').includes('offline_access'));}
        if(provider==='codex'&&form.get('refresh_token')?.startsWith('synthetic-import-seed-')) {
          const count=++importCalls,finish=()=>{res.setHeader('content-type','application/json');res.end(JSON.stringify({access_token:'synthetic-import-http-access-'+count,refresh_token:'synthetic-import-http-rotated-'+count,expires_in:3600}));};
          if(form.get('refresh_token')===unknownImport.refresh_token){heldImport=res;releaseImport=finish;}else finish();return;
        }
        const count=++pairedOAuthCalls[provider];
        assert.equal(form.get('refresh_token'),count===1?'synthetic-'+provider+'-http-seed':'synthetic-'+provider+'-http-refresh-'+(count-1));
        res.setHeader('content-type','application/json');res.end(JSON.stringify({access_token:'synthetic-'+provider+'-http-access-'+count,refresh_token:'synthetic-'+provider+'-http-refresh-'+count,expires_in:3600,...(provider==='kimi-coding'?{token_type:'Bearer',scope:'synthetic-scope'}:{})}));
      });return;
    }
    if(req.url==='/oauth/token') {
      let form='';req.on('data',chunk=>{form+=chunk;});req.on('end',()=>{
        oauthCalls++;
        oauthReceivedAt.push(Date.now());
        assert.equal(new URLSearchParams(form).get('refresh_token'),oauthCalls===1?'synthetic-http-oauth-seed':'synthetic-http-oauth-refresh-'+(oauthCalls-1));
        const finish=()=>{res.setHeader('content-type','application/json');res.end(JSON.stringify({access_token:'synthetic-http-oauth-access-'+oauthCalls,refresh_token:'synthetic-http-oauth-refresh-'+oauthCalls,expires_in:3600}));};
        if(holdOAuth) {heldOAuthResponse=res;releaseOAuth=finish;}else finish();
      });return;
    }
    const requestRecord={path:req.url,authorized:req.headers.authorization==='Bearer '+expectedProviderToken,ended:false,bytes:0};providerRequests.push(requestRecord);
    if (req.url !== '/v1/chat/completions' || req.headers.authorization !== 'Bearer '+expectedProviderToken) { res.writeHead(403).end(); return; }
    let wire = ''; req.on('data', chunk => { wire += chunk;requestRecord.bytes+=chunk.length; }); req.on('end', () => {
      const payload=JSON.parse(wire),toolCall=payload.tools?.length>0;
      requestRecord.outputCap=payload.max_completion_tokens??payload.max_tokens;requestRecord.hasResponsesCap=payload.max_output_tokens!=null;
      requestRecord.messageCount=payload.messages?.length;
      const media=payload.messages?.some(message=>Array.isArray(message.content)&&message.content.some(part=>['image_url','input_audio'].includes(part.type)));
      const usage={prompt_tokens:2,completion_tokens:1,total_tokens:3,...(media?{cost_usd:3}:{})};
      requestRecord.ended=true;requestRecord.stream=payload.stream;
      if(payload.stream===true) {
        streamingCalls++;res.setHeader('content-type','text/event-stream');res.setHeader('cache-control','no-cache');
        res.write('data: '+JSON.stringify({id:'synthetic-stream',object:'chat.completion.chunk',choices:[{index:0,delta:toolCall?{role:'assistant',tool_calls:[{index:0,id:'synthetic-lookup',type:'function',function:{name:'lookup',arguments:'{}'}}]}:{role:'assistant',content:'synthetic-stream-response'},finish_reason:null}]})+'\n\n');
        const finish=()=>res.end('data: '+JSON.stringify({id:'synthetic-stream',object:'chat.completion.chunk',choices:[{index:0,delta:{},finish_reason:toolCall?'tool_calls':'stop'}],usage})+'\n\ndata: [DONE]\n\n');
        if(holdNextStreaming){holdNextStreaming=false;heldStreamingResponse=res;releaseHeldStream=finish;}else setTimeout(finish,50);
        return;
      }
      providerCalls++;
      const finish = () => { res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({ id: 'synthetic-completion-'+providerCalls, object: 'chat.completion',
        choices: [{ message: toolCall?{role:'assistant',content:null,tool_calls:[{id:'synthetic-lookup',type:'function',function:{name:'lookup',arguments:'{}'}}]}:{ role: 'assistant', content: 'synthetic-response' }, finish_reason: toolCall?'tool_calls':'stop' }],
        usage })); };
      if (wire.includes('synthetic-held-provider-call')) { heldProviderResponse = res; releaseHeldProvider = finish; }
      else finish();
    });
  });
  // A real loopback CONNECT exchange for the original Free Proxy connectivity
  // probe. It serves only the explicit synthetic echo URL; never forwards traffic.
  providerServer.on('connect',(req,socket,head)=>{
    freeProxyTunnels.add(socket);socket.on('close',()=>freeProxyTunnels.delete(socket));socket.on('error',()=>{});socket.setTimeout(5000,()=>socket.destroy());
    if(req.url!=='127.0.0.1:'+providerServer.address().port){socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');return;}
    socket.write('HTTP/1.1 200 Connection Established\r\n\r\n');let wire='',done=false;
    const consume=chunk=>{if(done)return;wire+=chunk.toString();if(wire.length>16384){socket.destroy();return;}if(!wire.includes('\r\n\r\n'))return;done=true;
      if(!wire.startsWith('GET /synthetic-proxy-echo HTTP/1.1\r\n')){socket.destroy();return;}
      freeProxyConnects++;const body=JSON.stringify({ip:'192.0.2.44'});socket.end('HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '+Buffer.byteLength(body)+'\r\nConnection: close\r\n\r\n'+body);
    };
    socket.on('data',consume);if(head.length)consume(head);
  });
  await new Promise(resolve => providerServer.listen(0, '127.0.0.1', resolve));
  testEnv.OMNIROUTE_PROXY_ECHO_URL='http://127.0.0.1:'+providerServer.address().port+'/synthetic-proxy-echo';
  if(testEnv.HA_TEST_FULL_BACKEND==='1') {
    const free=await import('../upstream/omniroute/src/lib/db/freeProxies.ts');
    freeProxyId=(await free.upsertFreeProxy({source:'1proxy',host:'127.0.0.1',port:providerServer.address().port,type:'http',qualityScore:77,latencyMs:null,countryCode:'ZZ',lastValidated:'2099-01-01T00:00:00.000Z'})).id;
    await free.recordFreeProxySync('2000-01-01T00:00:00.000Z');await free.recordFreeProxySyncErrors('proxifly',['synthetic source offline']);
  }
  testEnv.HA_TEST_PROVIDER_PORT = String(providerServer.address().port);
  testEnv.FREE_PROXY_1PROXY_API_URL='http://127.0.0.1:'+providerServer.address().port+'/synthetic-free-proxies';
  testEnv.HA_TEST_OAUTH_REDIRECT='1';
  testEnv.OMNIROUTE_SELF_HOSTED_PROVIDERS = '';
  testEnv.OMNIROUTE_SELF_HOSTED_PROVIDERS_FILE = '';
  const { createProviderNode } = await import('../upstream/omniroute/src/lib/db/providers/nodes.ts');
  const localNode = await createProviderNode({ id: 'openai-compatible-ha-test', type: 'openai-compatible', name: 'Synthetic local provider', prefix: 'ha-local', apiType: 'chat', baseUrl: 'http://127.0.0.1:' + providerServer.address().port + '/v1' });
  if(process.env.HA_TEST_EXTENDED_QUOTA==='1'){
    const {mergeModelCompatOverride}=await import('../upstream/omniroute/src/lib/db/models/compat.ts');
    mergeModelCompatOverride(localNode.id,'synthetic-model',{supportsVision:true});
  }
  console.log('CHECKPOINT registered provider node created');
  const providerApi = await import('../upstream/omniroute/src/lib/db/providers.ts');
  const { hydrateCompatibleNodeCreation } = await import('../upstream/omniroute/src/lib/providers/compatibleNodeCreation.ts');
  const hydrated = hydrateCompatibleNodeCreation(localNode.id, localNode, null, true);
  assert.equal(hydrated.error, null);
  const inferenceConnection = await providerApi.createProviderConnection({ provider: localNode.id, authType: 'apikey', name: 'Synthetic inference connection', apiKey: 'synthetic-provider', isActive: true, providerSpecificData: hydrated.data });
  if(testEnv.HA_TEST_FULL_BACKEND==='1'){
    const recoveryNode=await createProviderNode({id:'openai-compatible-ha-recovery',type:'openai-compatible',name:'Synthetic quota recovery',prefix:'ha-recovery',apiType:'chat',baseUrl:'http://127.0.0.1:'+providerServer.address().port+'/recovery/v1'});
    const recoveryHydrated=hydrateCompatibleNodeCreation(recoveryNode.id,recoveryNode,null,true);assert.equal(recoveryHydrated.error,null);
    await providerApi.createProviderConnection({provider:recoveryNode.id,authType:'apikey',name:'Synthetic recovery transport',apiKey:'synthetic-recovery-provider',isActive:true,providerSpecificData:recoveryHydrated.data});
    const node=await createProviderNode({id:'openai-compatible-ha-quota',type:'openai-compatible',name:'Synthetic quota polling',prefix:'ha-quota',apiType:'chat',baseUrl:hydrated.data.baseUrl});
    quotaPollProvider=await providerApi.createProviderConnection({provider:node.id,authType:'oauth',name:'Synthetic owned quota refresh',apiKey:'synthetic-quota-read-only',accessToken:'synthetic-quota-read-only',expiresAt:'2099-01-01T00:00:00.000Z',isActive:true,
      providerSpecificData:{apiType:'chat',baseUrl:hydrated.data.baseUrl,quotaEndpoint:{url:'http://127.0.0.1:'+providerServer.address().port+'/synthetic-quota',auth:'none',quotas:{daily:{used:'$.used',total:'$.total',resetAt:'$.resetAt'}}}}});
  }
  console.log('CHECKPOINT registered inference credential created');
  testEnv.REQUIRE_API_KEY = 'true';
  const inference = await keys.createApiKey('Synthetic inference client', 'ha_test_machine');
  // The expanded failure scenarios can outlive RPM's two-minute history.
  // Exercise the durable request cap across epochs independently of that clock.
  bootstrap.prepare('UPDATE api_keys SET rate_limits=? WHERE id=?').run(JSON.stringify([{window:31536000,limit:2}]),inference.id);
  const reservationBlocked = await keys.createApiKey('Synthetic unsupported hard token quota', 'ha_test_machine');
  bootstrap.prepare('INSERT INTO api_key_quota_limits(api_key_id,tpm_limit) VALUES (?,?)').run(reservationBlocked.id, 100);
  const usdBlocked = await keys.createApiKey('Synthetic unsupported hard USD quota', 'ha_test_machine');
  bootstrap.prepare('INSERT INTO api_key_quota_limits(api_key_id,monthly_amount_usd) VALUES (?,?)').run(usdBlocked.id, 10);
  const bounded = await keys.createApiKey('Synthetic bounded quota', 'ha_test_machine');
  const poolAlpha = await keys.createApiKey('Synthetic pool alpha', 'ha_test_machine');
  const poolBeta = await keys.createApiKey('Synthetic pool beta', 'ha_test_machine');
  const inflight = await keys.createApiKey('Synthetic in-flight liability', 'ha_test_machine');
  const calendar = await keys.createApiKey('Synthetic calendar USD budget', 'ha_test_machine');
  const scoped = await keys.createApiKey('Synthetic scoped token budget', 'ha_test_machine');
  const keyUsage = await keys.createApiKey('Synthetic key USD usage budget', 'ha_test_machine');
  const teamAlpha = await keys.createApiKey('Synthetic Team alpha','ha_test_machine');
  const teamBeta = await keys.createApiKey('Synthetic Team beta','ha_test_machine');
  bootstrap.prepare('INSERT INTO api_key_quota_limits(api_key_id,tpm_limit,monthly_amount_usd) VALUES (?,?,?)').run(inflight.id, 5, 10);
  bootstrap.prepare('INSERT INTO api_key_quota_limits(api_key_id,tpm_limit,monthly_amount_usd) VALUES (?,?,?)').run(bounded.id, 8, 6.5);
  const price = JSON.stringify({ 'synthetic-model': { input: 1000000, output: 1000000 } });
  bootstrap.prepare("INSERT INTO key_value(namespace,key,value) VALUES ('pricing',?,?)").run(localNode.id, price);
  bootstrap.prepare('INSERT INTO api_key_quota_limits(api_key_id,rpm_limit) VALUES (?,?)').run(inference.id, 2);
  const operator = await keys.createApiKey('Synthetic operator', 'ha_test_machine', ['manage']);
  const pricingProbe=await keys.createApiKey('Synthetic pricing cache probe','ha_test_machine');
  bootstrap.prepare('UPDATE api_keys SET allowed_connections=? WHERE id=?').run(JSON.stringify([inferenceConnection.id]),pricingProbe.id);
  bootstrap.prepare('INSERT INTO usage_history(api_key_id,provider,model,timestamp,success,tokens_input,tokens_output) VALUES (?,?,?,?,?,?,?)').run(pricingProbe.id,localNode.id,'synthetic-model',new Date().toISOString(),1,2,1);
  if(testEnv.HA_TEST_FULL_BACKEND==='1')bootstrap.transaction(()=>{
    const insert=bootstrap.prepare('INSERT INTO usage_history(api_key_id,provider,model,timestamp,success,provider_cost_usd) VALUES (?,?,?,?,?,?)');
    for(let n=0;n<retentionExpiredRows;n++)insert.run(pricingProbe.id,localNode.id,'synthetic-model','2000-01-01T00:00:00.000Z',1,0.25);
  })();
  const revoked = await keys.createApiKey('Synthetic revocable operator', 'ha_test_machine', ['manage']);
  const providers = await import('../upstream/omniroute/src/lib/db/providers.ts');
  const initialDecayId=randomUUID();
  bootstrap.prepare('INSERT INTO memories(id,api_key_id,type,content,created_at) VALUES (?,?,?,?,?)').run(initialDecayId,operator.id,'episodic','Synthetic initial owned decay','2000-01-01T00:00:00.000Z');
  const provider = await providers.createProviderConnection({ provider: 'openai', authType: 'apikey', name: 'Failover provider', apiKey: 'synthetic-token-before-promotion' });
  const recoveryProvider=await providers.createProviderConnection({provider:'openai',authType:'apikey',name:'Synthetic cooldown recovery',apiKey:'synthetic-cooldown-only',testStatus:'unavailable',rateLimitedUntil:new Date(Date.now()-60000).toISOString(),lastErrorAt:new Date(Date.now()-120000).toISOString()});
  const oauthProvider=testEnv.HA_TEST_FULL_BACKEND==='1'?await providers.createProviderConnection({provider:'openference',authType:'oauth',name:'Synthetic one-time OAuth',accessToken:'synthetic-http-oauth-old-access',refreshToken:'synthetic-http-oauth-seed',expiresAt:new Date(Date.now()-60000).toISOString(),healthCheckInterval:1}):null;
  const pairedProviders={};
  if(testEnv.HA_TEST_FULL_BACKEND==='1')for(const provider of pairedOAuthProviders)pairedProviders[provider]=await providers.createProviderConnection({provider,authType:'oauth',name:'Synthetic '+provider+' HTTP refresh',accessToken:'synthetic-'+provider+'-http-before',refreshToken:'synthetic-'+provider+'-http-seed',expiresAt:new Date(Date.now()-60000).toISOString(),healthCheckInterval:1,
    providerSpecificData:provider==='gitlab-duo'?{baseUrl:'https://synthetic-gitlab.invalid',clientId:'synthetic-gitlab-http-client'}:provider==='kimi-coding'?{deviceId:'synthetic-http-device',deviceName:'synthetic-http',deviceModel:'synthetic-model',osVersion:'synthetic-os'}:
      provider==='kiro'?{authMethod:'builder-id',region:'us-east-1',clientId:'synthetic-http-old-client',clientSecret:'synthetic-http-old-secret'}:
      provider==='amazon-q'?{authMethod:'external_idp',clientId:'synthetic-http-idp-client',tokenEndpoint:'https://login.microsoftonline.com/synthetic-tenant/oauth2/v2.0/token',scopes:'synthetic-codewhisperer offline_access'}:{}});
  bootstrap.prepare('INSERT INTO call_logs(id,timestamp,artifact_relpath) VALUES (?,?,?)').run('expired-log', '2000-01-01T00:00:00.000Z', 'expired-artifact');
  for (const path of ['expired-artifact', 'orphan-artifact']) bootstrap.prepare('INSERT INTO ha_call_log_artifacts(path,payload,mtime_ms) VALUES (?,?,?)').run(path, '{}', 0);
  if(testEnv.HA_TEST_FULL_BACKEND==='1')for(const [id,expiry] of [['old-reasoning',1],['promotion-reasoning',3000000000],['retained-reasoning',3000000000]])
    bootstrap.prepare('INSERT INTO reasoning_cache(tool_call_id,provider,model,reasoning,char_count,expires_at) VALUES (?,?,?,?,?,?)').run(id,'synthetic','synthetic-model','Synthetic reasoning '+id,20,expiry);
  if(testEnv.HA_TEST_FULL_BACKEND==='1'&&process.env.HA_TEST_RESERVATIONS==='1'){
    const node=await createProviderNode({id:'openai-compatible-ha-legacy',type:'openai-compatible',name:'Synthetic legacy quota',prefix:'ha-legacy',apiType:'chat',baseUrl:'http://127.0.0.1:'+providerServer.address().port+'/legacy/v1'});
    const credentials=hydrateCompatibleNodeCreation(node.id,node,null,true);assert.equal(credentials.error,null);
    legacyHttpConnection=await providerApi.createProviderConnection({provider:node.id,authType:'apikey',name:'Synthetic legacy quota',apiKey:'synthetic-legacy-provider',isActive:true,providerSpecificData:credentials.data});
    legacyHttpKey=await keys.createApiKey('Synthetic historical shared pool','ha_test_machine');legacyHttpPoolId=randomUUID();
    bootstrap.prepare('INSERT INTO quota_pools(id,connection_id,name) VALUES (?,?,?)').run(legacyHttpPoolId,legacyHttpConnection.id,'Synthetic historical pool');
    bootstrap.prepare('INSERT INTO quota_allocations(pool_id,api_key_id,weight,policy) VALUES (?,?,?,?)').run(legacyHttpPoolId,legacyHttpKey.id,100,'hard');
    bootstrap.prepare('INSERT INTO provider_plans(connection_id,provider,dimensions_json) VALUES (?,?,?)').run(legacyHttpConnection.id,node.id,JSON.stringify([{unit:'tokens',window:'daily',limit:8},{unit:'usd',window:'daily',limit:6.5}]));
    bootstrap.prepare("INSERT INTO key_value(namespace,key,value) VALUES ('pricing',?,?)").run(node.id,JSON.stringify({model:{input:1000000,output:1000000}}));
    for(const [unit,used]of [['tokens',3],['usd',3]])bootstrap.prepare('INSERT INTO quota_consumption(api_key_id,dimension_key,bucket_index,consumed,updated_at) VALUES (?,?,?,?,?)').run(legacyHttpKey.id,legacyHttpPoolId+':'+unit+':daily',Math.floor(bootstrap.serverTime()/86400000),used,bootstrap.serverTime());
  }
  await bootstrap.close();
  if(legacyHttpKey){
    const cli=async(...args)=>JSON.parse((await promisify(execFile)(process.execPath,['scripts/quota.mjs',...args],{env:{...process.env,HA_MONGO_URI:test.uri},timeout:70000,maxBuffer:1048576})).stdout);
    const draft=await cli('pool-inspect',test.database,legacyHttpPoolId,'--offline','--models=model');
    legacyHttpEvidence={...draft,historyVerified:true,verifiedBy:'synthetic-HTTP-reviewer',reference:'synthetic-complete-history',rows:draft.rows.map(row=>({...row,used:row.bucket===Math.floor(Date.parse(draft.observedAt)/poolWindows[row.window])?{requests:1,tokens:3,usd:3000000000}[row.unit]:0}))};
    const file=path.join(dataDir,'legacy-pool-evidence.json');writeFileSync(file,JSON.stringify(legacyHttpEvidence),{mode:0o600});assert.equal((await cli('pool-apply',test.database,file,'--offline')).baselined,true);
  }
  const primary = await spawn(); await waitReady(primary, true);
  const standby = await spawn(); await waitReady(standby, false);
  assert.equal((await call(primary, { operation: 'state' })).value.phase, 'ready');
  assert.equal((await call(standby, { operation: 'state' })).value.phase, 'starting');
  assert.equal((await call(primary, { operation: 'state' })).value.maintenanceStarted, true);
  assert.equal((await call(standby, { operation: 'state' })).value.maintenanceStarted, false);
  assert.equal((await call(primary,{operation:'state'})).value.recoveryStarted,true);
  assert.equal((await call(standby,{operation:'state'})).value.recoveryStarted,false);
  assert.equal((await call(primary,{operation:'state'})).value.quotaRefresh.started,true);
  assert.equal((await call(standby,{operation:'state'})).value.quotaRefresh.started,false);
  assert.match((await call(standby,{operation:'quotaRefreshStart'})).error,/active ownership required/);
  assert.equal((await call(primary,{operation:'providerLimitsState'})).value.started,true);
  assert.equal((await call(standby,{operation:'providerLimitsState'})).value.started,false);
  assert.match((await call(standby,{operation:'providerLimitsSync'})).error,/active ownership required/);
  assert.equal((await call(primary,{operation:'quotaRecoveryState'})).value.started,true);
  assert.equal((await call(standby,{operation:'quotaRecoveryState'})).value.started,false);
  assert.match((await call(standby,{operation:'quotaRecovery'})).error,/active ownership required/);
  assert.equal((await call(primary,{operation:'reasoningCleanupState'})).value.runs,1);
  assert.equal((await call(standby,{operation:'reasoningCleanupState'})).value.started,false);
  assert.match((await call(standby,{operation:'reasoningCleanup'})).error,/active ownership required/);
  assert.match((await call(standby,{operation:'recovery'})).error,/active ownership required/);
  // Warm a real process-local proxy resolution on the standby. With Redis
  // disabled, promotion itself must invalidate this separately cached result.
  assert.equal((await call(primary,{operation:'proxyConfig',providerId:provider.id,value:{type:'http',host:'127.0.0.1',port:49101}})).error,undefined);
  assert.equal((await call(standby,{operation:'resolveProxy',providerId:provider.id})).value.proxy.port,49101);
  assert.equal((await call(primary,{operation:'proxyConfig',providerId:provider.id,value:{type:'http',host:'127.0.0.1',port:49102}})).error,undefined);
  assert.equal((await test.db.collection('provider_connections').findOne({id:recoveryProvider.id})).test_status,'active');
  assert.equal((await call(standby, { operation: 'maintenance' })).error, 'HA_FENCED');
  const maintenance = await call(primary, { operation: 'maintenance' });
  assert.equal(maintenance.error, undefined);
  assert.equal(await test.db.collection('ha_call_log_artifacts').countDocuments({ path: { $in: ['expired-artifact', 'orphan-artifact'] } }), 0);
  assert.equal(await test.db.collection('call_logs').countDocuments({ id: 'expired-log' }), 0);
  router = await trafficRouter([primary.base, standby.base]);
  const decayDeadline=Date.now()+10000;
  while(await test.db.collection('memories').countDocuments({id:initialDecayId})&&Date.now()<decayDeadline)await delay(50);
  assert.equal(await test.db.collection('memories').countDocuments({id:initialDecayId}),0,'Opt-in owned decay must run even with the global background flag disabled');
  assert.equal((await call(primary,{operation:'state'})).value.memoryDecay.started,true);
  assert.equal((await call(standby,{operation:'state'})).value.memoryDecay,undefined);
  assert.ok((await call(standby,{operation:'memoryDecay'})).error,'Standby cannot invoke a destructive decay pass');
  async function request(child, endpoint, token = operator.key, init = {}) {
    return fetch(child.base + endpoint, { ...init, headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', ...init.headers }, signal: AbortSignal.timeout(10000) });
  }
  if(quotaPollProvider){
    const response=await request(router,'/api/usage/'+quotaPollProvider.id),body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.ok(body.quotas?.daily);
    assert.equal((await call(primary,{operation:'ageQuotaCache',connectionId:quotaPollProvider.id})).value,true);
  }
  async function verifyVectorHttp(child,apiKeyId) {
    for(const strategy of ['semantic','hybrid']) {
      const response=await request(child,'/api/memory/retrieve-preview',operator.key,{method:'POST',body:JSON.stringify({apiKeyId,query:strategy==='semantic'?'semantic satellite':'satellite',strategy,maxTokens:1000,limit:10})});
      const body=await response.json();assert.equal(response.status,200,JSON.stringify(body));
      assert.equal(body.memories[0]?.id,vectorMemoryId,JSON.stringify(body));assert.equal(body.memories[0].tier,strategy==='semantic'?'vector':'hybrid-rrf');
      assert.ok(Number.isFinite(body.memories[0].vecScore));if(strategy==='hybrid')assert.ok(Number.isFinite(body.memories[0].ftsScore));
    }
  }
  if(testEnv.HA_TEST_FULL_BACKEND==='1') {
    const priceRequest={providerId:localNode.id,model:'synthetic-model'};
    assert.equal((await call(primary,{operation:'pricing',...priceRequest})).value.input,1000000);
    const status=async()=>{const response=await request(router,'/api/keys/'+pricingProbe.id+'/usage-limits'),body=await response.json();assert.equal(response.status,200,JSON.stringify(body));return body.status.dailySpentUsd;};
    assert.equal(await status(),3);
    assert.equal((await call(primary,{operation:'pricingUpdate',...priceRequest,value:{input:2000000,output:1000000}})).error,undefined);
    assert.equal(await status(),5,'Compiled USD status must observe pricing committed by the source module immediately');
    assert.equal((await call(standby,{operation:'pricing',...priceRequest})).value.input,2000000);
    const reset=await request(router,'/api/pricing',operator.key,{method:'PATCH',body:JSON.stringify({[localNode.id]:{'synthetic-model':{input:1000000,output:1000000}}})});assert.equal(reset.status,200,await reset.text());
    assert.equal((await call(primary,{operation:'pricing',...priceRequest})).value.input,1000000,'Source pricing must observe the compiled API commit immediately');
    assert.equal(await status(),3);
    console.log('PASS actual pricing cache consistency: source/compiled caches warmed; source commit updates compiled USD status and compiled PATCH updates source reads immediately without Redis');
    assert.equal((await call(primary,{operation:'state'})).value.usageRetention.started,true);
    assert.equal((await call(standby,{operation:'state'})).value.usageRetention,undefined);
    assert.ok(await test.db.collection('_ha_usage_history').countDocuments({api_key_id:pricingProbe.id})>=500,'Startup compliance cleanup must archive its bounded batch instead of losing raw financial history');
    const cleanupDeadline=Date.now()+50000;
    while(await test.db.collection('usage_history').countDocuments({api_key_id:pricingProbe.id,timestamp:'2000-01-01T00:00:00.000Z'})&&Date.now()<cleanupDeadline)await delay(250);
    assert.equal(await test.db.collection('usage_history').countDocuments({api_key_id:pricingProbe.id,timestamp:'2000-01-01T00:00:00.000Z'}),0,'The actual owned delayed retention timer must archive expired raw usage');
    assert.equal(await test.db.collection('_ha_usage_history').countDocuments({api_key_id:pricingProbe.id,timestamp:'2000-01-01T00:00:00.000Z'}),retentionExpiredRows);
    const archivedDay=await test.db.collection('daily_usage_summary').findOne({provider:localNode.id,model:'synthetic-model',date:'2000-01-01'});
    assert.equal(archivedDay.total_requests,retentionExpiredRows);assert.equal(archivedDay.total_cost,retentionExpiredRows*0.25);
    assert.ok((await call(primary,{operation:'state'})).value.usageRetention.runs>=1);
    assert.equal(await status(),3);
    console.log('PASS actual owned usage retention timer: delayed active job archives expired history despite global background flag; standby starts no job and current-day USD HTTP remains unchanged');
    const tomorrow=new Date(Date.now()+86400000).toISOString().slice(0,10);
    assert.ok((await call(standby,{operation:'usageRetention',before:tomorrow})).error,'Standby cannot archive usage');
    const archived=await call(primary,{operation:'usageRetention',before:tomorrow});assert.equal(archived.error,undefined);assert.equal(archived.value.errors,0);assert.ok(archived.value.deletedRows>0);
    assert.equal(await test.db.collection('usage_history').countDocuments({api_key_id:pricingProbe.id}),0);
    assert.equal(await test.db.collection('_ha_usage_history').countDocuments({api_key_id:pricingProbe.id}),retentionExpiredRows+1);
    assert.equal(await status(),3,'Compiled USD HTTP must include financial archive after raw deletion');
    console.log('PASS actual application archive: standby denied, original rollup/fenced purge removes raw history, compiled key USD HTTP still reports identical spend');
  }
  if(testEnv.HA_TEST_FULL_BACKEND==='1') {
    const endpoint='/api/settings/proxies/assignments?resolveConnectionId='+provider.id;
    let response=await request(router,endpoint),body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.proxy.port,49102);
    assert.equal((await call(primary,{operation:'proxyConfig',providerId:provider.id,value:{type:'http',host:'127.0.0.1',port:49103}})).error,undefined);
    response=await request(router,endpoint);body=await response.json();assert.equal(response.status,200,JSON.stringify(body));
    assert.equal(body.proxy.port,49103,'Compiled proxy resolution must observe a committed write made through the source module in the same process');
    assert.equal((await call(primary,{operation:'proxyConfig',providerId:provider.id,value:{type:'http',host:'127.0.0.1',port:49102}})).error,undefined);
    response=await request(router,endpoint);body=await response.json();assert.equal(body.proxy.port,49102);
    console.log('PASS actual compiled proxy cache: committed source-module writes invalidate the compiled resolver immediately without Redis');
  }
  async function verifyGamification(child,requests) {
    if(testEnv.HA_TEST_FULL_BACKEND!=='1')return;
    const {XP_REWARDS}=await import('../upstream/omniroute/src/lib/gamification/xp.ts');
    const expected=requests+XP_REWARDS.badge_unlock,deadline=Date.now()+10000;
    for(;;) {
      const response=await request(child,'/api/gamification/level?apiKeyId='+inference.id),body=await response.json();
      assert.equal(response.status,200,JSON.stringify(body));
      const leaderboard=await request(child,'/api/gamification/leaderboard?apiKeyId='+inference.id),scores=await leaderboard.json();
      assert.equal(leaderboard.status,200,JSON.stringify(scores));
      if(body.level?.totalXp===expected && scores.entries.find(row=>row.apiKeyId===inference.id)?.score===expected) {
        assert.equal(body.streak.current,1);assert.ok(scores.myRank>=1);break;
      }
      assert.ok(Date.now()<deadline,'Actual chat must persist its original XP and leaderboard event');await delay(100);
    }
    for(const suffix of ['', '?apiKeyId='+inference.id]) {
      const earned=await request(child,'/api/gamification/badges/earned'+suffix),body=await earned.json();
      assert.equal(earned.status,200,JSON.stringify(body));assert.equal(body.badges.filter(row=>row.badgeId==='first-token').length,1);
    }
    const anomalies=await request(child,'/api/gamification/anomalies'),body=await anomalies.json();
    assert.equal(anomalies.status,200,JSON.stringify(body));assert.ok(Array.isArray(body.anomalies));
  }
  if(oauthProvider) {
    assert.equal((await call(primary,{operation:'state'})).value.tokenHealth.started,true);
    assert.equal((await call(standby,{operation:'state'})).value.tokenHealth,undefined);
    const deadline=Date.now()+20000;
    while(oauthCalls<1 && Date.now()<deadline) await delay(100);
    assert.equal(oauthCalls,1,'The owned delayed token-health pass must reach the synthetic OAuth server');
    while(await test.db.collection('_ha_credential_intents').countDocuments({connectionId:oauthProvider.id,state:'completed'})!==1 && Date.now()<deadline) await delay(100);
    assert.equal(await test.db.collection('_ha_credential_intents').countDocuments({connectionId:oauthProvider.id,state:'completed'}),1);
    assert.ok((await test.db.collection('provider_connections').findOne({id:oauthProvider.id})).refresh_token.startsWith('enc:v1:'));
    const refreshed=await request(router,'/api/providers/'+oauthProvider.id+'/refresh',operator.key,{method:'POST'});
    assert.equal(refreshed.status,200,await refreshed.text());assert.equal(oauthCalls,2);
    assert.ok(oauthReceivedAt[1]-oauthReceivedAt[0]>=2000,'The original default group spacing must also be durable across completed refresh calls');
    knownOAuthCiphertext=(await test.db.collection('provider_connections').findOne({id:oauthProvider.id})).refresh_token;
    console.log('PASS compiled OAuth HTTP: active-only delayed health sweep, original refresh endpoint, two actual synthetic HTTP rotations and encrypted durable token publication');
    for(const provider of pairedOAuthProviders) {
      const id=pairedProviders[provider].id,deadline=Date.now()+15000;
      while(await test.db.collection('_ha_credential_intents').countDocuments({connectionId:id,state:'completed'})!==1 && Date.now()<deadline)await delay(100);
      const initial=provider==='kiro'?2:1;
      assert.equal(pairedOAuthCalls[provider],initial,provider+' initial token POSTs');assert.equal(await test.db.collection('_ha_credential_intents').countDocuments({connectionId:id,state:'completed'}),1,provider+' durable completed intent');
      if(provider==='kiro'){
        assert.equal(kiroRegistrations,1);const stored=await test.db.collection('provider_connections').findOne({id}),metadata=JSON.parse(stored.provider_specific_data);
        assert.equal(metadata.clientId,'synthetic-http-registered-client');assert.ok(metadata.clientSecret.startsWith('enc:v1:'));assert.equal(JSON.stringify(metadata).includes('synthetic-http-registered-secret'),false);
        assert.equal((await test.db.collection('_ha_credential_intents').findOne({connectionId:id,state:'completed'})).kiroPhase,'retry_dispatched');
      }
      const refreshed=await request(router,'/api/providers/'+id+'/refresh',operator.key,{method:'POST'}),body=await refreshed.json();
      assert.equal(refreshed.status,200,JSON.stringify(body));
      if(provider==='codex'){assert.equal(body.skipped,true);assert.equal(pairedOAuthCalls[provider],1);}else assert.equal(pairedOAuthCalls[provider],initial+1);
      pairedOAuthCiphertext[provider]=(await test.db.collection('provider_connections').findOne({id})).refresh_token;
      assert.ok(pairedOAuthCiphertext[provider].startsWith('enc:v1:'));
    }
    const sendImport=async record=>{const response=await request(router,'/api/oauth/codex/import',operator.key,{method:'POST',body:JSON.stringify({accounts:[record]})}),body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.imported,1,JSON.stringify(body));return body.results[0].connectionId;};
    importedProviderId=await sendImport(importRecord);assert.equal(importCalls,1);
    assert.equal(await sendImport(importRecord),importedProviderId);assert.equal(importCalls,1,'A repeated normalized import must not exchange the token again');
    const changed=await request(router,'/api/providers/'+importedProviderId,operator.key,{method:'PUT',body:JSON.stringify({priority:7,providerSpecificData:{workspaceId:importRecord.account_id,chatgptUserId:'synthetic-import-user',codexFingerprintMode:'off'}})});assert.equal(changed.status,200,await changed.text());
    const before=await test.db.collection('provider_connections').findOne({id:importedProviderId});
    assert.equal(await sendImport({...importRecord,refresh_token:'synthetic-import-seed-upsert',priority:99}),importedProviderId);assert.equal(importCalls,2);
    const stored=await test.db.collection('provider_connections').findOne({id:importedProviderId});assert.equal(stored.priority,before.priority);assert.equal(JSON.parse(stored.provider_specific_data).chatgptUserId,'synthetic-import-user');assert.equal(JSON.parse(stored.provider_specific_data).codexFingerprintMode,'off');assert.ok(stored.refresh_token.startsWith('enc:v1:'));
    assert.equal(pairedOAuthCalls.codex,1);
    console.log('PASS compiled Codex import HTTP: actual normalization and original writer, encrypted receipt attachment, repeat without exchange and metadata/priority-preserving workspace upsert');
    console.log('PASS compiled Claude/Codex/Cline/Kimi/GitLab/Kiro/Amazon Q/GitHub OAuth HTTP: original owned sweep/manual helpers, device/IdP contracts, fenced Kiro registration/retry, GitHub App single-use grant guard and encrypted early publication, Codex manual skip');
  }
  assert.equal((await request(standby, '/api/settings')).status, 503);
  assert.equal((await request(primary, '/api/settings', 'invalid')).status, 403);
  assert.equal((await request(primary, '/api/settings', 'invalid', { headers: { 'x-omniroute-auth-kind': 'management_key', 'x-omniroute-auth-label': 'local-cli-token' } })).status, 403);
  let response = await request(router, '/api/settings');
  let body = await response.json();
  assert.equal(response.status, 200, JSON.stringify(body));
  assert.equal(body.stickyRoundRobinLimit, 7);
  if (testEnv.HA_TEST_FULL_BACKEND==='1') {
    response = await dashboardRequest(router,'/api/auth/login',null,{method:'POST',body:JSON.stringify({password:'synthetic-wrong-password'})});
    assert.equal(response.status,401,await response.text());
    for (const second of [false,true]) {
      response = await dashboardRequest(router,'/api/auth/login',null,{method:'POST',body:JSON.stringify({password:'synthetic-dashboard-password'})});
      assert.equal(response.status,200,await response.text());
      const cookie = response.headers.getSetCookie().find(value=>value.startsWith('auth_token='))?.split(';')[0];
      assert.ok(cookie,'Actual login must issue a dashboard session cookie');
      if (second) revokedDashboardCookie=cookie; else dashboardCookie=cookie;
    }
    // Prove the natural timer before browser polling can read/refill this cache.
    if(quotaPollProvider){
      const deadline=Date.now()+75000;let latest;
      // Seeing the provider request is not acknowledgement of its later Mongo
      // snapshot write. Wait for the actual durable result, not only a counter.
      do{latest=await test.db.collection('quota_snapshots').find({connection_id:quotaPollProvider.id,window_key:'daily'}).sort({created_at:-1}).limit(1).next();if(quotaPollCalls>=2&&latest?.remaining_percentage===80)break;await delay(100);}while(Date.now()<deadline);
      assert.equal(quotaPollCalls,2,'Only the owned original timer may perform the second synthetic quota fetch');assert.equal(latest?.remaining_percentage,80);
      const limitsDeadline=Date.now()+20000;let limits;
      do{limits=(await call(primary,{operation:'providerLimitsState'})).value;if(limits.runs>=1)break;await delay(100);}while(Date.now()<limitsDeadline);
      assert.ok(limits.runs>=1,'Owned Provider Limits startup timer must execute the original sync');assert.ok(limits.lastRun.total>=1);
      const limitsCache=await test.db.collection('key_value').findOne({namespace:'providerLimitsCache',key:quotaPollProvider.id});assert.ok(limitsCache);
      assert.ok(Number.isFinite(Date.parse((await request(router,'/api/settings').then(response=>response.json())).provider_limits_auto_sync_last_run)),'Scheduled sync timestamp must reach the actual Settings API');
      console.log('PASS actual owned quota polling HTTP: original manual API warms shared state, quota timer persists changed snapshot, original Provider Limits startup sync persists its cache/timestamp and standby cannot run either job');
    }
    await verifyDashboardHttp(router,dashboardCookie,'before failover');
    await verifyReasoningHttp(router,'before failover',2);
    response=await dashboardRequest(router,'/api/settings/database',dashboardCookie);
    const databaseSettings=await response.json();assert.equal(response.status,200,JSON.stringify(databaseSettings));assert.equal(databaseSettings.driver,'mongodb');
    assert.ok(databaseSettings.location.schemaVersion>0);assert.equal(databaseSettings.stats.pageCount,null);assert.equal(databaseSettings.stats.freelistCount,null);assert.ok(databaseSettings.stats.databaseSizeBytes>0);
    response=await dashboardRequest(router,'/api/storage/health',dashboardCookie);
    const storage=await response.json();assert.equal(response.status,200,JSON.stringify(storage));assert.equal(storage.driver,'mongodb');assert.equal(storage.dbPath,'mongodb:'+test.database);assert.equal(storage.backupCount,null);assert.equal(storage.backupManagedExternally,true);
    console.log('PASS compiled MongoDB storage HTTP: real namespace/allocated size and migration version, explicit N/A SQLite pages and external backup inventory');
    if(process.env.HA_TEST_BROWSER_BIN)await verifyBrowserHttp(router.base,'synthetic-dashboard-password','before failover');
    response = await dashboardRequest(router,'/api/settings',dashboardCookie);
    assert.equal(response.status,200,await response.text());
    response = await dashboardRequest(router,'/api/settings',dashboardCookie,{method:'PATCH',headers:{origin:'https://untrusted.invalid'},body:JSON.stringify({stickyRoundRobinLimit:999})});
    assert.equal(response.status,403,await response.text());
    response = await dashboardRequest(router,'/api/settings',dashboardCookie);
    body=await response.json(); assert.equal(response.status,200,JSON.stringify(body)); assert.equal(body.stickyRoundRobinLimit,7,'Rejected browser mutation must not change settings');
  }
  response = await request(router, '/api/providers/' + provider.id); body = await response.json();
  assert.equal(response.status, 200, JSON.stringify(body));
  assert.equal(body.connection.name, 'Failover provider');
  assert.notEqual(body.connection.apiKey, 'synthetic-token-before-promotion');
  response = await request(router, '/api/providers/' + provider.id, operator.key, { method: 'PUT', body: JSON.stringify({ name: 'HTTP updated provider' }) });
  body = await response.json(); assert.equal(response.status, 200, JSON.stringify(body));
  assert.equal(body.connection.name, 'HTTP updated provider');
  if(freeProxyId) {
    const deadline=Date.now()+8000;while(freeProxySyncCalls<1&&Date.now()<deadline)await delay(50);assert.equal(freeProxySyncCalls,1);
    assert.equal((await call(primary,{operation:'state'})).value.freeProxySync.started,true);
    assert.equal((await call(primary,{operation:'state'})).value.freeProxySync.runs,1);
    assert.equal((await call(standby,{operation:'state'})).value.freeProxySync.started,false);
    response=await request(router,'/api/settings/free-proxies?sortBy=latency',operator.key);body=await response.json();assert.equal(response.status,200,JSON.stringify(body));
    assert.equal(body.data.proxies.find(row=>row.id===freeProxyId).inPool,false);assert.equal(body.data.stats.avgQuality,77);assert.notEqual(body.data.stats.lastSyncAt,'2000-01-01T00:00:00.000Z');assert.deepEqual(body.data.syncErrors.proxifly,['synthetic source offline']);
    assert.ok(body.data.proxies.some(row=>row.host==='93.184.216.34'&&row.port===8081),'Actual owned timer must populate the original catalog');
    response=await request(router,'/api/settings/free-proxies/'+freeProxyId+'/add-to-pool',operator.key,{method:'POST'});body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.ok(body.poolProxyId);promotedFreeProxyId=body.poolProxyId;assert.equal(freeProxyConnects,1);
    assert.equal((await test.db.collection('free_proxies').findOne({id:freeProxyId})).pool_proxy_id,promotedFreeProxyId);assert.ok(await test.db.collection('proxy_registry').findOne({id:promotedFreeProxyId}));
    console.log('PASS real Free Proxy HTTP: original list/stats/sync state, real loopback CONNECT connectivity probe and atomic promotion into registry without public egress');
  }
  response = await request(router, '/api/keys', operator.key, { method: 'POST', body: JSON.stringify({ name: 'Created through real HTTP' }) });
  body = await response.json();
  assert.equal(response.status, 201, JSON.stringify(body));
  const createdId = body.id;
  assert.ok(createdId);
  assert.ok(await test.db.collection('api_keys').findOne({ id: createdId }));
  response = await request(router, '/api/memory', operator.key, { method: 'POST', body: JSON.stringify({
    key: 'http-failover-memory', content: 'Persistent memory before failover', apiKeyId: createdId, sessionId: 'http-session',
  }) });
  body = await response.json();
  assert.equal(response.status, 200, JSON.stringify(body));
  // Upstream currently returns the Memory object in the field named `id`.
  const memoryId = typeof body.id === 'string' ? body.id : body.id.id;
  assert.equal(typeof memoryId, 'string');
  response = await request(primary, '/api/memory?apiKeyId=' + createdId);
  body = await response.json();
  assert.equal(response.status, 200, JSON.stringify(body));
  assert.equal(body.stats.total, 1); assert.ok(body.stats.tokensUsed > 0);
  response = await request(router, '/api/memory/engine-status'); body = await response.json();
  assert.equal(response.status, 200, JSON.stringify(body)); assert.equal(body.keyword.backend, 'MongoDB text');
  response = await request(router, '/api/memory/retrieve-preview', operator.key, { method: 'POST', body: JSON.stringify({
    apiKeyId: createdId, query: 'Persistent memory', strategy: 'semantic', maxTokens: 1000, limit: 10,
  }) }); body = await response.json();
  assert.equal(response.status, 200, JSON.stringify(body));
  assert.equal(body.memories[0].id, memoryId); assert.equal(body.memories[0].tier, 'mongodb-text');
  if(testEnv.HA_TEST_VECTOR_HTTP==='1') {
    response=await request(router,'/api/settings/memory',operator.key,{method:'PUT',body:JSON.stringify({embeddingSource:'remote',embeddingProviderModel:null,customBaseUrl:'http://127.0.0.1:'+providerServer.address().port+'/v1',customModelId:'synthetic-memory-embedding'})});
    body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.customModelId,'synthetic-memory-embedding');
    response=await request(router,'/api/memory',operator.key,{method:'POST',body:JSON.stringify({apiKeyId:createdId,key:'http-vector-memory',content:'Vector only satellite',sessionId:'http-session'})});body=await response.json();assert.equal(response.status,200,JSON.stringify(body));vectorMemoryId=typeof body.id==='string'?body.id:body.id.id;
    response=await request(router,'/api/memory/reindex',operator.key,{method:'POST',body:JSON.stringify({force:true})});assert.equal(response.status,200,await response.text());
    const deadline=Date.now()+15000;while(await test.db.collection('memories').countDocuments({id:{$in:[memoryId,vectorMemoryId]},'_ha_vector.values':{$exists:true}})!==2&&Date.now()<deadline)await delay(100);
    assert.equal(await test.db.collection('memories').countDocuments({id:{$in:[memoryId,vectorMemoryId]},'_ha_vector.values':{$exists:true}}),2);
    await verifyVectorHttp(router,createdId);assert.ok(embeddingCalls>0);
    console.log('PASS actual Memory vector/hybrid HTTP: original custom embedding transport and reindex route, unknown dimensions measured, native L2 and lexical/vector RRF scores');
  }
  const decayCreated=await request(router,'/api/memory',operator.key,{method:'POST',body:JSON.stringify({apiKeyId:operator.id,key:'synthetic-decay-promotion',type:'episodic',content:'Synthetic decayed memory for promotion'})}),decayBody=await decayCreated.json();
  assert.equal(decayCreated.status,200,JSON.stringify(decayBody));const promotionDecayId=typeof decayBody.id==='string'?decayBody.id:decayBody.id.id;
  assert.equal((await request(primary, '/api/settings', revoked.key)).status, 200);
  assert.equal((await call(primary, { operation: 'revoke', keyId: revoked.id })).value, true);
  assert.equal((await request(primary, '/api/settings', revoked.key)).status, 403);
  if(redisFixture){
    const connected=async()=>{const a=await call(primary,{operation:'state'}),b=await call(standby,{operation:'state'});return a.value?.redis.connected&&b.value?.redis.connected;};
    await waitRedis(connected,'both actual application subscribers');
    const epoch=(await call(primary,{operation:'state'})).value.epoch;
    const election=await redisFixture.failover();await waitRedis(connected,'application subscriber reconnect after Redis failover');
    assert.equal((await request(primary,'/readyz')).status,200);assert.equal((await request(standby,'/readyz')).status,503);
    const csrfResponse=await dashboardRequest(router,'/api/auth/csrf',dashboardCookie),csrf=await csrfResponse.json();assert.equal(csrfResponse.status,200,JSON.stringify(csrf));assert.ok(csrf.token);
    const created=await dashboardRequest(router,'/api/keys',dashboardCookie,{method:'POST',headers:{'x-omniroute-csrf':csrf.token},body:JSON.stringify({name:'Synthetic Redis outage revocation',scopes:['manage']})}),key=await created.json();assert.equal(created.status,201,JSON.stringify(key));
    redisRevokedKey=key;
    const warmed=await request(router,'/api/settings',key.key);assert.equal(warmed.status,200,await warmed.text());const metadata=await call(standby,{operation:'keyMetadata',key:key.key});assert.equal(metadata.error,undefined);assert.equal(metadata.value,true);
    await redisFixture.outage();
    const deleted=await request(router,'/api/keys/'+key.id,operator.key,{method:'DELETE'});assert.equal(deleted.status,200,await deleted.text());
    assert.equal((await request(router,'/api/settings',key.key)).status,403);assert.equal((await call(standby,{operation:'keyMetadata',key:key.key})).value,false);
    assert.equal((await request(primary,'/readyz')).status,200);assert.equal((await request(standby,'/readyz')).status,503);
    assert.equal((await call(primary,{operation:'state'})).value.epoch,epoch,'Redis failure cannot grant or change Mongo ownership');
    await redisFixture.recover();await waitRedis(connected,'application subscription after total Redis outage',45000);
    assert.equal((await request(router,'/api/settings',key.key)).status,403);assert.equal((await call(standby,{operation:'keyMetadata',key:key.key})).value,false);
    console.log('PASS actual Redis HA HTTP: primary '+election.old+' -> '+election.promoted+', both application subscribers reconnect, warmed key deleted during complete Redis outage stays denied across processes/reconnect and exactly one Mongo-owned active retains its epoch');
  }
  assert.equal((await call(standby, { operation: 'write', value: 'illegal' })).error, 'HA_FENCED');
  assert.equal((await call(primary, { operation: 'settings', value: { stickyRoundRobinLimit: 19 } })).error, undefined);
  assert.equal((await call(primary, { operation: 'provider', providerId: provider.id, value: { apiKey: 'synthetic-token-after-promotion' } })).error, undefined);
  // Pause the old primary rather than terminating it: it must be fenced when it returns.
  const chatBody = JSON.stringify({ model: 'ha-local/synthetic-model', messages: [{ role: 'user', content: 'test' }], stream: false });
  const chatPath = testEnv.HA_TEST_FULL_BACKEND === '1' ? '/v1/chat/completions' : '/api/v1/chat/completions';
  response = await request(router, chatPath, 'invalid', { method: 'POST', body: chatBody });
  assert.ok([401, 403].includes(response.status), `Unauthorized inference returned ${response.status}: ${await response.text()}`); assert.equal(providerCalls, 0);
  response = await request(router, chatPath, reservationBlocked.key, { method: 'POST', body: chatBody });
  assert.equal(response.status, 503);
  assert.match(await response.text(), /durable reservation support|Bounded quota/);
  assert.equal(providerCalls, 0, 'Unsupported hard quota must fail before provider dispatch');
  response = await request(router, chatPath, usdBlocked.key, { method: 'POST', body: chatBody });
  assert.equal(response.status, 503); assert.match(await response.text(), /durable reservation support|Bounded quota/);
  assert.equal(providerCalls, 0, 'Unsupported monthly budget must be rejected before the legacy ledger precheck');
  if(testEnv.HA_TEST_FULL_BACKEND==='1') {
    const catalog=await request(router,'/api/gamification/badges'),body=await catalog.json();
    assert.equal(catalog.status,200,JSON.stringify(body));assert.ok(body.badges.some(row=>row.id==='first-token'));
  }
  response = await request(router, chatPath, inference.key, { method: 'POST', body: chatBody });
  const rawCompletion = await response.text();
  assert.equal(response.status, 200, `HTTP ${response.status}, provider calls=${providerCalls}, requests=${JSON.stringify(providerRequests)}: ${rawCompletion}`);
  assert.ok(rawCompletion.trim(), `Empty completion response; provider calls=${providerCalls}`);
  body = JSON.parse(rawCompletion);
  assert.equal(body.choices[0].message.content, 'synthetic-response'); assert.equal(providerCalls, 1);
  await verifyGamification(router,1);
  assert.equal(await test.db.collection('domain_cost_history').countDocuments({ api_key_id: inference.id }), 1, 'Priced request spend must persist before response acknowledgement');
  let completedLog;
  for (let attempt = 0; attempt < 50; attempt++) {
    completedLog = await test.db.collection('call_logs').findOne({ api_key_id: inference.id, status: 200, detail_state: 'ready' });
    if (completedLog) break;
    await delay(100);
  }
  assert.ok(completedLog, 'Normal chat must persist a ready request artifact');
  const artifactEndpoint = '/api/usage/call-logs/' + completedLog.id;
  let artifactBody;
  if (testEnv.HA_TEST_FULL_BACKEND === '1') {
    response = await request(router, artifactEndpoint); artifactBody = await response.json();
    assert.equal(response.status, 200, JSON.stringify(artifactBody));
    assert.ok(JSON.stringify(artifactBody.responseBody).includes('synthetic-response'));
    response=await dashboardRequest(router,'/api/usage/call-logs?search=200&limit=2&offset=0&apiKey='+encodeURIComponent(inference.id),dashboardCookie);
    const searched=await response.json();assert.equal(response.status,200,JSON.stringify(searched));assert.ok(Array.isArray(searched));assert.ok(searched.some(row=>row.id===completedLog.id),'Status search must return the real completed provider call');
    assert.equal((await fetch(router.base+'/readyz')).status,200);
    console.log('PASS actual call-log search HTTP: persisted provider response matches the original multi-table resolved-account query, INTEGER status CAST TEXT, free-text/key predicate and pagination preserve readiness');
  }
  const boundedBody = JSON.stringify({ ...JSON.parse(chatBody), max_tokens: 3 });
  if(legacyHttpKey){
    const body=JSON.stringify({model:'ha-legacy/model',messages:[{role:'user',content:'Synthetic historical pool'}],stream:false,max_tokens:3});
    const accepted=await request(router,chatPath,legacyHttpKey.key,{method:'POST',body});assert.equal(accepted.status,200,await accepted.text());assert.equal(legacyProviderCalls,1);
    const blocked=await request(router,chatPath,legacyHttpKey.key,{method:'POST',body});assert.equal(blocked.status,429,await blocked.text());assert.equal(legacyProviderCalls,1);
    assert.equal((await test.db.collection('quota_consumption').findOne({api_key_id:legacyHttpKey.id,dimension_key:legacyHttpPoolId+':usd:weekly',bucket_index:Math.floor(Date.now()/poolWindows.weekly)}))._ha_usd_nano,6000000000);
    console.log('PASS actual reviewed legacy pool HTTP: offline CLI imports complete current/previous histories, original proxy admits one bounded call, exact USD settlement retains historical spend in every window and next dispatch refuses');
  }
  if(testEnv.HA_TEST_FULL_BACKEND==='1'){
    // Exercise terminal failure with the application's supported zero-cooldown-
    // retry setting. The synthetic ingress keeps its unchanged five-second
    // deadline; this is not a transport/ownership/quota safety bypass.
    const previousRetry=(await call(primary,{operation:'state'})).value.cachedSettings.requestRetry;
    assert.equal((await call(primary,{operation:'settings',value:{requestRetry:0}})).error,undefined);
    const created=await request(router,'/api/keys',operator.key,{method:'POST',body:JSON.stringify({name:'Synthetic quota recovery owner'})});recoveryKey=await created.json();assert.equal(created.status,201,JSON.stringify(recoveryKey));
    const body={model:'ha-recovery/model',messages:[{role:'user',content:'Synthetic quota-failed turn'}],stream:false,max_tokens:3};
    enqueueRecovery=async turn=>{
      recoveryMode='quota';
      const response=await request(router,chatPath,recoveryKey.key,{method:'POST',headers:{'x-omniroute-quota-recovery':'auto','x-omniroute-session':'ha-recovery-session','x-omniroute-recovery-turn':turn},body:JSON.stringify({...body,messages:[{role:'user',content:'Synthetic quota-failed turn '+turn}]})});
      const wire=await response.text();assert.equal(response.status,202,wire);const queued=JSON.parse(wire);assert.equal(queued.recovery.state,'pending');
      const row=await test.db.collection('quota_recovery_tickets').findOne({id:queued.recovery.id});assert.ok(row.request_ciphertext.startsWith('enc:v1:'));assert.equal(row.request_ciphertext.includes(recoveryKey.key),false);return row.id;
    };
    completedRecoveryId=await enqueueRecovery('complete');
    assert.equal((await request(router,'/api/v1/quota-recoveries/'+completedRecoveryId,inference.key)).status,404);
    recoveryMode='success';await delay(3500);
    assert.equal((await call(primary,{operation:'ageQuotaRecovery',ticketId:completedRecoveryId})).value.changes,1);
    const completed=await call(primary,{operation:'quotaRecovery'});assert.equal(completed.error,undefined,JSON.stringify(completed));
    const response=await request(router,'/api/v1/quota-recoveries/'+completedRecoveryId,recoveryKey.key),result=await response.json();assert.equal(response.status,200,JSON.stringify(result));assert.equal(response.headers.get('x-omniroute-recovery-state'),'completed');assert.equal(result.choices[0].message.content,'synthetic-recovered-response');
    const row=await test.db.collection('quota_recovery_tickets').findOne({id:completedRecoveryId});assert.equal(row.attempt_count,1);assert.equal(row.request_ciphertext,'');assert.ok(row.result_ciphertext.startsWith('enc:v1:'));
    const count=recoveryProviderCalls;assert.equal((await call(primary,{operation:'quotaRecovery'})).error,undefined);assert.equal(recoveryProviderCalls,count);
    console.log('PASS actual quota recovery HTTP: definitive provider quota failure queues an encrypted scoped ticket, original active-owned tick routes the stored turn once, durable encrypted result is returned by the real API and repeated ticks do not dispatch it again');
    assert.equal((await call(primary,{operation:'settings',value:{requestRetry:previousRetry}})).error,undefined);
  }
  let poolId,teamId,streamingKey,streamingLostKey;
  if(testEnv.HA_TEST_FULL_BACKEND==='1'&&process.env.HA_TEST_RESERVATIONS==='1'){
    const configure=async name=>{
      const created=await request(router,'/api/keys',operator.key,{method:'POST',body:JSON.stringify({name})}),key=await created.json();assert.equal(created.status,201,JSON.stringify(key));
      const budget=await request(router,'/api/usage/budget',operator.key,{method:'POST',body:JSON.stringify({apiKeyId:key.id,dailyLimitUsd:6.5,resetInterval:'daily'})});assert.equal(budget.status,200,await budget.text());return key;
    };
    const capture=await request(router,'/api/logs/detail',operator.key,{method:'POST',body:JSON.stringify({enabled:true})});const captureState=await capture.json();assert.equal(capture.status,200,JSON.stringify(captureState));assert.equal(captureState.enabled,true);
    assert.equal((await call(primary,{operation:'state'})).value.cachedSettings.call_log_pipeline_enabled,true,'The actual pipeline API must persist and invalidate source/compiled settings before Responses dispatch');
    responsesKey=await configure('Synthetic bounded Responses');responsesStreamKey=await configure('Synthetic bounded Responses stream');
    const body={model:'ha-local/synthetic-model',input:[{role:'user',content:'Synthetic Responses initial turn'}],max_output_tokens:3,stream:false};
    const response=await request(router,'/v1/responses',responsesKey.key,{method:'POST',body:JSON.stringify(body)}),result=await response.json();assert.equal(response.status,200,JSON.stringify(result));assert.equal(result.object,'response');assert.ok(JSON.stringify(result.output).includes('synthetic-response'));responsesId=result.id;boundedCalls++;
    assert.equal(typeof responsesId,'string');assert.ok(responsesId.length);assert.equal(providerRequests.at(-1).outputCap,3);assert.equal(providerRequests.at(-1).hasResponsesCap,false);
    const deadline=Date.now()+10000;while(!await test.db.collection('call_logs').findOne({response_id:responsesId,api_key_id:responsesKey.id,detail_state:'ready'})){assert.ok(Date.now()<deadline,'Responses continuation must be durably stored before failover');await delay(50);}
    const durable=await storedContinuationShape(responsesId);assert.ok(durable.some(row=>row.keyMatches&&(row.inputLength>0||row.effectiveLength>0)&&(row.outputLength>0||row.summaryLength>0)),'Continuation requires the original pipeline capture setting and a durable complete artifact: '+JSON.stringify(durable));
    const before=providerCalls,denied=await request(router,'/v1/responses',operator.key,{method:'POST',body:JSON.stringify({...body,previous_response_id:responsesId})});assert.ok([400,404].includes(denied.status),await denied.text());assert.equal(providerCalls,before,'Another key must not dispatch a stored conversation');
    const streamed=await request(router,'/v1/responses',responsesStreamKey.key,{method:'POST',headers:{Accept:'text/event-stream'},body:JSON.stringify({...body,stream:true})}),wire=await streamed.text();assert.equal(streamed.status,200,wire);assert.match(wire,/response.completed/);
    assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:responsesStreamKey.id,state:'settled'}),1);
    console.log('PASS actual bounded Responses HTTP: original request/response conversion, translated provider cap, key-scoped durable continuation, JSON/SSE receipts and committed settlement');
  }
  if (process.env.HA_TEST_RESERVATIONS === '1') {
    const configureStreamKey=async name=>{
      const created=await request(router,'/api/keys',operator.key,{method:'POST',body:JSON.stringify({name})}),key=await created.json();assert.equal(created.status,201,JSON.stringify(key));
      const tokens=await request(router,'/api/usage/token-limits',operator.key,{method:'POST',body:JSON.stringify({apiKeyId:key.id,scopeType:'global',scopeValue:'',tokenLimit:8,resetInterval:'monthly',resetTime:'00:00'})});assert.equal(tokens.status,200,await tokens.text());
      const budget=await request(router,'/api/usage/budget',operator.key,{method:'POST',body:JSON.stringify({apiKeyId:key.id,dailyLimitUsd:6.5,weeklyLimitUsd:6.5,monthlyLimitUsd:6.5,resetInterval:'daily',resetTime:'00:00'})});assert.equal(budget.status,200,await budget.text());return key;
    };
    streamingKey=await configureStreamKey('Synthetic hard streaming quota');
    if(process.env.HA_TEST_STREAMING==='1')streamingLostKey=await configureStreamKey('Synthetic interrupted streaming quota');
    const streamed=await request(router,chatPath,streamingKey.key,{method:'POST',body:JSON.stringify({...JSON.parse(boundedBody),stream:true})}),wire=await streamed.text();
    assert.equal(streamed.status,200,wire);assert.match(wire,/synthetic-stream-response/);assert.match(wire,/data: \[DONE\]/);
    assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:streamingKey.id,state:'settled'}),1);
    assert.equal(await test.db.collection('domain_cost_history').countDocuments({api_key_id:streamingKey.id}),1);
    assert.equal((await test.db.collection('request_cost_ledger').findOne({api_key_id:streamingKey.id})).amount_usd,3);
    response=await request(router,'/api/teams',operator.key,{method:'POST',body:JSON.stringify({name:'Synthetic HTTP Team',maxBudgetUsd:6.5,budgetDuration:'1d'})});
    body=await response.json(); assert.equal(response.status,201,JSON.stringify(body)); teamId=body.team.id;
    for(const key of [teamAlpha,teamBeta]) {
      response=await request(router,'/api/teams/'+teamId+'/members',operator.key,{method:'PUT',body:JSON.stringify({apiKeyId:key.id})});
      assert.equal(response.status,200,await response.text());
    }
    response=await request(router,chatPath,teamAlpha.key,{method:'POST',body:boundedBody});
    assert.equal(response.status,200,await response.text()); boundedCalls++;
    assert.equal((await test.db.collection('_ha_reservations').findOne({keyId:teamAlpha.id,state:'settled'})).teamId,teamId);
    response = await request(router,'/api/keys/'+keyUsage.id,operator.key,{method:'PATCH',body:JSON.stringify({usageLimitEnabled:true,dailyUsageLimitUsd:6.5,weeklyUsageLimitUsd:6.5,allowedConnections:[inferenceConnection.id]})});
    assert.equal(response.status,200,await response.text());
    response = await request(router,chatPath,keyUsage.key,{method:'POST',body:boundedBody});
    assert.equal(response.status,200,await response.text()); boundedCalls++;
    for (const [scopeType,scopeValue,resetInterval] of [['global','','monthly'],['provider',localNode.id,'weekly'],['model','synthetic-model','daily']]) {
      response = await request(router,'/api/usage/token-limits',operator.key,{method:'POST',body:JSON.stringify({apiKeyId:scoped.id,scopeType,scopeValue,tokenLimit:8,resetInterval,resetTime:'00:00'})});
      assert.equal(response.status,200,await response.text());
    }
    response = await request(router,chatPath,scoped.key,{method:'POST',body:boundedBody});
    assert.equal(response.status,200,await response.text()); boundedCalls++;
    response = await request(router, '/api/usage/budget', operator.key, { method: 'POST', body: JSON.stringify({ apiKeyId: calendar.id,
      dailyLimitUsd: 6.5, weeklyLimitUsd: 6.5, monthlyLimitUsd: 6.5, resetInterval: 'daily', resetTime: '00:00' }) });
    assert.equal(response.status, 200, await response.text());
    response = await request(router, chatPath, calendar.key, { method: 'POST', body: boundedBody });
    assert.equal(response.status, 200, await response.text()); boundedCalls++;
    response = await request(router, chatPath, bounded.key, { method: 'POST', body: boundedBody });
    assert.equal(response.status, 200, (await response.text()) + '; provider calls=' + providerCalls + '; intents=' + JSON.stringify(await test.db.collection('_ha_reservations').find({ keyId: bounded.id }, { projection: { state: 1 } }).toArray())); boundedCalls++;
    const reservation = await test.db.collection('_ha_reservations').findOne({ keyId: bounded.id });
    assert.equal(reservation.state, 'settled'); assert.equal(reservation.receipt.tokens, 3); assert.equal(reservation.receipt.nanoUsd, 3000000000);
    assert.equal(await test.db.collection('request_cost_ledger').countDocuments({ api_key_id: bounded.id }), 1);
    response = await request(router, '/api/quota/plans/' + inferenceConnection.id, operator.key, { method: 'PUT', body: JSON.stringify({ dimensions: [{ unit: 'tokens', window: 'daily', limit: 8 }, { unit: 'usd', window: 'daily', limit: 8 }] }) });
    assert.equal(response.status, 200, await response.text());
    response = await request(router, '/api/quota/pools', operator.key, { method: 'POST', body: JSON.stringify({ name: 'HTTP synthetic shared pool', connectionId: inferenceConnection.id,
      allocations: [poolAlpha, poolBeta].map(key => ({ apiKeyId: key.id, weight: 50, policy: 'hard' })) }) });
    body = await response.json(); assert.equal(response.status, 201, JSON.stringify(body)); poolId = body.pool.id;
    response = await request(router, chatPath, poolAlpha.key, { method: 'POST', body: boundedBody });
    assert.equal(response.status, 200, await response.text()); boundedCalls++;
    const poolIntent = await test.db.collection('_ha_reservations').findOne({ keyId: poolAlpha.id });
    assert.equal(poolIntent.state, 'settled'); assert.deepEqual(poolIntent.poolIds, [poolId]);
    assert.equal((await test.db.collection('quota_consumption').findOne({ api_key_id: poolAlpha.id, dimension_key: poolId + ':tokens:daily' })).consumed, 3);
    assert.equal((await test.db.collection('quota_consumption').findOne({api_key_id:poolAlpha.id,dimension_key:poolId+':usd:daily'}))._ha_usd_nano,3000000000);
  }
  response = await request(router,'/api/providers/'+inferenceConnection.id,operator.key,{method:'PUT',body:JSON.stringify({apiKey:'synthetic-provider-after-update'})});
  assert.equal(response.status,200,await response.text());
  expectedProviderToken = 'synthetic-provider-after-update';
  if(pairedProviders.kiro){
    const id=pairedProviders.kiro.id;
    const changed=await request(router,'/api/providers/'+id,operator.key,{method:'PUT',body:JSON.stringify({providerSpecificData:{authMethod:'builder-id',region:'us-east-1',clientId:'synthetic-http-old-client',clientSecret:'synthetic-http-old-secret'}})});assert.equal(changed.status,200,await changed.text());
    assert.equal((await call(primary,{operation:'loseKiroPublicationReply'})).value,true);
    const interrupted=await request(router,'/api/providers/'+id+'/refresh',operator.key,{method:'POST'});assert.equal(interrupted.status,500,await interrupted.text());
    const intent=await test.db.collection('_ha_credential_intents').findOne({connectionId:id,state:'dispatched'});
    assert.equal(intent.kiroPhase,'registration_published');kiroInterruptedId=intent._id;
    assert.equal(pairedOAuthCalls.kiro,4);assert.equal(kiroRegistrations,2);
    console.log('PASS actual interrupted Kiro publication HTTP: original manual refresh receives an injected lost DB acknowledgement after encrypted client commit; no token retry is dispatched');
  }
  if(testEnv.HA_TEST_FULL_BACKEND==='1'){
    const previousRetry=(await call(primary,{operation:'state'})).value.cachedSettings.requestRetry;
    assert.equal((await call(primary,{operation:'settings',value:{requestRetry:0}})).error,undefined);
    uncertainRecoveryId=await enqueueRecovery('ambiguous');recoveryMode='held';await delay(3500);
    assert.equal((await call(primary,{operation:'ageQuotaRecovery',ticketId:uncertainRecoveryId})).value.changes,1);
    pendingRecovery=call(primary,{operation:'quotaRecovery'}).then(value=>value,error=>({error:error.message}));
    const deadline=Date.now()+10000;while(!heldRecoveryResponse&&Date.now()<deadline)await delay(50);assert.ok(heldRecoveryResponse,'The original recovery dispatcher must reach the synthetic provider before failover');
    assert.equal((await test.db.collection('quota_recovery_tickets').findOne({id:uncertainRecoveryId})).state,'running');
    assert.equal((await call(primary,{operation:'settings',value:{requestRetry:previousRetry}})).error,undefined);
  }
  if(oauthProvider) {
    pendingImport=request(router,'/api/oauth/codex/import',operator.key,{method:'POST',body:JSON.stringify({accounts:[unknownImport]})}).then(async response=>({status:response.status,text:await response.text()}),error=>({error:error.name}));
    const importDeadline=Date.now()+8000;while(!heldImport&&Date.now()<importDeadline)await delay(50);assert.ok(heldImport);assert.equal(importCalls,3);
    holdOAuth=true;
    pendingOAuth=request(router,'/api/providers/'+oauthProvider.id+'/refresh',operator.key,{method:'POST'}).then(async response=>({status:response.status,text:await response.text()}),error=>({error:error.name}));
    const deadline=Date.now()+8000;while(!heldOAuthResponse && Date.now()<deadline) await delay(50);
    assert.ok(heldOAuthResponse);assert.equal(oauthCalls,3);
  }
  assert.equal((await call(primary,{operation:'provider',providerId:recoveryProvider.id,value:{testStatus:'unavailable',rateLimitedUntil:new Date(Date.now()-1000).toISOString(),lastErrorAt:new Date(Date.now()-120000).toISOString()}})).error,undefined);
  assert.equal((await call(primary,{operation:'ageMemory',memoryId:promotionDecayId})).value.changes,1);
  if(testEnv.HA_TEST_FULL_BACKEND==='1')assert.equal((await call(primary,{operation:'ageReasoning'})).value.changes,1);
  if(testEnv.HA_TEST_RESERVATIONS==='1'){
    // Alternate protocol-specific interruption profiles. Do not queue two
    // deliberately never-finishing calls on the same synthetic upstream.
    if(!streamingLostKey){
    pendingInference = request(router, chatPath, inflight.key, { method: 'POST', body: JSON.stringify({ ...JSON.parse(boundedBody), messages: [{ role: 'user', content: 'synthetic-held-provider-call' }] }) })
      .then(async response => ({ status: response.status, text: await response.text() }), error => ({ error: error.name }));
    const heldDeadline = Date.now() + 8000;
    while (!heldProviderResponse && Date.now() < heldDeadline) await delay(50);
    assert.ok(heldProviderResponse, 'The real provider must receive the in-flight request before pausing the active'); boundedCalls++;
    assert.equal((await test.db.collection('_ha_reservations').findOne({ keyId: inflight.id })).state, 'dispatched');
    }
  }
  if(streamingLostKey){
    holdNextStreaming=true;
    const stream=await request(router,chatPath,streamingLostKey.key,{method:'POST',body:JSON.stringify({...JSON.parse(boundedBody),stream:true})});assert.equal(stream.status,200,stream.status===200?'':'Streaming start: '+await stream.text()+'; source calls='+streamingCalls+'; held='+!!heldStreamingResponse+'; recent authorization='+JSON.stringify(providerRequests.slice(-3)));assert.ok(heldStreamingResponse);
    const reader=stream.body.getReader(),decoder=new TextDecoder();let prefix='';
    while(!prefix.includes('synthetic-stream-response')){const part=await reader.read();assert.equal(part.done,false);prefix+=decoder.decode(part.value,{stream:true});}
    pendingLostStream=(async()=>{let text=prefix;try{for(;;){const part=await reader.read();if(part.done)return{text};text+=decoder.decode(part.value,{stream:true});}}catch(error){return{error:error.name,text};}})();
    assert.equal((await test.db.collection('_ha_reservations').findOne({keyId:streamingLostKey.id})).state,'dispatched');
  }
  if(freeProxyId) {
    assert.equal((await call(primary,{operation:'ageFreeProxySync'})).value,'2000-01-01T00:00:00.000Z');
    holdFreeProxySync=true;
    pendingFreeProxySync=request(router,'/api/settings/free-proxies/sync',operator.key,{method:'POST',body:JSON.stringify({sources:['1proxy']})}).then(async response=>({status:response.status,text:await response.text()}),error=>({error:error.name}));
    const deadline=Date.now()+8000;while(!heldFreeProxySync&&Date.now()<deadline)await delay(25);assert.ok(heldFreeProxySync);assert.equal(freeProxySyncCalls,2);
    const busy=await request(router,'/api/settings/free-proxies/sync',operator.key,{method:'POST',body:JSON.stringify({sources:['1proxy']})});assert.equal(busy.status,409,await busy.text());assert.equal(freeProxySyncCalls,2);
  }
  const pendingMfa=testEnv.HA_TEST_FULL_BACKEND==='1'?await enrollMfa(router,operator.key,request):null;
  if(pendingMfa){const row=await test.db.collection('key_value').findOne({namespace:'dashboardMfa',key:'state'}),state=JSON.parse(row.value);assert.ok(state.secret.startsWith('enc:v1:'));assert.equal(row.value.includes(pendingMfa.secret),false);assert.equal(row.value.includes(pendingMfa.backupCodes[0]),false);}
  primary.kill('SIGSTOP');
  await waitReady(standby, true);
  await waitReady(router, true);
  if(pendingMfa)await verifyPromotedMfa(router,operator.key,pendingMfa,request);
  if(kiroInterruptedId){
    const id=pairedProviders.kiro.id,continued=await request(router,'/api/providers/'+id+'/refresh',operator.key,{method:'POST'});assert.equal(continued.status,200,await continued.text());
    assert.equal(pairedOAuthCalls.kiro,5);assert.equal(kiroRegistrations,2,'Promotion must use the already persisted client instead of repeating registration');
    const prior=await test.db.collection('_ha_credential_intents').findOne({_id:kiroInterruptedId});assert.equal(prior.state,'cancelled');assert.equal(prior.cancellationReason,'known-oidc-step-before-promotion');assert.ok(prior.recoveryEpoch>prior.epoch);
    pairedOAuthCiphertext.kiro=(await test.db.collection('provider_connections').findOne({id})).refresh_token;
    console.log('PASS actual Kiro safe-step promotion HTTP: standby reloads registered client, new epoch retires only the proven unissued step, one token call completes without re-registration or old-owner replay');
  }
  if(legacyHttpKey){const blocked=await request(router,chatPath,legacyHttpKey.key,{method:'POST',body:JSON.stringify({model:'ha-legacy/model',messages:[{role:'user',content:'Synthetic promoted historical pool'}],stream:false,max_tokens:3})});assert.equal(blocked.status,429,await blocked.text());assert.equal(legacyProviderCalls,1);console.log('PASS actual legacy pool promotion: reviewed history and native spend remain authoritative and no second provider request is sent');}
  if(responsesKey){
    const before=providerCalls,body={model:'ha-local/synthetic-model',input:[{role:'user',content:'Synthetic Responses continued turn'}],previous_response_id:responsesId,max_output_tokens:3,stream:false};
    const response=await request(router,'/v1/responses',responsesKey.key,{method:'POST',body:JSON.stringify(body)}),result=await response.json();assert.equal(response.status,200,JSON.stringify(result));assert.ok(JSON.stringify(result.output).includes('synthetic-response'));assert.equal(providerCalls,before+1);assert.ok(providerRequests.at(-1).messageCount>=3,'Promoted instance must load the stored conversation');boundedCalls++;
    const blocked=await request(router,'/v1/responses',responsesKey.key,{method:'POST',body:JSON.stringify(body)});assert.equal(blocked.status,429,await blocked.text());assert.equal(providerCalls,before+1);
    assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:responsesKey.id,state:'settled'}),2);
    console.log('PASS actual Responses promotion: shared tenant-scoped previous_response_id restores history, exactly one new provider call settles, hard USD exhaustion blocks the next call before dispatch');
  }
  if(uncertainRecoveryId){
    const count=recoveryProviderCalls,response=await request(router,'/api/v1/quota-recoveries/'+uncertainRecoveryId,recoveryKey.key),body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.recovery.state,'needs_confirmation');
    assert.equal((await call(standby,{operation:'quotaRecovery'})).error,undefined);assert.equal(recoveryProviderCalls,count,'Promotion must not repeat the old running ticket');releaseRecovery();
    console.log('PASS actual quota recovery failover: promotion changes the old running ticket to confirmation-required before readiness and the new owner does not repeat its unknown provider effect');
  }
  if(streamingKey){
    assert.deepEqual((await test.db.collection('domain_cost_history').find({api_key_id:streamingKey.id},{projection:{_id:0,cost:1}}).toArray()).map(row=>row.cost),[3],'Stream finalizers must not add another budget charge outside reservation context');
    const streamed=await request(router,chatPath,streamingKey.key,{method:'POST',body:JSON.stringify({...JSON.parse(boundedBody),stream:true})}),wire=await streamed.text();assert.equal(streamed.status,200,wire);assert.match(wire,/data: \[DONE\]/);
    assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:streamingKey.id,state:'settled'}),2);
    assert.equal(await test.db.collection('domain_cost_history').countDocuments({api_key_id:streamingKey.id}),2);
    const count=streamingCalls,blocked=await request(router,chatPath,streamingKey.key,{method:'POST',body:JSON.stringify({...JSON.parse(boundedBody),stream:true})});assert.equal(blocked.status,429,await blocked.text());assert.equal(streamingCalls,count);
    if(streamingLostKey){
    const uncertain=await request(router,chatPath,streamingLostKey.key,{method:'POST',body:JSON.stringify({...JSON.parse(boundedBody),stream:true})});assert.equal(uncertain.status,429,await uncertain.text());assert.equal(streamingCalls,count);
    releaseHeldStream();const abandoned=await pendingLostStream;assert.equal(abandoned.text.includes('data: [DONE]'),false);
    assert.equal((await test.db.collection('_ha_reservations').findOne({keyId:streamingLostKey.id})).state,'dispatched');
    console.log('PASS actual streaming interruption: no ingress replay/completion, promoted owner retains the dispatched token/USD liability and refuses another provider call');
    }
    console.log('PASS actual reserved streaming HTTP: token/USD admission, exact single settlement before DONE on both active epochs and hard exhaustion before another provider fetch');
  }
  if(freeProxyId) {
    const deadline=Date.now()+8000;while((await call(standby,{operation:'state'})).value.freeProxySync.runs<1&&Date.now()<deadline)await delay(50);
    assert.equal((await call(standby,{operation:'state'})).value.freeProxySync.runs,1);assert.equal(freeProxySyncCalls,3);
    releaseFreeProxySync();const ambiguous=await pendingFreeProxySync;assert.notEqual(ambiguous.status,200);
    assert.equal(await test.db.collection('free_proxies').countDocuments({host:'93.184.216.34',port:8082}),0);
    console.log('PASS actual owned Free Proxy auto-sync HTTP: original source parser populates shared catalog, standby starts no timer, manual overlap returns 409 without fetch, promotion resumes overdue sync and late old-active data is absent');
  }
  if(dashboardCookie)await verifyDashboardHttp(router,dashboardCookie,'after promotion');
  if(dashboardCookie){await verifyReasoningHttp(router,'after promotion',1);assert.equal((await call(standby,{operation:'reasoningCleanupState'})).value.runs,1);}
  if(vectorMemoryId){await verifyVectorHttp(router,createdId);console.log('PASS Memory vector/hybrid HTTP after promotion: shared persisted vectors and model-bound queries survive the active-process failure');}
  if(testEnv.HA_TEST_FULL_BACKEND==='1')assert.equal((await call(standby,{operation:'state'})).value.usageRetention.started,true);
  assert.equal((await call(standby,{operation:'providerLimitsState'})).value.started,true);
  if(testEnv.HA_TEST_FULL_BACKEND==='1')assert.equal((await call(standby,{operation:'pricing',providerId:localNode.id,model:'synthetic-model'})).value.input,1000000,'Promotion must clear the standby pricing cache before serving');
  if(testEnv.HA_TEST_FULL_BACKEND==='1') {
    const response=await request(router,'/api/keys/'+pricingProbe.id+'/usage-limits'),body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.status.dailySpentUsd,3);
    console.log('PASS archived key USD HTTP after promotion: financial history remains shared without local raw rows or Redis');
  }
  const promotedDecayDeadline=Date.now()+10000;
  while(await test.db.collection('memories').countDocuments({id:promotionDecayId})&&Date.now()<promotedDecayDeadline)await delay(50);
  assert.equal((await call(standby,{operation:'state'})).value.memoryDecay.started,true);
  assert.equal(await test.db.collection('memories').countDocuments({id:promotionDecayId}),0);
  assert.equal((await request(router,'/api/memory/'+promotionDecayId)).status,404);
  console.log('PASS actual owned Memory decay: opt-in timer runs despite headless background flag, standby cannot sweep, promoted process resumes cleanup and actual Memory HTTP observes deletion');
  assert.equal((await call(standby,{operation:'state'})).value.recoveryStarted,true);
  response=await request(router,'/api/providers/'+recoveryProvider.id,operator.key); body=await response.json();
  assert.equal(response.status,200,JSON.stringify(body));
  assert.equal((await test.db.collection('provider_connections').findOne({id:recoveryProvider.id})).test_status,'active');
  console.log('PASS owned connection recovery: actual upstream selection/CAS, active-only timer, standby invocation denied and expired cooldown cleared before promoted readiness');
  if(oauthProvider) {
    const rejected=await request(router,'/api/providers/'+oauthProvider.id+'/refresh',operator.key,{method:'POST'});
    assert.equal(rejected.status,500,await rejected.text());assert.equal(oauthCalls,3,'Promotion must not replay an unresolved one-time-token exchange');
    assert.equal((await test.db.collection('_ha_credential_lanes').findOne({_id:'openference'})).state,'dispatched');
    releaseOAuth();
    console.log('PASS in-flight OAuth failover: unresolved rotation survives promotion, standby cannot repeat it and no timeout refunds the group intent');
    const repeat=await request(router,'/api/oauth/codex/import',operator.key,{method:'POST',body:JSON.stringify({accounts:[unknownImport]})}),body=await repeat.json();assert.equal(repeat.status,200);assert.equal(body.failed,1);assert.equal(body.imported,0);assert.equal(importCalls,3);
    assert.equal(await test.db.collection('provider_connections').countDocuments({email:unknownImport.email}),0);releaseImport();
    console.log('PASS in-flight Codex import HTTP failover: durable unknown receipt without provider row; promoted application refuses exchange replay');
  }
  if (pendingInference) {
    const ambiguous = await pendingInference;
    assert.equal(ambiguous.status, 503, 'The ingress must fail an ambiguous request without replaying it');
    const before = providerCalls;
    response = await request(router, chatPath, inflight.key, { method: 'POST', body: boundedBody });
    assert.equal(response.status, 429, await response.text()); assert.equal(providerCalls, before);
    assert.equal((await test.db.collection('_ha_reservations').findOne({ keyId: inflight.id })).state, 'dispatched');
    releaseHeldProvider(); // A late real response cannot authorize the old epoch to settle.
  }
  if(pendingOAuth) {const result=await pendingOAuth;assert.ok(result.status===503 || result.error==='TimeoutError');}
  if(pendingImport) {const result=await pendingImport;assert.ok(result.status===503||result.error==='TimeoutError');}
  response = await request(router, '/api/settings'); body = await response.json();
  assert.equal(response.status, 200, JSON.stringify(body));
  assert.equal(body.stickyRoundRobinLimit, 19);
  if (dashboardCookie) {
    response = await dashboardRequest(router,'/api/settings',dashboardCookie);
    assert.equal(response.status,200,await response.text());
    response = await dashboardRequest(router,'/api/auth/csrf',revokedDashboardCookie);
    body = await response.json(); assert.equal(response.status,200,JSON.stringify(body)); assert.equal(typeof body.token,'string');
    response = await dashboardRequest(router,'/api/auth/logout',revokedDashboardCookie,{method:'POST',headers:{'x-omniroute-csrf':body.token},body:'{}'});
    assert.equal(response.status,200,await response.text());
    response = await dashboardRequest(router,'/api/settings',revokedDashboardCookie);
    assert.equal(response.status,401,await response.text());
    response = await dashboardRequest(router,'/api/settings',dashboardCookie);
    assert.equal(response.status,200,await response.text());
  }
  assert.equal((await call(standby, { operation: 'state' })).value.cachedSettings.stickyRoundRobinLimit, 19);
  assert.equal((await call(standby, { operation: 'state' })).value.cachedProviders.find(p => p.id === provider.id).apiKey, 'synthetic-token-after-promotion');
  assert.equal((await call(standby,{operation:'resolveProxy',providerId:provider.id})).value.proxy.port,49102);
  console.log('PASS real proxy cache promotion: stale standby resolution reloads current configuration before traffic, with Redis disabled and no proxy connection');
  response = await request(router, '/api/providers/' + provider.id); body = await response.json();
  assert.equal(response.status, 200, JSON.stringify(body)); assert.equal(body.connection.name, 'HTTP updated provider');
  assert.equal((await request(standby, '/api/keys/' + createdId)).status, 200);
  assert.equal((await request(standby, '/api/settings', revoked.key)).status, 403);
  assert.equal((await call(standby,{operation:'state'})).value.quotaRefresh.started,true);
  if(redisRevokedKey)assert.equal((await request(standby,'/api/settings',redisRevokedKey.key)).status,403,'Redis-outage deletion must remain revoked after real app promotion');
  if (testEnv.HA_TEST_FULL_BACKEND === '1') {
    response = await request(router, artifactEndpoint); body = await response.json();
    assert.equal(response.status, 200, JSON.stringify(body));
    assert.deepEqual(body.responseBody, artifactBody.responseBody, 'Request artifact must remain readable from the promoted host');
  }
  response = await request(router, '/api/memory/' + memoryId); body = await response.json();
  assert.equal(response.status, 200, JSON.stringify(body));
  assert.equal(body.memory.content, 'Persistent memory before failover');
  if(freeProxyId) {
    response=await request(router,'/api/settings/free-proxies',operator.key);body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.data.proxies.find(row=>row.id===freeProxyId).poolProxyId,promotedFreeProxyId);
    freeProxyLastSync=body.data.stats.lastSyncAt;
    response=await request(router,'/api/settings/proxies?id='+promotedFreeProxyId,operator.key,{method:'DELETE'});body=await response.json();assert.equal(response.status,200,JSON.stringify(body));
    response=await request(router,'/api/settings/free-proxies',operator.key);body=await response.json();assert.equal(response.status,200,JSON.stringify(body));const cleared=body.data.proxies.find(row=>row.id===freeProxyId);assert.equal(cleared.inPool,false);assert.equal(cleared.poolProxyId,null);
    console.log('PASS promoted Free Proxy HTTP: shared catalog survives failover; original registry DELETE atomically clears in-pool references');
  }
  response = await request(router, '/api/memory/' + memoryId, operator.key, { method: 'PUT', body: JSON.stringify({ content: 'Updated after failover' }) });
  assert.equal(response.status, 200, await response.text());
  assert.equal((await test.db.collection('memories').findOne({ id: memoryId })).content, 'Updated after failover');
  response = await request(router, chatPath, inference.key, { method: 'POST', body: chatBody });
  assert.equal(response.status, 200, await response.text());
  assert.equal(providerCalls, 2 + boundedCalls);
  const rpmState = await test.db.collection('_ha_request_limits').findOne({ _id: inference.id });
  assert.equal(rpmState.windows.find(row=>row.window===31536000).count,2,'The long-window request cap must survive promotion even when RPM history expires');
  assert.ok(rpmState.rpm.reduce((sum,row)=>sum+row.count,0)>=1,'Current RPM admission must also be recorded');
  assert.equal(await test.db.collection('domain_cost_history').countDocuments({ api_key_id: inference.id }), 2, 'Spend from both active epochs must be durable');
  response = await request(router, chatPath, inference.key, { method: 'POST', body: chatBody });
  assert.equal(response.status, 429, await response.text());
  assert.equal(providerCalls, 2 + boundedCalls, 'Hard request cap must block before provider dispatch after promotion');
  await verifyGamification(router,2);
  if (process.env.HA_TEST_RESERVATIONS === '1') {
    response=await request(router,chatPath,teamBeta.key,{method:'POST',body:boundedBody});
    assert.equal(response.status,200,await response.text()); boundedCalls++;
    const teamCalls=providerCalls;
    response=await request(router,chatPath,teamAlpha.key,{method:'POST',body:boundedBody});
    assert.equal(response.status,429,await response.text()); assert.equal(providerCalls,teamCalls);
    response=await request(router,'/api/teams/'+teamId,operator.key);
    body=await response.json(); assert.equal(response.status,200,JSON.stringify(body));
    assert.equal(body.members.length,2); assert.equal(body.budgetStatus.estimatedListCostUsd,6);
    console.log('PASS real hard Team HTTP: official Team/member APIs, original usage status, cross-key atomic capacity and pre-provider rejection after promotion');
    response = await request(router, chatPath, bounded.key, { method: 'POST', body: boundedBody });
    assert.equal(response.status, 200, await response.text()); boundedCalls++;
    assert.equal(await test.db.collection('_ha_reservations').countDocuments({ keyId: bounded.id, state: 'settled' }), 2);
    assert.equal(await test.db.collection('domain_cost_history').countDocuments({ api_key_id: bounded.id }), 2);
    const before = providerCalls;
    response = await request(router, chatPath, bounded.key, { method: 'POST', body: boundedBody });
    assert.equal(response.status, 429, await response.text()); assert.equal(providerCalls, before);
    response = await request(router, chatPath, poolBeta.key, { method: 'POST', body: boundedBody });
    assert.equal(response.status, 200, await response.text()); boundedCalls++;
    const poolCalls = providerCalls;
    response = await request(router, chatPath, poolAlpha.key, { method: 'POST', body: boundedBody });
    assert.equal(response.status, 429, await response.text()); assert.equal(providerCalls, poolCalls, 'Shared capacity must block before provider dispatch across keys and promotion');
    const poolRows = await test.db.collection('quota_consumption').find({ dimension_key: poolId + ':tokens:daily' }).toArray();
    assert.equal(poolRows.reduce((sum, row) => sum + row.consumed, 0), 6, 'Original async consumption recorder must not double-charge native settlement');
    response = await request(router, '/api/quota/pools/' + poolId + '/usage');
    assert.equal(response.status, 200, await response.text());
    response = await request(router, '/api/usage/budget', operator.key, { method: 'POST', body: JSON.stringify({ apiKeyId: calendar.id,
      dailyLimitUsd: 6.5, weeklyLimitUsd: 6.5, monthlyLimitUsd: 6.5, resetInterval: 'weekly', resetTime: '00:00' }) });
    assert.equal(response.status, 200, await response.text());
    response = await request(router, chatPath, calendar.key, { method: 'POST', body: boundedBody });
    assert.equal(response.status, 200, await response.text()); boundedCalls++;
    const calendarCalls = providerCalls;
    response = await request(router, chatPath, calendar.key, { method: 'POST', body: boundedBody });
    assert.equal(response.status, 429, await response.text()); assert.equal(providerCalls, calendarCalls);
    response = await request(router, '/api/usage/budget?apiKeyId=' + calendar.id);
    body = await response.json(); assert.equal(response.status, 200, JSON.stringify(body));
    assert.equal(body.totalCostPeriod, 6); assert.equal(body.resetInterval, 'weekly');
    response = await request(router,chatPath,scoped.key,{method:'POST',body:boundedBody});
    assert.equal(response.status,200,await response.text()); boundedCalls++;
    const scopedCalls = providerCalls;
    response = await request(router,chatPath,scoped.key,{method:'POST',body:boundedBody});
    assert.equal(response.status,429,await response.text()); assert.equal(providerCalls,scopedCalls);
    response = await request(router,'/api/usage/token-limits?apiKeyId='+scoped.id);
    body = await response.json(); assert.equal(response.status,200,JSON.stringify(body));
    assert.equal(body.limits.length,3); assert.ok(body.limits.every(row=>row.tokensUsed===6),'Scoped counters must settle once per provider call, not double-record asynchronously');
    response = await request(router,chatPath,keyUsage.key,{method:'POST',body:boundedBody});
    assert.equal(response.status,200,await response.text()); boundedCalls++;
    const usageCalls=providerCalls;
    response = await request(router,chatPath,keyUsage.key,{method:'POST',body:boundedBody});
    assert.equal(response.status,429,await response.text()); assert.equal(providerCalls,usageCalls);
    response = await request(router,'/api/keys/'+keyUsage.id+'/usage-limits');
    body=await response.json(); assert.equal(response.status,200,JSON.stringify(body));
    assert.equal(body.status.dailySpentUsd,6); assert.equal(body.status.weeklySpentUsd,6);
    if (testEnv.OMNIROUTE_API_KEY_WEEKLY_WINDOW==='calendar') {
      const native=await test.db.collection('_ha_reservations').findOne({keyId:keyUsage.id,state:'settled'});
      const {calendarWeekWindowMs}=await import('../upstream/omniroute/open-sse/services/dailyQuotaReset.ts');
      const expected=calendarWeekWindowMs(testEnv.OMNIROUTE_API_KEY_WEEKLY_WINDOW_TIMEZONE||'UTC',native.createdAt).startMs;
      assert.equal(native.usageWindows.weekly,expected); assert.equal(Date.parse(body.status.weeklyWindowStartIso),expected);
      console.log('PASS real key calendar window: application status and MongoDB worker match the original calendar, including the default UTC set before worker startup');
    }
    console.log('PASS real key USD usage HTTP: official key PATCH/status, original pricing/windows, atomic daily/weekly reservation, promotion persistence and hard rejection before provider dispatch');
    console.log('PASS real chat quota reservations: certified single-attempt token/USD bounds, synchronous receipt/ledger settlement, promotion persistence and pre-provider rejection on exhaustion');
    console.log('PASS real calendar USD HTTP: official budget CRUD/summary, daily-to-weekly policy change retains spend, capacity and hard pre-provider rejection across promotion');
    console.log('PASS real scoped token HTTP: official global/provider/model daily/weekly/monthly policy APIs, one atomic charge per receipt, promotion persistence and hard rejection before provider dispatch');
    console.log('PASS real shared-pool HTTP: official plan/pool CRUD and usage API, cross-key token/USD admission, one settlement per provider call, shared exhaustion after promotion');
  }
  const retired = once(primary, 'exit');
  primary.kill('SIGCONT');
  assert.equal((await retired)[0], 75, 'Former active must retire its timers and connection');
  if(uncertainRecoveryId){await pendingRecovery;assert.equal((await test.db.collection('quota_recovery_tickets').findOne({id:uncertainRecoveryId})).state,'needs_confirmation','Late old-active completion must not publish the recovery result');}
  if(streamingLostKey)assert.equal(await test.db.collection('domain_cost_history').countDocuments({api_key_id:streamingLostKey.id}),0,'Late old-active stream receipt cannot settle');
  if(freeProxyId){
    assert.equal(await test.db.collection('free_proxies').countDocuments({host:'93.184.216.34',port:8082}),0,'Resumed former active must not publish its delayed source result');
    assert.equal(freeProxySyncCalls,3,'The old timer must not start another source request');
  }
  if(oauthProvider) assert.equal((await test.db.collection('provider_connections').findOne({id:oauthProvider.id})).refresh_token,knownOAuthCiphertext,'Late old-active OAuth response must not publish over known credentials');
  for(const provider of Object.keys(pairedProviders))assert.equal((await test.db.collection('provider_connections').findOne({id:pairedProviders[provider].id})).refresh_token,pairedOAuthCiphertext[provider]);
  if(importedProviderId) {
    assert.equal(await test.db.collection('provider_connections').countDocuments({email:unknownImport.email}),0);
    const imported=await request(router,'/api/providers/'+importedProviderId,operator.key);assert.equal(imported.status,200,await imported.text());assert.equal(importCalls,3);
    console.log('PASS old-active import fencing: late response cannot attach tokens; completed imported provider remains readable after promotion');
  }
  if (pendingInference) {
    assert.equal(await test.db.collection('domain_cost_history').countDocuments({ api_key_id: inflight.id }), 0);
    assert.equal((await test.db.collection('_ha_reservations').findOne({ keyId: inflight.id })).state, 'dispatched');
    console.log('PASS real in-flight failure: provider received request once, ingress never replayed, promoted owner retained liability, late response could not settle through the stale epoch');
  }
  assert.equal((await call(standby, { operation: 'state' })).value.phase, 'ready');
  assert.equal((await call(standby, { operation: 'state' })).value.maintenanceStarted, true);
  assert.equal((await call(standby, { operation: 'state' })).value.fault, undefined);
  if (testEnv.HA_TEST_FULL_BACKEND === '1') {
    const exited = once(standby, 'exit'); standby.kill('SIGTERM'); await exited;
    const service = await spawn(true); await waitReady(service, true);
    response = await request(service, '/api/settings'); body = await response.json();
    assert.equal(response.status, 200, JSON.stringify(body)); assert.equal(body.stickyRoundRobinLimit, 19);
    assert.equal((await request(service, '/api/settings', revoked.key)).status, 403);
    await verifyGamification(service,2);
    if (dashboardCookie) {
      assert.equal((await dashboardRequest(service,'/api/settings',dashboardCookie)).status,200);
      assert.equal((await dashboardRequest(service,'/api/settings',revokedDashboardCookie)).status,401);
    }
    response = await request(service, '/api/memory/' + memoryId); body = await response.json();
    assert.equal(response.status, 200, JSON.stringify(body)); assert.equal(body.memory.content, 'Updated after failover');
    if (process.env.HA_TEST_RESERVATIONS === '1') {
      for (const key of [poolAlpha, ...(pendingInference ? [inflight] : []), calendar, scoped, keyUsage, teamAlpha, teamBeta]) {
        response = await request(service, chatPath, key.key, { method: 'POST', body: boundedBody });
        assert.equal(response.status, 429, await response.text());
      }
    }
    if(streamingLostKey){
      const before=streamingCalls;
      response=await request(service,chatPath,streamingLostKey.key,{method:'POST',body:JSON.stringify({...JSON.parse(boundedBody),stream:true})});
      assert.equal(response.status,429,await response.text());assert.equal(streamingCalls,before);
      assert.equal((await test.db.collection('_ha_reservations').findOne({keyId:streamingLostKey.id})).state,'dispatched');
    }
    console.log('PASS staging service entry: adapter/serve.mjs acquires the next epoch, serves actual APIs and retains state/revocation after application restart');
    if (process.env.HA_TEST_MONGO_ELECTION === '1') {
      const secondService = await spawn(true); await waitReady(secondService, false);
      const before = await test.client.db('admin').command({ replSetGetStatus: 1 });
      assert.equal(before.set, fixtureSet);
      assert.equal(before.members.length, 3);
      assert.equal(before.members.filter(m => m.state === 1 && m.health === 1).length, 1);
      assert.equal(before.members.filter(m => m.state === 2 && m.health === 1).length, 2);
      const oldPrimary = before.members.find(m => m.state === 1).name;
      // An opt-in fault on the known disposable set only. Never force a
      // stepdown, reconfigure the set, stop containers or reinitialize data.
      await test.client.db('admin').command({ replSetStepDown: 15, secondaryCatchUpPeriodSecs: 5, force: false });
      let newPrimary;
      const electionDeadline = Date.now() + 30000;
      while (Date.now() < electionDeadline) {
        try {
          const status = await test.client.db('admin').command({ replSetGetStatus: 1 });
          const elected = status.members.find(m => m.state === 1 && m.health === 1);
          if (elected && elected.name !== oldPrimary && status.members.filter(m => m.state === 2 && m.health === 1).length === 2) { newPrimary = elected.name; break; }
        } catch { /* Reconcile leadership after the topology transition; no write replay. */ }
        await delay(200);
      }
      assert.ok(newPrimary, 'A different MongoDB PRIMARY must be elected and all three members recover');
      let live = [service, secondService], selected;
      const recoveryDeadline = Date.now() + 45000;
      while (Date.now() < recoveryDeadline) {
        for (let i = 0; i < live.length; i++) {
          if (live[i].exitCode === 75) live[i] = await spawn(true); // Same restart behavior as the staging supervisor.
          else if (live[i].exitCode !== null || live[i].signalCode) throw new Error('Unexpected application exit during MongoDB election');
        }
        const readiness = await Promise.all(live.map(async child => {
          try { const probe = await fetch(child.base + '/readyz', { signal: AbortSignal.timeout(1500) }); await probe.arrayBuffer(); return probe.status === 200; }
          catch { return false; }
        }));
        assert.ok(readiness.filter(Boolean).length <= 1, 'Conflicting ready application instances after database election');
        if (readiness.filter(Boolean).length === 1) { selected = live[readiness.indexOf(true)]; break; }
        await delay(200);
      }
      assert.ok(selected, 'An application must automatically recover active service after MongoDB election');
      response = await request(selected, '/api/memory/' + memoryId); body = await response.json();
      assert.equal(response.status, 200, JSON.stringify(body)); assert.equal(body.memory.content, 'Updated after failover');
      assert.equal((await request(selected, '/api/settings', revoked.key)).status, 403);
      response = await request(selected, artifactEndpoint); body = await response.json();
      assert.equal(response.status, 200, JSON.stringify(body)); assert.deepEqual(body.responseBody, artifactBody.responseBody);
      response = await request(selected, '/api/memory/' + memoryId, operator.key, { method: 'PUT', body: JSON.stringify({ content: 'Updated after database election' }) });
      assert.equal(response.status, 200, await response.text());
      assert.equal((await test.db.collection('memories').findOne({ id: memoryId })).content, 'Updated after database election');
      console.log(`PASS real MongoDB primary election: ${oldPrimary} -> ${newPrimary}; two service entries recover exclusive active traffic, acknowledged state/artifacts/revocation survive, new writes commit`);
    }
  }
  assert.equal(readdirSync(dataDir, { recursive: true }).some(name => /\.(?:sqlite|db)(?:-wal|-shm)?$/.test(name)), false);
  assert.equal(readdirSync(dataDir, { recursive: true }).some(name => /(?:^|\/)call_logs\/.*\.json$/.test(name)), false, 'Request artifacts must not persist to host-local JSON');
  if (process.env.HA_TEST_DATA_RECOVERY === '1') {
    assert.equal(testEnv.HA_TEST_FULL_BACKEND, '1', 'Recovery test requires the actual backend');
    for (const child of children) {
      if (child.exitCode !== null || child.signalCode) continue;
      child.kill('SIGCONT'); const exited = once(child, 'exit'); child.kill('SIGTERM'); await exited;
    }
    // No lease rewriting or early release: the offline backup must wait for
    // MongoDB server time to expire the last process's ownership naturally.
    const deadline = Date.now() + 20000;
    while (true) {
      const row = await test.db.collection('_ha_serial').aggregate([{ $match: { _id: 'writer' } }, { $project: { expired: { $lte: ['$leaseUntil', '$$NOW'] } } }]).next();
      if (row.expired) break;
      assert.ok(Date.now() < deadline, 'Application lease failed to expire before offline backup');
      await delay(250);
    }
    mkdirSync('.work/test-artifacts', { recursive: true });
    const archive = path.join(mkdtempSync(path.resolve('.work/test-artifacts/http-recovery-')), 'snapshot.bson');
    if(legacyHttpEvidence)writeFileSync(path.join(path.dirname(archive),'legacy-pool-evidence.json'),JSON.stringify(legacyHttpEvidence),{flag:'wx',mode:0o600});
    let teamBaselineEvidence;
    if(teamId){
      const quotaCli=(...args)=>promisify(execFile)(process.execPath,['scripts/quota.mjs',...args],{env:{...process.env,HA_MONGO_URI:test.uri},timeout:60000,maxBuffer:1048576});
      const draft=JSON.parse((await quotaCli('team-inspect',test.database,teamId,'--offline')).stdout);
      assert.equal(draft.summary.nativeSettledNanoUsd,6000000000);
      teamBaselineEvidence={...draft,usdHistoryVerified:true,verifiedBy:'synthetic-Team-auditor',reference:'synthetic-historical-invoice',totalNanoUsd:7000000000};
      const file=path.join(path.dirname(archive),'team-baseline.json');writeFileSync(file,JSON.stringify(teamBaselineEvidence),{flag:'wx',mode:0o600});
      assert.equal(JSON.parse((await quotaCli('team-apply',test.database,file,'--offline')).stdout).baselined,true);
    }
    await backupDatabase(test.client, test.database, archive);
    recoveredDatabase = 'ha_test_http_restore_' + randomUUID().replaceAll('-', '');
    const restoredCutover=await restoreDatabase(test.client, archive, recoveredDatabase,{quiescentSource:test.database});
    assert.equal(restoredCutover.activated,true);assert.equal(restoredCutover.sourceRetired,test.database);
    assert.equal(typeof(await test.db.collection('_ha_serial').findOne({_id:'writer'})).ddl,'string','The old authority must stay retired after verified cutover');
    assert.equal(await test.client.db(recoveredDatabase).collection('_ha_usage_history').countDocuments({api_key_id:pricingProbe.id}),testEnv.HA_TEST_FULL_BACKEND==='1'?retentionExpiredRows+1:0);
    serviceDatabase = recoveredDatabase;
    // All old instances are stopped. Test an explicitly reviewed new policy on
    // the recovered instance; existing held liabilities remain unchanged.
    if(process.env.HA_TEST_RESERVATIONS==='1'){
      const bounds=JSON.parse(testEnv.HA_QUOTA_BOUNDS);bounds['openai-compatible-ha-test/synthetic-model'].defaultOutputTokens=3;testEnv.HA_QUOTA_BOUNDS=JSON.stringify(bounds);
    }
    const recovered = await spawn(true); await waitReady(recovered, true);
    if(completedRecoveryId){
      const count=recoveryProviderCalls,completed=await request(recovered,'/api/v1/quota-recoveries/'+completedRecoveryId,recoveryKey.key),result=await completed.json();assert.equal(completed.status,200,JSON.stringify(result));assert.equal(result.choices[0].message.content,'synthetic-recovered-response');
      const unknown=await request(recovered,'/api/v1/quota-recoveries/'+uncertainRecoveryId,recoveryKey.key),body=await unknown.json();assert.equal(unknown.status,200,JSON.stringify(body));assert.equal(body.recovery.state,'needs_confirmation');assert.equal(recoveryProviderCalls,count);
      console.log('PASS actual quota recovery archive: encrypted completed result and confirmation-required unknown turn survive restart/backup/restore and the recovered service returns them without another provider call');
    }
    if(vectorMemoryId){await verifyVectorHttp(recovered,createdId);console.log('PASS Memory vector/hybrid HTTP after restore: actual recovered service reads persistent vector/text state and uses the synthetic embedding endpoint');}
    response=await request(recovered,'/api/keys/'+pricingProbe.id+'/usage-limits');body=await response.json();
    assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.status.dailySpentUsd,3);
    console.log('PASS archived financial recovery: both history collections survive offline backup/restore and actual key USD HTTP preserves spend in the recovered application');
    response = await request(recovered, '/api/settings'); body = await response.json();
    assert.equal(response.status, 200, JSON.stringify(body)); assert.equal(body.stickyRoundRobinLimit, 19);
    assert.equal((await request(recovered, '/api/settings', revoked.key)).status, 403);
    if(redisRevokedKey)assert.equal((await request(recovered,'/api/settings',redisRevokedKey.key)).status,403,'Redis-outage deletion must remain revoked after backup restore');
    await verifyGamification(recovered,2);
    console.log('PASS actual Gamification HTTP: chat awards, catalog/key/operator badges, XP/streak/leaderboard/rank/anomalies and preserved state across promotion, service restart and backup restore');
    if (dashboardCookie) {
      await verifyDashboardHttp(recovered,dashboardCookie,'after backup restore');
      await verifyReasoningHttp(recovered,'after backup restore',1);
      if(process.env.HA_TEST_BROWSER_BIN)await verifyBrowserHttp(recovered.base,'synthetic-dashboard-password','after backup restore');
      assert.equal((await dashboardRequest(recovered,'/api/settings',dashboardCookie)).status,200);
      assert.equal((await dashboardRequest(recovered,'/api/settings',revokedDashboardCookie)).status,401);
      console.log('PASS actual dashboard auth: password login/failure, untrusted-origin mutation denied, shared cookie across promotion/restart/restore, CSRF-token logout and durable session revocation without revoking another session');
    }
    response = await request(recovered, '/api/memory/' + memoryId); body = await response.json();
    assert.equal(response.status, 200, JSON.stringify(body));
    assert.match(body.memory.content, /^Updated after (?:failover|database election)$/);
    if(freeProxyId) {
      response=await request(recovered,'/api/settings/free-proxies',operator.key);body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.data.proxies.find(row=>row.id===freeProxyId).inPool,false);assert.equal(body.data.stats.lastSyncAt,freeProxyLastSync);assert.deepEqual(body.data.syncErrors.proxifly,['synthetic source offline']);
      response=await request(recovered,'/api/settings/free-proxies/'+freeProxyId+'/add-to-pool',operator.key,{method:'POST'});body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.ok(body.poolProxyId);assert.equal(freeProxyConnects,2);
      console.log('PASS recovered Free Proxy HTTP: catalog/sync errors/cleanup metadata survive backup restore and original connectivity/promotion works again');
    }
    if (process.env.HA_TEST_RESERVATIONS === '1') {
      for (const key of [poolAlpha, ...(pendingInference ? [inflight] : []), calendar, scoped, keyUsage, teamAlpha, teamBeta]) {
        const before = providerCalls;
        response = await request(recovered, chatPath, key.key, { method: 'POST', body: boundedBody });
        assert.equal(response.status, 429, await response.text()); assert.equal(providerCalls, before);
      }
      assert.equal((await test.client.db(recoveredDatabase).collection('_ha_pool_profiles').findOne({ _id: poolId })).allWindows, true);
      const restoredMoney=await test.client.db(recoveredDatabase).collection('quota_consumption').find({dimension_key:poolId+':usd:daily'}).toArray();
      assert.equal(restoredMoney.reduce((sum,row)=>sum+row._ha_usd_nano,0),6000000000);
      assert.equal((await test.client.db(recoveredDatabase).collection('_ha_team_profiles').findOne({_id:teamId})).complete,true);
      if(teamBaselineEvidence){
        const restored=test.client.db(recoveredDatabase),profile=await restored.collection('_ha_team_profiles').findOne({_id:teamId});
        assert.equal(profile.legacyNanoUsd,1000000000);assert.equal(profile.baselineId,teamBaselineEvidence.id);
        assert.equal(await restored.collection('_ha_quota_baseline_audits').countDocuments({_id:teamBaselineEvidence.id,teamId}),1);
        const updated=await request(recovered,'/api/teams/'+teamId,operator.key,{method:'PATCH',body:JSON.stringify({maxBudgetUsd:10.2})});assert.equal(updated.status,200,await updated.text());
        const before=providerCalls,accepted=await request(recovered,chatPath,teamAlpha.key,{method:'POST',body:boundedBody});assert.equal(accepted.status,200,await accepted.text());assert.equal(providerCalls,before+1);
        const blocked=await request(recovered,chatPath,teamBeta.key,{method:'POST',body:boundedBody});assert.equal(blocked.status,429,await blocked.text());assert.equal(providerCalls,before+1);
        console.log('PASS actual reviewed Team cutover/recovery HTTP: offline CLI audit and historical debit survive backup/restore, real Team PATCH and proxy admission charge history plus native receipts without double counting; next member rejects before dispatch');
      }
      assert.equal((await test.client.db(recoveredDatabase).collection('_ha_reservations').findOne({ keyId: streamingLostKey?.id ?? inflight.id })).state, 'dispatched');
      console.log('PASS quota backup recovery: all-window pool baseline, spent shared capacity and unresolved in-flight liability survive restore and block new dispatch');
    }
    if(oauthProvider) {
      assert.equal((await test.client.db(recoveredDatabase).collection('_ha_credential_lanes').findOne({_id:'openference'})).state,'dispatched');
      assert.equal((await test.client.db(recoveredDatabase).collection('provider_connections').findOne({id:oauthProvider.id})).refresh_token,knownOAuthCiphertext);
      const before=oauthCalls;response=await request(recovered,'/api/providers/'+oauthProvider.id+'/refresh',operator.key,{method:'POST'});
      assert.equal(response.status,500,await response.text());assert.equal(oauthCalls,before);
      console.log('PASS OAuth backup recovery: encrypted known tokens and unresolved rotation group persist; restored application refuses another exchange');
      for(const provider of Object.keys(pairedProviders)) {
        const database=test.client.db(recoveredDatabase);
        assert.equal((await database.collection('provider_connections').findOne({id:pairedProviders[provider].id})).refresh_token,pairedOAuthCiphertext[provider]);
        const group=provider==='codex'?'openai-auth0':provider==='cline'?'cline':provider==='claude'?'anthropic-oauth':provider==='amazon-q'?'kiro':provider==='github'?'github-app':provider;assert.equal((await database.collection('_ha_credential_lanes').findOne({_id:group})).state,provider==='codex'?'dispatched':'verification_required');
        if(provider!=='codex')assert.equal((await request(recovered,'/api/providers/'+pairedProviders[provider].id+'/refresh',operator.key,{method:'POST'})).status,500);
      }
      assert.deepEqual(pairedOAuthCalls,{'claude':2,'codex':1,'cline':2,'kimi-coding':2,'gitlab-duo':2,'kiro':5,'amazon-q':2,'github':2});assert.equal(kiroRegistrations,2);
      assert.equal(importCalls,3);assert.equal(await test.client.db(recoveredDatabase).collection('provider_connections').countDocuments({email:unknownImport.email}),0);
      console.log('PASS Claude/Codex/Cline/Kimi/GitLab/Kiro/Amazon Q/GitHub archive safety: encrypted pairs/client metadata survive promotion/restore, copied groups require verification and no exchange is repeated');
    }
    response = await request(recovered, artifactEndpoint); body = await response.json();
    assert.equal(response.status, 200, JSON.stringify(body)); assert.deepEqual(body.responseBody, artifactBody.responseBody);
    response = await request(recovered, '/api/providers/' + provider.id); body = await response.json();
    assert.equal(response.status, 200, JSON.stringify(body));
    response = await request(recovered, '/api/memory/' + memoryId, operator.key, { method: 'PUT', body: JSON.stringify({ content: 'Updated after restore' }) });
    assert.equal(response.status, 200, await response.text());
    assert.equal((await test.client.db(recoveredDatabase).collection('memories').findOne({ id: memoryId })).content, 'Updated after restore');
    response=await request(recovered,'/api/keys',operator.key,{method:'POST',body:JSON.stringify({name:'Synthetic streaming validation'})});
    const streamKey=await response.json();assert.equal(response.status,201,JSON.stringify(streamKey));assert.equal(typeof streamKey.key,'string');
    const beforeStreaming=streamingCalls;
    response=await request(recovered,chatPath,streamKey.key,{method:'POST',body:JSON.stringify({...JSON.parse(chatBody),stream:true,stream_options:{include_usage:true},max_tokens:3})});
    const streamText=await response.text();assert.equal(response.status,200,streamText);assert.match(response.headers.get('content-type'),/text\/event-stream/);
    assert.match(streamText,/synthetic-stream-response/);assert.match(streamText,/data: \[DONE\]/);assert.equal(streamingCalls,beforeStreaming+1);
    const streamDb=test.client.db(recoveredDatabase),streamDeadline=Date.now()+8000;
    while(!await streamDb.collection('domain_cost_history').countDocuments({api_key_id:streamKey.id})&&Date.now()<streamDeadline)await delay(50);
    assert.equal(await streamDb.collection('domain_cost_history').countDocuments({api_key_id:streamKey.id}),1,'Streaming completion must retain the real cost writer');
    assert.equal((await streamDb.collection('domain_cost_history').findOne({api_key_id:streamKey.id})).cost,3);
    assert.equal((await request(recovered,'/readyz')).status,200);
    if(streamingKey){
      const restored=test.client.db(recoveredDatabase);assert.equal(await restored.collection('_ha_reservations').countDocuments({keyId:streamingKey.id,state:'settled'}),2);
      if(streamingLostKey){
      assert.equal((await restored.collection('_ha_reservations').findOne({keyId:streamingLostKey.id})).state,'dispatched');
      assert.equal(await restored.collection('domain_cost_history').countDocuments({api_key_id:streamingLostKey.id}),0);
      const blocked=await request(recovered,chatPath,streamingLostKey.key,{method:'POST',body:JSON.stringify({...JSON.parse(boundedBody),stream:true})});assert.equal(blocked.status,429,await blocked.text());assert.equal(streamingCalls,beforeStreaming+1);
      console.log('PASS streaming quota recovery: settled receipts and unresolved dispatched liability survive actual backup/restore, late old-active response cannot charge/refund, recovered retry rejects before provider');
      }
    }
    if(process.env.HA_TEST_EXTENDED_QUOTA==='1'){
      const newKey=async name=>{
        const created=await request(recovered,'/api/keys',operator.key,{method:'POST',body:JSON.stringify({name})}),key=await created.json();assert.equal(created.status,201,JSON.stringify(key));
        const limit=await request(recovered,'/api/usage/budget',operator.key,{method:'POST',body:JSON.stringify({apiKeyId:key.id,dailyLimitUsd:6.5,resetInterval:'daily'})});assert.equal(limit.status,200,await limit.text());return key;
      };
      const tools=[{type:'function',function:{name:'lookup',parameters:{type:'object',properties:{}}}}];
      const toolKey=await newKey('Synthetic certified tools HTTP');
      for(const stream of [false,true]){
        const response=await request(recovered,chatPath,toolKey.key,{method:'POST',body:JSON.stringify({...JSON.parse(boundedBody),stream,tools})}),wire=await response.text();
        assert.equal(response.status,200,wire);assert.match(wire,/tool_calls/);if(stream)assert.match(wire,/data: \[DONE\]/);
      }
      assert.equal(await streamDb.collection('_ha_reservations').countDocuments({keyId:toolKey.id,state:'settled'}),2);
      assert.equal(await streamDb.collection('domain_cost_history').countDocuments({api_key_id:toolKey.id}),2);
      const mediaKey=await newKey('Synthetic certified media HTTP');
      const wav=Buffer.alloc(46);wav.write('RIFF');wav.writeUInt32LE(38,4);wav.write('WAVEfmt ',8);wav.writeUInt32LE(16,16);wav.writeUInt16LE(1,20);wav.writeUInt16LE(1,22);wav.writeUInt32LE(8000,24);wav.writeUInt32LE(16000,28);wav.writeUInt16LE(2,32);wav.writeUInt16LE(16,34);wav.write('data',36);wav.writeUInt32LE(2,40);
      for(const part of [{type:'image_url',image_url:{url:'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/lxoAAAAASUVORK5CYII='}},{type:'input_audio',input_audio:{format:'wav',data:wav.toString('base64')}}]){
        const response=await request(recovered,chatPath,mediaKey.key,{method:'POST',body:JSON.stringify({...JSON.parse(boundedBody),model:'openai-compatible-ha-test/synthetic-model',messages:[{role:'user',content:[{type:'text',text:'Synthetic media'},part]}]})}),wire=await response.text();assert.equal(response.status,200,wire);
      }
      assert.equal(await streamDb.collection('_ha_reservations').countDocuments({keyId:mediaKey.id,state:'settled'}),2);
      assert.equal(await streamDb.collection('domain_cost_history').countDocuments({api_key_id:mediaKey.id}),2);
      const before=providerCalls;
      const denied=await request(recovered,chatPath,mediaKey.key,{method:'POST',body:boundedBody});assert.equal(denied.status,429,await denied.text());assert.equal(providerCalls,before);
      console.log('PASS actual certified tools/media HTTP: original JSON and SSE function calls, image/audio input, complete provider cost receipts, one settlement per call and shared hard USD exhaustion after recovery');
    }
    console.log('PASS actual restored streaming HTTP: real provider SSE chunks/usage/DONE traverse the original pipeline, original cost writer persists one priced usage row and readiness remains healthy (unmanaged quota profile)');
    if(process.env.HA_TEST_RESERVATIONS==='1'){
      const created=await request(recovered,'/api/keys',operator.key,{method:'POST',body:JSON.stringify({name:'Synthetic operator-default cap'})}),key=await created.json();assert.equal(created.status,201,JSON.stringify(key));
      const budget=await request(recovered,'/api/usage/budget',operator.key,{method:'POST',body:JSON.stringify({apiKeyId:key.id,dailyLimitUsd:6.5,resetInterval:'daily'})});assert.equal(budget.status,200,await budget.text());
      const before=providerCalls;
      for(const cap of [null,2]){
        const requestBody={...JSON.parse(chatBody),...(cap==null?{}:{max_tokens:cap})};if(cap==null)delete requestBody.stream;
        const reply=await request(recovered,chatPath,key.key,{method:'POST',body:JSON.stringify(requestBody)});assert.equal(reply.status,200,await reply.text());
        assert.equal(providerRequests.at(-1).stream,false,'Omitted stream must retain the original OpenAI JSON default');
        assert.equal(providerRequests.at(-1).outputCap,cap??3,'Real original executor must send the effective output cap to the provider');
      }
      assert.equal(providerCalls,before+2);assert.equal(await test.client.db(recoveredDatabase).collection('_ha_reservations').countDocuments({keyId:key.id,state:'settled'}),2);
      const rejected=await request(recovered,chatPath,key.key,{method:'POST',body:chatBody});assert.equal(rejected.status,429,await rejected.text());assert.equal(providerCalls,before+2);
      console.log('PASS real operator-default cap HTTP: omitted cap is forwarded to the actual provider, explicit client cap wins, durable settlement preserves hard USD exhaustion and the next call rejects before dispatch');
    }
    console.log('PASS actual application backup recovery: new database/epoch, startup, Settings/auth/revocation/Provider/Memory/artifact HTTP reads and majority-committed new write');
    if(legacyHttpKey){
      const plan=await request(recovered,'/api/quota/plans/'+legacyHttpConnection.id,operator.key,{method:'PUT',body:JSON.stringify({dimensions:[{unit:'usd',window:'weekly',limit:6.5}]})});assert.equal(plan.status,200,await plan.text());
      const blocked=await request(recovered,chatPath,legacyHttpKey.key,{method:'POST',body:JSON.stringify({model:'ha-legacy/model',messages:[{role:'user',content:'Synthetic restored historical pool'}],stream:false,max_tokens:3})});assert.equal(blocked.status,429,await blocked.text());assert.equal(legacyProviderCalls,1);
      assert.equal((await test.client.db(recoveredDatabase).collection('_ha_pool_profiles').findOne({_id:legacyHttpPoolId})).baselineId,legacyHttpEvidence.id);
      assert.ok(await test.client.db(recoveredDatabase).collection('_ha_quota_baseline_audits').findOne({_id:legacyHttpEvidence.id,kind:'pool'}));
      console.log('PASS actual legacy pool archive recovery: audit/profile/exact history survive restore; original plan PUT switches to a reviewed weekly USD window and exhausted capacity still refuses before provider');
    }
    if(responsesKey){
      const body={model:'ha-local/synthetic-model',input:[{role:'user',content:'Synthetic recovered Responses'}],max_output_tokens:3,stream:false};
      const before=providerCalls,blocked=await request(recovered,'/v1/responses',responsesKey.key,{method:'POST',body:JSON.stringify(body)});assert.equal(blocked.status,429,await blocked.text());assert.equal(providerCalls,before);
      const streamed=await request(recovered,'/v1/responses',responsesStreamKey.key,{method:'POST',headers:{Accept:'text/event-stream'},body:JSON.stringify({...body,stream:true})}),wire=await streamed.text();assert.equal(streamed.status,200,wire);assert.match(wire,/response.completed/);
      assert.equal(await test.client.db(recoveredDatabase).collection('_ha_reservations').countDocuments({keyId:responsesStreamKey.id,state:'settled'}),2);
      const calls=streamingCalls,denied=await request(recovered,'/v1/responses',responsesStreamKey.key,{method:'POST',body:JSON.stringify({...body,stream:true})});assert.equal(denied.status,429,await denied.text());assert.equal(streamingCalls,calls);
      console.log('PASS actual Responses archive recovery: prior JSON spend blocks before dispatch, SSE resumes on a new active epoch and exactly two durable receipts exhaust its budget without duplicate charges');
    }
    console.log('ARTIFACT synthetic HTTP backup retained: ' + archive);
  }
  console.log('PASS real Next.js HTTP routes: auth, key create/revoke, two-process promotion, cache reload, resumed-primary fencing');
  console.log('PASS actual Memory HTTP endpoints: create, list/token statistics, persistence across promotion and update on the promoted instance');
  console.log('PASS official chat/completions: real local provider response, API-key policy, hard request cap across promotion, rejected request never reaches provider');
  console.log('PASS Provider HTTP read/update: encrypted state survives promotion/restart/restore; real proxy uses the updated credential after promotion');
  console.log('PASS shared call artifacts: real HTTP detail read across promotion, no host-local JSON; active-only fenced maintenance removes expired/orphan data');
  console.log('PASS hard RPM with empty custom rules; missing bounds or unsupported bounded-request shape returns 503 before contacting the provider');
  if (testEnv.HA_TEST_FULL_BACKEND === '1') console.log('PASS official compiled backend: real instrumentation, native Next routing, authz proxy and public /v1 rewrite; no fixture dispatch');
  console.log('PASS official headless startup on both active epochs, independent local data directories, resumed former active retires');
  console.log('PASS automatic traffic failover through one stable loopback router address (single-host test, not a production load balancer)');
} catch (error) {
  failure = error;
  throw error;
} finally {
  await router?.close();
  for(const socket of freeProxyTunnels)socket.destroy();
  if (providerServer) { providerServer.closeAllConnections(); await new Promise(resolve => providerServer.close(resolve)); }
  for (const child of children) {
    if (child.exitCode !== null || child.signalCode) continue;
    child.kill('SIGCONT'); const exited = once(child, 'exit'); child.kill('SIGTERM');
    const timer = setTimeout(() => child.kill('SIGKILL'), 5000);
    await exited; clearTimeout(timer);
  }
  await bootstrap?.close(); delete globalThis.__omnirouteMongoAdapter;
  try {
    if (recoveredDatabase) await test.client.db(recoveredDatabase).dropDatabase(); // Fresh name generated exclusively by this test.
    if(provisionedDatabase)await test.client.db(provisionedDatabase).dropDatabase(); // Successful, owned CLI receipt only.
  } finally {
    try { await test.cleanup(); }
    catch (error) {
      if (!failure) throw error;
      console.error('Cleanup failed for this run only: ' + test.database + ': ' + error.message);
    } finally {try{await redisFixture?.close();}finally{rmSync(dataDir,{recursive:true,force:true});}}
  }
}
