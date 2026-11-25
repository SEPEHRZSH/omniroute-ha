import assert from 'node:assert/strict';
import { readFileSync, mkdirSync, readdirSync,writeFileSync,unlinkSync,chmodSync } from 'node:fs';
import path from 'node:path';
import { fork,execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { credentialVersion,credentialLaneVersion,rotationGroupFor,rotationGroupMembers } from '../adapter/credentialState.mjs';
import { createCredentialRuntime } from '../adapter/credentialRuntime.mjs';
import { createServer } from 'node:http';
import { AsyncResource } from 'node:async_hooks';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { testDatabase } from './mongoTestSupport.mjs';
import { provisionDomains } from './provisionDomains.mjs';

const test = await testDatabase('domains');
const dataDir = path.resolve('.work/domain-data');
mkdirSync(dataDir, { recursive: true });
Object.assign(process.env, {
  NODE_ENV: 'test', DATA_DIR: dataDir, DISABLE_SQLITE_AUTO_BACKUP: '1',
  OMNIROUTE_DISABLE_BACKGROUND_SERVICES: '1', OMNIROUTE_DISABLE_REDIS_AUTH_CACHE: '1',
  STORAGE_ENCRYPTION_KEY: 'synthetic-storage-key-for-ha-tests-only',
  API_KEY_SECRET: 'synthetic-api-key-secret-for-ha-tests-only',
  JWT_SECRET: 'synthetic-jwt-secret-for-ha-tests-only',
  REDIS_URL: '', QDRANT_HOST: '',
});
const db = new MongoAdapter({ uri: test.uri, database: test.database });
const unsupportedStatements = [];
db.onUnsupported = sql => unsupportedStatements.push(sql);
globalThis.__omnirouteMongoAdapter = db;
let peer, migrationOwner,credentialStandby;
try {
  await provisionDomains(db);
  db.installRequestLimits();
  const settings = await import('../upstream/omniroute/src/lib/db/settings.ts');
  const keys = await import('../upstream/omniroute/src/lib/db/apiKeys.ts');
  const { ensureDbReadyForBoot } = await import('../upstream/omniroute/src/instrumentation-node.ts');
  await ensureDbReadyForBoot();
  const { pingDb } = await import('../upstream/omniroute/src/lib/db/core.ts');
  assert.equal(pingDb(), true);
  peer = fork(new URL('./domainPeer.mjs', import.meta.url), [], {
    env: { ...process.env, HA_TEST_URI: test.uri, HA_TEST_DATABASE: test.database },
    stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
  });
  async function receive() {
    return Promise.race([
      once(peer, 'message').then(([value]) => value),
      new Promise((_, reject) => { const timer = setTimeout(() => reject(new Error('Peer response timeout')), 15000); timer.unref(); }),
    ]);
  }
  assert.equal((await receive()).ready, true);
  let requestId = 0;
  async function callPeer(request) {
    const response = receive();
    peer.send({ ...request, id: ++requestId });
    const message = await response;
    assert.equal(message.id, requestId); assert.equal(message.error, undefined);
    return message.value;
  }
  assert.equal(await settings.getSettingsRevision(), 0);
  await settings.updateSettings({ requireLogin: true, stickyRoundRobinLimit: 7 }, { expectedRevision: 0 });
  assert.equal((await settings.getSettings()).stickyRoundRobinLimit, 7);
  assert.equal((await callPeer({ operation: 'settings' })).stickyRoundRobinLimit, 7);
  assert.equal(await settings.getSettingsRevision(), 1);
  await assert.rejects(settings.updateSettings({ stickyRoundRobinLimit: 99 }, { expectedRevision: 0 }), { code: 'SETTINGS_REVISION_CONFLICT' });
  assert.equal((await settings.getSettings()).stickyRoundRobinLimit, 7);
  const key = await keys.createApiKey('Synthetic integration key', 'ha_test_machine');
  assert.equal(await keys.validateApiKey(key.key), true);
  assert.equal(await callPeer({ operation: 'validate', key: key.key }), true);
  const stored = await test.db.collection('api_keys').findOne({ id: key.id });
  assert.ok(stored.key.startsWith('enc:v1:'));
  assert.notEqual(stored.key, key.key);
  assert.equal(stored.key_hash.length, 64);
  assert.equal(await keys.revokeApiKey(key.id), true);
  assert.equal(await keys.validateApiKey(key.key), false);
  assert.equal(await callPeer({ operation: 'validate', key: key.key }), false);
  assert.equal(await keys.validateApiKey('synthetic-invalid-key'), false);
  const providers = await import('../upstream/omniroute/src/lib/db/providers.ts');
  const connection = await providers.createProviderConnection({ provider: 'openai', authType: 'apikey', name: 'Synthetic provider', apiKey: 'synthetic-provider-token-v1' });
  assert.equal((await providers.getProviderConnectionById(connection.id)).apiKey, 'synthetic-provider-token-v1');
  await providers.updateProviderConnection(connection.id, { apiKey: 'synthetic-provider-token-v2' });
  assert.equal((await providers.getProviderConnectionById(connection.id)).apiKey, 'synthetic-provider-token-v2');
  assert.ok((await test.db.collection('provider_connections').findOne({ id: connection.id })).api_key.startsWith('enc:v1:'));
  await Promise.all([
    providers.updateProviderConnection(connection.id, { apiKey: 'synthetic-provider-token-v3' }),
    providers.updateProviderConnection(connection.id, { name: 'Concurrent name update' }),
  ]);
  const concurrentProvider = await providers.getProviderConnectionById(connection.id);
  assert.equal(concurrentProvider.apiKey, 'synthetic-provider-token-v3');
  assert.equal(concurrentProvider.name, 'Concurrent name update');
  db.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/078_quota_consumption.sql', 'utf8'));
  const quota = await import('../upstream/omniroute/src/lib/db/quotaConsumption.ts');
  quota.incrementBucket(key.id, 'synthetic:tokens:hourly', 7, 10, 1000);
  quota.incrementBucket(key.id, 'synthetic:tokens:hourly', 7, 15, 1001);
  assert.equal(quota.getBucket(key.id, 'synthetic:tokens:hourly', 7), 25);
  assert.deepEqual(quota.getPair(key.id, 'synthetic:tokens:hourly', 7), { curr: 25, prev: 0 });
  db.exec('CREATE TABLE ha_call_log_artifacts(path TEXT PRIMARY KEY,payload TEXT NOT NULL,mtime_ms INTEGER NOT NULL)');
  const artifacts = await import('../upstream/omniroute/src/lib/usage/callLogArtifacts.ts');
  const { writeCallArtifactAsync } = await import('../upstream/omniroute/src/lib/usage/callLogArtifactWriter.ts');
  const artifact = { schemaVersion: 5, summary: { id: 'synthetic-artifact', timestamp: '2026-01-01T00:00:00.000Z' }, requestBody: { messages: [{ role: 'user', content: 'Synthetic prompt' }] }, responseBody: { content: 'Synthetic response' }, error: null };
  const written = await writeCallArtifactAsync(artifact);
  assert.ok(written); assert.equal(written.sizeBytes, Buffer.byteLength(JSON.stringify(artifact)));
  assert.deepEqual(await callPeer({ operation: 'artifact', path: written.relPath }), { artifact, state: 'ready' });
  assert.equal(artifacts.readCallArtifact('absent').state, 'missing');
  assert.equal(artifacts.listCallLogArtifactFiles().length, 1);
  // Lazy legacy-secret migrations must not be marked complete by a standby
  // whose protected writes were rejected, or be lost on its later promotion.
  const plaintext = JSON.stringify({ refreshToken: 'synthetic-legacy-refresh' });
  db.prepare('UPDATE provider_connections SET provider_specific_data=? WHERE id=?').run(plaintext, connection.id);
  db.prepare('UPDATE api_keys SET key=?, key_hash=NULL WHERE id=?').run(key.key, key.id);
  migrationOwner = new MongoAdapter({ uri: test.uri, database: test.database, ownerId: 'synthetic-migration-owner' });
  globalThis.__omnirouteMongoAdapter = migrationOwner;
  const { ensureProviderSpecificDataSecretsEncrypted } = await import('../upstream/omniroute/src/lib/db/providers/migrations.ts');
  const { backfillApiKeySecretsAtRest } = await import('../upstream/omniroute/src/lib/db/apiKeys/secretAtRest.ts');
  assert.equal(migrationOwner.canWrite(), false);
  assert.throws(() => artifacts.deleteCallArtifact(written.relPath), { code: 'HA_FENCED' });
  assert.equal(ensureProviderSpecificDataSecretsEncrypted(migrationOwner), 0);
  assert.equal(backfillApiKeySecretsAtRest(migrationOwner), 0);
  assert.equal((await test.db.collection('provider_connections').findOne({ id: connection.id })).provider_specific_data, plaintext);
  assert.equal((await test.db.collection('api_keys').findOne({ id: key.id })).key, key.key);
  migrationOwner.ownership('claim', 30000);
  assert.equal(migrationOwner.canWrite(), true);
  assert.equal(artifacts.deleteCallArtifact(written.relPath), true);
  assert.equal((await callPeer({ operation: 'artifact', path: written.relPath })).state, 'missing');
  assert.equal(ensureProviderSpecificDataSecretsEncrypted(migrationOwner), 1);
  assert.equal(backfillApiKeySecretsAtRest(migrationOwner), 1);
  const securedProvider = JSON.parse((await test.db.collection('provider_connections').findOne({ id: connection.id })).provider_specific_data);
  assert.ok(securedProvider.refreshToken.startsWith('enc:v1:'));
  assert.ok((await test.db.collection('api_keys').findOne({ id: key.id })).key.startsWith('enc:v1:'));
  assert.equal(migrationOwner.compatibilityFault, undefined);
  assert.equal(readdirSync(dataDir, { recursive: true }).some(name => /\.(?:sqlite|db)(?:-wal|-shm)?$/.test(name)), false);
  assert.deepEqual(unsupportedStatements, [], 'No unsupported SQL may be hidden by upstream catches');
  const rotating=await providers.createProviderConnection({provider:'openference',authType:'oauth',name:'Synthetic rotating credentials',accessToken:'synthetic-access-before',refreshToken:'synthetic-refresh-before'});
  const sibling=await providers.createProviderConnection({provider:'openference',authType:'oauth',name:'Synthetic sibling credentials',accessToken:'synthetic-sibling-access',refreshToken:'synthetic-sibling-refresh'});
  const raw=async id=>test.db.collection('provider_connections').findOne({id});
  const credentialRuntime=createCredentialRuntime(migrationOwner,{assertOwner(){if(!migrationOwner.canWrite())throw Object.assign(new Error('Owner required'),{code:'HA_FENCED'});}});
  globalThis.__omnirouteCredentialRuntime=credentialRuntime;
  const selected=[];
  const listingSelector=async provider=>{selected.push(provider);return{provider,originalSelector:true};};
  const oldListing=await credentialRuntime.listingLookup(listingSelector);
  assert.equal(await oldListing('nvidia'),null);
  await providers.createProviderConnection({provider:'nvidia_nim',authType:'apikey',name:'Synthetic listing alias',apiKey:'synthetic-listing-key'});
  assert.equal(await oldListing('nvidia'),null,'Each listing retains its point-in-time metadata view');
  const {createProviderNode}=await import('../upstream/omniroute/src/lib/db/providers/nodes.ts');
  const listingNode=await createProviderNode({id:'openai-compatible-listing',type:'openai-compatible',name:'Synthetic listing node',prefix:'ha-listing',apiType:'chat',baseUrl:'https://synthetic.invalid/v1'});
  await providers.createProviderConnection({provider:listingNode.id,authType:'apikey',name:'Synthetic node credentials',apiKey:'synthetic-node-listing-key'});
  let listingBridgeCalls=0;const callBeforeListing=migrationOwner.bridge.call;
  migrationOwner.bridge.call=function(request){listingBridgeCalls++;return callBeforeListing.call(this,request);};
  try{
    const listing=await credentialRuntime.listingLookup(listingSelector);
    assert.deepEqual(await listing('nvidia'),{provider:'nvidia',originalSelector:true});
    assert.deepEqual(await listing('ha-listing'),{provider:'ha-listing',originalSelector:true});
    for(const provider of ['gemini','jina-ai','jina-reader','jina-search'])assert.equal((await listing(provider)).originalSelector,true,'Original environment fallback must remain reachable');
    for(let index=0;index<250;index++)assert.equal(await listing('synthetic-unconfigured-'+index),null);
    assert.ok(listingBridgeCalls<=12,'Unconfigured pools must use the original alias resolver with a bulk node view, not remote DB lookups');
  }finally{migrationOwner.bridge.call=callBeforeListing;}
  assert.deepEqual(selected,['nvidia','ha-listing','gemini','jina-ai','jina-reader','jina-search']);
  console.log('PASS per-listing metadata view: actual encrypted Provider/node stores and alias resolver, configured custom prefixes and environment fallback retain original selection; 250 absent pools use at most 12 bridge operations, fresh listing sees new accounts');
  const passiveSetting=process.env.OMNIROUTE_TOKEN_REFRESH_PASSIVE_PROVIDERS;
  try {
    process.env.OMNIROUTE_TOKEN_REFRESH_PASSIVE_PROVIDERS='codex';
    assert.doesNotThrow(()=>credentialRuntime.validateCall('codex',{}),'Read-only passive token adoption must retain the original early-return contract');
    delete process.env.OMNIROUTE_TOKEN_REFRESH_PASSIVE_PROVIDERS;
    assert.throws(()=>credentialRuntime.validateCall('openai',{connectionId:'uncertified'}),{code:'HA_CREDENTIAL_PROFILE'});
  } finally {if(passiveSetting===undefined)delete process.env.OMNIROUTE_TOKEN_REFRESH_PASSIVE_PROVIDERS;else process.env.OMNIROUTE_TOKEN_REFRESH_PASSIVE_PROVIDERS=passiveSetting;}
  const {OAUTH_ENDPOINTS,PROVIDERS}=await import('../upstream/omniroute/open-sse/config/constants.ts');
  const endpointKeys=['openference','anthropic','openai','github'];
  const originalEndpoints=Object.fromEntries(endpointKeys.map(key=>[key,OAUTH_ENDPOINTS[key].token]));
  const originalClineEndpoint=PROVIDERS.cline.refreshUrl;
  const originalKimiEndpoint=PROVIDERS['kimi-coding'].refreshUrl;
  assert.equal(rotationGroupFor('clinepass'),rotationGroupFor('cline'));
  assert.deepEqual(rotationGroupMembers('cline').sort(),['cline','clinepass']);
  let exchanges=0,callbacks=0;
  const pairedConnections=new Map();
  const oauthServer=createServer((request,response)=>{
    let form='';request.on('data',chunk=>{form+=chunk;});request.on('end',()=>{
      if(request.url==='/cline') {
        assert.equal(request.headers['content-type'],'application/json');
        const body=JSON.parse(form);assert.equal(body.grantType,'refresh_token');assert.equal(body.clientType,'extension');assert.ok(body.refreshToken.startsWith('synthetic-'));
      } else assert.equal(new URLSearchParams(form).get('grant_type'),'refresh_token');
      if(request.url==='/claude') {assert.equal(new URLSearchParams(form).get('client_id'),PROVIDERS.claude.clientId);assert.equal(request.headers['anthropic-beta'],'oauth-2025-04-20');}
      if(request.url==='/codex') {assert.equal(new URLSearchParams(form).get('client_id'),PROVIDERS.codex.clientId);assert.equal(new URLSearchParams(form).has('scope'),false);}
      if(request.url==='/kimi')assert.equal(request.headers['x-msh-device-id'],'synthetic-stable-device');
      if(request.url==='/gitlab/oauth/token')assert.equal(new URLSearchParams(form).get('client_id'),'synthetic-gitlab-client');
      exchanges++;response.setHeader('content-type','application/json');
      response.end(JSON.stringify(request.url==='/cline'?{data:{accessToken:'synthetic-http-access-'+exchanges,refreshToken:'synthetic-http-refresh-'+exchanges,expiresAt:new Date(Date.now()+3600000).toISOString()}}:{access_token:'synthetic-http-access-'+exchanges,refresh_token:'synthetic-http-refresh-'+exchanges,expires_in:3600,...(request.url==='/kimi'?{token_type:'Bearer',scope:'synthetic-scope'}:{})}));
    });
  });
  try {
    await new Promise(resolve=>oauthServer.listen(0,'127.0.0.1',resolve));
    // Test-only constant mutation in this process, not a source edit or a live
    // provider call. The actual original HTTP refresh helper is exercised.
    const base='http://127.0.0.1:'+oauthServer.address().port;
    OAUTH_ENDPOINTS.openference.token=base+'/openference';OAUTH_ENDPOINTS.anthropic.token=base+'/claude';OAUTH_ENDPOINTS.openai.token=base+'/codex';
    OAUTH_ENDPOINTS.github.token=base+'/github';
    PROVIDERS.cline.refreshUrl=base+'/cline';
    PROVIDERS['kimi-coding'].refreshUrl=base+'/kimi';
    const refresh=await import('../upstream/omniroute/open-sse/services/tokenRefresh.ts');
    const {getAccessToken}=refresh;
    for(const name of ['refreshOpenferenceToken','refreshClaudeOAuthToken','refreshCodexToken','refreshClineToken','refreshKimiCodingToken','refreshGitLabDuoToken','refreshKiroToken','refreshGitHubToken'])await assert.rejects(refresh[name]('synthetic-direct-bypass',null),{code:'HA_CREDENTIAL_POLICY'});
    assert.throws(()=>refresh.refreshAccessToken('codex','synthetic-direct-bypass',{},null),{code:'HA_CREDENTIAL_POLICY'});
    assert.equal(exchanges,0,'The public low-level export must not bypass durable dispatch');
    const persist=async result=>{
      callbacks++;
      assert.equal((await providers.getProviderConnectionById(rotating.id)).refreshToken,result.refreshToken,'Rotated token must be majority-persisted before the callback');
      await providers.updateProviderConnection(rotating.id,{accessToken:result.accessToken,refreshToken:result.refreshToken,lastHealthCheckAt:new Date().toISOString()});
    };
    const results=await Promise.all([1,2].map(()=>getAccessToken('openference',{connectionId:rotating.id,refreshToken:'synthetic-stale-input'},null,null,persist)));
    assert.equal(exchanges,1);assert.equal(callbacks,1);assert.equal(results[0].refreshToken,'synthetic-http-refresh-1');assert.deepEqual(results[0],results[1]);
    const cached=await getAccessToken('openference',{connectionId:rotating.id,refreshToken:'synthetic-refresh-before'},null,null,persist);
    assert.equal(cached.refreshToken,'synthetic-http-refresh-1');assert.equal(exchanges,1);assert.equal(callbacks,2,'Cached rotation may update metadata without another exchange');
    assert.equal((await test.db.collection('_ha_credential_intents').findOne({connectionId:rotating.id})).state,'completed');
    for(const provider of ['claude','codex','cline','clinepass','kimi-coding','gitlab-duo','github']) {
      assert.equal(credentialRuntime.supportsProactive(provider),true);
      const providerSpecificData=provider==='gitlab-duo'?{baseUrl:base+'/gitlab',clientId:'synthetic-gitlab-client'}:provider==='kimi-coding'?{deviceId:'synthetic-stable-device',deviceName:'synthetic-device',deviceModel:'synthetic-model',osVersion:'synthetic-os'}:{};
      const connection=await providers.createProviderConnection({provider,authType:'oauth',name:'Synthetic '+provider+' OAuth',accessToken:'synthetic-'+provider+'-before',refreshToken:'synthetic-'+provider+'-seed',expiresAt:new Date(Date.now()-60000).toISOString(),providerSpecificData});
      pairedConnections.set(provider,connection.id);
      let persistCount=0;const before=exchanges;
      const persist=async result=>{
        persistCount++;assert.equal((await providers.getProviderConnectionById(connection.id)).refreshToken,result.refreshToken);
        await providers.updateProviderConnection(connection.id,{accessToken:result.accessToken,refreshToken:result.refreshToken});
      };
      if(['kimi-coding','gitlab-duo'].includes(provider)){
        await assert.rejects(getAccessToken(provider,{connectionId:connection.id,refreshToken:'synthetic-'+provider+'-seed',providerSpecificData:{...providerSpecificData,unexpected:'stale-device-or-instance'}},null,null,persist),{code:'HA_CREDENTIAL_STALE'});
        assert.equal(exchanges,before);assert.equal(await test.db.collection('_ha_credential_intents').countDocuments({connectionId:connection.id}),0,'Stale metadata must reject before durable dispatch');
      }
      const results=await Promise.all([1,2].map(()=>getAccessToken(provider,{connectionId:connection.id,refreshToken:'synthetic-'+provider+'-seed',providerSpecificData},null,null,persist)));
      assert.equal(exchanges,before+1);assert.equal(persistCount,1);assert.deepEqual(results[0],results[1]);
      assert.equal((await test.db.collection('_ha_credential_intents').findOne({connectionId:connection.id})).state,'completed');
      assert.ok((await raw(connection.id)).refresh_token.startsWith('enc:v1:'));
    }
    const {GithubExecutor}=await import('../upstream/omniroute/open-sse/executors/github.ts');
    const executor=new GithubExecutor();
    // Force the original fallback after a failed, read-only Copilot sub-token
    // request. Its App token POST still uses the real loopback HTTP endpoint.
    executor.refreshCopilotToken=async()=>null;
    await assert.rejects(executor.refreshGitHubToken('synthetic-direct-executor-bypass',null),{code:'HA_CREDENTIAL_POLICY'});
    const githubId=pairedConnections.get('github'),github=await providers.getProviderConnectionById(githubId),beforeExecutor=exchanges;
    const executorResults=await Promise.all([1,2].map(()=>executor.refreshCredentials({...github,connectionId:githubId},null)));
    assert.equal(exchanges,beforeExecutor+1,'Concurrent executor fallbacks must share one guarded App exchange');
    assert.deepEqual(executorResults[0],executorResults[1]);
    assert.equal((await providers.getProviderConnectionById(githubId)).refreshToken,executorResults[0].refreshToken,'The rotated App pair must survive even when the subsequent Copilot request fails');
    assert.equal(await test.db.collection('_ha_credential_intents').countDocuments({connectionId:githubId,state:'completed'}),2);
    console.log('PASS original GitHub executor fallback: durable single-use App rotation, concurrent deduplication and early encrypted publication despite failed Copilot sub-token; raw bypass refused');
    const classic=await providers.createProviderConnection({provider:'github',authType:'oauth',name:'Synthetic access-only Copilot',accessToken:'synthetic-classic-copilot-pat'});
    assert.equal((await raw(classic.id)).refresh_token,null);
    assert.doesNotThrow(()=>credentialRuntime.validateCall('github',{accessToken:'synthetic-classic-access'}));
    console.log('PASS actual Claude/Codex/Cline/ClinePass/Kimi/GitLab/GitHub OAuth source HTTP: original helpers, direct-export guards, device/instance/form/JSON contracts, concurrent dedup and encrypted publication; classic access-only GitHub does not require a rotating grant');
    const aliasId=pairedConnections.get('clinepass'),aliasSnapshot=await raw(aliasId),beforeHealth=exchanges;
    migrationOwner.prepare('UPDATE provider_connections SET expires_at=NULL,token_expires_at=NULL WHERE id=?').run(aliasId);
    const {refreshCredentials}=await import('../adapter/activeRuntime.mjs');
    const health=await refreshCredentials({db:migrationOwner,assertOwner(){assert.ok(migrationOwner.canWrite());}});
    assert.ok(health.skipped>=2,'Unknown-expiry canonical and alias rotating grants must skip proactive exchange');
    assert.equal(exchanges,beforeHealth);assert.equal((await raw(aliasId)).refresh_token,aliasSnapshot.refresh_token);
    migrationOwner.prepare('UPDATE provider_connections SET expires_at=?,token_expires_at=? WHERE id=?').run(aliasSnapshot.expires_at,aliasSnapshot.token_expires_at,aliasId);
    console.log('PASS original owned token health: ClinePass aliases without known expiry retain their one-time grant; canonical/sibling future-expiry checks perform no extra exchange');
    const administrator=new AsyncResource('synthetic-independent-admin');
    try {
      const {encrypt}=await import('../upstream/omniroute/src/lib/db/encryption.ts');
      await assert.rejects(getAccessToken('openference',{connectionId:rotating.id,refreshToken:'synthetic-http-refresh-1'},null,null,async result=>{
        // An independent HTTP/admin request has its own async context while
        // sharing the same currently fenced application's SQL connection.
        administrator.runInAsyncScope(()=>migrationOwner.prepare('UPDATE provider_connections SET access_token=?,refresh_token=? WHERE id=?').run(encrypt('synthetic-manual-newest'),encrypt('synthetic-manual-refresh-newest'),rotating.id));
        await providers.updateProviderConnection(rotating.id,{accessToken:result.accessToken,refreshToken:result.refreshToken});
      }),{code:'HA_CREDENTIAL_STALE'});
      assert.equal((await providers.getProviderConnectionById(rotating.id)).accessToken,'synthetic-manual-newest');
    } finally {administrator.emitDestroy();migrationOwner.credentialScope=undefined;}
    console.log('PASS actual OAuth source HTTP: one exchange for concurrent callers, encrypted durable tokens before onPersist and rejection of a late callback after a newer administrative credential write');
  } finally {for(const key of endpointKeys)OAUTH_ENDPOINTS[key].token=originalEndpoints[key];PROVIDERS.cline.refreshUrl=originalClineEndpoint;PROVIDERS['kimi-coding'].refreshUrl=originalKimiEndpoint;oauthServer.closeAllConnections();await new Promise(resolve=>oauthServer.close(resolve));delete globalThis.__omnirouteCredentialRuntime;}
  const beginRefresh=async(actor,id,provider='openference')=>{
    const intent=randomUUID(),request={id:intent,connectionId:id,provider,version:credentialVersion(await raw(id))};
    for(;;){const result=actor.credential('dispatch',request);if(result.dispatched)return intent;await new Promise(resolve=>setTimeout(resolve,Math.max(1,result.waitUntil-actor.serverTime())));}
  };
  const beforeNativeRefresh=(await providers.getProviderConnectionById(rotating.id)).accessToken;
  const proxySnapshot=await raw(rotating.id);
  migrationOwner.prepare('UPDATE provider_connections SET proxy_enabled=? WHERE id=?').run(proxySnapshot.proxy_enabled===0?1:0,rotating.id);
  assert.throws(()=>migrationOwner.credential('dispatch',{id:randomUUID(),connectionId:rotating.id,provider:'openference',version:credentialVersion(proxySnapshot)}),{code:'HA_CREDENTIAL_STALE'});
  migrationOwner.prepare('UPDATE provider_connections SET proxy_enabled=? WHERE id=?').run(proxySnapshot.proxy_enabled,rotating.id);
  const firstRefresh=await beginRefresh(migrationOwner,rotating.id);
  await assert.rejects(beginRefresh(migrationOwner,sibling.id),{code:'HA_CREDENTIAL_UNCERTAIN'});
  await assert.rejects(providers.updateProviderConnection(rotating.id,{name:'Must not change during unresolved rotation'}),{code:'HA_CREDENTIAL_UNCERTAIN'});
  assert.throws(()=>migrationOwner.prepare('DELETE FROM provider_connections WHERE id=?').run(rotating.id),{code:'HA_CREDENTIAL_UNCERTAIN'});
  assert.throws(()=>migrationOwner.credential('publish',{id:firstRefresh,fields:{access_token:'plaintext',refresh_token:'plaintext'}}),{code:'HA_CREDENTIAL_POLICY'});
  const {encrypt}=await import('../upstream/omniroute/src/lib/db/encryption.ts');
  const renewed={access_token:encrypt('synthetic-access-after'),refresh_token:encrypt('synthetic-refresh-after')};
  const committed=[],previousCommit=migrationOwner.onCommitted;
  migrationOwner.onCommitted=tables=>{committed.push(tables);return previousCommit?.(tables);};
  try{
    migrationOwner.begin();migrationOwner.credential('publish',{id:firstRefresh,fields:renewed});
    assert.deepEqual(committed,[],'Uncommitted credentials must not publish cache hints');migrationOwner.rollback();
    assert.equal((await providers.getProviderConnectionById(rotating.id)).accessToken,beforeNativeRefresh);
    assert.equal((await test.db.collection('_ha_credential_intents').findOne({_id:firstRefresh})).state,'dispatched');
    assert.deepEqual(committed,[],'Rollback must not announce published credentials');
    migrationOwner.begin();migrationOwner.credential('publish',{id:firstRefresh,fields:renewed});migrationOwner.commit();
    assert.deepEqual(committed,[['provider_connections']],'Native credential publication must notify only after majority commit');
  }finally{migrationOwner.onCommitted=previousCommit;}
  console.log('PASS native credential cache notification: private publication participates in the real transaction, rollback emits no hint and committed provider changes notify before later callbacks');
  assert.equal((await providers.getProviderConnectionById(rotating.id)).refreshToken,'synthetic-refresh-after');
  migrationOwner.credentialScope=()=>firstRefresh;
  await providers.updateProviderConnection(rotating.id,{lastHealthCheckAt:new Date().toISOString()});
  migrationOwner.credentialScope=undefined;
  await providers.updateProviderConnection(rotating.id,{accessToken:'synthetic-manual-newer',refreshToken:'synthetic-manual-newer-refresh'});
  migrationOwner.credentialScope=()=>firstRefresh;
  await assert.rejects(providers.updateProviderConnection(rotating.id,{accessToken:'synthetic-access-after',refreshToken:'synthetic-refresh-after'}),{code:'HA_CREDENTIAL_STALE'});
  migrationOwner.credentialScope=undefined;
  assert.equal((await providers.getProviderConnectionById(rotating.id)).accessToken,'synthetic-manual-newer');
  credentialStandby=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-credential-standby'});
  migrationOwner.ownership('release');credentialStandby.ownership('claim',30000);
  const unknownRefresh=await beginRefresh(credentialStandby,sibling.id);
  assert.throws(()=>migrationOwner.credential('publish',{id:firstRefresh,fields:renewed}),{code:'HA_FENCED'});
  credentialStandby.ownership('release');migrationOwner.ownership('claim',30000);
  await assert.rejects(beginRefresh(migrationOwner,rotating.id),{code:'HA_CREDENTIAL_UNCERTAIN'},'Promotion cannot refund an unknown one-time-token exchange');
  assert.equal((await test.db.collection('_ha_credential_intents').findOne({_id:unknownRefresh})).state,'dispatched');
  // Deliberately corrupt only this owned fixture's lane pointer: the native
  // unique partial index must independently stop a second dispatched intent.
  const savedLane=await test.db.collection('_ha_credential_lanes').findOne({_id:'openference'});
  await test.db.collection('_ha_credential_lanes').deleteOne({_id:'openference'});
  await assert.rejects(beginRefresh(migrationOwner,rotating.id),{code:'SQLITE_CONSTRAINT_UNIQUE'});
  await test.db.collection('_ha_credential_lanes').insertOne(savedLane);
  const evidenceFile=path.join(dataDir,'synthetic-credential-recovery-'+randomUUID()+'.json');
  const cli=(...args)=>promisify(execFile)(process.execPath,['scripts/credentials.mjs',...args],{env:{...process.env,HA_MONGO_URI:test.uri},maxBuffer:32000,timeout:20000});
  try {
    const status=JSON.parse((await cli('status',test.database,'openference')).stdout);
    assert.equal(status.state,'dispatched');assert.equal(status.connections.length,2);
    assert.equal(status.laneVersion,credentialLaneVersion(savedLane));
    assert.equal(JSON.stringify(status).includes('synthetic-refresh'),false);
    const evidence={id:randomUUID(),provider:'openference',laneVersion:status.laneVersion,previousGrantsRevoked:true,
      verifiedBy:'synthetic-test-operator',providerReference:'synthetic-out-of-band-grant-revocation',verifiedAt:new Date().toISOString(),
      connections:status.connections.map(row=>({...row,accessToken:'synthetic-reauth-access-'+row.id,refreshToken:'synthetic-reauth-refresh-'+row.id,expiresAt:new Date(Date.now()+3600000).toISOString()}))};
    writeFileSync(evidenceFile,JSON.stringify(evidence),{mode:0o600,flag:'wx'});
    chmodSync(evidenceFile,0o640);
    await assert.rejects(cli('recover',test.database,evidenceFile,'--offline'),error=>error.stderr.includes('HA_CREDENTIAL_PRIVATE_EVIDENCE_REQUIRED'));
    chmodSync(evidenceFile,0o600);
    await assert.rejects(cli('recover',test.database,evidenceFile,'--offline'),error=>error.stderr.includes('HA_CREDENTIAL_STOP_APPLICATIONS'));
    assert.throws(()=>migrationOwner.credential('recover',{id:evidence.id,provider:'openference'}),{code:'HA_CREDENTIAL_POLICY'},'An ordinary active adapter cannot invoke administrative credential recovery');
    migrationOwner.ownership('release');
    const originalCiphertext=(await raw(rotating.id)).refresh_token;
    writeFileSync(evidenceFile,JSON.stringify({...evidence,connections:evidence.connections.slice(0,1)}));
    await assert.rejects(cli('recover',test.database,evidenceFile,'--offline'),error=>error.stderr.includes('HA_CREDENTIAL_POLICY'));
    assert.equal((await raw(rotating.id)).refresh_token,originalCiphertext);
    assert.equal((await test.db.collection('_ha_credential_lanes').findOne({_id:'openference'})).state,'dispatched');
    // A new expired grant must be refused, but an already committed audit must
    // remain safely retryable after its credentials have subsequently expired.
    writeFileSync(evidenceFile,JSON.stringify({...evidence,connections:evidence.connections.map(row=>({...row,expiresAt:'2020-01-01T00:00:00.000Z'}))}));
    await assert.rejects(cli('recover',test.database,evidenceFile,'--offline'),error=>error.stderr.includes('HA_CREDENTIAL_POLICY'));
    assert.equal((await raw(rotating.id)).refresh_token,originalCiphertext);
    const expiresAt=new Date(Date.now()+5000).toISOString();
    evidence.connections=evidence.connections.map(row=>({...row,expiresAt}));
    writeFileSync(evidenceFile,JSON.stringify(evidence));
    const recovered=JSON.parse((await cli('recover',test.database,evidenceFile,'--offline')).stdout);
    assert.equal(recovered.recovered,true);assert.equal(recovered.connections,2);
    const restoredCiphertext=(await raw(rotating.id)).refresh_token;
    await new Promise(resolve=>setTimeout(resolve,Math.max(1,Date.parse(expiresAt)-Date.now()+25)));
    const repeated=JSON.parse((await cli('recover',test.database,evidenceFile,'--offline')).stdout);
    assert.equal(repeated.idempotent,true);assert.equal((await raw(rotating.id)).refresh_token,restoredCiphertext);
    assert.equal((await test.db.collection('_ha_credential_intents').findOne({_id:unknownRefresh})).state,'reconciled');
    const audit=await test.db.collection('_ha_credential_reconciliations').findOne({_id:evidence.id});
    assert.equal(audit.sha256,recovered.evidenceSha256);assert.equal(JSON.stringify(audit).includes('synthetic-reauth-refresh'),false);
    migrationOwner.ownership('claim',30000);
    assert.equal((await providers.getProviderConnectionById(rotating.id)).refreshToken,'synthetic-reauth-refresh-'+rotating.id);
    assert.throws(()=>migrationOwner.credential('publish',{id:unknownRefresh,fields:renewed}),{code:'HA_CREDENTIAL_STALE'});
    await beginRefresh(migrationOwner,rotating.id);
    console.log('PASS actual credential recovery CLI: active-owner refusal, complete-group evidence, atomic rejection, encrypted audited reauthentication, idempotent retry and stale-result fencing');
  } finally {try{unlinkSync(evidenceFile);}catch(error){if(error.code!=='ENOENT')throw error;}}
  const sharedOpenai=await providers.createProviderConnection({provider:'openai',authType:'oauth',name:'Synthetic shared Auth0 lane',accessToken:'synthetic-openai-before',refreshToken:'synthetic-openai-seed'});
  for(const provider of ['claude','codex','cline','kimi-coding','gitlab-duo','github']) {
    const id=pairedConnections.get(provider),unknown=await beginRefresh(migrationOwner,id,provider);
    if(provider==='codex')await assert.rejects(beginRefresh(migrationOwner,sharedOpenai.id,'openai'),{code:'HA_CREDENTIAL_UNCERTAIN'});
    if(provider==='cline')await assert.rejects(beginRefresh(migrationOwner,pairedConnections.get('clinepass'),'clinepass'),{code:'HA_CREDENTIAL_UNCERTAIN'});
    migrationOwner.ownership('release');
    const status=JSON.parse((await cli('status',test.database,provider)).stdout);
    const shared=['codex','cline'].includes(provider);assert.equal(status.connections.length,shared?2:1);
    const evidence={id:randomUUID(),provider,laneVersion:status.laneVersion,previousGrantsRevoked:true,verifiedBy:'synthetic-test-operator',providerReference:'synthetic-full-group-reauthentication',verifiedAt:new Date().toISOString(),
      connections:status.connections.map(row=>({...row,accessToken:'synthetic-new-group-access-'+row.id,refreshToken:'synthetic-new-group-refresh-'+row.id,expiresAt:new Date(Date.now()+3600000).toISOString()}))};
    try {
      if(shared) {
        writeFileSync(evidenceFile,JSON.stringify({...evidence,connections:evidence.connections.filter(row=>row.provider===provider)}),{mode:0o600,flag:'wx'});
        await assert.rejects(cli('recover',test.database,evidenceFile,'--offline'),error=>error.stderr.includes('HA_CREDENTIAL_POLICY'));
        assert.equal((await test.db.collection('_ha_credential_intents').findOne({_id:unknown})).state,'dispatched');
      }
      writeFileSync(evidenceFile,JSON.stringify(evidence),{mode:0o600,flag:shared?'w':'wx'});
      assert.equal(JSON.parse((await cli('recover',test.database,evidenceFile,'--offline')).stdout).recovered,true);
      assert.equal((await test.db.collection('_ha_credential_intents').findOne({_id:unknown})).state,'reconciled');
      migrationOwner.ownership('claim',30000);
      if(provider==='codex')assert.equal((await providers.getProviderConnectionById(sharedOpenai.id)).refreshToken,'synthetic-new-group-refresh-'+sharedOpenai.id);
      if(provider==='cline')assert.equal((await providers.getProviderConnectionById(pairedConnections.get('clinepass'))).refreshToken,'synthetic-new-group-refresh-'+pairedConnections.get('clinepass'));
      await beginRefresh(migrationOwner,id,provider);
    } finally {try{unlinkSync(evidenceFile);}catch(error){if(error.code!=='ENOENT')throw error;}}
  }
  console.log('PASS Claude/Codex/Cline/Kimi/GitLab/GitHub group recovery: shared lanes block siblings, partial-group evidence refused, GitHub App review excludes classic no-refresh rows and every participating grant is replaced atomically through the real CLI');
  assert.equal(migrationOwner.compatibilityFault,undefined);
  console.log('PASS durable credential rotation: group-wide uncertainty, encrypted atomic publication/rollback, conditional callback writes, manual-update precedence and unresolved exchange retention across promotion');
  console.log('PASS real OmniRoute modules: two-process revocation, encrypted provider updates, quota bucket UPSERT and migration 078');
  console.log('PASS active-only lazy migrations: standby leaves legacy secrets unchanged; promotion encrypts provider and API-key state');
  console.log('PASS actual call artifact boundary: MongoDB storage, independent-process reads, missing state and fenced delete; no local worker/file fallback');
} finally {
  if (peer && peer.exitCode === null) { const exited = once(peer, 'exit'); peer.kill('SIGTERM'); await exited; }
  await credentialStandby?.close();await migrationOwner?.close(); await db.close(); delete globalThis.__omnirouteMongoAdapter; await test.cleanup();
}
