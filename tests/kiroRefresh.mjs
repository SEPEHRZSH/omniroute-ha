// Original Kiro helpers + real loopback HTTP/MongoDB. No AWS, IdP or personal
// credential files are contacted. All database names belong to this invocation.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {mkdtempSync,rmSync,writeFileSync} from 'node:fs';
import {randomUUID} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';
import {testDatabase,uri} from './mongoTestSupport.mjs';
import {MongoAdapter} from '../adapter/mongoAdapter.mjs';
import {createCredentialRuntime} from '../adapter/credentialRuntime.mjs';
import {credentialVersion} from '../adapter/credentialState.mjs';
import {provisionDomains} from './provisionDomains.mjs';
if(['stable-peer','registration-fault-peer','registration-resume-peer'].includes(process.argv[2])){
  const fault=process.argv[2]==='registration-fault-peer';
  assert.match(process.env.HA_KIRO_DATABASE,/^ha_test_kiro_[a-f0-9]{32}$/);
  const port=Number(process.env.HA_KIRO_PROVIDER_PORT);assert.ok(Number.isInteger(port)&&port>0&&port<65536);
  await import('../adapter/register.mjs');const native=globalThis.fetch;
  globalThis.fetch=(input,options)=>{
    const routes=new Map([['https://oidc.us-east-1.amazonaws.com/token','/oidc/token'],['https://oidc.us-east-1.amazonaws.com/client/register','/oidc/client/register']]);
    const route=routes.get(String(input));assert.ok(route,'Only this fixture\'s two synthetic OIDC routes are allowed');
    return native('http://127.0.0.1:'+port+route,options);
  };
  const db=new MongoAdapter({uri,database:process.env.HA_KIRO_DATABASE,ownerId:'synthetic-fresh-kiro-'+randomUUID()});
  try{
    assert.ok(db.ownership('claim',60000));globalThis.__omnirouteMongoAdapter=db;
    globalThis.__omnirouteCredentialRuntime=createCredentialRuntime(db,{assertOwner(){if(!db.canWrite())throw new Error('Owner required');}});
    const providers=await import('../upstream/omniroute/src/lib/db/providers.ts'),{getAccessToken}=await import('../upstream/omniroute/open-sse/services/tokenRefresh.ts');
    const row=await providers.getProviderConnectionById(process.env.HA_KIRO_CONNECTION);
    let lost=false;
    if(fault){
      db.ownership('renew',10000);const credential=db.credential.bind(db);
      db.credential=(action,values)=>{const result=credential(action,values);if(action==='kiroStage'&&values.stage==='registered'){lost=true;throw Object.assign(new Error('Synthetic lost registration publication reply'),{code:'HA_DB_OUTCOME_UNKNOWN'});}return result;};
    }
    const refreshing=getAccessToken('kiro',{connectionId:row.id,refreshToken:row.refreshToken,providerSpecificData:row.providerSpecificData},null,null,
      result=>providers.updateProviderConnection(row.id,{accessToken:result.accessToken,refreshToken:result.refreshToken}));
    if(fault){await assert.rejects(refreshing);assert.equal(lost,true);console.log('PASS interrupted Kiro safe step: real registration metadata commits before an injected lost acknowledgement; no token retry is sent and the owner exits without releasing its lease');}
    else{
      const result=await refreshing;
      if(process.argv[2]==='stable-peer')assert.equal(result.refreshToken,row.refreshToken);else assert.notEqual(result.refreshToken,row.refreshToken);
      assert.equal((await providers.getProviderConnectionById(row.id)).accessToken,result.accessToken);
      console.log(process.argv[2]==='stable-peer'?'PASS fresh-process Kiro reusable grant: a new Node process/owner loads the acknowledged MongoDB history and performs one real refresh without an in-process rotation cache':'PASS fresh-process Kiro safe continuation: a new epoch uses the persisted client, issues one token request and publishes the result without repeating registration');
    }
  }finally{if(!fault&&db.open)db.ownership('release');await db.close();}
  process.exit(0);
}
const directory=mkdtempSync(path.resolve('.work/kiro-')),test=await testDatabase('kiro');
Object.assign(process.env,{NODE_ENV:'test',DATA_DIR:directory,TMPDIR:directory,DISABLE_SQLITE_AUTO_BACKUP:'1',OMNIROUTE_DISABLE_BACKGROUND_SERVICES:'true',OMNIROUTE_DISABLE_REDIS_AUTH_CACHE:'true',STORAGE_ENCRYPTION_KEY:'synthetic-kiro-test-storage-key',API_KEY_SECRET:'synthetic-kiro-test-api-secret',JWT_SECRET:'synthetic-kiro-test-jwt-secret',HA_REDIS_MODE:'disabled',REDIS_URL:'',QDRANT_HOST:''});
await import('../adapter/register.mjs');
const bootstrap=new MongoAdapter({uri:test.uri,database:test.database}),nativeFetch=globalThis.fetch;
let owner,standby,server,current,mode='normal',tokenCalls=0,registrations=0,pending,releaseRegistration;
let registeredClient;
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
try{
  globalThis.__omnirouteMongoAdapter=bootstrap;await provisionDomains(bootstrap);await bootstrap.close();
  const providers=await import('../upstream/omniroute/src/lib/db/providers.ts');
  const {decryptConnectionFields}=await import('../upstream/omniroute/src/lib/db/encryption.ts');
  const {PROVIDERS}=await import('../upstream/omniroute/open-sse/config/constants.ts');
  const originalSocial=PROVIDERS.kiro.tokenUrl;
  server=createServer((request,response)=>{
    let wire='';request.on('data',chunk=>{wire+=chunk;});request.on('end',()=>void(async()=>{
      response.setHeader('content-type','application/json');
      if(request.url==='/oidc/client/register'){
        registrations++;const finish=()=>response.end(JSON.stringify(registeredClient));
        registeredClient={clientId:'synthetic-new-client-'+registrations,clientSecret:'synthetic-new-secret-'+registrations,clientSecretExpiresAt:Math.floor(Date.now()/1000)+86400};
        if(mode==='held-registration')releaseRegistration=finish;else finish();return;
      }
      tokenCalls++;
      if(request.url==='/oidc/token'){
        const body=JSON.parse(wire);assert.equal(body.grantType,'refresh_token');
        if(['reregister','held-registration'].includes(mode)&&body.clientId==='synthetic-old-client'){
          response.writeHead(401).end(JSON.stringify({__type:'InvalidClientException',error:'invalid_client'}));return;
        }
        if(mode==='unknown'){response.writeHead(503).end(JSON.stringify({error:'server_error'}));return;}
        if(['reregister','safe-recovery'].includes(mode)){
          const stored=await test.db.collection('provider_connections').findOne({id:current.id}),metadata=JSON.parse(stored.provider_specific_data);
          assert.ok(metadata.clientSecret.startsWith('enc:v1:'));assert.equal(JSON.stringify(metadata).includes(registeredClient.clientSecret),false);
          const decrypted=decryptConnectionFields({providerSpecificData:metadata});assert.equal(decrypted.providerSpecificData.clientSecret,body.clientSecret);
          assert.equal((await test.db.collection('_ha_credential_intents').findOne({connectionId:current.id,state:'dispatched'})).kiroPhase,mode==='reregister'?'retry_dispatched':'token_dispatched');
        }
      }else if(request.url==='/external/token'){
        const form=new URLSearchParams(wire);assert.equal(form.get('grant_type'),'refresh_token');assert.equal(form.get('client_id'),'synthetic-org-client');assert.equal(form.has('client_secret'),false);
      }else{assert.equal(request.url,'/social');assert.ok(JSON.parse(wire).refreshToken.startsWith('synthetic-'));}
      const tokens={accessToken:'synthetic-access-'+tokenCalls,refreshToken:'synthetic-refresh-'+tokenCalls,expiresIn:3600};
      if(mode==='stable')delete tokens.refreshToken;
      response.end(JSON.stringify(request.url==='/external/token'?{access_token:tokens.accessToken,refresh_token:tokens.refreshToken,expires_in:tokens.expiresIn}:tokens));
    })().catch(error=>{response.destroy();console.error(error);process.exitCode=1;}));
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const base='http://127.0.0.1:'+server.address().port;
  const routes=new Map([['https://oidc.us-east-1.amazonaws.com/token','/oidc/token'],['https://oidc.us-east-1.amazonaws.com/client/register','/oidc/client/register'],['https://login.microsoftonline.com/synthetic-tenant/oauth2/v2.0/token','/external/token']]);
  globalThis.fetch=(input,options)=>{
    const url=String(input),route=routes.get(url);
    if(!route&&!url.startsWith(base+'/'))throw new Error('External network forbidden in Kiro fixture');
    return nativeFetch(route?base+route:input,options);
  };
  PROVIDERS.kiro.tokenUrl=base+'/social';
  try{
    owner=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-kiro-owner'});
    assert.ok(owner.ownership('claim',60000));globalThis.__omnirouteMongoAdapter=owner;
    const runtime=createCredentialRuntime(owner,{assertOwner(){if(!owner.canWrite())throw Object.assign(new Error('Owner required'),{code:'HA_FENCED'});}});
    globalThis.__omnirouteCredentialRuntime=runtime;
    const {getAccessToken}=await import('../upstream/omniroute/open-sse/services/tokenRefresh.ts');
    const oidc={authMethod:'builder-id',clientId:'synthetic-old-client',clientSecret:'synthetic-old-client-secret',region:'not-an-aws-region'};
    const create=async(provider,metadata)=>providers.createProviderConnection({provider,authType:'oauth',name:'Synthetic '+provider+' '+mode,accessToken:'synthetic-old-access-'+randomUUID(),refreshToken:'synthetic-old-refresh-'+randomUUID(),expiresAt:new Date(Date.now()-60000).toISOString(),providerSpecificData:metadata});
    const refresh=connection=>getAccessToken(connection.provider,{connectionId:connection.id,refreshToken:connection.refreshToken,providerSpecificData:connection.providerSpecificData},null,null,async result=>{
      const stored=await providers.getProviderConnectionById(connection.id);assert.equal(stored.refreshToken,result.refreshToken,'Final pair must be durable before onPersist');
      await providers.updateProviderConnection(connection.id,{accessToken:result.accessToken,refreshToken:result.refreshToken});
    });
    const peer=argument=>promisify(execFile)(process.execPath,[process.argv[1],argument],{env:{...process.env,HA_KIRO_DATABASE:test.database,HA_KIRO_CONNECTION:current.id,HA_KIRO_PROVIDER_PORT:String(server.address().port)},timeout:45000,maxBuffer:262144});
    for(const [provider,metadata]of [
      ['kiro',oidc],['amazon-q',{...oidc,authMethod:'imported'}],
      ['kiro',{authMethod:'external_idp',clientId:'synthetic-org-client',tokenEndpoint:'https://login.microsoftonline.com/synthetic-tenant/oauth2/v2.0/token',scopes:'synthetic-scope offline_access'}],
    ]){
      current=await create(provider,metadata);const before=tokenCalls;
      const results=await Promise.all([refresh(current),refresh(current)]);assert.deepEqual(results[0],results[1]);assert.equal(tokenCalls,before+1);assert.equal(registrations,0);
    }
    console.log('PASS original Kiro/Amazon Q refresh HTTP: OIDC safe-region fallback, imported-social routing, allowlisted External IdP public-client form, concurrent dedup and encrypted early publication');
    mode='reregister';current=await create('kiro',oidc);
    const original=await test.db.collection('provider_connections').findOne({id:current.id});
    const lane=await test.db.collection('_ha_credential_lanes').findOne({_id:'kiro'});
    while(owner.serverTime()<lane.readyAfter)await pause(25);
    for(const invalid of ['order','status','grant','prototype']){
      owner.begin();const id=randomUUID();assert.equal(owner.credential('dispatch',{id,connectionId:current.id,provider:'kiro',version:credentialVersion(original)}).dispatched,true);
      if(invalid!=='order')owner.credential('kiroStage',{id,stage:'initial'});
      assert.throws(()=>invalid==='grant'?owner.credential('publish',{id,fields:{},reusableGrant:true,stableTokenHash:'a'.repeat(64)}):owner.credential('kiroStage',{id,...(invalid==='order'?{stage:'register'}:invalid==='prototype'?{stage:'toString'}:{stage:'rejected',status:503,error:'invalid_client'})}),{code:'HA_CREDENTIAL_POLICY'});
      owner.rollback();assert.equal(await test.db.collection('_ha_credential_intents').countDocuments({_id:id}),0);
    }
    const before=tokenCalls;await refresh(current);assert.equal(tokenCalls,before+2);assert.equal(registrations,1);
    const completed=await test.db.collection('_ha_credential_intents').findOne({connectionId:current.id});assert.equal(completed.state,'completed');assert.equal(completed.kiroPhase,'retry_dispatched');
    assert.equal((await providers.getProviderConnectionById(current.id)).providerSpecificData.clientId,registeredClient.clientId);
    console.log('PASS durable Kiro OIDC recovery HTTP: explicit 401 invalid_client, fenced registration, encrypted client persisted before the one retry, final tokens durable before callback and transaction rollback/order checks');
    mode='stable';current=await create('kiro',oidc);const stableBefore=tokenCalls;
    await refresh(current);await refresh(current);assert.equal(tokenCalls,stableBefore+2);
    assert.equal((await providers.getProviderConnectionById(current.id)).refreshToken,current.refreshToken);
    assert.equal(await test.db.collection('_ha_credential_intents').countDocuments({connectionId:current.id,state:'completed',reusableGrant:true}),2);
    owner.ownership('release');
    const stablePeer=await peer('stable-peer');
    assert.match(stablePeer.stdout,/PASS fresh-process Kiro reusable grant/);console.log(stablePeer.stdout.trim());
    assert.equal(tokenCalls,stableBefore+3);assert.ok(owner.ownership('claim',60000));
    assert.equal(await test.db.collection('_ha_credential_intents').countDocuments({connectionId:current.id,state:'completed',reusableGrant:true}),3);
    console.log('PASS acknowledged Kiro reusable grant: the original helper retains an omitted refresh token, two real successful calls publish encrypted state and exact per-connection receipt proof without weakening unknown-result quarantine');
    mode='unknown';const calls=tokenCalls,registers=registrations;
    await assert.rejects(refresh(current));assert.equal(tokenCalls,calls+1);assert.equal(registrations,registers);
    const alias=await create('amazon-q',oidc);await assert.rejects(refresh(alias),{code:'HA_CREDENTIAL_UNCERTAIN'});assert.equal(tokenCalls,calls+1);
    owner.ownership('release');assert.ok(owner.ownership('claim',60000));
    const aliasVersion=credentialVersion(await test.db.collection('provider_connections').findOne({id:alias.id}));
    assert.throws(()=>owner.credential('dispatch',{id:randomUUID(),connectionId:alias.id,provider:'amazon-q',version:aliasVersion}),{code:'HA_CREDENTIAL_UNCERTAIN'});
    console.log('PASS Kiro uncertainty: a server-error outcome cannot trigger registration/replay, and the shared Amazon Q lane blocks a sibling before dispatch');
    owner.ownership('release');
    const cli=async(...args)=>JSON.parse((await promisify(execFile)(process.execPath,['scripts/credentials.mjs',...args],{env:{...process.env,HA_MONGO_URI:test.uri},timeout:60000,maxBuffer:262144})).stdout);
    const status=await cli('status',test.database,'kiro');assert.ok(status.connections.some(row=>row.provider==='amazon-q'));
    const evidence={id:randomUUID(),provider:'kiro',laneVersion:status.laneVersion,previousGrantsRevoked:true,verifiedBy:'synthetic-test-reviewer',providerReference:'synthetic-reauthenticated-group',verifiedAt:new Date().toISOString(),connections:status.connections.map(row=>({...row,accessToken:'synthetic-new-grant-access-'+row.id,refreshToken:'synthetic-new-grant-refresh-'+row.id,expiresAt:new Date(Date.now()+3600000).toISOString(),...(row.id===current.id?{oidcClient:{clientId:'synthetic-reviewed-client',clientSecret:'synthetic-reviewed-secret',clientSecretExpiresAt:Math.floor(Date.now()/1000)+86400}}:{})}))};
    const file=path.join(directory,'recovery.json');
    const oldGrant={...evidence,connections:evidence.connections.map(row=>row.id===current.id?{...row,refreshToken:current.refreshToken}:row)};
    writeFileSync(file,JSON.stringify(oldGrant),{mode:0o600});
    await assert.rejects(cli('recover',test.database,file,'--offline'),error=>error.stderr.includes('HA_CREDENTIAL_NEW_GRANT_REQUIRED'));
    writeFileSync(file,JSON.stringify(evidence),{mode:0o600});
    assert.equal((await cli('recover',test.database,file,'--offline')).recovered,true);assert.equal((await cli('recover',test.database,file,'--offline')).idempotent,true);
    assert.ok(owner.ownership('claim',60000));assert.equal((await providers.getProviderConnectionById(current.id)).providerSpecificData.clientSecret,'synthetic-reviewed-secret');
    console.log('PASS Kiro full-group recovery CLI: Kiro/Amazon Q members replaced together, optional OIDC client secret encrypted atomically, preserved metadata and identical evidence cannot reset newer state');
    mode='reregister';current=await create('kiro',oidc);owner.ownership('release');
    const safeCalls=tokenCalls,safeRegistrations=registrations,faultPeer=await peer('registration-fault-peer');
    assert.match(faultPeer.stdout,/PASS interrupted Kiro safe step/);console.log(faultPeer.stdout.trim());
    assert.equal(tokenCalls,safeCalls+1);assert.equal(registrations,safeRegistrations+1);
    const safeIntent=await test.db.collection('_ha_credential_intents').findOne({connectionId:current.id});assert.equal(safeIntent.kiroPhase,'registration_published');assert.equal(safeIntent.state,'dispatched');
    const leaseDeadline=Date.now()+15000;
    while(!await test.db.collection('_ha_serial').findOne({_id:'writer',$expr:{$lte:['$leaseUntil','$$NOW']}})){assert.ok(Date.now()<leaseDeadline);await pause(100);}
    mode='safe-recovery';const resumed=await peer('registration-resume-peer');assert.match(resumed.stdout,/PASS fresh-process Kiro safe continuation/);console.log(resumed.stdout.trim());
    assert.equal(tokenCalls,safeCalls+2);assert.equal(registrations,safeRegistrations+1);
    const retired=await test.db.collection('_ha_credential_intents').findOne({_id:safeIntent._id});assert.equal(retired.state,'cancelled');assert.equal(retired.cancellationReason,'known-oidc-step-before-promotion');assert.ok(retired.recoveryEpoch>retired.epoch);
    assert.ok(owner.ownership('claim',60000));
    mode='held-registration';current=await create('kiro',oidc);owner.ownership('renew',5000);
    pending=refresh(current).then(value=>({value}),error=>({error}));
    const deadline=Date.now()+10000;while(!releaseRegistration){assert.ok(Date.now()<deadline);await pause(25);}
    const old=await test.db.collection('provider_connections').findOne({id:current.id});await owner.close();
    standby=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-kiro-promoted'});
    const promotion=Date.now()+12000;while(!standby.ownership('claim',10000)){assert.ok(Date.now()<promotion);await pause(100);}
    const attempts=tokenCalls;releaseRegistration();assert.ok((await pending).error);assert.equal(tokenCalls,attempts,'Old registration response cannot dispatch a token retry after promotion');
    assert.equal((await test.db.collection('provider_connections').findOne({id:current.id})).provider_specific_data,old.provider_specific_data);
    assert.throws(()=>standby.credential('dispatch',{id:randomUUID(),connectionId:current.id,provider:'kiro',version:credentialVersion(old)}),{code:'HA_CREDENTIAL_UNCERTAIN'});
    console.log('PASS interrupted Kiro registration: natural lease expiry/promoted owner, no stale client publication or token retry, retained group uncertainty and no automatic replay');
  }finally{PROVIDERS.kiro.tokenUrl=originalSocial;}
}finally{
  releaseRegistration?.();if(pending)await pending;
  await owner?.close();await standby?.close();await bootstrap.close();
  if(server){server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
  globalThis.fetch=nativeFetch;delete globalThis.__omnirouteMongoAdapter;delete globalThis.__omnirouteCredentialRuntime;
  await test.cleanup();rmSync(directory,{recursive:true});
}
