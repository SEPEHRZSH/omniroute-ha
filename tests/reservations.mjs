import assert from 'node:assert/strict';
import { randomUUID,createHash } from 'node:crypto';
import { readFileSync, mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { installBaseSchema } from '../adapter/baseSchema.mjs';
import { testDatabase } from './mongoTestSupport.mjs';
import { installQuotaRuntime } from '../adapter/quotaRuntime.mjs';
import { Socket } from 'node:net';
import path from 'node:path';
import { getBudgetWindow } from '../adapter/budgetCalendar.mjs';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
const test = await testDatabase('reservations');
const bounds = { 'openai-compatible-test/model': { tokens: 100, nanoUsd: 100000000,tools:true,maxImages:2,maxAudioBytes:16,mediaPricing:'provider' }, 'openai-compatible-test/other': { tokens: 100, nanoUsd: 100000000 } };
bounds['openai-compatible-test/defaults']={tokens:100,nanoUsd:100000000,defaultOutputTokens:5};
const config = { uri: test.uri, database: test.database, quotaBounds: bounds };
for(const value of [0,-1,101,1.5,'5'])assert.throws(()=>new MongoAdapter({...config,quotaBounds:{'openai-compatible-test/defaults':{tokens:100,nanoUsd:100000000,defaultOutputTokens:value}}}),/Invalid certified/);
const bootstrap = new MongoAdapter(config);
let active, standby,calendarActor;
const windowCases=[];
try {
  installBaseSchema(bootstrap);
  bootstrap.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/033_create_reasoning_cache.sql','utf8'));
  bootstrap.exec('ALTER TABLE api_keys ADD COLUMN usage_limit_enabled INTEGER DEFAULT 0; ALTER TABLE api_keys ADD COLUMN daily_usage_limit_usd REAL; ALTER TABLE api_keys ADD COLUMN weekly_usage_limit_usd REAL; ALTER TABLE api_keys ADD COLUMN allowed_connections TEXT');
  for (const name of ['013_quota_snapshots','108_provider_quota_reset_events','212_usage_history_provider_reported_cost']) bootstrap.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/'+name+'.sql','utf8'));
  bootstrap.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/182_request_cost_ledger_and_key_quota.sql', 'utf8'));
  const tokenSource = readFileSync('upstream/omniroute/src/lib/db/tokenLimits.ts','utf8');
  bootstrap.exec(tokenSource.slice(tokenSource.indexOf('function ensureSchema()')).match(/db\.exec\(`([\s\S]*?)`\)/)[1]);
  bootstrap.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/215_team_cost_centers.sql','utf8'));
  for (const id of ['key','usd-only','token-only','calendar','scoped-global','scoped-provider','scoped-model','scoped-legacy','usage-usd','usage-calendar','usage-legacy','usage-disabled','team-a','team-b','team-legacy','stream-ok','stream-missing','stream-cancel','tool-json','tool-stream','media-json','media-unknown','media-limit','tool-disabled','default-cap','responses-json','responses-stream']) bootstrap.prepare('INSERT INTO api_keys(id,name,key,created_at) VALUES (?,?,?,?)').run(id, id, 'synthetic-' + id, new Date().toISOString());
  bootstrap.prepare('INSERT INTO provider_connections(id,provider,created_at,updated_at) VALUES (?,?,?,?)').run('subscription-conn','synthetic-subscription',new Date().toISOString(),new Date().toISOString());
  bootstrap.prepare('INSERT INTO provider_connections(id,provider,provider_specific_data,created_at,updated_at) VALUES (?,?,?,?,?)').run('unrelated-codex','codex','{"codexQuotaState":0.5}',new Date().toISOString(),new Date().toISOString());
  for (const id of ['usage-usd','usage-calendar','usage-legacy','usage-disabled']) bootstrap.prepare('UPDATE api_keys SET usage_limit_enabled=1,daily_usage_limit_usd=0.15,weekly_usage_limit_usd=0.15,allowed_connections=? WHERE id=?').run('["subscription-conn"]',id);
  for (const scope of ['global','provider','model','legacy']) bootstrap.prepare('INSERT INTO api_key_token_limits(id,api_key_id,scope_type,scope_value,token_limit) VALUES (?,?,?,?,?)')
    .run('limit-'+scope,'scoped-'+scope,scope==='legacy'?'global':scope,scope==='provider'?'openai-compatible-test':scope==='model'?'model':'',150);
  bootstrap.prepare('INSERT INTO api_key_quota_limits(api_key_id,tpm_limit,monthly_amount_usd) VALUES (?,?,?)').run('key', 250, 0.25);
  bootstrap.prepare('INSERT INTO api_key_quota_limits(api_key_id,monthly_amount_usd) VALUES (?,?)').run('usd-only', 0.15);
  for(const id of ['stream-ok','stream-missing','stream-cancel','tool-json','tool-stream','media-json','media-unknown','media-limit','tool-disabled','default-cap','responses-json','responses-stream'])bootstrap.prepare('INSERT INTO api_key_quota_limits(api_key_id,monthly_amount_usd) VALUES (?,?)').run(id,0.15);
  bootstrap.prepare('INSERT INTO api_key_quota_limits(api_key_id,tpm_limit) VALUES (?,?)').run('token-only', 150);
  bootstrap.close();
  active = new MongoAdapter({ ...config, ownerId: 'reservation-active' });
  standby = new MongoAdapter({ ...config, ownerId: 'reservation-standby' });
  active.ownership('claim', 30000);
  const reserve = (db = active, keyId = 'key', model = 'model') => { const id = randomUUID(); db.reservation('reserve', { id, keyId, provider: 'openai-compatible-test', model }); return id; };
  const quotaNow=active.serverTime(), resetAt=new Date(quotaNow+2*86400000).toISOString();
  for(const team of ['shared-team','legacy-team']) active.prepare('INSERT INTO teams(id,name,max_budget_usd,budget_duration,budget_reset_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?)')
    .run(team,team,0.15,'1d',new Date(quotaNow+86400000).toISOString(),new Date(quotaNow).toISOString(),new Date(quotaNow).toISOString());
  for(const [key,team] of [['team-a','shared-team'],['team-b','shared-team'],['team-legacy','legacy-team']]) active.prepare('INSERT INTO api_key_billing_team_history(id,api_key_id,team_id,valid_from,created_at) VALUES (?,?,?,?,?)')
    .run('binding-'+key,key,team,new Date(quotaNow-1000).toISOString(),new Date(quotaNow).toISOString());
  assert.equal(active.checkQuotaPolicy('team-a').managed,true);
  const teamHold=reserve(active,'team-a');
  assert.throws(()=>reserve(active,'team-b'),{code:'HA_QUOTA_EXCEEDED'},'Team members must share pending liabilities');
  active.reservation('dispatch',{id:teamHold});
  const teamReceipt={input:10,output:0,tokens:10,nanoUsd:60000000,priced:true};
  active.begin(); active.reservation('settle',{id:teamHold,receipt:teamReceipt}); active.rollback();
  assert.equal((await test.db.collection('_ha_reservations').findOne({_id:teamHold})).state,'dispatched');
  active.reservation('settle',{id:teamHold,receipt:teamReceipt});
  assert.throws(()=>reserve(active,'team-b'),{code:'HA_QUOTA_EXCEEDED'},'Team spend and holds must be atomic');
  active.prepare('UPDATE teams SET max_budget_usd=0.25 WHERE id=?').run('shared-team');
  const teamUnknown=reserve(active,'team-b'); active.reservation('dispatch',{id:teamUnknown});
  active.prepare('INSERT INTO domain_cost_history(api_key_id,cost,timestamp) VALUES (?,?,?)').run('team-legacy',0.01,quotaNow);
  assert.throws(()=>reserve(active,'team-legacy'),{code:'HA_QUOTA_POLICY'},'Team baselines must not ignore previously untracked member usage');
  active.prepare('INSERT INTO key_value(namespace,key,value) VALUES (?,?,?)').run('providerLimitsCache','subscription-conn',JSON.stringify({fetchedAt:new Date(quotaNow).toISOString(),quotas:{weekly:{remainingPercentage:50,resetAt}}}));
  active.prepare('INSERT INTO provider_quota_reset_events(provider,connection_id,window_key,window_started_at,window_resets_at,observed_at) VALUES (?,?,?,?,?,?)')
    .run('synthetic-subscription','subscription-conn','weekly',new Date(quotaNow-86400000).toISOString(),resetAt,new Date(quotaNow-3600000).toISOString());
  for (const [hours,remaining] of [[4,50],[2,99]]) active.prepare('INSERT INTO quota_snapshots(provider,connection_id,window_key,remaining_percentage,next_reset_at,created_at) VALUES (?,?,?,?,?,?)')
    .run('synthetic-subscription','subscription-conn','weekly',remaining,resetAt,new Date(quotaNow-hours*3600000).toISOString());
  const usageHold=reserve(active,'usage-usd');
  windowCases.push({id:usageHold,keyId:'usage-usd',mode:'provider',zone:'UTC'});
  assert.throws(()=>reserve(active,'usage-usd'),{code:'HA_QUOTA_EXCEEDED'},'Key USD policies must include pending liabilities');
  active.reservation('dispatch',{id:usageHold});
  active.reservation('settle',{id:usageHold,receipt:{input:10,output:0,tokens:10,nanoUsd:20000000,priced:true}});
  const secondUsage=reserve(active,'usage-usd'); active.reservation('dispatch',{id:secondUsage});
  active.reservation('settle',{id:secondUsage,receipt:{input:10,output:0,tokens:10,nanoUsd:50000000,priced:true}});
  assert.throws(()=>reserve(active,'usage-usd'),{code:'HA_QUOTA_EXCEEDED'},'Key USD policy must include settled spend');
  active.prepare('INSERT INTO domain_cost_history(api_key_id,cost,timestamp) VALUES (?,?,?)').run('usage-legacy',0.01,quotaNow);
  assert.throws(()=>reserve(active,'usage-legacy'),{code:'HA_QUOTA_POLICY'});
  const disabledUsage=reserve(active,'usage-disabled'); active.reservation('cancel',{id:disabledUsage});
  active.prepare('UPDATE api_keys SET usage_limit_enabled=0 WHERE id=?').run('usage-disabled');
  assert.equal(active.checkQuotaPolicy('usage-disabled').managed,false);
  assert.equal(await test.db.collection('_ha_key_usage_profiles').countDocuments({_id:'usage-disabled'}),0);
  active.prepare('INSERT INTO domain_cost_history(api_key_id,cost,timestamp) VALUES (?,?,?)').run('usage-disabled',0.01,quotaNow);
  active.prepare('UPDATE api_keys SET usage_limit_enabled=1 WHERE id=?').run('usage-disabled');
  assert.throws(()=>reserve(active,'usage-disabled'),{code:'HA_QUOTA_POLICY'},'A disabled-policy accounting gap must not reuse an old complete-tracking marker');
  active.prepare('UPDATE api_keys SET daily_usage_limit_usd=0.25,weekly_usage_limit_usd=0.25 WHERE id=?').run('usage-usd');
  const dispatchedUsage=reserve(active,'usage-usd'); active.reservation('dispatch',{id:dispatchedUsage});
  const oldMode=process.env.OMNIROUTE_API_KEY_WEEKLY_WINDOW,oldZone=process.env.OMNIROUTE_API_KEY_WEEKLY_WINDOW_TIMEZONE;
  try {process.env.OMNIROUTE_API_KEY_WEEKLY_WINDOW='calendar';process.env.OMNIROUTE_API_KEY_WEEKLY_WINDOW_TIMEZONE='America/New_York';calendarActor=new MongoAdapter({...config,ownerId:'calendar-usage'});}
  finally {for(const [name,value] of [['OMNIROUTE_API_KEY_WEEKLY_WINDOW',oldMode],['OMNIROUTE_API_KEY_WEEKLY_WINDOW_TIMEZONE',oldZone]]) if(value===undefined)delete process.env[name];else process.env[name]=value;}
  active.ownership('release'); calendarActor.ownership('claim',30000);
  const calendarUsage=reserve(calendarActor,'usage-calendar'); windowCases.push({id:calendarUsage,keyId:'usage-calendar',mode:'calendar',zone:'America/New_York'});
  calendarActor.reservation('cancel',{id:calendarUsage}); calendarActor.ownership('release'); active.ownership('claim',30000);
  for (const scope of ['global','provider','model']) {
    const keyId = 'scoped-'+scope, intent = reserve(active,keyId);
    assert.throws(() => reserve(active,keyId),{code:'HA_QUOTA_EXCEEDED'});
    if (scope==='model') { const unrelated = reserve(active,keyId,'other'); active.reservation('cancel',{id:unrelated}); }
    else assert.throws(() => reserve(active,keyId,'other'),{code:'HA_QUOTA_EXCEEDED'});
    active.reservation('dispatch',{id:intent});
    active.begin(); active.reservation('settle',{id:intent,receipt:{input:10,output:5,tokens:15,nanoUsd:10000,priced:true}}); active.rollback();
    assert.equal(await test.db.collection('api_key_token_counters').countDocuments({limit_id:'limit-'+scope}),0);
    assert.equal(active.reservation('settle',{id:intent,receipt:{input:10,output:5,tokens:15,nanoUsd:10000,priced:true}}).settled,true);
    assert.equal((await test.db.collection('api_key_token_counters').findOne({limit_id:'limit-'+scope})).tokens_used,15);
  }
  active.prepare('INSERT INTO api_key_token_counters(limit_id,window_start,tokens_used) VALUES (?,?,?)').run('limit-legacy',String(getBudgetWindow('monthly','00:00',active.serverTime()).periodStartAt),10);
  assert.throws(() => reserve(active,'scoped-legacy'),{code:'HA_QUOTA_POLICY'},'Unverified historical scoped usage must not be guessed away');
  active.prepare('UPDATE api_key_token_limits SET enabled=0 WHERE id=?').run('limit-global');
  const disabled = reserve(active,'scoped-global'); active.reservation('dispatch',{id:disabled});
  active.reservation('settle',{id:disabled,receipt:{input:10,output:0,tokens:10,nanoUsd:10000,priced:true}});
  active.prepare('UPDATE api_key_token_limits SET enabled=1,token_limit=100 WHERE id=?').run('limit-global');
  assert.throws(() => reserve(active,'scoped-global'),{code:'HA_QUOTA_EXCEEDED'},'Reenabling a scoped limit must retain disabled-period consumption');
  await test.db.collection('api_key_token_limits').updateOne({id:'limit-model'},{$set:{scope_value:{$ne:''}}});
  assert.throws(() => reserve(active,'scoped-model'),{code:'HA_QUOTA_POLICY'},'Malformed scope values must not become Mongo query operators or silently remove a cap');
  await test.db.collection('api_key_token_limits').updateOne({id:'limit-model'},{$set:{scope_value:'model'}});
  active.prepare('UPDATE api_key_token_limits SET reset_time=? WHERE id=?').run('05:30','limit-model');
  assert.throws(() => reserve(active,'scoped-model'),{code:'HA_QUOTA_POLICY'},'Changing a calendar cannot erase recorded scoped consumption');
  active.prepare('UPDATE api_key_token_limits SET reset_time=? WHERE id=?').run('00:00','limit-model');
  active.prepare('INSERT INTO domain_budgets(api_key_id,daily_limit_usd,weekly_limit_usd,monthly_limit_usd,reset_interval,reset_time) VALUES (?,?,?,?,?,?)')
    .run('calendar',0.15,0.15,0.15,'daily','00:00');
  for (const interval of ['daily','weekly','monthly']) {
    active.prepare('UPDATE domain_budgets SET reset_interval=? WHERE api_key_id=?').run(interval,'calendar');
    assert.equal(active.checkQuotaPolicy('calendar').managed,true);
    const intent = reserve(active,'calendar');
    assert.throws(() => reserve(active,'calendar'),{code:'HA_QUOTA_EXCEEDED'},'Calendar USD quota must include pending reservations');
    active.reservation('cancel',{id:intent});
  }
  active.prepare('INSERT INTO domain_cost_history(api_key_id,cost,timestamp) VALUES (?,?,?)').run('calendar',0.06,active.serverTime());
  assert.throws(() => reserve(active,'calendar'),{code:'HA_QUOTA_EXCEEDED'},'Calendar USD quota must include already recorded spend');
  active.prepare('UPDATE domain_budgets SET monthly_limit_usd=? WHERE api_key_id=?').run(0.2,'calendar');
  active.prepare('INSERT INTO key_value(namespace,key,value) VALUES (?,?,?)').run('budgetUnpricedUsage','calendar',String(getBudgetWindow('monthly','00:00',active.serverTime()).periodStartAt));
  assert.throws(() => reserve(active,'calendar'),{code:'HA_QUOTA_UNCERTAIN'});
  active.prepare('DELETE FROM key_value WHERE namespace=? AND key=?').run('budgetUnpricedUsage','calendar');
  const calendarHold = reserve(active,'calendar'); active.reservation('dispatch',{id:calendarHold});
  for (const keyId of ['usd-only','token-only']) { const intent = reserve(active, keyId); assert.throws(() => reserve(active, keyId), { code: 'HA_QUOTA_EXCEEDED' }); active.reservation('cancel', { id: intent }); }
  const first = reserve(), second = reserve();
  assert.throws(() => reserve(), { code: 'HA_QUOTA_EXCEEDED' });
  assert.equal(await test.db.collection('_ha_reservations').countDocuments({ keyId: 'key' }), 2);
  assert.throws(() => standby.reservation('dispatch', { id: first }), { code: 'HA_FENCED' });
  active.reservation('dispatch', { id: first });
  assert.throws(() => active.reservation('dispatch', { id: first }), { code: 'HA_RESERVATION_CONFLICT' });
  active.reservation('cancel', { id: first });
  assert.equal((await test.db.collection('_ha_reservations').findOne({ _id: first })).state, 'dispatched');
  const receipt = { input: 10, output: 5, tokens: 15, nanoUsd: 15000000, priced: true };
  assert.equal(active.reservation('settle', { id: first, receipt }).settled, true);
  assert.equal(active.reservation('settle', { id: first, receipt: Object.fromEntries(Object.entries(receipt).reverse()) }).settled, true, 'Receipt field order must not change idempotence');
  assert.equal(active.reservation('settle', { id: first, receipt }).settled, true);
  assert.equal(await test.db.collection('domain_cost_history').countDocuments({ api_key_id: 'key' }), 1);
  assert.equal(await test.db.collection('request_cost_ledger').countDocuments({api_key_id:'key'}), 1);
  assert.equal((await test.db.collection('api_key_quota_counters').findOne({api_key_id:'key'})).consumed, 15);
  active.reservation('cancel', { id: second });
  const uncertain = reserve(); active.reservation('dispatch', { id: uncertain });
  const abandoned = reserve(); // Intent remains undispatched when active disappears.
  active.ownership('release'); standby.ownership('claim', 30000);
  assert.throws(() => active.reservation('dispatch', { id: abandoned }), { code: 'HA_FENCED' });
  const promoted = reserve(standby);
  assert.throws(()=>reserve(standby,'team-a'),{code:'HA_QUOTA_EXCEEDED'},'Dispatched Team liability must survive promotion');
  assert.equal(standby.reservation('settle',{id:teamUnknown,receipt:{...teamReceipt,nanoUsd:1000000000}}).settled,false);
  assert.throws(()=>reserve(standby,'team-a'),{code:'HA_QUOTA_UNCERTAIN'},'Uncertain provider accounting must quarantine the whole Team');
  console.log('PASS hard Team budgets: cross-key pending/spent capacity, transaction rollback, new-policy baseline rejection, promotion liability retention and shared uncertainty quarantine');
  assert.throws(()=>reserve(standby,'usage-usd'),{code:'HA_QUOTA_EXCEEDED'},'Unknown key USD liability must survive promotion');
  assert.throws(() => reserve(standby,'calendar'),{code:'HA_QUOTA_EXCEEDED'},'Calendar liability must survive promotion');
  assert.equal((await test.db.collection('_ha_reservations').findOne({ _id: abandoned })).state, 'cancelled');
  assert.equal((await test.db.collection('_ha_reservations').findOne({ _id: uncertain })).state, 'dispatched');
  assert.throws(() => reserve(standby), { code: 'HA_QUOTA_EXCEEDED' });
  // A missing receipt never frees a liability. An impossible/contradictory one
  // quarantines this key in the same transaction rather than acknowledging $0.
  assert.equal(standby.reservation('settle', { id: promoted, receipt }).settled, false);
  assert.throws(() => reserve(standby), { code: 'HA_QUOTA_UNCERTAIN' });
  assert.ok(await test.db.collection('_ha_quota_blocked').findOne({ _id: 'key' }));
  const evidence = { reservationId: promoted, provider: 'openai-compatible-test', model: 'model', providerReceiptId: 'synthetic-verified-receipt',
    verifiedBy: 'synthetic-operator', receipt };
  assert.throws(() => standby.reservation('reconcile', {id:promoted,receipt,evidence:{...evidence,sha256:'a'.repeat(64)}}),{code:'HA_QUOTA_POLICY'},'Normal application connections cannot perform operator reconciliation');
  mkdirSync('.work/test-artifacts',{recursive:true});
  const evidenceFile = path.join(mkdtempSync(path.resolve('.work/test-artifacts/quota-recovery-')),'evidence.json');
  const writeEvidence = value => writeFileSync(evidenceFile,JSON.stringify(value),{mode:0o600});
  writeEvidence(evidence);
  const cli = (...args) => promisify(execFile)(process.execPath,['scripts/quota.mjs',...args],{env:{...process.env,HA_MONGO_URI:test.uri},maxBuffer:64000,timeout:20000});
  await assert.rejects(cli('reconcile',test.database,evidenceFile,'--offline'),/Stop both applications/);
  standby.ownership('release');
  writeEvidence({...evidence,provider:'wrong-provider'});
  await assert.rejects(cli('reconcile',test.database,evidenceFile,'--offline'),/does not match/);
  assert.equal((await test.db.collection('_ha_reservations').findOne({_id:promoted})).state,'uncertain');
  writeEvidence(evidence);
  const recovered = await cli('reconcile',test.database,evidenceFile,'--offline');
  assert.equal(JSON.parse(recovered.stdout).settled,true);
  await cli('reconcile',test.database,evidenceFile,'--offline');
  assert.equal(await test.db.collection('_ha_quota_reconciliations').countDocuments({_id:promoted}),1);
  assert.equal(await test.db.collection('_ha_quota_blocked').countDocuments({_id:'key'}),0);
  assert.equal(await test.db.collection('request_cost_ledger').countDocuments({api_key_id:'key'}),2,'Repeated evidence must not charge twice');
  const status = JSON.parse((await cli('status',test.database,promoted)).stdout);
  assert.equal(status.state,'settled'); assert.equal(status.receipt,undefined,'Status must not print provider evidence or credentials');
  standby.ownership('claim',30000);
  const afterReconciliation = reserve(standby); standby.reservation('cancel',{id:afterReconciliation});
  assert.equal((await test.db.collection('_ha_reservations').findOne({_id:uncertain})).state,'dispatched','Reconciliation of one intent cannot refund another unresolved call');
  // Exercise the actual pinned proxyFetch and cost hook without contacting a
  // public host. An injected native transport is the only permitted egress;
  // the ordinary Undici retry/dispatcher path must never be used in this scope.
  const fetchBefore = globalThis.fetch, connectBefore = Socket.prototype.connect;
  let sent = 0;
  try {
    Object.assign(process.env, { DATA_DIR: path.resolve('.work/quota-data'), HTTP_PROXY: '', HTTPS_PROXY: '', ALL_PROXY: '', http_proxy: '', https_proxy: '', all_proxy: '', TLS_FINGERPRINT_ENABLED: 'false' });
    Socket.prototype.connect = () => { throw new Error('Unexpected network in quota transport test'); };
    globalThis.fetch = async (_input, options) => {
      if (!globalThis.__omnirouteHardQuota?.current()) return new Response('', { status: 503 });
      sent++; assert.equal(options.redirect, 'error'); assert.ok(options.signal); return Response.json({ ok: true });
    };
    const quota = installQuotaRuntime(standby, { assertOwner() { assert.equal(standby.canWrite(), true); } }, globalThis.fetch);
    globalThis.__omnirouteHardQuota = quota;
    globalThis.__omnirouteMongoAdapter = standby;
    await import('../adapter/register.mjs');
    const { proxyFetch } = await import('../upstream/omniroute/open-sse/utils/proxyFetch.ts');
    const { recordChatCallCost,recordCost, getBudgetWindow: originalCalendar } = await import('../upstream/omniroute/src/domain/costRules.ts');
    const {recordStreamingCost}=await import('../upstream/omniroute/open-sse/handlers/chatCore/streamingCost.ts');
    const {calculateCostDetailed}=await import('../upstream/omniroute/src/lib/usage/costCalculator.ts');
    const {getApiKeyUsageLimitStatus}=await import('../upstream/omniroute/src/lib/usage/apiKeyUsageLimits.ts');
    const {getProviderLimitsCache,getAllProviderLimitsCache}=await import('../upstream/omniroute/src/lib/db/providerLimits.ts');
    for(const fixture of windowCases) {
      const intent=standby.prepare('SELECT id FROM api_keys WHERE id=?').get(fixture.keyId);
      assert.ok(intent);
      const stored=await test.db.collection('_ha_reservations').findOne({_id:fixture.id});
      const oldMode=process.env.OMNIROUTE_API_KEY_WEEKLY_WINDOW,oldZone=process.env.OMNIROUTE_API_KEY_WEEKLY_WINDOW_TIMEZONE;
      try {
        process.env.OMNIROUTE_API_KEY_WEEKLY_WINDOW=fixture.mode; process.env.OMNIROUTE_API_KEY_WEEKLY_WINDOW_TIMEZONE=fixture.zone;
        const status=await getApiKeyUsageLimitStatus({id:fixture.keyId,allowedConnections:['subscription-conn'],usageLimitEnabled:true,dailyUsageLimitUsd:0.15,weeklyUsageLimitUsd:0.15},
          {now:()=>stored.createdAt,getProviderConnectionById:async id=>({id,provider:'synthetic-subscription',isActive:true}),getProviderConnections:async()=>[{id:'subscription-conn',provider:'synthetic-subscription',isActive:true}],getProviderLimitsCache,getAllProviderLimitsCache});
        assert.equal(Date.parse(status.dailyWindowStartIso),stored.usageWindows.daily); assert.equal(Date.parse(status.weeklyWindowStartIso),stored.usageWindows.weekly);
      } finally {for(const[name,value]of[['OMNIROUTE_API_KEY_WEEKLY_WINDOW',oldMode],['OMNIROUTE_API_KEY_WEEKLY_WINDOW_TIMEZONE',oldZone]]) if(value===undefined)delete process.env[name];else process.env[name]=value;}
    }
    for (const time of [Date.UTC(2024,1,29,5,29),Date.UTC(2024,2,1),Date.UTC(2026,10,9,5,30),Date.UTC(2026,11,31,23,59)]) {
      for (const interval of ['daily','weekly','monthly']) for (const reset of ['00:00','05:30',' 99:99 ','invalid']) {
        assert.deepEqual(getBudgetWindow(interval,reset,time),originalCalendar(interval,reset,time));
      }
    }
    const args = { apiKeyInfo: { id: 'usd-only' }, modelInfo: { provider: 'openai-compatible-test', model: 'model' }, body: { messages: [{ role: 'user', content: 'text' }], stream: false, max_tokens: 5 } };
    const defaultArgs={...args,apiKeyInfo:{id:'default-cap'},modelInfo:{provider:'openai-compatible-test',model:'defaults'},body:{messages:[{role:'user',content:'Synthetic bounded default'}]}};
    const applyDefault=expected=>async value=>{
      assert.equal(value.body.max_tokens??value.body.max_completion_tokens,expected);
      assert.equal(value.body.stream,false,'Original OpenAI default must remain JSON when stream is omitted');
      await quota.fetch('https://synthetic-provider.invalid',{method:'POST',body:JSON.stringify({...value.body,model:value.modelInfo.model})});
      recordChatCallCost(value.apiKeyInfo,0.01,{provider:'openai-compatible-test',model:'defaults',tokens:{prompt_tokens:2,completion_tokens:1}},true);
      return{success:true,response:Response.json({ok:true})};
    };
    await assert.rejects(quota.run({...args,body:defaultArgs.body},applyDefault(5)),{code:'HA_RESERVATION_REQUIRED'});
    assert.equal(standby.compatibilityFault,undefined,'Request validation must not poison storage: '+standby.compatibilityFault);
    await quota.run(defaultArgs,applyDefault(5));assert.equal(defaultArgs.body.max_tokens,undefined,'Operator default must not mutate the original request object');
    await quota.run({...defaultArgs,body:{...defaultArgs.body,max_completion_tokens:2}},applyDefault(2));
    assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:'default-cap',state:'settled'}),2);
    console.log('PASS certified default output cap: missing cap still refuses without policy; optional default is actually forwarded, explicit cap wins, original body remains unchanged and both provider receipts settle durably (synthetic transport)');
    const {openaiResponsesToOpenAIRequest}=await import('../upstream/omniroute/open-sse/translator/request/openai-responses.ts');
    const responsesArgs={...args,apiKeyInfo:{id:'responses-json'},credentials:{providerSpecificData:{apiType:'chat'}},clientRawRequest:{endpoint:'/v1/responses'},body:{input:'Synthetic Responses turn',stream:false,max_output_tokens:5}};
    const chatWire=value=>({...openaiResponsesToOpenAIRequest('model',value.body,value.body.stream,value.credentials),model:'model'});
    let badWireCalls=0;
    for(const mutation of [wire=>{delete wire.max_tokens;},wire=>{wire.max_tokens=6;},wire=>{wire.model='uncertified';}]){
      const before=sent;
      await assert.rejects(quota.run(responsesArgs,async value=>{badWireCalls++;const wire=chatWire(value);mutation(wire);await quota.fetch('https://synthetic-provider.invalid',{method:'POST',body:JSON.stringify(wire)});}),{code:'HA_RESERVATION_REQUIRED'});
      assert.equal(sent,before,'Unbounded or changed Responses wire must refuse before dispatch');
    }
    assert.equal(badWireCalls,3);assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:'responses-json',state:'dispatched'}),0);
    await quota.run(responsesArgs,async value=>{
      const wire=chatWire(value);assert.equal(wire.max_tokens,5);assert.ok(wire.messages.length);assert.equal(wire.input,undefined);
      await quota.fetch('https://synthetic-provider.invalid',{method:'POST',body:JSON.stringify(wire)});
      recordChatCallCost(value.apiKeyInfo,0.01,{provider:'openai-compatible-test',model:'model',tokens:{prompt_tokens:2,completion_tokens:1}},true);
      return{success:true,response:Response.json({ok:true})};
    });
    assert.equal(responsesArgs.body.max_tokens,undefined);assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:'responses-json',state:'settled'}),1);
    await assert.rejects(quota.run({...responsesArgs,body:{...responsesArgs.body,max_tokens:6}},()=>assert.fail('Conflicting output caps dispatched')),{code:'HA_RESERVATION_REQUIRED'});
    console.log('PASS bounded Responses JSON: original request converter, translated Chat wire ceiling/model checks, untouched input, pre-dispatch refusal/cancellation and durable original cost-hook settlement (synthetic transport)');
    for(const mutation of [wire=>{delete wire.max_tokens;},wire=>{wire.max_tokens=6;},wire=>{wire.n=2;},wire=>{wire.model='uncertified';},wire=>{wire.stream=true;},wire=>{wire.max_output_tokens=1000;}]){
      const before=sent;
      await assert.rejects(quota.run({...args,apiKeyInfo:{id:'responses-json'}},async value=>{
        const wire={...value.body,model:'model'};mutation(wire);
        await quota.fetch('https://synthetic-provider.invalid',{method:'POST',body:JSON.stringify(wire)});
      }),{code:'HA_RESERVATION_REQUIRED'});
      assert.equal(sent,before,'Chat payload rules must not bypass final provider liability validation');
    }
    console.log('PASS final Chat wire guard: removed/increased/ambiguous caps, multiple choices, changed model and changed stream all refuse before provider dispatch; original policy remains usable');
    sent=0;
    await quota.run(args, async () => {
      await proxyFetch('https://synthetic-provider.invalid/v1/chat/completions', { method: 'POST', body: JSON.stringify({...args.body,model:'model'}) }, { undiciFetch: () => { throw new Error('Unsafe Undici bypass'); } });
      recordChatCallCost(args.apiKeyInfo, 0.01, { provider: 'openai-compatible-test', model: 'model', requestId: 'synthetic-original-request',
        tokens: { prompt_tokens: 2, completion_tokens: 1, completion_tokens_details: { reasoning_tokens: 2 } } }, true);
      assert.throws(() => quota.fetch('https://synthetic-provider.invalid',{method:'POST',body:JSON.stringify({...args.body,model:'model'})}), { code: 'HA_RESERVATION_CONFLICT' });
    });
    assert.equal(sent, 1);
    assert.equal(await test.db.collection('_ha_reservations').countDocuments({ keyId: 'usd-only', state: 'settled' }), 1);
    const reasoningLedger = await test.db.collection('request_cost_ledger').findOne({ api_key_id: 'usd-only' });
    assert.equal(reasoningLedger.tokens_reasoning, 2); assert.equal(reasoningLedger.request_id, 'synthetic-original-request');
    assert.equal((await test.db.collection('api_key_quota_counters').findOne({ api_key_id: 'usd-only' })).consumed, 5);
    await assert.rejects(quota.run(args, () => proxyFetch('https://synthetic-provider.invalid', { dispatcher: {} })), /direct egress/);
    assert.equal(sent, 1, 'Unsupported dispatcher must not send a request');
    console.log('PASS actual proxyFetch/cost hooks: bounded non-loopback path, native single dispatch, no Undici bypass/replay/redirect, durable settlement and unsupported dispatcher rejection (synthetic transport)');
    let mode='complete',streamSends=0,cancelled=0;
    const encoded=new TextEncoder(),event=value=>'data: '+JSON.stringify(value)+'\n\n';
    const usage={prompt_tokens:2,completion_tokens:1,total_tokens:3,cost_usd:0.01};
    const chunk=event({choices:[{index:0,delta:{content:'سلام'},finish_reason:null}]});
    const terminal=event({choices:[{index:0,delta:{},finish_reason:'stop'}],usage});
    const streamQuota=installQuotaRuntime(standby,{assertOwner(){assert.equal(standby.canWrite(),true);}},async()=>{
      streamSends++;return new Response(new ReadableStream({start(controller){
        if(mode==='missing')controller.enqueue(encoded.encode(event({choices:[],usage:{prompt_tokens:0,completion_tokens:0,cost_usd:0}})));
        const bytes=encoded.encode(mode==='tools'?event({choices:[{index:0,delta:{tool_calls:[{index:0,id:'synthetic-call',type:'function',function:{name:'lookup',arguments:'{}'}}]},finish_reason:null}]}):chunk);controller.enqueue(bytes.slice(0,13));controller.enqueue(bytes.slice(13));
        if(mode!=='cancel'){controller.enqueue(encoded.encode(mode==='tools'?event({choices:[{index:0,delta:{},finish_reason:'tool_calls'}],usage}):mode==='complete'?terminal:event({choices:[{index:0,delta:{},finish_reason:'stop'}]})));controller.enqueue(encoded.encode('data: [DONE]\n\n'));controller.close();}
      },cancel(){cancelled++;}}),{headers:{'content-type':'text/event-stream'}});
    });
    globalThis.__omnirouteHardQuota=streamQuota;
    const streamingArgs=id=>({...args,apiKeyInfo:{id},credentials:{providerSpecificData:{apiType:'chat'}},body:{...args.body,stream:true}});
    const executeStream=async value=>{
      assert.equal(value.body.stream_options.include_usage,true);
      const response=await streamQuota.fetch('https://synthetic-provider.invalid',{method:'POST',body:JSON.stringify({...value.body,model:value.modelInfo.model})});
      // Reuse the actual original cost hook in the stream callback too. The
      // transport receipt must prevent a second/estimated budget charge.
      const onComplete=streamQuota.bind(()=>recordStreamingCost({apiKeyId:value.apiKeyInfo.id,provider:'openai-compatible-test',model:'model',streamUsage:usage,calculateCost:calculateCostDetailed,recordCost}));
      return {success:true,onComplete,response:new Response(response.body.pipeThrough(new TransformStream({flush:onComplete})))};
    };
    const implicitStream={...streamingArgs('stream-ok'),clientRawRequest:{headers:{Accept:'text/event-stream'}},body:{...args.body}};
    delete implicitStream.body.stream;
    const streamResult=await streamQuota.run(implicitStream,executeStream);
    assert.match(await streamResult.response.text(),/\[DONE\]/);
    streamResult.onComplete(); // Completion may be invoked outside the caller's ALS context.
    await new Promise(resolve=>setImmediate(resolve)); // The original streaming cost helper resolves price asynchronously.
    assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:'stream-ok',state:'settled'}),1);
    assert.equal(await test.db.collection('domain_cost_history').countDocuments({api_key_id:'stream-ok'}),1);
    assert.equal((await test.db.collection('api_key_quota_counters').findOne({api_key_id:'stream-ok'})).consumed,3);
    mode='missing';const incomplete=await streamQuota.run(streamingArgs('stream-missing'),executeStream);
    await assert.rejects(incomplete.response.text(),{code:'HA_QUOTA_UNCERTAIN'});
    assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:'stream-missing',state:'dispatched'}),1);
    assert.equal(await test.db.collection('domain_cost_history').countDocuments({api_key_id:'stream-missing'}),0);
    await assert.rejects(streamQuota.run(streamingArgs('stream-missing'),executeStream),{code:'HA_QUOTA_EXCEEDED'});
    mode='cancel';const abandoned=await streamQuota.run(streamingArgs('stream-cancel'),executeStream),reader=abandoned.response.body.getReader();
    assert.equal((await reader.read()).done,false);await reader.cancel();
    await new Promise(resolve=>setImmediate(resolve));assert.equal(cancelled,1);
    assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:'stream-cancel',state:'dispatched'}),1);
    await assert.rejects(streamQuota.run(streamingArgs('stream-cancel'),executeStream),{code:'HA_QUOTA_EXCEEDED'});assert.equal(streamSends,3);
    console.log('PASS reserved SSE transport: original parser/cost hooks, live split UTF-8 frames, committed exact receipt before DONE, one charge/counter, missing usage and cancellation retain liabilities and block another dispatch (synthetic transport, real MongoDB)');
    mode='complete';
    const responseStream=await streamQuota.run({...responsesArgs,apiKeyInfo:{id:'responses-stream'},clientRawRequest:{endpoint:'/v1/responses',headers:{Accept:'text/event-stream'}},body:{input:'Synthetic Responses stream',max_output_tokens:5}},async value=>{
      assert.equal(value.body.stream,true,'Preserve the original Responses Accept streaming opt-in');
      const response=await streamQuota.fetch('https://synthetic-provider.invalid',{method:'POST',body:JSON.stringify(chatWire(value))});
      return{success:true,response};
    });
    assert.match(await responseStream.response.text(),/\[DONE\]/);assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:'responses-stream',state:'settled'}),1);
    console.log('PASS bounded Responses SSE: original Accept streaming opt-in, Chat-wire cap and committed raw-provider receipt before terminal completion (synthetic transport)');
    const tools=[{type:'function',function:{name:'lookup',parameters:{type:'object',properties:{}}}}];
    mode='tools';const toolStream=await streamQuota.run({...streamingArgs('tool-stream'),body:{...args.body,stream:true,tools}},executeStream);
    const toolWire=await toolStream.response.text();assert.match(toolWire,/tool_calls/);assert.match(toolWire,/\[DONE\]/);
    assert.equal(await test.db.collection('domain_cost_history').countDocuments({api_key_id:'tool-stream'}),1);
    globalThis.__omnirouteHardQuota=quota;
    const executeJson=async value=>{
      await quota.fetch('https://synthetic-provider.invalid',{method:'POST',body:JSON.stringify({...value.body,model:value.modelInfo.model})});
      recordChatCallCost(value.apiKeyInfo,0.01,{provider:'openai-compatible-test',model:'model',tokens:value.apiKeyInfo.id==='media-unknown'?{prompt_tokens:2,completion_tokens:1}:{prompt_tokens:2,completion_tokens:1,cost_usd:0.01}},true);
      return {success:true,response:Response.json({ok:true})};
    };
    await quota.run({...args,apiKeyInfo:{id:'tool-json'},body:{...args.body,tools,messages:[{role:'assistant',content:null,tool_calls:[{id:'synthetic-call',type:'function',function:{name:'lookup',arguments:'{}'}}]},{role:'tool',tool_call_id:'synthetic-call',content:'synthetic result'}]}},executeJson);
    const media=[{type:'text',text:'Describe'},{type:'image_url',image_url:{url:'data:image/png;base64,c3ludGhldGlj'}},{type:'input_audio',input_audio:{format:'wav',data:'c3ludGhldGlj'}}];
    const mediaArgs=id=>({...args,apiKeyInfo:{id},body:{...args.body,max_tokens:undefined,max_completion_tokens:5,messages:[{role:'user',content:media}]}});
    await quota.run(mediaArgs('media-json'),executeJson);
    assert.equal(await test.db.collection('domain_cost_history').countDocuments({api_key_id:'media-json'}),1);
    await assert.rejects(quota.run(mediaArgs('media-unknown'),executeJson),{code:'HA_QUOTA_UNCERTAIN'});
    assert.equal((await test.db.collection('_ha_reservations').findOne({keyId:'media-unknown'})).state,'uncertain');
    const before=sent;
    await assert.rejects(quota.run({...mediaArgs('media-limit'),body:{...mediaArgs('media-limit').body,messages:[{role:'user',content:[...media,...media,...media]}]}},executeJson),{code:'HA_RESERVATION_REQUIRED'});
    await assert.rejects(quota.run({...args,apiKeyInfo:{id:'tool-disabled'},modelInfo:{provider:'openai-compatible-test',model:'other'},body:{...args.body,tools}},executeJson),{code:'HA_RESERVATION_REQUIRED'});
    assert.equal(sent,before);assert.equal(await test.db.collection('_ha_reservations').countDocuments({keyId:'media-limit'}),0);
    console.log('PASS certified tools/media: actual cost hooks and real MongoDB, function-call SSE completion, JSON tool history, image/audio bounds, exact-price media settlement, unknown-price quarantine and pre-dispatch shape/capability refusal (synthetic transport)');
  } finally {
    globalThis.fetch = fetchBefore; Socket.prototype.connect = connectBefore;
    delete globalThis.__omnirouteHardQuota; delete globalThis.__omnirouteMongoAdapter;
  }
  const legacy='baseline-legacy',legacyLimit='baseline-legacy-limit',legacyAt=standby.serverTime();
  standby.prepare('INSERT INTO api_keys(id,name,key,created_at,usage_limit_enabled,daily_usage_limit_usd,weekly_usage_limit_usd,allowed_connections) VALUES (?,?,?,?,?,?,?,?)')
    .run(legacy,legacy,'synthetic-'+legacy,new Date(legacyAt).toISOString(),1,0.15,0.15,'["subscription-conn"]');
  standby.prepare('INSERT INTO api_key_token_limits(id,api_key_id,scope_type,scope_value,token_limit) VALUES (?,?,?,?,?)').run(legacyLimit,legacy,'global','',150);
  const legacyWindow=String(getBudgetWindow('monthly','00:00',legacyAt).periodStartAt);
  await test.db.collection('api_keys').updateOne({id:legacy},{$set:{_ha_requires_key_baseline:true}});
  assert.throws(()=>reserve(standby,legacy),/Imported key token\/USD history/,'An imported key with empty analytics must not silently certify zero history');
  standby.prepare('INSERT INTO api_key_token_counters(limit_id,window_start,tokens_used) VALUES (?,?,?)').run(legacyLimit,legacyWindow,3);
  standby.prepare('INSERT INTO request_cost_ledger(api_key_id,provider,model,tokens_input,amount_usd,timestamp) VALUES (?,?,?,?,?,?)').run(legacy,'openai-compatible-test','model',5,0.02,new Date(legacyAt).toISOString());
  standby.prepare('INSERT INTO domain_cost_history(api_key_id,cost,timestamp) VALUES (?,?,?)').run(legacy,0.02,legacyAt);
  assert.throws(()=>reserve(standby,legacy),{code:'HA_QUOTA_POLICY'});
  assert.throws(()=>standby.reservation('baselineInspect',{id:randomUUID(),keyId:legacy}),{code:'HA_QUOTA_POLICY'});
  await assert.rejects(cli('baseline-inspect',test.database,legacy,'--offline'),/Stop both applications/);
  standby.ownership('release');
  let draft=JSON.parse((await cli('baseline-inspect',test.database,legacy,'--offline')).stdout);
  assert.equal(draft.scoped[0].minimumTokens,5);assert.equal(draft.usdHistoryVerified,false);
  writeEvidence(draft);await assert.rejects(cli('baseline-apply',test.database,evidenceFile,'--offline'),/Invalid verified baseline evidence/);
  const reviewed=value=>({...value,usdHistoryVerified:true,verifiedBy:'synthetic-provider-auditor',reference:'synthetic-reconciled-invoice',scoped:value.scoped.map(scope=>({...scope,tokensUsed:5}))});
  writeEvidence({...reviewed(draft),scoped:[{...draft.scoped[0],tokensUsed:4}]});
  await assert.rejects(cli('baseline-apply',test.database,evidenceFile,'--offline'),/below known usage/);
  assert.ok(standby.ownership('claim',30000));
  standby.prepare('INSERT INTO domain_cost_history(api_key_id,cost,timestamp) VALUES (?,?,?)').run(legacy,0.001,legacyAt+1);standby.ownership('release');
  writeEvidence(reviewed(draft));await assert.rejects(cli('baseline-apply',test.database,evidenceFile,'--offline'),/source changed/);
  draft=JSON.parse((await cli('baseline-inspect',test.database,legacy,'--offline')).stdout);const verified=reviewed(draft);writeEvidence(verified);
  const admin=new MongoAdapter({...config,ownerId:'offline-reconciliation-'+randomUUID(),allowReconciliation:true});
  try{
    assert.ok(admin.ownership('claim',30000));admin.begin();
    assert.equal(admin.reservation('baselineApply',{id:verified.id,evidence:verified,evidenceSha256:createHash('sha256').update(readFileSync(evidenceFile)).digest('hex')}).baselined,true);
    assert.equal(await test.db.collection('_ha_quota_baseline_audits').countDocuments({_id:verified.id}),0,'Uncommitted baseline must remain invisible');
    admin.rollback();assert.equal(await test.db.collection('_ha_key_usage_profiles').countDocuments({_id:legacy}),0);
    assert.equal((await test.db.collection('api_keys').findOne({id:legacy}))._ha_requires_key_baseline,true,'Rollback must retain the imported-key review fence');
    assert.equal((await test.db.collection('api_key_token_counters').findOne({limit_id:legacyLimit})).tokens_used,3);
    admin.ownership('release');
  }finally{admin.close();}
  assert.equal(JSON.parse((await cli('baseline-apply',test.database,evidenceFile,'--offline')).stdout).baselined,true);
  assert.ok(standby.ownership('claim',30000));const legacyCall=reserve(standby,legacy);standby.reservation('dispatch',{id:legacyCall});
  standby.reservation('settle',{id:legacyCall,receipt:{input:10,output:0,tokens:10,nanoUsd:10000000,priced:true}});
  assert.equal((await test.db.collection('api_key_token_counters').findOne({limit_id:legacyLimit})).tokens_used,15);standby.ownership('release');
  assert.equal(JSON.parse((await cli('baseline-apply',test.database,evidenceFile,'--offline')).stdout).idempotent,true);
  assert.equal((await test.db.collection('api_key_token_counters').findOne({limit_id:legacyLimit})).tokens_used,15,'Repeated cutover cannot reset later consumption');
  assert.equal(await test.db.collection('_ha_quota_baseline_audits').countDocuments({_id:verified.id}),1);
  assert.equal((await test.db.collection('_ha_reservations').findOne({_id:uncertain})).state,'dispatched');
  console.log('PASS offline legacy key/scoped baseline: actual CLI, active-owner refusal, reviewed source fingerprint, conservative known-token floor, atomic rollback, audited apply, live admission/settlement and repeat without counter reset; unrelated liability preserved');
  // Team history is reviewed separately from each key's USD/scoped policies.
  assert.ok(standby.ownership('claim',30000));
  assert.throws(()=>standby.reservation('teamBaselineInspect',{id:randomUUID(),teamId:'legacy-team'}),{code:'HA_QUOTA_POLICY'});
  await assert.rejects(cli('team-inspect',test.database,'legacy-team','--offline'),/Stop both applications/);
  standby.ownership('release');
  let teamDraft=JSON.parse((await cli('team-inspect',test.database,'legacy-team','--offline')).stdout);
  assert.equal(teamDraft.summary.minimumNanoUsd,10000000);
  writeEvidence(teamDraft);await assert.rejects(cli('team-apply',test.database,evidenceFile,'--offline'),/Invalid verified Team baseline/);
  const reviewTeam=value=>({...value,usdHistoryVerified:true,verifiedBy:'synthetic-provider-auditor',reference:'synthetic-complete-Team-invoice',totalNanoUsd:40000000});
  writeEvidence({...reviewTeam(teamDraft),totalNanoUsd:1});await assert.rejects(cli('team-apply',test.database,evidenceFile,'--offline'),/below known usage/);
  assert.ok(standby.ownership('claim',30000));
  standby.prepare('UPDATE teams SET name=? WHERE id=?').run('Reviewed historical Team','legacy-team');standby.ownership('release');
  writeEvidence(reviewTeam(teamDraft));await assert.rejects(cli('team-apply',test.database,evidenceFile,'--offline'),/source changed/);
  teamDraft=JSON.parse((await cli('team-inspect',test.database,'legacy-team','--offline')).stdout);
  const teamEvidence=reviewTeam(teamDraft);writeEvidence(teamEvidence);
  const teamAdmin=new MongoAdapter({...config,ownerId:'offline-reconciliation-'+randomUUID(),allowReconciliation:true});
  try{
    assert.ok(teamAdmin.ownership('claim',30000));teamAdmin.begin();
    assert.equal(teamAdmin.reservation('teamBaselineApply',{id:teamEvidence.id,evidence:teamEvidence,evidenceSha256:createHash('sha256').update(readFileSync(evidenceFile)).digest('hex')}).baselined,true);
    assert.equal(await test.db.collection('_ha_team_profiles').countDocuments({_id:'legacy-team'}),0);
    teamAdmin.rollback();assert.equal(await test.db.collection('_ha_team_memberships').countDocuments({teamId:'legacy-team'}),0);teamAdmin.ownership('release');
  }finally{teamAdmin.close();}
  assert.equal(JSON.parse((await cli('team-apply',test.database,evidenceFile,'--offline')).stdout).baselined,true);
  assert.ok(standby.ownership('claim',30000));
  const legacyTeamCall=reserve(standby,'team-legacy');assert.throws(()=>reserve(standby,'team-legacy'),{code:'HA_QUOTA_EXCEEDED'});
  standby.reservation('dispatch',{id:legacyTeamCall});standby.reservation('settle',{id:legacyTeamCall,receipt:{input:10,output:0,tokens:10,nanoUsd:20000000,priced:true}});
  assert.throws(()=>reserve(standby,'team-legacy'),{code:'HA_QUOTA_EXCEEDED'},'Historical baseline and new receipt must both remain charged');
  standby.ownership('release');
  assert.equal(JSON.parse((await cli('team-apply',test.database,evidenceFile,'--offline')).stdout).idempotent,true);
  assert.ok(standby.ownership('claim',30000));assert.throws(()=>reserve(standby,'team-legacy'),{code:'HA_QUOTA_EXCEEDED'});
  const teamReset=(await test.db.collection('teams').findOne({id:'legacy-team'})).budget_reset_at;
  standby.prepare('UPDATE teams SET budget_reset_at=? WHERE id=?').run(new Date(Date.parse(teamReset)+3600000).toISOString(),'legacy-team');
  assert.throws(()=>reserve(standby,'team-legacy'),{code:'HA_QUOTA_POLICY'},'A shifted window cannot reuse a partial historical baseline');
  standby.prepare('UPDATE teams SET budget_reset_at=? WHERE id=?').run(teamReset,'legacy-team');standby.ownership('release');
  // A second verified cutover includes native receipts, without counting them twice.
  const nextTeamDraft=JSON.parse((await cli('team-inspect',test.database,'legacy-team','--offline')).stdout);
  assert.equal(nextTeamDraft.summary.minimumNanoUsd,60000000);assert.equal(nextTeamDraft.summary.nativeSettledNanoUsd,20000000);
  writeEvidence({...reviewTeam(nextTeamDraft),totalNanoUsd:60000000});
  assert.equal(JSON.parse((await cli('team-apply',test.database,evidenceFile,'--offline')).stdout).baselined,true);
  assert.ok(standby.ownership('claim',30000));standby.prepare('UPDATE teams SET max_budget_usd=0.17 WHERE id=?').run('legacy-team');
  const afterRebaseline=reserve(standby,'team-legacy');standby.reservation('cancel',{id:afterRebaseline});
  assert.equal((await test.db.collection('_ha_team_profiles').findOne({_id:'legacy-team'})).legacyNanoUsd,40000000);
  standby.begin();standby.prepare('UPDATE teams SET max_budget_usd=NULL,budget_duration=NULL,budget_reset_at=NULL WHERE id=?').run('legacy-team');
  assert.equal(standby.checkQuotaPolicy('team-legacy').managed,false);standby.rollback();
  assert.equal((await test.db.collection('_ha_team_profiles').findOne({_id:'legacy-team'})).complete,true,'Rolled-back budget edits must not invalidate the baseline');
  standby.prepare('UPDATE teams SET max_budget_usd=NULL,budget_duration=NULL,budget_reset_at=NULL WHERE id=?').run('legacy-team');
  assert.equal(standby.checkQuotaPolicy('team-legacy').managed,false);
  assert.equal((await test.db.collection('_ha_team_profiles').findOne({_id:'legacy-team'})).complete,false,'Disabled budget must invalidate complete receipt tracking without clearing history');
  assert.equal((await test.db.collection('_ha_team_profiles').findOne({_id:'legacy-team'})).legacyNanoUsd,40000000);
  standby.prepare('UPDATE teams SET max_budget_usd=0.17,budget_duration=?,budget_reset_at=? WHERE id=?').run('1d',teamReset,'legacy-team');
  assert.throws(()=>reserve(standby,'team-legacy'),{code:'HA_QUOTA_POLICY'},'Reenabled Team budget requires review of untracked usage');
  assert.throws(()=>reserve(standby,'team-a'),{code:'HA_QUOTA_UNCERTAIN'},'Review must not clear unrelated Team uncertainty');
  console.log('PASS offline legacy Team baseline: actual CLI, reviewed membership/source fingerprint, known-cost floor, atomic rollback, historical-plus-native admission, idempotence, calendar/disabled-period refusal and repeat cutover without double charging or clearing liability');
  console.log('PASS durable token/USD reservation: atomic admission, no over-admission, single dispatch, cancellation boundary, idempotent receipt and ledger/counter settlement');
  console.log('PASS calendar USD budgets: official pinned UTC boundaries including leap/year/week/reset normalization, daily/weekly/monthly admission, spend/unpriced refusal and liability preserved across promotion');
  console.log('PASS key USD usage: durable holds/spend, empty-history baseline guard, provider-observed reset/drop and IANA calendar windows match actual upstream status logic');
  console.log('PASS scoped token reservations: global/provider/model isolation, pending holds, atomic counter rollback/settlement, disabled-period tracking and unverified legacy baseline refusal');
  console.log('PASS reservation promotion: stale epoch fenced, abandoned undispatched intent refunded, dispatched uncertainty retained, conflicting receipt quarantined');
  console.log('PASS actual offline quota recovery CLI: active-owner refusal, provider identity evidence check, audited atomic settlement, idempotence, individual quarantine recovery and unrelated hold preservation');
} finally { bootstrap.close(); active?.close(); standby?.close(); calendarActor?.close(); await test.cleanup(); }
