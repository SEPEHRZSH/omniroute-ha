import assert from 'node:assert/strict';
import { readFileSync,mkdtempSync,writeFileSync,rmSync } from 'node:fs';
import { randomUUID,createHash } from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';
import {windows} from '../adapter/poolQuota.mjs';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { installBaseSchema } from '../adapter/baseSchema.mjs';
import { splitStatements, parse } from '../adapter/sql.mjs';
import { testDatabase } from './mongoTestSupport.mjs';
const test = await testDatabase('poolquota');
const config = { uri: test.uri, database: test.database, quotaBounds: {
  'openai-compatible-test/model': { tokens: 100, nanoUsd: 100000000 },
  'openai-compatible-test/other': { tokens: 100, nanoUsd: 100000000 },
  'openai-compatible-test/unreviewed': {tokens:100,nanoUsd:100000000},
} };
const bootstrap = new MongoAdapter(config);
let active, standby, strict,baselineAdmin,evidenceDirectory;
try {
  installBaseSchema(bootstrap);
  for (const name of ['078_quota_consumption','079_provider_plans','085_quota_pools','087_quota_pool_connections','106_quota_allocation_model_caps','189_quota_schedules','182_request_cost_ledger_and_key_quota']) {
    for (const sql of splitStatements(readFileSync(`upstream/omniroute/src/lib/db/migrations/${name}.sql`, 'utf8'))) {
      if (name !== '087_quota_pool_connections' || parse(sql).ast.type === 'create') bootstrap.exec(sql);
    }
  }
  for (const id of ['alpha','beta','old-unused']) bootstrap.prepare('INSERT INTO api_keys(id,name,key,created_at) VALUES (?,?,?,?)').run(id, id, 'synthetic-' + id, new Date().toISOString());
  bootstrap.prepare('INSERT INTO quota_pools(id,connection_id,name) VALUES (?,?,?)').run('pool', 'primary-connection', 'Synthetic pool');
  for (const id of ['primary-connection','secondary-connection']) bootstrap.prepare('INSERT INTO quota_pool_connections(pool_id,connection_id) VALUES (?,?)').run('pool', id);
  for (const id of ['alpha','beta']) bootstrap.prepare('INSERT INTO quota_allocations(pool_id,api_key_id,weight,policy) VALUES (?,?,?,?)').run('pool', id, 50, 'hard');
  bootstrap.prepare('INSERT INTO provider_plans(connection_id,provider,dimensions_json) VALUES (?,?,?)').run('primary-connection','openai-compatible-test',JSON.stringify([{unit:'tokens',window:'daily',limit:150}]));
  bootstrap.installRequestLimits(); bootstrap.close();
  active = new MongoAdapter({ ...config, ownerId: 'pool-active' });
  standby = new MongoAdapter({ ...config, ownerId: 'pool-standby' });
  active.ownership('claim', 30000);
  const reserve = (keyId, model = 'model', db = active) => { const id = randomUUID(); db.reservation('reserve', { id, keyId, model, provider: 'openai-compatible-test', connectionId: 'secondary-connection' }); return id; };
  const cancel = id => active.reservation('cancel', { id });
  const receipt = (tokens, nanoUsd = 10000000) => ({ input: tokens, output: 0, tokens, nanoUsd, priced: true });
  const settle = (id, tokens, nanoUsd) => { active.reservation('dispatch', { id }); return active.reservation('settle', { id, receipt: receipt(tokens, nanoUsd) }); };
  assert.equal(active.checkQuotaPolicy('alpha').managed, true);
  const first = reserve('alpha'), second = reserve('beta'), third = reserve('alpha');
  assert.throws(() => reserve('beta'), { code: 'HA_QUOTA_EXCEEDED' });
  cancel(third); assert.equal(settle(first, 50).settled, true); cancel(second);
  assert.equal(active.reservation('settle', { id: first, receipt: receipt(50) }).settled, true);
  assert.equal((await test.db.collection('quota_consumption').findOne({ api_key_id: 'alpha', dimension_key: 'pool:tokens:weekly' })).consumed, 50);
  active.prepare('UPDATE quota_allocations SET cap_value=?,cap_unit=? WHERE pool_id=? AND api_key_id=?').run(160,'tokens','pool','alpha');
  const absolute = reserve('alpha');
  assert.throws(() => reserve('alpha'), { code: 'HA_QUOTA_EXCEEDED' });
  assert.equal(settle(absolute,100,30000000).settled,true);
  const independent = reserve('beta'); cancel(independent);
  active.prepare('INSERT INTO quota_allocation_model_caps(pool_id,api_key_id,model,cap_value,cap_unit) VALUES (?,?,?,?,?)').run('pool','beta','model',50,'tokens');
  assert.throws(() => reserve('beta'), { code: 'HA_QUOTA_EXCEEDED' });
  const other = reserve('beta','other'); cancel(other);
  active.prepare('UPDATE quota_allocations SET cap_value=NULL,cap_unit=NULL WHERE pool_id=? AND api_key_id=?').run('pool','alpha');
  active.prepare('INSERT INTO quota_allocation_model_caps(pool_id,api_key_id,model,cap_value,cap_unit) VALUES (?,?,?,?,?)').run('pool','alpha','model',180,'tokens');
  assert.throws(() => reserve('alpha'), { code: 'HA_QUOTA_EXCEEDED' }, 'Enabling a model cap must retain already consumed model tokens');
  active.prepare('UPDATE provider_plans SET dimensions_json=? WHERE connection_id=?').run(JSON.stringify([{unit:'tokens',window:'weekly',limit:70}]),'primary-connection');
  assert.throws(() => reserve('beta','other'), { code: 'HA_QUOTA_EXCEEDED' }, 'Changing the plan window must retain earlier usage');
  active.prepare('UPDATE provider_plans SET dimensions_json=? WHERE connection_id=?').run(JSON.stringify([{unit:'tokens',window:'daily',limit:150}]),'primary-connection');
  active.prepare('INSERT INTO quota_schedules(id,pool_id,days,start_minute,end_minute,timezone,budget_value,budget_unit) VALUES (?,?,?,?,?,?,?,?)')
    .run('schedule','pool','0,1,2,3,4,5,6',0,1440,'UTC',0.12,'usd');
  const scheduled = reserve('beta','other');
  assert.throws(() => reserve('beta','other'), { code:'HA_QUOTA_EXCEEDED' });
  cancel(scheduled);
  active.prepare('UPDATE quota_schedules SET mode=? WHERE id=?').run('block','schedule');
  assert.throws(() => reserve('beta','other'), { code:'HA_QUOTA_EXCEEDED' });
  active.prepare('UPDATE quota_schedules SET mode=?,reserves=? WHERE id=?').run('allow','[{"window":"weekly","percent":20}]','schedule');
  assert.throws(() => reserve('beta','other'), { code:'HA_RESERVATION_REQUIRED' });
  active.prepare('UPDATE quota_schedules SET reserves=NULL WHERE id=?').run('schedule');
  const rollback = reserve('beta','other'); active.reservation('dispatch', { id: rollback });
  const before = await test.db.collection('domain_cost_history').countDocuments();
  active.begin(); active.reservation('settle', { id: rollback, receipt: receipt(50) }); active.rollback();
  assert.equal(await test.db.collection('domain_cost_history').countDocuments(),before);
  assert.equal(await test.db.collection('quota_consumption').countDocuments({ api_key_id: 'beta', dimension_key: 'pool:sched:schedule:usd:weekly' }), 0, 'Native bulk settlement must roll back with spend and reservation state');
  assert.equal((await test.db.collection('_ha_reservations').findOne({_id:rollback})).state,'dispatched');
  active.ownership('release'); standby.ownership('claim',30000);
  assert.throws(() => active.reservation('settle',{id:rollback,receipt:receipt(50)}),{code:'HA_FENCED'});
  assert.throws(() => reserve('beta','other',standby), {code:'HA_QUOTA_EXCEEDED'}, 'Dispatched schedule liability must survive promotion');
  assert.equal(standby.reservation('settle',{id:rollback,receipt:receipt(50)}).settled,true);
  assert.equal(await test.db.collection('domain_cost_history').countDocuments(),before+1);
  assert.equal((await test.db.collection('quota_consumption').findOne({api_key_id:'beta',dimension_key:'pool:sched:schedule:usd:weekly'})).consumed,0.01);
  const future = reserve('beta','other',standby); standby.reservation('cancel',{id:future});
  standby.prepare('INSERT INTO quota_pools(id,connection_id,name) VALUES (?,?,?)').run('legacy-pool','legacy-connection','Synthetic legacy pool');
  standby.prepare('INSERT INTO quota_allocations(pool_id,api_key_id,weight,policy) VALUES (?,?,?,?)').run('legacy-pool','alpha',100,'hard');
  standby.prepare('INSERT INTO provider_plans(connection_id,provider,dimensions_json) VALUES (?,?,?)').run('legacy-connection','openai-compatible-test',JSON.stringify([{unit:'tokens',window:'daily',limit:1000}]));
  standby.prepare('INSERT INTO quota_consumption(api_key_id,dimension_key,bucket_index,consumed,updated_at) VALUES (?,?,?,?,?)')
    .run('alpha','legacy-pool:tokens:daily',Math.floor(standby.serverTime()/86400000),10,standby.serverTime());
  assert.throws(() => standby.reservation('reserve', {id:randomUUID(),keyId:'alpha',connectionId:'legacy-connection',provider:'openai-compatible-test',model:'other'}),
    {code:'HA_QUOTA_POLICY'}, 'Legacy pool counters must not masquerade as a complete migration baseline');
  standby.prepare('INSERT INTO quota_pools(id,connection_id,name) VALUES (?,?,?)').run('precision-pool','precision-connection','Synthetic precision pool');
  standby.prepare('INSERT INTO quota_allocations(pool_id,api_key_id,weight,policy) VALUES (?,?,?,?)').run('precision-pool','beta',100,'hard');
  standby.prepare('INSERT INTO provider_plans(connection_id,provider,dimensions_json) VALUES (?,?,?)').run('precision-connection','openai-compatible-test',JSON.stringify([{unit:'usd',window:'daily',limit:999999.9999}]));
  const preciseReserve=()=>{ const id=randomUUID(); standby.reservation('reserve',{id,keyId:'beta',connectionId:'precision-connection',provider:'openai-compatible-test',model:'other'}); return id; };
  standby.reservation('cancel',{id:preciseReserve()});
  const precisionFilter={api_key_id:'beta',dimension_key:'precision-pool:usd:daily',bucket_index:Math.floor(standby.serverTime()/86400000)};
  // Synthetic accumulated REAL drift: the exact balance, not the smaller
  // floating approximation, must decide admission near a monetary hard cap.
  await test.db.collection('quota_consumption').insertOne({...precisionFilter,consumed:999999.8998389754,_ha_usd_nano:999999900000000,updated_at:standby.serverTime(),_ha_rowid:999999});
  assert.throws(preciseReserve,{code:'HA_QUOTA_EXCEEDED'});
  const updatePrecision=value=>standby.prepare('UPDATE quota_consumption SET consumed=? WHERE api_key_id=? AND dimension_key=? AND bucket_index=?')
    .run(value,precisionFilter.api_key_id,precisionFilter.dimension_key,precisionFilter.bucket_index);
  updatePrecision(0);
  assert.equal((await test.db.collection('quota_consumption').findOne(precisionFilter))._ha_usd_nano,undefined);
  standby.reservation('cancel',{id:preciseReserve()});
  updatePrecision(0.01);
  assert.throws(preciseReserve,{code:'HA_QUOTA_POLICY'},'Positive SQL REAL edits must not preserve or invent an exact balance');
  updatePrecision(0);
  const precise=preciseReserve(); standby.reservation('dispatch',{id:precise});
  const preciseReceipt=receipt(50,30000000);
  assert.equal(standby.reservation('settle',{id:precise,receipt:preciseReceipt}).settled,true);
  assert.equal(standby.reservation('settle',{id:precise,receipt:preciseReceipt}).settled,true);
  const preciseCounter=await test.db.collection('quota_consumption').findOne(precisionFilter);
  assert.equal(preciseCounter._ha_usd_nano,30000000); assert.equal(preciseCounter.consumed,0.03);
  await test.db.collection('_ha_schema').updateOne({ _id: 'quota_consumption' }, { $set: { 'columns.3.type': 'TEXT' } });
  assert.throws(() => reserve('beta','other',standby), { code: 'HA_QUOTA_POLICY' }, 'Native bulk boundary must reject schema drift before dispatch');
  await test.db.collection('_ha_schema').updateOne({ _id: 'quota_consumption' }, { $set: { 'columns.3.type': 'REAL' } });
  const previousThreshold = process.env.QUOTA_SATURATION_THRESHOLD;
  try { process.env.QUOTA_SATURATION_THRESHOLD = '0'; strict = new MongoAdapter({ ...config, ownerId: 'strict-pool-active' }); }
  finally { if (previousThreshold === undefined) delete process.env.QUOTA_SATURATION_THRESHOLD; else process.env.QUOTA_SATURATION_THRESHOLD = previousThreshold; }
  standby.ownership('release'); strict.ownership('claim',30000);
  assert.throws(() => reserve('alpha','other',strict), /Hard fair share/, 'Strict fair share must reserve against the allocation before overshooting it');
  const exactShare = reserve('beta','other',strict); strict.reservation('cancel',{id:exactShare});
  strict.prepare('INSERT INTO quota_allocations(pool_id,api_key_id,weight,policy) VALUES (?,?,?,?)').run('precision-pool','alpha',0,'soft');
  const uncertainPool=randomUUID();
  strict.reservation('reserve',{id:uncertainPool,keyId:'beta',connectionId:'precision-connection',provider:'openai-compatible-test',model:'other'});
  strict.reservation('dispatch',{id:uncertainPool});
  assert.equal(strict.reservation('settle',{id:uncertainPool,receipt:receipt(1,1000000000)}).settled,false);
  assert.throws(()=>strict.reservation('reserve',{id:randomUUID(),keyId:'alpha',connectionId:'precision-connection',provider:'openai-compatible-test',model:'other'}),{code:'HA_QUOTA_UNCERTAIN'},'Unknown cost must quarantine shared capacity across keys');
  assert.equal(active.compatibilityFault,undefined); assert.equal(standby.compatibilityFault,undefined);
  console.log('PASS shared-pool reservations: two keys/two connections, canonical plan scaling, global/allocation/model caps, all-window history, idempotence, schema drift and unverified legacy baseline rejection');
  console.log('PASS pool schedules: official calendar, shared budget holds, blackout, unknown percentage floors fail closed, transaction rollback and liabilities preserved across promotion');
  console.log('PASS exact pool USD: integer nano-dollar admission/settlement/idempotence, REAL drift rejection and SQL edit baseline invalidation');
  console.log('PASS shared uncertainty: invalid provider accounting blocks other keys in the same pool before dispatch');
  strict.prepare('INSERT INTO quota_pools(id,connection_id,name) VALUES (?,?,?)').run('imported-empty-pool','imported-empty-connection','Synthetic imported empty pool');
  strict.prepare('INSERT INTO quota_allocations(pool_id,api_key_id,weight,policy) VALUES (?,?,?,?)').run('imported-empty-pool','alpha',100,'hard');
  await test.db.collection('_ha_pool_profiles').insertOne({_id:'imported-empty-pool',allWindows:false,trackingVersion:0,requiresReview:'sqlite-import'});
  assert.throws(()=>strict.reservation('reserve',{id:randomUUID(),keyId:'alpha',connectionId:'imported-empty-connection',provider:'openai-compatible-test',model:'model'}),/Incomplete or old pool usage baseline/,'Empty imported counters cannot prove zero historical spend');
  strict.prepare('INSERT INTO quota_consumption(api_key_id,dimension_key,bucket_index,consumed,updated_at) VALUES (?,?,?,?,?)').run('alpha','legacy-pool:usd:daily',Math.floor(strict.serverTime()/86400000),0.01,strict.serverTime());
  const cli=async(...args)=>JSON.parse((await promisify(execFile)(process.execPath,['scripts/quota.mjs',...args],{env:{...process.env,HA_MONGO_URI:test.uri},timeout:70000,maxBuffer:1048576})).stdout);
  await assert.rejects(cli('pool-inspect',test.database,'legacy-pool','--offline','--models=model,other'),/Stop both applications/);
  strict.ownership('release');
  baselineAdmin=new MongoAdapter({...config,ownerId:'offline-reconciliation-pool-test',allowReconciliation:true,timeoutMs:60000});assert.ok(baselineAdmin.ownership('claim',60000)?.epoch>=1);
  const inspect=()=>baselineAdmin.reservation('poolBaselineInspect',{id:randomUUID(),poolId:'legacy-pool',models:['model','other']});
  const complete=draft=>({...draft,historyVerified:true,verifiedBy:'synthetic-pool-reviewer',reference:'synthetic-complete-provider-invoice',rows:draft.rows.map(row=>({...row,
    used:row.bucket===Math.floor(Date.parse(draft.observedAt)/windows[row.window])&&(row.namespace==='legacy-pool'||row.namespace==='legacy-pool:model:model')?{requests:1,tokens:10,usd:10000000}[row.unit]:0}))});
  const apply=evidence=>{const bytes=JSON.stringify(evidence);return baselineAdmin.reservation('poolBaselineApply',{id:evidence.id,evidence,evidenceSha256:createHash('sha256').update(bytes).digest('hex')});};
  const stale=complete(inspect());baselineAdmin.prepare('UPDATE quota_pools SET name=? WHERE id=?').run('Synthetic reviewed name','legacy-pool');
  assert.throws(()=>apply(stale),/source or windows changed/);
  const evidence=complete(inspect());
  assert.throws(()=>apply({...evidence,historyVerified:false}),/Invalid verified pool baseline/);
  assert.throws(()=>apply({...evidence,observedAt:new Date(Date.parse(evidence.observedAt)-windows.monthly*2).toISOString()}),/review windows|observation|observed|stale/i);
  assert.throws(()=>apply({...evidence,rows:evidence.rows.slice(1)}),/every current\/previous/);
  const lower=structuredClone(evidence),daily=lower.rows.find(row=>row.namespace==='legacy-pool'&&row.unit==='tokens'&&row.window==='daily'&&row.minimum>0);daily.used=0;
  assert.throws(()=>apply(lower),/below existing accounting/);
  const inconsistent=structuredClone(evidence);for(const row of inconsistent.rows)if(row.window==='weekly')row.used=0;
  assert.throws(()=>apply(inconsistent),/windows contradict/);
  baselineAdmin.begin();assert.equal(apply(evidence).baselined,true);baselineAdmin.rollback();
  assert.equal(await test.db.collection('_ha_pool_profiles').countDocuments({_id:'legacy-pool'}),0);
  assert.equal(await test.db.collection('_ha_quota_baseline_audits').countDocuments({poolId:'legacy-pool'}),0);
  assert.equal((await test.db.collection('quota_consumption').findOne({dimension_key:'legacy-pool:usd:daily'}))._ha_usd_nano,undefined,'Rolled-back review must not publish exact USD metadata');
  baselineAdmin.ownership('release');await baselineAdmin.close();baselineAdmin=undefined;
  const draft=await cli('pool-inspect',test.database,'legacy-pool','--offline','--models=model,other'),reviewed=complete(draft);
  evidenceDirectory=mkdtempSync(path.resolve('.work/pool-baseline-'));const evidenceFile=path.join(evidenceDirectory,'reviewed.json');writeFileSync(evidenceFile,JSON.stringify(reviewed),{mode:0o600});
  const unrelated=await test.db.collection('_ha_reservations').findOne({_id:uncertainPool});
  assert.equal((await cli('pool-apply',test.database,evidenceFile,'--offline')).baselined,true);
  assert.deepEqual(await test.db.collection('_ha_reservations').findOne({_id:uncertainPool}),unrelated,'Pool cutover must not refund or clear another unresolved liability');
  assert.equal((await test.db.collection('quota_consumption').findOne({dimension_key:'legacy-pool:usd:weekly',bucket_index:Math.floor(Date.now()/windows.weekly)}))._ha_usd_nano,10000000);
  assert.ok(strict.ownership('claim',30000)?.epoch>=1);
  const legacyArgs={keyId:'alpha',connectionId:'legacy-connection',provider:'openai-compatible-test',model:'model'};
  const continued=randomUUID();strict.reservation('reserve',{id:continued,...legacyArgs});strict.reservation('dispatch',{id:continued});assert.equal(strict.reservation('settle',{id:continued,receipt:receipt(20,10000000)}).settled,true);
  const globalDaily=()=>test.db.collection('quota_consumption').findOne({api_key_id:'alpha',dimension_key:'legacy-pool:tokens:daily',bucket_index:Math.floor(Date.now()/windows.daily)});
  assert.equal((await globalDaily()).consumed,30);
  strict.prepare('UPDATE provider_plans SET dimensions_json=? WHERE connection_id=?').run(JSON.stringify([{unit:'usd',window:'weekly',limit:0.025}]),'legacy-connection');
  assert.throws(()=>strict.reservation('reserve',{id:randomUUID(),...legacyArgs}),{code:'HA_QUOTA_EXCEEDED'},'A later plan window must retain the reviewed integer-USD history');
  strict.prepare('UPDATE provider_plans SET dimensions_json=? WHERE connection_id=?').run(JSON.stringify([{unit:'tokens',window:'daily',limit:1000}]),'legacy-connection');
  assert.throws(()=>strict.reservation('reserve',{id:randomUUID(),...legacyArgs,model:'unreviewed'}),/model requires a reviewed/);
  strict.prepare('INSERT INTO quota_allocations(pool_id,api_key_id,weight,policy) VALUES (?,?,?,?)').run('legacy-pool','old-unused',0,'soft');
  assert.throws(()=>strict.reservation('reserve',{id:randomUUID(),...legacyArgs,keyId:'old-unused'}),/Pre-cutover key/);
  strict.prepare('INSERT INTO api_keys(id,name,key,created_at) VALUES (?,?,?,?)').run('new-after-review','Synthetic new key','synthetic-post-cutover',new Date().toISOString());
  strict.prepare('INSERT INTO quota_allocations(pool_id,api_key_id,weight,policy) VALUES (?,?,?,?)').run('legacy-pool','new-after-review',0,'soft');
  const newKey=randomUUID();strict.reservation('reserve',{id:newKey,...legacyArgs,keyId:'new-after-review'});strict.reservation('cancel',{id:newKey});
  strict.prepare('INSERT INTO quota_schedules(id,pool_id,days,start_minute,end_minute,timezone,budget_value,budget_unit) VALUES (?,?,?,?,?,?,?,?)').run('unreviewed-legacy-schedule','legacy-pool','0,1,2,3,4,5,6',0,1440,'UTC',1000,'tokens');
  assert.throws(()=>strict.reservation('reserve',{id:randomUUID(),...legacyArgs}),/schedule requires a reviewed/);
  strict.ownership('release');assert.equal((await cli('pool-apply',test.database,evidenceFile,'--offline')).idempotent,true);
  assert.equal((await globalDaily()).consumed,30);assert.equal(await test.db.collection('_ha_quota_baseline_audits').countDocuments({poolId:'legacy-pool'}),1);
  console.log('PASS actual shared-pool cutover CLI: offline-owner refusal, stale-source/missing/low/inconsistent review rejection, atomic rollback, all-window/model USD/token baseline, later plan change, new/old key and unreviewed model/schedule guards, no hold refund and no idempotent spend reset');
} finally { bootstrap.close(); active?.close(); standby?.close(); strict?.close();baselineAdmin?.close();try{await test.cleanup();}finally{if(evidenceDirectory)rmSync(evidenceDirectory,{recursive:true});} }
