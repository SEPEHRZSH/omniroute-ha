// Offline, audited legacy key/USD and scoped-token cutover. A source fingerprint
// prevents applying a review to changed data; no hold or quarantine is removed.
import {createHash} from 'node:crypto';
import {BSON} from 'mongodb';
import {getBudgetWindow} from './budgetCalendar.mjs';
import {teamBudgetWindow,teamMembershipFingerprint} from './teamQuota.mjs';
import {windows,assertPoolConsumptionSchema,poolScheduleNamespace} from './poolQuota.mjs';
const fail=message=>{throw Object.assign(new Error(message),{code:'HA_QUOTA_POLICY'});};
const integer=value=>{if(!Number.isSafeInteger(value)||value<0)fail('Invalid historical token accounting');return value;};
const tokens=row=>integer(integer(row.tokens_input??0)+integer(row.tokens_output??0)+integer(row.tokens_reasoning??0));
const sum=(a,b)=>integer(a+b);
const canonical=value=>{
 if(!value||typeof value!=='object'||value instanceof Date||Buffer.isBuffer(value)||value._bsontype)return value;
 if(Array.isArray(value))return value.map(canonical);
 return Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])]));
};
const poolUnits=['requests','tokens','usd'];
const identity=value=>typeof value==='string'&&value.length>0&&value.length<=200&&!/[\x00-\x1f]/.test(value);
function inventoryList(values){
 if(!Array.isArray(values)||values.length>1000||values.some(value=>!identity(value))||new Set(values).size!==values.length)fail('Invalid pool baseline inventory');
 return [...values].sort();
}
const cellId=row=>JSON.stringify([row.apiKeyId,row.namespace,row.unit,row.window,row.bucket]);
async function poolSnapshot(db,session,poolId,now,nanoUsd){
 const options={session},pool=await db.collection('quota_pools').findOne({id:poolId},options);
 if(!pool)fail('Unknown pool for baseline');await assertPoolConsumptionSchema(db,session);
 const pools=await db.collection('quota_pools').find({},{...options,projection:{id:1}}).limit(1001).toArray();
 if(pools.length>1000||pools.some(row=>row.id!==poolId&&(row.id.startsWith(poolId+':')||poolId.startsWith(row.id+':'))))fail('Ambiguous or oversized pool namespace inventory');
 const hash=createHash('sha256'),keyIds=new Set(),models=new Set(),scheduleIds=new Set(),minimum=new Map();let keyRowid=0;
 const buckets=Object.fromEntries(Object.entries(windows).map(([window,ms])=>[window,Math.floor(now/ms)]));
 hash.update(BSON.serialize({pool:canonical(pool),buckets}));
 const include=(set,value)=>{if(!identity(value))fail('Invalid historical pool identity');set.add(value);};
 async function scan(table,filter,visit,projection){
  hash.update(table);
  for await(const row of db.collection(table).find(filter,{...options,batchSize:256,...(projection?{projection}:{})}).sort({_id:1})){
   hash.update(BSON.serialize({row:canonical(row)}));visit?.(row);
  }
 }
 await scan('api_keys',{},row=>{if(!Number.isSafeInteger(row._ha_rowid))fail('Invalid API key rowid for pool cutover');keyRowid=Math.max(keyRowid,row._ha_rowid);},{id:1,_ha_rowid:1});
 await scan('quota_allocations',{pool_id:poolId},row=>include(keyIds,row.api_key_id));
 await scan('quota_pool_connections',{pool_id:poolId});
 await scan('provider_plans',{connection_id:pool.connection_id});
 await scan('quota_schedules',{pool_id:poolId},row=>include(scheduleIds,row.id));
 await scan('quota_allocation_model_caps',{pool_id:poolId},row=>{include(models,row.model);include(keyIds,row.api_key_id);});
 await scan('_ha_pool_profiles',{_id:poolId});
 await scan('_ha_reservations',{poolIds:poolId},row=>{include(keyIds,row.keyId);include(models,row.model);});
 const prefix=poolId.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+':';
 await scan('quota_consumption',{dimension_key:{$regex:'^'+prefix}},row=>{
  const match=/^(.*):(requests|tokens|usd):(hourly|5h|daily|weekly|monthly)$/.exec(row.dimension_key);
  if(!match)fail('Historical pool dimension is not certified countable accounting');
  const [,namespace,unit,window]=match;include(keyIds,row.api_key_id);
  if(namespace.startsWith(poolId+':model:'))include(models,namespace.slice((poolId+':model:').length));
  else if(namespace.startsWith(poolId+':sched:'))include(scheduleIds,namespace.slice((poolId+':sched:').length));
  else if(namespace!==poolId)fail('Unknown historical pool namespace');
  if(!Number.isSafeInteger(row.bucket_index)||row.bucket_index>buckets[window])fail('Invalid or future pool bucket');
  if(row.bucket_index<buckets[window]-1)return;
  if(typeof row.consumed!=='number'||!Number.isFinite(row.consumed)||row.consumed<0)fail('Invalid historical pool usage');
  let used=unit==='usd'?nanoUsd(row.consumed):integer(row.consumed);
  if(unit==='usd'&&row._ha_usd_nano!=null){
   used=integer(row._ha_usd_nano);
   if(Math.abs(row.consumed-used/1e9)>Math.max(1e-9,Math.abs(row.consumed)*Number.EPSILON*1024))fail('Inconsistent exact historical USD counter');
  }
  const id=cellId({apiKeyId:row.api_key_id,namespace,unit,window,bucket:row.bucket_index});
  if(minimum.has(id))fail('Duplicate historical pool counter');minimum.set(id,used);
 });
 // History identifies potentially relevant models, never invents pool spend or
 // membership attribution from best-effort analytics. The operator reviews it.
 for(const table of ['usage_history','_ha_usage_history','request_cost_ledger'])await scan(table,{api_key_id:{$in:[...keyIds]}},row=>{if(identity(row.model))models.add(row.model);});
 return{sourceSha256:hash.digest('hex'),keyRowid,buckets,minimum,inventory:{keyIds:[...keyIds].sort(),models:[...models].sort(),scheduleIds:[...scheduleIds].sort()}};
}
function poolCells(poolId,inventory,source){
 const namespaces=[poolId,...inventory.models.map(model=>poolId+':model:'+model),...inventory.scheduleIds.map(id=>poolScheduleNamespace(poolId,id))];
 if(new Set(namespaces).size!==namespaces.length||inventory.keyIds.length*namespaces.length*30>4000)fail('Pool baseline exceeds the bounded 4000-cell profile');
 return inventory.keyIds.flatMap(apiKeyId=>namespaces.flatMap(namespace=>poolUnits.flatMap(unit=>Object.keys(windows).flatMap(window=>[source.buckets[window]-1,source.buckets[window]].map(bucket=>{
  const row={apiKeyId,namespace,unit,window,bucket};return{...row,minimum:source.minimum.get(cellId(row))??0,used:null};
 })))));
}
function validatePoolCells(poolId,inventory,rows,now){
 const map=new Map(rows.map(row=>[cellId(row),row.used]));
 const value=(key,namespace,unit,window,bucket)=>map.get(cellId({apiKeyId:key,namespace,unit,window,bucket}));
 const namespaces=[poolId,...inventory.models.map(model=>poolId+':model:'+model),...inventory.scheduleIds.map(id=>poolScheduleNamespace(poolId,id))];
 for(const key of inventory.keyIds)for(const namespace of namespaces)for(const window of Object.keys(windows))for(const bucket of [Math.floor(now/windows[window])-1,Math.floor(now/windows[window])]){
  if((value(key,namespace,'tokens',window,bucket)>0||value(key,namespace,'usd',window,bucket)>0)&&value(key,namespace,'requests',window,bucket)===0)fail('Pool token/USD history requires matching request history');
  for(const unit of poolUnits)if(namespace===poolId){
   const total=value(key,poolId,unit,window,bucket);
   const modeled=inventory.models.reduce((n,model)=>sum(n,value(key,poolId+':model:'+model,unit,window,bucket)),0);
   const scheduled=inventory.scheduleIds.reduce((n,id)=>sum(n,value(key,poolScheduleNamespace(poolId,id),unit,window,bucket)),0);
   if(modeled!==total||scheduled>total)fail('Reviewed model partition/schedule totals do not match pool history');
  }
 }
 // Every fully contained disjoint interval is a lower bound for its containing
 // bucket. Weighted interval scheduling avoids adding overlapping windows twice.
 for(const key of inventory.keyIds)for(const namespace of namespaces)for(const unit of poolUnits){
  const intervals=rows.filter(row=>row.apiKeyId===key&&row.namespace===namespace&&row.unit===unit).map(row=>({start:row.bucket*windows[row.window],end:Math.min(now,(row.bucket+1)*windows[row.window]),used:row.used}));
  for(const outer of intervals){
   const inside=intervals.filter(row=>row!==outer&&row.start>=outer.start&&row.end<=outer.end).sort((a,b)=>a.end-b.end||a.start-b.start),dp=[0];
   for(let i=0;i<inside.length;i++){let previous=i-1;while(previous>=0&&inside[previous].end>inside[i].start)previous--;dp.push(Math.max(dp[i],sum(dp[previous+1],inside[i].used)));}
   if(dp.at(-1)>outer.used)fail('Reviewed pool windows contradict contained historical usage');
  }
 }
}
async function poolBaseline(db,session,request,{writer,now,execute,nanoUsd}){
 const applying=request.action==='poolBaselineApply',evidence=request.evidence,audits=db.collection('_ha_quota_baseline_audits'),options={session};
 if(applying){
  if(!evidence||evidence.version!==1||evidence.kind!=='pool'||evidence.id!==request.id||evidence.historyVerified!==true||
   !/^[a-f0-9]{64}$/.test(request.evidenceSha256??'')||!/^[a-f0-9]{64}$/.test(evidence.sourceSha256??'')||
   !['verifiedBy','reference'].every(key=>identity(evidence[key]))||
   Object.keys(evidence).some(key=>!['version','kind','id','poolId','sourceSha256','observedAt','summary','inventory','rows','historyVerified','verifiedBy','reference'].includes(key)))fail('Invalid verified pool baseline evidence');
  const prior=await audits.findOne({_id:request.id},options);
  if(prior){if(prior.kind!=='pool'||prior.poolId!==evidence.poolId||prior.sha256!==request.evidenceSha256)fail('Pool baseline identity already used');
   const current=await db.collection('_ha_pool_profiles').findOne({_id:prior.poolId},options);
   if(current?.allWindows!==true||current.trackingVersion!==1)fail('Prior pool baseline is no longer certified; inspect a new review');
   return{baselined:true,idempotent:true,poolId:prior.poolId};}
 }
 const poolId=applying?evidence.poolId:request.poolId;if(!identity(poolId))fail('Invalid pool baseline identity');
 if(!applying&&request.models!==undefined)inventoryList(request.models);
 const source=await poolSnapshot(db,session,poolId,now,nanoUsd);
 const provided=applying?evidence.inventory:{...source.inventory,models:[...new Set([...source.inventory.models,...(request.models??[])])]};
 if(!provided||Object.keys(provided).some(key=>!['keyIds','models','scheduleIds'].includes(key)))fail('Invalid pool inventory fields');
 const inventory=Object.fromEntries(['keyIds','models','scheduleIds'].map(field=>[field,inventoryList(provided[field])]));
 if(Object.keys(inventory).some(field=>source.inventory[field].some(value=>!inventory[field].includes(value))))fail('Pool review omits known history/inventory');
 const cells=poolCells(poolId,inventory,source);
 if(!applying)return{version:1,kind:'pool',id:request.id,poolId,sourceSha256:source.sourceSha256,observedAt:new Date(now).toISOString(),
  summary:{cells:cells.length,usdUnit:'nanoUsd',requiresCompleteModelAttribution:true},inventory,rows:cells,historyVerified:false,verifiedBy:'',reference:''};
 if(evidence.sourceSha256!==source.sourceSha256)fail('Pool baseline source or windows changed after review');
 const observed=Date.parse(evidence.observedAt);
 if(!Number.isSafeInteger(observed)||observed>now||Object.entries(windows).some(([window,ms])=>Math.floor(observed/ms)!==source.buckets[window]))fail('Pool baseline observation is stale or invalid');
 if(!inventory.models.length||!Array.isArray(evidence.rows)||evidence.rows.length!==cells.length)fail('Pool review must cover models and every current/previous countable bucket');
 const reviewed=new Map();for(const row of evidence.rows){
  if(!row||Object.keys(row).some(key=>!['apiKeyId','namespace','unit','window','bucket','minimum','used'].includes(key)))fail('Invalid reviewed pool cell');
  const id=cellId(row);if(reviewed.has(id))fail('Duplicate reviewed pool cell');reviewed.set(id,integer(row.used));
 }
 for(const cell of cells){const used=reviewed.get(cellId(cell));if(!Number.isSafeInteger(used)||used<cell.minimum)fail('Reviewed pool usage is missing or below existing accounting');cell.used=used;}
 validatePoolCells(poolId,inventory,cells,now);
 const sql='INSERT INTO quota_consumption(api_key_id,dimension_key,bucket_index,consumed,updated_at) VALUES (?,?,?,?,?) ON CONFLICT(api_key_id,dimension_key,bucket_index) DO UPDATE SET consumed=excluded.consumed,updated_at=excluded.updated_at';
 for(const row of cells){
  const dimension=row.namespace+':'+row.unit+':'+row.window;
  await execute({sql,args:[row.apiKeyId,dimension,row.bucket,row.unit==='usd'?row.used/1e9:row.used,now],mode:'run'});
  if(row.unit==='usd')await db.collection('quota_consumption').updateOne({api_key_id:row.apiKeyId,dimension_key:dimension,bucket_index:row.bucket},{$set:{_ha_usd_nano:row.used}},options);
 }
 const until=Math.max(...Object.entries(windows).map(([window,ms])=>(source.buckets[window]+2)*ms));
 await db.collection('_ha_pool_profiles').replaceOne({_id:poolId},{_id:poolId,allWindows:true,trackingVersion:1,startedAt:now,baselineId:request.id,
  legacyReview:{version:1,until,keyRowid:source.keyRowid,...inventory}}, {...options,upsert:true});
 await audits.insertOne({_id:request.id,kind:'pool',poolId,sha256:request.evidenceSha256,sourceSha256:source.sourceSha256,verifiedBy:evidence.verifiedBy,reference:evidence.reference,
  appliedAt:now,epoch:writer.epoch,cells:cells.length,inventory},options);
 return{baselined:true,idempotent:false,poolId,cells:cells.length,reviewUntil:new Date(until).toISOString()};
}
async function snapshot(db,session,keyId,now,specification,nanoUsd){
 const options={session},key=await db.collection('api_keys').findOne({id:keyId},options);
 if(!key)fail('Unknown API key for baseline');
 const limits=await db.collection('api_key_token_limits').find({api_key_id:keyId},options).sort({id:1}).limit(129).toArray();
 if(limits.length>128)fail('Too many scoped token policies');
 const scoped=limits.map(row=>{
  if(!['global','provider','model'].includes(row.scope_type)||!['daily','weekly','monthly'].includes(row.reset_interval)||typeof row.scope_value!=='string'||!Number.isSafeInteger(row._ha_rowid))fail('Invalid historical scoped policy');
  const start=getBudgetWindow(row.reset_interval,row.reset_time||'00:00',now).periodStartAt;
  return{row,start,windowStart:String(start),counter:0,ledger:0,history:0};
 });
 const hash=createHash('sha256');hash.update(BSON.serialize({key:canonical(key)}));
 const summaries={domainNanoUsd:0,ledgerNanoUsd:0,providerReportedNanoUsd:0,usageRowsWithoutProviderCost:0,rows:{}};
 const tables=[
  ['domain_cost_history',{api_key_id:keyId}],['request_cost_ledger',{api_key_id:keyId}],
  ['usage_history',{api_key_id:keyId}],['_ha_usage_history',{api_key_id:keyId}],
  ['api_key_quota_limits',{api_key_id:keyId}],['domain_budgets',{api_key_id:keyId}],
  ['api_key_token_limits',{api_key_id:keyId}],['api_key_token_counters',{limit_id:{$in:limits.map(row=>row.id)}}],
  ['key_value',{key:keyId,namespace:{$in:['keyQuotaUnpricedUsage','budgetUnpricedUsage']}}],
  ['_ha_reservations',{keyId}],['_ha_quota_blocked',{_id:keyId}],
 ];
 for(const[table,filter]of tables){
  hash.update(table);let count=0;
  // _id is indexed in every collection, including imported SQL tables. Stream
  // rows instead of materializing customer history into the synchronous bridge.
  for await(const row of db.collection(table).find(filter,{...options,batchSize:256}).sort({_id:1})){
   hash.update(BSON.serialize({row:canonical(row)}));count++;
   if(table==='domain_cost_history')summaries.domainNanoUsd=sum(summaries.domainNanoUsd,nanoUsd(row.cost));
   if(table==='request_cost_ledger')summaries.ledgerNanoUsd=sum(summaries.ledgerNanoUsd,nanoUsd(row.amount_usd));
   if(['usage_history','_ha_usage_history'].includes(table)){
    if(row.provider_cost_usd==null)summaries.usageRowsWithoutProviderCost++;
    else summaries.providerReportedNanoUsd=sum(summaries.providerReportedNanoUsd,nanoUsd(row.provider_cost_usd));
   }
   if(table==='api_key_token_counters'){
    const scope=scoped.find(scope=>scope.row.id===row.limit_id&&scope.windowStart===row.window_start);
    if(scope){if(scope.hasCounter)fail('Duplicate scoped counter');scope.counter=integer(row.tokens_used);scope.hasCounter=true;}
   }
   if(['request_cost_ledger','usage_history','_ha_usage_history'].includes(table)){
    const at=Date.parse(row.timestamp);if(!Number.isFinite(at))fail('Unverifiable historical usage timestamp');
    const n=tokens(row);
    for(const scope of scoped)if(at>=scope.start&&(scope.row.scope_type==='global'||row[scope.row.scope_type]===scope.row.scope_value)){
     const field=table==='request_cost_ledger'?'ledger':'history';scope[field]=sum(scope[field],n);
    }
   }
  }
  summaries.rows[table]=count;
 }
 return{sourceSha256:hash.digest('hex'),summaries,scoped:scoped.map(scope=>({limitId:scope.row.id,windowStart:scope.windowStart,
  minimumTokens:Math.max(scope.counter,scope.ledger,scope.history),spec:specification(scope.row)}))};
}
export async function quotaBaselineOperation(db,session,request,{writer,now,execute,schema,allowAdmin,specification,nanoUsd}){
 if(!allowAdmin||!writer.ownerId.startsWith('offline-reconciliation-'))fail('Legacy quota baseline requires the offline administrative connection');
 if(request.action.startsWith('poolBaseline'))return poolBaseline(db,session,request,{writer,now,execute,nanoUsd});
 if(request.action.startsWith('teamBaseline'))return teamBaseline(db,session,request,{writer,now,nanoUsd});
 const audits=db.collection('_ha_quota_baseline_audits'),evidence=request.evidence;
 if(request.action==='baselineApply'){
  if(!evidence||evidence.version!==1||evidence.id!==request.id||!evidence.keyId||evidence.usdHistoryVerified!==true||
    !/^[a-f0-9]{64}$/.test(request.evidenceSha256??'')||!/^[a-f0-9]{64}$/.test(evidence.sourceSha256??'')||
    !['verifiedBy','reference'].every(key=>typeof evidence[key]==='string'&&evidence[key].trim()&&evidence[key].length<=200)||
    Object.keys(evidence).some(key=>!['version','id','keyId','sourceSha256','observedAt','summary','usdHistoryVerified','verifiedBy','reference','scoped'].includes(key)))fail('Invalid verified baseline evidence');
  const prior=await audits.findOne({_id:request.id},{session});
  if(prior){if(prior.sha256!==request.evidenceSha256||prior.keyId!==evidence.keyId)fail('Baseline identity already used');
   if((await db.collection('api_keys').findOne({id:prior.keyId},{session}))?._ha_requires_key_baseline===true)fail('Imported key requires a new historical review');
   return{baselined:true,idempotent:true,keyId:prior.keyId};}
 }
 const keyId=request.action==='baselineInspect'?request.keyId:evidence.keyId;
 if(typeof keyId!=='string'||!keyId||keyId.length>200)fail('Invalid baseline API key identity');
 const source=await snapshot(db,session,keyId,now,specification,nanoUsd);
 if(request.action==='baselineInspect')return{version:1,id:request.id,keyId,sourceSha256:source.sourceSha256,observedAt:new Date(now).toISOString(),summary:source.summaries,
  usdHistoryVerified:false,verifiedBy:'',reference:'',scoped:source.scoped.map(({limitId,windowStart,minimumTokens})=>({limitId,windowStart,minimumTokens,tokensUsed:null}))};
 if(evidence.sourceSha256!==source.sourceSha256)fail('Baseline source changed after review');
 if(!Array.isArray(evidence.scoped)||evidence.scoped.length!==source.scoped.length)fail('Baseline must cover every current scoped policy');
 for(let index=0;index<source.scoped.length;index++){
  const scope=source.scoped[index],verified=evidence.scoped[index];
  if(!verified||verified.limitId!==scope.limitId||verified.windowStart!==scope.windowStart||
    Object.keys(verified).some(key=>!['limitId','windowStart','minimumTokens','tokensUsed'].includes(key))||
    !Number.isSafeInteger(verified.tokensUsed)||verified.tokensUsed<scope.minimumTokens)fail('Verified scoped total is stale or below known usage');
 }
 // USD history completeness is an explicit operator/provider reconciliation,
 // not inferred from best-effort analytics. Unpriced flags remain authoritative.
 await db.collection('_ha_key_usage_profiles').replaceOne({_id:keyId},{_id:keyId,allHistory:true,startedAt:now,baselineId:request.id},{session,upsert:true});
 await db.collection('api_keys').updateOne({id:keyId},{$unset:{_ha_requires_key_baseline:''}},{session});
 if(source.scoped.length){await schema('api_key_token_counters');await schema('api_key_token_limit_reset_logs');}
 for(let index=0;index<source.scoped.length;index++){
  const scope=source.scoped[index],verified=evidence.scoped[index];
  await execute({sql:'INSERT INTO api_key_token_counters(limit_id,window_start,tokens_used) VALUES (?,?,?) ON CONFLICT(limit_id,window_start) DO UPDATE SET tokens_used=excluded.tokens_used',args:[scope.limitId,scope.windowStart,verified.tokensUsed],mode:'run'});
  await db.collection('_ha_scoped_token_profiles').replaceOne({_id:scope.limitId},{_id:scope.limitId,spec:scope.spec,startedAt:now,allWindows:true,baselineId:request.id},{session,upsert:true});
 }
 await audits.insertOne({_id:request.id,keyId,sha256:request.evidenceSha256,sourceSha256:source.sourceSha256,verifiedBy:evidence.verifiedBy,reference:evidence.reference,
  appliedAt:now,epoch:writer.epoch,summary:source.summaries,scoped:evidence.scoped}, {session});
 return{baselined:true,keyId,scoped:source.scoped.length,idempotent:false};
}

// A reviewed Team total covers the current calendar window, not arbitrary past
// windows. Receipts remain authoritative after cutover; holds are never deducted
// from the reviewed spend or cleared. Calendar changes overlapping unverified
// history require a new review instead of silently dropping the baseline.
async function teamBaseline(db,session,request,{writer,now,nanoUsd}) {
 const options={session},audits=db.collection('_ha_quota_baseline_audits'),evidence=request.evidence,applying=request.action==='teamBaselineApply';
 if(applying){
  if(!evidence||evidence.version!==1||evidence.id!==request.id||evidence.kind!=='team'||evidence.usdHistoryVerified!==true||
    !/^[a-f0-9]{64}$/.test(request.evidenceSha256??'')||!/^[a-f0-9]{64}$/.test(evidence.sourceSha256??'')||
    !['verifiedBy','reference'].every(key=>typeof evidence[key]==='string'&&evidence[key].trim()&&evidence[key].length<=200)||
    Object.keys(evidence).some(key=>!['version','kind','id','teamId','sourceSha256','observedAt','window','summary','usdHistoryVerified','verifiedBy','reference','totalNanoUsd'].includes(key)))fail('Invalid verified Team baseline');
  const prior=await audits.findOne({_id:request.id},options);
  if(prior){if(prior.sha256!==request.evidenceSha256||prior.teamId!==evidence.teamId)fail('Baseline identity already used');
   if((await db.collection('_ha_team_profiles').findOne({_id:prior.teamId},options))?.complete!==true)fail('Prior Team baseline is no longer certified; inspect a new review');
   return{baselined:true,idempotent:true,teamId:prior.teamId};}
 }
 const teamId=applying?evidence.teamId:request.teamId;
 if(typeof teamId!=='string'||!teamId||teamId.length>200)fail('Invalid baseline Team identity');
 const team=await db.collection('teams').findOne({id:teamId},options);if(!team)fail('Unknown Team for baseline');
 const window=teamBudgetWindow(team,now),hash=createHash('sha256');hash.update(BSON.serialize({team:canonical(team),window}));
 const members=await db.collection('api_key_billing_team_history').find({team_id:teamId},options).sort({id:1}).limit(1001).toArray();
 if(members.length>1000)fail('Too many Team memberships for reviewed cutover');
 const intervals=new Map();
 for(const member of members){
  const from=Date.parse(member.valid_from),to=member.valid_to==null?Infinity:Date.parse(member.valid_to);
  if(typeof member.id!=='string'||typeof member.api_key_id!=='string'||!Number.isSafeInteger(from)||from>now||!(to>from))fail('Invalid historical Team membership');
  const existing=intervals.get(member.api_key_id)||[];
  if(existing.some(interval=>from<interval.to&&to>interval.from))fail('Overlapping historical Team membership');
  existing.push({from,to});intervals.set(member.api_key_id,existing);hash.update(BSON.serialize({member:canonical(member)}));
 }
 const summary={domainNanoUsd:0,ledgerNanoUsd:0,providerReportedNanoUsd:0,usageRowsWithoutProviderCost:0,nativeSettledNanoUsd:0,rows:{}};
 const membershipAt=(row,at)=>(intervals.get(row.api_key_id)||[]).some(interval=>at>=interval.from&&at<interval.to);
 const keys=[...intervals.keys()],teamOrKey={$or:[{billing_team_id:teamId},{api_key_id:{$in:keys}}]};
 for(const[table,filter]of[
  ['domain_cost_history',{api_key_id:{$in:keys}}],['request_cost_ledger',{api_key_id:{$in:keys}}],
  ['usage_history',teamOrKey],['_ha_usage_history',teamOrKey],['daily_team_usage_summary',{team_id:teamId}],
  ['_ha_reservations',{teamId}],['_ha_team_profiles',{_id:teamId}],['_ha_team_memberships',{teamId}],
 ]){
  hash.update(table);let count=0;
  for await(const row of db.collection(table).find(filter,{...options,batchSize:256}).sort({_id:1})){
   hash.update(BSON.serialize({row:canonical(row)}));count++;
   if(['domain_cost_history','request_cost_ledger','usage_history','_ha_usage_history'].includes(table)){
    const at=table==='domain_cost_history'?row.timestamp:Date.parse(row.timestamp);
    if(!Number.isSafeInteger(at))fail('Unverifiable historical Team usage timestamp');
    if(at<window.start||at>=window.end||!(row.billing_team_id===teamId||membershipAt(row,at)))continue;
    if(table==='domain_cost_history')summary.domainNanoUsd=sum(summary.domainNanoUsd,nanoUsd(row.cost));
    else if(table==='request_cost_ledger')summary.ledgerNanoUsd=sum(summary.ledgerNanoUsd,nanoUsd(row.amount_usd));
    else if(row.provider_cost_usd==null)summary.usageRowsWithoutProviderCost++;
    else summary.providerReportedNanoUsd=sum(summary.providerReportedNanoUsd,nanoUsd(row.provider_cost_usd));
   }
   if(table==='_ha_reservations'&&row.state==='settled'&&row.settledAt>=window.start)summary.nativeSettledNanoUsd=sum(summary.nativeSettledNanoUsd,integer(row.receipt?.nanoUsd));
  }
  summary.rows[table]=count;
 }
 const profile=await db.collection('_ha_team_profiles').findOne({_id:teamId},options);
 if(profile?.baselineId!=null&&(!Number.isSafeInteger(profile.startedAt)||!Number.isSafeInteger(profile.windowStart)||!Number.isSafeInteger(profile.windowEnd)))fail('Invalid previous Team baseline');
 // A changed calendar may need another review. Conservatively retain the old
 // historical debit when the new window overlaps the pre-cutover interval;
 // ordinary admission refuses that change until this offline review completes.
 const previousLegacy=profile?.baselineId!=null&&window.start<profile.startedAt?integer(profile.legacyNanoUsd):0;
 summary.minimumNanoUsd=Math.max(summary.domainNanoUsd,summary.ledgerNanoUsd,summary.providerReportedNanoUsd,
  sum(summary.nativeSettledNanoUsd,previousLegacy));
 const sourceSha256=hash.digest('hex');
 if(!applying)return{version:1,kind:'team',id:request.id,teamId,sourceSha256,observedAt:new Date(now).toISOString(),window,
  summary,usdHistoryVerified:false,verifiedBy:'',reference:'',totalNanoUsd:null};
 if(evidence.sourceSha256!==sourceSha256)fail('Team baseline source changed after review');
 if(!Number.isSafeInteger(evidence.totalNanoUsd)||evidence.totalNanoUsd<summary.minimumNanoUsd)fail('Verified Team total is below known usage');
 const legacyNanoUsd=evidence.totalNanoUsd-summary.nativeSettledNanoUsd;
 await db.collection('_ha_team_profiles').replaceOne({_id:teamId},{_id:teamId,complete:true,startedAt:now,baselineId:request.id,legacyNanoUsd,windowStart:window.start,windowEnd:window.end},{...options,upsert:true});
 for(const member of members)await db.collection('_ha_team_memberships').replaceOne({_id:member.id},{_id:member.id,teamId,fingerprint:teamMembershipFingerprint(member),baselineId:request.id},{...options,upsert:true});
 await audits.insertOne({_id:request.id,kind:'team',teamId,sha256:request.evidenceSha256,sourceSha256,verifiedBy:evidence.verifiedBy,reference:evidence.reference,
  appliedAt:now,epoch:writer.epoch,window,summary,totalNanoUsd:evidence.totalNanoUsd,legacyNanoUsd},options);
 return{baselined:true,idempotent:false,teamId,legacyNanoUsd};
}
