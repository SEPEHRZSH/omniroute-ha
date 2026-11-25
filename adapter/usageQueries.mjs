// Exact pinned application queries, not a generic JSON/aggregate SQL dialect.
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { parseTree, findNodeAtLocation, createScanner, SyntaxKind } from 'jsonc-parser';
const canonical = sql => sql.replace(/\s+/g,' ').trim();
const reject = message => { throw Object.assign(new Error(message),{code:'HA_UNSUPPORTED_SQL'}); };
function source(file,hash) {
  const text = readFileSync(new URL('../upstream/omniroute/src/lib/'+file,import.meta.url),'utf8');
  if (createHash('sha256').update(text).digest('hex')!==hash) reject('Usage-query source contract changed');
  return text;
}
const usageSource = source('usage/apiKeyUsageLimits.ts','d07934edabb10d05111a2cdb7b5afbdf061854f170f13f18d9ce7b8a12a6bab1');
const databaseSettingsSource=source('db/databaseSettings.ts','cc0a6ad240dc5d6852871f119d879b9aebb82eef54317a6f3f370ce994c75ba7');
export const migrationVersionSql=databaseSettingsSource.match(/\.prepare\("(SELECT MAX\(CAST\(version AS INTEGER\)\) AS version FROM _omniroute_migrations)"\)/)?.[1];
if(!migrationVersionSql)reject('Migration version query extraction failed');
const providerSource = source('db/providerLimits.ts','3d2adaece375c8a4f688df9bda2700f80f3c8c0e14a2ef4b7a47ef3d11a24c93');
const teamSource=source('usage/teamUsageLimits.ts','e61744f84a33cd321e87b1dcd5134927586d784eb8f48d33bdc8eba46ad04946');
const unpricedSource=source('db/usageModelGroups.ts','37d76e2d15a7cc78809f7ea25e819e32bb4429071c2315e5dac0ae7cace883a1');
const limitedKeySql=unpricedSource.match(/const LIMITED_KEY_SQL = `([\s\S]*?)`;/)?.[1];
export const unpricedUsageSql=[...unpricedSource.matchAll(/\.prepare\(\s*`([\s\S]*?)`\s*\)/g)].map(match=>match[1].replaceAll('${LIMITED_KEY_SQL}',limitedKeySql));
if(!limitedKeySql||unpricedUsageSql.length!==2)reject('Unpriced usage-query extraction failed');
const callStats=source('db/callLogStats.ts','933bb86cdaa8c4e554d8a18a0191f8f8db905fb8f6006886a3256cfabc382e1b');
source('db/logicalCallSql.ts','5f59112ddcce301c4095635b7ec56456412c75465586371e256235707cf2b617');
const noauth=source('../shared/constants/providers/noauth.ts','0885c88d9247c3d2b7d9885d049138aec665c30dc1f2bb091829d3851acb1544');
const noauthKeys=[...noauth.matchAll(/\b(?:id|alias):\s*"([a-z0-9._-]+)"/gi)].map(match=>match[1]);
const logicalCallSql=alias=>`(${alias}.request_type IS NULL OR ${alias}.request_type <> 'upstream_http_attempt')`;
const liveFilter=callStats.match(/const LIVE_PROVIDER_FILTER_SQL = `([\s\S]*?)`;/)?.[1];
const metricTemplate=callStats.match(/export const PROVIDER_METRICS_SQL = `([\s\S]*?)`;/)?.[1];
const usageTemplate=callStats.slice(callStats.indexOf('export function getProviderUsageSince(')).match(/\.prepare\(\s*`([\s\S]*?)`\s*\)/)?.[1];
if(!liveFilter||!metricTemplate||!usageTemplate||!noauthKeys.length)reject('Provider metrics extraction failed');
// Only the hash-pinned upstream templates are evaluated, never caller SQL.
const liveProviderFilter=Function('NOAUTH_PROVIDER_KEYS','return `'+liveFilter+'`;')(noauthKeys);
export const providerMetricsSql=[metricTemplate,usageTemplate].map(template=>Function('logicalCallSql','LIVE_PROVIDER_FILTER_SQL','return `'+template+'`;')(logicalCallSql,liveProviderFilter));
export const teamSpendSql=[...teamSource.matchAll(/\.prepare\(\s*`([\s\S]*?)`\s*\)/g)].map(match=>match[1]);
if(teamSpendSql.length!==2) reject('Team usage-query extraction failed');
export const usageSpendSql = usageSource.slice(usageSource.indexOf('async function getApiKeyUsdSpendSince(')).match(/\.prepare\(\s*`([\s\S]*?)`\s*\)/)?.[1];
export const codexDisplaySql = providerSource.match(/const CODEX_QUOTA_DISPLAY_SQL = `([\s\S]*?)`;/)?.[1];
if (!usageSpendSql || !codexDisplaySql) reject('Usage-query extraction failed');
export function parseUsageQuery(sql,bindings) {
  let type;
  const text = canonical(sql);
  if(text===migrationVersionSql){if(bindings.length)reject('Migration version bindings');return{type:'ha_migration_version'};}
  const metric=providerMetricsSql.findIndex(query=>canonical(query)===text);
  if(metric>=0){if(bindings.length!==metric||bindings.some(value=>typeof value!=='string'))reject('Provider metrics bindings');return{type:'ha_provider_metrics',since:bindings[0]};}
  const report=unpricedUsageSql.findIndex(query=>canonical(query)===text);
  if(report>=0){
    // sql.parse has already resolved named parameters in occurrence order.
    const fields=report===0?['sinceIso']:['sinceIso','labels'];
    if(bindings.length!==fields.length||bindings.some(value=>typeof value!=='string'))reject('Unpriced usage-query bindings');
    const parameters=Object.fromEntries(fields.map((key,index)=>[key,bindings[index]]));
    let labels;if(report===1){try{labels=JSON.parse(parameters.labels);}catch{reject('Unpriced usage labels');}if(!Array.isArray(labels)||labels.length>5000||labels.some(label=>typeof label!=='string'||label.length>2000))reject('Unpriced usage labels');}
    return{type:report===0?'ha_unpriced_groups':'ha_unpriced_affected',since:parameters.sinceIso,labels};
  }
  if (text===canonical(usageSpendSql)) type='ha_usage_spend';
  else if(text===canonical(teamSpendSql[0])) type='ha_team_raw';
  else if(text===canonical(teamSpendSql[1])) type='ha_team_summary';
  else if (text===canonical(codexDisplaySql) || text===canonical(codexDisplaySql)+' AND id = ?') type='ha_codex_display';
  else return null;
  const count = type.startsWith('ha_team_')?3:type==='ha_usage_spend'?2:text===canonical(codexDisplaySql)?0:1;
  if (bindings.length!==count || bindings.some(value=>typeof value!=='string')) reject('Unsupported usage-query bindings');
  return {type,bindings};
}
export function usageSpendPipeline(keyId,since,asciiLower) {
  const presentCost = {$ne:[{$ifNull:['$provider_cost_usd',null]},null]};
  const sum = field => ({$sum:{$cond:[presentCost,0,{$ifNull:['$'+field,0]}]}});
  return [{$match:{api_key_id:keyId,timestamp:{$gte:since},$or:[{success:1},{provider_cost_usd:{$ne:null}}]}},
    {$group:{_id:{provider:asciiLower({$ifNull:['$provider',null]}),model:asciiLower({$ifNull:['$model',null]}),
      serviceTier:{$cond:[{$in:[{$ifNull:['$service_tier',null]},['',null]]},'standard','$service_tier']}},
      storedCost:{$sum:{$ifNull:['$provider_cost_usd',0]}},unmeteredRequests:{$sum:{$cond:[presentCost,0,1]}},
      promptTokens:sum('tokens_input'),completionTokens:sum('tokens_output'),cacheReadTokens:sum('tokens_cache_read'),
      cacheCreationTokens:sum('tokens_cache_creation'),reasoningTokens:sum('tokens_reasoning')}},
    {$project:{_id:0,provider:'$_id.provider',model:'$_id.model',serviceTier:'$_id.serviceTier',storedCost:1,unmeteredRequests:1,
      promptTokens:1,completionTokens:1,cacheReadTokens:1,cacheCreationTokens:1,reasoningTokens:1}}];
}
function quotaJson(text) {
  if (text==null) return null;
  if (typeof text!=='string') reject('Codex quota projection requires JSON text, not binary or native BSON values');
  const errors=[], tree=parseTree(text,errors,{disallowComments:true,allowTrailingComma:false});
  if (errors.length || !tree) return null;
  const entries = ['codexQuotaStateByScope','codexQuotaState'].map(key=> {
    const node=findNodeAtLocation(tree,[key]);
    if (!node || node.type==='null') return JSON.stringify(key)+':null';
    if (!['object','array'].includes(node.type)) reject('Unsupported scalar Codex quota DTO');
    // SQLite json_extract marks object/array values as JSON, retaining number
    // lexemes and duplicate fields; JSON.parse/stringify would change them.
    const scanner=createScanner(text.slice(node.offset,node.offset+node.length),true), tokens=[];
    for (let kind;(kind=scanner.scan())!==SyntaxKind.EOF;) tokens.push(text.slice(node.offset+scanner.getTokenOffset(),node.offset+scanner.getTokenOffset()+scanner.getTokenLength()));
    return JSON.stringify(key)+':'+tokens.join('');
  });
  return '{'+entries.join(',')+'}';
}
export async function executeUsageQuery(db,ast,session,schema,asciiLower) {
  if(ast.type==='ha_migration_version'){
    await schema('_omniroute_migrations');const rows=await db.collection('_omniroute_migrations').find({},{session,projection:{version:1}}).limit(5001).toArray();
    if(rows.length>5000||rows.some(row=>typeof row.version!=='string'||!/^\d+$/.test(row.version)||!Number.isSafeInteger(Number(row.version))))reject('Invalid or unbounded migration version history');
    return[{version:rows.length?Math.max(...rows.map(row=>Number(row.version))):null}];
  }
  if(ast.type==='ha_provider_metrics')return executeProviderMetrics(db,ast,session,schema);
  if(ast.type.startsWith('ha_unpriced_'))return executeUnpricedUsage(db,ast,session,schema,asciiLower);
  if(ast.type.startsWith('ha_team_')) return executeTeamUsage(db,ast,session,schema,asciiLower);
  const table=ast.type==='ha_usage_spend'?'usage_history':'provider_connections', spec=await schema(table);
  const required=table==='usage_history'?['api_key_id','timestamp','success','provider','model','service_tier','provider_cost_usd','tokens_input','tokens_output','tokens_cache_read','tokens_cache_creation','tokens_reasoning']:['id','provider','provider_specific_data'];
  if (required.some(name=>!spec.columns.some(c=>c.name===name))) reject('Missing usage-query schema column');
  if (table==='usage_history') {
    if(!session)reject('Financial history read requires a shared snapshot');
    const merged=new Map();
    for(const name of ['usage_history','_ha_usage_history']) {
      const rows=await db.collection(name).aggregate([...usageSpendPipeline(...ast.bindings,asciiLower),{$limit:5001}],{session,maxTimeMS:5000}).toArray();
      if(rows.length>5000)reject('Financial history projection resource limit');
      for(const row of rows) {
        const key=JSON.stringify([row.provider,row.model,row.serviceTier]),prior=merged.get(key);
        if(!prior){merged.set(key,row);continue;}
        for(const field of Object.keys(row).filter(key=>!['provider','model','serviceTier'].includes(key))) {
          prior[field]+=row[field];if(!Number.isFinite(prior[field]))reject('Financial history counter overflow');
        }
      }
    }
    if(merged.size>5000)reject('Financial history projection resource limit');return [...merged.values()];
  }
  return codexQuotaRows(db,session,ast.bindings.length?ast.bindings:undefined);
}
async function executeProviderMetrics(db,ast,session,schema){
 const spec=await schema('call_logs');await schema('provider_connections');
 if(!session||['provider','request_type','status','duration','timestamp','id','error_summary'].some(name=>!spec.columns.some(column=>column.name===name)))reject('Provider metrics schema/snapshot');
 const error={$or:[{$and:[{$ne:[{$ifNull:['$status',null]},null]},{$or:[{$lt:['$status',200]},{$gte:['$status',400]}]}]},{$ne:[{$ifNull:['$error_summary',null]},null]}]};
 const pipeline=[{$match:{request_type:{$ne:'upstream_http_attempt'},provider:{$nin:[null,'-']},...(ast.since?{timestamp:{$gte:ast.since}}:{})}},
  {$lookup:{from:'provider_connections',let:{provider:'$provider'},pipeline:[{$match:{$expr:{$eq:['$provider','$$provider']}}},{$limit:1},{$project:{_id:1}}],as:'_live'}},
  {$match:{$or:[{provider:{$in:noauthKeys}},{'_live.0':{$exists:true}}]}},{$set:{_err:error}},{$sort:{provider:1,timestamp:-1,id:-1}},
  {$group:{_id:'$provider',requests:{$sum:1},successes:{$sum:{$cond:[{$and:[{$gte:['$status',200]},{$lt:['$status',400]}]},1,0]}},
    duration:{$sum:{$convert:{input:'$duration',to:'double',onNull:0,onError:0}}},latencySamples:{$sum:{$cond:[{$ne:[{$ifNull:['$duration',null]},null]},1,0]}},
    lastRequestAt:{$max:'$timestamp'},lastErrorAt:{$max:{$cond:['$_err','$timestamp',null]}},lastStatus:{$first:{$ifNull:['$status',null]}},
    lastErrorStatus:{$top:{sortBy:{_err:-1,timestamp:-1,id:-1},output:{$cond:['$_err',{$ifNull:['$status',null]},null]}}}}},{$limit:5001}];
 const rows=await db.collection('call_logs').aggregate(pipeline,{session,maxTimeMS:5000}).toArray();if(rows.length>5000)reject('Provider metrics resource limit');
 return rows.map(row=>{const average=row.latencySamples?row.duration/row.latencySamples:null,avgLatencyMs=average==null?null:Math.sign(average)*Math.floor(Math.abs(average)+0.5);
  const common={provider:row._id,avgLatencyMs,latencySamples:row.latencySamples,lastRequestAt:row.lastRequestAt??null};
  return ast.since?{...common,requests:row.requests,successes:row.successes}:{...common,totalRequests:row.requests,totalSuccesses:row.successes,lastErrorAt:row.lastErrorAt??null,lastStatus:row.lastStatus,lastErrorStatus:row.lastErrorStatus};});
}
async function executeUnpricedUsage(db,ast,session,schema,asciiLower){
  if(!session)reject('Unpriced report requires a shared snapshot');
  for(const[table,fields]of [['usage_history',['provider','model','api_key_id','timestamp','success','provider_cost_usd']],['api_keys',['id','usage_limit_enabled','daily_usage_limit_usd','weekly_usage_limit_usd']]]){
    const spec=await schema(table);if(fields.some(name=>!spec.columns.some(column=>column.name===name)))reject('Unpriced report schema mismatch');
  }
  const groups=new Map(),affected=new Set();let seen=0;
  for(const table of ['usage_history','_ha_usage_history']){
    const pipeline=[{$match:{timestamp:{$gte:ast.since},success:1,provider_cost_usd:null}}];
    if(ast.labels)pipeline.push({$match:{$expr:{$in:[{$concat:[asciiLower('$provider'),'/',asciiLower('$model')]},{$literal:ast.labels}]}}});
    pipeline.push({$group:{_id:{provider:asciiLower('$provider'),model:asciiLower('$model'),keyId:'$api_key_id'},requests:{$sum:1},lastSeenAt:{$max:'$timestamp'}}},
      {$lookup:{from:'api_keys',localField:'_id.keyId',foreignField:'id',as:'_keys'}},
      {$project:{_id:0,provider:'$_id.provider',model:'$_id.model',keyId:'$_id.keyId',requests:1,lastSeenAt:1,
        limited:{$and:[{$eq:[{$arrayElemAt:['$_keys.usage_limit_enabled',0]},1]},{$or:['daily_usage_limit_usd','weekly_usage_limit_usd'].map(field=>({$gt:[{$ifNull:[{$arrayElemAt:['$_keys.'+field,0]},0]},0]}))}]}}});
    for await(const row of db.collection(table).aggregate(pipeline,{session,maxTimeMS:5000,batchSize:256})){
      if(++seen>100000)reject('Unpriced report resource limit');
      if(ast.labels){if(row.limited&&row.keyId!=null)affected.add(row.keyId);continue;}
      const id=JSON.stringify([row.provider??null,row.model??null]);let group=groups.get(id);
      if(!group){if(groups.size>=5000)reject('Unpriced model report resource limit');group={provider:row.provider??null,model:row.model??null,requests:0,keys:new Set(),limited:new Set(),lastSeenAt:null};groups.set(id,group);}
      group.requests+=row.requests;if(!Number.isSafeInteger(group.requests))reject('Unpriced request count overflow');
      if(row.keyId!=null){group.keys.add(row.keyId);if(row.limited)group.limited.add(row.keyId);}
      if(typeof row.lastSeenAt==='string'&&(group.lastSeenAt==null||row.lastSeenAt>group.lastSeenAt))group.lastSeenAt=row.lastSeenAt;
    }
  }
  return ast.labels?[{affected:affected.size}]:[...groups.values()].map(group=>({provider:group.provider,model:group.model,requests:group.requests,apiKeys:group.keys.size,limitedApiKeys:group.limited.size,lastSeenAt:group.lastSeenAt}));
}
async function executeTeamUsage(db,ast,session,schema,asciiLower) {
  const raw=ast.type==='ha_team_raw',table=raw?'usage_history':'daily_team_usage_summary',spec=await schema(table);
  const fields=['input','output','cache_read','cache_creation','reasoning'];
  const column=field=>raw?'tokens_'+field:'successful_'+field+'_tokens';
  const required=['provider','model','service_tier',...fields.map(column),...(raw?['billing_team_id','team_rollup_processed_at','timestamp','success']:['team_id','api_key_id','date','successful_requests','successful_token_shapes_json'])];
  if(required.some(name=>!spec.columns.some(c=>c.name===name))) reject('Missing Team usage-query schema column');
  const [id,start,end]=ast.bindings;
  const grouped={provider:asciiLower({$ifNull:['$provider',null]}),model:asciiLower({$ifNull:['$model',null]}),serviceTier:{$ifNull:['$service_tier',null]}};
  if(raw) for(const field of fields) grouped[field]={$ifNull:['$'+column(field),null]};
  else {grouped.api_key_id='$api_key_id';grouped.date='$date';}
  const totals=Object.fromEntries(fields.map(field=>[field.replace(/_([a-z])/g,(_,letter)=>letter.toUpperCase())+'Tokens',{$sum:{$ifNull:['$'+column(field),0]}}]));
  const label=(field,fallback)=>({$cond:[{$in:['$_id.'+field,[null,'']]},fallback,'$_id.'+field]});
  const pipeline=[{$match:raw?{billing_team_id:id,team_rollup_processed_at:null,timestamp:{$gte:start,$lt:end},success:1}:{team_id:id,date:{$gte:start,$lt:end},successful_requests:{$gt:0}}},
    {$group:{_id:grouped,...totals,requests:{$sum:raw?1:'$successful_requests'},...(!raw?{tokenShapesJson:{$max:'$successful_token_shapes_json'}}:{})}},
    {$project:{_id:0,provider:label('provider','__unknown_provider__'),model:label('model','__unknown_model__'),serviceTier:label('serviceTier','standard'),...Object.fromEntries(Object.keys(totals).map(key=>[key,1])),requests:1,
      tokenShapesJson:raw?{$literal:null}:{$ifNull:['$tokenShapesJson',null]},...(!raw?{retained:{$literal:1}}:{})}},{$limit:5001}];
  const rows=await db.collection(table).aggregate(pipeline,{session,...(!session?{readConcern:{level:'snapshot'}}:{}),maxTimeMS:5000}).toArray();
  if(rows.length>5000) reject('Team usage projection resource limit');
  return rows;
}
export async function codexQuotaRows(db,session,ids) {
  if (ids && (!Array.isArray(ids) || ids.length>1024 || ids.some(id=>typeof id!=='string'))) reject('Invalid Codex quota projection scope');
  const rows=[],cursor=db.collection('provider_connections').find({provider:'codex',...(ids?{id:{$in:ids}}:{})},{session,projection:{id:1,provider_specific_data:1,_id:0}}).limit(1025);
  let bytes=0;
  for await (const row of cursor) {
    if (row.provider_specific_data!=null && typeof row.provider_specific_data!=='string') reject('Codex quota projection requires JSON text, not binary or native BSON values');
    if (rows.length===1024 || Buffer.byteLength(row.provider_specific_data??'')>2*1024*1024) reject('Codex quota projection resource limit');
    const result={id:row.id,quota_data:quotaJson(row.provider_specific_data)};
    bytes+=Buffer.byteLength(JSON.stringify(result)); if (bytes>4*1024*1024) reject('Codex quota projection response limit');
    rows.push(result);
  }
  return rows;
}
