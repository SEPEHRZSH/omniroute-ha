// Exact SQL boundaries for official usage rollup and cache/audit retention.
// Pricing, callers and application transactions stay in the original code.
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {parseTree} from 'jsonc-parser';
const fail=message=>{throw Object.assign(new Error(message),{code:'HA_UNSUPPORTED_SQL'});};
const text=readFileSync(new URL('../upstream/omniroute/src/lib/usage/aggregateHistory.ts',import.meta.url),'utf8');
if(createHash('sha256').update(text).digest('hex')!=='69551a1787e53c3964e7e290be7710c396e2e484ba1b017feae0878473ecad8e')fail('Usage rollup source changed');
const source=text.slice(text.indexOf('export async function rollupUsageHistoryBeforeDate('),text.indexOf('export async function getRawDataCutoffDate('));
export const usageRollupSql=source.match(/\.prepare\(\s*`(SELECT[\s\S]*?)`\s*\)/)?.[1];
export const teamRollupSql=source.match(/const teamAggregateQuery = `([\s\S]*?)`;/)?.[1];
export const globalRollupSql=source.match(/\.prepare\(\s*`(INSERT INTO daily_usage_summary[\s\S]*?)`\s*\)/)?.[1];
export const rollupMarkerSql=source.match(/\.prepare\(\s*`(UPDATE usage_history[\s\S]*?)`\s*\)/)?.[1];
if(!usageRollupSql||!teamRollupSql||!globalRollupSql||!rollupMarkerSql)fail('Usage rollup SQL extraction failed');
const canonical=sql=>sql.replace(/\s+/g,' ').trim();
const compliance=readFileSync(new URL('../upstream/omniroute/src/lib/compliance/index.ts',import.meta.url),'utf8');
if(createHash('sha256').update(compliance).digest('hex')!=='d8a2607d81db1d717286868a8114571af8308a4d6e0700e93f223d320aec0c30')fail('Audit retention source changed');
export const auditPruneSql=compliance.match(/`(DELETE FROM audit_log\s+WHERE id IN \([\s\S]*?LIMIT -1 OFFSET \?\s*\))`/)?.[1];
if(!auditPruneSql)fail('Audit retention SQL extraction failed');
const reasoning=readFileSync(new URL('../upstream/omniroute/src/lib/db/reasoningCache.ts',import.meta.url),'utf8');
if(createHash('sha256').update(reasoning).digest('hex')!=='373cfe574e32aa660e6ef4a1f7d021cbb5c640f0fe53cc188126db2e3a25af89')fail('Reasoning cache source changed');
const reasoningEpoch=reasoning.match(/const EXPIRES_AT_EPOCH_SQL = `([^`]+)`;/)?.[1];
export {reasoningEpoch as reasoningExpirySql};
const reasoningQueries=new Map();
for(const [,template] of reasoning.matchAll(/`([^`]*\$\{EXPIRES_AT_EPOCH_SQL\}[^`]*)`/g)) {
  if(!/^\s*(?:SELECT|DELETE)\b/.test(template))continue;
  reasoningQueries.set(canonical(template.replaceAll('${EXPIRES_AT_EPOCH_SQL}',reasoningEpoch)),template.replaceAll('${EXPIRES_AT_EPOCH_SQL}','CAST(expires_at AS INTEGER)'));
}
const reasoningEntries=reasoning.match(/`(SELECT tool_call_id[^`]*\$\{where\}[^`]*)`/)?.[1];
if(!reasoningEpoch||reasoningQueries.size!==7||!reasoningEntries)fail('Reasoning cache SQL extraction failed');
for(const provider of [false,true])for(const model of [false,true]) {
  const condition=epoch=>'WHERE '+epoch+" > unixepoch('now')"+(provider?' AND provider = ?':'')+(model?' AND model = ?':'');
  reasoningQueries.set(canonical(reasoningEntries.replace('${where}',condition(reasoningEpoch))),reasoningEntries.replace('${where}',condition('CAST(expires_at AS INTEGER)')));
}
export const reasoningRetentionSql=[...reasoningQueries.keys()];
export function assertRetentionMarkerSchema(spec) {
  const column=spec.columns.find(column=>column.name==='team_rollup_processed_at');
  // A CHECK attached to another column can still reference this marker. Native
  // updates must not bypass any expression that the generic SQL path enforces.
  if(column?.type!=='TEXT'||column.allowed||column.numericCheck||spec.columns.some(column=>column.checkExpression)||spec.checks?.length||spec.unique.some(keys=>keys.includes(column.name))||spec.foreignKeys?.some(key=>key.columns.includes(column.name)))fail('Unsupported usage rollup marker constraints');
}
export function parseRetentionQuery(sql,bindings) {
  const reasoningSql=reasoningQueries.get(canonical(sql));
  if(reasoningSql)return {type:/^DELETE/.test(reasoningSql)?'ha_reasoning_write':'ha_reasoning_read',sql:reasoningSql,bindings};
  if(canonical(sql)===canonical(auditPruneSql)) {
    if(bindings.length!==2||(bindings[0]!==null&&typeof bindings[0]!=='string')||!Number.isSafeInteger(bindings[1])||bindings[1]<0)fail('Audit retention binding contract');
    return {type:'ha_audit_prune',action:bindings[0],offset:bindings[1]};
  }
  if(canonical(sql)===canonical(globalRollupSql)) {
    if(bindings.length!==7||bindings.slice(0,3).some(value=>typeof value!=='string')||bindings.slice(3).some(value=>!Number.isFinite(value)))fail('Global rollup binding contract');
    return {type:'ha_usage_rollup_upsert',bindings};
  }
  if(canonical(sql)===canonical(rollupMarkerSql)) {
    if(bindings.length!==2||bindings.some(value=>typeof value!=='string'||value.length>64))fail('Rollup marker binding contract');
    return {type:'ha_usage_rollup_marker',bindings,before:bindings[1]};
  }
  const type=canonical(sql)===canonical(usageRollupSql)?'ha_usage_rollup_read':canonical(sql)===canonical(teamRollupSql)?'ha_team_rollup_write':null;
  if(!type)return null;
  if(bindings.length!==(type==='ha_usage_rollup_read'?1:2)||bindings.some(value=>typeof value!=='string'||value.length>64)||bindings.length===2&&bindings[0]!==bindings[1])fail('Usage rollup binding contract');
  return {type,before:bindings[0]};
}
const tokenFields=['input','output','cache_read','cache_creation','reasoning'];
const date={$dateToString:{date:{$convert:{input:'$timestamp',to:'date',onError:null,onNull:null}},format:'%Y-%m-%d',timezone:'UTC',onNull:null}};
export const retentionDayMatch=days=>({$expr:{$in:[date,{$literal:days}]}});
const coalesce=field=>({$ifNull:['$'+field,0]});
const tier={$cond:[{$in:[{$ifNull:['$service_tier',null]},[null,'']]},'standard','$service_tier']};
const integer=value=>{if(!Number.isSafeInteger(value))fail('Rollup integer resource/type limit');return value;};
const add=(a,b)=>integer(a+b);
function shapes(json) {
  if(json==null)return new Map();
  if(typeof json!=='string'||Buffer.byteLength(json)>1024*1024)fail('Team rollup shape JSON resource/type limit');
  const errors=[],tree=parseTree(json,errors,{disallowComments:true,allowTrailingComma:false});
  if(errors.length||tree?.type!=='object')fail('Team rollup shape JSON must be an object');
  const result=new Map();
  for(const node of tree.children??[]) {
    const [key,value]=node.children;
    if(value.type!=='number'||value.value<0)fail('Team rollup shape counts must be nonnegative integers');
    result.set(key.value,add(result.get(key.value)??0,integer(value.value)));
  }
  return result;
}
function mergeShapes(previous,total,incoming) {
  if(total>0&&previous==null)return null; // Unknown legacy shape must stay unknown.
  const result=shapes(previous);
  for(const [key,count] of incoming)result.set(key,add(result.get(key)??0,count));
  const json=JSON.stringify(Object.fromEntries([...result].sort(([a],[b])=>Buffer.compare(Buffer.from(a),Buffer.from(b)))));
  if(Buffer.byteLength(json)>1024*1024)fail('Merged Team rollup shape resource limit');
  return json;
}
export async function executeRetentionQuery(db,session,ast,schema,asciiLower,execute) {
  if(ast.type==='ha_audit_prune') {
    if(!session)fail('Audit retention requires a fenced transaction');
    const spec=await schema('audit_log'),column=name=>spec.columns.find(column=>column.name===name);
    if(column('id')?.type!=='INTEGER'||!column('id').primary||spec.withoutRowid||
      ['timestamp','action'].some(name=>column(name)?.type!=='TEXT'||!column(name).notNull||column(name).collation))fail('Unsupported audit retention schema');
    if(ast.action===null)return {changes:0}; // SQL action = NULL matches no rows.
    const boundary=await db.collection('audit_log').find({action:ast.action},{session,projection:{timestamp:1,id:1},maxTimeMS:5000})
      .sort({timestamp:-1,id:-1}).skip(ast.offset).limit(1).next();
    if(!boundary)return {changes:0};
    if(typeof boundary.timestamp!=='string'||!Number.isSafeInteger(boundary.id))fail('Unsupported audit retention boundary');
    // Preserve FK/trigger behavior through the ordinary DELETE implementation.
    // The boundary and deletion share the same fenced transaction snapshot.
    return execute({sql:'DELETE FROM audit_log WHERE action=? AND (timestamp<? OR (timestamp=? AND id<=?))',
      args:[ast.action,boundary.timestamp,boundary.timestamp,boundary.id],mode:'run'});
  }
  const spec=await schema('usage_history');
  const required=['provider','model','timestamp','service_tier','provider_cost_usd',...tokenFields.map(field=>'tokens_'+field)];
  if(ast.type==='ha_team_rollup_write')required.push('billing_team_id','api_key_id','api_key_name','success','team_rollup_processed_at');
  if(required.some(name=>!spec.columns.some(column=>column.name===name)))fail('Incomplete usage rollup schema');
  const options={session,maxTimeMS:5000,...(!session?{readConcern:{level:'snapshot'}}:{})};
  if(ast.type==='ha_usage_rollup_read') {
    const hasCost={$ne:[{$ifNull:['$provider_cost_usd',null]},null]};
    const key={provider:asciiLower('$provider'),model:asciiLower('$model'),date,serviceTier:tier,hasStoredCost:{$cond:[hasCost,1,0]}};
    const sums={},projection={_id:0,provider:'$_id.provider',model:'$_id.model',date:'$_id.date',serviceTier:'$_id.serviceTier',hasStoredCost:'$_id.hasStoredCost',totalRequests:1,storedCost:1};
    for(const field of tokenFields) {
      const camel=field.replace(/_([a-z])/g,(_,letter)=>letter.toUpperCase()),title=camel[0].toUpperCase()+camel.slice(1);
      key['request'+title+'Tokens']=coalesce('tokens_'+field);
      sums[camel+'Tokens']={$sum:coalesce('tokens_'+field)};
      projection['request'+title+'Tokens']='$_id.request'+title+'Tokens';projection[camel+'Tokens']=1;
    }
    const rows=await db.collection('usage_history').aggregate([
      {$match:{timestamp:{$lt:ast.before},provider:{$nin:[null,'']},model:{$nin:[null,'']}}},
      ...(ast.days?[{$match:{$expr:{$in:[date,{$literal:ast.days}]}}}]:[]),
      {$group:{_id:key,totalRequests:{$sum:1},storedCost:{$sum:coalesce('provider_cost_usd')},...sums}},{$project:projection},{$limit:5001},
    ],options).toArray();
    if(rows.length>5000)fail('Usage rollup exceeds 5000 token-shape groups');
    return rows;
  }
  if(!session)fail('Team rollup requires a fenced transaction');
  const normalized=(field,fallback)=>({$cond:[{$in:[{$ifNull:['$'+field,null]},['',null]]},fallback,asciiLower('$'+field)]});
  const key={team_id:'$billing_team_id',api_key_id:{$cond:[{$in:[{$ifNull:['$api_key_id',null]},['',null]]},'unknown','$api_key_id']},provider:normalized('provider','__unknown_provider__'),model:normalized('model','__unknown_model__'),service_tier:tier,date,
    shape:tokenFields.map(field=>coalesce('tokens_'+field))};
  const rows=await db.collection('usage_history').aggregate([
    {$match:{timestamp:{$lt:ast.before},billing_team_id:{$nin:[null,'']},team_rollup_processed_at:null}},
    ...(ast.days?[{$match:{$expr:{$in:[date,{$literal:ast.days}]}}}]:[]),
    {$group:{_id:key,api_key_name:{$max:{$cond:[{$eq:['$api_key_name','']},null,'$api_key_name']}},count_all:{$sum:1},count_success:{$sum:{$cond:[{$eq:['$success',1]},1,0]}}}},{$limit:5001},
  ],options).toArray();
  if(rows.length>5000)fail('Team rollup exceeds 5000 token-shape groups');
  const grouped=new Map(),keys=['team_id','api_key_id','provider','model','service_tier','date'];
  const totals=['total_requests','successful_requests',...tokenFields.map(field=>'total_'+field+'_tokens'),...tokenFields.map(field=>'successful_'+field+'_tokens')];
  for(const row of rows) {
    if(row._id.date==null)fail('Team rollup requires a valid timestamp');
    row._id.shape.forEach(integer);
    const identity=keys.map(key=>row._id[key]),id=JSON.stringify(identity);
    let value=grouped.get(id);
    if(!value){value={...Object.fromEntries(keys.map((key,index)=>[key,identity[index]])),api_key_name:null,...Object.fromEntries(totals.map(key=>[key,0])),all:new Map(),successful:new Map()};grouped.set(id,value);}
    if(row.api_key_name!=null&&(value.api_key_name==null||Buffer.compare(Buffer.from(row.api_key_name),Buffer.from(value.api_key_name))>0))value.api_key_name=row.api_key_name;
    value.total_requests=add(value.total_requests,row.count_all);value.successful_requests=add(value.successful_requests,row.count_success);
    for(const [index,field] of tokenFields.entries()) {
      value['total_'+field+'_tokens']=add(value['total_'+field+'_tokens'],integer(row._id.shape[index]*row.count_all));
      value['successful_'+field+'_tokens']=add(value['successful_'+field+'_tokens'],integer(row._id.shape[index]*row.count_success));
    }
    const shape=JSON.stringify(row._id.shape);value.all.set(shape,row.count_all);value.successful.set(shape,row.count_success);
  }
  const columns=[...keys,'api_key_name',...totals,'token_shapes_json','successful_token_shapes_json'];
  const sql='INSERT INTO daily_team_usage_summary('+columns.join(',')+') VALUES ('+columns.map(()=>'?').join(',')+') ON CONFLICT('+keys.join(',')+') DO UPDATE SET '+columns.filter(key=>!keys.includes(key)).map(key=>key+'=excluded.'+key).join(',');
  let changes=0;
  for(const value of grouped.values()) {
    const before=(await execute({sql:'SELECT * FROM daily_team_usage_summary WHERE '+keys.map(key=>key+'=?').join(' AND '),args:keys.map(key=>value[key]),mode:'get'}));
    value.token_shapes_json=mergeShapes(before?.token_shapes_json,before?.total_requests??0,value.all);
    value.successful_token_shapes_json=mergeShapes(before?.successful_token_shapes_json,before?.successful_requests??0,value.successful);
    if(before) {
      value.api_key_name??=before.api_key_name;
      for(const key of totals)value[key]=add(integer(before[key]),value[key]);
    }
    changes+=(await execute({sql,args:columns.map(key=>value[key]),mode:'run'})).changes;
  }
  return {changes};
}
