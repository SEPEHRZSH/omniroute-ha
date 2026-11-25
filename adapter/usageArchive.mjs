// Durable financial history is distinct from disposable request telemetry.
// Archive/delete and cumulative daily totals commit in the same fenced TX.
import {createHash} from 'node:crypto';
const fail=message=>{throw Object.assign(new Error(message),{code:'HA_ROLLUP_BASELINE'});};
const fields=['id','api_key_id','billing_team_id','team_rollup_processed_at','provider','model','timestamp','success','service_tier','provider_cost_usd',
  'tokens_input','tokens_output','tokens_cache_read','tokens_cache_creation','tokens_reasoning'];
export const archiveProjection=Object.fromEntries(fields.map(key=>[key,1]));
const lower=value=>typeof value==='string'?value.replace(/[A-Z]/g,letter=>letter.toLowerCase()):value;
export function archiveDay(value) {
  if(typeof value!=='string')fail('Archive requires timestamp text');
  const parsed=new Date(/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?$/.test(value)?value.replace(' ','T')+'Z':value);
  if(!Number.isFinite(parsed.getTime()))fail('Archive requires a valid timestamp');return parsed.toISOString().slice(0,10);
}
const shapeFields=['Input','Output','CacheRead','CacheCreation','Reasoning'];
const rowKey=row=>JSON.stringify([row.provider,row.model,row.date,row.serviceTier,row.hasStoredCost,...shapeFields.map(field=>row['request'+field+'Tokens'])]);
const dataKey=row=>JSON.stringify([lower(row.provider),lower(row.model),archiveDay(row.timestamp),row.service_tier||'standard',row.provider_cost_usd==null?0:1,
  ...['input','output','cache_read','cache_creation','reasoning'].map(field=>row['tokens_'+field]??0)]);
const tuple=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const finite=value=>{if(!Number.isFinite(value)||value<0)fail('Invalid archived cost');return value;};
const integer=value=>{if(!Number.isSafeInteger(value))fail('Archive counter overflow/type');return value;};
export function recordArchivePrices(preview,rows) {
  if(!preview||preview.prices||!Array.isArray(rows)||rows.length!==preview.rows.length)fail('Missing/repeated rollup pricing preview');
  const originals=new Map(preview.rows.map(row=>[rowKey(row),row])),prices=new Map();
  for(const row of rows) {
    const key=rowKey(row),original=originals.get(key);
    if(!original||prices.has(key)||Object.keys(row).length!==Object.keys(original).length+1||Object.entries(original).some(([key,value])=>row[key]!==value))fail('Rollup pricing does not match its snapshot');
    finite(row.totalCost);prices.set(key,row.totalCost/row.totalRequests);
  }
  preview.prices=prices;
}
const columns=['provider','model','date','total_requests','total_input_tokens','total_output_tokens','total_cost'];
const originalValues=row=>columns.map(key=>row?.[key]??null);
export async function archiveGlobalUpsert(db,session,values,sql,execute) {
  if(!session||values.length!==7||values.slice(0,3).some(value=>typeof value!=='string'))fail('Global archive upsert contract');
  const options={session},identity=values.slice(0,3),id=tuple(identity),collection=db.collection('_ha_usage_archive_totals');
  const prior=await db.collection('daily_usage_summary').findOne(Object.fromEntries(columns.slice(0,3).map((key,index)=>[key,identity[index]])),options);
  let meta=await collection.findOne({_id:id},options);
  if(meta) {
    if(meta.version!==1||JSON.stringify(meta.published)!==JSON.stringify(originalValues(prior)))fail('Published usage summary diverged from its archive baseline');
  } else {
    if(prior)fail('Existing global usage summary requires a verified archive baseline');
    meta={_id:id,version:1,archived:[0,0,0,0]};
  }
  const merged=[...identity,...values.slice(3).map((value,index)=>index===3?finite(meta.archived[index]+finite(value)):integer(meta.archived[index]+integer(value)))];
  const result=await execute({sql:'/* native cumulative archive */ '+sql,args:merged,mode:'run'});
  await collection.replaceOne({_id:id},{...meta,published:merged},{...options,upsert:true});return result;
}
export async function finishUsageArchive(db,session,preview,execute) {
  if(!session||!preview?.validated||!preview.prices||preview.finished)fail('Rollup cannot finish without its validated transaction/pricing');
  preview.finished=true;
  if(!preview.purge||!preview.selectedIds.length)return {deleted:0,hasMore:false};
  const options={session},rows=await db.collection('usage_history').find({_id:{$in:preview.selectedIds}},{...options,projection:archiveProjection}).toArray();
  if(rows.length!==preview.selectedIds.length)fail('Archive selection changed');
  const grouped=new Map(),documents=[];
  for(const row of rows) {
    if(!Number.isSafeInteger(row.id)||row.id<1)fail('Archive requires a stable usage row ID');
    const document={_id:row.id,_ha_archive_version:1,...Object.fromEntries(fields.map(key=>[key,row[key]??null]))};
    if(row.billing_team_id&&row.team_rollup_processed_at==null)fail('A Team usage row was not rolled up before deletion');
    documents.push(document);
    if(row.provider==null||row.provider===''||row.model==null||row.model==='')continue;
    const identity=[lower(row.provider),lower(row.model),archiveDay(row.timestamp)],id=tuple(identity);
    const price=row.provider_cost_usd==null?preview.prices.get(dataKey(row)):row.provider_cost_usd;
    if(price==null)fail('Deleted usage has no snapshot price');
    let value=grouped.get(id);if(!value){value=[0,0,0,0];grouped.set(id,value);}
    value[0]=integer(value[0]+1);value[1]=integer(value[1]+(row.tokens_input??0));value[2]=integer(value[2]+(row.tokens_output??0));value[3]=finite(value[3]+finite(price));
  }
  // Reusing a deleted explicit SQL id cannot overwrite an old financial row.
  if(await db.collection('_ha_usage_history').findOne({_id:{$in:documents.map(row=>row._id)}},options))fail('Usage IDs overlap previously archived history');
  await db.collection('_ha_usage_history').insertMany(documents,options);
  for(const [id,delta] of grouped) {
    const collection=db.collection('_ha_usage_archive_totals'),meta=await collection.findOne({_id:id},options);
    if(meta?.version!==1)fail('Deleted usage has no cumulative daily baseline');
    await collection.updateOne({_id:id},{$set:{archived:meta.archived.map((value,index)=>index===3?finite(value+delta[index]):integer(value+delta[index]))}},options);
  }
  const result=await execute({sql:'DELETE FROM usage_history WHERE id IN ('+rows.map(()=>'?').join(',')+')',args:rows.map(row=>row.id),mode:'run'});
  if(result.changes!==rows.length)fail('Archive/delete row count mismatch');
  return {deleted:result.changes,hasMore:await db.collection('usage_history').findOne({timestamp:{$lt:preview.before}},{...options,projection:{_id:1}})!==null};
}
