// One pinned CAS update, preserving SQLite json_remove's text/numeric lexemes.
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { parseTree,modify,applyEdits,createScanner,SyntaxKind } from 'jsonc-parser';
const reject=message=>{throw Object.assign(new Error(message),{code:'HA_UNSUPPORTED_SQL'});};
const source=readFileSync(new URL('../upstream/omniroute/src/lib/db/providers/rateLimit.ts',import.meta.url),'utf8');
if(createHash('sha256').update(source).digest('hex')!=='1188be8e5053805a1bb51870a863e6f4c4edbe61af5f92a6f4146202620e014d') reject('Provider recovery source changed');
export const providerRecoverySql=source.slice(source.indexOf('export async function clearConnectionErrorIfUnchanged(')).match(/\.prepare\(\s*`([\s\S]*?)`\s*\)/)?.[1];
if(!providerRecoverySql) reject('Provider recovery SQL extraction failed');
const providers=readFileSync(new URL('../upstream/omniroute/src/lib/db/providers.ts',import.meta.url),'utf8');
if(createHash('sha256').update(providers).digest('hex')!=='68eee6bd6d9938f9c6cead176e482cc987485782d4a74892e5c00ca0d5fb8f62')reject('Provider identity source changed');
const normalize=sql=>sql.replace(/\s+/g,' ').trim();
export const providerIdentitySql=[...providers.matchAll(/(?:"|`)(SELECT \* FROM provider_connections[^"`]*?json_extract[^"`]*?)(?:"|`)/g)].map(match=>match[1]);
if(providerIdentitySql.length!==4)reject('Provider identity SQL extraction failed');
const reconciliation=readFileSync(new URL('../upstream/omniroute/src/lib/db/providers/usageIdentityReconciliation.ts',import.meta.url),'utf8');
if(createHash('sha256').update(reconciliation).digest('hex')!=='200b1ae1dd7e8dd881a82a6fe212f0ed1d453e35109be280a5dd8c3acfe97919')reject('Provider usage identity source changed');
export const providerReconciliationSql=reconciliation.match(/`(UPDATE usage_history[\s\S]*?)`/)?.[1];
if(!providerReconciliationSql)reject('Provider usage identity SQL extraction failed');
export function parseProviderQuery(sql,bindings) {
  if(normalize(sql)===normalize(providerReconciliationSql)) {
    if(bindings.length!==6||![0,2,4,5].every(index=>typeof bindings[index]==='string')||!Number.isSafeInteger(bindings[1])||bindings[1]<0||bindings[1]!==bindings[3])reject('Provider usage identity binding types');
    return {type:'ha_provider_identity_update',bindings};
  }
  const identity=providerIdentitySql.findIndex(query=>normalize(query)===normalize(sql));
  if(identity>=0) {
    if(bindings.length!==[3,2,2,3][identity]||bindings.some(value=>typeof value!=='string'))reject('Provider identity binding types');
    return {type:'ha_provider_identity',identity,bindings};
  }
  if(sql.replace(/\s+/g,' ').trim()!==providerRecoverySql.replace(/\s+/g,' ').trim()) return null;
  if(bindings.length!==5 || bindings.some(value=>typeof value!=='string')) reject('Provider recovery binding types');
  return {type:'ha_provider_recovery',bindings};
}
function jsonIdentity(text,key) {
  if(text==null)return null;
  if(typeof text!=='string'||Buffer.byteLength(text)>2*1024*1024)reject('Provider identity JSON resource/type limit');
  const errors=[],tree=parseTree(text,errors,{disallowComments:true,allowTrailingComma:false});
  if(errors.length||!tree)throw Object.assign(new Error('malformed JSON'),{code:'SQLITE_ERROR'});
  // SQLite JSON1 selects the first duplicate property; JSON.parse selects the
  // last. These comparisons have no column affinity: numbers are not strings.
  const value=tree.type==='object'?tree.children?.find(property=>property.children?.[0]?.value===key)?.children?.[1]:null;
  if(!value||value.type==='null')return null;
  if(['string','number'].includes(value.type))return value.value;
  if(value.type==='boolean')return value.value?1:0;
  const fragment=text.slice(value.offset,value.offset+value.length),scanner=createScanner(fragment,true),parts=[];
  for(let token;(token=scanner.scan())!==SyntaxKind.EOF;)parts.push(fragment.slice(scanner.getTokenOffset(),scanner.getTokenOffset()+scanner.getTokenLength()));
  return parts.join('');
}
export async function executeProviderIdentity(db,session,ast,schema) {
  const spec=await schema('provider_connections'),[provider,first,second]=ast.bindings;
  const rows=await db.collection('provider_connections').find({provider,auth_type:'oauth'},{session}).sort({_ha_rowid:1}).limit(1001).toArray();
  if(rows.length>1000)reject('Provider identity exceeds bounded provider profile');
  let result=rows.filter(row=>{
    const workspace=jsonIdentity(row.provider_specific_data,'workspaceId');
    if(ast.identity===0)return workspace===first&&jsonIdentity(row.provider_specific_data,'chatgptUserId')===second;
    if(ast.identity===1)return (workspace===null||workspace==='')&&jsonIdentity(row.provider_specific_data,'chatgptUserId')===first;
    return workspace===first&&(ast.identity!==3||row.email===second);
  });
  if(ast.identity===2)result.sort((a,b)=>a.created_at==null?(b.created_at==null?0:-1):b.created_at==null?1:a.created_at<b.created_at?-1:a.created_at>b.created_at?1:0);
  if(ast.identity===3)result=result.slice(0,1);
  return result.map(row=>Object.fromEntries(spec.columns.map(column=>[column.name,row[column.name]??null])));
}
export async function executeProviderReconciliation(db,session,ast,schema) {
  const spec=await schema('usage_history');
  if(!session||['account_key','account_label','account_label_priority','connection_id'].some(name=>!spec.columns.some(column=>column.name===name)))reject('Provider usage identity requires fenced schema/session');
  const [key,priority,label,,connectionId,oldKey]=ast.bindings,previous={$ifNull:['$account_label_priority',0]};
  const result=await db.collection('usage_history').updateMany({connection_id:connectionId,account_key:oldKey},[{$set:{
    account_key:{$literal:key},account_label:{$cond:[{$gt:[priority,previous]},{$literal:label},'$account_label']},
    account_label_priority:{$max:[previous,priority]},
  }}],{session});
  return {changes:result.matchedCount};
}
function removeCooldowns(text) {
  if(text==null) return text;
  if(typeof text!=='string' || Buffer.byteLength(text)>2*1024*1024) reject('Provider recovery JSON resource/type limit');
  const errors=[],tree=parseTree(text,errors,{disallowComments:true,allowTrailingComma:false});
  if(errors.length) return text; // Original CASE json_valid leaves invalid JSON intact.
  if(tree?.type==='object') for(const key of ['codexScopeRateLimitedUntil','codexScopeRateLimitSource']) text=applyEdits(text,modify(text,[key],undefined,{}));
  const scanner=createScanner(text,true),parts=[];
  for(let token;(token=scanner.scan())!==SyntaxKind.EOF;) parts.push(text.slice(scanner.getTokenOffset(),scanner.getTokenOffset()+scanner.getTokenLength()));
  return parts.join('');
}
export async function executeProviderQuery(db,session,ast,schema) {
  const spec=await schema('provider_connections'),columns=['provider','provider_specific_data','test_status','last_error','last_error_at','last_error_type','last_error_source','error_code','rate_limited_until','backoff_level','updated_at','id'];
  if(!session || !spec.unique.some(keys=>keys.length===1 && keys[0]==='id') || columns.some(name=>!spec.columns.some(column=>column.name===name))) reject('Provider recovery requires fenced schema/session');
  const [updatedAt,id,status,errorAt,until]=ast.bindings;
  const collection=db.collection('provider_connections'),row=await collection.findOne({id},{session});
  if(!row || (row.test_status??'')!==status || (row.last_error_at??'')!==errorAt || (row.rate_limited_until??'')!==until) return {changes:0};
  const data=row.provider==='codex'?removeCooldowns(row.provider_specific_data):row.provider_specific_data;
  await collection.updateOne({_id:row._id},{$set:{test_status:'active',last_error:null,last_error_at:null,last_error_type:null,last_error_source:null,error_code:null,rate_limited_until:null,backoff_level:0,provider_specific_data:data,updated_at:updatedAt}},{session});
  return {changes:1};
}
