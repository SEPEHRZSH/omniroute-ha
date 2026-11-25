// Native equivalents of the final pinned provider/lease retirement triggers.
// They execute inside the caller's fenced transaction, never as background work.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

const sources=[
  ['164_retire_microsoft_designer_web','95a4b4a330abbb61846abe3d7b48be3333cdee8f2db0e120795791780719819c'],
  ['165_retire_felo_web','ad197e2a81e99f59f2347d66aabb8d8f1afffc5902c5a0b83c5a67ed98b74ef0'],
  ['167_retire_qwen_web','6c2538e58486f2d8f9f123997cd487776135f00e777d70dc0c7e95aa42894d0d'],
  ['168_retire_chatgpt_web','a9a5c6f9fa23940afffb06e2ad4cf0ef29abf2346acaf68ba7d8a32b4a6b520e'],
  ['171_restore_chatgpt_web_cleanroom','3c2c1656c52a8fa103023272934a6d00d0a28d1c869dbd08b5fc12733e513413'],
];
const definitions=new Map();
for(const [name,hash] of sources) {
  const sql=readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/'+name+'.sql',import.meta.url),'utf8');
  assert.equal(createHash('sha256').update(sql).digest('hex'),hash,'Provider retirement source changed: '+name);
  for(const match of sql.matchAll(/CREATE TRIGGER (?:IF NOT EXISTS )?(\w+)[\s\S]*?END;/g))definitions.set(match[1],match[0]);
}
assert.equal(definitions.size,20,'Final provider retirement trigger catalog changed');
export function providerRetirementTriggers(){return new Map(definitions);}
const failure=(code,message)=>Object.assign(new Error(message),{code});
const unicodeSpace=/^[\u0009-\u000d\u0020\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+|[\u0009-\u000d\u0020\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+$/g;
const asciiSpace=/^[\u0009-\u000d ]+|[\u0009-\u000d ]+$/g;
const normalize=(value,ascii=false)=>typeof value==='string'?value.replace(ascii?asciiSpace:unicodeSpace,'').replace(/[A-Z]/g,char=>char.toLowerCase()):null;
// Reverse creation order matches the final SQLite AFTER triggers when a lease
// names one retired group but references a connection from a different group.
const groups=[
  {names:['cgpt-web'],source:'chatgpt-web',version:'3.8.51'},
  {names:['qwen-web','qw'],source:'qwen-web',version:'3.8.51',preserveEnded:true},
  {names:['felo-web','felo'],source:'felo-web',version:'3.8.50'},
  {names:['microsoft-designer-web','msdesigner'],source:'microsoft-designer-web',ascii:true,preserveEnded:true,designer:true},
];
const matches=(group,value)=>group.names.includes(normalize(value,group.ascii));
const watched=['provider','is_active','test_status','error_code','last_error','last_error_type','last_error_source','last_error_at'];
const identityGroups=[['chatgpt-web','cgpt-web'],['felo-web','felo']];
function tombstone(group) {
  return {is_active:0,test_status:'unavailable',last_error:group.designer?'Provider retired from OmniRoute runtime.':'Provider integration retired from OmniRoute v'+group.version,
    last_error_type:group.designer?'provider_retired':'provider_removed',last_error_source:'migration:retire-'+group.source,
    ...(!group.designer?{error_code:'PROVIDER_REMOVED'}:{})};
}
const differs=(group,row,changes)=>Object.entries(changes).some(([key,value])=>(group.designer&&key==='is_active'?(row[key]??0):row[key])!==value);
export async function inspectProviderRetirement(db,session) {
  const options={session,maxTimeMS:5000};
  const rows=await db.collection('provider_connections').find({}, {...options,projection:Object.fromEntries(['id',...watched].map(name=>[name,1]))}).limit(1001).toArray();
  const leases=await db.collection('exclusive_connection_leases').find({state:'ACTIVE'},{...options,projection:{id:1,provider:1,connection_id:1}}).limit(1001).toArray();
  if(rows.length>1000||leases.length>1000)throw failure('HA_RESOURCE_LIMIT','Retirement verification exceeds bounded 1000-provider/active-lease profile');
  const byId=new Map(rows.filter(row=>row.id!=null).map(row=>[row.id,row.provider]));
  return {
    providers:rows.filter(row=>{const group=groups.find(group=>matches(group,row.provider));return row.id!=null&&group&&(differs(group,row,tombstone(group))||!group.designer&&row.last_error_at==null);}).map(row=>row.id),
    leases:leases.filter(row=>groups.some(group=>matches(group,row.provider)||matches(group,byId.get(row.connection_id)))).map(row=>row.id),
  };
}
export async function verifyProviderRetirement(db,session) {
  const invalid=await inspectProviderRetirement(db,session);
  if(invalid.providers.length||invalid.leases.length)throw failure('HA_SCHEMA_MISMATCH','Legacy Provider retirement state requires offline repair-provider-retirement before startup or backup; restore may explicitly use --repair-retirement');
  return invalid;
}
export async function protectProviderIdentity(db,session,row,before,event,columns) {
  if(event==='update'&&!columns.has('provider'))return;
  const previous=before??(row.id==null?null:await db.collection('provider_connections').findOne({id:row.id},{session}));
  const old=normalize(previous?.provider),next=normalize(row.provider);
  if(next!=null&&identityGroups.some(names=>names.includes(old)&&!names.includes(next)))
    throw failure('SQLITE_CONSTRAINT_TRIGGER','Retired provider connection identity cannot be changed');
}
export function assertProviderRetirementSchema(spec,table) {
  const fields=table==='provider_connections'?[...watched,'id','updated_at']:['id','provider','connection_id','state','ended_at','end_reason'];
  assert.ok(spec&&fields.every(name=>spec.columns.some(column=>column.name===name)), 'Provider retirement schema is incomplete: '+table);
}
export async function applyProviderRetirement({db,session,table,row,event,columns,schema,store,now}) {
  if(table==='provider_connections') {
    const group=groups.find(group=>matches(group,row.provider));
    const fields=group?.designer?watched.filter(name=>!['error_code','last_error_at'].includes(name)):watched;
    if(!group||row.id==null||event==='update'&&!fields.some(name=>columns.has(name)))return row;
    assertProviderRetirementSchema(await schema(table),table);
    assertProviderRetirementSchema(await schema('exclusive_connection_leases'),'exclusive_connection_leases');
    const changes=tombstone(group),different=differs(group,row,changes);
    if(group.designer&&event==='update'&&!different)return row;
    let persisted=row;
    if(group.designer||different||row.last_error_at==null) {
      const time=await now();
      persisted={...row,...changes,last_error_at:group.designer?(row.last_error_at??time):time,...(!group.designer?{updated_at:time}:{})};
      await store(table,row,persisted);
    }
    const leases=await db.collection('exclusive_connection_leases').find({state:'ACTIVE',connection_id:row.id},{session}).limit(1001).toArray();
    if(leases.length>1000)throw failure('HA_RESOURCE_LIMIT','Provider retirement exceeds bounded active lease profile');
    for(const lease of leases)await invalidateLease(group,lease);
    return persisted;
  }
  if(table==='exclusive_connection_leases'&&row.state==='ACTIVE'&&(event==='insert'||['provider','connection_id','state'].some(name=>columns.has(name)))) {
    const provider=row.connection_id==null?null:await db.collection('provider_connections').findOne({id:row.connection_id},{session,projection:{provider:1}});
    const group=groups.find(group=>matches(group,row.provider)||matches(group,provider?.provider));
    if(group){assertProviderRetirementSchema(await schema(table),table);return invalidateLease(group,row);}
  }
  return row;
  async function invalidateLease(group,lease) {
    const changed={...lease,state:'INVALIDATED',ended_at:group.preserveEnded?(lease.ended_at??await now()):await now(),end_reason:group.designer?'AUTHORIZATION_CHANGED':'CONNECTION_INELIGIBLE'};
    await store('exclusive_connection_leases',lease,changed);return changed;
  }
}
