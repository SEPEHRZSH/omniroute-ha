// Retain the official decay policy/selection; replace only deletion with a
// fenced recheck and SQL DELETE in one MongoDB transaction.
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {MongoMemoryVectors} from './memoryVectors.mjs';
const source=new URL('../upstream/omniroute/src/lib/memory/typedDecay.ts',import.meta.url);
if(createHash('sha256').update(readFileSync(source)).digest('hex')!=='32d15003632ebbf659a2c800fa9cca8fb1562deecf57a56a6d054fce848b11c2')throw new Error('Memory decay policy changed');
let policy;
async function loadPolicy() {
  policy??=(async()=>{await import('./register.mjs');return import(source);})();return policy;
}
const types=['factual','episodic','procedural','semantic'];
function validate(config,now) {
  if(!config||config.enabled!==true||!Number.isSafeInteger(now)||Math.abs(now)>8640000000000000||
     !Number.isSafeInteger(config.accessImmunityThreshold)||config.accessImmunityThreshold<0||
     !config.ttlDaysByType||Object.keys(config.ttlDaysByType).length!==types.length||
     types.some(type=>config.ttlDaysByType[type]!==null&&(!Number.isSafeInteger(config.ttlDaysByType[type])||config.ttlDaysByType[type]<=0)))throw new TypeError('Invalid enabled Memory decay policy/clock');
}
export async function memoryDecaySchedule(env=process.env) {
  const {resolveTypedDecayConfig}=await loadPolicy(),config=resolveTypedDecayConfig(env),seconds=Number(env.MEMORY_TYPED_DECAY_SWEEP_INTERVAL);
  const interval=Number.isFinite(seconds)&&seconds>0?Math.floor(seconds*1000):0;
  if(config.enabled&&interval>2147483647)throw new RangeError('Memory decay interval exceeds the Node timer range');
  return {config,interval};
}
export async function deleteDecayedMemory(db,session,request,remove) {
  validate(request.config,request.now);
  if(!session||typeof request.memoryId!=='string'||!request.memoryId)throw new TypeError('Memory decay requires a fenced transaction and ID');
  const external=await db.collection('key_value').findOne({namespace:'settings',key:'qdrantEnabled'},{session});
  let enabled=false;try{enabled=external?JSON.parse(external.value):false;}catch{throw Object.assign(new Error('Invalid external Memory configuration'),{code:'HA_MEMORY_DECAY_PROFILE'});}
  // Deleting a remote Qdrant point cannot join this MongoDB transaction. Never
  // silently leave that configured store stale or perform an unfenced request.
  if(enabled===true)throw Object.assign(new Error('Typed decay with external Qdrant requires a certified recovery protocol'),{code:'HA_MEMORY_DECAY_PROFILE'});
  const {isMemoryDecayed}=await loadPolicy();
  const row=await db.collection('memories').findOne({id:request.memoryId},{session});
  if(!row)return false;
  const current={id:row.id,type:row.type,accessCount:typeof row.access_count==='number'?row.access_count:0,
    createdAt:new Date(String(row.created_at)),lastAccessedAt:row.last_accessed_at?new Date(String(row.last_accessed_at)):null};
  if(!isMemoryDecayed(current,request.config,new Date(request.now)))return false;
  return remove(row.id); // Original SQL path preserves constraints/cascades too.
}
export async function runMemoryDecay(runtime,{config,dryRun=false,apiKeyId}={}) {
  runtime.assertOwner();
  const {resolveTypedDecayConfig,isMemoryDecayed,SWEEP_SCAN_CAP}=await loadPolicy();
  config??=resolveTypedDecayConfig();
  if(!config.enabled&&!dryRun)return {scanned:0,decayed:0,deletedIds:[],skippedDisabled:true,capped:false};
  runtime.assertOwner();
  const {listMemoriesForDecay}=await import('../upstream/omniroute/src/lib/memory/store.ts');
  const now=runtime.db.serverTime();
  if(!dryRun)validate(config,now);
  const candidates=listMemoriesForDecay({apiKeyId,limit:SWEEP_SCAN_CAP+1}),batch=candidates.slice(0,SWEEP_SCAN_CAP);
  const decayed=batch.filter(row=>isMemoryDecayed(row,config,new Date(now))),deletedIds=[],vectors=new MongoMemoryVectors(runtime.db);
  if(!dryRun)for(const row of decayed) {
    // Yield between remote synchronous calls so lease renewal and request work
    // are not starved by a large pass. Every deletion rechecks current policy.
    await new Promise(resolve=>setImmediate(resolve));runtime.assertOwner();
    if(await vectors.deleteDecayedMemory(row.id,config,now))deletedIds.push(row.id);
  }
  runtime.assertOwner();
  return {scanned:batch.length,decayed:decayed.length,deletedIds,skippedDisabled:false,capped:candidates.length>SWEEP_SCAN_CAP};
}
