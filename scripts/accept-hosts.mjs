// Verify application failover over the private host and ingress endpoints.
// The operator controls the fault and restarts the former owner.
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,lstatSync} from 'node:fs';
import {randomUUID} from 'node:crypto';
import path from 'node:path';
import {MongoClient} from 'mongodb';
import {loadHostEnvironment,validateHostEnvironment,hostStatus} from './host.mjs';
import {nativeRevision} from '../adapter/runtimeIdentity.mjs';
import {workspacePath} from '../adapter/workspacePath.mjs';
const [action,...args]=process.argv.slice(2);
const option=name=>{const i=args.indexOf(name);assert.ok(i>=0&&args[i+1],'Missing '+name);return args[i+1];};
const env=loadHostEnvironment(option('--env')),file=workspacePath(option('--record'),{existing:action!=='capture'});
assert.ok(env.HA_ACCEPTANCE_API_KEY,'Provide a synthetic staging management key privately as HA_ACCEPTANCE_API_KEY');
const fingerprint=validateHostEnvironment(env).sharedFingerprint;
const origin=value=>{const url=new URL(value);assert.ok(['http:','https:'].includes(url.protocol)&&!url.username&&!url.password&&!url.search&&!url.hash&&url.pathname==='/');return url.origin;};
let record;
const save=()=>writeFileSync(file,JSON.stringify(record,null,2),{mode:0o600});
async function api(endpoint,{method='GET',body,token=env.HA_ACCEPTANCE_API_KEY,allowMissing=false}={}){
  const route=endpoint.split('/').slice(0,3).join('/');
  let response;
  try {
    response=await fetch(record.ingress+endpoint,{method,body:body===undefined?undefined:JSON.stringify(body),headers:{authorization:'Bearer '+token,'content-type':'application/json'},redirect:'error',signal:AbortSignal.timeout(10000)});
  } catch {
    assert.fail('Acceptance API '+method+' '+route+' did not return a response');
  }
  if(allowMissing&&response.status===404){await response.arrayBuffer();return null;}
  assert.ok(response.ok,'Acceptance API '+endpoint+' returned '+response.status);return response.json();
}
async function owned(state){
  assert.equal(state.active,true);assert.equal(state.nativeRevision,nativeRevision(),'Native release mismatch');assert.equal(state.sharedFingerprint,fingerprint,'Shared configuration mismatch');
  const client=new MongoClient(env.HA_MONGO_URI,{maxPoolSize:1,timeoutMS:7000,readConcern:{level:'majority'}});
  try{await client.connect();const writer=await client.db(env.HA_MONGO_DATABASE).collection('_ha_serial').findOne({_id:'writer'});assert.equal(state.instanceId,writer.ownerId);assert.equal(state.epoch,writer.epoch);}finally{await client.close();}
}
function comboSettings({strategy, models, config}) {
  return {strategy, models, config: config ?? null};
}
async function preserved(){
  const settings=await api('/api/settings');assert.equal(settings.stickyRoundRobinLimit,record.sticky,'Existing Settings changed or were lost');
  const keys=await api('/api/keys');const list=Array.isArray(keys)?keys:keys.keys;assert.ok(Array.isArray(list));assert.ok(list.some(key=>key.id===record.durableKey.id&&key.name===record.durableKey.name));
  const memory=await api('/api/memory/'+record.memoryId);assert.equal(memory.memory.content,record.content);
  if (record.combo) {
    const combo = await api('/api/combos/' + record.combo.id);
    assert.equal(combo.id, record.combo.id);
    assert.equal(combo.name, record.combo.name);
    assert.deepEqual(comboSettings(combo), record.combo.settings, 'Persisted Combo settings changed');
  }
}
async function main(){
  assert.ok(['capture','watch-promotion','verify-rejoin','cleanup'].includes(action),'Use capture|watch-promotion|verify-rejoin|cleanup --env PRIVATE_ENV --record PRIVATE_RECORD');
  if(action==='capture'){
    record={id:randomUUID(),database:env.HA_MONGO_DATABASE,fingerprint,a:origin(option('--a')),b:origin(option('--b')),ingress:origin(option('--ingress')),phase:'capturing'};assert.notEqual(record.a,record.b);
    // Exclusive creation records intent before any synthetic management write.
    writeFileSync(file,JSON.stringify(record),{flag:'wx',mode:0o600});
    const states=await Promise.all([record.a,record.b].map(base=>hostStatus(base,env.HA_STATUS_TOKEN)));assert.equal(states.filter(state=>state.active).length,1);assert.notEqual(states[0].instanceId,states[1].instanceId);assert.equal(states[0].buildId,states[1].buildId);
    for(const state of states){assert.equal(state.nativeRevision,nativeRevision());assert.equal(state.sharedFingerprint,fingerprint);}
    const active=states.find(state=>state.active);await owned(active);record.oldOrigin=states[0].active?record.a:record.b;record.peerOrigin=states[0].active?record.b:record.a;record.old=active;record.sticky=(await api('/api/settings')).stickyRoundRobinLimit;save();
    const create=async name=>{const key=await api('/api/keys',{method:'POST',body:{name,scopes:['manage']}});assert.ok(key.id&&key.key);return{id:key.id,key:key.key,name};};
    record.durableKey=await create('HA acceptance durable '+record.id);save();record.revocableKey=await create('HA acceptance revocation '+record.id);save();
    const comboName='ha-acceptance-'+record.id;
    const models = ['openai/gpt-4o-mini', 'openai/gpt-4o'];
    const config = {maxRetries: 2, retryDelayMs: 350, timeoutMs: 15000};
    const combo = await api('/api/combos', {
      method: 'POST',
      body: {name: comboName, models, strategy: 'priority', config},
    });
    assert.ok(combo.id);
    const savedCombo = await api('/api/combos/' + combo.id);
    assert.equal(savedCombo.id, combo.id);
    assert.equal(savedCombo.name, comboName);
    assert.equal(savedCombo.strategy, 'priority');
    assert.deepEqual(savedCombo.models.map(step => step.model), models);
    for (const [key, value] of Object.entries(config)) assert.equal(savedCombo.config?.[key], value);
    record.combo = {id: combo.id, name: comboName, settings: comboSettings(savedCombo)};
    save();
    record.content='Synthetic two-host acceptance '+record.id;
    const memory=await api('/api/memory',{method:'POST',body:{apiKeyId:record.durableKey.id,type:'factual',key:'HA acceptance '+record.id,content:record.content}});record.memoryId=typeof memory.id==='string'?memory.id:memory.id?.id;assert.equal(typeof record.memoryId,'string','Original Memory API must return an identifiable record');record.phase='captured';save();await preserved();
    console.log(JSON.stringify({pass:true,phase:record.phase,activeOrigin:record.oldOrigin,epoch:active.epoch,record:file,note:'Contains private synthetic keys; physical-host independence must be verified by the operator'},null,2));return;
  }
  const stat=lstatSync(file);assert.equal(stat.uid,process.getuid());assert.equal(stat.mode&0o077,0,'Acceptance record must be private');record=JSON.parse(readFileSync(file,'utf8'));assert.equal(record.database,env.HA_MONGO_DATABASE);assert.equal(record.fingerprint,fingerprint);
  if(action==='watch-promotion'){
    assert.equal(record.phase,'captured');console.log('ARMED: now stop/isolate only the identified Active using the approved host procedure. No provider request will be replayed.');
    const deadline=Date.now()+180000;let failureAt,after,lastError;
    while(Date.now()<deadline){const probeStart=performance.now();const states=await Promise.all([record.oldOrigin,record.peerOrigin].map(base=>hostStatus(base,env.HA_STATUS_TOKEN,{timeoutMs:1500}).catch(()=>null)));
      if(!states[0]?.active)failureAt??=probeStart;
      if(failureAt!==undefined&&states[1]?.active&&states[1].epoch>record.old.epoch){try{assert.notEqual(states[1].instanceId,record.old.instanceId);await owned(states[1]);await preserved();after=states[1];break;}catch(error){lastError=error;}}
      await new Promise(resolve=>setTimeout(resolve,500));
    }
    assert.ok(after,'No verified promotion/ingress preservation before deadline'+(lastError?' (last data/ownership verification failed)':''));
    record.promoted=after;record.observedRtoMs=Math.round(performance.now()-failureAt);record.phase='promoted';save();
    await api('/api/keys/'+record.revocableKey.id,{method:'DELETE'});
    const denied=await fetch(record.ingress+'/api/settings',{headers:{authorization:'Bearer '+record.revocableKey.key},redirect:'error',signal:AbortSignal.timeout(10000)});assert.ok([401,403].includes(denied.status),'Deleted key still authorizes');await denied.arrayBuffer();record.revoked=true;save();
    console.log(JSON.stringify({pass:true,oldEpoch:record.old.epoch,newEpoch:after.epoch,oldOwnerId:record.old.instanceId,newOwnerId:after.instanceId,combo:record.combo,observedRtoMs:record.observedRtoMs,measurement:'From start of first failed/not-active old-host probe to successful preserved-data ingress read; 500ms polling and 1500ms probe deadlines, not exact failure time',revocation:true},null,2));return;
  }
  if(action==='verify-rejoin'){
    assert.equal(record.phase,'promoted');const [old,active]=await Promise.all([record.oldOrigin,record.peerOrigin].map(base=>hostStatus(base,env.HA_STATUS_TOKEN)));assert.equal(old.role,'standby');assert.notEqual(old.instanceId,record.old.instanceId);assert.equal(active.instanceId,record.promoted.instanceId);await owned(active);await preserved();
    const denied=await fetch(record.ingress+'/api/settings',{headers:{authorization:'Bearer '+record.revocableKey.key},redirect:'error',signal:AbortSignal.timeout(10000)});assert.ok([401,403].includes(denied.status));await denied.arrayBuffer();record.phase='rejoined';save();console.log(JSON.stringify({pass:true,phase:record.phase,epoch:active.epoch,preserved:true,revocation:true},null,2));return;
  }
  assert.equal(action,'cleanup');assert.ok(record.id&&record.durableKey?.name==='HA acceptance durable '+record.id,'Refusing unknown cleanup identity');
  if(record.phase==='cleaned'){console.log('PASS acceptance record was already cleaned; no further mutation');return;}
  if(record.phase!=='cleaning'){
    await preserved();
    record.phase='cleaning';
    save();
  }
  if(record.combo){
    assert.equal(record.combo.name,'ha-acceptance-'+record.id);
    await api('/api/combos/'+record.combo.id,{method:'DELETE',allowMissing:true});
  }
  await api('/api/memory/'+record.memoryId,{method:'DELETE',allowMissing:true});
  await api('/api/keys/'+record.durableKey.id,{method:'DELETE',allowMissing:true});
  if(!record.revoked){assert.equal(record.revocableKey?.name,'HA acceptance revocation '+record.id);await api('/api/keys/'+record.revocableKey.id,{method:'DELETE',allowMissing:true});}
  record.phase='cleaned';save();console.log('PASS removed only this private record’s synthetic records (API keys, Memory and Combo when present); database, acceptance evidence and unrelated records preserved');
}
main().catch(error=>{console.error('ACCEPTANCE_FAILED '+(error.code||error.name)+': '+(error.name==='AssertionError'?error.message:'Inspect private record, authorized endpoints and service logs; no credentials printed'));process.exitCode=1;});
