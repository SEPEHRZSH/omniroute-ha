// Durable Codex import receipts. No provider row is made routable before the
// external result is durable; the original SQL writer attaches it atomically.
import {createHmac} from 'node:crypto';
const fail=(code,message)=>{throw Object.assign(new Error(message),{code});};
const sha=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
export function credentialTokenHash(value) {
  if(typeof value!=='string'||!value||!process.env.STORAGE_ENCRYPTION_KEY?.trim())fail('HA_CREDENTIAL_POLICY','Credential fingerprint requires the shared encryption key');
  return createHmac('sha256',process.env.STORAGE_ENCRYPTION_KEY).update(value).digest('hex');
}
const active=lane=>lane?.kind==='import'&&['import-reserved','dispatched','import-published'].includes(lane.state);
export async function releaseAbandonedImport(db,session,lane,writer) {
  if(lane?.kind!=='import'||lane.state!=='import-reserved')return lane;
  const entry=await db.collection('_ha_credential_intents').findOne({_id:lane.intentId},{session});
  if(entry?.state==='reserved'&&entry.epoch<writer.epoch&&!entry.copyVerificationRequired) {
    await db.collection('_ha_credential_intents').updateOne({_id:entry._id},{$set:{state:'cancelled',cancelledByEpoch:writer.epoch}},{session});
    const result={_id:lane._id,state:'completed',readyAfter:lane.readyAfter??0};
    await db.collection('_ha_credential_lanes').replaceOne({_id:lane._id},result,{session});return result;
  }
  return lane;
}
export async function credentialImportOperation(db,session,request,{writer,now,validateFields,getRefreshSpacingMs}) {
  const options={session},intents=db.collection('_ha_credential_intents'),lanes=db.collection('_ha_credential_lanes'),lane='openai-auth0';
  if(request.action==='importStage') {
    if(!sha(request.importKey)||!sha(request.payloadHash)||typeof request.identity?.email!=='string'||!request.identity.email||request.identity.email.length>2048||
       request.identity.workspaceId!==null&&(typeof request.identity.workspaceId!=='string'||request.identity.workspaceId.length>2048))fail('HA_CREDENTIAL_POLICY','Invalid normalized Codex import');
    let entry=await intents.findOne({kind:'import',importKey:request.importKey},options);
    if(entry&&(entry.provider!=='codex'||entry.lane!==lane))fail('HA_CREDENTIAL_STALE','Import receipt has a different provider group');
    if(entry&&entry.payloadHash!==request.payloadHash)fail('HA_CREDENTIAL_STALE','This import token is bound to a different record');
    if(entry?.state==='attached') {
      const row=await db.collection('provider_connections').findOne({id:entry.connectionId,provider:'codex',auth_type:'oauth'},options);
      let metadata;try{metadata=row?.provider_specific_data?JSON.parse(row.provider_specific_data):{};}catch{fail('HA_CREDENTIAL_STALE','The imported account metadata changed');}
      if(!row||row.email!==entry.identity.email||(metadata.workspaceId??null)!==entry.identity.workspaceId)fail('HA_CREDENTIAL_STALE','The imported account changed or was removed');
      return {id:entry._id,state:'attached',connectionId:row.id};
    }
    if(entry?.copyVerificationRequired||entry&&['dispatched','reconciled'].includes(entry.state))fail('HA_CREDENTIAL_UNCERTAIN','The imported grant requires verified recovery; do not retry its exchange');
    let current=await lanes.findOne({_id:lane},options);
    if(entry?.state==='published') {
      if(current?.intentId!==entry._id||current.state!=='import-published')fail('HA_CREDENTIAL_STALE','Import publication was superseded');
      await intents.updateOne({_id:entry._id},{$set:{epoch:writer.epoch}},options);
      return {id:entry._id,state:'published',fields:entry.fields};
    }
    if(entry&&!['reserved','cancelled'].includes(entry.state))fail('HA_CREDENTIAL_STALE','Unknown import receipt state');
    current=await releaseAbandonedImport(db,session,current,writer);
    if(current&&!['completed','published'].includes(current.state)&&!(entry&&current.intentId===entry._id&&current.state==='import-reserved'))fail('HA_CREDENTIAL_UNCERTAIN','The Auth0 rotation group is occupied or uncertain');
    if(await intents.findOne({lane,tokenHash:request.importKey,state:{$in:['dispatched','published','completed','attached','reconciled']}},options))fail('HA_CREDENTIAL_STALE','This refresh token has already been dispatched');
    const id=entry?._id??request.id,spacing=Math.ceil(getRefreshSpacingMs());
    if(!Number.isSafeInteger(spacing)||spacing<0||!Number.isSafeInteger(now+spacing))fail('HA_CREDENTIAL_POLICY','Invalid import spacing');
    if(entry)await intents.updateOne({_id:id},{$set:{state:'reserved',epoch:writer.epoch,spacing}},options);
    else await intents.insertOne({_id:id,kind:'import',importKey:request.importKey,payloadHash:request.payloadHash,identity:request.identity,provider:'codex',lane,state:'reserved',epoch:writer.epoch,spacing,createdAt:now},options);
    await lanes.replaceOne({_id:lane},{_id:lane,kind:'import',intentId:id,state:'import-reserved',epoch:writer.epoch,readyAfter:current?.readyAfter??0},{...options,upsert:true});
    return {id,state:'reserved'};
  }
  const entry=await intents.findOne({_id:request.id,kind:'import'},options);
  if(!entry||entry.epoch!==writer.epoch||entry.copyVerificationRequired)fail('HA_CREDENTIAL_STALE','Unknown or stale credential import');
  if(request.action==='importInspect')return {state:entry.state,connectionId:entry.connectionId};
  const current=await lanes.findOne({_id:lane,intentId:entry._id,kind:'import'},options);
  if(!current)fail('HA_CREDENTIAL_STALE','Credential import lost its group');
  if(request.action==='importDispatch') {
    if(entry.state!=='reserved'||current.state!=='import-reserved')fail('HA_CREDENTIAL_UNCERTAIN','Import already dispatched; no replay is permitted');
    if(current.readyAfter>now)return {dispatched:false,waitUntil:current.readyAfter};
    await intents.updateOne({_id:entry._id},{$set:{state:'dispatched',tokenHash:entry.importKey,dispatchedAt:now}},options);
    await lanes.updateOne({_id:lane},{$set:{state:'dispatched'}},options);return {dispatched:true};
  }
  if(request.action==='importPublish') {
    if(entry.state!=='dispatched'||current.state!=='dispatched')fail('HA_CREDENTIAL_STALE','Import result already published');
    await validateFields(request.fields);
    const readyAfter=now+entry.spacing;if(!Number.isSafeInteger(readyAfter))fail('HA_CREDENTIAL_POLICY','Invalid import publication spacing');
    await intents.updateOne({_id:entry._id},{$set:{state:'published',fields:request.fields,publishedAt:now}},options);
    await lanes.updateOne({_id:lane},{$set:{state:'import-published',readyAfter}},options);return {published:true};
  }
  fail('HA_CREDENTIAL_POLICY','Unknown import operation');
}
export async function protectImportWrite(db,session,row,importId,deleting,rotationGroupFor) {
  if(!row||row.auth_type!=='oauth')return;
  const lane=rotationGroupFor(row.provider);if(!lane)return;
  const current=await db.collection('_ha_credential_lanes').findOne({_id:lane},{session});
  if(active(current)&&(deleting||current.intentId!==importId||current.state!=='import-published'))fail('HA_CREDENTIAL_UNCERTAIN','A credential import owns this rotation group');
}
let decryptModule;
async function decode(value) {
  if(!process.env.STORAGE_ENCRYPTION_KEY?.trim()||typeof value!=='string'||!value.startsWith('enc:v1:'))fail('HA_CREDENTIAL_POLICY','Import attachment requires encrypted tokens');
  // Load the official encryption implementation, including legacy-key support.
  // The worker never installs a package or looks for an external secret file.
  decryptModule??=(async()=>{await import('./register.mjs');return import('../upstream/omniroute/src/lib/db/encryption.ts');})();
  const result=(await decryptModule).decrypt(value);
  if(typeof result!=='string'||!result||result.startsWith('enc:'))fail('HA_CREDENTIAL_POLICY','Import tokens could not be authenticated');
  return result;
}
export async function recordImportWrite(db,session,row,importId,before,{credentialVersion,rotationGroupFor}) {
  await protectImportWrite(db,session,row,importId,false,rotationGroupFor);
  if(!importId)return;
  const options={session},intents=db.collection('_ha_credential_intents');
  const entry=await intents.findOne({_id:importId,kind:'import'},options);
  const writer=await db.collection('_ha_serial').findOne({_id:'writer'},options);
  if(!entry||entry.epoch!==writer.epoch||entry.copyVerificationRequired)fail('HA_CREDENTIAL_STALE','Stale import SQL callback');
  if(entry.state==='attached') {
    if(!before||credentialVersion(before)!==credentialVersion(row)||row.id===entry.connectionId&&credentialVersion(row)!==entry.version)fail('HA_CREDENTIAL_STALE','Completed import cannot overwrite newer credentials');
    return;
  }
  if(entry.state!=='published'||row.provider!=='codex'||row.auth_type!=='oauth'||row.email!==entry.identity.email)fail('HA_CREDENTIAL_STALE','Import SQL result does not match its account');
  let metadata;try{metadata=row.provider_specific_data?JSON.parse(row.provider_specific_data):{};}catch{fail('HA_CREDENTIAL_POLICY','Invalid imported account metadata');}
  if((metadata.workspaceId??null)!==entry.identity.workspaceId)fail('HA_CREDENTIAL_STALE','Import SQL result has a different workspace');
  for(const field of ['access_token','refresh_token'])if(await decode(row[field])!==await decode(entry.fields[field]))fail('HA_CREDENTIAL_STALE','Import SQL result has a different token pair');
  await intents.updateOne({_id:entry._id,state:'published'},{$set:{state:'attached',connectionId:row.id,version:credentialVersion(row)}},options);
  await db.collection('_ha_credential_lanes').updateOne({_id:entry.lane,intentId:entry._id,state:'import-published'},{$set:{state:'completed'}},options);
}
