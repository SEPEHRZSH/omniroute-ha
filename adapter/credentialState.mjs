// Durable one-time-token exchanges. No timeout can prove a remote rotation did
// not happen; dispatched uncertainty is intentionally not a lease or a TTL.
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import {credentialImportOperation,protectImportWrite,recordImportWrite,releaseAbandonedImport} from './credentialImports.mjs';
import {kiroFamily,storedKiroOidc,validateKiroClientUpdate,applyKiroStage,releaseSafeKiroStep} from './kiroRefresh.mjs';
const serializer=new URL('../upstream/omniroute/open-sse/services/refreshSerializer.ts',import.meta.url);
if(createHash('sha256').update(readFileSync(serializer)).digest('hex')!=='55556e9a79cb52c2f0232e55667adb4107bf8b2432c39d5c96ff92e23199fdb8') throw new Error('Credential rotation-group contract changed');
const rotation=await import(serializer);
export const {getRefreshSpacingMs}=rotation;
// Upstream dispatch explicitly aliases clinepass to Cline's refresh flow, but
// its serializer map omits the alias. Both names must share the durable lane.
// GitHub App refresh tokens are single-use even though upstream's serializer
// omits this provider. Classic Copilot/PAT access-only rows do not participate.
export const rotationGroupFor=provider=>provider==='github'?'github-app':rotation.rotationGroupFor(provider==='clinepass'?'cline':provider);
export const credentialRecoveryProviders=Object.freeze(['openference','claude','codex','cline','kimi-coding','gitlab-duo','kiro','github']);
export const rotationGroupMembers=provider=>[...new Set([...Object.keys(rotation.ROTATION_LOCK_GROUP),'clinepass','github'])].filter(member=>rotationGroupFor(member)===rotationGroupFor(provider));
export const rotationGroupQuery=provider=>({provider:{$in:rotationGroupMembers(provider)},auth_type:'oauth',...(provider==='github'?{refresh_token:{$nin:[null,'']}}:{})});
export const rotationGroupForRow=row=>row.provider==='github'&&(row.refresh_token==null||row.refresh_token==='')?null:rotationGroupFor(row.provider);
export const credentialVersion=row=>createHash('sha256').update(JSON.stringify(['provider','auth_type','api_key','access_token','refresh_token','id_token','provider_specific_data','proxy_enabled','per_key_proxy_enabled','is_active','test_status','expires_at','token_expires_at'].map(key=>row?.[key]??null))).digest('hex');
export const credentialLaneVersion=row=>createHash('sha256').update(JSON.stringify(['_id','state','intentId','epoch','readyAfter','copiedBy','copiedAt'].map(key=>row?.[key]??null))).digest('hex');
const fail=(code,message)=>{throw Object.assign(new Error(message),{code});};
const uuid=value=>typeof value==='string' && /^[a-f0-9-]{36}$/.test(value);
const encrypted=value=>typeof value==='string' && value.length<=131200 && /^enc:v1:[a-f0-9]{32}:(?:[a-f0-9]{2})+:[a-f0-9]{32}$/.test(value);
async function validateFields(fields,schema,metadataRow,now) {
  const allowed=['access_token','refresh_token','id_token','expires_at','token_expires_at',...(metadataRow&&kiroFamily(metadataRow.provider)?['provider_specific_data']:[])];
  if(!fields || Object.getPrototypeOf(fields)!==Object.prototype || Object.keys(fields).some(key=>!allowed.includes(key)) || !encrypted(fields.access_token) || !encrypted(fields.refresh_token)) fail('HA_CREDENTIAL_POLICY','Refreshed tokens require encrypted persistence');
  for(const [key,value] of Object.entries(fields)) {
    if(key.endsWith('_token') && !encrypted(value) || key.endsWith('_at') && (typeof value!=='string' || !Number.isFinite(Date.parse(value)))) fail('HA_CREDENTIAL_POLICY','Invalid credential result field');
  }
  const spec=await schema('provider_connections');
  if(Object.keys(fields).some(key=>!spec.columns.some(column=>column.name===key && column.type==='TEXT' && !column.numericCheck && !column.allowed))) fail('HA_CREDENTIAL_POLICY','Incompatible credential schema');
  if(Object.hasOwn(fields,'provider_specific_data'))validateKiroClientUpdate(metadataRow.provider_specific_data,fields.provider_specific_data,now,encrypted);
}
export async function credentialOperation(db,session,request,schema,allowRecovery=false) {
  const options={session},intents=db.collection('_ha_credential_intents'),lanes=db.collection('_ha_credential_lanes');
  if(!session || !uuid(request.id)) throw new TypeError('Invalid credential intent');
  const writer=await db.collection('_ha_serial').findOne({_id:'writer'},options);
  if(!writer.ownerId) fail('HA_FENCED','Credential exchange requires active ownership');
  const [{now}]=await db.aggregate([{$documents:[{}]},{$project:{now:{$toLong:'$$NOW'}}}],options).toArray();
  if(request.action.startsWith('import'))return credentialImportOperation(db,session,request,{writer,now,validateFields:fields=>validateFields(fields,schema),getRefreshSpacingMs});
  if(request.action==='recover') {
    const evidence=request.evidence;
    if(!allowRecovery || !writer.ownerId.startsWith('offline-credential-') || !credentialRecoveryProviders.includes(request.provider) ||
       !evidence || Object.keys(evidence).some(key=>!['sha256','previousGrantsRevoked','verifiedBy','providerReference','verifiedAt'].includes(key)) || !/^[a-f0-9]{64}$/.test(evidence.sha256??'') || evidence.previousGrantsRevoked!==true ||
       !['verifiedBy','providerReference'].every(key=>typeof evidence[key]==='string' && evidence[key].trim() && evidence[key].length<=200) ||
       typeof evidence.verifiedAt!=='string' || !Number.isFinite(Date.parse(evidence.verifiedAt))) fail('HA_CREDENTIAL_POLICY','Recovery requires offline ownership and recent verified provider reauthentication');
    const audits=db.collection('_ha_credential_reconciliations'),previous=await audits.findOne({_id:request.id},options);
    if(previous) {
      if(previous.sha256!==evidence.sha256 || previous.provider!==request.provider) fail('HA_CREDENTIAL_STALE','Credential recovery identity already used');
      return {recovered:true,idempotent:true,connections:previous.connections};
    }
    if(Date.parse(evidence.verifiedAt)>now+60000 || Date.parse(evidence.verifiedAt)<now-3600000) fail('HA_CREDENTIAL_POLICY','New recovery requires recent verified provider reauthentication');
    const lane=rotationGroupFor(request.provider),current=await lanes.findOne({_id:lane},options);
    if(!current || !['dispatched','verification_required'].includes(current.state) || credentialLaneVersion(current)!==request.laneVersion) fail('HA_CREDENTIAL_STALE','Credential recovery group changed');
    const rows=await db.collection('provider_connections').find(rotationGroupQuery(request.provider),options).sort({id:1}).limit(1001).toArray();
    const pendingImport=await intents.findOne({kind:'import',lane,state:{$in:['reserved','dispatched','published','cancelled']}},options);
    if(!rows.length&&!pendingImport || rows.length>1000 || !Array.isArray(request.connections) || rows.length!==request.connections.length) fail('HA_CREDENTIAL_POLICY','Recovery must replace every connection in the rotation group');
    for(let index=0;index<rows.length;index++) {
      const row=rows[index],replacement=request.connections[index];
      if(!replacement || row.id!==replacement.id || credentialVersion(row)!==replacement.version) fail('HA_CREDENTIAL_STALE','Credential recovery connection changed');
      await validateFields(replacement.fields,schema,row,now);
      if(replacement.fields.expires_at!==replacement.fields.token_expires_at || !(Date.parse(replacement.fields.expires_at)>now)) fail('HA_CREDENTIAL_POLICY','Recovered credentials must have a future verified expiry');
    }
    for(let index=0;index<rows.length;index++) await db.collection('provider_connections').updateOne({_id:rows[index]._id},{$set:{...request.connections[index].fields,updated_at:new Date(now).toISOString()}},options);
    // Preserve unknown attempts as audit records. They can no longer authorize
    // old callbacks or block this newly reauthenticated (revoked old grant) lane.
    await intents.updateMany({lane,state:'dispatched'},{$set:{state:'reconciled',reconciliationId:request.id,reconciledAt:now}},options);
    await intents.updateMany({lane,kind:'import',state:{$in:['reserved','published','cancelled']}},{$set:{state:'reconciled',reconciliationId:request.id,reconciledAt:now}},options);
    const spacing=Math.ceil(getRefreshSpacingMs());
    if(!Number.isSafeInteger(spacing) || spacing<0 || !Number.isSafeInteger(now+spacing)) fail('HA_CREDENTIAL_POLICY','Invalid recovery spacing');
    await lanes.replaceOne({_id:lane},{_id:lane,state:'completed',reconciliationId:request.id,epoch:writer.epoch,readyAfter:now+spacing},options);
    await audits.insertOne({_id:request.id,provider:request.provider,...evidence,connections:rows.length,epoch:writer.epoch,recoveredAt:now},options);
    return {recovered:true,connections:rows.length};
  }
  const intent=await intents.findOne({_id:request.id},options);
  if(request.action==='dispatch') {
    const lane=typeof request.provider==='string'?rotationGroupFor(request.provider):null;
    if(intent || typeof request.connectionId!=='string' || !request.connectionId || !lane || !/^[a-f0-9]{64}$/.test(request.version??'')) fail('HA_CREDENTIAL_POLICY','Invalid credential dispatch');
    const row=await db.collection('provider_connections').findOne({id:request.connectionId},options);
    if(!row || row.provider!==request.provider || row.auth_type!=='oauth' || row.is_active===0 || ['banned','invalid'].includes(row.test_status) || credentialVersion(row)!==request.version) fail('HA_CREDENTIAL_STALE','Credential snapshot changed before dispatch');
    let previous=await releaseAbandonedImport(db,session,await lanes.findOne({_id:lane},options),writer);
    if(kiroFamily(request.provider))previous=await releaseSafeKiroStep(db,session,previous,writer,now,credentialVersion);
    if(previous && !['published','completed'].includes(previous.state)) fail('HA_CREDENTIAL_UNCERTAIN','This rotation group requires verified credential recovery');
    if(previous?.readyAfter>now) return {dispatched:false,waitUntil:previous.readyAfter};
    const seen={lane,tokenHash:request.tokenHash,state:{$in:['dispatched','published','completed','attached','reconciled']}};
    if(kiroFamily(request.provider))seen.$or=[{state:{$nin:['published','completed']}},{reusableGrant:{$ne:true}},{connectionId:{$ne:request.connectionId}}];
    if(request.tokenHash!==undefined&&(!/^[a-f0-9]{64}$/.test(request.tokenHash)||await intents.findOne(seen,options)))fail('HA_CREDENTIAL_STALE','Refresh token was already dispatched');
    const spacing=Math.ceil(getRefreshSpacingMs());
    if(!Number.isSafeInteger(spacing) || !Number.isSafeInteger(now+spacing)) fail('HA_CREDENTIAL_POLICY','Invalid durable rotation spacing');
    await intents.insertOne({_id:request.id,connectionId:request.connectionId,provider:request.provider,lane,epoch:writer.epoch,state:'dispatched',version:request.version,...(request.tokenHash?{tokenHash:request.tokenHash}:{}),...(kiroFamily(request.provider)&&storedKiroOidc(row.provider_specific_data)?{kiroFlow:'oidc-v1'}:{}),spacing,createdAt:now},options);
    await lanes.replaceOne({_id:lane},{_id:lane,intentId:request.id,state:'dispatched',epoch:writer.epoch},{...options,upsert:true});
    return {dispatched:true};
  }
  if(!intent || intent.epoch!==writer.epoch) fail('HA_CREDENTIAL_STALE','Unknown or stale credential exchange');
  const row=await db.collection('provider_connections').findOne({id:intent.connectionId},options);
  if(!row || credentialVersion(row)!==intent.version) fail('HA_CREDENTIAL_STALE','Provider credentials changed during exchange');
  if(request.action==='kiroStage')return applyKiroStage(db,session,request,intent,row,now,encrypted,credentialVersion);
  if(request.action==='publish') {
    if(intent.state!=='dispatched') fail('HA_CREDENTIAL_STALE','Credential result already published');
    if(!Number.isSafeInteger(intent.spacing) || intent.spacing<0 || !Number.isSafeInteger(now+intent.spacing)) fail('HA_CREDENTIAL_POLICY','Credential intent has no verified rotation spacing');
    if(kiroFamily(intent.provider)&&storedKiroOidc(row.provider_specific_data)&&(intent.kiroFlow!=='oidc-v1'||!['token_dispatched','retry_dispatched'].includes(intent.kiroPhase)))fail('HA_CREDENTIAL_POLICY','Kiro publication requires its fenced token stage');
    if((request.reusableGrant!==undefined||request.stableTokenHash!==undefined)&&
      (!kiroFamily(intent.provider)||request.reusableGrant!==true||!intent.tokenHash||request.stableTokenHash!==intent.tokenHash))fail('HA_CREDENTIAL_POLICY','Reusable Kiro grants require the acknowledged token receipt and original token identity');
    const fields=request.fields;await validateFields(fields,schema);
    const changed={...fields,updated_at:new Date(now).toISOString()};
    await db.collection('provider_connections').updateOne({_id:row._id},{$set:changed},options);
    await intents.updateOne({_id:intent._id},{$set:{state:'published',version:credentialVersion({...row,...changed}),publishedAt:now,...(request.reusableGrant?{reusableGrant:true}:{})}},options);
    await lanes.updateOne({_id:intent.lane,intentId:intent._id},{$set:{state:'published',readyAfter:now+intent.spacing}},options);
    return {published:true};
  }
  if(request.action==='finish') {
    if(intent.state==='dispatched') return {uncertain:true};
    if(!['published','completed'].includes(intent.state)) fail('HA_CREDENTIAL_STALE','Invalid credential completion');
    await intents.updateOne({_id:intent._id},{$set:{state:'completed',completedAt:now}},options);
    await lanes.updateOne({_id:intent.lane,intentId:intent._id},{$set:{state:'completed'}},options);
    return {completed:true};
  }
  throw new TypeError('Invalid credential operation');
}
export async function protectCredentialWrite(db,session,before,intentId,deleting=false,snapshot,importId) {
  await protectImportWrite(db,session,before,importId,deleting,rotationGroupFor);
  if(!before) return;
  const options={session};
  if(await db.collection('_ha_credential_intents').findOne({connectionId:before.id,state:'dispatched'},options)) fail('HA_CREDENTIAL_UNCERTAIN','Provider has an unresolved token exchange');
  if(snapshot && (deleting || snapshot.connectionId!==before.id || snapshot.version!==credentialVersion(before))) fail('HA_CREDENTIAL_STALE','Cached credential callback cannot overwrite newer state');
  if(!intentId) return;
  const intent=await db.collection('_ha_credential_intents').findOne({_id:intentId},options);
  const writer=await db.collection('_ha_serial').findOne({_id:'writer'},options);
  if(!intent || intent.epoch!==writer.epoch || intent.connectionId!==before.id || intent.state!=='published' || deleting || credentialVersion(before)!==intent.version) fail('HA_CREDENTIAL_STALE','Late credential callback cannot overwrite newer state');
}
export async function recordCredentialWrite(db,session,row,intentId,importId,before) {
  if(intentId) await db.collection('_ha_credential_intents').updateOne({_id:intentId,connectionId:row.id,state:'published'},{$set:{version:credentialVersion(row)}},{session});
  await recordImportWrite(db,session,row,importId,before,{credentialVersion,rotationGroupFor});
}

// An internally consistent archive cannot prove the current state of a remote
// one-time token. Copying an older snapshot must never authorize its reuse.
export async function fenceCopiedCredentials(db,reason,writeConcern) {
  if(!/^ha_test_[a-z0-9_]+$/.test(db.databaseName) || !['restore','sqlite-import'].includes(reason)) throw new Error('Invalid credential copy fence');
  if(!await db.collection('_ha_serial').findOne({_id:'writer',ownerId:null,ddl:{$type:'string'}})) throw new Error('Credential copy fence requires offline maintenance ownership');
  const groups=new Set();
  for await(const entry of db.collection('_ha_credential_intents').find({kind:'import',state:{$in:['reserved','dispatched','published','cancelled']}},{projection:{lane:1}}))if(entry.lane)groups.add(entry.lane);
  await db.collection('_ha_credential_intents').updateMany({kind:'import'},{$set:{copyVerificationRequired:true}},{writeConcern});
  for await(const row of db.collection('provider_connections').find({auth_type:'oauth'},{projection:{provider:1,refresh_token:1,_id:0}})) {
    const group=typeof row.provider==='string'?rotationGroupForRow(row):null;
    if(group) groups.add(group);
  }
  const lanes=db.collection('_ha_credential_lanes');
  for(const group of groups) {
    const previous=await lanes.findOne({_id:group});
    if(previous?.state==='dispatched' || previous?.state==='verification_required') continue;
    await lanes.replaceOne({_id:group},{...previous,_id:group,state:'verification_required',copiedBy:reason,copiedAt:new Date()},{upsert:true,writeConcern});
  }
  return groups.size;
}
