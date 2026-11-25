// Offline recovery of revoked/re-authenticated grants only; never retry an
// unknown OAuth request or assume a copied one-time token is still usable.
import '../adapter/register.mjs';
import { MongoClient } from 'mongodb';
import { randomUUID,createHash } from 'node:crypto';
import { readFileSync,statSync } from 'node:fs';
import { credentialVersion,credentialLaneVersion,rotationGroupFor,rotationGroupMembers,rotationGroupQuery,credentialRecoveryProviders } from '../adapter/credentialState.mjs';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { workspacePath } from '../adapter/workspacePath.mjs';
import {kiroFamily} from '../adapter/kiroRefresh.mjs';
const fail=code=>{throw Object.assign(new Error(code),{code});};
async function main() {
  const [command,database,argument,offline,...extra]=process.argv.slice(2);
  if(!process.env.HA_MONGO_URI || !/^ha_test_[a-z0-9_]+$/.test(database??'') || extra.length ||
     !(command==='status' && credentialRecoveryProviders.includes(argument) && !offline || command==='recover' && argument && offline==='--offline')) fail('HA_CREDENTIAL_USAGE');
  let provider=command==='status'?argument:undefined,bytes,evidence;
  if(command==='recover') {
    if(!process.env.STORAGE_ENCRYPTION_KEY?.trim()) fail('HA_CREDENTIAL_ENCRYPTION_REQUIRED');
    const file=workspacePath(argument),stat=statSync(file);
    if(stat.size>262144 || stat.uid!==process.getuid() || (stat.mode&0o077)!==0) fail('HA_CREDENTIAL_PRIVATE_EVIDENCE_REQUIRED');
    bytes=readFileSync(file);evidence=JSON.parse(bytes);provider=evidence.provider;
    if(Object.keys(evidence).some(key=>!['id','provider','laneVersion','previousGrantsRevoked','verifiedBy','providerReference','verifiedAt','connections'].includes(key)) ||
       !/^[a-f0-9-]{36}$/.test(evidence.id??'') || !credentialRecoveryProviders.includes(provider) || !/^[a-f0-9]{64}$/.test(evidence.laneVersion??'') ||
       !Array.isArray(evidence.connections) || evidence.connections.length>1000) fail('HA_CREDENTIAL_EVIDENCE');
  }
  const client=new MongoClient(process.env.HA_MONGO_URI,{maxPoolSize:1,minPoolSize:0,serverSelectionTimeoutMS:5000,timeoutMS:5000,readConcern:{level:'majority'},readPreference:'primary'});
  let actor,claimed=false;
  try {
    await client.connect();const db=client.db(database),members=rotationGroupMembers(provider);
    const lane=await db.collection('_ha_credential_lanes').findOne({_id:rotationGroupFor(provider)});
    if(command==='status') {
      const rows=await db.collection('provider_connections').find(rotationGroupQuery(provider)).sort({id:1}).limit(1001).toArray();
      if(rows.length>1000) fail('HA_CREDENTIAL_POLICY');
      const pendingImports=await db.collection('_ha_credential_intents').find({kind:'import',lane:rotationGroupFor(provider),state:{$in:['reserved','dispatched','published','cancelled']}},{projection:{_id:1,state:1,epoch:1,copyVerificationRequired:1}}).limit(1001).toArray();
      if(pendingImports.length>1000)fail('HA_CREDENTIAL_POLICY');
      console.log(JSON.stringify({provider,group:rotationGroupFor(provider),laneVersion:credentialLaneVersion(lane),state:lane?.state??'unused',pendingImports,
        connections:rows.map(row=>({id:row.id,provider:row.provider,version:credentialVersion(row)}))}));
      return;
    }
    // The same id/digest may be resubmitted after an uncertain commit; the
    // native audit returns success without rewriting credentials a second time.
    const {encryptConnectionFields,decryptConnectionFields}=await import('../upstream/omniroute/src/lib/db/encryption.ts');
    const replacements=[];
    for(const row of evidence.connections) {
      if(!row || Object.keys(row).some(key=>!['id','provider','version','accessToken','refreshToken','expiresAt','oidcClient'].includes(key)) ||
         typeof row.id!=='string' || !row.id || !/^[a-f0-9]{64}$/.test(row.version??'') ||
         row.provider!==undefined&&!members.includes(row.provider) ||
         !['accessToken','refreshToken'].every(key=>typeof row[key]==='string' && row[key].length>0 && row[key].length<=65536 && !row[key].startsWith('enc:')) ||
         typeof row.expiresAt!=='string' || !Number.isFinite(Date.parse(row.expiresAt))) fail('HA_CREDENTIAL_EVIDENCE');
      const encrypted=encryptConnectionFields({accessToken:row.accessToken,refreshToken:row.refreshToken});
      const fields={access_token:encrypted.accessToken,refresh_token:encrypted.refreshToken,expires_at:row.expiresAt,token_expires_at:row.expiresAt};
      if(row.oidcClient!==undefined){
        const value=row.oidcClient;
        if(provider!=='kiro'||!value||Object.keys(value).sort().join(',')!=='clientId,clientSecret,clientSecretExpiresAt'||
          !['clientId','clientSecret'].every(key=>typeof value[key]==='string'&&value[key]&&value[key].length<=65536&&!value[key].startsWith('enc:'))||!Number.isSafeInteger(value.clientSecretExpiresAt))fail('HA_CREDENTIAL_EVIDENCE');
        const stored=await db.collection('provider_connections').findOne({id:row.id});
        if(!stored||!kiroFamily(stored.provider))fail('HA_CREDENTIAL_STALE');
        const sealed=encryptConnectionFields({providerSpecificData:{clientSecret:value.clientSecret}});
        fields.provider_specific_data=JSON.stringify({...JSON.parse(stored.provider_specific_data||'{}'),...value,clientSecret:sealed.providerSpecificData.clientSecret});
      }
      replacements.push({id:row.id,version:row.version,fields});
    }
    replacements.sort((a,b)=>a.id<b.id?-1:a.id>b.id?1:0);
    if(new Set(replacements.map(row=>row.id)).size!==replacements.length || new Set(evidence.connections.map(row=>row.refreshToken)).size!==replacements.length) fail('HA_CREDENTIAL_EVIDENCE');
    actor=new MongoAdapter({uri:process.env.HA_MONGO_URI,database,ownerId:'offline-credential-'+randomUUID(),allowCredentialRecovery:true});
    if(!actor.ownership('claim',60000)) fail('HA_CREDENTIAL_STOP_APPLICATIONS');
    claimed=true;
    const sha256=createHash('sha256').update(bytes).digest('hex');
    const audit=await db.collection('_ha_credential_reconciliations').findOne({_id:evidence.id});
    if(!audit) {
      for(const replacement of evidence.connections) {
        const stored=await db.collection('provider_connections').findOne({id:replacement.id,provider:{$in:members},auth_type:'oauth'});
        if(!stored || replacement.provider!==undefined&&replacement.provider!==stored.provider || credentialVersion(stored)!==replacement.version) fail('HA_CREDENTIAL_STALE');
        const old=decryptConnectionFields({accessToken:stored.access_token,refreshToken:stored.refresh_token});
        const submitted=evidence.connections.find(row=>row.id===replacement.id);
        if(old.credentialDecryptFailed || old.accessToken===submitted.accessToken || old.refreshToken===submitted.refreshToken) fail('HA_CREDENTIAL_NEW_GRANT_REQUIRED');
      }
    }
    const result=actor.credential('recover',{id:evidence.id,provider,laneVersion:evidence.laneVersion,connections:replacements,
      evidence:{sha256,previousGrantsRevoked:evidence.previousGrantsRevoked,verifiedBy:evidence.verifiedBy,providerReference:evidence.providerReference,verifiedAt:evidence.verifiedAt}});
    console.log(JSON.stringify({...result,recoveryId:evidence.id,evidenceSha256:sha256}));
  } finally {if(claimed && actor.open)actor.ownership('release');await actor?.close();await client.close();}
}
// Never include JSON parser context, credential values or connection strings in
// a CLI error. The audit/status output contains identifiers and hashes only.
await main().catch(error=>{console.error('Credential command failed:',String(error.code||'HA_CREDENTIAL_RECOVERY_FAILED'));process.exitCode=1;});
