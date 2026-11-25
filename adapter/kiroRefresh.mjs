// The pinned Kiro helper can re-register an OIDC client and retry. Each remote
// effect is fenced separately; only an explicit client-authentication rejection
// permits that retry. Timeouts, 5xx and malformed replies never authorize it.
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {safeAwsRegion} from '../upstream/omniroute/open-sse/services/kiroRegion.ts';
import {isExternalIdpAuthMethod,buildExternalIdpRefreshParams} from '../upstream/omniroute/open-sse/services/kiroExternalIdp.ts';
for(const [file,hash]of [
  ['kiroRegion.ts','3fd231c58c9d9929e776f2808a16c7164152076b3c36dfd733a9325ce6e6415c'],
  ['kiroExternalIdp.ts','12becb7bb54738cc6de280454d55ee17d84452c4bbca3b7da228cc5bf26d888d'],
])if(createHash('sha256').update(readFileSync(new URL('../upstream/omniroute/open-sse/services/'+file,import.meta.url))).digest('hex')!==hash)throw new Error('Kiro refresh endpoint contract changed');
const fail=message=>{throw Object.assign(new Error(message),{code:'HA_CREDENTIAL_POLICY'});};
export const kiroFamily=provider=>provider==='kiro'||provider==='amazon-q';
export function kiroOidc(metadata){return !isExternalIdpAuthMethod(metadata?.authMethod)&&metadata?.authMethod!=='imported'&&!!metadata?.clientId&&!!metadata?.clientSecret;}
export function storedKiroOidc(text){let metadata;try{metadata=JSON.parse(text||'{}');}catch{fail('Invalid stored Kiro metadata');}return kiroOidc(metadata);}
export function kiroEndpoint(metadata,refreshToken,providers){
  if(isExternalIdpAuthMethod(metadata?.authMethod))return buildExternalIdpRefreshParams(refreshToken,metadata).tokenEndpoint;
  return kiroOidc(metadata)?'https://oidc.'+safeAwsRegion(metadata.region)+'.amazonaws.com/token':providers.kiro?.tokenUrl;
}
export function validateKiroClientUpdate(beforeText,afterText,now,encrypted){
  if(typeof beforeText!=='string'||typeof afterText!=='string'||afterText.length>262144)fail('Invalid Kiro metadata size/type');
  let before,after;try{before=JSON.parse(beforeText);after=JSON.parse(afterText);}catch{fail('Invalid Kiro client metadata');}
  if(!before||!after||Array.isArray(before)||Array.isArray(after)||!kiroOidc(before)||!kiroOidc(after)||
    typeof after.clientId!=='string'||!after.clientId||after.clientId.length>65536||!encrypted(after.clientSecret)||
    !Number.isSafeInteger(after.clientSecretExpiresAt)||after.clientSecretExpiresAt*1000<=now||!Number.isSafeInteger(after.clientSecretExpiresAt*1000)||
    Object.keys(after).some(key=>!Object.hasOwn(before,key)&&!['clientId','clientSecret','clientSecretExpiresAt'].includes(key))||
    Object.keys(before).some(key=>!['clientId','clientSecret','clientSecretExpiresAt'].includes(key)&&JSON.stringify(before[key])!==JSON.stringify(after[key])))fail('Kiro client update must preserve unrelated metadata and encrypt its future-dated secret');
  return after;
}
export async function applyKiroStage(db,session,request,intent,row,now,encrypted,version){
  if(!kiroFamily(intent.provider)||intent.kiroFlow!=='oidc-v1'||intent.state!=='dispatched'||!storedKiroOidc(row.provider_specific_data))fail('Kiro stage requires an unresolved owned OIDC intent');
  const transitions={initial:[undefined,'token_dispatched'],rejected:['token_dispatched','client_rejected'],register:['client_rejected','registration_dispatched'],registered:['registration_dispatched','registration_published'],retry:['registration_published','retry_dispatched']};
  if(typeof request.stage!=='string'||!Object.hasOwn(transitions,request.stage))fail('Unknown Kiro stage');
  const transition=transitions[request.stage];if(intent.kiroPhase!==transition[0])fail('Kiro stage is stale, duplicated or out of order');
  if(request.stage==='rejected'&&(request.status!==401||request.error!=='invalid_client'))fail('Only explicit OIDC client-authentication rejection permits re-registration');
  const changes={kiroPhase:transition[1],kiroStageAt:now},options={session};
  if(request.stage==='registered'){
    if(typeof request.metadata!=='string'||request.metadata.length>262144)fail('Invalid registered Kiro metadata');
    validateKiroClientUpdate(row.provider_specific_data,request.metadata,now,encrypted);
    const fields={provider_specific_data:request.metadata,updated_at:new Date(now).toISOString()};
    await db.collection('provider_connections').updateOne({_id:row._id},{$set:fields},options);
    changes.version=version({...row,...fields});
  }
  await db.collection('_ha_credential_intents').updateOne({_id:intent._id},{$set:changes},options);
  return {stage:changes.kiroPhase,...(changes.version?{version:changes.version}:{})};
}
export async function releaseSafeKiroStep(db,session,lane,writer,now,version){
  if(lane?.state!=='dispatched'||lane._id!=='kiro')return lane;
  const options={session},intents=db.collection('_ha_credential_intents');
  const intent=await intents.findOne({_id:lane.intentId,state:'dispatched'},options);
  // A new epoch fences the old process before any subsequent step. These
  // phases prove no token POST can be outstanding: either none was marked,
  // client authentication explicitly failed, or registration was persisted but
  // the retry was never marked. Never infer safety for an older adapter/backup.
  if(!intent||!kiroFamily(intent.provider)||intent.kiroFlow!=='oidc-v1'||!Number.isSafeInteger(intent.epoch)||intent.epoch>=writer.epoch||
    !/^[a-f0-9]{64}$/.test(intent.tokenHash??'')||![undefined,'client_rejected','registration_published'].includes(intent.kiroPhase))return lane;
  const row=await db.collection('provider_connections').findOne({id:intent.connectionId},options);
  if(!row||version(row)!==intent.version||!storedKiroOidc(row.provider_specific_data))return lane;
  const readyAfter=(intent.kiroStageAt??intent.createdAt)+intent.spacing;
  if(!Number.isSafeInteger(readyAfter)||!Number.isSafeInteger(intent.spacing)||intent.spacing<0)fail('Invalid recovered Kiro spacing');
  await intents.updateOne({_id:intent._id},{$set:{state:'cancelled',cancelledAt:now,recoveryEpoch:writer.epoch,cancellationReason:'known-oidc-step-before-promotion'}},options);
  const recovered={_id:lane._id,intentId:intent._id,state:'completed',epoch:writer.epoch,readyAfter,recoveredAt:now};
  await db.collection('_ha_credential_lanes').replaceOne({_id:lane._id},recovered,options);return recovered;
}
async function replyJson(response){
  const clone=response.clone(),reader=clone.body?.getReader();if(!reader)fail('Missing Kiro reply');
  const chunks=[];let bytes=0;
  try{for(;;){const {done,value}=await reader.read();if(done)break;bytes+=value.length;if(bytes>65536)fail('Kiro reply exceeds the certified bound');chunks.push(value);}let value;try{value=JSON.parse(Buffer.concat(chunks).toString('utf8'));}catch{fail('Malformed Kiro reply');}if(!value||typeof value!=='object'||Array.isArray(value))fail('Kiro reply requires a JSON object');return value;}
  finally{void reader.cancel().catch(()=>{});reader.releaseLock();}
}
const same=(left,right)=>JSON.stringify(left)===JSON.stringify(right);
export function createKiroFetch(db,runtime,state,rawMetadata,encrypt){
  const registerUrl=state.endpoint.replace(/\/token$/,'/client/register');
  const oidc=kiroOidc(state.expected.providerSpecificData),external=isExternalIdpAuthMethod(state.expected.providerSpecificData?.authMethod);
  let phase,registered,knownReply;
  const stage=(name,values={})=>{runtime.assertOwner();const result=db.credential('kiroStage',{id:state.id,stage:name,...values});phase=result.stage;if(result.version)state.version=result.version;return result;};
  const body=options=>{if(typeof options.body!=='string'||Buffer.byteLength(options.body)>262144)fail('Kiro OIDC requires the pinned JSON body');try{return JSON.parse(options.body);}catch{fail('Invalid Kiro OIDC body');}};
  const tokenReply=async response=>{
    if(response.ok){
      const value=await replyJson(response);
      knownReply={accessToken:external?value.access_token:value.accessToken,refreshToken:external?value.refresh_token:value.refreshToken,expiresIn:external?(value.expires_in||3600):value.expiresIn};
      if(typeof knownReply.accessToken!=='string'||!knownReply.accessToken||knownReply.accessToken.length>65536||
        knownReply.refreshToken!=null&&(typeof knownReply.refreshToken!=='string'||!knownReply.refreshToken||knownReply.refreshToken.length>65536)||
        !Number.isSafeInteger(knownReply.expiresIn)||knownReply.expiresIn<=0)fail('Incomplete Kiro token reply');
    }
    return response;
  };
  return {
    async fetch(target,input,options,send){
      if(!oidc){
        if(state.fetched||target!==state.endpoint)fail('Kiro refresh cannot replay an unknown request');
        state.fetched=true;const response=await send(input,options);runtime.assertOwner();return tokenReply(response);
      }
      const data=body(options);
      if(target===state.endpoint&&(phase===undefined||phase==='registration_published')){
        const expected={clientId:registered?.clientId??state.expected.providerSpecificData.clientId,clientSecret:registered?.clientSecret??state.expected.providerSpecificData.clientSecret,refreshToken:state.expected.refreshToken,grantType:'refresh_token'};
        if(!same(data,expected))fail('Kiro token request disagrees with its fenced credential snapshot');
        const initial=phase===undefined;stage(initial?'initial':'retry');state.fetched=true;
        const response=await send(input,options);runtime.assertOwner();
        if(initial&&response.status===401){
          const rejected=await replyJson(response);
          if(['invalid_client','InvalidClientException'].includes(rejected.error??rejected.__type)&&
            [rejected.error,rejected.__type].filter(value=>value!==undefined).every(value=>['invalid_client','InvalidClientException'].includes(value)))stage('rejected',{status:401,error:'invalid_client'});
        }
        return tokenReply(response);
      }
      if(target!==registerUrl||phase!=='client_rejected')fail('Kiro fallback cannot replay an unknown or rejected refresh');
      const expected={clientName:'kiro-oauth-client',clientType:'public',scopes:['codewhisperer:completions','codewhisperer:analysis','codewhisperer:conversations'],grantTypes:['urn:ietf:params:oauth:grant-type:device_code','refresh_token'],issuerUrl:'https://identitycenter.amazonaws.com/ssoins-722374e8c3c8e6c6'};
      if(!same(data,expected))fail('Kiro client registration contract changed');
      stage('register');const response=await send(input,options);runtime.assertOwner();
      if(response.ok){
        const result=await replyJson(response);
        if(typeof result.clientId!=='string'||!result.clientId||typeof result.clientSecret!=='string'||!result.clientSecret||result.clientId.length>65536||result.clientSecret.length>65536)fail('Incomplete Kiro client registration');
        const metadata=JSON.stringify({...JSON.parse(rawMetadata),clientId:result.clientId,clientSecret:encrypt(result.clientSecret),clientSecretExpiresAt:result.clientSecretExpiresAt});
        stage('registered',{metadata});registered={clientId:result.clientId,clientSecret:result.clientSecret,clientSecretExpiresAt:result.clientSecretExpiresAt};
        state.expected.providerSpecificData={...state.expected.providerSpecificData,...registered};
      }
      return response;
    },
    validateResult(result){
      const extras=['_newClientId','_newClientSecret','_newClientSecretExpiresAt'];
      const refreshToken=knownReply?.refreshToken??state.expected.refreshToken;
      if(!knownReply||result?.accessToken!==knownReply.accessToken||result?.refreshToken!==refreshToken||result?.expiresIn!==knownReply.expiresIn)fail('Kiro helper result does not match its acknowledged token reply');
      if(registered){if(phase!=='retry_dispatched'||result?._newClientId!==registered.clientId||result?._newClientSecret!==registered.clientSecret||result?._newClientSecretExpiresAt!==registered.clientSecretExpiresAt)fail('Kiro retry result disagrees with durable registration');}
      else if(extras.some(key=>Object.hasOwn(result??{},key)))fail('Kiro result has no durable client registration');
      // AWS / OAuth permit omitting a replacement refresh token on known
      // success. This is not permission to replay a lost/ambiguous response.
      return {extras,reusable:refreshToken===state.expected.refreshToken};
    },
  };
}
