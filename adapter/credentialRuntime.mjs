import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID,createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { credentialVersion,rotationGroupFor } from './credentialState.mjs';
import { isPassiveCodex } from '../upstream/omniroute/open-sse/services/tokenRefresh/passiveCodex.ts';
import {credentialTokenHash} from './credentialImports.mjs';
import {kiroFamily,kiroEndpoint,createKiroFetch} from './kiroRefresh.mjs';
// Certify each provider's request/result contract as well as the central overlay.
// Kiro's explicit durable stages are separate from ordinary one-POST refreshes.
// An upstream helper change needs explicit review.
const contracts=Object.freeze({
  openference:{file:'openference',endpoint:'openference',sha256:'995c67166908001ae06aaf6b61c5e936a70b4aac820eedf5cdf2ed5dfdf36a75'},
  claude:{file:'claudeOAuth',endpoint:'anthropic',sha256:'6ab1c60154cc40775ed98763cb58966de1a6c1c4fbf49cb52321044474c272d4'},
  codex:{file:'codex',endpoint:'openai',sha256:'518006860170bfabb482a16919273e7ff146f6e6645e572e4475430110df920e'},
  cline:{file:'cline',providerConfig:'cline',sha256:'717e96145713c8c8ba008c34ae0a75861dd654e18053d95a10368d8da06d3576'},
  clinepass:{file:'cline',providerConfig:'cline',sha256:'717e96145713c8c8ba008c34ae0a75861dd654e18053d95a10368d8da06d3576'},
  'kimi-coding':{file:'kimiCoding',providerConfig:'kimi-coding',resultExtras:['tokenType','scope'],sha256:'d55e7869eaefb2f555ef9d6c7dbf7062324de0a591ddefa20f6b966c48fa5ffc'},
  'gitlab-duo':{file:'gitlabDuo',instanceEndpoint:true,sha256:'3825087d068f7b7085acce6116de840e7f90f8694e0011fbeb4e4071ab27dd35'},
  kiro:{file:'kiro',kiro:true,sha256:'ae77f8cee5c230044b231921307b0a6e2095ac9c9b79ff37d883dad85c2296d4'},
  'amazon-q':{file:'kiro',kiro:true,sha256:'ae77f8cee5c230044b231921307b0a6e2095ac9c9b79ff37d883dad85c2296d4'},
  github:{file:'github',endpoint:'github',sha256:'e4a1a2c2252be18f85f9811588ae326a2e7997761d6393a3ee0b9b583de4a5f5'},
});
for(const contract of Object.values(contracts))if(createHash('sha256').update(readFileSync(new URL('../upstream/omniroute/open-sse/services/tokenRefresh/providers/'+contract.file+'.ts',import.meta.url))).digest('hex')!==contract.sha256)throw new Error('Certified OAuth provider contract changed');
if(createHash('sha256').update(readFileSync(new URL('../upstream/omniroute/src/lib/oauth/gitlab.ts',import.meta.url))).digest('hex')!=='2369fd6a43e09897c72e9c84c0f6c55f35b02e12c1d74fd2c75e7d89bb241f0a')throw new Error('GitLab instance endpoint contract changed');
if(createHash('sha256').update(readFileSync(new URL('../upstream/omniroute/open-sse/utils/providerRequestLogging.ts',import.meta.url))).digest('hex')!=='760ae2a32466ec6ee5c1f03efa99213321b93b2cba24b70da007db0ac1bc7634')throw new Error('Provider fetch-capture integration contract changed');
if(createHash('sha256').update(readFileSync(new URL('../upstream/omniroute/src/sse/services/auth.ts',import.meta.url))).digest('hex')!=='ade9d1915bc72ccc46cff18573e1fcb29a6f0fb3cf15bbc1a6548ce2b4528356')throw new Error('Credential listing fallback contract changed');
const fail=(code,message)=>{throw Object.assign(new Error(message),{code});};
// Install the guard in the capture wrapper's inner transport slot. Wrapping
// the outer fetch would create a recursive reference when capture reinstalls.
export function createCredentialFetchGuard(scope,runtime,nativeFetch=globalThis.fetch) {
  const guarded=new WeakSet();
  return function installFetchGuard(){
    const capture=globalThis[Symbol.for('omniroute.providerRequestCapture.state')];
    const slot=capture?.wrappedFetch&&globalThis.fetch===capture.wrappedFetch?capture:null;
    if(capture?.wrappedFetch&&!slot)fail('HA_CREDENTIAL_POLICY','Unexpected fetch wrapper after provider capture; review integration before exchange');
    const previous=slot?slot.wrappedInnerFetch:globalThis.fetch;
    if(typeof previous!=='function')fail('HA_CREDENTIAL_POLICY','Provider fetch-capture inner transport is unavailable');
    if(guarded.has(previous))return;
    const fetch=(input,options={})=>{
      const state=scope.getStore();if(!state)return previous(input,options);
      runtime.assertOwner();
      const target=typeof input==='string'||input instanceof URL?String(input):input instanceof Request?input.url:null;
      if(!state.id||state.published||(options.method||'GET').toUpperCase()!=='POST'||options.dispatcher)fail('HA_CREDENTIAL_POLICY','OAuth requires direct non-replayed POSTs in its verified context');
      const signal=options.signal??(input instanceof Request?input.signal:null);
      const init={...options,redirect:'error',signal:signal?AbortSignal.any([signal,AbortSignal.timeout(30000)]):AbortSignal.timeout(30000)};
      if(state.kiro)return state.kiro.fetch(target,input,init,nativeFetch);
      if(state.fetched||target!==state.endpoint)fail('HA_CREDENTIAL_POLICY','OAuth requires one direct non-replayed POST to its verified endpoint');
      state.fetched=true;return nativeFetch(input,init);
    };
    guarded.add(fetch);if(slot)slot.wrappedInnerFetch=fetch;else globalThis.fetch=fetch;
  };
}
// Enable only provider contracts with verified result and persistence behavior.
// An exchange with an unknown outcome cannot be retried.
const supported=new Set(Object.keys(contracts));
export function createCredentialRuntime(db,runtime) {
  const scope=new AsyncLocalStorage();
  const installFetchGuard=createCredentialFetchGuard(scope,runtime);
  const pendingImports=new Map();
  const canonical=value=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
  async function encodeResult(result,credentials,state) {
    const extras=contracts[state.provider]?.resultExtras??[],kiroResult=state.kiro?.validateResult(result),kiroExtras=kiroResult?.extras??[];
    state.reusableGrant=kiroResult?.reusable===true;
    if(!state.fetched || !result || typeof result.accessToken!=='string' || !result.accessToken || typeof result.refreshToken!=='string' || !result.refreshToken || result.refreshToken===credentials.refreshToken&&!state.reusableGrant || Object.keys(result).some(key=>!['accessToken','refreshToken','expiresIn','expiresAt',...extras,...kiroExtras].includes(key)) || extras.some(key=>result[key]!=null&&typeof result[key]!=='string')) fail('HA_CREDENTIAL_UNCERTAIN','OAuth exchange did not produce a supported complete token pair');
    const {encryptConnectionFields}=await import('../upstream/omniroute/src/lib/db/encryption.ts');
    runtime.assertOwner();const encrypted=encryptConnectionFields({accessToken:result.accessToken,refreshToken:result.refreshToken});
    const fields={access_token:encrypted.accessToken,refresh_token:encrypted.refreshToken};
    if(result.expiresAt || result.expiresIn) {
      const instant=result.expiresAt?Date.parse(result.expiresAt):db.serverTime()+Number(result.expiresIn)*1000;
      if(!Number.isFinite(instant))fail('HA_CREDENTIAL_UNCERTAIN','Invalid OAuth token expiry');
      fields.expires_at=fields.token_expires_at=new Date(instant).toISOString();
    }
    return fields;
  }
  db.credentialScope=()=>scope.getStore();
  const api={
    async listingLookup(lookup) {
      runtime.assertOwner();if(typeof lookup!=='function')fail('HA_CREDENTIAL_POLICY','Credential listing requires the original selector');
      const [{getProviderConnections,getProviderNodes},{getProviderSearchPool},{resolveProviderId}]=await Promise.all([
        import('../upstream/omniroute/src/lib/db/providers.ts'),
        import('../upstream/omniroute/open-sse/services/providerConnectionPool.ts'),
        import('../upstream/omniroute/src/shared/constants/providers.ts'),
      ]);
      const connections=await getProviderConnections({},1001,undefined,['provider']);
      const nodes=await getProviderNodes({},1001);runtime.assertOwner();
      if(connections.length>1000||nodes.length>1000)fail('HA_CREDENTIAL_PROFILE','Credential listing exceeds the bounded provider profile');
      const configured=new Set(connections.map(row=>row.provider));
    // Load fresh metadata for each listing. Preserve upstream selection for
    // aliases, custom pools, environment fallbacks, and unavailable accounts.
    // Missing pools cannot expose credentials.
      const envFallbacks=new Set(['gemini','jina-ai','jina-reader','jina-search']);
      return async provider=>{
        const pool=await getProviderSearchPool(provider,nodes);
        if(!envFallbacks.has(resolveProviderId(provider))&&!pool.some(id=>configured.has(id)))return null;
        return lookup(provider);
      };
    },
    supportsProactive:provider=>supported.has(provider),
    requireExchange(provider) {
      runtime.assertOwner();if(isPassiveCodex(provider))return;
      const state=scope.getStore();
      if(!state?.id || !supported.has(state.provider) || rotationGroupFor(state.provider)!==rotationGroupFor(provider) || state.published || state.fetched) fail('HA_CREDENTIAL_POLICY','Direct token refresh requires a durable exchange context');
    },
    validateDirect(provider) {
      runtime.assertOwner();if(rotationGroupFor(provider))api.requireExchange(provider);
    },
    validateCall(provider,credentials) {
      runtime.assertOwner();
      if(isPassiveCodex(provider)) return; // Original read-only adoption performs no exchange.
      if(provider==='github'&&!credentials?.refreshToken)return; // Classic Copilot/PAT has no rotating grant.
      if(rotationGroupFor(provider) && (!supported.has(provider) || !credentials?.connectionId)) fail('HA_CREDENTIAL_PROFILE','Rotating OAuth requires a certified provider and persisted connection');
    },
    async importCodex(payload,create,record=payload) {
      runtime.assertOwner();
      if(payload?.provider!=='codex'||payload.authType!=='oauth'||typeof create!=='function')fail('HA_CREDENTIAL_POLICY','Codex import requires a normalized OAuth record');
      // The original passive mode adopts supplied tokens without exchanging
      // them. Keep that no-network behavior and the ordinary fenced SQL path.
      if(isPassiveCodex('codex'))return create(payload);
      // Fingerprint the submitted record: upstream's default expiry depends
      // on wall time and is deliberately regenerated on every normalization.
      const importKey=credentialTokenHash(payload.refreshToken),payloadHash=credentialTokenHash(JSON.stringify(canonical(record)));
      const pending=pendingImports.get(importKey);
      if(pending) {
        if(pending.payloadHash!==payloadHash)fail('HA_CREDENTIAL_STALE','Concurrent imports disagree on account metadata');
        return pending.promise;
      }
      const promise=(async()=>{
        const [{resolveProxyForCredentials},{resolveProxyForRequest},{OAUTH_ENDPOINTS}]=await Promise.all([
          import('../upstream/omniroute/src/sse/services/tokenRefresh.ts'),import('../upstream/omniroute/open-sse/utils/proxyFetch.ts'),import('../upstream/omniroute/open-sse/config/constants.ts'),
        ]);
        const endpoint=OAUTH_ENDPOINTS.openai.token;
        if(await resolveProxyForCredentials('codex',payload)||resolveProxyForRequest(endpoint).proxyUrl)fail('HA_CREDENTIAL_PROFILE','Certified import requires direct egress');
        runtime.assertOwner();
        const receipt=db.credential('importStage',{id:randomUUID(),importKey,payloadHash,identity:{email:payload.email,workspaceId:payload.providerSpecificData?.workspaceId??null}});
        if(receipt.state==='attached') {
          const {getProviderConnectionById}=await import('../upstream/omniroute/src/lib/db/providers.ts');
          const connection=await getProviderConnectionById(receipt.connectionId);runtime.assertOwner();
          if(!connection)fail('HA_CREDENTIAL_STALE','Imported account was removed');return connection;
        }
        const state={id:receipt.id,importId:receipt.id,provider:'codex',endpoint,published:receipt.state==='published'};
        return scope.run(state,async()=>{
          let fields=receipt.fields;
          if(!state.published) {
            for(;;) {
              const admission=db.credential('importDispatch',{id:state.id});if(admission.dispatched)break;
              if(!Number.isSafeInteger(admission.waitUntil))fail('HA_CREDENTIAL_POLICY','Invalid import admission');
              await new Promise(resolve=>setTimeout(resolve,Math.min(1000,Math.max(1,admission.waitUntil-db.serverTime()))));runtime.assertOwner();
            }
            installFetchGuard();
            const {refreshCodexToken}=await import('../upstream/omniroute/open-sse/services/tokenRefresh.ts');
            const result=await refreshCodexToken(payload.refreshToken,null,null);runtime.assertOwner();
            fields=await encodeResult(result,payload,state);
            db.credential('importPublish',{id:state.id,fields});state.published=true;
          }
          const {decryptConnectionFields}=await import('../upstream/omniroute/src/lib/db/encryption.ts');
          const tokens=decryptConnectionFields({accessToken:fields.access_token,refreshToken:fields.refresh_token});
          if(tokens.credentialDecryptFailed)fail('HA_CREDENTIAL_POLICY','Published import tokens are unavailable');
          const validated={...payload,accessToken:tokens.accessToken,refreshToken:tokens.refreshToken,
            ...(fields.expires_at?{expiresAt:fields.expires_at,tokenExpiresAt:fields.token_expires_at}:{})};
          const connection=await create(validated);runtime.assertOwner();
          const attached=db.credential('importInspect',{id:state.id});
          if(attached.state!=='attached'||attached.connectionId!==connection?.id)fail('HA_CREDENTIAL_UNCERTAIN','Import SQL did not attach its durable receipt');
          return connection;
        });
      })();
      const entry={payloadHash,promise};pendingImports.set(importKey,entry);
      try{return await promise;}finally{if(pendingImports.get(importKey)===entry)pendingImports.delete(importKey);}
    },
    async run(provider,credentials,fn) {
      runtime.assertOwner();
      if(!rotationGroupFor(provider)) return fn();
      if(!supported.has(provider)) fail('HA_CREDENTIAL_PROFILE','Rotating OAuth provider is not yet certified for HA refresh');
      if(typeof credentials?.connectionId!=='string' || !credentials.connectionId) fail('HA_CREDENTIAL_POLICY','Rotating OAuth requires a persisted connection');
      const row=db.prepare('SELECT * FROM provider_connections WHERE id=?').get(credentials.connectionId);
      if(!row || row.provider!==provider) fail('HA_CREDENTIAL_STALE','Provider connection changed');
      const [{resolveProxyForCredentials},{resolveProxyForRequest},{OAUTH_ENDPOINTS,PROVIDERS}]=await Promise.all([
        import('../upstream/omniroute/src/sse/services/tokenRefresh.ts'),
        import('../upstream/omniroute/open-sse/utils/proxyFetch.ts'),
        import('../upstream/omniroute/open-sse/config/constants.ts'),
      ]);
      let metadata;try {metadata=row.provider_specific_data?JSON.parse(row.provider_specific_data):{};}catch{fail('HA_CREDENTIAL_POLICY','Invalid provider credential metadata');}
      const {decryptConnectionFields,encrypt}=await import('../upstream/omniroute/src/lib/db/encryption.ts');
      runtime.assertOwner();
      const fresh=decryptConnectionFields({accessToken:row.access_token,refreshToken:row.refresh_token,idToken:row.id_token,providerSpecificData:metadata});
      if(fresh.credentialDecryptFailed || !fresh.refreshToken) fail('HA_CREDENTIAL_POLICY','Provider refresh credential is unavailable');
      const contract=contracts[provider];let endpoint;
      if(contract.kiro)endpoint=kiroEndpoint(fresh.providerSpecificData,fresh.refreshToken,PROVIDERS);
      else if(contract.instanceEndpoint){
        const {resolveGitLabOAuthBaseUrl,buildGitLabOAuthEndpoints}=await import('../upstream/omniroute/src/lib/oauth/gitlab.ts');
        endpoint=buildGitLabOAuthEndpoints(resolveGitLabOAuthBaseUrl(metadata)).tokenUrl;
      }else endpoint=contract.providerConfig?(PROVIDERS[contract.providerConfig]?.refreshUrl||(provider==='kimi-coding'?PROVIDERS[contract.providerConfig]?.tokenUrl:undefined)):OAUTH_ENDPOINTS[contract.endpoint]?.token;
      if(typeof endpoint!=='string'||!endpoint)fail('HA_CREDENTIAL_PROFILE','Certified OAuth endpoint is unavailable');
      if(await resolveProxyForCredentials(provider,credentials) || resolveProxyForRequest(endpoint).proxyUrl) fail('HA_CREDENTIAL_PROFILE','Certified OAuth exchange currently requires direct egress');
      runtime.assertOwner();
      if((['kimi-coding','gitlab-duo'].includes(provider)||kiroFamily(provider))&&JSON.stringify(canonical(credentials.providerSpecificData??{}))!==JSON.stringify(canonical(fresh.providerSpecificData??{})))fail('HA_CREDENTIAL_STALE','OAuth endpoint/device metadata changed; reload the connection before refresh');
      // Preserve the caller's token: upstream uses that difference to skip a
      // redundant exchange when another caller already rotated a valid token.
      const state={connectionId:row.id,provider,endpoint,version:credentialVersion(row),expected:fresh};
      if(contract.kiro)state.kiro=createKiroFetch(db,runtime,state,row.provider_specific_data,encrypt);
      return scope.run(state,async()=>{
        try {
          const result=await fn();runtime.assertOwner();
          if(state.id && !db.credential('finish',{id:state.id}).completed) fail('HA_CREDENTIAL_UNCERTAIN','OAuth exchange has no durable terminal result');
          return result;
        } catch(error) {
          // Published tokens are already durable even when a later upstream
          // callback fails. Unknown exchanges remain blocked across epochs.
          if(state.published) {try {db.credential('finish',{id:state.id});}catch{}}
          throw error;
        }
      });
    },
    async exchange(provider,credentials,fn) {
      if(!rotationGroupFor(provider)) {runtime.assertOwner();return fn();}
      const state=scope.getStore();
      if(!state || state.provider!==provider || state.connectionId!==credentials.connectionId || state.id) fail('HA_CREDENTIAL_POLICY','OAuth exchange is outside its owned single-attempt context');
      runtime.assertOwner();state.id=randomUUID();
      for(;;) {
        const admission=db.credential('dispatch',{id:state.id,connectionId:state.connectionId,provider,version:state.version,tokenHash:credentialTokenHash(credentials.refreshToken)});
        if(admission.dispatched) break;
        if(!Number.isSafeInteger(admission.waitUntil)) fail('HA_CREDENTIAL_POLICY','Invalid rotation admission result');
        await new Promise(resolve=>setTimeout(resolve,Math.min(1000,Math.max(1,admission.waitUntil-db.serverTime()))));
        runtime.assertOwner();
      }
      // Next and upstream may wrap global fetch after initial runtime setup.
      // Reattach only the ALS-scoped guard; ordinary requests keep their wrapper.
      installFetchGuard();
      const result=await fn();runtime.assertOwner();
      const fields=await encodeResult(result,credentials,state);
      db.credential('publish',{id:state.id,fields,...(state.reusableGrant?{reusableGrant:true,stableTokenHash:credentialTokenHash(credentials.refreshToken)}:{})});state.published=true;state.expected={...state.expected,accessToken:result.accessToken,refreshToken:result.refreshToken};
      return result;
    },
    validateUpdate(id,data) {
      const state=scope.getStore();if(!state || state.connectionId!==id) return;
      for(const key of ['accessToken','refreshToken']) if(Object.hasOwn(data,key) && data[key]!==state.expected[key]) fail('HA_CREDENTIAL_STALE','A refresh callback cannot replace the published token pair');
      if(state.kiro&&Object.hasOwn(data,'providerSpecificData')&&JSON.stringify(canonical(data.providerSpecificData))!==JSON.stringify(canonical(state.expected.providerSpecificData)))fail('HA_CREDENTIAL_STALE','A Kiro callback cannot replace its fenced client registration');
    },
  };
  return api;
}
