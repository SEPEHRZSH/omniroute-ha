import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import { nanoUsd } from './reservations.mjs';
import { fetch as guardedFetch } from 'undici';
import {resolveExplicitStreamAlias,resolveStreamFlag} from '../upstream/omniroute/open-sse/utils/aiSdkCompat.ts';
import {getHeaderValueCaseInsensitive} from '../upstream/omniroute/open-sse/handlers/chatCore/headers.ts';
const fault = message => Object.assign(new Error('Bounded quota: '+message), { code: 'HA_RESERVATION_REQUIRED' });
const nativeTransport = globalThis.fetch; // Capture before Next installs its fetch instrumentation.
// Bounds cover the entire one-fetch request, including context, tool schemas
// and certified media. They are never inferred from client token estimates.
function requestProfile(body,bound) {
  if(!body||typeof body.stream!=='boolean'||!Array.isArray(body.messages)||!body.messages.length||body.n!=null&&body.n!==1)throw fault('Bounded chat requires messages, explicit stream and one choice');
  const ceiling=body.max_completion_tokens??body.max_tokens;
  if(!Number.isSafeInteger(ceiling)||ceiling<1||ceiling>bound.tokens||body.max_tokens!=null&&body.max_completion_tokens!=null&&body.max_tokens!==body.max_completion_tokens)throw fault('Bounded chat requires a certified explicit output-token ceiling');
  if(body.modalities!=null&&(!Array.isArray(body.modalities)||body.modalities.some(value=>value!=='text'))||body.audio)throw fault('Output audio requires its own certified billing contract');
  let images=0,audioBytes=0,usesTools=false;
  const functions=body.functions??[];const tools=body.tools??[];
  if(!Array.isArray(functions)||!Array.isArray(tools)||functions.length+tools.length>128)throw fault('Invalid bounded tools');
  for(const definition of [...functions,...tools.map(tool=>tool?.type==='function'?tool.function:null)]){
    if(!definition||typeof definition.name!=='string'||!/^[A-Za-z0-9_-]{1,64}$/.test(definition.name))throw fault('Only declared function tools are certified');
    usesTools=true;
  }
  for(const message of body.messages){
    if(!message||typeof message!=='object')throw fault('Invalid bounded message');
    if(message.tool_calls!=null||message.function_call!=null||message.role==='tool'||message.role==='function')usesTools=true;
    if(typeof message.content==='string')continue;
    if(message.content===null&&(message.tool_calls?.length||message.function_call))continue;
    if(!Array.isArray(message.content))throw fault('Unrecognized bounded message content');
    for(const part of message.content){
      if(part?.type==='text'&&typeof part.text==='string')continue;
      if(part?.type==='image_url'&&typeof part.image_url?.url==='string'){
        if(!/^(?:https?:\/\/|data:image\/)/i.test(part.image_url.url))throw fault('Invalid image reference');images++;continue;
      }
      if(part?.type==='input_audio'&&['wav','mp3'].includes(part.input_audio?.format)){
        const data=part.input_audio.data;
        if(typeof data!=='string'||!data.length||data.length%4||!/^[A-Za-z0-9+/]*={0,2}$/.test(data))throw fault('Invalid base64 input audio');
        audioBytes+=Buffer.byteLength(data,'base64');continue;
      }
      throw fault('Uncertified multimodal content');
    }
  }
  if(usesTools&&!bound.tools)throw fault('Function tools require a certified whole-request liability bound');
  if(images>(bound.maxImages??0)||audioBytes>(bound.maxAudioBytes??0))throw fault('Media exceeds the certified whole-request bound');
  return {requireExactPrice:!!(images||audioBytes)&&bound.mediaPricing==='provider',outputCeiling:ceiling};
}
function hasExactPrice(usage){return ['cost_usd','cost_in_usd_ticks'].some(key=>typeof usage?.[key]==='number'&&Number.isFinite(usage[key])&&usage[key]>=0);}
async function reservedStream(response,current,db,runtime,scope) {
  if(!response.ok)return response;
  if(!/text\/event-stream/i.test(response.headers.get('content-type')||'')||!response.body)throw fault('Reserved streaming requires an OpenAI chat SSE provider response');
  const [{parseSSEDataLines},{calculateCostDetailed},{getLoggedInputTokens,getLoggedOutputTokens,getReasoningTokens}]=await Promise.all([
    import('../upstream/omniroute/open-sse/utils/streamHelpers.ts'),
    import('../upstream/omniroute/src/lib/usage/costCalculator.ts'),
    import('../upstream/omniroute/src/lib/usage/tokenAccounting.ts'),
  ]);
  const decoder=new TextDecoder('utf-8',{fatal:true}),encoder=new TextEncoder();
  let buffer='',doneFrame,finished=false,usage,serviceTier=current.serviceTier;
  const unknown=()=>Object.assign(new Error('Streaming usage is unresolved; reservation retained'),{code:'HA_QUOTA_UNCERTAIN'});
  const frame=(wire,controller)=>{
    runtime.assertOwner();
    const lines=wire.split(/\r?\n/),data=lines.filter(line=>line.startsWith('data:')).map(line=>line.slice(5).replace(/^ /,''));
    if(!data.length){controller.enqueue(encoder.encode(wire));return;}
    if(doneFrame)throw unknown();
    if(data.join('\n').trim()==='[DONE]'){doneFrame=wire;return;}
    const value=parseSSEDataLines(data,{logWarning:false});
    if(!value||Array.isArray(value)||typeof value!=='object'||value.error)throw unknown();
    if(value.service_tier!==undefined){if(typeof value.service_tier!=='string'||value.service_tier.length>100)throw unknown();serviceTier=value.service_tier;}
    if(value.choices!==undefined){
      if(!Array.isArray(value.choices)||value.choices.length>1)throw unknown();
      for(const choice of value.choices){
        if(!choice||typeof choice!=='object')throw unknown();
        if(choice.index!==undefined&&choice.index!==0)throw unknown();
        if(finished&&Object.values(choice.delta||{}).some(value=>value!=null&&value!==''))throw unknown();
        if(choice.finish_reason!=null){if(!['stop','length','tool_calls','function_call'].includes(choice.finish_reason))throw unknown();finished=true;}
      }
    }
    if(value.usage!=null){
      const candidate=value.usage;
      if(!Number.isSafeInteger(candidate.prompt_tokens)||candidate.prompt_tokens<0||!Number.isSafeInteger(candidate.completion_tokens)||candidate.completion_tokens<0||candidate.estimated)throw unknown();
      // Initial/cumulative usage is not a terminal receipt. Require complete
      // counts on/after the successful finish frame, never an early zero count.
      if(finished)usage=structuredClone(candidate);
    }
    controller.enqueue(encoder.encode(wire));
  };
  const consume=(text,controller)=>{
    buffer+=text;
    // Bound a single incomplete event, not the entire streamed response.
    if(buffer.length>1048576)throw unknown();
    let match;while((match=/\r?\n\r?\n/.exec(buffer))){const end=match.index+match[0].length,wire=buffer.slice(0,end);buffer=buffer.slice(end);frame(wire,controller);}
  };
  const transform=new TransformStream({
    transform(chunk,controller){return scope.run(current,()=>consume(decoder.decode(chunk,{stream:true}),controller));},
    flush(controller){return scope.run(current,async()=>{
      consume(decoder.decode(),controller);
      if(buffer.trim()||!doneFrame||!finished||!usage)throw unknown();
      runtime.assertOwner();
      const price=await calculateCostDetailed(current.provider,current.model,usage,{serviceTier});runtime.assertOwner();
      const input=getLoggedInputTokens(usage),output=getLoggedOutputTokens(usage),reasoning=getReasoningTokens(usage);
      const receipt={input,output,reasoning,tokens:input+output+reasoning,nanoUsd:nanoUsd(price.costUsd),priced:price.priced===true&&price.failed!==true&&(!current.requireExactPrice||hasExactPrice(usage)),serviceTier,requestId:current.id};
      current.settled=db.reservation('settle',{id:current.id,receipt}).settled;
      if(!current.settled)throw unknown();
      // Preserve live content forwarding, but acknowledge successful completion
      // only after the exact provider receipt commits. EOF/error/cancel without
      // this receipt retains the full dispatched liability, with no auto-refund.
      controller.enqueue(encoder.encode(doneFrame));
    });},
  });
  const wrapped=new Response(response.body.pipeThrough(transform),{status:response.status,statusText:response.statusText,headers:response.headers});
  Object.defineProperty(wrapped,'url',{value:response.url});return wrapped;
}
export function installQuotaRuntime(db, runtime, transport = nativeTransport, guardedTransport = guardedFetch) {
  const scope = new AsyncLocalStorage();
  const dispatchers = new WeakSet();
  return {
    current() { return scope.getStore() != null; },
    bind(fn) { const current=scope.getStore();return current?(...args)=>scope.run(current,()=>fn(...args)):fn; },
    ownsKey(keyId) { return scope.getStore()?.keyId === keyId; },
    ownsStreamCost(keyId) { const current=scope.getStore();return current?.streaming===true&&current.keyId===keyId; },
    allowDispatcher(dispatcher) { dispatchers.add(dispatcher); return dispatcher; },
    isAllowedDispatcher(dispatcher) { return dispatchers.has(dispatcher); },
    fetch(input, options = {}) {
      const current = scope.getStore();
      if (!current) throw fault('Bounded fetch requires a reservation scope');
      if (options.dispatcher && !dispatchers.has(options.dispatcher)) throw fault('Unrecognized bounded transport dispatcher');
      // Inspect every managed request after translation/payload rules. Validating
      // only the client body cannot prove the actual provider output ceiling.
      let wire;try{wire=typeof options.body==='string'?JSON.parse(options.body):null;}catch{throw fault('Provider requires a JSON Chat wire body');}
      const profile=requestProfile(wire,current.bound);
      if(String(options.method??'GET').toUpperCase()!=='POST'||wire.input!=null||wire.max_output_tokens!=null||wire.model!==current.model||wire.stream!==current.streaming||profile.outputCeiling>current.outputCeiling)throw fault('Provider wire body exceeds its certified Chat contract');
      current.requireExactPrice ||= profile.requireExactPrice;
      runtime.assertOwner();
      db.reservation('dispatch', { id: current.id }); current.dispatched = true;
      // No transport replay, automatic redirect, proxy or TLS-spoofing fallback.
      const signals = [options.signal, input instanceof Request ? input.signal : null, AbortSignal.timeout(30000)].filter(Boolean);
      const send = options.dispatcher ? guardedTransport : transport;
      return send(input, { ...options, redirect: 'error', signal: AbortSignal.any(signals) }).then(response=>current.streaming?reservedStream(response,current,db,runtime,scope):response).catch(error => {
        const code = error?.code ?? error?.cause?.code;
        console.error('[HA] Reserved transport failed:', /^[A-Z_0-9]+$/.test(String(code)) ? code : 'TRANSPORT_ERROR');
        throw error;
      });
    },
    async run(args, execute) {
      const keyId = args.apiKeyInfo?.id;
      if (!keyId || !db.checkQuotaPolicy(keyId).managed) return execute(args);
      let body=args.body;const provider = args.modelInfo?.provider, model = args.modelInfo?.model;
      const bound = db.quotaBounds[provider + '/' + model];
      if(!bound)throw fault('A certified whole-request provider/model bound is required');
      // Match upstream's Responses path and its input-without-messages Chat
      // compatibility branch. Only Chat/Responses ingress is certified here;
      // importing the full provider service for this pure decision also starts
      // unrelated catalog/configuration work during request validation.
      const endpoint=String(args.clientRawRequest?.endpoint||'');
      const responses=/\/responses(?=\/|$)/i.test(endpoint)||/^responses(?=\/|$)/i.test(endpoint)||body?.input!==undefined&&body?.messages===undefined;
      const format=responses?'openai-responses':'openai';
      if(responses&&args.credentials?.providerSpecificData?.apiType!=='chat')throw fault('Reserved Responses requires a Chat executor');
      if(body){
        const alias=resolveExplicitStreamAlias(body);
        if(alias!==undefined||body.stream==null){
          const headers=args.clientRawRequest?.headers;
          const userAgent=[args.userAgent,getHeaderValueCaseInsensitive(headers??null,'user-agent')].filter(value=>typeof value==='string').join(' ');
          body={...body,stream:alias??resolveStreamFlag(body.stream,getHeaderValueCaseInsensitive(headers??null,'accept'),format,{userAgent,streamDefaultMode:args.apiKeyInfo?.streamDefaultMode})};
        }
      }
      // Optional operator policy, never a token estimate. Forward the cap too:
      // validating a synthetic value without sending it would not bound output.
      if(body&&body.max_tokens==null&&body.max_completion_tokens==null&&(!responses||body.max_output_tokens==null)&&bound.defaultOutputTokens!=null)body={...body,...(responses?{max_output_tokens:bound.defaultOutputTokens}:{max_tokens:bound.defaultOutputTokens})};
      let profileBody=body;
      if(responses){
        const caps=[body?.max_output_tokens,body?.max_completion_tokens,body?.max_tokens].filter(value=>value!=null);
        if(!caps.length||caps.some(value=>!Number.isSafeInteger(value)||value<1||value!==caps[0]))throw fault('Responses requires one consistent output-token ceiling');
        // Upstream preserves max_output_tokens when downgrading to Chat. Supply
        // the equivalent Chat cap without changing the original input/format.
        body={...body,max_tokens:caps[0]};
        delete body.max_output_tokens;
        const {openaiResponsesToOpenAIRequest}=await import('../upstream/omniroute/open-sse/translator/request/openai-responses.ts');
        profileBody=openaiResponsesToOpenAIRequest(model,body,body.stream,args.credentials);
      }
      const profile=requestProfile(profileBody,bound);
      if(body.stream&&args.credentials?.providerSpecificData?.apiType!=='chat')throw fault('Reserved SSE requires a chat-type node');
      const id = randomUUID();
      db.reservation('reserve', { id, keyId, provider, model, connectionId: args.connectionId ?? args.credentials?.connectionId });
      const current = { id, keyId, dispatched: false,...profile,responses,bound,streaming:body.stream===true,provider,model,serviceTier:body.service_tier||'standard' };
      return scope.run(current, async () => {
        try {
          const forwarded={...args,body:current.streaming?{...body,stream_options:{...body.stream_options,include_usage:true}}:body};
          const result = await execute({ ...forwarded, skipUpstreamRetry: true });
          if (!current.dispatched && result?.success === false && result.status >= 500) throw fault('Bounded request failed before provider dispatch');
          if (current.dispatched && !current.settled && !(current.streaming&&result?.success===true&&result.response?.body)) throw Object.assign(new Error('Provider liability is unresolved; reservation retained'), { code: 'HA_QUOTA_UNCERTAIN' });
          return result;
        }
        finally { if (!current.dispatched && db.open) db.reservation('cancel', { id }); }
      });
    },
    observe(keyId, cost, input, output, usage, success, unpriced, reasoning = 0, requestId = null, serviceTier = null) {
      const current = scope.getStore();
      if (!current || current.keyId !== keyId) return false;
      if(current.streaming)return true; // Exact raw provider receipt owns settlement; never charge an estimated/footer copy.
      if (!current.dispatched) return true; // Cached/deduplicated response has no new provider liability.
      const known = usage && typeof usage === 'object' &&
        ['input','prompt_tokens','input_tokens'].some(k => Number.isFinite(usage[k])) &&
        ['output','completion_tokens','output_tokens'].some(k => Number.isFinite(usage[k]));
      const result = db.reservation('settle', { id: current.id, receipt: { input, output, reasoning, tokens: input + output + reasoning,
        requestId, serviceTier, nanoUsd: nanoUsd(cost), priced: !!known && success === true && !unpriced&&(!current.requireExactPrice||hasExactPrice(usage)) } });
      current.settled = result.settled;
      return true;
    },
  };
}
