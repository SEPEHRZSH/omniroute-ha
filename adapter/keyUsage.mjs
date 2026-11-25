// Apply upstream calendar and reset rules to MongoDB snapshots.
// SQL result materialization is performed by the same translator/session as the
// reservation; the pure functions never open another DB or catch network errors.
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { stripTypeScriptTypes } from 'node:module';
import { codexQuotaRows } from './usageQueries.mjs';
const fail = message => { throw Object.assign(new Error(message),{code:'HA_QUOTA_POLICY'}); };
function pinned(file,hash) {
  const url=new URL('../upstream/omniroute/'+file,import.meta.url), text=readFileSync(url,'utf8');
  if (createHash('sha256').update(text).digest('hex')!==hash) fail('Key usage source contract changed');
  return {url,text};
}
function block(text,name) {
  const start=text.search(new RegExp('^(?:export )?(?:async )?function '+name+'\\(','m'));
  if (start<0) fail('Key usage function missing: '+name);
  const tail=text.slice(start), next=tail.slice(1).search(/^(?:export )?(?:(?:async )?function|interface|type|const|let|class) /m);
  return tail.slice(0,next<0?undefined:next+1).replace(/^export /gm,'');
}
const usage=pinned('src/lib/usage/apiKeyUsageLimits.ts','d07934edabb10d05111a2cdb7b5afbdf061854f170f13f18d9ce7b8a12a6bab1').text;
const reset=pinned('src/lib/db/quotaResetEvents.ts','61947fc1389d8de680d16a21c8aa1ae0b1b76d5598fd9115e3beed3cec46c744').text;
const day=pinned('open-sse/services/dailyQuotaReset.ts','9bfaee523eb1650cfb4841ffc57d4b51d5104309616e629f6c286307b7d3c764');
const {calendarWeekWindowMs,isValidIanaTimeZone}=await import(day.url);
const scope=pinned('open-sse/config/codexQuotaScopes.ts','72b61cc61a5551b459a4cf0a0220a9c614527e92c0a01efa3df47ef87151527e');
const {CODEX_SPARK_QUOTA_SESSION,CODEX_SPARK_QUOTA_WEEKLY}=await import(scope.url);
const codex=pinned('src/lib/providers/codexQuotaDisplay.ts','ea7ec7d624cc9e352d15d001d2333471018d9f4fe77552669095ee2d556de364').text;
const projectCodexQuotaDisplay=new Function('CODEX_SPARK_QUOTA_SESSION','CODEX_SPARK_QUOTA_WEEKLY',
  stripTypeScriptTypes(codex.replace(/^import [\s\S]*?from "[^"\n]+";\s*/gm,'').replace(/^export /gm,''))+'\nreturn projectCodexQuotaDisplay;')(CODEX_SPARK_QUOTA_SESSION,CODEX_SPARK_QUOTA_WEEKLY);
const pureNames=['toNumber','asRecord','clampPercent','resetDay','getFortalezaDayStartIso','getFortalezaDayResetIso','getRollingWeekStartIso','normalizeQuotaName','normalizeProvider','findWeeklyQuotaResetAt','connectionFromValue','warnWeeklyWindowValueOnce','getProcessTimeZone','getApiKeyWeeklyWindowSetting','getCalendarWeeklyWindow','getProviderWeeklyWindow'];
const pure=stripTypeScriptTypes('const FORTALEZA_UTC_OFFSET_MS=10800000, DAY_MS=86400000, WEEK_MS=604800000; const warnedWeeklyWindowValues=new Set();\n'+pureNames.map(name=>block(usage,name)).join('\n')
  .replaceAll('observedWindowStartIso: getWeeklyWindowStartIso(','observedWindowStartIso: await getWeeklyWindowStartIso('));
const library=new Function('getWeeklyWindowStartIso','calendarWeekWindowMs','isValidIanaTimeZone',pure+'\nreturn {getProviderWeeklyWindow,getApiKeyWeeklyWindowSetting,getCalendarWeeklyWindow,getFortalezaDayStartIso,getFortalezaDayResetIso,getRollingWeekStartIso};');
const resetNames=['toNumberOrNull','clampPercent','usedPercent','isResetDrop','parseResetIso','resetDay','getRecordedQuotaWindowStartIso','getObservedQuotaWindowStartIso','getProviderQuotaWindowStart','getProviderQuotaWindowStartIso'];
const resetLibrary=new Function('getDbInstance',stripTypeScriptTypes(resetNames.map(name=>block(reset,name)).join('\n'))+'\nreturn getProviderQuotaWindowStartIso;');
const recordedSql=block(reset,'getRecordedQuotaWindowStartIso').match(/\.prepare<[^>]+>\(\s*`([\s\S]*?)`/)?.[1];
const observedSql=block(reset,'getObservedQuotaWindowStartIso').match(/\.prepare<[^>]+>\(\s*`([\s\S]*?)`/)?.[1];
if (!recordedSql || !observedSql) fail('Reset SQL extraction failed');
const canonical=sql=>sql.replace(/\s+/g,' ').trim();
export async function keyUsageWindows(db,session,key,now,execute) {
  const options={session};
  const getWeeklyWindowStartIso=async(connectionId,target,instant)=> {
    const parameters={connectionId,nowIso:new Date(instant).toISOString()}, snapshots=new Map();
    for (const [sql,table] of [[recordedSql,'provider_quota_reset_events'],[observedSql,'quota_snapshots']]) {
      let rows=[];
      if (await db.collection('_ha_schema').findOne({_id:table},options)) {
        rows=await execute({sql:sql+' LIMIT 5001',args:[parameters],mode:'all'});
        if (rows.length>5000) fail('Provider reset history exceeds bounded materialization');
      }
      snapshots.set(canonical(sql),rows);
    }
    const resolve=resetLibrary(()=>({prepare(sql){
      const rows=snapshots.get(canonical(sql)); if (!rows) fail('Unrecognized reset snapshot SQL');
      return {all(args){if (args.connectionId!==connectionId || args.nowIso!==parameters.nowIso) fail('Reset snapshot parameters changed'); return rows;}};
    }}));
    return resolve(connectionId,target,instant);
  };
  const functions=library(getWeeklyWindowStartIso,calendarWeekWindowMs,isValidIanaTimeZone);
  const setting=functions.getApiKeyWeeklyWindowSetting();
  let weekly;
  if (setting.mode==='calendar') weekly=functions.getCalendarWeeklyWindow(setting.timeZone,now);
  else {
    let allowed=[]; try {allowed=key.allowed_connections?JSON.parse(key.allowed_connections):[];} catch {fail('Invalid allowed connections');}
    if (!Array.isArray(allowed) || allowed.length>1024 || allowed.some(id=>typeof id!=='string')) fail('Invalid allowed connections');
    const rows=await db.collection('provider_connections').find({is_active:1,...(allowed.length?{id:{$in:allowed}}:{})},options).project({id:1,provider:1,is_active:1}).limit(1025).toArray();
    if (rows.length>1024) fail('Too many provider connections for quota window');
    const connections=rows.map(row=>({id:row.id,provider:row.provider,isActive:row.is_active!==0}));
    const cacheRows=await db.collection('key_value').find({namespace:'providerLimitsCache',...(allowed.length?{key:{$in:allowed}}:{})},options).limit(1025).toArray();
    if (cacheRows.length>1024) fail('Too many provider quota caches');
    const caches=Object.create(null);
    for (const row of cacheRows) {
      let value; try {value=JSON.parse(row.value);} catch {continue;}
      if (value && typeof value==='object' && typeof value.fetchedAt==='string' && value.fetchedAt.trim()) caches[row.key]={...value,quotas:value.quotas && !Array.isArray(value.quotas) && typeof value.quotas==='object'?value.quotas:null};
    }
    for (const row of await codexQuotaRows(db,session,connections.filter(row=>row.provider==='codex').map(row=>row.id))) {
      const projected=projectCodexQuotaDisplay(caches[row.id]??null,JSON.parse(row.quota_data??'null'));
      if (projected) caches[row.id]=projected;
    }
    weekly=await functions.getProviderWeeklyWindow({allowedConnections:allowed},{getProviderConnectionById:async id=>connections.find(row=>row.id===id),getProviderConnections:async()=>connections,
      getProviderLimitsCache:id=>caches[id]??null,getAllProviderLimitsCache:()=>caches},now);
  }
  const weeklyStart=weekly.windowStartIso??(weekly.resetAtIso?new Date(Date.parse(weekly.resetAtIso)-604800000).toISOString():functions.getRollingWeekStartIso(now));
  const dailyStart=functions.getFortalezaDayStartIso(now);
  if (![dailyStart,weeklyStart].every(value=>Number.isFinite(Date.parse(value)) && Date.parse(value)<=now)) fail('Invalid provider quota window start');
  return {daily:Date.parse(dailyStart),weekly:Date.parse(weeklyStart)};
}
export async function checkKeyUsage(db,session,keyId,now,execute,checkSpend) {
  const options={session},key=await db.collection('api_keys').findOne({id:keyId},options);
  if (!key?.usage_limit_enabled || !(key.daily_usage_limit_usd>0 || key.weekly_usage_limit_usd>0)) return;
  const profiles=db.collection('_ha_key_usage_profiles'),profile=await profiles.findOne({_id:keyId},options);
  if (!profile) {
    for (const table of ['usage_history','_ha_usage_history','request_cost_ledger','domain_cost_history']) if (await db.collection(table).findOne({api_key_id:keyId},options)) fail('Key USD usage requires a verified historical baseline before HA admission');
    await profiles.insertOne({_id:keyId,allHistory:true,startedAt:now},options);
  } else if (profile.allHistory!==true) fail('Incomplete key USD usage baseline');
  const windows=await keyUsageWindows(db,session,key,now,execute);
  for (const window of ['daily','weekly']) if (key[window+'_usage_limit_usd']>0) await checkSpend(windows[window],key[window+'_usage_limit_usd'],'keyQuotaUnpricedUsage');
  return windows;
}
