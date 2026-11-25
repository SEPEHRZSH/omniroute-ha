// Team admission is stricter than upstream's soft analytics check. A fresh,
// verified tracking baseline and durable reservation receipts own hard budgets.
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
const source=readFileSync(new URL('../upstream/omniroute/src/lib/db/teams.ts',import.meta.url));
if(createHash('sha256').update(source).digest('hex')!=='700f5db85f8e9bde3e7b4638e1ccefd764b623e3efd01fe7d2300a9efed59e55') throw new Error('Team calendar contract changed');
const durations={'1d':86400000,'7d':604800000,'30d':2592000000};
const fail=(code,message)=>{throw Object.assign(new Error(message),{code});};
export const teamMembershipFingerprint=member=>createHash('sha256').update(JSON.stringify([member.team_id,member.api_key_id,member.valid_from])).digest('hex');
export function teamBudgetWindow(team,now) {
  const duration=durations[team.budget_duration],reset=Date.parse(team.budget_reset_at),capacity=Math.floor(team.max_budget_usd*1e9);
  if(!duration || !Number.isSafeInteger(reset) || !Number.isSafeInteger(capacity) || capacity<=0) fail('HA_QUOTA_POLICY','Invalid hard Team budget');
  const end=reset>now?reset:reset+(Math.floor((now-reset)/duration)+1)*duration,start=end-duration;
  if(!Number.isSafeInteger(end) || start>now) fail('HA_QUOTA_POLICY','Invalid Team budget window');
  return{start,end,capacity};
}
export function historicalTeamDebit(profile,start,end) {
  if(profile?.baselineId==null)return 0;
  if(!Number.isSafeInteger(profile.startedAt)||!Number.isSafeInteger(profile.legacyNanoUsd)||profile.legacyNanoUsd<0)fail('HA_QUOTA_POLICY','Invalid Team historical accounting');
  if(start>=profile.startedAt)return 0;
  if(start!==profile.windowStart||end!==profile.windowEnd)fail('HA_QUOTA_POLICY','Changed Team calendar requires another verified historical baseline');
  return profile.legacyNanoUsd;
}
export async function activeTeam(db,session,keyId) {
  const rows=await db.collection('api_key_billing_team_history').find({api_key_id:keyId,valid_to:null},{session}).limit(2).toArray();
  if(rows.length>1) fail('HA_QUOTA_POLICY','Ambiguous billing team membership');
  if(!rows.length) return null;
  if(typeof rows[0].team_id!=='string' || !rows[0].team_id) fail('HA_QUOTA_POLICY','Invalid billing Team identity');
  const team=await db.collection('teams').findOne({id:rows[0].team_id},{session});
  if(!team) fail('HA_QUOTA_POLICY','Billing membership has no Team');
  if(team.max_budget_usd!=null && (!Number.isFinite(team.max_budget_usd) || team.max_budget_usd<=0)) fail('HA_QUOTA_POLICY','Invalid Team budget amount');
  return team?.status==='active'?team:null;
}
export async function teamLiability(db,session,keyId,bound,now) {
  const options={session},team=await activeTeam(db,session,keyId);
  if(!(team?.max_budget_usd>0)) return null;
  const {start,end,capacity}=teamBudgetWindow(team,now);
  const profiles=db.collection('_ha_team_profiles'),profile=await profiles.findOne({_id:team.id},options);
  if(!profile) {
    for(const [table,filter] of [['usage_history',{billing_team_id:team.id}],['_ha_usage_history',{billing_team_id:team.id}],['daily_team_usage_summary',{team_id:team.id}],['_ha_reservations',{teamId:team.id,state:{$ne:'cancelled'}}]]) {
      if(await db.collection(table).findOne(filter,options)) fail('HA_QUOTA_POLICY','Team usage requires a verified historical baseline');
    }
  } else if(profile.complete!==true) fail('HA_QUOTA_POLICY','Incomplete Team usage baseline');
  const members=await db.collection('api_key_billing_team_history').find({team_id:team.id,...(profile?{valid_to:null}:{})},options).limit(1001).toArray();
  if(members.length>1000) fail('HA_QUOTA_POLICY','Too many Team members for bounded admission');
  for(const member of members) {
    const from=Date.parse(member.valid_from);
    if(typeof member.id!=='string' || typeof member.api_key_id!=='string' || !Number.isSafeInteger(from) || from>now) fail('HA_QUOTA_POLICY','Invalid Team membership interval');
    const fingerprint=teamMembershipFingerprint(member);
    const recorded=await db.collection('_ha_team_memberships').findOne({_id:member.id},options);
    if(recorded && recorded.fingerprint!==fingerprint) fail('HA_QUOTA_POLICY','Team membership baseline changed');
    if(!recorded || !profile) {
      for(const [table,time] of [['domain_cost_history',from],['request_cost_ledger',new Date(from).toISOString()],['usage_history',new Date(from).toISOString()],['_ha_usage_history',new Date(from).toISOString()]]) {
        if(await db.collection(table).findOne({api_key_id:member.api_key_id,timestamp:{$gte:time}},options)) fail('HA_QUOTA_POLICY','Team membership contains unverified historical usage');
      }
      await db.collection('_ha_team_memberships').replaceOne({_id:member.id},{_id:member.id,teamId:team.id,fingerprint},{...options,upsert:true});
    }
  }
  if(!profile) await profiles.insertOne({_id:team.id,complete:true,startedAt:now},options);
  const reservations=db.collection('_ha_reservations');
  if(await reservations.findOne({teamId:team.id,$or:[{state:'uncertain'},{accountingConflict:true}]},options)) fail('HA_QUOTA_UNCERTAIN','Team has unresolved provider accounting');
  const totals=await reservations.aggregate([{$match:{teamId:team.id,$or:[{state:'settled',settledAt:{$gte:start}},{state:{$in:['reserved','dispatched']}}]}},
    {$group:{_id:null,n:{$sum:{$cond:[{$eq:['$state','settled']},'$receipt.nanoUsd','$bound.nanoUsd']}}}}],options).next();
  const used=(totals?.n??0)+historicalTeamDebit(profile,start,end);
  if(!Number.isSafeInteger(used) || used<0) fail('HA_QUOTA_UNCERTAIN','Invalid Team accounting');
  if(used+bound.nanoUsd>capacity) fail('HA_QUOTA_EXCEEDED','Shared Team budget has insufficient unreserved capacity');
  return team.id;
}
