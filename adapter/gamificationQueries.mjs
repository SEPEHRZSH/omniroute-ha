// Exact mappings for the pinned upstream XP/anti-cheat SQL. This does not
// advertise generic JSON1, derived-table aggregates or SQLite date modifiers.
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {parseTree,findNodeAtLocation} from 'jsonc-parser';
const reject=message=>{throw Object.assign(new Error(message),{code:'HA_UNSUPPORTED_SQL'});};
function source(file,hash) {
  const text=readFileSync(new URL('../upstream/omniroute/src/lib/'+file,import.meta.url),'utf8');
  if(createHash('sha256').update(text).digest('hex')!==hash) reject('Gamification query contract changed');
  return text;
}
const domain=source('db/gamification.ts','c0322e8b697eb5987054cb984b8885c8ffd15618fc19f62c1e67a30c433867ee');
const anti=source('gamification/antiCheat.ts','3f165e41fd1f58e8cf1a896eae34209d9d88c8439c651fa0f6ed62a02483d016');
const canonical=sql=>sql.replace(/\s+/g,' ').trim().replace(/;$/,'');
function query(text,pattern) {
  const matches=[...text.matchAll(/\.prepare\(\s*(`([^`]+)`|"([^"\n]+)")\s*\)/g)].map(match=>match[2]??match[3]).filter(sql=>pattern.test(sql));
  if(matches.length!==1) reject('Gamification SQL extraction failed');
  return matches[0];
}
export const gamificationSql=Object.freeze({
  count:query(domain,/INSERT INTO xp_action_counts/),
  today:query(domain,/SELECT 1 FROM xp_audit_log/),
  rank:query(domain,/COUNT\(\*\) \+ 1 AS rank/),
  recent:query(anti,/created_at > datetime\('now', \?\)/),
  hour:query(anti,/SELECT COALESCE\(SUM\(xp_earned\), 0\) AS total[\s\S]*-1 hour/),
  stats:query(anti,/SELECT AVG\(hourly_total\)/),
  anomalies:query(anti,/HAVING hourly_total > 1000/),
  allBadges:query(domain,/GROUP BY ub\.badge_id/),
  balance:query(domain,/AS balance/),
});
const patterns=new Map(Object.entries(gamificationSql).map(([name,sql])=>[canonical(sql),name]));
const arity={count:3,today:2,rank:2,recent:2,hour:1,stats:0,anomalies:0,allBadges:0,balance:2};
export function parseGamificationQuery(sql,bindings) {
  const name=patterns.get(canonical(sql));if(!name)return null;
  if(bindings.length!==arity[name]) reject('Gamification query binding count');
  if(name==='rank') {
    if(typeof bindings[0]!=='string'||!Number.isSafeInteger(bindings[1]))reject('Gamification rank bindings');
  } else if(bindings.some((value,index)=>!(name==='count'&&index===2&&value===null)&&typeof value!=='string')) reject('Gamification query bindings');
  if(name==='recent'&&!/^-\d{1,5} seconds$/.test(bindings[1]))reject('Unsupported XP window');
  return {type:name==='count'?'ha_game_count':'ha_game_read',name,bindings};
}
function weight(metadata) {
  if(metadata===null)return 1;
  const errors=[],root=parseTree(metadata,errors,{disallowComments:true,allowTrailingComma:false});
  if(errors.length||!root)throw Object.assign(new Error('malformed JSON'),{code:'SQLITE_ERROR'});
  const node=findNodeAtLocation(root,['amount']);
  if(!node||node.type==='null')return 1;
  let value=0;
  if(node.type==='number')value=Math.trunc(node.value);
  else if(node.type==='boolean')value=node.value?1:0;
  else if(node.type==='string')value=Number(/^[\t\n\v\f\r ]*([+-]?\d+)/.exec(node.value)?.[1]??0);
  if(!Number.isSafeInteger(value)) reject('XP JSON integer exceeds the safe synchronous numeric interface');
  return value;
}
export async function executeGamificationQuery(db,ast,session,schema,execute) {
  const {name,bindings}=ast;
  if(name==='count') {
    const sql=gamificationSql.count.replace("COALESCE(CAST(json_extract(?, '$.amount') AS INTEGER), 1)",'?');
    return execute({sql,args:[bindings[0],bindings[1],weight(bindings[2])],mode:'run'});
  }
  if(name==='allBadges') {
    const definitions=await schema('badge_definitions'),earned=await schema('user_badges');
    const fields=['name','description','icon','category','rarity'];
    if(!['id',...fields].every(name=>definitions.columns.some(c=>c.name===name)) || !['badge_id','unlocked_at'].every(name=>earned.columns.some(c=>c.name===name)))reject('Gamification badge schema changed');
    return db.collection('user_badges').aggregate([
      {$group:{_id:'$badge_id',unlocked_at:{$min:'$unlocked_at'}}},
      {$lookup:{from:'badge_definitions',localField:'_id',foreignField:'id',as:'definition'}},{$unwind:'$definition'},
      {$project:{_id:0,badge_id:'$_id',unlocked_at:1,...Object.fromEntries(fields.map(name=>[name,{$ifNull:['$definition.'+name,null]}]))}},
    ],{session,...(!session?{readConcern:{level:'snapshot'}}:{}),maxTimeMS:5000}).toArray();
  }
  const table=name==='rank'?'leaderboard':name==='balance'?'token_ledger':'xp_audit_log',spec=await schema(table);
  const needed=table==='leaderboard'?['scope','score']:table==='token_ledger'?['from_api_key_id','to_api_key_id','amount']:['api_key_id','action','xp_earned','created_at'];
  if(needed.some(column=>!spec.columns.some(value=>value.name===column)))reject('Gamification schema changed');
  const options={session,...(!session?{readConcern:{level:'snapshot'}}:{}),maxTimeMS:5000};
  let pipeline;
  if(name==='balance')pipeline=[{$match:{$or:[{to_api_key_id:bindings[0]},{from_api_key_id:bindings[1]}]}},
    {$group:{_id:null,balance:{$sum:{$subtract:[{$cond:[{$eq:['$to_api_key_id',{$literal:bindings[0]}]},'$amount',0]},{$cond:[{$eq:['$from_api_key_id',{$literal:bindings[1]}]},'$amount',0]}]}}}},{$project:{_id:0,balance:1}}];
  else if(name==='rank')pipeline=[{$match:{scope:bindings[0],score:{$gt:bindings[1]}}},{$count:'rank'},{$project:{_id:0,rank:{$add:['$rank',1]}}}];
  else if(name==='today')pipeline=[{$match:{api_key_id:bindings[0],action:bindings[1],$expr:{$gte:['$created_at',{$dateToString:{date:'$$NOW',format:'%Y-%m-%d',timezone:'UTC'}}]}}},{$limit:1},{$project:{_id:0,'1':{$literal:1}}}];
  else {
    const seconds=name==='recent'?Number(bindings[1].slice(1).split(' ')[0]):3600;
    const cutoff={$dateToString:{date:{$subtract:['$$NOW',seconds*1000]},format:'%Y-%m-%d %H:%M:%S',timezone:'UTC'}};
    const match={$expr:{$gt:['$created_at',cutoff]}};
    if(['hour','recent'].includes(name))match.api_key_id=bindings[0];
    pipeline=[{$match:match}];
    if(['hour','recent'].includes(name))pipeline.push({$group:{_id:null,total:{$sum:'$xp_earned'}}},{$project:{_id:0,total:1}});
    else {
      pipeline.push({$group:{_id:'$api_key_id',hourly_total:{$sum:'$xp_earned'}}});
      if(name==='anomalies')pipeline.push({$match:{hourly_total:{$gt:1000}}},{$project:{_id:0,api_key_id:'$_id',hourly_total:1}});
      else pipeline.push({$group:{_id:null,mean:{$avg:'$hourly_total'},square:{$avg:{$multiply:['$hourly_total','$hourly_total']}}}},{$project:{_id:0,mean:1,variance:{$cond:[{$eq:['$mean',0]},1,{$subtract:['$square',{$multiply:['$mean','$mean']}]}]}}});
    }
  }
  const rows=await db.collection(table).aggregate(pipeline,options).toArray();
  if(name==='balance'&&rows.length&&!Number.isSafeInteger(rows[0].balance))reject('Token-ledger balance exceeds the safe numeric interface');
  if(rows.length)return rows;
  return name==='balance'?[{balance:0}]:name==='rank'?[{rank:1}]:['hour','recent'].includes(name)?[{total:0}]:name==='stats'?[{mean:null,variance:null}]:[];
}
