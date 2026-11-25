import assert from 'node:assert/strict';
import {readFileSync,mkdirSync} from 'node:fs';
import path from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {MongoAdapter} from '../adapter/mongoAdapter.mjs';
import {ActiveRuntime} from '../adapter/activeRuntime.mjs';
import {installBaseSchema} from '../adapter/baseSchema.mjs';
import {splitStatements,parse} from '../adapter/sql.mjs';
import {gamificationSql} from '../adapter/gamificationQueries.mjs';
import {testDatabase} from './mongoTestSupport.mjs';

// Preload the in-memory SQLite oracle before installing the unchanged
// application loader, which continues to forbid every later SQLite import.
Object.assign(process.env,{NODE_ENV:'test',DATA_DIR:path.resolve('.work/domain-data'),DISABLE_SQLITE_AUTO_BACKUP:'1',STORAGE_ENCRYPTION_KEY:'synthetic-storage-key-for-ha-tests-only'});
mkdirSync(process.env.DATA_DIR,{recursive:true,mode:0o700});
await import('../adapter/register.mjs');
const test=await testDatabase('gamification'),sqlite=new DatabaseSync(':memory:');
const bootstrap=new MongoAdapter({uri:test.uri,database:test.database});
let primary,standby;
try {
  installBaseSchema(bootstrap);
  for(const name of ['060_create_gamification','176_xp_action_counts']) {
    let source=readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/'+name+'.sql',import.meta.url),'utf8');
    if(name==='176_xp_action_counts')source=source.slice(0,source.indexOf('-- Backfill current lifetime totals'));
    for(const sql of splitStatements(source))if(parse(sql).ast.type==='create') {bootstrap.exec(sql);sqlite.exec(sql);}
  }
  assert.deepEqual(bootstrap.prepare(gamificationSql.stats).get(),{...sqlite.prepare(gamificationSql.stats).get()});
  const metadata=[null,'{}','[]','"scalar"','{"amount":null}','{"amount":true}','{"amount":false}',
    '{"amount":"  -12e3x"}','{"amount":1.7}','{"amount":[5]}','{"amount":3,"amount":99}','{"amount":{"x":2}}'];
  for(const value of metadata) {
    bootstrap.prepare(gamificationSql.count).run('synthetic-weight','token_share',value);
    sqlite.prepare(gamificationSql.count).run('synthetic-weight','token_share',value);
    const sql='SELECT api_key_id,action,count FROM xp_action_counts';
    assert.deepEqual(bootstrap.prepare(sql).all(),sqlite.prepare(sql).all().map(row=>({...row})));
  }
  const insert='INSERT INTO xp_audit_log(api_key_id,action,xp_earned,created_at) VALUES (?,?,?,?)';
  for(const [key,points,seconds] of [['a',1200,10],['a',30,4000],['b',2400,20],['c',0,5]]) {
    const time=new Date(Date.now()-seconds*1000).toISOString().slice(0,19).replace('T',' ');
    bootstrap.prepare(insert).run(key,'request',points,time);sqlite.prepare(insert).run(key,'request',points,time);
  }
  const cases=[['recent',['a','-60 seconds']],['hour',['a']],['stats',[]],['anomalies',[]],['today',['a','request']],['today',['absent','request']],['rank',['global',100]]];
  const sorted=rows=>rows.map(row=>({...row})).sort((a,b)=>String(a.api_key_id??'').localeCompare(String(b.api_key_id??'')));
  for(const [name,args] of cases)assert.deepEqual(sorted(bootstrap.prepare(gamificationSql[name]).all(...args)),sorted(sqlite.prepare(gamificationSql[name]).all(...args)));
  const ledger='INSERT INTO token_ledger(from_api_key_id,to_api_key_id,amount) VALUES (?,?,?)';
  for(const args of [['treasury','synthetic-ledger-a',100],['synthetic-ledger-a','synthetic-ledger-b',30],['synthetic-ledger-b','synthetic-ledger-b',5],['synthetic-ledger-b','synthetic-ledger-a',7]]){
    bootstrap.prepare(ledger).run(...args);sqlite.prepare(ledger).run(...args);
  }
  for(const key of ['synthetic-ledger-a','synthetic-ledger-b','treasury','missing','$literal-key'])assert.deepEqual(bootstrap.prepare(gamificationSql.balance).get(key,key),{...sqlite.prepare(gamificationSql.balance).get(key,key)});
  const definition='INSERT INTO badge_definitions(id,name) VALUES (?,?)',earned='INSERT INTO user_badges(api_key_id,badge_id,unlocked_at) VALUES (?,?,?)';
  for(const args of [['synthetic-b1','One'],['synthetic-b2','Two']]) {bootstrap.prepare(definition).run(...args);sqlite.prepare(definition).run(...args);}
  for(const args of [['u1','synthetic-b1','2026-01-01'],['u2','synthetic-b1','2025-01-01'],['u1','synthetic-b2','2026-02-01'],['u3','unseeded','2026-03-01']]){bootstrap.prepare(earned).run(...args);sqlite.prepare(earned).run(...args);}
  const badges=rows=>rows.map(row=>({...row})).sort((a,b)=>a.badge_id.localeCompare(b.badge_id));
  assert.deepEqual(badges(bootstrap.prepare(gamificationSql.allBadges).all()),badges(sqlite.prepare(gamificationSql.allBadges).all()));
  console.log('PASS original Gamification SQL vs SQLite: JSON weights/null/types/duplicates, UTC windows, nested XP mean/variance, anomaly HAVING, daily guard and rank');
  primary=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-xp-primary'});
  standby=new MongoAdapter({uri:test.uri,database:test.database,ownerId:'synthetic-xp-standby'});
  primary.ownership('claim',30000);
  globalThis.__omnirouteMongoAdapter=primary;
  globalThis.__omnirouteActiveRuntime=new ActiveRuntime(primary);
  const domain=await import('../upstream/omniroute/src/lib/db/gamification.ts');
  const {emitGamificationEvent}=await import('../upstream/omniroute/src/lib/gamification/events.ts');
  const {XP_REWARDS,calculateLevel}=await import('../upstream/omniroute/src/lib/gamification/xp.ts');
  const {getStreak}=await import('../upstream/omniroute/src/lib/gamification/streaks.ts');
  const {seedBuiltinBadges}=await import('../upstream/omniroute/src/lib/gamification/badges.ts');
  await seedBuiltinBadges();
  assert.equal(domain.transferTokens('synthetic-ledger-a','synthetic-ledger-b',10,'synthetic-transfer','synthetic-transfer-id').success,true);
  assert.equal(domain.transferTokens('synthetic-ledger-a','synthetic-ledger-b',10,'synthetic-transfer','synthetic-transfer-id').success,true);
  assert.equal(domain.getBalance('synthetic-ledger-a'),67);assert.equal(domain.getBalance('synthetic-ledger-b'),33);
  assert.equal(domain.getHistory('synthetic-ledger-a',10).filter(row=>row.idempotencyKey==='synthetic-transfer-id').length,1);
  assert.equal(domain.transferTokens('synthetic-ledger-a','synthetic-ledger-b',1000,'synthetic-transfer','synthetic-too-large').success,false);
  console.log('PASS actual Gamification token ledger: SQLite-differential balances, self-transfers, atomic insufficiency check and idempotent original transfer/history');
  await Promise.all([1,2].map(()=>emitGamificationEvent({apiKeyId:'synthetic-xp-key',action:'request'})));
  assert.equal(primary.compatibilityFault,undefined);
  const expected=2+XP_REWARDS.badge_unlock;
  assert.equal(domain.getXp('synthetic-xp-key').totalXp,expected);
  assert.equal(domain.getXp('synthetic-xp-key').currentLevel,calculateLevel(expected));
  assert.equal((await getStreak('synthetic-xp-key')).currentStreak,1);
  assert.equal(domain.getBadges('synthetic-xp-key').filter(row=>row.badgeId==='first-token').length,1);
  assert.equal(domain.getAllEarnedBadges().filter(row=>row.badgeId==='first-token').length,1);
  for(const scope of ['global','weekly','monthly'])assert.equal(domain.getTopN(scope,10).find(row=>row.apiKeyId==='synthetic-xp-key').score,expected);
  globalThis.__omnirouteMongoAdapter=standby;
  globalThis.__omnirouteActiveRuntime=new ActiveRuntime(standby);
  await emitGamificationEvent({apiKeyId:'synthetic-xp-key',action:'request'});
  assert.equal(domain.getXp('synthetic-xp-key').totalXp,expected,'Standby event must not award XP');
  primary.ownership('release');standby.ownership('claim',30000);
  await emitGamificationEvent({apiKeyId:'synthetic-xp-key',action:'request'});
  assert.equal(domain.getXp('synthetic-xp-key').totalXp,expected+1);
  assert.equal(domain.getBadges('synthetic-xp-key').filter(row=>row.badgeId==='first-token').length,1);
  console.log('PASS actual Gamification source: concurrent request events, one badge reward, XP/levels/streak/leaderboard, standby rejection and persisted promotion');
  assert.throws(()=>domain.addXp('synthetic-xp-rollback','request',7,'invalid-json'),{code:'SQLITE_ERROR'});
  assert.equal(await test.db.collection('xp_audit_log').countDocuments({api_key_id:'synthetic-xp-rollback'}),0);
  assert.equal(await test.db.collection('user_levels').countDocuments({api_key_id:'synthetic-xp-rollback'}),0);
  assert.equal(await test.db.collection('xp_action_counts').countDocuments({api_key_id:'synthetic-xp-rollback'}),0);
  console.log('PASS atomic XP rollback: malformed metadata cannot leave a partial audit/count/level award');
} finally {
  delete globalThis.__omnirouteActiveRuntime;delete globalThis.__omnirouteMongoAdapter;
  await primary?.close();await standby?.close();await bootstrap.close();sqlite.close();await test.cleanup();
}
