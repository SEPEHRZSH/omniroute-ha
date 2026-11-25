import assert from 'node:assert/strict';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { testDatabase } from './mongoTestSupport.mjs';
import { ActiveOwnership } from '../adapter/ownership.mjs';

const test = await testDatabase('ownership');
const config = { uri: test.uri, database: test.database };
let bootstrap, primary, standby, retiredBootstrap,lateStandby;
try {
  bootstrap = new MongoAdapter(config);
  bootstrap.exec('CREATE TABLE state (key TEXT PRIMARY KEY, value TEXT NOT NULL)');
  await bootstrap.close();
  primary = new MongoAdapter({ ...config, ownerId: 'synthetic-primary-process' });
  standby = new MongoAdapter({ ...config, ownerId: 'synthetic-standby-process' });
  const insert = "INSERT OR REPLACE INTO state(key,value) VALUES ('role',?)";
  assert.throws(() => standby.prepare(insert).run('illegal'), { code: 'HA_FENCED' });
  const first = primary.ownership('claim', 1000);
  assert.equal(first.epoch, 1);
  assert.equal(primary.ownership('status'), true);
  assert.equal(standby.ownership('claim', 1000), null);
  primary.prepare(insert).run('primary');
  retiredBootstrap = new MongoAdapter(config);
  assert.throws(() => retiredBootstrap.prepare(insert).run('unfenced-bootstrap'), { code: 'HA_FENCED' });
  assert.throws(() => retiredBootstrap.exec('CREATE INDEX IF NOT EXISTS blocked_index ON state(value)'), { code: 'HA_FENCED' });
  assert.ok(!(await test.db.collection('state').listIndexes().toArray()).some(index => index.name === 'blocked_index'));

  primary.exec('BEGIN IMMEDIATE');
  primary.prepare(insert).run('pending');
  // Cold standby startup must not issue even a no-op writer upsert while the
  // old active holds that document in an unfinished transaction.
  lateStandby=new MongoAdapter({...config,ownerId:'synthetic-late-standby'});
  assert.equal(lateStandby.healthy(),true);assert.equal(lateStandby.ownership('status'),false);
  await lateStandby.close();
  // Simulate a paused active event loop. The lease uses MongoDB server time.
  await new Promise(resolve => setTimeout(resolve, 1200));
  // A competing acquisition must not bypass a transaction holding the fencing row.
  // Reject a commit after expiry and roll back before admitting a new epoch.
  assert.throws(() => primary.exec('COMMIT'), { code: 'HA_FENCED' });
  assert.equal((await test.db.collection('state').findOne({ key: 'role' })).value, 'primary');
  assert.equal(primary.ownership('status'), false);
  assert.throws(() => primary.ownership('renew', 1000), { code: 'HA_FENCED' });
  const second = standby.ownership('claim', 3000);
  assert.equal(second.epoch, 2);
  standby.prepare(insert).run('standby');
  assert.throws(() => primary.prepare(insert).run('stale-primary'), { code: 'HA_FENCED' });
  assert.throws(() => primary.exec('BEGIN IMMEDIATE'), { code: 'HA_FENCED' });
  assert.equal((await test.db.collection('state').findOne({ key: 'role' })).value, 'standby');
  assert.equal(standby.ownership('release'), true);
  assert.equal(primary.ownership('claim', 3000).epoch, 3);
  const readiness=new ActiveOwnership(primary,{onPromote:async()=>{}});readiness.state='active';
  const invoke=primary.bridge.call;let pings=0,statusReads=0;
  primary.bridge.call=function(request){if(request.operation==='ping')pings++;if(request.operation==='ownership'&&request.action==='status')statusReads++;return invoke.call(this,request);};
  try{
    assert.equal(readiness.ready(),true);assert.equal(pings,0);assert.equal(statusReads,1);
    primary.compatibilityFault='synthetic-readiness-poison';assert.equal(readiness.ready(),false);assert.equal(statusReads,1);
  }finally{primary.compatibilityFault=undefined;primary.bridge.call=invoke;}
  console.log('PASS readiness: one real uncached linearizable MongoDB ownership read per check, no redundant ping; local poisoned state still denies immediately');
  assert.throws(() => standby.prepare(insert).run('stale-standby'), { code: 'HA_FENCED' });
  await standby.close();
  let recycled=0;
  const lifecycle=new ActiveOwnership(standby,{onPromote:async()=>assert.fail('Closed standby cannot promote'),onDemote:()=>{recycled++;}});
  await lifecycle.step(); assert.equal(recycled,1); assert.equal(lifecycle.state,'standby');
  console.log('PASS closed standby lifecycle: poisoned bridge triggers supervisor replacement instead of permanent unpromotable liveness');
  console.log('PASS ownership: server-time lease, exclusive claim, monotonic epochs, promotion and fenced stale writers');
  console.log('PASS cold standby connection: initializes without a writer mutation while the active holds an uncommitted fencing-row transaction');
} finally {
  await lateStandby?.close();await retiredBootstrap?.close(); await bootstrap?.close(); await primary?.close(); await standby?.close(); await test.cleanup();
}
