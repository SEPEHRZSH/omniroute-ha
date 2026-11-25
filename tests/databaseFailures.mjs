import assert from 'node:assert/strict';
import { SyncBridge } from '../adapter/syncBridge.mjs';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { testDatabase } from './mongoTestSupport.mjs';
import { BSON } from 'mongodb';

const test = await testDatabase('failures');
const proxy = new SyncBridge(new URL('./faultProxy.mjs', import.meta.url), { config: { database: test.database } });
const config = { uri: test.uri, database: test.database, testProxyPort: proxy.call({ operation: 'port' }), timeoutMs: 12000 };
let db, recovered;
try {
  db = new MongoAdapter(config);
  db.exec('CREATE TABLE durable (id TEXT PRIMARY KEY, value TEXT)');
  db.exec('BEGIN IMMEDIATE');
  db.prepare('INSERT INTO durable(id,value) VALUES (?,?)').run('ambiguous', 'committed');
  proxy.call({ operation: 'suppressCommit' });
  assert.throws(() => db.exec('COMMIT'), error => error.outcomeUnknown === true);
  assert.equal(proxy.call({ operation: 'status' }).suppressedCommitOK, 1);
  assert.equal(db.open, false, 'Unknown commit must poison the bridge');
  assert.equal(db.healthy(), false);
  assert.throws(() => db.prepare('SELECT * FROM durable').all(), { code: 'HA_BRIDGE_CLOSED' });
  assert.equal((await test.db.collection('durable').findOne({ id: 'ambiguous' })).value, 'committed');
  assert.throws(() => new MongoAdapter(config), /connect|selection|socket|closed|ECONN|timed out/i);
  proxy.call({ operation: 'recover' });
  recovered = new MongoAdapter(config);
  assert.deepEqual(recovered.prepare('SELECT * FROM durable').all(), [{ id: 'ambiguous', value: 'committed' }]);
  recovered.exec('BEGIN IMMEDIATE');
  recovered.prepare('INSERT INTO durable(id,value) VALUES (?,?)').run('uncommitted', 'must-not-leak');
  proxy.call({ operation: 'drop' });
  assert.throws(() => recovered.prepare('UPDATE durable SET value=? WHERE id=?').run('bad', 'uncommitted'));
  assert.equal(recovered.healthy(), false);
  assert.equal(await test.db.collection('durable').countDocuments({ id: 'uncommitted' }), 0);
  console.log('PASS network faults: lost successful commit reply is unknown, connection poisoned, no replay/fallback, explicit recovery, uncommitted data invisible');
} finally {
  await db?.close(); await recovered?.close();
  try {
    const command = proxy.call({ operation: 'abortCommand' });
    if (command) {
      const { lsid } = BSON.deserialize(Buffer.from(command), { promoteLongs: false });
      assert.ok(lsid?.id instanceof BSON.Binary && lsid.id.length() === 16);
      // Db.command attaches its own implicit session, so a raw abortTransaction
      // would target the wrong lsid. Kill only the captured fixture session;
      // never enumerate sessions or issue killAllSessions/server-wide controls.
      await test.client.db('admin').command({ killSessions: [lsid] }, { timeoutMS: 5000 });
    }
  } finally { await proxy.close(); await test.cleanup(); }
}
