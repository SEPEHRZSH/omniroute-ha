import assert from 'node:assert/strict';
import { MongoClient } from 'mongodb';
import { randomUUID } from 'node:crypto';

// Only explicitly supported disposable loopback fixtures are eligible for writes.
export const nativeFixture = process.env.HA_TEST_NATIVE_INFRA === '1';
export const fixturePorts = nativeFixture ? [47217,47218,47219] : [47117,47118,47119];
export const fixtureSet = nativeFixture ? 'ha_install' : 'ha_test';
export const uri = 'mongodb://' + fixturePorts.map(port => '127.0.0.1:' + port).join(',') +
  '/?replicaSet=' + fixtureSet + '&serverSelectionTimeoutMS=5000';
async function resourceSnapshot(reserveFiles = 0) {
  const nofile = Number(process.env.HA_TEST_MONGO_NOFILE || 1024);
  assert.ok(Number.isSafeInteger(nofile) && nofile >= 1024 && nofile <= 524288, 'Invalid verified test-container nofile limit');
  for (const port of fixturePorts) {
    const member = new MongoClient(`mongodb://127.0.0.1:${port}/?directConnection=true`, { maxPoolSize: 1, minPoolSize: 0, serverSelectionTimeoutMS: 3000, timeoutMS: 5000 });
    try {
      await member.connect();
      const status = await member.db('admin').command({ serverStatus: 1 });
      const files = status.wiredTiger?.connection?.['files currently open'];
      const connections = status.connections?.current;
      assert.ok(Number.isFinite(files) && Number.isFinite(connections), 'MongoDB resource counters unavailable');
      // These counters cover WiredTiger files and connections. Reserve additional
      // descriptors for journals, logs, sockets, and the next fixture.
      console.log(`RESOURCE mongo:${port} wt_open_files=${files} connections=${connections} configured_nofile=${nofile}`);
      assert.ok(files + connections + 128 + reserveFiles < nofile,
        `Insufficient conservative nofile headroom on ${port}; stop tests and raise the disposable container limit manually`);
    } finally { await member.close(); }
  }
}
export async function testDatabase(label) {
  const client = new MongoClient(uri, { maxPoolSize: 2, minPoolSize: 0, maxConnecting: 1, serverSelectionTimeoutMS: 5000, socketTimeoutMS: 10000, timeoutMS: 10000 });
  try {
    await client.connect();
    const status = await client.db('admin').command({ replSetGetStatus: 1, maxTimeMS: 3000 });
    const members = status.members.map(m => ({ name: m.name, state: m.stateStr, health: m.health }));
    assert.equal(status.set, fixtureSet);
    assert.deepEqual(members.map(m => m.name.replace(/^localhost:/, '127.0.0.1:')).sort(), fixturePorts.map(port => '127.0.0.1:' + port));
    assert.equal(members.filter(m => m.state === 'PRIMARY').length, 1);
    assert.equal(members.filter(m => m.state === 'SECONDARY').length, 2);
    assert.ok(members.every(m => m.health === 1));
    await resourceSnapshot(['http', 'activeboot','data','retirement','freeproxy'].includes(label) ? 350 : 100);
    console.log('PASS replica set: exactly one PRIMARY and two healthy SECONDARY members');
    const database = `ha_test_${label}_${randomUUID().replaceAll('-', '')}`;
    console.log('TEST_DATABASE ' + database);
    return { client, database, uri, db: client.db(database), async cleanup() {
      // The generated name belongs only to this test run; never enumerate/drop other DBs.
      // The deliberate lost-commit test can retain a transaction for MongoDB's
      // 60-second lifetime. Allow that cleanup, but never wait indefinitely.
      try {
        await client.db(database).dropDatabase({ maxTimeMS: 75000, timeoutMS: 80000, writeConcern: { w: 'majority', wtimeoutMS: 10000 } });
        await resourceSnapshot();
      }
      finally { await client.close(); }
    } };
  } catch (error) {
    await client.close();
    if (error.name === 'MongoServerSelectionError' && error.reason?.servers instanceof Map) {
      const members = [...error.reason.servers].map(([address, member]) => `${address}: ${member.error?.cause?.code || member.type}`);
      throw Object.assign(new Error(`Cannot verify isolated replica set (${members.join(', ')})`), { code: 'HA_TEST_CLUSTER_UNAVAILABLE' });
    }
    throw error;
  }
}
