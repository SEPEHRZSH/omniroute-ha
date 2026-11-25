import assert from 'node:assert/strict';
import { testDatabase } from './mongoTestSupport.mjs';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { installBaseSchema } from '../adapter/baseSchema.mjs';
import {provisionArtifactSchema} from '../adapter/startupSchema.mjs';
import { attachActiveRuntime } from '../adapter/activeRuntime.mjs';
import { register } from '../upstream/omniroute/src/instrumentation.ts';
import { getServerLifecyclePhase } from '../upstream/omniroute/src/lib/serverLifecycle.ts';

process.env.NEXT_RUNTIME = 'nodejs';
const test = await testDatabase('boot');
const config = { uri: test.uri, database: test.database };
let bootstrap, active, standby, runtime;
const retired = [];
try {
  bootstrap = new MongoAdapter(config);
  const incomplete = new MongoAdapter({ ...config, ownerId: 'empty-boot' });
  try {
    assert.throws(() => incomplete.verifyBaseSchema(), { code: 'HA_SCHEMA_MISMATCH' });
    assert.equal(incomplete.healthy(), false);
  } finally { await incomplete.close(); }
  assert.deepEqual(installBaseSchema(bootstrap), { tables: 17, indexes: 22 });
  bootstrap.installRequestLimits();
  const missingArtifacts=new MongoAdapter({...config,ownerId:'missing-artifact-boot'});
  try{assert.throws(()=>missingArtifacts.verifyBaseSchema({runtime:true}),{code:'HA_SCHEMA_MISMATCH'});assert.equal(missingArtifacts.healthy(),false);}
  finally{await missingArtifacts.close();}
  assert.equal(provisionArtifactSchema(bootstrap),true);
  assert.equal(provisionArtifactSchema(bootstrap),false);
  bootstrap.exec('CREATE TABLE migration_extension(id TEXT PRIMARY KEY)');
  await test.db.collection('_ha_schema').updateOne({_id:'migration_extension'},{$set:{'columns.0.type':'UNKNOWN_FUTURE_TYPE'}});
  const future=new MongoAdapter({...config,ownerId:'future-schema-boot'});
  try {
    assert.throws(()=>future.verifyBaseSchema(),{code:'HA_SCHEMA_MISMATCH'});
    assert.equal(future.healthy(),false,'Unknown noncore schema must refuse readiness before any route accesses it');
    assert.equal(future.canWrite(),false);
  } finally {await future.close();}
  await test.db.collection('_ha_schema').updateOne({_id:'migration_extension'},{$set:{'columns.0.type':'TEXT'}});
  console.log('PASS promotion schema capability gate: an unknown noncore type refuses readiness and writes; restored metadata requires a fresh connection');
  await bootstrap.close();
  active = new MongoAdapter({ ...config, ownerId: 'boot-primary' });
  assert.ok(active.ownership('claim', 30000));
  standby = new MongoAdapter({ ...config, ownerId: 'boot-standby' });
  runtime = attachActiveRuntime(standby, { recycle: reason => retired.push(reason) });
  const before = await test.db.collection('_ha_serial').findOne({ _id: 'writer' });
  const fetchBefore = globalThis.fetch;
  await register();
  await runtime.owner.step();
  assert.equal(runtime.ready(), false);
  assert.equal(getServerLifecyclePhase(), 'starting');
  assert.equal(globalThis.fetch, fetchBefore);
  assert.deepEqual(await test.db.collection('_ha_serial').findOne({ _id: 'writer' }), before);
  assert.equal(retired.length, 0);
  console.log('PASS real MongoDB standby startup: empty schema rejected; complete base registry verified, existing active owner preserved, upstream jobs not booted');
} finally {
  runtime?.retire('end isolated standby test');
  await standby?.close(); await active?.close(); await bootstrap?.close();
  delete globalThis.__omnirouteActiveRuntime; delete globalThis.__omnirouteMongoAdapter; delete globalThis.__omnirouteMongoVectors;
  await test.cleanup();
}
